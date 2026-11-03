//! A merge session: the steps that merge a fork, split a merge that's too
//! large, rebase a fork whose merge conflicts, hand the new fork to the
//! resolver, and merge again.

use crate::refusal::{is_gone, refusal, refused, Refusal};
use crate::{
    ConflictReport, Mechanics, MergeError, MergeFailure, MergeSessionOutcome, MergeSessionState,
    MergeSessionStatus, PendingCommit, Resolver, Result,
};
use loonfs_types::{generated_id, ActorId, ChangeSeq, MergeConflict, NamespaceId};

/// The most new forks that one session's rebases create.
///
/// A session creates a new fork for each merge that conflicts, and to
/// replace a new fork whose conflict report was lost. A new fork's merge
/// conflicts only if the namespace changed one of the new fork's files after
/// the rebase created the new fork, so a session that reaches this limit is
/// merging into a namespace that doesn't stop conflicting. The limit bounds
/// the state's list of new forks, and the passes that recovery makes over
/// the namespace's forks.
pub const MAX_NEW_FORKS: usize = 8;

/// Options for starting or recovering a merge session.
#[derive(Debug, Clone, Default)]
pub struct MergeSessionOptions {
    /// The fork sequence to merge the original fork through, or `None` for
    /// the fork's head when the session starts.
    pub through_seq: Option<ChangeSeq>,
}

/// One fork's merge session, which merges the fork through a [`Mechanics`]
/// implementation and hands conflicts to a [`Resolver`].
///
/// Each [`step`](Self::step) sends one or two requests and updates the
/// session's state only after its requests returned, so a step that fails or
/// is dropped leaves the state as it was, and the next step sends the same
/// requests again. A caller saves [`state`](Self::state) after every step.
pub struct MergeSession<M, R> {
    mechanics: M,
    resolver: R,
    state: MergeSessionState,
}

/// What one step changes in the session's state.
struct Transition {
    status: MergeSessionStatus,
    /// The new fork that the step chose for a rebase to create.
    created_fork_id: Option<NamespaceId>,
    outcome: MergeSessionOutcome,
}

impl Transition {
    fn new(status: MergeSessionStatus, outcome: MergeSessionOutcome) -> Self {
        Self {
            status,
            created_fork_id: None,
            outcome,
        }
    }

    fn failed(fork_id: &NamespaceId, failure: MergeFailure, reports: &[ConflictReport]) -> Self {
        Self::new(
            MergeSessionStatus::Failed {
                fork_id: fork_id.clone(),
                failure,
                reports: reports.to_vec(),
            },
            MergeSessionOutcome::Failed,
        )
    }
}

impl<M: Mechanics, R: Resolver> MergeSession<M, R> {
    /// Starts a session that merges `fork_id` into `namespace_id` as
    /// `actor_id`, after it reads where the fork's next merge starts.
    ///
    /// The session merges through `options.through_seq`, or through the
    /// fork's head. When the fork's next merge would start at or after that
    /// sequence, the session starts `merged`.
    pub async fn start(
        mechanics: M,
        resolver: R,
        namespace_id: NamespaceId,
        fork_id: NamespaceId,
        actor_id: ActorId,
        options: &MergeSessionOptions,
    ) -> Result<Self> {
        let range = mechanics.next_merge_range(&namespace_id, &fork_id).await?;
        let through_seq = options.through_seq.unwrap_or(range.source_head_seq);
        let status = merging_or_merged(fork_id.clone(), through_seq, range.next_after_seq);
        Ok(Self::resume(
            mechanics,
            resolver,
            MergeSessionState {
                namespace_id,
                original_fork_id: fork_id,
                actor_id,
                new_fork_ids: Vec::new(),
                status,
            },
        ))
    }

    /// Continues a session from the state that the caller saved after its
    /// last step. Performs no I/O.
    pub fn resume(mechanics: M, resolver: R, state: MergeSessionState) -> Self {
        Self {
            mechanics,
            resolver,
            state,
        }
    }

    /// Returns the session's state, which the caller saves after every step.
    pub fn state(&self) -> &MergeSessionState {
        &self.state
    }

    /// Runs the session's next step, and returns how it ended, or `None` when
    /// the session has no step left: it's `merged` or `abandoned` and has
    /// deleted its new forks, or it's `failed` and the caller chooses what
    /// happens next.
    pub async fn step(&mut self) -> Result<Option<MergeSessionOutcome>> {
        let transition = match &self.state.status {
            MergeSessionStatus::Merging {
                fork_id,
                through_seq,
                merge,
            } => self.merge(fork_id, *through_seq, merge).await?,
            MergeSessionStatus::Rebasing {
                fork_id,
                through_seq,
                new_fork_id,
                replay,
                reports,
            } => {
                self.rebase(fork_id, *through_seq, new_fork_id, replay, reports)
                    .await?
            }
            MergeSessionStatus::Resolving {
                new_fork_id,
                reports,
            } => self.resolve(new_fork_id, reports).await?,
            MergeSessionStatus::Merged | MergeSessionStatus::Abandoned => {
                return self.delete_new_fork().await
            }
            MergeSessionStatus::Failed { .. } => return Ok(None),
        };
        if let Some(new_fork_id) = transition.created_fork_id {
            self.state.new_fork_ids.push(new_fork_id);
        }
        self.state.status = transition.status;
        Ok(Some(transition.outcome))
    }

    /// Abandons a `failed` session, so that its next steps delete its new
    /// forks.
    pub fn abandon(&mut self) -> Result<()> {
        if !self.state.status.can_abandon() {
            return Err(MergeError::NotFailed {
                status: self.state.status.as_str(),
            });
        }
        self.state.status = MergeSessionStatus::Abandoned;
        Ok(())
    }

    /// Sends the pending merge of `fork_id`, and moves on to the rest of the
    /// range, to a rebase, or to a smaller piece.
    async fn merge(
        &self,
        fork_id: &NamespaceId,
        through_seq: ChangeSeq,
        merge: &PendingCommit,
    ) -> Result<Transition> {
        let answer = self
            .mechanics
            .merge_fork_with_options(
                &self.state.namespace_id,
                fork_id,
                &self.state.actor_id,
                &merge.options(),
            )
            .await;
        let error = match answer {
            Ok(commit) => {
                let status = if merge.through_seq < through_seq {
                    MergeSessionStatus::Merging {
                        fork_id: fork_id.clone(),
                        through_seq,
                        merge: PendingCommit::new(through_seq),
                    }
                } else {
                    MergeSessionStatus::Merged
                };
                return Ok(Transition::new(
                    status,
                    MergeSessionOutcome::MergeCommitted {
                        fork_id: fork_id.clone(),
                        commit_id: commit.commit_id,
                        committed_seq: commit.committed_seq,
                        through_seq: merge.through_seq,
                    },
                ));
            }
            Err(error) => refused(error)?,
        };
        let merging = |merge| MergeSessionStatus::Merging {
            fork_id: fork_id.clone(),
            through_seq,
            merge,
        };
        Ok(match refusal(merge, &error) {
            Refusal::Split(split_at_seq) => Transition::new(
                merging(PendingCommit::new(split_at_seq)),
                MergeSessionOutcome::Split {
                    error,
                    split_at_seq,
                },
            ),
            Refusal::Retry => {
                let merge = merge.retried();
                let failed_attempts = merge.failed_attempts;
                Transition::new(
                    merging(merge),
                    MergeSessionOutcome::Retrying {
                        error,
                        failed_attempts,
                    },
                )
            }
            Refusal::Conflict => match self.new_fork_id() {
                Some(new_fork_id) => Transition {
                    status: MergeSessionStatus::Rebasing {
                        fork_id: fork_id.clone(),
                        through_seq,
                        new_fork_id: new_fork_id.clone(),
                        replay: PendingCommit::new(through_seq),
                        reports: Vec::new(),
                    },
                    created_fork_id: Some(new_fork_id.clone()),
                    outcome: MergeSessionOutcome::Conflicted { error, new_fork_id },
                },
                None => Transition::failed(fork_id, MergeFailure::NewForkLimit, &[]),
            },
            Refusal::Fail => Transition::failed(fork_id, MergeFailure::Refused { error }, &[]),
        })
    }

    /// Sends the pending rebase of `fork_id` into `new_fork_id`, and moves on
    /// to the rest of the range, to a smaller piece, or to the resolver.
    async fn rebase(
        &self,
        fork_id: &NamespaceId,
        through_seq: ChangeSeq,
        new_fork_id: &NamespaceId,
        replay: &PendingCommit,
        reports: &[ConflictReport],
    ) -> Result<Transition> {
        let answer = self
            .mechanics
            .rebase_fork_with_options(
                &self.state.namespace_id,
                fork_id,
                new_fork_id,
                &self.state.actor_id,
                &replay.options(),
            )
            .await;
        let error = match answer {
            Ok(response) => match response.conflicts {
                Some(conflicts) => {
                    return Ok(replayed(
                        fork_id,
                        through_seq,
                        new_fork_id,
                        replay,
                        reports,
                        conflicts,
                    ))
                }
                None => {
                    return self
                        .replace_new_fork(fork_id, through_seq, new_fork_id)
                        .await
                }
            },
            Err(error) => refused(error)?,
        };
        let rebasing = |replay| MergeSessionStatus::Rebasing {
            fork_id: fork_id.clone(),
            through_seq,
            new_fork_id: new_fork_id.clone(),
            replay,
            reports: reports.to_vec(),
        };
        Ok(match refusal(replay, &error) {
            Refusal::Split(split_at_seq) => Transition::new(
                rebasing(PendingCommit::new(split_at_seq)),
                MergeSessionOutcome::Split {
                    error,
                    split_at_seq,
                },
            ),
            Refusal::Retry => {
                let replay = replay.retried();
                let failed_attempts = replay.failed_attempts;
                Transition::new(
                    rebasing(replay),
                    MergeSessionOutcome::Retrying {
                        error,
                        failed_attempts,
                    },
                )
            }
            // A rebase reports conflicts instead of returning
            // `merge_conflict`, so the loop has nothing to do with one.
            Refusal::Conflict | Refusal::Fail => {
                Transition::failed(fork_id, MergeFailure::Refused { error }, reports)
            }
        })
    }

    /// Deletes a new fork whose conflict report was lost, and starts the
    /// rebase again into another new fork, which repeats the classification
    /// of every change.
    async fn replace_new_fork(
        &self,
        fork_id: &NamespaceId,
        through_seq: ChangeSeq,
        new_fork_id: &NamespaceId,
    ) -> Result<Transition> {
        delete_if_present(&self.mechanics, new_fork_id).await?;
        let Some(replacement_id) = self.new_fork_id() else {
            return Ok(Transition::failed(fork_id, MergeFailure::NewForkLimit, &[]));
        };
        Ok(Transition {
            status: MergeSessionStatus::Rebasing {
                fork_id: fork_id.clone(),
                through_seq,
                new_fork_id: replacement_id.clone(),
                replay: PendingCommit::new(through_seq),
                reports: Vec::new(),
            },
            created_fork_id: Some(replacement_id.clone()),
            outcome: MergeSessionOutcome::ReportLost {
                deleted_fork_id: new_fork_id.clone(),
                new_fork_id: replacement_id,
            },
        })
    }

    /// Hands the new fork to the resolver, and then merges it through its
    /// head.
    async fn resolve(
        &self,
        new_fork_id: &NamespaceId,
        reports: &[ConflictReport],
    ) -> Result<Transition> {
        if let Err(error) = self.resolver.resolve(new_fork_id, reports).await {
            let failure = MergeFailure::Unresolved {
                message: error.to_string(),
            };
            return Ok(Transition::failed(new_fork_id, failure, reports));
        }
        let range = self
            .mechanics
            .next_merge_range(&self.state.namespace_id, new_fork_id)
            .await?;
        let through_seq = range.source_head_seq;
        Ok(Transition::new(
            merging_or_merged(new_fork_id.clone(), through_seq, range.next_after_seq),
            MergeSessionOutcome::Resolved {
                fork_id: new_fork_id.clone(),
                through_seq,
            },
        ))
    }

    /// Deletes the oldest new fork that the session hasn't deleted yet.
    async fn delete_new_fork(&mut self) -> Result<Option<MergeSessionOutcome>> {
        let Some(namespace_id) = self.state.new_fork_ids.first().cloned() else {
            return Ok(None);
        };
        delete_if_present(&self.mechanics, &namespace_id).await?;
        self.state.new_fork_ids.remove(0);
        Ok(Some(MergeSessionOutcome::Deleted { namespace_id }))
    }

    /// Returns a new fork ID for a rebase to create, or `None` once the
    /// session's rebases have created [`MAX_NEW_FORKS`].
    fn new_fork_id(&self) -> Option<NamespaceId> {
        new_fork_id(&self.state.new_fork_ids)
    }
}

/// Returns a new fork ID for a rebase to create, or `None` when
/// `new_fork_ids` already lists [`MAX_NEW_FORKS`].
pub(crate) fn new_fork_id(new_fork_ids: &[NamespaceId]) -> Option<NamespaceId> {
    (new_fork_ids.len() < MAX_NEW_FORKS).then(|| {
        NamespaceId::parse(generated_id("rebase"))
            .expect("a generated ID should be a valid namespace ID")
    })
}

/// Returns the status that merges `fork_id` through `through_seq`, or
/// `merged` when the fork's next merge starts at or after `through_seq`.
pub(crate) fn merging_or_merged(
    fork_id: NamespaceId,
    through_seq: ChangeSeq,
    next_after_seq: Option<ChangeSeq>,
) -> MergeSessionStatus {
    if next_after_seq.is_some_and(|after_seq| after_seq >= through_seq) {
        return MergeSessionStatus::Merged;
    }
    MergeSessionStatus::Merging {
        fork_id,
        through_seq,
        merge: PendingCommit::new(through_seq),
    }
}

/// Returns the status after a replay commit through `replay.through_seq`
/// committed with `conflicts`: the rest of the range, or the resolver.
fn replayed(
    fork_id: &NamespaceId,
    through_seq: ChangeSeq,
    new_fork_id: &NamespaceId,
    replay: &PendingCommit,
    reports: &[ConflictReport],
    conflicts: Vec<MergeConflict>,
) -> Transition {
    let conflict_count = conflicts.len();
    let mut reports = reports.to_vec();
    reports.push(ConflictReport {
        through_seq: replay.through_seq,
        conflicts,
    });
    let status = if replay.through_seq < through_seq {
        MergeSessionStatus::Rebasing {
            fork_id: fork_id.clone(),
            through_seq,
            new_fork_id: new_fork_id.clone(),
            replay: PendingCommit::new(through_seq),
            reports,
        }
    } else {
        MergeSessionStatus::Resolving {
            new_fork_id: new_fork_id.clone(),
            reports,
        }
    };
    Transition::new(
        status,
        MergeSessionOutcome::ReplayCommitted {
            new_fork_id: new_fork_id.clone(),
            through_seq: replay.through_seq,
            conflict_count,
        },
    )
}

/// Deletes `namespace_id`, unless it was already deleted or never created.
pub(crate) async fn delete_if_present(
    mechanics: &impl Mechanics,
    namespace_id: &NamespaceId,
) -> Result<()> {
    match mechanics.delete_namespace(namespace_id).await {
        Ok(_) => Ok(()),
        Err(error) if is_gone(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
