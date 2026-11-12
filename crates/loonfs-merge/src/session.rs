//! A merge session: the steps that merge a fork, split a merge that's too
//! large, rebase a fork whose merge conflicts, hand the new fork to the
//! resolver, and merge again.

use crate::refusal::{is_gone, refusal, refused, NextRequest, Refusal};
use crate::{
    ConflictReport, Mechanics, MergeError, MergeFailure, MergeSessionOutcome, MergeSessionState,
    MergeSessionStatus, PendingCommit, Resolver, Result,
};
use loonfs_types::{
    CommitSeq, ErrorCode, ErrorResponse, MergeConflict, NamespaceId, NextMergeRangeOutcome,
};

/// The most new forks that one session's rebases create.
///
/// A session creates a new fork for each merge that conflicts, and to
/// replace a new fork whose conflict report was lost. A new fork's merge
/// conflicts only if the namespace changed one of the new fork's files after
/// the rebase created the new fork, so a session that reaches this limit is
/// merging into a namespace that doesn't stop conflicting. The limit bounds
/// the state's list of new forks, and the new forks that recovery finds.
pub const MAX_NEW_FORKS: usize = 8;

/// Options for starting a merge session.
#[derive(Debug, Clone, Default)]
pub struct MergeSessionOptions {
    /// The fork sequence to merge the original fork through, or `None` for
    /// the fork's head when the session starts.
    pub through_seq: Option<CommitSeq>,
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
        through_seq: CommitSeq,
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
        Ok(match refusal(merge, &error) {
            Refusal::Next(next) => {
                next_request(merge, next, error, |merge| MergeSessionStatus::Merging {
                    fork_id: fork_id.clone(),
                    through_seq,
                    merge,
                })
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

    /// Creates `new_fork_id` for the rebase of `fork_id` unless the new fork
    /// exists, sends the pending replay commit into the new fork, and moves
    /// on to the rest of the range, to a smaller piece, or to the resolver.
    async fn rebase(
        &self,
        fork_id: &NamespaceId,
        through_seq: CommitSeq,
        new_fork_id: &NamespaceId,
        replay: &PendingCommit,
        reports: &[ConflictReport],
    ) -> Result<Transition> {
        let created = self
            .mechanics
            .fork_namespace_for_rebase(
                &self.state.namespace_id,
                fork_id,
                new_fork_id,
                &self.state.actor_id,
            )
            .await;
        // `namespace_exists` means that an earlier step created the new fork,
        // so the replay commit goes into it.
        let answer = match created {
            Err(error) if error.code() != Some(ErrorCode::NamespaceExists) => Err(error),
            _ => {
                self.mechanics
                    .replay_fork_with_options(
                        new_fork_id,
                        fork_id,
                        &self.state.actor_id,
                        &replay.options(),
                    )
                    .await
            }
        };
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
                None => return Ok(self.replace_new_fork(fork_id, through_seq, new_fork_id)),
            },
            Err(error) => refused(error)?,
        };
        Ok(match refusal(replay, &error) {
            Refusal::Next(next) => {
                next_request(replay, next, error, |replay| MergeSessionStatus::Rebasing {
                    fork_id: fork_id.clone(),
                    through_seq,
                    new_fork_id: new_fork_id.clone(),
                    replay,
                    reports: reports.to_vec(),
                })
            }
            // A rebase reports conflicts instead of returning
            // `merge_conflict`, so the loop has nothing to do with one.
            Refusal::Conflict | Refusal::Fail => {
                Transition::failed(fork_id, MergeFailure::Refused { error }, reports)
            }
        })
    }

    /// Starts the rebase again into another new fork after the conflict
    /// report of `new_fork_id` was lost, which repeats the classification of
    /// every change.
    ///
    /// `new_fork_id` stays in the state's new forks, and the session deletes
    /// it with the others once it's `merged` or `abandoned`. If this step
    /// deleted it, a run that stopped before the caller saved the step's state
    /// would resume from a state whose new fork is deleted, and its rebase
    /// would fail with `namespace_deleted`.
    fn replace_new_fork(
        &self,
        fork_id: &NamespaceId,
        through_seq: CommitSeq,
        new_fork_id: &NamespaceId,
    ) -> Transition {
        let Some(replacement_id) = self.new_fork_id() else {
            return Transition::failed(fork_id, MergeFailure::NewForkLimit, &[]);
        };
        Transition {
            status: MergeSessionStatus::Rebasing {
                fork_id: fork_id.clone(),
                through_seq,
                new_fork_id: replacement_id.clone(),
                replay: PendingCommit::new(through_seq),
                reports: Vec::new(),
            },
            created_fork_id: Some(replacement_id.clone()),
            outcome: MergeSessionOutcome::ReportLost {
                replaced_fork_id: new_fork_id.clone(),
                new_fork_id: replacement_id,
            },
        }
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
        let status = status_after_range(new_fork_id, through_seq, &range.outcome, reports);
        let outcome = match status {
            MergeSessionStatus::Failed { .. } => MergeSessionOutcome::Failed,
            _ => MergeSessionOutcome::Resolved {
                fork_id: new_fork_id.clone(),
                through_seq,
            },
        };
        Ok(Transition::new(status, outcome))
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
    (new_fork_ids.len() < MAX_NEW_FORKS).then(NamespaceId::generate)
}

/// Returns the transition to the request that follows `request` after the
/// server refused it with `error`: the piece through the split hint, or the
/// same request again. `status` returns the step's status with the next
/// request pending.
fn next_request(
    request: &PendingCommit,
    next: NextRequest,
    error: ErrorResponse,
    status: impl FnOnce(PendingCommit) -> MergeSessionStatus,
) -> Transition {
    match next {
        NextRequest::Split(split_at_seq) => Transition::new(
            status(PendingCommit::new(split_at_seq)),
            MergeSessionOutcome::Split {
                error,
                split_at_seq,
            },
        ),
        NextRequest::Retry => {
            let request = request.retried();
            let failed_attempts = request.failed_attempts;
            Transition::new(
                status(request),
                MergeSessionOutcome::Retrying {
                    error,
                    failed_attempts,
                },
            )
        }
    }
}

/// Returns the status after the next-range query of `fork_id` returned
/// `outcome`: `merging` through `through_seq`, `merged` when the fork's next
/// merge starts at or after `through_seq`, or `failed` with `reports` when a
/// retention floor refuses that merge or the outcome is one that this crate
/// doesn't know.
pub(crate) fn status_after_range(
    fork_id: &NamespaceId,
    through_seq: CommitSeq,
    outcome: &NextMergeRangeOutcome,
    reports: &[ConflictReport],
) -> MergeSessionStatus {
    let failure = match *outcome {
        NextMergeRangeOutcome::Open { next_after_seq, .. } => {
            if next_after_seq >= through_seq {
                return MergeSessionStatus::Merged;
            }
            return MergeSessionStatus::Merging {
                fork_id: fork_id.clone(),
                through_seq,
                merge: PendingCommit::new(through_seq),
            };
        }
        NextMergeRangeOutcome::RangeStartBelowFloor {
            retention_floor_seq,
            ..
        } => MergeFailure::RangeStartBelowFloor {
            retention_floor_seq,
        },
        NextMergeRangeOutcome::ForkPointBelowFloor {
            retention_floor_seq,
        } => MergeFailure::ForkPointBelowFloor {
            retention_floor_seq,
        },
        NextMergeRangeOutcome::Unknown => MergeFailure::UnknownRangeOutcome,
    };
    MergeSessionStatus::Failed {
        fork_id: fork_id.clone(),
        failure,
        reports: reports.to_vec(),
    }
}

/// Returns the status after a replay commit through `replay.through_seq`
/// committed with `conflicts`: the rest of the range, or the resolver.
fn replayed(
    fork_id: &NamespaceId,
    through_seq: CommitSeq,
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
