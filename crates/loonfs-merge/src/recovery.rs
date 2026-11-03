//! Recovering a merge session whose state was lost, from the store.

use crate::refusal::is_gone;
use crate::session::{new_fork_id, status_after_range};
use crate::{
    Mechanics, MergeError, MergeFailure, MergeSession, MergeSessionOptions, MergeSessionState,
    MergeSessionStatus, PendingCommit, Resolver, Result, MAX_NEW_FORKS,
};
use loonfs_types::{ActorId, CheckpointOwner, NamespaceId, NextMergeRangeOutcome};

/// A fork of the session's namespace that a rebase created.
struct RebasedFork {
    namespace_id: NamespaceId,
    /// The fork whose unmerged commits the rebase replayed into this one.
    rebased_from: NamespaceId,
}

impl<M: Mechanics, R: Resolver> MergeSession<M, R> {
    /// Recovers the session that merges `original_fork_id` into
    /// `namespace_id` as `actor_id`, for a caller that lost the session's
    /// state, from what the store records.
    ///
    /// The session's new forks are the forks of `namespace_id` whose fork
    /// basis records a rebase of the original fork or of another new fork,
    /// which the namespace's checkpoint inventory lists. Recovery follows
    /// them from the original fork. A new fork that merged into the
    /// namespace, or that was itself rebased, is one that the lost session
    /// continued with, so the session merges the last fork that it continued
    /// with. When that fork was rebased into new forks that never merged and
    /// weren't rebased, their conflict reports are lost, so the session
    /// rebases the fork again into another new fork, which repeats the
    /// classification. The session deletes every new fork that it found once
    /// it's `merged` or `abandoned`.
    ///
    /// The session merges the original fork through `options.through_seq`,
    /// or through its head, and a new fork through its head. It starts
    /// `failed` when the next-range query of the fork to merge says that the
    /// fork can't merge, as in [`start`](Self::start), or when the query of a
    /// new fork returns an outcome that this crate doesn't know.
    pub async fn recover(
        mechanics: M,
        resolver: R,
        namespace_id: NamespaceId,
        original_fork_id: NamespaceId,
        actor_id: ActorId,
        options: &MergeSessionOptions,
    ) -> Result<Self> {
        let rebased = find_rebased_forks(&mechanics, &namespace_id, &original_fork_id).await?;
        let mut new_fork_ids: Vec<NamespaceId> = rebased
            .iter()
            .map(|fork| fork.namespace_id.clone())
            .collect();
        let mut fork_id = original_fork_id.clone();
        // Each pass that doesn't end the loop moves to a fork that `rebased`
        // lists, and the forks form a tree under the original fork, so the
        // loop makes at most `rebased.len()` + 1 passes.
        let status = 'recover: loop {
            let mut continued = Vec::new();
            let mut was_rebased = false;
            for new_fork in rebased.iter().filter(|fork| fork.rebased_from == fork_id) {
                was_rebased = true;
                let rebased_again = rebased
                    .iter()
                    .any(|fork| fork.rebased_from == new_fork.namespace_id);
                let was_continued = rebased_again
                    || match mechanics
                        .next_merge_range(&namespace_id, &new_fork.namespace_id)
                        .await?
                        .outcome
                    {
                        NextMergeRangeOutcome::Open { merge, .. }
                        | NextMergeRangeOutcome::RangeStartBelowFloor { merge, .. } => {
                            merge.is_some()
                        }
                        // A merge of this new fork would return
                        // `rebootstrap_required`, so recovery doesn't
                        // continue with it.
                        NextMergeRangeOutcome::ForkPointBelowFloor { .. } => false,
                        NextMergeRangeOutcome::Unknown => {
                            break 'recover MergeSessionStatus::Failed {
                                fork_id: new_fork.namespace_id.clone(),
                                failure: MergeFailure::UnknownRangeOutcome,
                                reports: Vec::new(),
                            };
                        }
                    };
                if was_continued {
                    continued.push(new_fork.namespace_id.clone());
                }
            }
            if continued.len() > 1 {
                return Err(MergeError::AmbiguousRecovery {
                    fork_id,
                    new_fork_ids: continued,
                });
            }
            if let Some(continued_id) = continued.pop() {
                fork_id = continued_id;
                continue;
            }
            let range = mechanics.next_merge_range(&namespace_id, &fork_id).await?;
            let through_seq = match options.through_seq {
                Some(through_seq) if fork_id == original_fork_id => through_seq,
                _ => range.source_head_seq,
            };
            let status = status_after_range(&fork_id, through_seq, &range.outcome, &[]);
            if !was_rebased || matches!(status, MergeSessionStatus::Failed { .. }) {
                break status;
            }
            let Some(new_fork_id) = new_fork_id(&new_fork_ids) else {
                break MergeSessionStatus::Failed {
                    fork_id,
                    failure: MergeFailure::NewForkLimit,
                    reports: Vec::new(),
                };
            };
            new_fork_ids.push(new_fork_id.clone());
            break MergeSessionStatus::Rebasing {
                fork_id,
                through_seq,
                new_fork_id,
                replay: PendingCommit::new(through_seq),
                reports: Vec::new(),
            };
        };
        Ok(Self::resume(
            mechanics,
            resolver,
            MergeSessionState {
                namespace_id,
                original_fork_id,
                actor_id,
                new_fork_ids,
                status,
            },
        ))
    }
}

/// Returns the forks of `namespace_id` that rebases of `original_fork_id`,
/// or of another fork that the result lists, created.
///
/// Each pass reads the namespace's checkpoint inventory and the namespace
/// of every fork pin's target. A fork's pin can come before the pin of the
/// fork that it was rebased from, so a pass that finds a new fork makes
/// another pass.
async fn find_rebased_forks(
    mechanics: &impl Mechanics,
    namespace_id: &NamespaceId,
    original_fork_id: &NamespaceId,
) -> Result<Vec<RebasedFork>> {
    let mut found: Vec<RebasedFork> = Vec::new();
    let is_found =
        |found: &[RebasedFork], id: &NamespaceId| found.iter().any(|fork| fork.namespace_id == *id);
    // A pass that finds no new fork ends the search. Every other pass finds
    // one or more, and `found` stops at `MAX_NEW_FORKS`, so the search makes
    // at most `MAX_NEW_FORKS` + 1 passes.
    loop {
        let found_before = found.len();
        let mut pages = mechanics.list_checkpoints(namespace_id);
        while let Some(page) = pages.next().await {
            for checkpoint in page?.checkpoints {
                let CheckpointOwner::Fork {
                    target_namespace_id,
                } = checkpoint.owner
                else {
                    continue;
                };
                if target_namespace_id == *original_fork_id
                    || is_found(&found, &target_namespace_id)
                {
                    continue;
                }
                let Some(rebased_from) = rebased_from(mechanics, &target_namespace_id).await?
                else {
                    continue;
                };
                if rebased_from != *original_fork_id && !is_found(&found, &rebased_from) {
                    continue;
                }
                if found.len() == MAX_NEW_FORKS {
                    return Err(MergeError::TooManyNewForks {
                        original_fork_id: original_fork_id.clone(),
                    });
                }
                found.push(RebasedFork {
                    namespace_id: target_namespace_id,
                    rebased_from,
                });
            }
        }
        if found.len() == found_before {
            return Ok(found);
        }
    }
}

/// Returns the fork whose commits a rebase replayed into `namespace_id`, or
/// `None` when a rebase didn't create the namespace or the namespace is
/// gone.
async fn rebased_from(
    mechanics: &impl Mechanics,
    namespace_id: &NamespaceId,
) -> Result<Option<NamespaceId>> {
    match mechanics.get_namespace(namespace_id).await {
        Ok(namespace) => Ok(namespace
            .fork_basis
            .and_then(|basis| basis.rebased_from)
            .map(|rebased_from| rebased_from.source_namespace_id)),
        Err(error) if is_gone(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
