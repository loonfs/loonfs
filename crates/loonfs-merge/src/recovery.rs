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
    /// Starts the session that merges `original_fork_id` into
    /// `namespace_id` as `actor_id`, and recovers what an earlier run of the
    /// session did from what the store records, for a caller that has no
    /// saved state.
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
    /// or through its head, and a new fork through its head. When the next
    /// merge of the fork to merge would start at or after that sequence, the
    /// session starts `merged`. When a retention floor refuses that merge, or
    /// the next-range query of the fork or of a new fork returns an outcome
    /// that this crate doesn't know, the session starts `failed`. With no
    /// new forks, the session merges the original fork.
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
/// The search reads the namespace's checkpoint inventory once, and the
/// namespace of each fork pin's target once. A fork's pin can come before
/// the pin of the fork that it was rebased from, so the search follows the
/// rebases from the original fork only after it read every pin.
async fn find_rebased_forks(
    mechanics: &impl Mechanics,
    namespace_id: &NamespaceId,
    original_fork_id: &NamespaceId,
) -> Result<Vec<RebasedFork>> {
    let mut rebased: Vec<RebasedFork> = Vec::new();
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
                || rebased
                    .iter()
                    .any(|fork| fork.namespace_id == target_namespace_id)
            {
                continue;
            }
            if let Some(rebased_from) = rebased_from(mechanics, &target_namespace_id).await? {
                rebased.push(RebasedFork {
                    namespace_id: target_namespace_id,
                    rebased_from,
                });
            }
        }
    }
    let mut found: Vec<RebasedFork> = Vec::new();
    let mut parent_id = original_fork_id.clone();
    // The forks form a tree under the original fork. Each turn moves the
    // forks rebased from `parent_id` into `found`, and then takes the next
    // fork that `found` lists as `parent_id`.
    for next in 0.. {
        for fork in rebased.extract_if(.., |fork| fork.rebased_from == parent_id) {
            if found.len() == MAX_NEW_FORKS {
                return Err(MergeError::TooManyNewForks {
                    original_fork_id: original_fork_id.clone(),
                });
            }
            found.push(fork);
        }
        let Some(fork) = found.get(next) else {
            break;
        };
        parent_id = fork.namespace_id.clone();
    }
    Ok(found)
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
