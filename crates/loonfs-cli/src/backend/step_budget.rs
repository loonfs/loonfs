//! Progress and budget state for waiting on a grep index.

use crate::error::Result;
use crate::resolve::{Backend, ResolvedTarget};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepProgress, GrepReorganizeOutcome,
    DEFAULT_GREP_PUBLISH_INTERVAL_MS,
};
use loonfs_types::{CommitSeq, GrepIndexStatus, MonotonicTimer, NamespaceId, StdMonotonicTimer};
use std::future::Future;

impl ResolvedTarget {
    pub(crate) async fn wait_for_grep_index(
        &self,
        namespace_id: &NamespaceId,
        target_seq: CommitSeq,
        budget: StepBudget,
    ) -> Result<GrepWaitProgress> {
        let read_published =
            || async { Ok(self.client.get_grep_index(namespace_id).await?.status) };
        let Backend::Embedded { host, .. } = &self.backend else {
            return wait_for_grep_index(target_seq, budget, read_published, || async {
                rest_between_status_checks().await;
                Ok(GrepWaitOutcome::Continue)
            })
            .await;
        };
        // The steps' changes are kept in memory between publications, so the
        // wait reads the pending status, and each step keeps the run locked
        // across its awaits.
        let steps = tokio::sync::Mutex::new(
            host.grep_worker
                .steps(namespace_id, DEFAULT_GREP_PUBLISH_INTERVAL_MS),
        );
        let progress = wait_for_grep_index(
            target_seq,
            budget,
            || async {
                let pending = steps
                    .lock()
                    .await
                    .pending_status()
                    .map(GrepIndexStatus::from);
                match pending {
                    Some(status) => Ok(status),
                    None => read_published().await,
                }
            },
            || async {
                let outcome = steps
                    .lock()
                    .await
                    .step(GramIndexBuildPolicy::default())
                    .await?;
                Ok(match outcome {
                    GrepProgress::Built(
                        GrepBuildOutcome::Indexed { .. }
                        | GrepBuildOutcome::BackfillRestarted { .. }
                        | GrepBuildOutcome::Superseded,
                    )
                    | GrepProgress::Reorganized(
                        GrepReorganizeOutcome::UnitWritten { .. }
                        | GrepReorganizeOutcome::Abandoned
                        | GrepReorganizeOutcome::Superseded,
                    ) => GrepWaitOutcome::Continue,
                    GrepProgress::Built(
                        GrepBuildOutcome::UpToDate { .. } | GrepBuildOutcome::NotEnabled,
                    )
                    | GrepProgress::Reorganized(
                        GrepReorganizeOutcome::NotNeeded { .. } | GrepReorganizeOutcome::NotEnabled,
                    ) => GrepWaitOutcome::Settled,
                })
            },
        )
        .await;
        // A one-shot run has no later maintenance interval to publish what's
        // pending. A step
        // changes the pending entry only once it succeeds, so the run
        // publishes the steps before a failed one, and then reports the
        // failure.
        let published = steps.into_inner().publish().await;
        let progress = progress?;
        published?;
        Ok(GrepWaitProgress {
            reached: read_published().await?.is_built_through(target_seq),
            ..progress
        })
    }
}

/// Delay between remote status checks.
const REMOTE_STATUS_POLL_INTERVAL_MS: u64 = 250;

/// Optional step-count and elapsed-time limits for iterative commands.
///
/// A step may be an index update, a remote status check, or a maintenance
/// operation. Commands without either limit continue until the work settles.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StepBudget {
    pub max_steps: Option<u64>,
    pub deadline_ms: Option<u64>,
}

impl StepBudget {
    pub(super) fn spent(&self, steps: u64, elapsed_ms: u64) -> bool {
        self.max_steps.is_some_and(|max_steps| steps >= max_steps)
            || self
                .deadline_ms
                .is_some_and(|deadline_ms| elapsed_ms >= deadline_ms)
    }
}

/// Result of waiting for a grep index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrepWaitProgress {
    /// Number of polling intervals or maintenance steps completed.
    pub steps: u64,
    /// True when the index reached the target sequence.
    pub reached: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GrepWaitOutcome {
    Continue,
    Settled,
}

/// Waits until the grep index reaches the target, stops, or exhausts the budget.
pub(super) async fn wait_for_grep_index<Read, ReadTurn, Step, StepTurn>(
    target_seq: CommitSeq,
    budget: StepBudget,
    read: Read,
    step: Step,
) -> Result<GrepWaitProgress>
where
    Read: Fn() -> ReadTurn,
    ReadTurn: Future<Output = Result<GrepIndexStatus>>,
    Step: Fn() -> StepTurn,
    StepTurn: Future<Output = Result<GrepWaitOutcome>>,
{
    let timer = StdMonotonicTimer::default();
    let started_at_ms = timer.monotonic_now_ms();
    let mut steps = 0;
    let mut settled = false;
    loop {
        let lifecycle = read().await?;
        let reached = lifecycle.is_built_through(target_seq);
        let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_at_ms);
        if reached || settled || lifecycle_stopped(&lifecycle) || budget.spent(steps, elapsed_ms) {
            return Ok(GrepWaitProgress { steps, reached });
        }
        settled = step().await? == GrepWaitOutcome::Settled;
        steps += 1;
    }
}

fn lifecycle_stopped(lifecycle: &GrepIndexStatus) -> bool {
    match lifecycle {
        GrepIndexStatus::Disabled | GrepIndexStatus::Unknown => true,
        GrepIndexStatus::Backfilling { .. } | GrepIndexStatus::Active { .. } => false,
    }
}

/// Waits before the next remote status check.
#[allow(
    clippy::disallowed_methods,
    reason = "the CLI host paces its own polling; protocol time is not affected"
)]
pub(super) async fn rest_between_status_checks() {
    tokio::time::sleep(std::time::Duration::from_millis(
        REMOTE_STATUS_POLL_INTERVAL_MS,
    ))
    .await;
}

#[cfg(test)]
mod tests {
    use super::{wait_for_grep_index, GrepWaitOutcome, GrepWaitProgress, StepBudget};
    use loonfs_types::{CommitSeq, GrepIndexStatus};
    use std::cell::Cell;

    #[tokio::test]
    async fn an_unbudgeted_wait_stops_where_the_index_stops() {
        let turns = Cell::new(0u64);
        let disabled = wait_for_grep_index(
            CommitSeq(3),
            StepBudget::default(),
            || async { Ok(GrepIndexStatus::Disabled) },
            || async {
                turns.set(turns.get() + 1);
                assert!(turns.get() < 4, "a disabled index must end the wait");
                Ok(GrepWaitOutcome::Continue)
            },
        )
        .await
        .expect("wait over a disabled index");
        assert_eq!(
            disabled,
            GrepWaitProgress {
                steps: 0,
                reached: false
            }
        );

        let backfilling = wait_for_grep_index(
            CommitSeq(3),
            StepBudget::default(),
            || async {
                Ok(GrepIndexStatus::Backfilling {
                    captured_seq: CommitSeq(3),
                    cursor_inode_id: None,
                    snapshot_id: loonfs_types::SnapshotId::parse(
                        "snp_00000000000000000001-00000000000123456789abcdef",
                    )
                    .expect("snapshot id"),
                })
            },
            || async { Ok(GrepWaitOutcome::Settled) },
        )
        .await
        .expect("wait over an index that settles short");
        assert_eq!(
            backfilling,
            GrepWaitProgress {
                steps: 1,
                reached: false
            }
        );
    }
}
