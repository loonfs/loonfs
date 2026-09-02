//! Progress and budget state for iterative maintenance and grep operations.

use crate::error::CliError;
use loonfs::{MaintenanceJobId, MaintenanceStepConclusion};
use loonfs_api::v0::GrepIndexLifecycle;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::future::Future;

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

/// Final progress for one assigned maintenance job and namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceKeyProgress {
    pub job: MaintenanceJobId,
    pub namespace_id: NamespaceId,
    /// Steps this drain ran for this key.
    pub steps: u64,
    /// Result of the last step, or `None` if the budget expired first.
    pub conclusion: Option<MaintenanceStepConclusion>,
}

impl MaintenanceKeyProgress {
    /// Returns whether another step would make no immediate progress.
    ///
    /// `Progressed` and `Superseded` require another step. `Blocked` is
    /// settled because repeating the same step would not change the result.
    pub(crate) fn settled(&self) -> bool {
        match self.conclusion {
            Some(
                MaintenanceStepConclusion::Idle
                | MaintenanceStepConclusion::Blocked
                | MaintenanceStepConclusion::NotEnabled,
            ) => true,
            Some(MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded)
            | None => false,
        }
    }
}

/// Progress from draining a set of maintenance assignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceDrainProgress {
    /// Assignments in processing order.
    pub keys: Vec<MaintenanceKeyProgress>,
    /// Steps this drain ran across every key.
    pub steps: u64,
}

impl MaintenanceDrainProgress {
    /// Returns whether the budget expired before every assignment settled.
    pub(crate) fn budget_exhausted(&self) -> bool {
        !self.keys.iter().all(MaintenanceKeyProgress::settled)
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
pub(super) enum GrepWaitStep {
    Continue,
    Settled,
}

/// Waits until the grep index reaches the target, stops, or exhausts the budget.
pub(super) async fn wait_for_grep_index<Read, ReadTurn, Step, StepTurn>(
    target_seq: ChangeSeq,
    budget: StepBudget,
    read: Read,
    step: Step,
) -> Result<GrepWaitProgress, CliError>
where
    Read: Fn() -> ReadTurn,
    ReadTurn: Future<Output = Result<GrepIndexLifecycle, CliError>>,
    Step: Fn() -> StepTurn,
    StepTurn: Future<Output = Result<GrepWaitStep, CliError>>,
{
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let mut steps = 0;
    let mut settled = false;
    loop {
        let lifecycle = read().await?;
        let reached = lifecycle.is_built_through(target_seq);
        let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
        if reached || settled || lifecycle_stopped(&lifecycle) || budget.spent(steps, elapsed_ms) {
            return Ok(GrepWaitProgress { steps, reached });
        }
        settled = step().await? == GrepWaitStep::Settled;
        steps += 1;
    }
}

fn lifecycle_stopped(lifecycle: &GrepIndexLifecycle) -> bool {
    match lifecycle {
        GrepIndexLifecycle::Disabled => true,
        GrepIndexLifecycle::Backfilling { .. } | GrepIndexLifecycle::Active { .. } => false,
    }
}

/// Waits before the next remote status check.
#[allow(clippy::disallowed_methods)]
pub(super) async fn rest_between_status_checks() {
    tokio::time::sleep(std::time::Duration::from_millis(
        REMOTE_STATUS_POLL_INTERVAL_MS,
    ))
    .await;
}

#[cfg(test)]
mod tests {
    use super::{wait_for_grep_index, GrepWaitProgress, GrepWaitStep, StepBudget};
    use loonfs_api::v0::GrepIndexLifecycle;
    use loonfs_api::ChangeSeq;
    use std::cell::Cell;

    #[tokio::test]
    async fn an_unbudgeted_wait_stops_where_the_index_stops() {
        let turns = Cell::new(0u64);
        let disabled = wait_for_grep_index(
            ChangeSeq(3),
            StepBudget::default(),
            || async { Ok(GrepIndexLifecycle::Disabled) },
            || async {
                turns.set(turns.get() + 1);
                assert!(turns.get() < 4, "a disabled index must end the wait");
                Ok(GrepWaitStep::Continue)
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
            ChangeSeq(3),
            StepBudget::default(),
            || async {
                Ok(GrepIndexLifecycle::Backfilling {
                    target_seq: ChangeSeq(3),
                    cursor_inode_id: None,
                    checkpoint_id: loonfs_api::CheckpointId::parse(
                        "chk_0123456789abcdef0123456789abcdef",
                    )
                    .expect("checkpoint id"),
                })
            },
            || async { Ok(GrepWaitStep::Settled) },
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
