//! Progress and budget state for iterative maintenance and grep operations.

use loonfs::{MaintenanceJobId, MaintenanceStepConclusion};
use loonfs_api::NamespaceId;

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
    /// Number of status checks or maintenance steps performed.
    pub steps: u64,
    /// True when the index reached the target sequence.
    pub reached: bool,
}

/// Waits before the next remote status check.
#[allow(clippy::disallowed_methods)]
pub(super) async fn rest_between_status_checks() {
    tokio::time::sleep(std::time::Duration::from_millis(
        REMOTE_STATUS_POLL_INTERVAL_MS,
    ))
    .await;
}
