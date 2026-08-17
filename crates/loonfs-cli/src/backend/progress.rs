//! Progress and budget state for iterative maintenance and grep operations.

use loonfs::{MaintenanceJobId, MaintenanceStepConclusion};
use loonfs_api::NamespaceId;

/// How long a remote wait rests between status checks.
///
/// A hosted server drives its own index, so this is only how often the
/// command asks. Modest and fixed: the wait is bounded by the caller's own
/// budgets, not by how fast it polls.
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

/// One assigned `{job, namespace}` key, as a drain left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceKeyProgress {
    pub job: MaintenanceJobId,
    pub namespace_id: NamespaceId,
    /// Steps this drain ran for this key.
    pub steps: u64,
    /// What its last step concluded, absent when the budget ran out before
    /// the key took one.
    pub conclusion: Option<MaintenanceStepConclusion>,
}

impl MaintenanceKeyProgress {
    /// Whether this key has nothing left for a drain to drive.
    ///
    /// Progress and a lost race both leave work behind, so a drain keeps
    /// going. Everything else is where it stops — including `Blocked`, which
    /// says there is work this step's policy cannot move: repeating it would
    /// spin rather than finish.
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

/// Where a drain stopped and what it cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceDrainProgress {
    /// Every assigned key, in the order the drain drove them.
    pub keys: Vec<MaintenanceKeyProgress>,
    /// Steps this drain ran across every key.
    pub steps: u64,
}

impl MaintenanceDrainProgress {
    /// Whether the budget stopped the drain before every key settled. A key
    /// only goes unsettled that way: the loop that drives it ends at a
    /// settled conclusion or at a spent budget, and at nothing else.
    pub(crate) fn budget_exhausted(&self) -> bool {
        !self.keys.iter().all(MaintenanceKeyProgress::settled)
    }
}

/// Where a wait stopped and what it cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrepWaitProgress {
    /// Steps this wait spent.
    pub steps: u64,
    /// True when the index reached the target sequence.
    pub reached: bool,
}

/// The one timer this CLI owns: how long a remote wait rests between status
/// checks. Nothing durable depends on it — it only decides how often a
/// command that is already waiting asks again.
#[allow(clippy::disallowed_methods)]
pub(super) async fn rest_between_status_checks() {
    tokio::time::sleep(std::time::Duration::from_millis(
        REMOTE_STATUS_POLL_INTERVAL_MS,
    ))
    .await;
}
