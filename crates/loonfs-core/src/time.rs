//! Clocks used for durable timestamps and local publication time limits.

use crate::error::{CoreError, Result};
use crate::limits::{METADATA_PUBLICATION_BUDGET_MS, READ_REVALIDATION_BOUND_MS};
use loonfs_types::NamespaceId;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use loonfs_types::MonotonicTimer;
#[cfg(test)]
pub(crate) use loonfs_types::StdMonotonicTimer;
pub use loonfs_types::{SystemWallClock, WallClock};

/// The clocks of one bounded operation: the wall time when the operation
/// started, and the monotonic timer reading at that moment.
///
/// The operation's durable timestamps record the wall time at the start. A
/// later check, such as an expiry check after a read, uses the wall time at
/// the start plus the monotonic time elapsed since the start.
#[derive(Debug, Clone)]
pub struct OperationClock {
    timer: Arc<dyn MonotonicTimer>,
    started_at_ms: u64,
    request_at_ms: u64,
}

impl OperationClock {
    /// Starts a clock for an operation that began at the wall time
    /// `request_at_ms`, at the timer's current reading.
    pub fn start(request_at_ms: u64, timer: Arc<dyn MonotonicTimer>) -> Self {
        let started_at_ms = timer.monotonic_now_ms();
        Self {
            timer,
            started_at_ms,
            request_at_ms,
        }
    }

    /// Returns the wall time when the operation started, in unix
    /// milliseconds.
    pub fn request_at_ms(&self) -> u64 {
        self.request_at_ms
    }

    /// Returns the current wall time of the operation: the wall time at the
    /// start plus the monotonic time elapsed since the start.
    pub fn now_ms(&self) -> u64 {
        self.wall_time_after(self.elapsed_ms())
    }

    /// Returns the wall time of `observation`, a reading of this clock's
    /// timer, by the same rule as [`Self::now_ms`].
    pub(crate) fn observed_at_ms(&self, observation: &Observation) -> u64 {
        self.wall_time_after(observation.at_ms.saturating_sub(self.started_at_ms))
    }

    fn wall_time_after(&self, elapsed_ms: u64) -> u64 {
        self.request_at_ms.saturating_add(elapsed_ms)
    }

    /// Returns the milliseconds elapsed since the operation started.
    pub fn elapsed_ms(&self) -> u64 {
        self.timer
            .monotonic_now_ms()
            .saturating_sub(self.started_at_ms)
    }

    /// Returns an observation of the current time on this clock's timer.
    pub fn observe(&self) -> Observation {
        Observation::now(Arc::clone(&self.timer))
    }

    /// Returns the observation this clock started from.
    pub(crate) fn origin(&self) -> Observation {
        Observation {
            timer: Arc::clone(&self.timer),
            at_ms: self.started_at_ms,
        }
    }

    /// Returns `MetadataPublicationBudgetExceeded` once more than
    /// [`METADATA_PUBLICATION_BUDGET_MS`] has elapsed since the operation
    /// started.
    pub fn ensure_metadata_publication_budget(&self, namespace_id: &NamespaceId) -> Result<()> {
        let elapsed_ms = self.elapsed_ms();
        if elapsed_ms <= METADATA_PUBLICATION_BUDGET_MS {
            return Ok(());
        }
        tracing::error!(
            namespace_id = namespace_id.as_str(),
            elapsed_ms,
            budget_ms = METADATA_PUBLICATION_BUDGET_MS,
            "metadata publication ran out of its budget; stopping before its next write",
        );
        Err(CoreError::MetadataPublicationBudgetExceeded {
            elapsed_ms,
            budget_ms: METADATA_PUBLICATION_BUDGET_MS,
        })
    }
}

/// One reading of a monotonic timer, used to measure the age of a check.
#[derive(Debug, Clone)]
pub struct Observation {
    timer: Arc<dyn MonotonicTimer>,
    at_ms: u64,
}

impl Observation {
    /// Reads the timer's current time.
    pub fn now(timer: Arc<dyn MonotonicTimer>) -> Self {
        let at_ms = timer.monotonic_now_ms();
        Self { timer, at_ms }
    }

    /// Reads the same timer again.
    pub fn renew(&self) -> Self {
        Self::now(Arc::clone(&self.timer))
    }

    /// Returns the milliseconds elapsed since this observation.
    pub fn age_ms(&self) -> u64 {
        self.timer.monotonic_now_ms().saturating_sub(self.at_ms)
    }

    /// Returns whether an answer arriving now is still inside the revalidation bound of this check.
    ///
    /// An absent manifest successor confirms a view only if the answer arrives less than
    /// [`READ_REVALIDATION_BOUND_MS`] after the view's previous check (format section 4.2).
    pub fn is_within_revalidation_bound(&self) -> bool {
        self.age_ms() < READ_REVALIDATION_BOUND_MS
    }

    pub(crate) fn age_at(&self, observation: &Self) -> u64 {
        observation.at_ms.saturating_sub(self.at_ms)
    }
}

/// Two observations are equal when they're the same reading of the same
/// timer.
impl PartialEq for Observation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.timer, &other.timer) && self.at_ms == other.at_ms
    }
}

impl Eq for Observation {}

/// Reads the wall clock as unix milliseconds.
///
/// Every timestamp that reaches durable state — commit, checkpoint, upload
/// session, maintenance schedule — is stamped here and then carried as a
/// value, so replay below this boundary stays deterministic.
pub fn current_time_ms() -> u64 {
    SystemWallClock.now_ms()
}

/// Reports whether the system clock reads after the unix epoch.
///
/// A runtime builder checks this once and refuses to start otherwise, so
/// [`current_time_ms`] can stay infallible: LoonFS stamps unix milliseconds
/// into durable state and can't run on a clock that reads before 1970.
#[allow(
    clippy::disallowed_methods,
    reason = "the startup check reads the system clock once"
)]
pub fn system_clock_reads_after_epoch() -> bool {
    reads_after_epoch(SystemTime::now())
}

fn reads_after_epoch(now: SystemTime) -> bool {
    now.duration_since(UNIX_EPOCH).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn retries_keep_the_clock_origin_and_observations_age_independently() {
        let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(100));
        let clock = OperationClock::start(5_000, timer.clone());
        timer.advance_ms(30);
        let observation = clock.observe();
        let retry = clock.clone();
        timer.advance_ms(20);
        assert_eq!(clock.elapsed_ms(), 50);
        assert_eq!(retry.elapsed_ms(), 50);
        assert_eq!(observation.age_ms(), 20);
        assert_eq!(clock.request_at_ms(), 5_000);
        assert_eq!(clock.observed_at_ms(&observation), 5_030);
        assert_eq!(retry.now_ms(), 5_050);
    }

    #[test]
    fn the_startup_clock_check_refuses_a_pre_epoch_clock() {
        assert!(!reads_after_epoch(UNIX_EPOCH - Duration::from_secs(1)));
        assert!(reads_after_epoch(UNIX_EPOCH + Duration::from_millis(1)));
    }
}
