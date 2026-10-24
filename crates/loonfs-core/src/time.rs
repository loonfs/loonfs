//! Clocks used for durable timestamps and local publication time limits.

use crate::error::{CoreError, Result};
use crate::limits::{METADATA_PUBLICATION_BUDGET_MS, READ_REVALIDATION_BOUND_MS};
use loonfs_types::NamespaceId;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
pub use loonfs_types::{SystemWallClock, WallClock};

/// The start of one bounded publication, measured on a monotonic timer.
#[derive(Debug, Clone)]
pub struct Deadline {
    timer: Arc<dyn MonotonicTimer>,
    started_at_ms: u64,
}

impl Deadline {
    /// Starts a deadline at the timer's current time.
    pub fn start(timer: Arc<dyn MonotonicTimer>) -> Self {
        let started_at_ms = timer.monotonic_now_ms();
        Self {
            timer,
            started_at_ms,
        }
    }

    /// Returns the milliseconds elapsed since the deadline started.
    pub fn elapsed_ms(&self) -> u64 {
        self.now_ms().saturating_sub(self.started_at_ms)
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.timer.monotonic_now_ms()
    }

    /// Returns an observation of the current time on this deadline's timer.
    pub fn observe(&self) -> Observation {
        Observation::now(Arc::clone(&self.timer))
    }

    /// Returns the observation this deadline started from.
    pub(crate) fn origin(&self) -> Observation {
        Observation {
            timer: Arc::clone(&self.timer),
            at_ms: self.started_at_ms,
        }
    }

    pub(crate) fn elapsed_at(&self, observation: &Observation) -> u64 {
        observation.at_ms.saturating_sub(self.started_at_ms)
    }

    /// Returns `MetadataPublicationBudgetExceeded` once more than
    /// [`METADATA_PUBLICATION_BUDGET_MS`] has elapsed since the deadline started.
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
#[allow(clippy::disallowed_methods)]
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
    fn retries_keep_the_deadline_origin_and_observations_age_independently() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(100));
        let deadline = Deadline::start(clock.clone());
        clock.advance_ms(30);
        let observation = deadline.observe();
        let retry = deadline.clone();
        clock.advance_ms(20);
        assert_eq!(deadline.elapsed_ms(), 50);
        assert_eq!(retry.elapsed_ms(), 50);
        assert_eq!(observation.age_ms(), 20);
        assert_eq!(deadline.elapsed_at(&observation), 30);
    }

    #[test]
    fn the_startup_clock_check_refuses_a_pre_epoch_clock() {
        assert!(!reads_after_epoch(UNIX_EPOCH - Duration::from_secs(1)));
        assert!(reads_after_epoch(UNIX_EPOCH + Duration::from_millis(1)));
    }
}
