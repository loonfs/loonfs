//! Clocks used for durable timestamps and local publication time limits.

use crate::error::{CoreError, Result};
use crate::limits::{METADATA_PUBLICATION_BUDGET_MS, READ_REVALIDATION_BOUND_MS};
use loonfs_api::NamespaceId;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};

/// The start of one bounded publication, measured on a monotonic timer.
#[derive(Debug, Clone)]
pub struct Deadline {
    timer: Arc<dyn MonotonicTimer>,
    started_ms: u64,
}

impl Deadline {
    /// Starts a deadline at the timer's current time.
    pub fn start(timer: Arc<dyn MonotonicTimer>) -> Self {
        let started_ms = timer.monotonic_now_ms();
        Self { timer, started_ms }
    }

    /// Returns the milliseconds elapsed since the deadline started.
    pub fn elapsed_ms(&self) -> u64 {
        self.now_ms().saturating_sub(self.started_ms)
    }

    fn now_ms(&self) -> u64 {
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
            at_ms: self.started_ms,
        }
    }

    pub(crate) fn elapsed_at(&self, observation: &Observation) -> u64 {
        observation.at_ms.saturating_sub(self.started_ms)
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

/// The wall clock a writer stamps into durable state.
pub trait WallClock: std::fmt::Debug + Send + Sync {
    /// Unix milliseconds now.
    fn now_ms(&self) -> Result<u64>;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> Result<u64> {
        current_time_ms()
    }
}

/// Reads the wall clock as unix milliseconds.
///
/// Every timestamp that reaches durable state — commit, checkpoint, upload
/// session, maintenance schedule — is stamped here and then carried as a
/// value, so replay below this boundary stays deterministic.
#[allow(clippy::disallowed_methods)]
pub fn current_time_ms() -> Result<u64> {
    unix_ms(SystemTime::now())
}

/// Converts a wall-clock instant to unix milliseconds, failing clearly on a
/// pre-epoch clock instead of stamping timestamp zero into durable state.
fn unix_ms(now: SystemTime) -> Result<u64> {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|_| CoreError::Internal("system time is before unix epoch".to_owned()))
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
    fn pre_epoch_clock_is_an_error_not_timestamp_zero() {
        let error = unix_ms(UNIX_EPOCH - Duration::from_secs(1)).expect_err("pre-epoch must fail");
        assert!(
            error.to_string().contains("before unix epoch"),
            "unexpected error: {error}"
        );
    }
}
