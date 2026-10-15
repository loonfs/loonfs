//! Clocks used for durable timestamps and local publication time limits.

use crate::error::{CoreError, Result};
use crate::limits::{METADATA_PUBLICATION_BUDGET_MS, READ_REVALIDATION_BOUND_MS};
use loonfs_types::NamespaceId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};

/// The start of one bounded publication, measured on a monotonic timer.
#[derive(Debug, Clone)]
pub struct Deadline {
    timer: Arc<dyn MonotonicTimer>,
    started_at_ms: u64,
    started_order: u64,
}

impl Deadline {
    /// Starts a deadline at the timer's current time.
    pub fn start(timer: Arc<dyn MonotonicTimer>) -> Self {
        let started_order = next_observation_order();
        let started_at_ms = timer.monotonic_now_ms();
        Self {
            timer,
            started_at_ms,
            started_order,
        }
    }

    /// Returns the milliseconds elapsed since the deadline started.
    pub fn elapsed_ms(&self) -> u64 {
        self.now_ms().saturating_sub(self.started_at_ms)
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
            at_ms: self.started_at_ms,
            order: self.started_order,
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

/// The order of the next observation that the process takes.
static NEXT_OBSERVATION_ORDER: AtomicU64 = AtomicU64::new(0);

fn next_observation_order() -> u64 {
    NEXT_OBSERVATION_ORDER.fetch_add(1, Ordering::SeqCst)
}

/// One reading of a monotonic timer, used to measure the age of a check.
#[derive(Debug, Clone)]
pub struct Observation {
    timer: Arc<dyn MonotonicTimer>,
    at_ms: u64,
    /// Where this observation falls among every observation that the
    /// process takes. Two readings in the same millisecond have the same
    /// time, so the order is what tells which one was taken first.
    order: u64,
}

impl Observation {
    /// Reads the timer's current time.
    pub fn now(timer: Arc<dyn MonotonicTimer>) -> Self {
        let order = next_observation_order();
        let at_ms = timer.monotonic_now_ms();
        Self {
            timer,
            at_ms,
            order,
        }
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

    /// Returns whether this observation was taken after `other`. When one
    /// observation was taken before the other began, the order of the two
    /// observations follows, also within one millisecond.
    pub fn is_after(&self, other: &Self) -> bool {
        self.order > other.order
    }

    /// Returns the newer of this observation and `other`.
    pub fn newer(self, other: Self) -> Self {
        if other.is_after(&self) {
            other
        } else {
            self
        }
    }

    pub(crate) fn age_at(&self, observation: &Self) -> u64 {
        observation.at_ms.saturating_sub(self.at_ms)
    }
}

/// The wall clock a writer stamps into durable state.
pub trait WallClock: std::fmt::Debug + Send + Sync {
    /// Unix milliseconds now.
    fn now_ms(&self) -> u64;
}

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> u64 {
        current_time_ms()
    }
}

/// Reads the wall clock as unix milliseconds.
///
/// Every timestamp that reaches durable state — commit, checkpoint, upload
/// session, maintenance schedule — is stamped here and then carried as a
/// value, so replay below this boundary stays deterministic.
#[allow(clippy::disallowed_methods)]
pub fn current_time_ms() -> u64 {
    unix_ms(SystemTime::now())
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

/// Converts a wall-clock instant to unix milliseconds.
fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| {
            u64::try_from(elapsed.as_millis()).expect("unix milliseconds should fit u64")
        })
        .expect(
            "the system clock should read after the unix epoch, which the runtime builder \
             checks with `system_clock_reads_after_epoch` before it starts",
        )
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
        assert_eq!(unix_ms(UNIX_EPOCH + Duration::from_millis(1)), 1);
    }
}
