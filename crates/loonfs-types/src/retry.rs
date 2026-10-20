//! Monotonic timers, the wall clock, and bounded transport retry policy.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Supplies monotonic milliseconds for retry deadlines and deterministic test injection.
pub trait MonotonicTimer: std::fmt::Debug + Send + Sync {
    /// Returns milliseconds since an arbitrary per-timer origin.
    fn monotonic_now_ms(&self) -> u64;
}

/// Measures monotonic elapsed time, including host sleep.
///
/// Every instance measures from one process-wide origin, so readings from any two standard timers
/// are comparable.
#[derive(Debug, Default)]
pub struct StdMonotonicTimer(());

impl MonotonicTimer for StdMonotonicTimer {
    fn monotonic_now_ms(&self) -> u64 {
        static ORIGIN: OnceLock<Duration> = OnceLock::new();
        let now = monotonic_now();
        let origin = ORIGIN.get_or_init(|| now);
        saturating_duration_ms(now.saturating_sub(*origin))
    }
}

/// These clocks count host sleep, so publication budgets keep advancing where `Instant` would stop.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[allow(clippy::disallowed_methods, unsafe_code)]
fn monotonic_now() -> Duration {
    #[cfg(target_os = "linux")]
    const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
    #[cfg(target_vendor = "apple")]
    const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // The pointer refers to writable storage for one initialized timespec.
    let result = unsafe { libc::clock_gettime(CLOCK, &mut time) };
    assert_eq!(result, 0, "the monotonic clock should be available");
    Duration::new(
        u64::try_from(time.tv_sec).expect("monotonic seconds should be nonnegative"),
        u32::try_from(time.tv_nsec).expect("monotonic nanoseconds should fit u32"),
    )
}

/// `Instant` stops during host sleep. That is adequate for the client's retry timing, and the
/// runtime refuses to build on these platforms.
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
#[allow(clippy::disallowed_methods)]
fn monotonic_now() -> Duration {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    start.elapsed()
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
    #[expect(
        clippy::disallowed_methods,
        reason = "the runtime's wall clock reads the system time, and tests inject their own clock"
    )]
    fn now_ms(&self) -> u64 {
        unix_ms(SystemTime::now())
    }
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

/// Bounded retry configuration for replay-safe transport operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportRetryPolicy {
    /// Maximum retries after the first attempt.
    pub max_retries: u32,
    /// Backoff before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff between attempts.
    pub max_backoff: Duration,
    /// Total time allowed for the logical operation.
    pub operation_deadline: Duration,
}

impl TransportRetryPolicy {
    /// Grants one retry under the policy's retry count and the operation's
    /// deadline, and counts it in `retries` when it is granted.
    ///
    /// The backoff never runs past the deadline.
    pub fn next_backoff(
        &self,
        retries: &mut u32,
        deadline: Option<&OperationDeadline<'_>>,
    ) -> TransportRetryOutcome {
        if *retries >= self.max_retries {
            return TransportRetryOutcome::RetriesExhausted;
        }
        let remaining = match deadline {
            Some(deadline) => match deadline.remaining() {
                Some(remaining) => remaining,
                None => return TransportRetryOutcome::DeadlinePassed,
            },
            None => Duration::MAX,
        };
        *retries += 1;
        TransportRetryOutcome::Wait {
            backoff: transport_retry_backoff(self, *retries).min(remaining),
        }
    }
}

/// How one retry decision under a [`TransportRetryPolicy`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportRetryOutcome {
    /// The retry is granted after `backoff`.
    Wait {
        /// Delay before the retry.
        backoff: Duration,
    },
    /// Every retry the policy allows has been used.
    RetriesExhausted,
    /// The operation deadline has passed.
    DeadlinePassed,
}

/// Elapsed-time state for one logical operation's retry loop.
pub struct OperationDeadline<'timer> {
    timer: &'timer dyn MonotonicTimer,
    started_at_ms: u64,
    deadline: Duration,
}

impl<'timer> OperationDeadline<'timer> {
    /// Starts a deadline at the timer's current monotonic reading.
    pub fn start(timer: &'timer dyn MonotonicTimer, deadline: Duration) -> Self {
        Self {
            timer,
            started_at_ms: timer.monotonic_now_ms(),
            deadline,
        }
    }

    /// Returns the remaining duration, or `None` once the deadline expires.
    pub fn remaining(&self) -> Option<Duration> {
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_at_ms);
        let deadline_ms = saturating_duration_ms(self.deadline);
        if elapsed_ms >= deadline_ms {
            return None;
        }
        Some(Duration::from_millis(deadline_ms - elapsed_ms))
    }

    /// Returns the total duration assigned to the operation.
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

/// Converts a duration to whole milliseconds, saturating at `u64::MAX`.
pub fn saturating_duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Computes capped exponential backoff for a retry number starting at one.
pub fn transport_retry_backoff(policy: &TransportRetryPolicy, retry: u32) -> Duration {
    let doublings = retry.saturating_sub(1).min(16);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_timers_created_at_different_times_read_on_one_scale() {
        let first = StdMonotonicTimer::default();
        let started_at_ms = first.monotonic_now_ms();
        let mut earlier_ms = started_at_ms;
        while earlier_ms == started_at_ms {
            earlier_ms = first.monotonic_now_ms();
        }
        let second = StdMonotonicTimer::default();
        assert!(second.monotonic_now_ms() >= earlier_ms);
    }

    #[test]
    fn the_wall_clock_counts_milliseconds_from_the_unix_epoch() {
        assert_eq!(unix_ms(UNIX_EPOCH + Duration::from_millis(1)), 1);
    }
}
