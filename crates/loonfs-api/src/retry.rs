//! Monotonic timers and bounded transport retry policy.

use std::sync::OnceLock;
use std::time::Duration;

/// Supplies monotonic milliseconds for retry deadlines and deterministic test injection.
pub trait MonotonicTimer: std::fmt::Debug + Send + Sync {
    /// Returns milliseconds since an arbitrary per-timer origin.
    fn monotonic_now_ms(&self) -> u64;
}

/// Measures monotonic elapsed time, including host sleep.
#[derive(Debug, Default)]
pub struct StdMonotonicTimer {
    origin: OnceLock<Duration>,
}

impl MonotonicTimer for StdMonotonicTimer {
    fn monotonic_now_ms(&self) -> u64 {
        let now = monotonic_now();
        let origin = self.origin.get_or_init(|| now);
        u64::try_from(now.saturating_sub(*origin).as_millis()).unwrap_or(u64::MAX)
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

/// Elapsed-time state for one logical operation's retry loop.
pub struct OperationDeadline<'timer> {
    timer: &'timer dyn MonotonicTimer,
    started_ms: u64,
    deadline: Duration,
}

impl<'timer> OperationDeadline<'timer> {
    /// Starts a deadline at the timer's current monotonic reading.
    pub fn start(timer: &'timer dyn MonotonicTimer, deadline: Duration) -> Self {
        Self {
            timer,
            started_ms: timer.monotonic_now_ms(),
            deadline,
        }
    }

    /// Returns the remaining duration, or `None` once the deadline expires.
    pub fn remaining(&self) -> Option<Duration> {
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_ms);
        let deadline_ms = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX);
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

/// Computes capped exponential backoff for a retry number starting at one.
pub fn transport_retry_backoff(policy: &TransportRetryPolicy, retry: u32) -> Duration {
    let doublings = retry.saturating_sub(1).min(16);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}
