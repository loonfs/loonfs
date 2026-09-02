//! Monotonic timers and bounded transport retry policy.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Supplies monotonic milliseconds for retry deadlines and deterministic test injection.
pub trait MonotonicTimer: std::fmt::Debug + Send + Sync {
    /// Returns milliseconds since an arbitrary per-timer origin.
    fn monotonic_now_ms(&self) -> u64;
}

/// Process-clock implementation backed by [`std::time::Instant`].
#[derive(Debug, Default)]
pub struct StdMonotonicTimer {
    origin: OnceLock<Instant>,
}

impl MonotonicTimer for StdMonotonicTimer {
    fn monotonic_now_ms(&self) -> u64 {
        // This monotonic boundary controls local retry timing, so it cannot affect durable state.
        #[allow(clippy::disallowed_methods)]
        let now = Instant::now();
        let origin = self.origin.get_or_init(|| now);
        u64::try_from(now.saturating_duration_since(*origin).as_millis()).unwrap_or(u64::MAX)
    }
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
