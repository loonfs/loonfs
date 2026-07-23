//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::timing::MonotonicTimer;
use crate::PROVIDER_OP_DEADLINE;
use std::time::Duration;

/// Bounded retry configuration matching the provider client's read budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportRetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) op_deadline: Duration,
}

impl TransportRetryPolicy {
    pub(crate) const DEFAULT: Self = Self {
        max_retries: 10,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(15),
        op_deadline: PROVIDER_OP_DEADLINE,
    };
}

pub(crate) fn transport_retry_backoff(policy: &TransportRetryPolicy, retry: u32) -> Duration {
    let doublings = retry.saturating_sub(1).min(16);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}

/// Elapsed-time state for one logical operation's retry loop.
pub(crate) struct OperationDeadline<'timer> {
    timer: &'timer dyn MonotonicTimer,
    started_ms: u64,
    deadline: Duration,
}

impl<'timer> OperationDeadline<'timer> {
    pub(crate) fn start(timer: &'timer dyn MonotonicTimer, deadline: Duration) -> Self {
        Self {
            timer,
            started_ms: timer.monotonic_now_ms(),
            deadline,
        }
    }

    pub(crate) fn remaining(&self) -> Option<Duration> {
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
}

#[allow(clippy::disallowed_methods)]
pub(crate) async fn transport_retry_pause(backoff: Duration) {
    // Replay-safe operations wait at this isolated timer boundary between attempts.
    tokio::time::sleep(backoff).await;
}
