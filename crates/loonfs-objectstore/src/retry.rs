//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::attempts::count_retry_attempt;
use crate::timing::MonotonicTimer;
use crate::PROVIDER_OPERATION_DEADLINE;
use std::fmt::Display;
use std::time::Duration;

/// Bounded retry configuration matching the provider client's read budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportRetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) operation_deadline: Duration,
}

impl TransportRetryPolicy {
    pub(crate) const DEFAULT: Self = Self {
        max_retries: 10,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(15),
        operation_deadline: PROVIDER_OPERATION_DEADLINE,
    };
}

pub(crate) fn transport_retry_backoff(policy: &TransportRetryPolicy, retry: u32) -> Duration {
    let doublings = retry.saturating_sub(1).min(16);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}

/// Grants one retry under the shared count and deadline budgets.
///
/// Every replay-safe write retry passes through here, so attempt metrics and
/// granted/exhausted warnings cannot drift between callers.
pub(crate) fn next_retry_backoff(
    policy: &TransportRetryPolicy,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    retries: &mut u32,
    deadline: Option<&OperationDeadline<'_>>,
    error: &dyn Display,
) -> Option<Duration> {
    if *retries >= policy.max_retries {
        tracing::warn!(
            object_key = key,
            operation,
            retry = *retries,
            payload_bytes,
            error = %error,
            "object store write retry budget exhausted; not retrying",
        );
        return None;
    }
    let mut remaining = Duration::MAX;
    if let Some(deadline) = deadline {
        let Some(deadline_remaining) = deadline.remaining() else {
            tracing::warn!(
                object_key = key,
                operation,
                retry = *retries,
                payload_bytes,
                error = %error,
                "object store operation deadline exhausted; not retrying",
            );
            return None;
        };
        remaining = deadline_remaining;
    }
    *retries += 1;
    count_retry_attempt();
    let backoff = transport_retry_backoff(policy, *retries).min(remaining);
    tracing::warn!(
        object_key = key,
        operation,
        retry = *retries,
        max_retries = policy.max_retries,
        backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
        error = %error,
        "transient object store write failure, backing off before retry",
    );
    Some(backoff)
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
    // Replay-safe operations wait at this isolated timer boundary so backoff
    // pacing never feeds protocol state and replay stays deterministic.
    tokio::time::sleep(backoff).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attempts::counting_attempts;

    #[tokio::test]
    async fn every_granted_retry_counts_as_an_attempt() {
        let policy = TransportRetryPolicy {
            max_retries: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            operation_deadline: Duration::from_secs(1),
        };
        let error = "injected transport failure".to_owned();
        let (_, attempts) = counting_attempts(async {
            let mut retries = 0;
            assert_eq!(
                next_retry_backoff(
                    &policy,
                    "namespaces/demo/wal/segments/seg_test.wal.zst",
                    "put_immutable_verified",
                    7,
                    &mut retries,
                    None,
                    &error,
                ),
                Some(Duration::from_millis(1))
            );
        })
        .await;

        assert_eq!(attempts, 2);
    }
}
