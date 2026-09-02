//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::attempts::count_retry_attempt;
use crate::timing::MonotonicTimer;
use crate::PROVIDER_OPERATION_DEADLINE;
use std::future::Future;
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
) -> Option<Duration> {
    if *retries >= policy.max_retries {
        tracing::warn!(
            object_key = key,
            operation,
            retry = *retries,
            payload_bytes,
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
        "transient object store write failure, backing off before retry",
    );
    Some(backoff)
}

pub(crate) async fn with_transport_retry<T, F, Fut>(
    policy: &TransportRetryPolicy,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    deadline: Option<&OperationDeadline<'_>>,
    mut attempt: F,
) -> object_store::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = object_store::Result<T>>,
{
    let mut retries = 0;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !provider_transport_retryable(&error) {
            return Err(error);
        }
        let Some(backoff) = next_retry_backoff(
            policy,
            key,
            operation,
            payload_bytes,
            &mut retries,
            deadline,
        ) else {
            return Err(error);
        };
        transport_retry_pause(backoff).await;
    }
}

pub(crate) fn provider_transport_retryable(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Generic { .. })
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
