//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::attempts::count_retry_attempt;
use crate::PROVIDER_OPERATION_DEADLINE;
use loonfs_api::{transport_retry_backoff, OperationDeadline, TransportRetryPolicy};
use std::future::Future;
use std::time::Duration;

/// Bounded retry configuration matching the provider client's read budget.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 10,
    initial_backoff: Duration::from_millis(100),
    max_backoff: Duration::from_secs(15),
    operation_deadline: PROVIDER_OPERATION_DEADLINE,
};

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

// This retry delay is local scheduling, so it cannot affect replayed state.
#[allow(clippy::disallowed_methods)]
pub(crate) async fn transport_retry_pause(backoff: Duration) {
    tokio::time::sleep(backoff).await;
}
