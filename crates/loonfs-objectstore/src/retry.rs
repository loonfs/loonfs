//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::attempts::count_retry_attempt;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{PROVIDER_MAX_RETRY_BACKOFF, PROVIDER_OPERATION_DEADLINE};
use loonfs_types::{OperationDeadline, TransportRetryOutcome, TransportRetryPolicy};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

/// Bounded retry configuration matching the provider client's read budget.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 10,
    initial_backoff: Duration::from_millis(100),
    max_backoff: PROVIDER_MAX_RETRY_BACKOFF,
    operation_deadline: PROVIDER_OPERATION_DEADLINE,
};

/// Shortest time between two logged retry warnings. The retries in between
/// log at `debug!`, and the next logged warning reports how many there were.
const RETRY_WARNING_INTERVAL_MS: u64 = 60_000;

/// The process's retry warnings, which a provider outage would otherwise log
/// once per retry.
static RETRY_WARNINGS: RetryWarnings = RetryWarnings::new();

/// Logs the first retry warning, and then at most one per
/// [`RETRY_WARNING_INTERVAL_MS`], with the count suppressed since the last.
#[derive(Debug)]
struct RetryWarnings {
    state: Mutex<RetryWarningState>,
}

#[derive(Debug)]
struct RetryWarningState {
    logged_at_ms: Option<u64>,
    suppressed: u64,
}

/// What to do with one retry warning.
#[derive(Debug, PartialEq, Eq)]
enum RetryWarning {
    /// Log it at `warn!`, with the count of warnings suppressed before it.
    Log { suppressed: u64 },
    /// Log it at `debug!` and count it.
    Suppress,
}

impl RetryWarnings {
    const fn new() -> Self {
        Self {
            state: Mutex::new(RetryWarningState {
                logged_at_ms: None,
                suppressed: 0,
            }),
        }
    }

    fn next(&self, now_ms: u64) -> RetryWarning {
        let mut state = self
            .state
            .lock()
            .expect("the retry warning lock should not be poisoned");
        let due = state.logged_at_ms.is_none_or(|logged_at_ms| {
            now_ms.saturating_sub(logged_at_ms) >= RETRY_WARNING_INTERVAL_MS
        });
        if !due {
            state.suppressed = state.suppressed.saturating_add(1);
            return RetryWarning::Suppress;
        }
        state.logged_at_ms = Some(now_ms);
        RetryWarning::Log {
            suppressed: std::mem::take(&mut state.suppressed),
        }
    }
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
    let backoff = match policy.next_backoff(retries, deadline) {
        TransportRetryOutcome::Wait { backoff } => backoff,
        TransportRetryOutcome::RetriesExhausted => {
            tracing::warn!(
                object_key = key,
                operation,
                retry = *retries,
                payload_bytes,
                "object store write retry budget exhausted; not retrying",
            );
            return None;
        }
        TransportRetryOutcome::DeadlinePassed => {
            tracing::warn!(
                object_key = key,
                operation,
                retry = *retries,
                payload_bytes,
                "object store operation deadline exhausted; not retrying",
            );
            return None;
        }
    };
    count_retry_attempt();
    let backoff_ms = loonfs_types::saturating_duration_ms(backoff);
    match RETRY_WARNINGS.next(StdMonotonicTimer::default().monotonic_now_ms()) {
        RetryWarning::Log { suppressed } => tracing::warn!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = policy.max_retries,
            backoff_ms,
            suppressed,
            "transient object store write failure, backing off before retry",
        ),
        RetryWarning::Suppress => tracing::debug!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = policy.max_retries,
            backoff_ms,
            "transient object store write failure, backing off before retry",
        ),
    }
    Some(backoff)
}

pub(crate) async fn with_transport_retry<T, E, F, Fut>(
    policy: &TransportRetryPolicy,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    deadline: Option<&OperationDeadline<'_>>,
    retryable: impl Fn(&E) -> bool,
    mut attempt: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut retries = 0;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if !retryable(&error) {
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

#[cfg(test)]
mod tests {
    use super::{RetryWarning, RetryWarnings, RETRY_WARNING_INTERVAL_MS};

    #[test]
    fn retry_warnings_log_the_first_and_then_one_count_per_interval() {
        let warnings = RetryWarnings::new();
        assert_eq!(warnings.next(1_000), RetryWarning::Log { suppressed: 0 });
        for now_ms in [1_001, 2_000, RETRY_WARNING_INTERVAL_MS + 999] {
            assert_eq!(warnings.next(now_ms), RetryWarning::Suppress);
        }
        assert_eq!(
            warnings.next(RETRY_WARNING_INTERVAL_MS + 1_000),
            RetryWarning::Log { suppressed: 3 }
        );
        assert_eq!(
            warnings.next(RETRY_WARNING_INTERVAL_MS + 1_001),
            RetryWarning::Suppress
        );
        assert_eq!(
            warnings.next(10 * RETRY_WARNING_INTERVAL_MS),
            RetryWarning::Log { suppressed: 1 }
        );
    }
}
