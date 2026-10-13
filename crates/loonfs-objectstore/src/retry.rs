//! Shared bounded-backoff machinery for replay-safe store operations.

use crate::attempts::count_retry_attempt;
use crate::timing::MonotonicTimer;
use crate::{PROVIDER_MAX_RETRY_BACKOFF, PROVIDER_OPERATION_DEADLINE};
use loonfs_types::{OperationDeadline, TransportRetryOutcome, TransportRetryPolicy};
use std::future::Future;
use std::sync::{Arc, Mutex};
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

/// The retry warnings of one store, which a provider outage would otherwise
/// log once per retry.
///
/// Logs the first retry warning, and then at most one per
/// [`RETRY_WARNING_INTERVAL_MS`], with the count suppressed since the last
/// and the time since the last.
#[derive(Debug)]
pub(crate) struct RetryWarnings {
    timer: Arc<dyn MonotonicTimer>,
    state: Mutex<RetryWarningState>,
}

#[derive(Debug, Default)]
struct RetryWarningState {
    logged_at_ms: Option<u64>,
    suppressed: u64,
}

/// What to do with one retry warning.
#[derive(Debug, PartialEq, Eq)]
enum RetryWarning {
    /// Log it at `warn!`, with the count of warnings suppressed since the
    /// previous logged warning and the time since it, when there was one.
    Log {
        suppressed: u64,
        since_ms: Option<u64>,
    },
    /// Log it at `debug!` and count it.
    Suppress,
}

impl RetryWarnings {
    /// Creates the limiter of a store that reads time from `timer`.
    pub(crate) fn new(timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            timer,
            state: Mutex::new(RetryWarningState::default()),
        }
    }

    fn next(&self) -> RetryWarning {
        let now_ms = self.timer.monotonic_now_ms();
        self.state
            .lock()
            .expect("the retry warning lock should not be poisoned")
            .next(now_ms)
    }
}

impl RetryWarningState {
    fn next(&mut self, now_ms: u64) -> RetryWarning {
        let since_ms = self
            .logged_at_ms
            .map(|logged_at_ms| now_ms.saturating_sub(logged_at_ms));
        if since_ms.is_some_and(|since_ms| since_ms < RETRY_WARNING_INTERVAL_MS) {
            self.suppressed = self.suppressed.saturating_add(1);
            return RetryWarning::Suppress;
        }
        self.logged_at_ms = Some(now_ms);
        RetryWarning::Log {
            suppressed: std::mem::take(&mut self.suppressed),
            since_ms,
        }
    }
}

/// One store's transport retry policy and the limiter for its retry warnings.
#[derive(Clone, Copy)]
pub(crate) struct StoreRetry<'store> {
    pub(crate) policy: &'store TransportRetryPolicy,
    pub(crate) warnings: &'store RetryWarnings,
}

/// Grants one retry under the shared count and deadline budgets.
///
/// Every replay-safe write retry passes through here, so attempt metrics and
/// granted/exhausted warnings cannot drift between callers.
pub(crate) fn next_retry_backoff(
    retry: StoreRetry<'_>,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    retries: &mut u32,
    deadline: Option<&OperationDeadline<'_>>,
) -> Option<Duration> {
    let backoff = match retry.policy.next_backoff(retries, deadline) {
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
    match retry.warnings.next() {
        RetryWarning::Log {
            suppressed,
            since_ms,
        } => tracing::warn!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = retry.policy.max_retries,
            backoff_ms,
            suppressed,
            since_ms,
            "transient object store write failure, backing off before retry",
        ),
        RetryWarning::Suppress => tracing::debug!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = retry.policy.max_retries,
            backoff_ms,
            "transient object store write failure, backing off before retry",
        ),
    }
    Some(backoff)
}

pub(crate) async fn with_transport_retry<T, E, F, Fut>(
    retry: StoreRetry<'_>,
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
        let Some(backoff) =
            next_retry_backoff(retry, key, operation, payload_bytes, &mut retries, deadline)
        else {
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
    use super::{RetryWarning, RetryWarningState, RETRY_WARNING_INTERVAL_MS};

    #[test]
    fn retry_warnings_log_the_first_and_then_one_count_per_interval() {
        let mut warnings = RetryWarningState::default();
        assert_eq!(
            warnings.next(1_000),
            RetryWarning::Log {
                suppressed: 0,
                since_ms: None
            }
        );
        for now_ms in [1_001, 2_000, RETRY_WARNING_INTERVAL_MS + 999] {
            assert_eq!(warnings.next(now_ms), RetryWarning::Suppress);
        }
        assert_eq!(
            warnings.next(RETRY_WARNING_INTERVAL_MS + 1_000),
            RetryWarning::Log {
                suppressed: 3,
                since_ms: Some(RETRY_WARNING_INTERVAL_MS)
            }
        );
        assert_eq!(
            warnings.next(RETRY_WARNING_INTERVAL_MS + 1_001),
            RetryWarning::Suppress
        );
        assert_eq!(
            warnings.next(10 * RETRY_WARNING_INTERVAL_MS),
            RetryWarning::Log {
                suppressed: 1,
                since_ms: Some(9 * RETRY_WARNING_INTERVAL_MS - 1_000)
            }
        );
    }
}
