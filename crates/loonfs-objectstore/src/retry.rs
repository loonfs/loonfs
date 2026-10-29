//! The one retry loop of the store layer, and the warnings it logs.
//!
//! The retrying provider client retries the requests it sends. LoonFS
//! retries the requests it signs itself, verified immutable writes, and a
//! delete whose attempt timed out, through [`with_transport_retry`]. A
//! conditional publication is one attempt.

use crate::attempts::count_retry_attempt;
use crate::timing::MonotonicTimer;
use crate::{
    ObjectStoreError, ObjectStoreErrorKind, PROVIDER_MAX_RETRY_BACKOFF, PROVIDER_OPERATION_DEADLINE,
};
use loonfs_types::{OperationDeadline, TransportRetryOutcome, TransportRetryPolicy};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The retry count, backoff and operation deadline of every LoonFS retry,
/// which match the provider client's own retry configuration.
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
/// log once per retry, and the clock that times its retries.
///
/// Logs the first retry warning, and then at most one per
/// [`RETRY_WARNING_INTERVAL_MS`], with the count suppressed since the last
/// and the time since the last. The same timer measures each retry loop's
/// operation deadline.
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

/// Grants one retry under the shared count and deadline budgets.
fn next_retry_backoff(
    warnings: &RetryWarnings,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    retries: &mut u32,
    deadline: &OperationDeadline<'_>,
) -> Option<Duration> {
    let backoff = match DEFAULT.next_backoff(retries, Some(deadline)) {
        TransportRetryOutcome::Wait { backoff } => backoff,
        TransportRetryOutcome::RetriesExhausted => {
            tracing::warn!(
                object_key = key,
                operation,
                retry = *retries,
                payload_bytes,
                "object store retry budget exhausted; not retrying",
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
    match warnings.next() {
        RetryWarning::Log {
            suppressed,
            since_ms,
        } => tracing::warn!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = DEFAULT.max_retries,
            backoff_ms,
            suppressed,
            since_ms,
            "transient object store failure, backing off before retry",
        ),
        RetryWarning::Suppress => tracing::debug!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = DEFAULT.max_retries,
            backoff_ms,
            "transient object store failure, backing off before retry",
        ),
    }
    Some(backoff)
}

/// Sends a replay-safe request again while it fails with a retryable
/// transport error, under [`DEFAULT`]'s retry count and operation deadline.
///
/// An error of any other kind (a refused request, a failed precondition, or
/// a permission failure) returns at once. When the count or the deadline runs
/// out, the last error returns as a non-retryable transport failure, so an
/// outer loop (a verified write around its parts) doesn't spend a second
/// budget on the same request. A provider client error is retryable only
/// from the client that sends each request once (see
/// [`crate::provider_object_store::map_provider_error`]), so a request that
/// the retrying client already retried isn't retried here again.
pub(crate) async fn with_transport_retry<T, F, Fut>(
    warnings: &RetryWarnings,
    key: &str,
    operation: &'static str,
    payload_bytes: u64,
    mut attempt: F,
) -> Result<T, ObjectStoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ObjectStoreError>>,
{
    let deadline = OperationDeadline::start(warnings.timer.as_ref(), DEFAULT.operation_deadline);
    let mut retries = 0;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if error.kind() != ObjectStoreErrorKind::RetryableTransport {
            return Err(error);
        }
        let Some(backoff) = next_retry_backoff(
            warnings,
            key,
            operation,
            payload_bytes,
            &mut retries,
            &deadline,
        ) else {
            return Err(error.exhausted());
        };
        crate::clock::pause(backoff).await;
    }
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
