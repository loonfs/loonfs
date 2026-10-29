//! The one retry policy of the store layer: the provider client's retries,
//! LoonFS's own retry loop, and the warnings that loop logs.
//!
//! The retrying provider client retries the requests it sends. LoonFS
//! retries the requests it signs itself, verified immutable writes, and a
//! delete whose attempt timed out, through [`TransportRetry::run`]. A
//! conditional publication is one attempt.

use crate::attempts::count_retry_attempt;
use crate::{
    ObjectStoreError, ObjectStoreErrorKind, PROVIDER_MAX_RETRY_BACKOFF, PROVIDER_OPERATION_DEADLINE,
};
use loonfs_types::{
    duration_ms, MonotonicTimer, OperationDeadline, TransportRetryOutcome, TransportRetryPolicy,
};
use object_store::{BackoffConfig, RetryConfig};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The retry count, backoff and operation deadline of every LoonFS retry
/// and of the provider client's retries.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 10,
    initial_backoff_ms: 100,
    max_backoff_ms: duration_ms(PROVIDER_MAX_RETRY_BACKOFF),
    operation_deadline_ms: duration_ms(PROVIDER_OPERATION_DEADLINE),
};

/// Shortest time between two logged retry warnings. The retries in between
/// log at `debug!`, and the next logged warning reports how many there were.
const RETRY_WARNING_INTERVAL_MS: u64 = 60_000;

/// Returns the retry configuration every provider builder applies: the
/// client's retries of each request it sends follow [`DEFAULT`], and share
/// its operation deadline as one budget.
pub(crate) fn provider_retry_config() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(DEFAULT.initial_backoff_ms),
            max_backoff: Duration::from_millis(DEFAULT.max_backoff_ms),
            ..Default::default()
        },
        max_retries: loonfs_types::usize_from_u64(u64::from(DEFAULT.max_retries)),
        retry_timeout: Duration::from_millis(DEFAULT.operation_deadline_ms),
    }
}

/// Returns the retry configuration of the provider client that sends each
/// request once.
pub(crate) fn one_attempt_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        ..provider_retry_config()
    }
}

/// The retry clock of one store, and the warnings its retries log, which a
/// provider outage would otherwise log once per retry.
///
/// The store and its signer share one value, so a bucket keeps one limiter
/// and one retry clock. The limiter logs the first retry warning, and then at
/// most one per [`RETRY_WARNING_INTERVAL_MS`], with the count suppressed since
/// the last and the time since the last. The same timer measures each retry
/// loop's operation deadline.
#[derive(Debug)]
pub(crate) struct TransportRetry {
    timer: Arc<dyn MonotonicTimer>,
    warnings: Mutex<RetryWarningState>,
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

impl TransportRetry {
    /// Creates the retry state of a store that reads time from `timer`.
    pub(crate) fn new(timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            timer,
            warnings: Mutex::new(RetryWarningState::default()),
        }
    }

    /// Sends a replay-safe request again while it fails with a retryable
    /// transport error, under [`DEFAULT`]'s retry count and operation deadline.
    ///
    /// An error of any other kind (a refused request, a failed precondition,
    /// or a permission failure) returns at once. When the count or the
    /// deadline runs out, the last error returns as a non-retryable transport
    /// failure, so an outer loop (a verified write around its parts) doesn't
    /// spend a second budget on the same request. A provider client error is
    /// retryable only from the client that sends each request once (see
    /// [`crate::provider_error::map_provider_error`]), so a request
    /// that the retrying client already retried isn't retried here again.
    pub(crate) async fn run<T, F, Fut>(
        &self,
        key: &str,
        operation: &'static str,
        payload_bytes: u64,
        mut attempt: F,
    ) -> Result<T, ObjectStoreError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ObjectStoreError>>,
    {
        let mut retry_loop = RetryLoop {
            key,
            operation,
            payload_bytes,
            retries: 0,
            deadline: OperationDeadline::start(self.timer.as_ref(), DEFAULT.operation_deadline_ms),
        };
        loop {
            let error = match attempt().await {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            if error.kind() != ObjectStoreErrorKind::RetryableTransport {
                return Err(error);
            }
            let Some(backoff) = retry_loop.next_backoff(self) else {
                return Err(error.exhausted());
            };
            crate::clock::pause(backoff).await;
        }
    }

    fn next_warning(&self) -> RetryWarning {
        let now_ms = self.timer.monotonic_now_ms();
        self.warnings
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

/// One retry loop: the request it sends again, and the retries and the time
/// it has used.
struct RetryLoop<'a> {
    key: &'a str,
    operation: &'static str,
    payload_bytes: u64,
    retries: u32,
    deadline: OperationDeadline<'a>,
}

impl RetryLoop<'_> {
    /// Grants one retry under the shared count and deadline budgets.
    fn next_backoff(&mut self, retry: &TransportRetry) -> Option<Duration> {
        let (key, operation, payload_bytes) = (self.key, self.operation, self.payload_bytes);
        let backoff_ms = match DEFAULT.next_backoff(&mut self.retries, Some(&self.deadline)) {
            TransportRetryOutcome::Wait { backoff_ms } => backoff_ms,
            TransportRetryOutcome::RetriesExhausted => {
                tracing::warn!(
                    object_key = key,
                    operation,
                    retry = self.retries,
                    payload_bytes,
                    "object store retry budget exhausted; not retrying",
                );
                return None;
            }
            TransportRetryOutcome::DeadlinePassed => {
                tracing::warn!(
                    object_key = key,
                    operation,
                    retry = self.retries,
                    payload_bytes,
                    "object store operation deadline exhausted; not retrying",
                );
                return None;
            }
        };
        count_retry_attempt();
        match retry.next_warning() {
            RetryWarning::Log {
                suppressed,
                since_ms,
            } => tracing::warn!(
                object_key = key,
                operation,
                retry = self.retries,
                max_retries = DEFAULT.max_retries,
                backoff_ms,
                suppressed,
                since_ms,
                "transient object store failure, backing off before retry",
            ),
            RetryWarning::Suppress => tracing::debug!(
                object_key = key,
                operation,
                retry = self.retries,
                max_retries = DEFAULT.max_retries,
                backoff_ms,
                "transient object store failure, backing off before retry",
            ),
        }
        Some(Duration::from_millis(backoff_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryWarning, RetryWarningState, DEFAULT, RETRY_WARNING_INTERVAL_MS};
    use std::time::Duration;

    #[test]
    fn transport_retry_backoff_doubles_and_caps() {
        let policy = DEFAULT;
        assert_eq!(
            Duration::from_millis(policy.backoff_ms(1)),
            Duration::from_millis(100)
        );
        assert_eq!(
            Duration::from_millis(policy.backoff_ms(2)),
            Duration::from_millis(200)
        );
        assert_eq!(
            Duration::from_millis(policy.backoff_ms(8)),
            Duration::from_millis(12_800)
        );
        assert_eq!(
            Duration::from_millis(policy.backoff_ms(9)),
            Duration::from_secs(15)
        );
        assert_eq!(
            Duration::from_millis(policy.backoff_ms(10)),
            Duration::from_secs(15)
        );
    }

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
