//! Counting the provider attempts one measured object-store call made.
//!
//! [`InstrumentedObjectStore`](crate::metrics::InstrumentedObjectStore)
//! wraps above the bounded retry loops in
//! [`provider_object_store`](crate::provider_object_store), so a sample's
//! elapsed time already covers every retry while the count itself is
//! invisible from up there — a slow call and a call that was retried nine
//! times read the same. This task-local tally is the one channel between
//! the two: the wrapper opens a tally around the call it is timing, the
//! retry gate below counts each attempt it grants, and the wrapper reads
//! the total back when it builds the sample.
//!
//! A retry gate outside a measured call finds no tally and counts nothing,
//! so an uninstrumented store pays one failed task-local lookup per granted
//! retry and nothing on any other path.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

tokio::task_local! {
    /// Attempts made so far by the measured call running in this task.
    static CALL_ATTEMPTS: Arc<AtomicU32>;
}

/// Runs `call` under a fresh tally and reports the attempts it made.
///
/// The count starts at the one attempt every call makes, so a sample's
/// attempt count is never zero.
pub(crate) async fn counting_attempts<F: Future>(call: F) -> (F::Output, u32) {
    let attempts = Arc::new(AtomicU32::new(1));
    let output = CALL_ATTEMPTS.scope(Arc::clone(&attempts), call).await;
    (output, attempts.load(Ordering::Relaxed))
}

/// Counts one granted retry against the measured call it belongs to.
pub(crate) fn count_retry_attempt() {
    let _ = CALL_ATTEMPTS.try_with(|attempts| attempts.fetch_add(1, Ordering::Relaxed));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_call_that_never_retries_reports_one_attempt() {
        let (output, attempts) = counting_attempts(async { 7 }).await;
        assert_eq!(output, 7);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn granted_retries_count_against_the_call_that_made_them() {
        let (_, attempts) = counting_attempts(async {
            count_retry_attempt();
            count_retry_attempt();
        })
        .await;
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn a_retry_outside_a_measured_call_counts_nothing() {
        count_retry_attempt();
        let (_, attempts) = counting_attempts(async {}).await;
        assert_eq!(attempts, 1);
    }
}
