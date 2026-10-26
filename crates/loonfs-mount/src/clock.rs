//! The engine's timer boundary.

use std::future::Future;
use std::time::Duration;

/// Waits for `interval_ms`, for the timer that schedules the engine's feed
/// polls.
#[expect(
    clippy::disallowed_methods,
    reason = "the engine's poll timer waits for its configured interval"
)]
pub(crate) async fn delay(interval_ms: u64) {
    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
}

/// Waits for `future` for at most `timeout`, and returns `None` when the
/// timeout passes first.
#[expect(
    clippy::disallowed_methods,
    reason = "a refresh waits for the feed for at most its caller's timeout"
)]
pub(crate) async fn within<F: Future>(timeout: Duration, future: F) -> Option<F::Output> {
    tokio::time::timeout(timeout, future).await.ok()
}
