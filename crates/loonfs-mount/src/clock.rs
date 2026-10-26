//! The engine's timer boundary.

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
