//! Time advanced explicitly by tests, and the bound a test waits under.

use loonfs_types::MonotonicTimer;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Bounds how long a test waits for `future`: the returned future resolves
/// to `Err` once `duration` passes, as [`tokio::time::timeout`] does.
#[expect(
    clippy::disallowed_methods,
    reason = "the hang guard every test waits under, so a hung test fails instead of stalling the suite"
)]
pub fn bounded<F: IntoFuture>(
    duration: Duration,
    future: F,
) -> tokio::time::Timeout<F::IntoFuture> {
    tokio::time::timeout(duration, future)
}

#[derive(Debug)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    pub fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    pub fn advance_ms(&self, elapsed_ms: u64) {
        self.0.fetch_add(elapsed_ms, Ordering::SeqCst);
    }
}

impl MonotonicTimer for ManualClock {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms()
    }
}
