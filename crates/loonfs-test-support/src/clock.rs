//! Time advanced explicitly by tests, and the bound a test waits under.

use loonfs_types::{MonotonicTimer, WallClock};
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

/// A clock that a test moves by hand, as a monotonic timer and as a wall clock.
#[derive(Debug)]
pub struct ManualClock {
    now_ms: AtomicU64,
    step_ms: u64,
}

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
            step_ms: 0,
        }
    }

    /// Returns a clock at zero that moves `step_ms` forward after each monotonic reading.
    ///
    /// A test can then spend a publication budget in a known number of timer readings instead of
    /// in wall time. Wall-clock readings don't move the clock.
    pub fn stepping(step_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(0),
            step_ms,
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    pub fn advance_ms(&self, elapsed_ms: u64) {
        self.now_ms.fetch_add(elapsed_ms, Ordering::SeqCst);
    }

    /// Moves the clock to `now_ms`, which may be earlier than its current reading.
    pub fn set_ms(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }
}

impl MonotonicTimer for ManualClock {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms.fetch_add(self.step_ms, Ordering::SeqCst)
    }
}

impl WallClock for ManualClock {
    fn now_ms(&self) -> u64 {
        ManualClock::now_ms(self)
    }
}
