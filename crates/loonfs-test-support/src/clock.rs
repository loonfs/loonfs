//! Time advanced explicitly by tests.

use loonfs_api::MonotonicTimer;
use std::sync::atomic::{AtomicU64, Ordering};

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
