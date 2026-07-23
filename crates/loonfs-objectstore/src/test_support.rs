//! Test-only support shared by object-store unit suites.

use crate::timing::MonotonicTimer;
use std::sync::atomic::{AtomicU64, Ordering};

/// Deterministic timer that advances a fixed step per reading.
#[derive(Debug)]
pub(crate) struct SteppingTimer {
    now_ms: AtomicU64,
    step_ms: u64,
}

impl SteppingTimer {
    pub(crate) fn new(step_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(0),
            step_ms,
        }
    }
}

impl MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms.fetch_add(self.step_ms, Ordering::SeqCst)
    }
}
