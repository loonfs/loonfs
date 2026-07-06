//! Local monotonic timer used only for self-enforced publish budgets.
//!
//! Commit validity never depends on wall-clock or monotonic time.

use std::sync::OnceLock;
use std::time::Instant;

pub trait MonotonicTimer: std::fmt::Debug + Send + Sync {
    /// Milliseconds since an arbitrary per-timer origin. Never goes
    /// backward; only differences are meaningful.
    fn monotonic_now_ms(&self) -> u64;
}

/// Process-clock implementation backed by [`std::time::Instant`].
#[derive(Debug, Default)]
pub struct StdMonotonicTimer {
    origin: OnceLock<Instant>,
}

impl MonotonicTimer for StdMonotonicTimer {
    fn monotonic_now_ms(&self) -> u64 {
        // The explicit timing boundary the workspace lint points to: this is
        // a self-imposed deadline source, not an input to commit validity.
        #[allow(clippy::disallowed_methods)]
        let now = Instant::now();
        let origin = self.origin.get_or_init(|| now);
        u64::try_from(now.saturating_duration_since(*origin).as_millis()).unwrap_or(u64::MAX)
    }
}
