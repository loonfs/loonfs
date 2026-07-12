//! Local monotonic elapsed-time boundary for self-enforced deadlines and
//! budgets.
//!
//! This module is the one place storage and writer code observes time.
//! Deadlines bound retry loops and budgets bound a writer's own
//! write-to-publish window so the GC grace window is deterministically safe;
//! they gate only the observer's next action (stop retrying,
//! abandon-and-rebuild). No validator compares timestamps, nothing on the
//! wire carries these readings, and commit validity never consults time.
//! `loonfs-core` re-exports these types; the trait is an injection seam the
//! external simulation harness also consumes.

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
