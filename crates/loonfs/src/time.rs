//! The runtime's only wall-clock and monotonic-clock touchpoints.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn current_time_ms() -> u64 {
    wall_clock_now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn wall_clock_now() -> SystemTime {
    // Runtime cache TTLs and request timestamps are explicit wall-clock boundaries.
    SystemTime::now()
}
