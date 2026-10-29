//! The clock and timer boundary of this crate.
//!
//! `clippy.toml` disallows ambient clocks and sleeps, so these functions are
//! the only places in production code that read the wall clock, the monotonic
//! clock, or the async timer.

use std::time::{Duration, SystemTime};

/// Returns the current wall time.
#[expect(
    clippy::disallowed_methods,
    reason = "provider requests are signed with the wall time, credentials expire at a wall time, \
              and local scratch files carry a wall-time stamp; no durable state is derived from \
              the reading"
)]
pub(crate) fn wall_now() -> SystemTime {
    SystemTime::now()
}

/// Returns a timer that completes after `wait`.
#[expect(
    clippy::disallowed_methods,
    reason = "a retry backoff or a response idle timeout changes when a request is sent or \
              abandoned, never what the request writes"
)]
pub(crate) fn pause(wait: Duration) -> tokio::time::Sleep {
    tokio::time::sleep(wait)
}

/// Returns the current instant of the async runtime's monotonic clock.
#[expect(
    clippy::disallowed_methods,
    reason = "the instant measures metric durations and re-arms the response idle timeout; \
              no protocol decision reads the measurement"
)]
pub(crate) fn monotonic_instant() -> tokio::time::Instant {
    tokio::time::Instant::now()
}
