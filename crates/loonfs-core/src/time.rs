//! The one wall-clock boundary durable timestamps are stamped at.

use crate::error::{CoreError, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// Reads the wall clock as unix milliseconds.
///
/// Every timestamp that reaches durable state — commit, checkpoint, upload
/// session, maintenance schedule — is stamped here and then carried as a
/// value, so replay below this boundary stays deterministic.
#[allow(clippy::disallowed_methods)]
pub fn current_time_ms() -> Result<u64> {
    unix_ms(SystemTime::now())
}

/// Converts a wall-clock instant to unix milliseconds, failing clearly on a
/// pre-epoch clock instead of stamping timestamp zero into durable state.
fn unix_ms(now: SystemTime) -> Result<u64> {
    now.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|_| CoreError::Internal("system time is before unix epoch".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn pre_epoch_clock_is_an_error_not_timestamp_zero() {
        let error = unix_ms(UNIX_EPOCH - Duration::from_secs(1)).expect_err("pre-epoch must fail");
        assert!(
            error.to_string().contains("before unix epoch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn epoch_and_later_instants_convert_to_milliseconds() {
        assert_eq!(unix_ms(UNIX_EPOCH).expect("epoch converts"), 0);
        let later = UNIX_EPOCH + Duration::from_millis(1500);
        assert_eq!(unix_ms(later).expect("later converts"), 1500);
    }
}
