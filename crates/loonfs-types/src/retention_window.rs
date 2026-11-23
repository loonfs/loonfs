//! The retention window of a namespace, which is at least the grace window.

use crate::limits::GC_GRACE_WINDOW_MS;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use thiserror::Error;

/// A namespace's retention window, in milliseconds, which is at least the
/// grace window ([`GC_GRACE_WINDOW_MS`]).
///
/// An advance never moves the floor past a commit younger than the grace
/// window, so a namespace with a shorter window would keep more history than
/// its window says (format spec, "Advancing the retention floor"). Every
/// request body and configuration setting that sets a window decodes to this
/// type, and every function that records a requested window takes it. A
/// manifest's `retention_window_ms` stays a plain number, so a manifest that
/// already records a shorter window still loads, and an advance under that
/// window stops at the grace window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NonZeroU64", into = "NonZeroU64")]
pub struct RetentionWindow(NonZeroU64);

/// A retention window shorter than the grace window.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("a retention window of {window_ms} ms is shorter than the grace window of {GC_GRACE_WINDOW_MS} ms")]
pub struct RetentionWindowValidationError {
    window_ms: u64,
}

impl RetentionWindowValidationError {
    /// Returns the refused window, in milliseconds.
    pub fn window_ms(&self) -> u64 {
        self.window_ms
    }
}

impl RetentionWindow {
    /// Returns the window of `window_ms` milliseconds, or an error when it's
    /// shorter than the grace window.
    pub fn new(window_ms: u64) -> Result<Self, RetentionWindowValidationError> {
        match NonZeroU64::new(window_ms) {
            Some(window) if window_ms >= GC_GRACE_WINDOW_MS => Ok(Self(window)),
            _ => Err(RetentionWindowValidationError { window_ms }),
        }
    }

    /// Returns the window in milliseconds.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<NonZeroU64> for RetentionWindow {
    type Error = RetentionWindowValidationError;

    fn try_from(window_ms: NonZeroU64) -> Result<Self, Self::Error> {
        Self::new(window_ms.get())
    }
}

impl From<RetentionWindow> for NonZeroU64 {
    fn from(window: RetentionWindow) -> Self {
        window.0
    }
}

#[cfg(test)]
mod tests {
    use super::RetentionWindow;
    use crate::limits::GC_GRACE_WINDOW_MS;

    /// Checks that a window shorter than the grace window, zero included,
    /// is refused when it's built and when it's decoded, and that the grace
    /// window itself and longer windows are kept.
    #[test]
    fn a_retention_window_shorter_than_the_grace_window_is_refused() {
        for window_ms in [0, 1, GC_GRACE_WINDOW_MS - 1] {
            let error = RetentionWindow::new(window_ms).expect_err("a short window");
            assert_eq!(error.window_ms(), window_ms);
            assert_eq!(
                error.to_string(),
                format!(
                    "a retention window of {window_ms} ms is shorter than the grace window of \
                     {GC_GRACE_WINDOW_MS} ms"
                )
            );
            serde_json::from_value::<RetentionWindow>(serde_json::json!(window_ms))
                .expect_err("a short window doesn't decode");
        }
        for window_ms in [GC_GRACE_WINDOW_MS, 30 * GC_GRACE_WINDOW_MS] {
            let window = RetentionWindow::new(window_ms).expect("a long enough window");
            assert_eq!(window.get(), window_ms);
            assert_eq!(
                serde_json::to_value(window).expect("encode"),
                serde_json::json!(window_ms)
            );
            assert_eq!(
                serde_json::from_value::<RetentionWindow>(serde_json::json!(window_ms))
                    .expect("decode"),
                window
            );
        }
    }
}
