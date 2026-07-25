//! GC configuration.

use crate::error::{CoreError, Result};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use serde::{Deserialize, Serialize};

/// Grace and reap windows for the sweep (format spec, "Garbage collection").
/// Both are wall-clock cleanup policy, never validity inputs. The defaults
/// are conservative: every object gets one hour of unconditional protection,
/// while old upload sessions and abandoned fork records wait seven days.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcConfig {
    pub grace_window_ms: u64,
    pub reap_window_ms: u64,
    /// Maximum sweep candidates examined by this invocation. `None` keeps
    /// the run-to-completion behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque enumeration cursor returned by an earlier invocation.
    ///
    /// The cursor is valid only for the same namespace. Resuming always
    /// rebuilds the live roots and safety floors; a stale cursor can only
    /// re-examine work or defer keys that moved before it until the next
    /// full pass, never authorize deletion of a newly live object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_window_ms: 60 * 60 * 1000,
            reap_window_ms: 7 * 24 * 60 * 60 * 1000,
            max_objects: None,
            cursor: None,
        }
    }
}

impl GcConfig {
    pub(super) fn validate(&self) -> Result<()> {
        // The minimum grace window is derived from the publication budgets
        // and provider deadlines in `limits`. Below it, a publish still in
        // flight could have written objects that already look old enough to
        // delete, so the configuration is rejected outright.
        if self.grace_window_ms < GC_MIN_GRACE_WINDOW_MS {
            return Err(CoreError::InvalidGcConfig(format!(
                "grace_window_ms {} is below the derived safety minimum {}",
                self.grace_window_ms, GC_MIN_GRACE_WINDOW_MS
            )));
        }
        if self.reap_window_ms < self.grace_window_ms {
            return Err(CoreError::InvalidGcConfig(
                "reap_window_ms must be at least the grace window".to_owned(),
            ));
        }
        if self.max_objects == Some(0) {
            return Err(CoreError::InvalidGcConfig(
                "max_objects must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}
