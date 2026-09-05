//! GC configuration.

use crate::error::{CoreError, Result};
use crate::limits::{GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use serde::{Deserialize, Serialize};

/// The grace window for the sweep (format spec, "Garbage collection"). It is
/// wall-clock cleanup policy, never a validity input, and the default is
/// conservative: every object gets one hour of unconditional protection.
/// Abandoned fork records are not under it — a fork attempt carries its own
/// lease, and letting that pass is the whole proof
/// (`gc/fork_checkpoints.rs`) — and neither are upload sessions or the
/// content they leave behind: a session carries its own lease, and the
/// window a completed session's content is protected for is derived in
/// `limits`, not configured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcConfig {
    pub grace_window_ms: u64,
    /// Maximum durable work steps in this invocation. One source object,
    /// merge page, revision block, or sweep candidate is one step. Progress
    /// is saved between calls, including with a budget of one.
    /// `None` runs the active collection to completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque namespace-bound run identity returned by an earlier invocation.
    /// Progress and deletion evidence live on the server. Omitting the token
    /// joins any active run; an old token never starts a new collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_window_ms: GC_DEFAULT_GRACE_WINDOW_MS,
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
        if self.max_objects == Some(0) {
            return Err(CoreError::InvalidGcConfig(
                "max_objects must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}
