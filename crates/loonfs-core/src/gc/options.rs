//! Options for one namespace collection.

use crate::error::{CoreError, Result};
use crate::limits::{GC_CONTENT_SHARD_ROWS, GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use loonfs_types::GcRequest;
use serde::{Deserialize, Serialize};

/// Limits for one namespace collection pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcOptions {
    pub grace_window_ms: u64,
    pub content_shard_rows: usize,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            grace_window_ms: GC_DEFAULT_GRACE_WINDOW_MS,
            content_shard_rows: GC_CONTENT_SHARD_ROWS,
        }
    }
}

impl GcOptions {
    /// Resolves a wire-level collection request. An absent grace window
    /// takes the default.
    pub fn from_request(request: GcRequest) -> Self {
        Self {
            grace_window_ms: request
                .grace_window_ms
                .unwrap_or(GC_DEFAULT_GRACE_WINDOW_MS),
            ..Self::default()
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        // The minimum grace window is derived from the publication budgets
        // and provider deadlines in `limits`. Below it, a publish still in
        // flight could have written objects that already look old enough to
        // delete, so the configuration is rejected outright.
        if self.grace_window_ms < GC_MIN_GRACE_WINDOW_MS {
            return Err(CoreError::InvalidGcOptions(format!(
                "grace_window_ms {} is below the derived safety minimum {}",
                self.grace_window_ms, GC_MIN_GRACE_WINDOW_MS
            )));
        }
        if self.content_shard_rows == 0 {
            return Err(CoreError::InvalidGcOptions(
                "content_shard_rows must be at least 1".to_owned(),
            ));
        }
        Ok(())
    }
}
