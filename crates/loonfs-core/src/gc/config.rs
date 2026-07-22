//! GC configuration and the per-run report.

use crate::error::CoreError;
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use serde::{Deserialize, Serialize};

/// Grace and reap windows for the sweep (format spec, "Garbage collection").
/// Both are wall-clock cleanup policy, never validity inputs. The defaults
/// are conservative: every object gets one hour of unconditional protection,
/// while old upload sessions and abandoned fork records wait seven days.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcConfig {
    pub grace_window_ms: u64,
    pub reap_window_ms: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_window_ms: 60 * 60 * 1000,
            reap_window_ms: 7 * 24 * 60 * 60 * 1000,
        }
    }
}

impl GcConfig {
    pub(super) fn validate(&self) -> Result<(), CoreError> {
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
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub deleted_wal_segments: u64,
    pub deleted_metadata_tables: u64,
    pub deleted_manifests: u64,
    pub deleted_checkpoint_records: u64,
    /// Fork-owned checkpoint records flipped to released because their
    /// target namespace is provably gone (terminally deleted, or its
    /// installation tree is absent past the reap window).
    pub released_fork_checkpoints: u64,
    /// Upload-session control objects deleted after the reap window.
    #[serde(default)]
    pub deleted_upload_sessions: u64,
    /// Active checkpoint records released because their basis manifest is
    /// verifiably gone (the record-write-then-crash window skipped the
    /// creator's own release).
    #[serde(default)]
    pub released_missing_basis_checkpoints: u64,
    /// Candidates dropped at delete time: still inside the grace window,
    /// missing a provider timestamp, or reachable from the fresh root set.
    pub retained_candidates: u64,
    /// True when a checkpoint record could not be read or validated, which
    /// suppresses manifest and table deletion for the whole pass (rule 5:
    /// ambiguous roots cause retention).
    pub degraded_retention: bool,
    /// The namespace lacked a complete head-and-descriptor pair, so GC
    /// deliberately skipped it without listing or deleting any objects.
    pub incomplete_namespace_ignored: bool,
}
