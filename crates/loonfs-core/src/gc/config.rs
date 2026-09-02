//! GC configuration.

use super::cursor::{initial_cursor, GcCursor};
use crate::error::{CoreError, Result};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use loonfs_api::NamespaceId;
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
    /// Maximum objects this invocation may read or decide, marking
    /// included. `None` keeps the run-to-completion behavior. What one unit
    /// buys is spelled out on `gc::budget::PassBudget`; the short version
    /// is that everything pays out of one purse, and the pass spends it in
    /// the order it needs things.
    ///
    /// Two budgets are therefore worth naming. One below the namespace's
    /// roots — the head, the floor, the checkpoint records, the live
    /// manifests, and the retained WAL chain — buys nothing at all: the
    /// pass reports `budget_exhausted`, deletes nothing, and hands back the
    /// position it came in with. One above the roots but below the content
    /// reference scan on top of them sweeps normally and defers content
    /// reclamation, pass after pass, until a budget with room for the scan
    /// runs.
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

/// Validated, namespace-bound policy for one collection pass.
///
/// The decoded cursor belongs here because it controls this pass's walk. The
/// original token is retained separately only because a pass that makes no
/// progress must return it byte for byte; decoding and re-encoding is not the
/// external contract.
#[derive(Debug)]
pub(super) struct GcPolicy {
    pub(super) grace_window_ms: u64,
    pub(super) max_objects: Option<u64>,
    pub(super) resume: GcCursor,
    pub(super) unchanged_cursor: Option<String>,
}

impl GcPolicy {
    pub(super) fn settle(config: &GcConfig, namespace_id: &NamespaceId) -> Result<Self> {
        config.validate()?;
        let resume = match config.cursor.as_deref() {
            Some(token) => GcCursor::decode(token, namespace_id)?,
            None => initial_cursor(namespace_id),
        };
        Ok(Self {
            grace_window_ms: config.grace_window_ms,
            max_objects: config.max_objects,
            resume,
            unchanged_cursor: config.cursor.clone(),
        })
    }
}
