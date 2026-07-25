//! Per-operation option types for the runtime handle surface, plus the
//! request-to-options resolutions the server and embedded hosts share.
//!
//! Results are the `loonfs-api` wire shapes themselves: the handles return
//! `MaintenanceStepResponse` and `GcResponse` directly, the same way they
//! already return `CommitResponse` and `FlushWalResponse`.

use crate::{
    CommitId, DeleteDirectoryBehavior, DestinationBehavior, EffectiveLimit, GcConfig, InodeId,
};
use loonfs_api::v0::{CreateCheckpointRequest, GcRequest, MaintenanceStepRequest};
use loonfs_core::publish::WalTailPolicy;

/// Options for one maintenance step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStepOptions {
    /// Flush the visible WAL tail into metadata tables when it reaches this many
    /// segments.
    pub max_wal_tail_segments: u64,
    /// Run the mark-and-sweep garbage collector after the step's flush work.
    /// Nothing sweeps unless this is set; an absent `max_objects` inside the
    /// config resolves to the per-step default.
    pub gc: Option<GcConfig>,
}

impl Default for MaintenanceStepOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: WalTailPolicy::DEFAULT.checkpoint_at_segments,
            gc: None,
        }
    }
}

impl MaintenanceStepOptions {
    /// Resolves wire-level step overrides onto the runtime defaults.
    pub fn from_request(request: MaintenanceStepRequest) -> Self {
        let defaults = Self::default();
        Self {
            max_wal_tail_segments: request
                .max_wal_tail_segments
                .unwrap_or(defaults.max_wal_tail_segments),
            gc: request.gc.map(|request| {
                let mut config = gc_config_from_request(request);
                if config.max_objects.is_none() {
                    config.max_objects = Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS);
                }
                config
            }),
        }
    }
}

/// Resolves wire-level GC window overrides onto the conservative defaults.
///
/// [`GcRequest`] carries optional overrides; [`GcConfig`] carries the values
/// the pass actually runs with, so the two are deliberately distinct shapes.
pub fn gc_config_from_request(request: GcRequest) -> GcConfig {
    let defaults = GcConfig::default();
    GcConfig {
        grace_window_ms: request.grace_window_ms.unwrap_or(defaults.grace_window_ms),
        reap_window_ms: request.reap_window_ms.unwrap_or(defaults.reap_window_ms),
        max_objects: request.max_objects,
        cursor: request.cursor,
    }
}

/// Options for creating a durable checkpoint pin.
///
/// The name is a label recorded on the record, not a key. No `Default`:
/// a checkpoint always names its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointOptions {
    /// Label recorded on the checkpoint record.
    pub name: String,
    /// Optional lifetime; the record's expiry is computed from the runtime's
    /// clock. Absent means the pin holds until explicitly released.
    pub ttl_ms: Option<u64>,
}

impl CreateCheckpointOptions {
    /// Resolves the wire-level create request onto runtime options.
    pub fn from_request(request: CreateCheckpointRequest) -> Self {
        Self {
            name: request.name,
            ttl_ms: request.ttl_ms,
        }
    }
}

/// Options for creating a namespace; feeds core's
/// [`loonfs_core::BootstrapOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateNamespaceOptions {
    /// If true, creating an already-existing namespace is treated as success.
    pub allow_existing: bool,
}

/// Options for writing a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    /// Create-only or replace-existing behavior.
    pub behavior: DestinationBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit_id: None,
        }
    }
}

/// Options for creating a directory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateDirectoryOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
    /// Also create missing ancestor directories, like `put_file` does.
    pub parents: bool,
}

/// Options for deleting a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Directory delete behavior.
    pub behavior: DeleteDirectoryBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
    /// When set, the delete applies only while the path still resolves to
    /// this inode, so a raced rebinding fails instead of deleting the
    /// wrong inode.
    pub expected_inode_id: Option<InodeId>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            behavior: DeleteDirectoryBehavior::NonRecursive,
            commit_id: None,
            expected_inode_id: None,
        }
    }
}

/// Options for moving a path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MoveOptions {
    /// Create-only or replace-existing behavior for the destination.
    pub behavior: DestinationBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for copying a file path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyOptions {
    /// Create-only or replace-existing behavior for the destination.
    pub behavior: DestinationBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for restoring a file revision by path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RestoreRevisionOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for recovering a deleted file or subtree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UndeleteOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for reading the change feed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListChangesOptions {
    /// Page limit; `None` resolves the default pagination policy.
    pub limit: Option<EffectiveLimit>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_gc_stays_unbounded_while_step_gc_gets_the_default_budget() {
        let explicit = gc_config_from_request(GcRequest::default());
        assert_eq!(explicit.max_objects, None);
        assert_eq!(explicit.cursor, None);

        let step = MaintenanceStepOptions::from_request(MaintenanceStepRequest {
            max_wal_tail_segments: None,
            gc: Some(GcRequest::default()),
        });
        assert_eq!(
            step.gc.expect("GC opted in").max_objects,
            Some(loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS)
        );
    }

    #[test]
    fn step_gc_preserves_explicit_budget_and_cursor() {
        let step = MaintenanceStepOptions::from_request(MaintenanceStepRequest {
            max_wal_tail_segments: None,
            gc: Some(GcRequest {
                max_objects: Some(7),
                cursor: Some("opaque".to_owned()),
                ..GcRequest::default()
            }),
        });
        let gc = step.gc.expect("GC opted in");
        assert_eq!(gc.max_objects, Some(7));
        assert_eq!(gc.cursor.as_deref(), Some("opaque"));
    }
}
