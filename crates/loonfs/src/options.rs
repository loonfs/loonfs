//! Per-operation option and result types for the runtime handle surface,
//! plus the wire conversions the server and embedded hosts share so both
//! transports report identical maintenance shapes.

use crate::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, EffectiveLimit, GcConfig,
    GcReport, InodeId, ManifestId, NamespaceId, NamespaceStatusResponse,
};
use loonfs_api::v0::{
    CreateCheckpointRequest, GcRequest, GcResponse,
    MaintenanceTickOutcome as WireMaintenanceTickOutcome, MaintenanceTickRequest,
    MaintenanceTickResponse,
};
use loonfs_core::publish::WalTailPolicy;

/// Options for one maintenance tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTickOptions {
    /// Flush the visible WAL tail into metadata tables when it reaches this many
    /// segments.
    pub max_wal_tail_segments: u64,
    /// Run the mark-and-sweep garbage collector after the tick's
    /// flush work. Nothing sweeps unless this is set.
    pub gc: Option<crate::GcConfig>,
}

impl Default for MaintenanceTickOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: WalTailPolicy::DEFAULT.checkpoint_at_segments,
            gc: None,
        }
    }
}

impl MaintenanceTickOptions {
    /// Resolves wire-level tick overrides onto the runtime defaults.
    pub fn from_request(request: MaintenanceTickRequest) -> Self {
        let defaults = Self::default();
        Self {
            max_wal_tail_segments: request
                .max_wal_tail_segments
                .unwrap_or(defaults.max_wal_tail_segments),
            gc: request.gc.map(gc_config_from_request),
        }
    }
}

/// Resolves wire-level GC window overrides onto the conservative defaults.
pub fn gc_config_from_request(request: GcRequest) -> GcConfig {
    let defaults = GcConfig::default();
    GcConfig {
        grace_window_ms: request.grace_window_ms.unwrap_or(defaults.grace_window_ms),
        reap_window_ms: request.reap_window_ms.unwrap_or(defaults.reap_window_ms),
    }
}

/// Wire response for one GC report against a namespace.
pub fn gc_response_from_report(namespace_id: NamespaceId, report: GcReport) -> GcResponse {
    GcResponse {
        namespace_id,
        deleted_wal_segments: report.deleted_wal_segments,
        deleted_metadata_tables: report.deleted_metadata_tables,
        deleted_manifests: report.deleted_manifests,
        deleted_checkpoint_records: report.deleted_checkpoint_records,
        released_fork_checkpoints: report.released_fork_checkpoints,
        deleted_upload_sessions: report.deleted_upload_sessions,
        released_missing_basis_checkpoints: report.released_missing_basis_checkpoints,
        retained_candidates: report.retained_candidates,
        degraded_retention: report.degraded_retention,
        incomplete_namespace_ignored: report.incomplete_namespace_ignored,
    }
}

/// Outcome of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceTickOutcome {
    /// The WAL tail was below the threshold; the root was left alone.
    NotNeeded,
    /// The tick flushed the WAL tail and advanced the metadata root.
    WalFlushed {
        /// Sequence covered by the published manifest.
        manifest_head_seq: ChangeSeq,
    },
    /// The root already covered the attempted sequence — another publisher
    /// got there first.
    WalFlushSuperseded {
        /// Sequence this tick attempted to flush through.
        attempted_seq: ChangeSeq,
        /// Manifest the root currently references.
        current_manifest_id: ManifestId,
    },
    /// A concurrent head update won the race.
    WalFlushRaceLost {
        /// Head sequence observed before the advance attempt.
        observed_head_seq: ChangeSeq,
    },
}

/// Result of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickResult {
    /// Namespace the tick ran against.
    pub namespace_id: NamespaceId,
    /// Namespace status observed before the tick acted.
    pub status_before: NamespaceStatusResponse,
    /// What the tick did.
    pub outcome: MaintenanceTickOutcome,
    /// Garbage-collection report when the tick opted into sweeping.
    pub gc: Option<crate::GcReport>,
}

impl MaintenanceTickResult {
    /// Wire response for this tick.
    pub fn into_response(self) -> MaintenanceTickResponse {
        let namespace_id = self.namespace_id;
        MaintenanceTickResponse {
            gc: self
                .gc
                .map(|report| gc_response_from_report(namespace_id.clone(), report)),
            namespace_id,
            status_before: self.status_before,
            outcome: match self.outcome {
                MaintenanceTickOutcome::NotNeeded => WireMaintenanceTickOutcome::NotNeeded,
                MaintenanceTickOutcome::WalFlushed { manifest_head_seq } => {
                    WireMaintenanceTickOutcome::WalFlushed { manifest_head_seq }
                }
                MaintenanceTickOutcome::WalFlushSuperseded {
                    attempted_seq,
                    current_manifest_id,
                } => WireMaintenanceTickOutcome::WalFlushSuperseded {
                    attempted_seq,
                    current_manifest_id,
                },
                MaintenanceTickOutcome::WalFlushRaceLost { observed_head_seq } => {
                    WireMaintenanceTickOutcome::WalFlushRaceLost { observed_head_seq }
                }
            },
        }
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
