//! Per-operation option and result types for the [`Fs`] surface.

use crate::DEFAULT_MAX_WAL_TAIL_SEGMENTS;
use crate::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, ManifestId, MoveBehavior, NamespaceId,
    PutBehavior,
};

/// Current maintenance-related namespace status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStatus {
    /// Namespace being inspected.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Current manifest pointer recorded by the head.
    pub current_manifest_id: Option<ManifestId>,
    /// Latest checkpoint recorded by the head.
    /// Number of visible WAL segments after the manifest materialization.
    pub wal_tail_segments: u64,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// Options for one maintenance tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTickOptions {
    /// Publish a checkpoint when the visible WAL tail reaches this many segments.
    pub max_wal_tail_segments: u64,
    /// Run the mark-and-sweep garbage collector after the tick's checkpoint
    /// and retention work. Nothing sweeps unless this is set.
    pub gc: Option<loonfs_core::GcConfig>,
}

impl Default for MaintenanceTickOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: DEFAULT_MAX_WAL_TAIL_SEGMENTS,
            gc: None,
        }
    }
}

/// Outcome of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceTickOutcome {
    /// No checkpoint was needed.
    NotNeeded,
    /// A checkpoint was published.
    CheckpointPublished {
        /// Sequence covered by the published checkpoint.
        checkpoint_seq: ChangeSeq,
    },
    /// Another checkpoint already covered the attempted sequence.
    CheckpointSuperseded {
        /// Sequence this tick attempted to checkpoint.
        attempted_seq: ChangeSeq,
        /// Manifest pointer the head currently records.
        current_manifest_id: ManifestId,
    },
    /// A concurrent head update won the race.
    CheckpointPublishRaceLost {
        /// Head sequence observed before the checkpoint attempt.
        observed_head_seq: ChangeSeq,
    },
}

/// Result of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickResult {
    /// Namespace the tick ran against.
    pub namespace_id: NamespaceId,
    /// Namespace status observed before the tick acted.
    pub status_before: NamespaceStatus,
    /// What the tick did.
    pub outcome: MaintenanceTickOutcome,
    /// Garbage-collection report when the tick opted into sweeping.
    pub gc: Option<loonfs_core::GcReport>,
}

/// Options for creating a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateNamespaceOptions {
    /// Treat an existing namespace as success.
    pub allow_existing: bool,
}

/// Options for writing a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    /// Create-only or replace-existing behavior.
    pub behavior: PutBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: PutBehavior::NoReplace,
            commit_id: None,
        }
    }
}

/// Options for creating a directory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateDirOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for deleting a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Directory delete behavior.
    pub behavior: DeleteDirectoryBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            behavior: DeleteDirectoryBehavior::NonRecursive,
            commit_id: None,
        }
    }
}

/// Options for moving a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveOptions {
    /// Destination handling behavior.
    pub behavior: MoveBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl Default for MoveOptions {
    fn default() -> Self {
        Self {
            behavior: MoveBehavior::NoReplace,
            commit_id: None,
        }
    }
}

/// Options for copying a file path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for restoring a file revision by path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RestoreRevisionOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}
