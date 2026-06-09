//! Per-operation option and result types for the [`Fs`] surface.

use crate::DEFAULT_MAX_WAL_TAIL_SEGMENTS;
use crate::{ChangeSeq, CommitId, ManifestId, NamespaceId, PutFileBehavior};

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
    pub latest_checkpoint_id: Option<String>,
    /// Number of visible WAL segments after the manifest basis.
    pub wal_tail_segments: u64,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// Options for one maintenance tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTickOptions {
    /// Publish a checkpoint when the visible WAL tail reaches this many segments.
    pub max_wal_tail_segments: u64,
}

impl Default for MaintenanceTickOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: DEFAULT_MAX_WAL_TAIL_SEGMENTS,
        }
    }
}

/// Outcome of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceTickOutcome {
    /// No checkpoint was needed.
    NotNeeded,
    /// A checkpoint was published.
    CheckpointPublished { checkpoint_seq: ChangeSeq },
    /// Another checkpoint already covered the attempted sequence.
    CheckpointSuperseded {
        attempted_seq: ChangeSeq,
        current_manifest_id: ManifestId,
    },
    /// A concurrent head update won the race.
    CheckpointPublishRaceLost { observed_head_seq: ChangeSeq },
}

/// Result of one maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickResult {
    pub namespace_id: NamespaceId,
    pub status_before: NamespaceStatus,
    pub outcome: MaintenanceTickOutcome,
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
    pub behavior: PutFileBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: PutFileBehavior::CreateOnly,
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteOptions {
    /// Whether directory deletes may remove a subtree.
    pub recursive: bool,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

/// Options for moving a path.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MoveOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
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
