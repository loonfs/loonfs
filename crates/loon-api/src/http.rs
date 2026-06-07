use crate::{v0::RenameMode, ChangeSeq, CommitId, ContentRef, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// HTTP error body used by LoonFS APIs.
pub struct ApiError {
    /// Stable machine-readable reason.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Request to create a namespace.
pub struct CreateNamespaceRequest {
    /// Durable namespace id to create.
    pub namespace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Request to fork a namespace.
pub struct ForkNamespaceRequest {
    /// Durable namespace id for the fork target.
    pub new_namespace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Short namespace listing entry.
pub struct NamespaceSummary {
    /// Durable namespace id.
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Response for namespace listing.
pub struct ListNamespacesResponse {
    /// Complete namespaces visible to the store.
    pub namespaces: Vec<NamespaceSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Result of a namespace-visible mutation.
pub struct MutationResult {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence number where the mutation became visible.
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Put behavior for path-oriented file writes.
pub enum FilesystemPutBehavior {
    /// Fail if the path already exists.
    CreateOnly,
    /// Replace the current file if it exists.
    ReplaceExisting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
/// One path-oriented filesystem operation.
pub enum FilesystemOperation {
    /// Create one directory.
    CreateDir { path: String },
    /// Create or replace one file with an already-durable content ref.
    PutFile {
        path: String,
        content_ref: ContentRef,
        behavior: FilesystemPutBehavior,
    },
    /// Delete one path.
    DeletePath { path: String },
    /// Move one path to another path.
    MovePath {
        from_path: String,
        to_path: String,
        #[serde(default = "crate::v0::default_rename_mode")]
        mode: RenameMode,
    },
    /// Copy one file path to another path.
    CopyPath { from_path: String, to_path: String },
    /// Restore an older revision as the current revision for a path.
    RestoreRevision {
        path: String,
        source_revision_no: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Request wrapper for one path-oriented operation.
pub struct FilesystemOperationRequest {
    /// Caller-supplied idempotency key for this operation.
    pub commit_id: CommitId,
    /// Operation to apply.
    pub operation: FilesystemOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Response for one path-oriented operation.
pub struct FilesystemOperationResponse {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence where the operation became visible.
    pub committed_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// One immutable file revision.
pub struct FileRevision {
    /// File inode that owns this revision.
    pub inode_id: InodeId,
    /// Revision number within the file inode.
    pub revision_no: RevisionNo,
    /// Namespace sequence that created this revision.
    pub committed_seq: ChangeSeq,
    /// Content stored for this revision.
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Response for listing file revisions.
pub struct ListFileRevisionsResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// File inode whose revisions were returned.
    pub inode_id: InodeId,
    /// Namespace head sequence used for the read.
    pub head_seq: ChangeSeq,
    /// Retained revisions in order.
    pub revisions: Vec<FileRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Request to restore a file revision by inode.
pub struct RestoreFileRevisionRequest {
    /// Caller-supplied idempotency key.
    pub commit_id: CommitId,
    /// Current revision the caller expects to replace.
    pub base_revision_no: RevisionNo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Result of creating or reusing a checkpoint.
pub struct CreateCheckpointResponse {
    /// Namespace that was checkpointed.
    pub namespace_id: NamespaceId,
    /// Durable checkpoint id.
    pub checkpoint_id: String,
    /// Sequence covered by the checkpoint.
    pub checkpoint_seq: ChangeSeq,
    /// Head's current checkpoint hint after the operation.
    pub checkpoint_hint_seq: Option<ChangeSeq>,
    /// Whether the head now points at this checkpoint.
    pub checkpoint_hint_points_at_checkpoint: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Result of advancing the retention floor.
pub struct AdvanceRetentionResponse {
    /// Namespace whose retention floor changed.
    pub namespace_id: NamespaceId,
    /// New minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

impl From<MutationResult> for FilesystemOperationResponse {
    fn from(value: MutationResult) -> Self {
        Self {
            namespace_id: value.namespace_id,
            committed_seq: value.committed_seq,
        }
    }
}

impl From<FilesystemOperationResponse> for MutationResult {
    fn from(value: FilesystemOperationResponse) -> Self {
        Self {
            namespace_id: value.namespace_id,
            committed_seq: value.committed_seq,
        }
    }
}
