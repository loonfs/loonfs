use crate::{
    v0::RenameMode, ChangeSeq, CommitId, ContentRef, ErrorCode, InodeId, ManifestId, NamespaceId,
    RevisionNo,
};
use serde::{Deserialize, Serialize};

/// HTTP error body used by LoonFS APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    /// Stable machine-readable reason from the [`ErrorCode`] registry.
    ///
    /// Carried as a string so clients keep working when a newer server
    /// introduces a code they do not know; use [`ApiError::error_code`] for
    /// typed access.
    pub code: String,
    /// For `not_supported` errors, the capability-document feature key the
    /// client should reconcile against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    /// Human-readable error message.
    pub message: String,
}

impl ApiError {
    /// Returns the registered code, or `None` for codes this build does not
    /// know.
    pub fn error_code(&self) -> Option<ErrorCode> {
        ErrorCode::parse(&self.code)
    }
}

/// Request to create a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Durable namespace id to create.
    pub namespace_id: String,
}

/// Request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkNamespaceRequest {
    /// Durable namespace id for the fork target.
    pub new_namespace_id: String,
}

/// Short namespace listing entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSummary {
    /// Durable namespace id.
    pub namespace_id: NamespaceId,
}

/// Response for namespace listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    /// Complete namespaces visible to the store.
    pub namespaces: Vec<NamespaceSummary>,
}

/// Status summary for one namespace.
///
/// This is the point-lookup answer to "does this namespace exist, and where
/// is its head?" — cheaper than listing all namespaces when only one matters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceStatusResponse {
    /// Namespace being inspected.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Current manifest pointer recorded by the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_manifest_id: Option<ManifestId>,
    /// Latest checkpoint recorded by the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_id: Option<String>,
    /// Number of visible WAL segments after the manifest basis.
    pub wal_tail_segments: u64,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// Result of a namespace-visible mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationResult {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence number where the mutation became visible.
    pub committed_seq: ChangeSeq,
}

/// Put behavior for path-oriented file writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemPutBehavior {
    /// Fail if the path already exists.
    CreateOnly,
    /// Replace the current file if it exists.
    ReplaceExisting,
}

/// One path-oriented filesystem operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
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

/// Request wrapper for one path-oriented operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemOperationRequest {
    /// Caller-supplied idempotency key for this operation.
    pub commit_id: CommitId,
    /// Operation to apply.
    pub operation: FilesystemOperation,
}

/// Response for one path-oriented operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemOperationResponse {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence where the operation became visible.
    pub committed_seq: ChangeSeq,
}

/// One immutable file revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Response for listing file revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Request to restore a file revision by inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreFileRevisionRequest {
    /// Caller-supplied idempotency key.
    pub commit_id: CommitId,
    /// Current revision the caller expects to replace.
    pub base_revision_no: RevisionNo,
}

/// Result of creating or reusing a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCheckpointResponse {
    /// Namespace that was checkpointed.
    pub namespace_id: NamespaceId,
    /// Durable checkpoint id.
    pub checkpoint_id: String,
    /// Sequence covered by the checkpoint.
    pub checkpoint_seq: ChangeSeq,
    /// Manifest pinned by the checkpoint.
    pub manifest_id: ManifestId,
    /// Head's current manifest pointer after the operation.
    pub current_manifest_id: Option<ManifestId>,
    /// Latest checkpoint id recorded on the head after the operation.
    pub latest_checkpoint_id: Option<String>,
}

/// Result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
