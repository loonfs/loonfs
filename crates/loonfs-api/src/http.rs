use crate::{
    v0::{MoveBehavior, ValidatedContentToken},
    ChangeSeq, CommitId, ContentRef, ErrorCode, InodeId, ManifestId, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// HTTP error body used by LoonFS APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateNamespaceRequest {
    /// Durable namespace id to create.
    pub namespace_id: String,
}

/// Request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ForkNamespaceRequest {
    /// Durable namespace id for the fork target.
    pub new_namespace_id: String,
}

/// Short namespace identifier returned by namespace create/fork operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceSummary {
    /// Durable namespace id.
    pub namespace_id: NamespaceId,
}

/// Status summary for one namespace.
///
/// This is the point-lookup answer to "does this namespace exist, and where
/// is its head?" — cheaper than listing all namespaces when only one matters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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

/// Result of deleting a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteNamespaceResponse {
    /// Namespace whose history ended.
    pub namespace_id: NamespaceId,
    /// The head's last committed sequence; the delete linearized
    /// immediately after it, so this is where history ended.
    pub head_seq: ChangeSeq,
}

/// Result of a namespace-visible mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MutationResult {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence number where the mutation became visible.
    pub committed_seq: ChangeSeq,
}

/// Put behavior for path-oriented file writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum PutBehavior {
    /// Fail if the path already exists.
    #[default]
    NoReplace,
    /// Replace the current file if it exists.
    Replace,
}

/// Directory delete behavior for path-oriented deletes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DeleteDirectoryBehavior {
    /// Fail if the target is a non-empty directory.
    #[default]
    NonRecursive,
    /// Delete a directory subtree.
    Recursive,
}

/// One path-oriented filesystem operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FilesystemOperation {
    /// Create one directory.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpCreateDir"))]
    CreateDir { path: String },
    /// Create or replace one file with an already-durable content ref.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpPutFile"))]
    PutFile {
        path: String,
        content_ref: ContentRef,
        #[serde(default)]
        behavior: PutBehavior,
    },
    /// Delete one path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpDeletePath"))]
    DeletePath {
        path: String,
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
    },
    /// Move one path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpMovePath"))]
    MovePath {
        from_path: String,
        to_path: String,
        #[serde(default)]
        behavior: MoveBehavior,
    },
    /// Copy one file path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpCopyPath"))]
    CopyPath { from_path: String, to_path: String },
    /// Restore an older revision as the current revision for a path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpRestoreRevision"))]
    RestoreRevision {
        path: String,
        source_revision_no: RevisionNo,
    },
}

/// Request wrapper for one path-oriented operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FilesystemOperationRequest {
    /// Caller-supplied idempotency key for this operation.
    pub commit_id: CommitId,
    /// Proofs for any new external content refs introduced by this operation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ValidatedContentToken>,
    /// Operation to apply.
    pub operation: FilesystemOperation,
}

/// Response for one path-oriented operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FilesystemOperationResponse {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Sequence where the operation became visible.
    pub committed_seq: ChangeSeq,
}

/// One immutable file revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListFileRevisionsResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// File inode whose revisions were returned.
    pub inode_id: InodeId,
    /// Namespace head sequence used for the read.
    pub head_seq: ChangeSeq,
    /// Retained revisions in order.
    pub revisions: Vec<FileRevision>,
    /// Opaque cursor for the next page, if more revisions are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request to restore a file revision by inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RestoreFileRevisionRequest {
    /// Caller-supplied idempotency key.
    pub commit_id: CommitId,
    /// Current revision the caller expects to replace.
    pub base_revision_no: RevisionNo,
}

/// Result of creating or reusing a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behavior_enums_use_snake_case_wire_values() {
        assert_eq!(PutBehavior::default(), PutBehavior::NoReplace);
        assert_eq!(
            DeleteDirectoryBehavior::default(),
            DeleteDirectoryBehavior::NonRecursive
        );
        assert_eq!(
            serde_json::to_value(PutBehavior::NoReplace).expect("put behavior json"),
            serde_json::json!("no_replace")
        );
        assert_eq!(
            serde_json::to_value(PutBehavior::Replace).expect("put behavior json"),
            serde_json::json!("replace")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::NonRecursive)
                .expect("delete behavior json"),
            serde_json::json!("non_recursive")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::Recursive).expect("delete behavior json"),
            serde_json::json!("recursive")
        );
        assert_eq!(
            serde_json::to_value(MoveBehavior::Exchange).expect("move behavior json"),
            serde_json::json!("exchange")
        );
    }

    #[test]
    fn filesystem_delete_and_move_operations_use_behavior_field() {
        let delete = FilesystemOperation::DeletePath {
            path: "/docs".to_owned(),
            behavior: DeleteDirectoryBehavior::Recursive,
        };
        assert_eq!(
            serde_json::to_value(&delete).expect("delete op json"),
            serde_json::json!({
                "op": "delete_path",
                "path": "/docs",
                "behavior": "recursive"
            })
        );

        let move_path = FilesystemOperation::MovePath {
            from_path: "/docs/a.txt".to_owned(),
            to_path: "/docs/b.txt".to_owned(),
            behavior: MoveBehavior::NoReplace,
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "op": "move_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "no_replace"
            })
        );
    }

    #[test]
    fn filesystem_operations_default_omitted_behavior_fields() {
        let put: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "op": "put_file",
            "path": "/docs/a.txt",
            "content_ref": {
                "kind": "whole_file_v0",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size_bytes": 1
            }
        }))
        .expect("put op defaults behavior");
        assert!(matches!(
            put,
            FilesystemOperation::PutFile {
                behavior: PutBehavior::NoReplace,
                ..
            }
        ));

        let delete: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "op": "delete_path",
            "path": "/docs"
        }))
        .expect("delete op defaults behavior");
        assert_eq!(
            delete,
            FilesystemOperation::DeletePath {
                path: "/docs".to_owned(),
                behavior: DeleteDirectoryBehavior::NonRecursive,
            }
        );

        let move_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "op": "move_path",
            "from_path": "/docs/a.txt",
            "to_path": "/docs/b.txt"
        }))
        .expect("move op defaults behavior");
        assert_eq!(
            move_path,
            FilesystemOperation::MovePath {
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                behavior: MoveBehavior::NoReplace,
            }
        );
    }
}
