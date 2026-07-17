//! Request/response shapes for the v0 HTTP API's operation endpoints:
//! namespace lifecycle (create/fork/status/delete), path-oriented filesystem
//! operations, file revisions, maintenance (checkpoint/retention), and the
//! shared [`ApiError`] body. Explicit commits and the change feed live in
//! [`super::commits`]; read-result shapes live in [`super::reads`].

use super::ValidatedContentToken;
use crate::{
    ChangeSeq, CheckpointId, CommitId, ContentRef, InodeId, ManifestId, NamespaceId, RevisionNo,
    WriterEpoch,
};
use serde::{Deserialize, Serialize};

/// HTTP error body used by LoonFS APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ApiError {
    /// Stable machine-readable reason from the [`ErrorCode`](crate::ErrorCode)
    /// registry.
    ///
    /// Carried as a string so clients keep working when a newer server
    /// introduces a code they do not know; use
    /// [`ErrorCode::parse`](crate::ErrorCode::parse) for typed access.
    pub code: String,
    /// For `not_supported` errors, the capability-document feature key the
    /// client should reconcile against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    /// Human-readable error message.
    pub message: String,
    /// Correlation id the server assigned to the failed request; the same
    /// value is sent as the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Structured context for the code, present when the failure carries
    /// machine-usable identity (API spec, "Standard error contract"). Boxed
    /// so the rare detailed error does not widen every error-carrying result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<ErrorDetails>>,
}

/// Structured, machine-readable context accompanying an [`ApiError`].
///
/// Every field is optional: a code populates the fields that apply to it
/// (API spec, "Standard error contract"), and clients must tolerate absent
/// fields exactly as they tolerate unknown codes. Retry decisions still key
/// off the code; these fields carry the identity a caller needs to act —
/// which commit to resubmit, which epoch displaced it, which revision the
/// precondition saw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDetails {
    /// Idempotency key of the mutation the error concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<CommitId>,
    /// Epoch the failing writer session held when it was displaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fenced_epoch: Option<WriterEpoch>,
    /// Epoch that currently owns the namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer_epoch: Option<WriterEpoch>,
    /// Writer id recorded by the current epoch's acquirer, when the head
    /// recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer: Option<String>,
    /// Inode the failed precondition or operation targeted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode_id: Option<InodeId>,
    /// Revision the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<RevisionNo>,
    /// Revision that is actually current; absent when the inode has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_revision: Option<RevisionNo>,
    /// Change-feed cursor the request asked to resume after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_seq: Option<ChangeSeq>,
    /// Oldest sequence still promised for incremental replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_floor_seq: Option<ChangeSeq>,
    /// Deletion generation an undelete asked to recover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_deletion_seq: Option<ChangeSeq>,
    /// Deletion generation actually active for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_deletion_seq: Option<ChangeSeq>,
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
    /// Number of visible WAL segments after the current manifest.
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

/// Move behavior for path-oriented moves, mirroring [`PutBehavior`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MoveBehavior {
    /// Fail if the destination path already exists.
    #[default]
    NoReplace,
    /// Replace the current file at the destination if one exists.
    Replace,
}

/// Copy behavior for path-oriented copies, mirroring [`PutBehavior`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum CopyBehavior {
    /// Fail if the destination path already exists.
    #[default]
    NoReplace,
    /// Replace the current file at the destination if one exists.
    Replace,
}

/// One path-oriented filesystem operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilesystemOperation {
    /// Create one directory.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpCreateDirectory"))]
    CreateDirectory {
        path: String,
        /// Also create missing ancestor directories (the same auto-create
        /// `put_file` performs). The final component must still be new.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        parents: bool,
    },
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
        /// When set, the delete applies only if the path still resolves to
        /// this inode; a raced rebinding fails the request instead of
        /// deleting (and reporting a recovery handle for) the wrong inode.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_inode_id: Option<InodeId>,
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
    CopyPath {
        from_path: String,
        to_path: String,
        #[serde(default)]
        behavior: CopyBehavior,
    },
    /// Restore an older revision as the current revision for a path.
    /// Recover a deleted file or subtree: revoke the deletion of
    /// `inode_id` recorded at `deleted_at_seq` (both reported by the
    /// delete and by the change feed) and re-bind it at `path`. Answers
    /// `not_deleted` when that generation is not the live one, so a stale
    /// request never cancels a later delete.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpUndelete"))]
    Undelete {
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        path: String,
    },
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
    /// Wall-clock stamp of the commit that created this revision, in Unix
    /// milliseconds. Observational: `committed_seq` is the order.
    pub committed_at_ms: u64,
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

/// Request to create a durable checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateCheckpointRequest {
    /// Label recorded on the checkpoint record. A label, not a key: several
    /// records may carry the same name over different bases.
    pub name: String,
    /// Optional lifetime; the server computes the record's expiry from its
    /// own clock. Absent means the pin holds until explicitly released.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

/// Result of creating or reusing a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateCheckpointResponse {
    /// Namespace that was checkpointed.
    pub namespace_id: NamespaceId,
    /// Durable checkpoint id.
    pub checkpoint_id: CheckpointId,
    /// Sequence covered by the checkpoint.
    pub checkpoint_seq: ChangeSeq,
    /// Manifest pinned by the checkpoint.
    pub manifest_id: ManifestId,
    /// Manifest `metadata/root.json` references after the operation.
    pub current_manifest_id: Option<ManifestId>,
    /// Expiry recorded on the record, when the request carried a `ttl_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

/// Result of releasing a checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleaseCheckpointResponse {
    /// Namespace the checkpoint belonged to.
    pub namespace_id: NamespaceId,
    /// Checkpoint the release targeted.
    pub checkpoint_id: CheckpointId,
    /// True when this call flipped an active record to released; false when
    /// the record was already released or no longer exists. Release is
    /// idempotent — the end state is the same either way.
    pub was_active: bool,
}

/// How one WAL flush satisfied its goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlushWalOutcome {
    /// The root already covered the head; nothing was published.
    AlreadyCurrent,
    /// This call published a new manifest and advanced the root to it.
    Published,
    /// This call published a manifest, but a newer root already covered
    /// the attempted sequence.
    Superseded,
}

/// Result of one WAL flush: how the metadata root covers the head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FlushWalResponse {
    /// Namespace whose WAL tail was flushed.
    pub namespace_id: NamespaceId,
    /// Head sequence the flush attempted to cover.
    pub target_head_seq: ChangeSeq,
    /// Manifest `metadata/root.json` references after the operation.
    pub manifest_id: ManifestId,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// How the root came to cover the head.
    pub outcome: FlushWalOutcome,
}

/// Optional overrides for one garbage-collection pass. Absent fields use
/// the server's conservative defaults.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcRequest {
    /// Objects younger than this are never deleted, reachable or not. The
    /// window has a derived safety floor (publication budgets plus provider
    /// deadlines); a smaller value is rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_window_ms: Option<u64>,
    /// Abandoned bootstrap trees older than this may be reaped. Must be at
    /// least the grace window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reap_window_ms: Option<u64>,
}

/// Result of one mark-and-sweep garbage-collection pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// Namespace the pass ran against.
    pub namespace_id: NamespaceId,
    /// Unreferenced WAL segments deleted.
    pub deleted_wal_segments: u64,
    /// Unreferenced metadata tables deleted.
    pub deleted_metadata_tables: u64,
    /// Unreferenced derived-index segments deleted.
    #[serde(default)]
    pub deleted_index_segments: u64,
    /// Unreferenced manifests deleted.
    pub deleted_manifests: u64,
    /// Released or expired checkpoint records deleted.
    pub deleted_checkpoint_records: u64,
    /// Fork-owned checkpoint records released because their target namespace
    /// is provably gone.
    pub released_fork_checkpoints: u64,
    /// Objects removed while reaping an abandoned bootstrap tree.
    pub reaped_abandoned_objects: u64,
    /// Upload-session control objects deleted after the reap window.
    #[serde(default)]
    pub deleted_upload_sessions: u64,
    /// Candidates retained at delete time (grace window, missing
    /// timestamps, or reachable from the fresh root set).
    pub retained_candidates: u64,
    /// True when ambiguous roots suppressed manifest/table deletion.
    pub degraded_retention: bool,
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

/// Options for one explicit maintenance tick. Absent fields use the
/// server's defaults; garbage collection runs only when `gc` is present.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceTickRequest {
    /// Flush the visible WAL tail into metadata tables when it reaches this
    /// many segments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_tail_segments: Option<u64>,
    /// Run the mark-and-sweep garbage collector after the tick's
    /// flush work. Nothing sweeps unless this is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcRequest>,
}

/// What one maintenance tick did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
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

/// Result of one explicit maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceTickResponse {
    /// Namespace the tick ran against.
    pub namespace_id: NamespaceId,
    /// Namespace status observed before the tick acted.
    pub status_before: NamespaceStatusResponse,
    /// What the tick did.
    pub outcome: MaintenanceTickOutcome,
    /// Garbage-collection report when the tick opted into sweeping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcResponse>,
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
    }

    #[test]
    fn filesystem_delete_and_move_operations_use_behavior_field() {
        let create_directory = FilesystemOperation::CreateDirectory {
            path: "/docs".to_owned(),
            parents: false,
        };
        assert_eq!(
            serde_json::to_value(&create_directory).expect("create directory op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs"
            })
        );

        let create_directory_with_parents = FilesystemOperation::CreateDirectory {
            path: "/docs/notes".to_owned(),
            parents: true,
        };
        assert_eq!(
            serde_json::to_value(&create_directory_with_parents)
                .expect("create directory with parents op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs/notes",
                "parents": true
            })
        );

        let delete = FilesystemOperation::DeletePath {
            path: "/docs".to_owned(),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        };
        assert_eq!(
            serde_json::to_value(&delete).expect("delete op json"),
            serde_json::json!({
                "kind": "delete_path",
                "path": "/docs",
                "behavior": "recursive"
            })
        );

        let move_path = FilesystemOperation::MovePath {
            from_path: "/docs/a.txt".to_owned(),
            to_path: "/docs/b.txt".to_owned(),
            behavior: MoveBehavior::Replace,
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace"
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            from_path: "/docs/a.txt".to_owned(),
            to_path: "/docs/b.txt".to_owned(),
            behavior: CopyBehavior::Replace,
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace"
            })
        );
    }

    #[test]
    fn filesystem_operations_default_omitted_behavior_fields() {
        let put: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "put_file",
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
            "kind": "delete_path",
            "path": "/docs"
        }))
        .expect("delete op defaults behavior");
        assert_eq!(
            delete,
            FilesystemOperation::DeletePath {
                path: "/docs".to_owned(),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }
        );

        let move_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "move_path",
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

        let copy_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "copy_path",
            "from_path": "/docs/a.txt",
            "to_path": "/docs/b.txt"
        }))
        .expect("copy op defaults behavior");
        assert_eq!(
            copy_path,
            FilesystemOperation::CopyPath {
                from_path: "/docs/a.txt".to_owned(),
                to_path: "/docs/b.txt".to_owned(),
                behavior: CopyBehavior::NoReplace,
            }
        );
    }
}
