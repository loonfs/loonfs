//! Request/response shapes for the v0 HTTP API's operation endpoints:
//! namespace lifecycle (create/fork/status/delete), path-oriented filesystem
//! operations, file revisions, maintenance (checkpoint/retention), and the
//! shared [`ApiError`] body. Explicit commits and the change feed live in
//! [`super::commits`]; read-result shapes live in [`super::reads`].

use super::ContentToken;
use crate::{
    AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue, ChangeSeq, CheckpointId,
    CommitId, ContentRef, DisplayName, InodeId, ManifestNo, NamespaceId, RevisionNo, WriterEpoch,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Identifies the invalid input. Body fields use JSON Pointer paths;
    /// query and path parameters use their names; CLI errors use the flag or
    /// argument as written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    /// Correlation id the server assigned to the failed request; the same
    /// value is sent as the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Structured context for the code, present when the failure carries
    /// machine-usable identity (API spec, "Standard error contract"). Boxed
    /// so the rare detailed error does not widen every error-carrying result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub details: Option<Box<ErrorDetails>>,
}

/// Optional machine-readable details for an [`ApiError`].
///
/// Clients make retry decisions from the error code and use these fields for
/// relevant identifiers such as commit ids, writer epochs, and revisions.
/// Fields may be absent and clients must ignore fields they do not use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDetails {
    /// Idempotency key of the commit the error concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub commit_id: Option<CommitId>,
    /// Sequence at which that commit id already landed. Present when the
    /// failure was decided against a durable commit receipt, which is what
    /// holds the sequence; absent when nothing has committed under the id
    /// yet and two live requests are simply claiming it at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub committed_seq: Option<ChangeSeq>,
    /// Semantic identity of the mutation that already landed under that
    /// commit id, from the same receipt as `committed_seq` and present
    /// exactly when it is. A retry recomputes this value from the request it
    /// just made — see
    /// [`put_retry_fingerprint`](crate::put_retry_fingerprint) — and equality
    /// is what proves the two are the same request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_fingerprint: Option<String>,
    /// Position, in the request's operation list, of the operation that
    /// failed. A commit applies all of its operations or none of them, so
    /// this names the one that stopped the whole request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_index: Option<u32>,
    /// Epoch the failing writer session held when it was displaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fenced_writer_epoch: Option<WriterEpoch>,
    /// Epoch that currently owns the namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_writer_epoch: Option<WriterEpoch>,
    /// Writer id recorded by the current epoch's acquirer, when the head
    /// recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer: Option<String>,
    /// Unix milliseconds at which the current epoch's acquirer took it, when
    /// the head recorded one. Writer ids are process labels, so two runs on
    /// one machine can share one; the stamp is what tells them apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_acquired_at_ms: Option<u64>,
    /// Inode the failed precondition or operation targeted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub inode_id: Option<InodeId>,
    /// Revision the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_revision_no: Option<RevisionNo>,
    /// Revision that is actually current; absent when the inode has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_revision_no: Option<RevisionNo>,
    /// Attribute revision the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_attributes_revision_no: Option<AttributeRevisionNo>,
    /// Attribute revision that is actually current for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_attributes_revision_no: Option<AttributeRevisionNo>,
    /// Change-feed cursor the request asked to resume after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub after_seq: Option<ChangeSeq>,
    /// Oldest sequence still promised for incremental replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_floor_seq: Option<ChangeSeq>,
    /// Deletion generation the undelete expected to be active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_deletion_seq: Option<ChangeSeq>,
    /// Deletion generation actually active for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_deletion_seq: Option<ChangeSeq>,
    /// Head sequence a namespace delete required the namespace to still be
    /// at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_head_seq: Option<ChangeSeq>,
    /// Head sequence the namespace was actually at, which is what a caller
    /// that still means to delete it retries against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_head_seq: Option<ChangeSeq>,
}

/// Request to create a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// Durable namespace id to create.
    pub namespace_id: NamespaceId,
}

/// Request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// Durable namespace id for the fork target.
    pub new_namespace_id: NamespaceId,
}

/// Current state for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Namespace {
    /// Namespace ID.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// Namespace state and storage details used by maintenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceDiagnostics {
    /// Namespace ID.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
    /// Current manifest pointer recorded by the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub current_manifest_no: Option<ManifestNo>,
    /// Number of visible WAL segments after the current manifest.
    pub wal_tail_segments: u64,
    /// Number of snapshots that had not expired when diagnostics began.
    pub live_snapshots: u64,
    /// Number of active user checkpoints, including expired records awaiting collection.
    pub live_checkpoints: u64,
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

/// Destination-conflict behavior for path-oriented puts, moves, and copies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationBehavior {
    /// Fail if the destination path already exists.
    #[default]
    NoReplace,
    /// Replace the current file at the destination; only a file
    /// destination can be replaced.
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

/// One filesystem operation.
///
/// Unknown fields are rejected so a misspelled concurrency guard cannot be ignored.
/// Fieldless variants must use empty braces so serde rejects unexpected fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FilesystemOperation {
    /// Create one directory.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectory")
    )]
    CreateDirectory {
        /// Absolute destination path, rejected when invalid or already bound.
        path: AbsolutePath,
        /// Also create missing ancestor directories (the same auto-create
        /// `put_file` performs). The final component must still be new.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        parents: bool,
    },
    /// Create a directory under an existing parent inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectoryByInode")
    )]
    CreateDirectoryByInode {
        /// Parent directory.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        parent_inode_id: InodeId,
        /// New directory name.
        display_name: DisplayName,
    },
    /// Create or replace one file with an already-durable content ref.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationPutFile"))]
    PutFile {
        /// Absolute destination path; missing ancestors are created automatically.
        path: AbsolutePath,
        /// Immutable bytes that must be covered by a valid preparation proof.
        content_ref: ContentRef,
        /// Whether an existing file may receive a new revision instead of causing a conflict.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// With `replace` behavior, require the path to still point to this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        expected_inode_id: Option<InodeId>,
        /// With `replace` behavior, require the file to still have this revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// Create a file under an existing parent inode. The name must be unused.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileByInode")
    )]
    PutFileByInode {
        /// Parent directory.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        parent_inode_id: InodeId,
        /// New file name.
        display_name: DisplayName,
        /// Immutable bytes that must be covered by a valid preparation proof.
        content_ref: ContentRef,
    },
    /// Append a revision to a file inode if its current revision matches.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileRevisionByInode")
    )]
    PutFileRevisionByInode {
        /// File to update.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        inode_id: InodeId,
        /// Immutable bytes that must be covered by a valid preparation proof.
        content_ref: ContentRef,
        /// Current revision required for the write.
        expected_revision_no: RevisionNo,
    },
    /// Delete one path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationDeletePath"))]
    DeletePath {
        /// Absolute path that must resolve to a visible inode.
        path: AbsolutePath,
        /// Whether a non-empty directory may be tombstoned recursively.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
        /// When set, the delete applies only if the path still resolves to
        /// this inode; a raced rebinding fails the request instead of
        /// deleting (and reporting a recovery handle for) the wrong inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        expected_inode_id: Option<InodeId>,
    },
    /// Delete an inode if its current binding matches.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationDeleteByInode")
    )]
    DeleteByInode {
        /// Inode to delete.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        inode_id: InodeId,
        /// Binding generation required for the delete.
        expected_binding_generation: String,
        /// Whether a non-empty directory may be tombstoned recursively.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
    },
    /// Move one path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMovePath"))]
    MovePath {
        /// Absolute source path that must resolve to a visible inode.
        from_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        to_path: AbsolutePath,
        /// Whether an existing destination file may be replaced.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// With `replace` behavior, require the destination to still point to this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        destination_expected_inode_id: Option<InodeId>,
        /// With `replace` behavior, require the destination to still have this revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_expected_revision_no: Option<RevisionNo>,
    },
    /// Move an inode if its current binding matches.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMoveByInode"))]
    MoveByInode {
        /// Inode to move.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        inode_id: InodeId,
        /// Binding generation required for the move.
        expected_binding_generation: String,
        /// Destination directory.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        to_parent_inode_id: InodeId,
        /// New name.
        to_display_name: DisplayName,
        /// Whether an existing destination file may be replaced.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// With `replace` behavior, require the destination to still point to this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        destination_expected_inode_id: Option<InodeId>,
        /// With `replace` behavior, require the destination to still have this revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_expected_revision_no: Option<RevisionNo>,
    },
    /// Copy one file path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyPath"))]
    CopyPath {
        /// Absolute source path that must resolve to a visible file.
        from_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        to_path: AbsolutePath,
        /// Whether an existing destination file may receive a copied revision.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// With `replace` behavior, require the destination to still point to this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        destination_expected_inode_id: Option<InodeId>,
        /// With `replace` behavior, require the destination to still have this revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_expected_revision_no: Option<RevisionNo>,
    },
    /// Restore a deleted file or subtree.
    ///
    /// `inode_id` and `deletion_seq` identify one exact deletion. A stale
    /// sequence returns `not_deleted` and cannot undo a later deletion.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUndelete"))]
    Undelete {
        /// Deleted inode to make reachable again.
        #[serde(with = "crate::public_inode_id")]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer tombstone generation.
        deletion_seq: ChangeSeq,
        /// Optional destination for the restored inode.
        ///
        /// When absent, the inode is rebound to the parent and name recorded by the
        /// deletion. Parent identity, rather than an old path string, keeps this
        /// correct after ancestor renames. An explicit path is required when the
        /// deletion recorded no binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        path: Option<AbsolutePath>,
    },
    /// Restore an older revision as the current revision for a path.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationRestoreRevision")
    )]
    RestoreRevision {
        /// Absolute path that must resolve to a visible file.
        path: AbsolutePath,
        /// Existing historical revision whose content will be copied into a new current revision.
        source_revision_no: RevisionNo,
    },
    /// Write and remove attributes on the inode one path resolves to.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAttributes")
    )]
    UpdateAttributes {
        /// Absolute path that must resolve to a visible file or directory.
        path: AbsolutePath,
        /// Attributes to write. Each key replaces whatever the inode
        /// currently holds under it; keys the inode holds and this map does
        /// not name are left alone.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// Attribute keys to remove.
        ///
        /// A list preserves duplicate entries so validation can report them instead
        /// of silently deduplicating the request.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remove: Vec<AttributeKey>,
        /// When set, the update applies only if the path still resolves to
        /// this inode; a raced rebinding fails the request instead of
        /// writing attributes onto the wrong inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(
            feature = "openapi",
            schema(schema_with = crate::public_inode_id::schema)
        )]
        expected_inode_id: Option<InodeId>,
        /// When set, the update applies only while the inode's attribute
        /// revision is still this one. Absent means the update is applied
        /// over whatever revision is current; either way the write carries
        /// its own revision guard, so a concurrent update never merges
        /// silently.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributeRevisionNo>,
    },
}

impl FilesystemOperation {
    /// Returns the content written by this operation, if any.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::PutFile { content_ref, .. }
            | Self::PutFileByInode { content_ref, .. }
            | Self::PutFileRevisionByInode { content_ref, .. } => Some(content_ref),
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::UpdateAttributes { .. } => None,
        }
    }
}

/// A request to commit one or more filesystem operations.
///
/// Operations run in order and either all succeed or none are committed. A
/// request with one operation uses the same fingerprint rules as a batch.
///
/// Unknown fields are rejected here for the same reason they are on
/// [`FilesystemOperation`]: the fields a typo can hide are the ones that
/// decide whether the commit is guarded at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// Caller-supplied idempotency key for the whole request.
    pub commit_id: CommitId,
    /// Actor responsible for the commit, as supplied by the application.
    pub actor: crate::ActorRef,
    /// Caller annotation recorded on the commit and reported by the change
    /// feed. Part of the commit's identity: reusing `commit_id` with a
    /// different message is a `commit_id_reuse_conflict`, exactly as it is
    /// for an explicit commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Proofs for any new external content refs introduced by this request.
    /// One proof covers every operation that names its content ref.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ContentToken>,
    /// Ordered operations to apply. Must be non-empty; they commit all
    /// together or not at all.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// A request carrying exactly one operation.
    pub fn single(
        commit_id: CommitId,
        actor: crate::ActorRef,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            actor,
            message,
            content_tokens: Vec::new(),
            operations: vec![operation],
        }
    }
}

/// One immutable file revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FileRevision {
    /// File inode that owns this revision.
    #[serde(with = "crate::public_inode_id")]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub inode_id: InodeId,
    /// Revision number within the file inode.
    pub revision_no: RevisionNo,
    /// Namespace sequence that created this revision.
    pub committed_seq: ChangeSeq,
    /// Commit ID for this revision.
    pub commit_id: CommitId,
    /// Wall-clock stamp of the commit that created this revision, in Unix
    /// milliseconds. Observational: `committed_seq` is the order.
    pub committed_at_ms: u64,
    /// Actor responsible for this revision, as supplied by the application.
    pub committed_by: crate::ActorRef,
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
    #[serde(with = "crate::public_inode_id")]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub inode_id: InodeId,
    /// Namespace head sequence used for the read.
    pub head_seq: ChangeSeq,
    /// Retained revisions in order.
    pub revisions: Vec<FileRevision>,
    /// Opaque cursor for the next page, if more revisions are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request to create a durable checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateCheckpointRequest {
    /// Label recorded on the checkpoint record. A label, not a key: several
    /// records may carry the same name over different bases.
    pub name: String,
    /// Optional lifetime; the server computes the record's expiry from its
    /// own clock. Absent means the pin holds until explicitly released.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

/// Request to create a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateSnapshotRequest {
    /// A label that does not need to be unique.
    pub name: String,
    /// Snapshot lifetime from the current server time, in milliseconds.
    pub ttl_ms: u64,
}

/// Request to extend a read snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ExtendSnapshotRequest {
    /// Requested lifetime from the server's current time, in milliseconds.
    pub ttl_ms: u64,
}

/// Result of releasing a checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleaseCheckpointResponse {
    /// Namespace the checkpoint belonged to.
    pub namespace_id: NamespaceId,
    /// Checkpoint the release targeted.
    pub checkpoint_id: CheckpointId,
}

/// The owner of a checkpoint record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwnerSummary {
    /// An operator-created pin, released by id or by its own expiry.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerUser"))]
    User {
        /// The label the creator recorded. Not a key: several records may
        /// carry one label over different bases.
        name: String,
    },
    /// A fork target keeping its source basis alive for the length of one
    /// fork attempt.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerFork"))]
    Fork {
        /// Namespace whose continued existence keeps this pin standing.
        target_namespace_id: NamespaceId,
    },
    /// An application-created read view.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerSnapshot"))]
    Snapshot {
        /// A label that does not need to be unique.
        name: String,
        /// When the snapshot lease expires, in Unix milliseconds.
        expires_at_ms: u64,
    },
}

/// One checkpoint resource, reported from what its durable record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Checkpoint {
    /// Namespace that owns the checkpoint.
    pub namespace_id: NamespaceId,
    /// Durable checkpoint id used to address the checkpoint for release.
    pub checkpoint_id: CheckpointId,
    /// Who owns the checkpoint, including the label carried by a user pin.
    pub owner: CheckpointOwnerSummary,
    /// Time the checkpoint record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// When garbage collection may release the record without being asked,
    /// in Unix milliseconds. Absent means the pin holds until it is
    /// released. An instant already in the past is a record whose expiry
    /// has passed and which no collection pass has reached yet: it is still
    /// a root, so it is still listed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Sequence covered by the checkpoint's pinned basis.
    pub checkpoint_seq: ChangeSeq,
    /// Manifest pinned by the checkpoint.
    pub manifest_no: ManifestNo,
}

/// A live snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SnapshotSummary {
    /// Snapshot id.
    pub snapshot_id: CheckpointId,
    /// Namespace whose state the snapshot captured.
    pub namespace_id: NamespaceId,
    /// Snapshot label.
    pub name: String,
    /// Namespace sequence captured by the snapshot.
    pub head_seq: ChangeSeq,
    /// Time the snapshot record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// When the snapshot lease expires, in Unix milliseconds.
    pub expires_at_ms: u64,
}

impl SnapshotSummary {
    /// Converts a snapshot-owned checkpoint to a snapshot summary.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Option<Self> {
        let CheckpointOwnerSummary::Snapshot {
            name,
            expires_at_ms,
        } = checkpoint.owner
        else {
            return None;
        };
        Some(Self {
            snapshot_id: checkpoint.checkpoint_id,
            namespace_id: checkpoint.namespace_id,
            name,
            head_seq: checkpoint.checkpoint_seq,
            created_at_ms: checkpoint.created_at_ms,
            expires_at_ms,
        })
    }
}

/// One page of active checkpoint records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListCheckpointsResponse {
    /// Namespace the records belong to.
    pub namespace_id: NamespaceId,
    /// Active records in ascending checkpoint-id order. Released records are
    /// omitted even if garbage collection has not deleted them yet.
    pub checkpoints: Vec<Checkpoint>,
    /// Opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// One page of live read snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListSnapshotsResponse {
    /// Namespace the snapshots belong to.
    pub namespace_id: NamespaceId,
    /// Live snapshot records in ascending snapshot-id order.
    pub snapshots: Vec<SnapshotSummary>,
    /// Opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Result of releasing a read snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleaseSnapshotResponse {
    /// Namespace the snapshot belonged to.
    pub namespace_id: NamespaceId,
    /// Released snapshot id.
    pub snapshot_id: CheckpointId,
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
    /// Another publisher updated the root first, so this call's manifest is
    /// not referenced by the root.
    RootAdvanced,
}

/// Result of one WAL flush: what the metadata root references afterward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FlushWalResponse {
    /// Namespace whose WAL tail was flushed.
    pub namespace_id: NamespaceId,
    /// Head sequence the flush attempted to cover.
    pub target_head_seq: ChangeSeq,
    /// Manifest `metadata/root.json` references after the operation.
    pub manifest_no: ManifestNo,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// What this call did to the metadata root.
    pub outcome: FlushWalOutcome,
}

/// Optional overrides for one garbage-collection pass. Absent fields use
/// the server's conservative defaults.
///
/// Every field is optional, so a typo would take the default instead of the
/// override the caller asked for. Unknown fields are rejected so a misspelled
/// `max_objects` fails loudly rather than running an unbounded pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// Objects younger than this are never deleted, reachable or not. The
    /// window has a derived safety floor (publication budgets plus provider
    /// deadlines); a smaller value is rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_window_ms: Option<u64>,
    /// Maximum objects this invocation may read or decide. Omit to retain
    /// the run-to-completion behavior.
    ///
    /// A completed upload session past its reclamation grace makes the pass
    /// read every live manifest and retained WAL segment to find out
    /// whether anything still references its content, and that read is
    /// charged here like any other. A budget too small to finish it does
    /// not stall the pass: the session is retained, the response sets
    /// `content_reclamation_deferred`, and the sweep carries on through
    /// everything else. What a chronically small budget costs is content
    /// left unreclaimed, not progress. Give a pass at least as many objects
    /// as the namespace has live manifests and retained segments for that
    /// content to come back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque resume token returned as `next_cursor` by an earlier pass
    /// against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Candidates inspected but not deleted by one GC pass.
///
/// Every field is present, including zero counts. The fields sum to
/// `retained_candidates` in [`GcResponse`].
///
/// An object inspected by multiple passes is counted once per pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// Candidates found reachable during the final check before deletion.
    pub referenced: u64,
    /// Unreachable, but younger than the grace window by the object's own
    /// provider timestamp. A later pass deletes it.
    pub within_grace_window: u64,
    /// Unreachable candidates with no provider timestamp. Their age is
    /// unknown, so the pass keeps them.
    pub no_provider_timestamp: u64,
    /// Unreachable candidates that cannot be checked against a manifest old
    /// enough to cover the grace window.
    pub no_reference_manifest: u64,
    /// Candidates kept because root resolution failed. The response also
    /// sets `retention_degraded`.
    pub degraded_roots: u64,
    /// Unrecognized keys in a family scanned by GC. These keys are never
    /// deleted.
    pub unrecognized_key: u64,
    /// Checkpoint records that could not be safely released or deleted.
    pub checkpoint_not_releasable: u64,
    /// Upload sessions still protected by a lease or grace window.
    pub upload_session_window: u64,
    /// Upload sessions kept because the pass could not determine whether
    /// they were safe to delete.
    pub upload_session_undecided: u64,
    /// Completed sessions skipped because the reference scan exceeded
    /// `max_objects`. The response also sets `content_reclamation_deferred`.
    pub content_scan_deferred: u64,
}

/// Objects deleted by one GC pass, grouped by object family. Every field is
/// present, including zero counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedObjectCounts {
    /// Unreferenced WAL segments deleted.
    pub wal_segments: u64,
    /// Unreferenced metadata segments deleted.
    pub metadata_segments: u64,
    /// Unreferenced manifests deleted.
    pub manifests: u64,
    /// Released checkpoint records deleted after their grace window.
    pub checkpoint_records: u64,
    /// Upload-session control objects deleted after the reap window.
    pub upload_sessions: u64,
    /// Content objects deleted after their completed upload session passed
    /// the reclamation grace period and no reachable data referenced them.
    /// Cleanup of abandoned sessions is not counted here.
    pub content_objects: u64,
}

impl DeletedObjectCounts {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            wal_segments,
            metadata_segments,
            manifests,
            checkpoint_records,
            upload_sessions,
            content_objects,
        } = other;
        self.wal_segments += wal_segments;
        self.metadata_segments += metadata_segments;
        self.manifests += manifests;
        self.checkpoint_records += checkpoint_records;
        self.upload_sessions += upload_sessions;
        self.content_objects += content_objects;
    }
}

/// Checkpoint records released by one GC pass, grouped by reason.
///
/// Releasing a record stops it from pinning data. A later pass may delete the
/// record after its grace window and count that under
/// [`DeletedObjectCounts::checkpoint_records`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleasedCheckpointCounts {
    /// Fork-owned records released because their target namespace is
    /// provably gone.
    pub fork: u64,
    /// User-owned records released because their expiry passed, or because
    /// they sit on a terminally deleted namespace.
    pub expired: u64,
    /// Active records released because their basis manifest is verifiably
    /// gone.
    pub missing_basis: u64,
    /// Snapshot-owned records released because their expiry passed, or
    /// because they sit on a terminally deleted namespace.
    pub snapshot: u64,
}

impl ReleasedCheckpointCounts {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            fork,
            expired,
            missing_basis,
            snapshot,
        } = other;
        self.fork += fork;
        self.expired += expired;
        self.missing_basis += missing_basis;
        self.snapshot += snapshot;
    }
}

/// Result of one mark-and-sweep garbage-collection pass.
///
/// Deletion counts are grouped by object family. Checkpoint releases and
/// retained candidates are grouped by reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// Namespace the pass ran against.
    pub namespace_id: NamespaceId,
    /// Objects the pass deleted, split by object family.
    pub deleted: DeletedObjectCounts,
    /// Checkpoint records the pass released, split by the reason each one
    /// was released.
    pub released_checkpoints: ReleasedCheckpointCounts,
    /// Candidates retained at delete time (grace window, missing
    /// timestamps, or reachable from the fresh root set).
    pub retained_candidates: u64,
    /// `retained_candidates` grouped by reason.
    pub retained: RetainedCandidates,
    /// True when ambiguous roots suppressed manifest/segment deletion.
    pub retention_degraded: bool,
    /// True when `max_objects` was too small to build the complete reference
    /// set required for completed-content reclamation.
    pub content_reclamation_deferred: bool,
    /// True when the pass reached `max_objects` before it finished. Use
    /// `next_cursor` to continue or run again with a larger limit.
    pub budget_exhausted: bool,
    /// Opaque resume token when more candidates remain. It is valid only for
    /// the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Earliest known time when a retained upload session may become
    /// reclaimable. This covers open-session leases and grace periods for
    /// aborted or completed sessions. It only reflects candidates inspected
    /// by this pass, so absence does not mean no future work remains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_reclamation_at_ms: Option<u64>,
}

impl GcResponse {
    /// An empty report for `namespace_id`, before any candidate is examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted: DeletedObjectCounts::default(),
            released_checkpoints: ReleasedCheckpointCounts::default(),
            retained_candidates: 0,
            retained: RetainedCandidates::default(),
            retention_degraded: false,
            content_reclamation_deferred: false,
            budget_exhausted: false,
            next_cursor: None,
            next_reclamation_at_ms: None,
        }
    }

    /// Records one retained candidate under the reason that spared it.
    ///
    /// The total and the breakdown move together here so they cannot drift:
    /// every sweep site names a reason, and no site can count a retention
    /// without naming one.
    pub fn retain(&mut self, reason: RetainedReason) {
        self.retained_candidates += 1;
        *reason.counter(&mut self.retained) += 1;
    }
}

/// The reason one candidate was retained, as the sweep site knows it. Each
/// variant is the field of [`RetainedCandidates`] it counts into, where the
/// reason itself is described.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedReason {
    /// Counts into [`RetainedCandidates::referenced`].
    Referenced,
    /// Counts into [`RetainedCandidates::within_grace_window`].
    WithinGraceWindow,
    /// Counts into [`RetainedCandidates::no_provider_timestamp`].
    NoProviderTimestamp,
    /// Counts into [`RetainedCandidates::no_reference_manifest`].
    NoReferenceManifest,
    /// Counts into [`RetainedCandidates::degraded_roots`].
    DegradedRoots,
    /// Counts into [`RetainedCandidates::unrecognized_key`].
    UnrecognizedKey,
    /// Counts into [`RetainedCandidates::checkpoint_not_releasable`].
    CheckpointNotReleasable,
    /// Counts into [`RetainedCandidates::upload_session_window`].
    UploadSessionWindow,
    /// Counts into [`RetainedCandidates::upload_session_undecided`].
    UploadSessionUndecided,
    /// Counts into [`RetainedCandidates::content_scan_deferred`].
    ContentScanDeferred,
}

impl RetainedReason {
    fn counter(self, retained: &mut RetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::WithinGraceWindow => &mut retained.within_grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::NoReferenceManifest => &mut retained.no_reference_manifest,
            Self::DegradedRoots => &mut retained.degraded_roots,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::CheckpointNotReleasable => &mut retained.checkpoint_not_releasable,
            Self::UploadSessionWindow => &mut retained.upload_session_window,
            Self::UploadSessionUndecided => &mut retained.upload_session_undecided,
            Self::ContentScanDeferred => &mut retained.content_scan_deferred,
        }
    }
}

impl RetainedCandidates {
    /// Returns every reason and count in a fixed order.
    pub fn by_reason(&self) -> [(&'static str, u64); 10] {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            no_reference_manifest,
            degraded_roots,
            unrecognized_key,
            checkpoint_not_releasable,
            upload_session_window,
            upload_session_undecided,
            content_scan_deferred,
        } = *self;
        [
            ("referenced", referenced),
            ("within_grace_window", within_grace_window),
            ("no_provider_timestamp", no_provider_timestamp),
            ("no_reference_manifest", no_reference_manifest),
            ("degraded_roots", degraded_roots),
            ("unrecognized_key", unrecognized_key),
            ("checkpoint_not_releasable", checkpoint_not_releasable),
            ("upload_session_window", upload_session_window),
            ("upload_session_undecided", upload_session_undecided),
            ("content_scan_deferred", content_scan_deferred),
        ]
    }

    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            no_reference_manifest,
            degraded_roots,
            unrecognized_key,
            checkpoint_not_releasable,
            upload_session_window,
            upload_session_undecided,
            content_scan_deferred,
        } = other;
        self.referenced += referenced;
        self.within_grace_window += within_grace_window;
        self.no_provider_timestamp += no_provider_timestamp;
        self.no_reference_manifest += no_reference_manifest;
        self.degraded_roots += degraded_roots;
        self.unrecognized_key += unrecognized_key;
        self.checkpoint_not_releasable += checkpoint_not_releasable;
        self.upload_session_window += upload_session_window;
        self.upload_session_undecided += upload_session_undecided;
        self.content_scan_deferred += content_scan_deferred;
    }

    /// The reason with the highest count, and that count. `None` when
    /// nothing was retained. Ties go to the first in [`Self::by_reason`]
    /// order, so one pass's report is stable.
    pub fn top_reason(&self) -> Option<(&'static str, u64)> {
        self.by_reason()
            .into_iter()
            .filter(|(_, count)| *count > 0)
            // `max_by_key` keeps the last of equal maxima, so the reversal
            // is what makes a tie report the earlier reason.
            .rev()
            .max_by_key(|(_, count)| *count)
    }
}

/// Selects retention-floor advancement. This request has no options yet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct AdvanceRetentionRequest {}

/// Result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// New minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// One explicit maintenance step: the actions it selects, and nothing more.
///
/// Selection is presence. Each field names one independent action, and a
/// step runs exactly the ones the body carries — a request that selects
/// nothing is rejected rather than quietly doing nothing. Unknown fields are
/// rejected for the same reason: a misspelled selector would leave its action
/// unrun, and the caller would read the empty report as "there was nothing to
/// do".
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MaintenanceStepRequest {
    /// Flush the visible WAL tail into metadata segments, then run one bounded
    /// reorganization step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub metadata_maintenance: Option<MetadataMaintenanceRequest>,
    /// Advance the retention floor to the flushed manifest head. Include this
    /// field to select the action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention: Option<AdvanceRetentionRequest>,
    /// Run one bounded mark-and-sweep garbage-collection pass. Omit this
    /// field to skip garbage collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub gc: Option<GcRequest>,
}

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataMaintenanceRequest {
    /// Flush the visible WAL tail once it reaches this many segments.
    /// Absent uses the server's default threshold; zero, and any value above
    /// the write-rejection threshold, are rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_tail_segments: Option<u64>,
}

/// What the WAL-flush part of a maintenance step did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WalFlushStepOutcome {
    /// The tail was below the threshold, so there was nothing to flush.
    NotNeeded,
    /// The step flushed the WAL tail and advanced the metadata root.
    Flushed {
        /// Sequence covered by the published manifest.
        manifest_head_seq: ChangeSeq,
    },
    /// The step did not update the root because it already referenced a
    /// different manifest.
    AlreadyPublished {
        /// Sequence this step attempted to flush through.
        attempted_seq: ChangeSeq,
        /// Manifest the root currently references.
        current_manifest_no: ManifestNo,
    },
    /// Concurrent updates prevented every attempt from publishing. Nothing
    /// was flushed, and a later step can try again.
    RetriesExhausted {
        /// Head sequence observed before the step ran.
        observed_head_seq: ChangeSeq,
    },
}

/// What the metadata-reorganization part of a maintenance step did.
///
/// Deliberately coarse: the run counts and byte budgets a reorganization
/// consumes are engine policy, not a wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReorganizeStepOutcome {
    /// No family group had enough delta runs to merge.
    NotNeeded,
    /// One family group was merged and a manifest published.
    UnitPublished,
    /// A group has outgrown one step, and this step started the background
    /// streaming compaction that rebuilds it. The step published nothing;
    /// the job publishes once, when it finishes.
    CompactionStarted,
    /// A job for this namespace is already running, so this step started
    /// none. One runs at a time per namespace; a later step plans this group
    /// again.
    CompactionRunning,
    /// This step's job holds the namespace's slot and is waiting for a
    /// process compaction permit. It starts when one frees; nothing is
    /// needed to make it.
    CompactionAtCapacity,
    /// A group needs a streaming compaction and this handle schedules no
    /// background work, so nothing will run one until an operator does. The
    /// self-hosting guide names the call.
    CompactionRequired,
    /// Another publisher updated the metadata root first. This step's
    /// manifest is unreferenced, and a later step can retry the merge.
    RootAdvanced,
}

/// Result of one explicit maintenance step.
///
/// One report per action the request selected, and none for an action it
/// did not: an absent field means "not selected", never "ran and found
/// nothing to do". The latter is what the outcomes inside a report say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceStepResponse {
    /// Namespace the step ran against.
    pub namespace_id: NamespaceId,
    /// Namespace diagnostics observed before the step acted.
    pub status_before: NamespaceDiagnostics,
    /// What the metadata-upkeep action did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub metadata_maintenance: Option<MetadataMaintenanceResponse>,
    /// Where the retention floor ended up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention: Option<AdvanceRetentionResponse>,
    /// What the collection pass reclaimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub gc: Option<GcResponse>,
}

/// What one metadata-upkeep action did, part by part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceResponse {
    /// What the WAL flush did.
    pub wal_flush: WalFlushStepOutcome,
    /// What the reorganization unit did.
    pub reorganize: ReorganizeStepOutcome,
}

/// Options for one store contract probe. Empty today; a body is still sent
/// so later options do not change the shape of the request. An option this
/// build does not know is rejected rather than ignored, so a caller never
/// believes it selected something.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StoreProbeRequest {}

/// What one store contract probe observed, check by check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeResponse {
    /// Label the server minted for this run. It scopes the objects the run
    /// wrote, so it identifies the run in provider logs too.
    pub run_id: String,
    /// Every check the run performed, in the order it performed them. A
    /// failed check lives here rather than in an error: the probe answered
    /// the question, and the answer is that the store is wrong.
    pub checks: Vec<StoreProbeCheckResult>,
}

/// One named contract check and what the store did with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeCheckResult {
    /// Stable check name.
    pub name: String,
    /// What the store did.
    pub outcome: StoreProbeCheckOutcome,
    /// What was expected and what happened instead. Present only on
    /// `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// What one contract check concluded about the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum StoreProbeCheckOutcome {
    /// The store behaved as the contract requires.
    Passed,
    /// The store declares it cannot do this at all. Only the optional
    /// capabilities answer this way, and it is an answer rather than a
    /// fault.
    Unsupported,
    /// The store did something the contract forbids, or the operation
    /// failed outright.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentId;

    #[test]
    fn file_revision_provenance_fields_are_pinned_on_the_wire() {
        let content_ref = ContentRef::blob_v1(crate::ContentId::generate(), b"hello");
        let revision = FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(7),
            commit_id: CommitId::parse("c_revision_owner").expect("commit id"),
            committed_at_ms: 1_752_624_000_000,
            committed_by: crate::ActorRef::loonfs_system(),
            content_ref: content_ref.clone(),
        };

        assert_eq!(
            serde_json::to_value(revision).expect("serialize file revision"),
            serde_json::json!({
                "inode_id": "ino_2",
                "revision_no": 3,
                "committed_seq": 7,
                "commit_id": "c_revision_owner",
                "committed_at_ms": 1_752_624_000_000_u64,
                "committed_by": { "kind": "system", "id": "loonfs" },
                "content_ref": content_ref,
            })
        );
    }
    fn path(value: &str) -> AbsolutePath {
        AbsolutePath::parse(value).expect("valid test path")
    }

    fn attribute_key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid test attribute key")
    }

    fn sample_content_ref() -> ContentRef {
        ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id"),
            b"hello",
        )
    }

    #[test]
    fn namespace_wire_shape_has_only_core_state() {
        let namespace = Namespace {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
        };
        assert_eq!(
            serde_json::to_value(namespace).expect("serialize namespace"),
            serde_json::json!({
                "namespace_id": "demo",
                "head_seq": 11,
                "retention_floor_seq": 4
            })
        );
    }

    #[test]
    fn namespace_diagnostics_wire_shape_keeps_storage_fields() {
        let diagnostics = NamespaceDiagnostics {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
            current_manifest_no: Some(ManifestNo(8)),
            wal_tail_segments: 3,
            live_snapshots: 2,
            live_checkpoints: 5,
        };
        assert_eq!(
            serde_json::to_value(diagnostics).expect("serialize namespace diagnostics"),
            serde_json::json!({
                "namespace_id": "demo",
                "head_seq": 11,
                "retention_floor_seq": 4,
                "current_manifest_no": 8,
                "wal_tail_segments": 3,
                "live_snapshots": 2,
                "live_checkpoints": 5
            })
        );
    }

    #[test]
    fn behavior_enums_use_snake_case_wire_values() {
        assert_eq!(
            DestinationBehavior::default(),
            DestinationBehavior::NoReplace
        );
        assert_eq!(
            DeleteDirectoryBehavior::default(),
            DeleteDirectoryBehavior::NonRecursive
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::NoReplace)
                .expect("destination behavior json"),
            serde_json::json!("no_replace")
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::Replace).expect("destination behavior json"),
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
            path: path("/docs"),
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
            path: path("/docs/notes"),
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
            path: path("/docs"),
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
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(InodeId(7)),
            destination_expected_revision_no: Some(RevisionNo(3)),
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace",
                "destination_expected_inode_id": "ino_7",
                "destination_expected_revision_no": 3
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(InodeId(7)),
            destination_expected_revision_no: Some(RevisionNo(3)),
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace",
                "destination_expected_inode_id": "ino_7",
                "destination_expected_revision_no": 3
            })
        );

        let update_attributes = FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(
                attribute_key("owner"),
                AttributeValue::parse("ada").expect("valid attribute value"),
            )]),
            remove: vec![attribute_key("draft")],
            expected_inode_id: Some(InodeId(7)),
            expected_attributes_revision_no: Some(AttributeRevisionNo(3)),
        };
        assert_eq!(
            serde_json::to_value(&update_attributes).expect("update attributes op json"),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": "ada"},
                "remove": ["draft"],
                "expected_inode_id": "ino_7",
                "expected_attributes_revision_no": 3
            })
        );
    }

    #[test]
    fn update_attributes_omits_empty_collections_and_absent_guards() {
        let set_only = FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(
                attribute_key("owner"),
                AttributeValue::parse("ada,grace").expect("valid attribute value"),
            )]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        };
        assert_eq!(
            serde_json::to_value(&set_only).expect("set-only op json"),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": "ada,grace"}
            })
        );

        let decoded: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "update_attributes",
            "path": "/docs/a.txt",
            "remove": ["draft"]
        }))
        .expect("remove-only op defaults the set map and both guards");
        assert_eq!(
            decoded,
            FilesystemOperation::UpdateAttributes {
                path: path("/docs/a.txt"),
                set: BTreeMap::new(),
                remove: vec![attribute_key("draft")],
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            }
        );
    }

    #[test]
    fn update_attributes_validates_keys_and_values_during_deserialization() {
        // The key grammar and the value shape are enforced on the way in, so
        // a malformed update never reaches planning.
        for encoded in [
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"": "ada"}
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": {"kind": "string", "value": "ada"}}
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "remove": ["a\u{0}b"]
            }),
        ] {
            assert!(serde_json::from_value::<FilesystemOperation>(encoded).is_err());
        }
    }

    #[test]
    fn filesystem_operations_default_omitted_behavior_fields() {
        let put: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "put_file",
            "path": "/docs/a.txt",
            "content_ref": {
                "kind": "blob_v1",
                "content_id": "con_0123456789abcdef0123456789abcdef",
                "size_bytes": 1,
                "checksum": {
                    "algorithm": "sha256",
                    "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
            }
        }))
        .expect("put op defaults behavior");
        assert!(matches!(
            put,
            FilesystemOperation::PutFile {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
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
                path: path("/docs"),
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
                from_path: path("/docs/a.txt"),
                to_path: path("/docs/b.txt"),
                behavior: DestinationBehavior::NoReplace,
                destination_expected_inode_id: None,
                destination_expected_revision_no: None,
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
                from_path: path("/docs/a.txt"),
                to_path: path("/docs/b.txt"),
                behavior: DestinationBehavior::NoReplace,
                destination_expected_inode_id: None,
                destination_expected_revision_no: None,
            }
        );
    }

    #[test]
    fn filesystem_operation_paths_keep_the_plain_string_wire_shape() {
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let cases = [
            (
                FilesystemOperation::PutFile {
                    path: path("/docs/a.txt"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
                serde_json::json!({
                    "kind": "put_file",
                    "path": "/docs/a.txt",
                    "content_ref": content_ref,
                    "behavior": "no_replace"
                }),
            ),
            (
                FilesystemOperation::Undelete {
                    inode_id: InodeId(7),
                    deletion_seq: ChangeSeq(8),
                    path: Some(path("/docs/restored")),
                },
                serde_json::json!({
                    "kind": "undelete",
                    "inode_id": "ino_7",
                    "deletion_seq": 8,
                    "path": "/docs/restored"
                }),
            ),
            (
                FilesystemOperation::RestoreRevision {
                    path: path("/docs/a.txt"),
                    source_revision_no: RevisionNo(2),
                },
                serde_json::json!({
                    "kind": "restore_revision",
                    "path": "/docs/a.txt",
                    "source_revision_no": 2
                }),
            ),
            (
                FilesystemOperation::UpdateAttributes {
                    path: path("/docs/a.txt"),
                    set: BTreeMap::new(),
                    remove: vec![attribute_key("draft")],
                    expected_inode_id: None,
                    expected_attributes_revision_no: None,
                },
                serde_json::json!({
                    "kind": "update_attributes",
                    "path": "/docs/a.txt",
                    "remove": ["draft"]
                }),
            ),
        ];

        for (operation, string_shaped_json) in cases {
            assert_eq!(
                serde_json::to_value(operation).expect("serialize filesystem operation"),
                string_shaped_json
            );
        }
    }

    #[test]
    fn filesystem_operation_paths_validate_during_deserialization() {
        for encoded in [
            serde_json::json!({"kind": "create_directory", "path": "relative", "parents": false}),
            serde_json::json!({
                "kind": "put_file",
                "path": "relative",
                "content_ref": ContentRef::blob_v1(ContentId::generate(), b"hello")
            }),
            serde_json::json!({"kind": "delete_path", "path": "relative"}),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "relative",
                "to_path": "/target"
            }),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/source",
                "to_path": "relative"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": "ino_7",
                "deletion_seq": 8,
                "path": "relative"
            }),
            serde_json::json!({
                "kind": "restore_revision",
                "path": "relative",
                "source_revision_no": 2
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "relative",
                "remove": ["draft"]
            }),
        ] {
            assert!(serde_json::from_value::<FilesystemOperation>(encoded).is_err());
        }
    }

    #[test]
    fn inode_request_fields_accept_only_the_public_format() {
        let operations = [
            serde_json::json!({
                "kind": "delete_path",
                "path": "/docs/a.txt",
                "expected_inode_id": "ino_27"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": "ino_27",
                "deletion_seq": 8
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "expected_inode_id": "ino_27"
            }),
        ];

        for operation in operations {
            serde_json::from_value::<FilesystemOperation>(operation.clone())
                .expect("valid public inode ID");

            let inode_key = if operation["kind"] == "undelete" {
                "inode_id"
            } else {
                "expected_inode_id"
            };
            for invalid in [serde_json::json!(27), serde_json::json!("27")] {
                let mut invalid_operation = operation.clone();
                invalid_operation[inode_key] = invalid;
                assert!(
                    serde_json::from_value::<FilesystemOperation>(invalid_operation).is_err(),
                    "{inode_key} accepted an invalid inode ID"
                );
            }
        }
    }

    #[test]
    fn a_misspelled_guard_does_not_decode() {
        let put = |guard: &str| {
            let mut operation = serde_json::json!({
                "kind": "put_file",
                "path": "/docs/a.txt",
                "content_ref": sample_content_ref(),
                "behavior": "replace"
            });
            operation[guard] = serde_json::json!(3);
            serde_json::json!({
                "commit_id": "guarded-put",
                "actor": crate::ActorRef::loonfs_system(),
                "operations": [operation]
            })
        };

        let spelled: CommitRequest = serde_json::from_value(put("expected_revision_no"))
            .expect("the guard spelled correctly decodes");
        assert!(matches!(
            spelled.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(3)),
                ..
            }]
        ));

        for misspelling in ["expected_revsion_no", "expectedRevisionNo"] {
            assert!(
                serde_json::from_value::<CommitRequest>(put(misspelling)).is_err(),
                "`{misspelling}` decoded instead of failing the request"
            );
        }
    }

    #[test]
    fn expected_revision_no_must_fit_the_public_integer_range() {
        let body = |expected_revision_no: u64| {
            serde_json::json!({
                "commit_id": "bounded-revision-guard",
                "actor": crate::ActorRef::loonfs_system(),
                "operations": [{
                    "kind": "put_file",
                    "path": "/docs/a.txt",
                    "content_ref": sample_content_ref(),
                    "behavior": "replace",
                    "expected_revision_no": expected_revision_no
                }]
            })
        };

        let request: CommitRequest = serde_json::from_value(body(crate::MAX_PUBLIC_INTEGER))
            .expect("deserialize the maximum revision number");
        assert!(matches!(
            request.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(value)),
                ..
            }] if *value == crate::MAX_PUBLIC_INTEGER
        ));

        let error = serde_json::from_value::<CommitRequest>(body(crate::MAX_PUBLIC_INTEGER + 1))
            .expect_err("reject a revision number above the public limit");
        assert!(
            error
                .to_string()
                .contains("must be an integer from 0 through 9007199254740991"),
            "unexpected range error: {error}"
        );
    }

    #[test]
    fn a_commit_request_rejects_unknown_fields_at_every_level() {
        let valid = || {
            serde_json::json!({
                "commit_id": "strict-commit",
                "actor": crate::ActorRef::loonfs_system(),
                "content_tokens": [{
                    "content_ref": sample_content_ref(),
                    "token": "opaque-proof"
                }],
                "operations": [{
                    "kind": "update_attributes",
                    "path": "/docs/a.txt",
                    "set": {"owner": "ada"},
                    "expected_inode_id": "ino_7"
                }]
            })
        };
        serde_json::from_value::<CommitRequest>(valid())
            .expect("the same body without a typo decodes");

        let mut at_root = valid();
        at_root["mesage"] = serde_json::json!("a note");

        let mut in_operation = valid();
        in_operation["operations"][0]["expectedAttributesRevisionNo"] = serde_json::json!(3);

        let mut in_content_token = valid();
        in_content_token["content_tokens"][0]["expires_at_ms"] = serde_json::json!(1);

        let mut in_content_ref = valid();
        in_content_ref["content_tokens"][0]["content_ref"]["sizeBytes"] = serde_json::json!(5);

        for (level, body) in [
            ("the request root", at_root),
            ("an operation variant", in_operation),
            ("a nested content token", in_content_token),
            ("a content ref below that", in_content_ref),
        ] {
            assert!(
                serde_json::from_value::<CommitRequest>(body).is_err(),
                "an unknown field in {level} decoded instead of failing the request"
            );
        }
    }

    #[test]
    fn checkpoint_responses_use_one_checkpoint_wire_object() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let checkpoint = Checkpoint {
            namespace_id: namespace_id.clone(),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwnerSummary::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: Some(1_752_626_600_000),
            checkpoint_seq: ChangeSeq(12),
            manifest_no: ManifestNo(9),
        };
        let checkpoint_json = serde_json::json!({
            "namespace_id": "demo",
            "checkpoint_id": "chk_00000000000000000000000000000001",
            "owner": {"kind": "user", "name": "release"},
            "created_at_ms": 1_752_623_000_000_u64,
            "expires_at_ms": 1_752_626_600_000_u64,
            "checkpoint_seq": 12,
            "manifest_no": 9,
        });
        assert_eq!(
            serde_json::to_value(checkpoint.clone()).expect("serialize checkpoint"),
            checkpoint_json,
        );
        assert_eq!(
            serde_json::to_value(ListCheckpointsResponse {
                namespace_id: namespace_id.clone(),
                checkpoints: vec![checkpoint.clone()],
                next_cursor: None,
            })
            .expect("serialize list checkpoints response"),
            serde_json::json!({
                "namespace_id": "demo",
                "checkpoints": [checkpoint_json],
            }),
        );
        assert_eq!(
            serde_json::to_value(ReleaseCheckpointResponse {
                namespace_id,
                checkpoint_id: checkpoint.checkpoint_id,
            })
            .expect("serialize release checkpoint response"),
            serde_json::json!({
                "namespace_id": "demo",
                "checkpoint_id": "chk_00000000000000000000000000000001",
            }),
        );
    }

    #[test]
    fn optional_response_fields_are_omitted_and_default_when_absent() {
        let checkpoint_json = serde_json::to_value(Checkpoint {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwnerSummary::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: None,
            checkpoint_seq: ChangeSeq(3),
            manifest_no: ManifestNo(3),
        })
        .expect("serialize checkpoint");
        assert!(checkpoint_json.get("expires_at_ms").is_none());
        let checkpoint: Checkpoint = serde_json::from_value(checkpoint_json)
            .expect("decode checkpoint without optional fields");
        assert_eq!(checkpoint.expires_at_ms, None);

        let gc = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
        let gc_json = serde_json::to_value(gc).expect("serialize gc response");
        assert!(gc_json.get("next_reclamation_at_ms").is_none());
        let gc: GcResponse =
            serde_json::from_value(gc_json).expect("decode gc response without optional fields");
        assert_eq!(gc.next_reclamation_at_ms, None);
    }

    #[test]
    fn maintenance_step_outcomes_use_the_outcome_tag() {
        assert_eq!(
            serde_json::to_value(WalFlushStepOutcome::Flushed {
                manifest_head_seq: ChangeSeq(9),
            })
            .expect("serialize WAL flush outcome"),
            serde_json::json!({"outcome": "flushed", "manifest_head_seq": 9})
        );
        assert_eq!(
            serde_json::to_value(ReorganizeStepOutcome::UnitPublished)
                .expect("serialize reorganize outcome"),
            serde_json::json!({"outcome": "unit_published"})
        );
    }

    #[test]
    fn maintenance_request_bodies_reject_unknown_fields() {
        serde_json::from_value::<MaintenanceStepRequest>(serde_json::json!({
            "metadata_maintenance": {"max_wal_tail_segments": 4},
            "retention": {},
            "gc": {"grace_window_ms": 1_800_000, "max_objects": 32}
        }))
        .expect("the same body without a typo decodes");

        for body in [
            serde_json::json!({"retenton": {}}),
            serde_json::json!({"retention": {"through_seq": 4}}),
            serde_json::json!({"metadata_maintenance": {"maxWalTailSegments": 4}}),
            serde_json::json!({"gc": {"max_object": 32}}),
        ] {
            assert!(
                serde_json::from_value::<MaintenanceStepRequest>(body.clone()).is_err(),
                "an unknown field decoded instead of failing the step: {body}"
            );
        }

        serde_json::from_value::<CreateCheckpointRequest>(
            serde_json::json!({"name": "nightly", "ttl_ms": 60_000}),
        )
        .expect("the same checkpoint body without a typo decodes");
        assert!(serde_json::from_value::<CreateCheckpointRequest>(
            serde_json::json!({"name": "nightly", "ttlMs": 60_000})
        )
        .is_err());

        // The probe body carries no options yet, so an unknown one is the
        // only thing it can be sent.
        serde_json::from_value::<StoreProbeRequest>(serde_json::json!({}))
            .expect("an empty probe body decodes");
        assert!(
            serde_json::from_value::<StoreProbeRequest>(serde_json::json!({"deep": true})).is_err()
        );

        serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
            "namespace_id": "demo"
        }))
        .expect("the same create body without a typo decodes");
        assert!(
            serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
                "namespace_id": "demo",
                "fork_of": "other"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ForkNamespaceRequest>(serde_json::json!({
                "new_namespace_id": "demo",
                "source_namespace_id": "other"
            }))
            .is_err()
        );
    }
}
