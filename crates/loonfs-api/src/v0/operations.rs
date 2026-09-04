//! Operation requests and responses for the v0 HTTP API.

use super::ContentToken;
use crate::{
    AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue, BindingGeneration, ChangeSeq,
    CheckpointId, CommitId, ContentRef, DisplayName, InodeId, ManifestNo, NamespaceId, RevisionNo,
    WriterEpoch,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// HTTP error body used by LoonFS APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = ErrorResponse))]
pub struct ApiError {
    /// The stable machine-readable error code as a string.
    pub code: String,
    /// The capability feature key for a `not_supported` error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    /// Human-readable error message.
    pub message: String,
    /// The invalid JSON Pointer, parameter name, CLI flag, or CLI argument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    /// The request correlation ID also sent in the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// The optional machine-readable context for the error code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub details: Option<Box<ErrorDetails>>,
}

/// Optional machine-readable identifiers and state for an [`ApiError`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDetails {
    /// Idempotency key of the commit the error concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub commit_id: Option<CommitId>,
    /// The sequence where this commit ID already landed, when recorded by a durable receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub committed_seq: Option<ChangeSeq>,
    /// The fingerprint of the mutation that landed under `commit_id`, present with `committed_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_fingerprint: Option<String>,
    /// The index of the failed operation in the request.
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
    /// The writer ID recorded for the current epoch, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer: Option<String>,
    /// The Unix-millisecond time when the current writer acquired its epoch, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_acquired_at_ms: Option<u64>,
    /// Maximum writer sessions admitted by the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_writer_sessions: Option<usize>,
    /// Inode the failed precondition or operation targeted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    pub inode_id: Option<InodeId>,
    /// The request expected the path to contain this inode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    pub expected_inode_id: Option<InodeId>,
    /// The path actually contained this inode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    pub actual_inode_id: Option<InodeId>,
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
    /// The head sequence required by a namespace delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_head_seq: Option<ChangeSeq>,
    /// The actual namespace head sequence.
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
    /// The final committed sequence before the namespace was deleted.
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
    /// Replace the current destination file.
    Replace,
}

/// Fields that guard replacement of a move or copy destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DestinationGuard {
    /// Whether an existing destination file may be replaced.
    #[serde(default)]
    pub behavior: DestinationBehavior,
    /// With `replace` behavior, the destination inode required by the request.
    #[serde(
        rename = "expected_destination_inode_id",
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    pub expected_inode_id: Option<InodeId>,
    /// With `replace` behavior and an inode guard, the required content revision.
    #[serde(
        rename = "expected_destination_revision_no",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_revision_no: Option<RevisionNo>,
}

/// Field-name family used when validating replacement guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardFields {
    /// Fields on a file put operation.
    Put,
    /// Destination fields on a move or copy operation.
    Destination,
}

impl GuardFields {
    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Put => ("expected_revision_no", "expected_inode_id"),
            Self::Destination => (
                "expected_destination_revision_no",
                "expected_destination_inode_id",
            ),
        }
    }
}

/// Validated file state required before replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedFileState {
    /// Destination inode required by the request.
    pub inode_id: InodeId,
    /// Destination content revision required by the request.
    pub revision_no: Option<RevisionNo>,
}

/// Why a destination guard is not a valid request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DestinationGuardError {
    /// A create-only operation supplied a replacement guard.
    #[error("write guards require replace behavior")]
    GuardsRequireReplace,
    /// A revision guard did not name the inode whose revision it checks.
    #[error(
        "{revision_field} asserts a revision of a specific file; pair it with {inode_field} so the assertion names which file"
    )]
    RevisionRequiresInode {
        /// Revision field supplied by the request.
        revision_field: &'static str,
        /// Inode field required beside the revision.
        inode_field: &'static str,
    },
}

impl DestinationGuard {
    /// Validates the guard and returns the required destination state.
    pub fn resolve(
        &self,
        fields: GuardFields,
    ) -> Result<Option<ExpectedFileState>, DestinationGuardError> {
        if self.behavior == DestinationBehavior::NoReplace
            && !matches!(
                (self.expected_inode_id, self.expected_revision_no),
                (None, None)
            )
        {
            return Err(DestinationGuardError::GuardsRequireReplace);
        }
        let Some(inode_id) = self.expected_inode_id else {
            if self.expected_revision_no.is_some() {
                let (revision_field, inode_field) = fields.names();
                return Err(DestinationGuardError::RevisionRequiresInode {
                    revision_field,
                    inode_field,
                });
            }
            return Ok(None);
        };
        Ok(Some(ExpectedFileState {
            inode_id,
            revision_no: self.expected_revision_no,
        }))
    }
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
/// Unknown fields are rejected, and fieldless variants require empty objects.
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
        /// Whether to create missing ancestor directories while requiring the final
        /// component to be new.
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
        /// With `replace` behavior, the request requires the path to contain this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        expected_inode_id: Option<InodeId>,
        /// With `replace` behavior and an inode guard, the request requires this content revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// Create a file with an unused name under an existing parent inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileByInode")
    )]
    PutFileByInode {
        /// Parent directory.
        #[serde(with = "crate::public_inode_id")]
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
        /// The inode that the path must still resolve to before deletion.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
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
        inode_id: InodeId,
        /// Binding generation required for the delete.
        expected_binding_generation: BindingGeneration,
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
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        guard: DestinationGuard,
    },
    /// Move an inode if its current binding matches.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMoveByInode"))]
    MoveByInode {
        /// Inode to move.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Binding generation required for the move.
        expected_binding_generation: BindingGeneration,
        /// Destination directory.
        #[serde(with = "crate::public_inode_id")]
        to_parent_inode_id: InodeId,
        /// New name.
        to_display_name: DisplayName,
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        guard: DestinationGuard,
    },
    /// Copy one file path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyPath"))]
    CopyPath {
        /// Absolute source path that must resolve to a visible file.
        from_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        to_path: AbsolutePath,
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        guard: DestinationGuard,
    },
    /// Restore the deletion identified by `inode_id` and `deletion_seq`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUndelete"))]
    Undelete {
        /// Deleted inode to make reachable again.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer tombstone generation.
        deletion_seq: ChangeSeq,
        /// The restore destination, or `None` to use the recorded binding.
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
        /// The attributes to write, replacing values for matching keys and leaving
        /// other keys unchanged.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// The attribute keys to remove, including duplicates that validation must reject.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remove: Vec<AttributeKey>,
        /// The inode that the path must still resolve to before the update.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        expected_inode_id: Option<InodeId>,
        /// The attribute revision that must still be current before the update.
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

/// A request to commit one or more filesystem operations atomically in order.
///
/// Unknown fields are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// Caller-supplied idempotency key for the whole request.
    pub commit_id: CommitId,
    /// Actor responsible for the commit, as supplied by the application.
    pub actor: crate::ActorRef,
    /// The caller annotation that forms part of the commit identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The proofs for new external content references in this request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ContentToken>,
    /// The non-empty ordered operations to commit atomically.
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
    pub inode_id: InodeId,
    /// Revision number within the file inode.
    pub revision_no: RevisionNo,
    /// Namespace sequence that created this revision.
    pub committed_seq: ChangeSeq,
    /// Commit ID for this revision.
    pub commit_id: CommitId,
    /// The commit time in Unix milliseconds; `committed_seq` defines commit order.
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
    /// The non-unique label recorded on the checkpoint.
    pub name: String,
    /// The checkpoint lifetime in milliseconds, or `None` for an explicit release only.
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
        /// The non-unique label recorded by the creator.
        name: String,
    },
    /// A fork target retaining its source basis for one fork attempt.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerFork"))]
    Fork {
        /// The target namespace whose existence retains this checkpoint.
        target_namespace_id: NamespaceId,
    },
    /// An application-created read view.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerSnapshot"))]
    Snapshot {
        /// A label that does not need to be unique.
        name: String,
    },
}

/// One checkpoint resource described by its durable record.
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
    /// The automatic release time in Unix milliseconds, or `None` until an explicit release.
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
#[cfg_attr(feature = "openapi", schema(as = Snapshot))]
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
    ///
    /// Returns `None` for another owner. A snapshot owner always carries the
    /// checkpoint's top-level `expires_at_ms`.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Option<Self> {
        let CheckpointOwnerSummary::Snapshot { name } = checkpoint.owner else {
            return None;
        };
        Some(Self {
            snapshot_id: checkpoint.checkpoint_id,
            namespace_id: checkpoint.namespace_id,
            name,
            head_seq: checkpoint.checkpoint_seq,
            created_at_ms: checkpoint.created_at_ms,
            expires_at_ms: checkpoint.expires_at_ms?,
        })
    }
}

/// One page of active checkpoint records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListCheckpointsResponse {
    /// Namespace the records belong to.
    pub namespace_id: NamespaceId,
    /// The active records in ascending checkpoint ID order.
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
    /// Another publisher updated the root before this call could reference its manifest.
    RootAdvanced,
}

/// The metadata root state after one WAL flush.
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

/// Optional overrides for one garbage-collection pass.
///
/// Unknown fields are rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// The minimum object age for deletion in milliseconds, which must meet the
    /// server's advertised safety floor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_window_ms: Option<u64>,
    /// The maximum objects this pass may inspect, or `None` to run to completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// The opaque `next_cursor` returned by an earlier pass for the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// The candidates inspected but not deleted by one garbage-collection pass.
///
/// Every field is present and contributes to [`GcResponse::retained_candidates`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// Candidates found reachable during the final check before deletion.
    pub referenced: u64,
    /// Unreachable candidates younger than the grace window by their provider timestamps.
    pub within_grace_window: u64,
    /// Unreachable candidates without provider timestamps.
    pub no_provider_timestamp: u64,
    /// Unreachable candidates without a reference manifest old enough to cover the grace window.
    pub no_reference_manifest: u64,
    /// Candidates retained because root resolution failed.
    pub degraded_roots: u64,
    /// Unrecognized keys retained from object families scanned by garbage collection.
    pub unrecognized_key: u64,
    /// Checkpoint records that could not be safely released or deleted.
    pub checkpoint_not_releasable: u64,
    /// Upload sessions still protected by a lease or grace window.
    pub upload_session_window: u64,
    /// Upload sessions whose deletion safety could not be determined.
    pub upload_session_undecided: u64,
    /// Completed sessions retained because their reference scan exceeded `max_objects`.
    pub content_scan_deferred: u64,
}

/// Object counts deleted by one garbage-collection pass, grouped by family.
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
    /// Unreferenced content objects deleted after their completed sessions passed the
    /// reclamation grace period.
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

/// Checkpoint record counts released by one garbage-collection pass, grouped by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleasedCheckpointCounts {
    /// Fork-owned records released because their target namespaces are gone.
    pub fork: u64,
    /// User-owned records released after expiry or terminal namespace deletion.
    pub expired: u64,
    /// Active records released because their basis manifests are gone.
    pub missing_basis: u64,
    /// Snapshot-owned records released after expiry or terminal namespace deletion.
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

/// The result of one mark-and-sweep garbage-collection pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// Namespace the pass ran against.
    pub namespace_id: NamespaceId,
    /// Objects the pass deleted, split by object family.
    pub deleted: DeletedObjectCounts,
    /// The checkpoint records released by the pass, grouped by reason.
    pub released_checkpoints: ReleasedCheckpointCounts,
    /// The number of candidates retained at deletion time.
    pub retained_candidates: u64,
    /// `retained_candidates` grouped by reason.
    pub retained: RetainedCandidates,
    /// True when ambiguous roots suppressed manifest/segment deletion.
    pub retention_degraded: bool,
    /// Whether `max_objects` prevented a complete reference scan for content reclamation.
    pub content_reclamation_deferred: bool,
    /// Whether the pass reached `max_objects` before completion.
    pub budget_exhausted: bool,
    /// The opaque resume token for remaining candidates in the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// The earliest known reclamation time for an upload session inspected by this
    /// pass, in Unix milliseconds.
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

/// The reason one candidate was retained and the corresponding [`RetainedCandidates`] field.
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

/// An option-free request that selects retention-floor advancement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct AdvanceRetentionRequest {}

/// Result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// Namespace whose retention floor was advanced.
    pub namespace_id: NamespaceId,
    /// New minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// One maintenance job for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunMaintenanceRequest {
    /// Runs WAL flushing and one bounded metadata reorganization step.
    Metadata(MetadataMaintenanceRequest),
    /// Runs one full metadata compaction.
    MetadataCompaction(MetadataCompactionRequest),
    /// Runs one bounded mark-and-sweep garbage-collection pass.
    Gc(GcRequest),
    /// Advances the retention floor to the flushed manifest head.
    Retention(AdvanceRetentionRequest),
}

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataMaintenanceRequest {
    /// The WAL-tail threshold for flushing, or `None` for the server default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_tail_segments: Option<u64>,
}

/// An option-free request that selects one full metadata compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataCompactionRequest {}

/// What the WAL-flush part of a maintenance pass did.
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
    /// The step did not update a root that already referenced another manifest.
    AlreadyPublished {
        /// Sequence this step attempted to flush through.
        attempted_seq: ChangeSeq,
        /// Manifest the root currently references.
        current_manifest_no: ManifestNo,
    },
    /// Concurrent updates prevented every publication attempt.
    RetriesExhausted {
        /// Head sequence observed before the step ran.
        observed_head_seq: ChangeSeq,
    },
}

/// The outcome of the metadata-reorganization part of a maintenance pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReorganizeStepOutcome {
    /// No family group had enough delta runs to merge.
    NotNeeded,
    /// One family group was merged and a manifest published.
    UnitPublished,
    /// A family group needs a streaming compaction. Run the `metadata_compaction` job.
    CompactionRequired,
    /// Another publisher updated the metadata root before this step could reference its manifest.
    RootAdvanced,
}

/// The result of one maintenance job. The `kind` matches the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunMaintenanceResponse {
    /// Result of WAL flushing and one bounded metadata reorganization step.
    Metadata(MetadataMaintenanceResponse),
    /// Result of one full metadata compaction.
    MetadataCompaction(MetadataCompactionResponse),
    /// Result of one bounded mark-and-sweep garbage-collection pass.
    Gc(GcResponse),
    /// Result of advancing the retention floor.
    Retention(AdvanceRetentionResponse),
}

/// What one metadata-upkeep action did, part by part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceResponse {
    /// Namespace maintained by this run.
    pub namespace_id: NamespaceId,
    /// What the WAL flush did.
    pub wal_flush: WalFlushStepOutcome,
    /// What the reorganization unit did.
    pub reorganize: ReorganizeStepOutcome,
}

/// What one metadata compaction run did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataCompactionResponse {
    /// Namespace compacted by this run.
    pub namespace_id: NamespaceId,
    /// The compaction outcome.
    pub compaction: MetadataCompactionOutcome,
}

/// The outcome of one metadata compaction run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MetadataCompactionOutcome {
    /// No family group has outgrown a bounded reorganization step and nothing was published.
    NotNeeded,
    /// The planner chose a bounded merge and this run published it; no full compaction was needed.
    BoundedMergePublished,
    /// The rebuilt group replaced its snapshot in a published manifest.
    Published {
        /// Manifest published by the compaction.
        manifest_no: ManifestNo,
        /// Rows read by the compaction.
        rows_read: u64,
        /// Rows written by the compaction.
        rows_written: u64,
        /// Input bytes read by the compaction.
        input_bytes: u64,
        /// Output bytes written by the compaction.
        output_bytes: u64,
        /// Output segments written by the compaction.
        output_segments: u64,
    },
    /// The run was cancelled; the manifest did not move.
    Cancelled,
    /// A run the job read changed under it; nothing was published.
    Abandoned,
    /// The job lost its lease; nothing was published.
    Fenced,
    /// Every publication attempt lost the root race; nothing was published.
    Superseded,
}

/// An empty request for one store contract probe.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StoreProbeRequest {}

/// The ordered results from one store contract probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeResponse {
    /// The server-generated label for this probe run and its objects.
    pub run_id: String,
    /// The check results in execution order.
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
    /// The expected and actual behavior for a failed check.
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
    /// The store does not support this optional capability.
    Unsupported,
    /// The store violated the contract or the operation failed.
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
            guard: crate::DestinationGuard {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_7",
                "expected_destination_revision_no": 3
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            guard: crate::DestinationGuard {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_7",
                "expected_destination_revision_no": 3
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
                guard: crate::DestinationGuard {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
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
                guard: crate::DestinationGuard {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
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
                "behavior": "replace",
                "expected_inode_id": "ino_7"
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
                    "expected_inode_id": "ino_7",
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
    fn maintenance_outcomes_use_the_outcome_tag() {
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
        assert_eq!(
            serde_json::to_value(RunMaintenanceResponse::MetadataCompaction(
                MetadataCompactionResponse {
                    namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                    compaction: MetadataCompactionOutcome::Published {
                        manifest_no: ManifestNo(7),
                        rows_read: 11,
                        rows_written: 9,
                        input_bytes: 120,
                        output_bytes: 80,
                        output_segments: 2,
                    },
                },
            ))
            .expect("serialize metadata compaction response"),
            serde_json::json!({
                "kind": "metadata_compaction",
                "namespace_id": "demo",
                "compaction": {
                    "outcome": "published",
                    "manifest_no": 7,
                    "rows_read": 11,
                    "rows_written": 9,
                    "input_bytes": 120,
                    "output_bytes": 80,
                    "output_segments": 2
                }
            })
        );
    }

    #[test]
    fn run_maintenance_requests_are_strict_and_round_trip() {
        let cases = [
            (
                serde_json::json!({"kind": "metadata"}),
                Some(RunMaintenanceRequest::Metadata(
                    MetadataMaintenanceRequest::default(),
                )),
            ),
            (
                serde_json::json!({"kind": "metadata", "max_wal_tail_segments": 4}),
                Some(RunMaintenanceRequest::Metadata(
                    MetadataMaintenanceRequest {
                        max_wal_tail_segments: Some(4),
                    },
                )),
            ),
            (
                serde_json::json!({"kind": "metadata_compaction"}),
                Some(RunMaintenanceRequest::MetadataCompaction(
                    MetadataCompactionRequest {},
                )),
            ),
            (
                serde_json::json!({"kind": "gc"}),
                Some(RunMaintenanceRequest::Gc(GcRequest::default())),
            ),
            (
                serde_json::json!({
                    "kind": "gc",
                    "max_objects": 10_000,
                    "grace_window_ms": 600_000,
                    "cursor": "..."
                }),
                Some(RunMaintenanceRequest::Gc(GcRequest {
                    grace_window_ms: Some(600_000),
                    max_objects: Some(10_000),
                    cursor: Some("...".to_owned()),
                })),
            ),
            (
                serde_json::json!({"kind": "retention"}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {})),
            ),
            (serde_json::json!({}), None),
            (serde_json::json!({"kind": "nope"}), None),
            (serde_json::json!({"kind": "gc", "bogus": 1}), None),
            (serde_json::json!({"kind": "retention", "bogus": 1}), None),
            (
                serde_json::json!({"kind": "metadata_compaction", "bogus": 1}),
                None,
            ),
        ];

        for (body, expected) in cases {
            let decoded = serde_json::from_value::<RunMaintenanceRequest>(body.clone());
            match expected {
                Some(expected) => {
                    let decoded = decoded.expect("valid maintenance request should decode");
                    assert_eq!(decoded, expected);
                    assert_eq!(
                        serde_json::to_value(decoded)
                            .expect("maintenance request should serialize"),
                        body
                    );
                }
                None => assert!(
                    decoded.is_err(),
                    "invalid maintenance request decoded: {body}"
                ),
            }
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
