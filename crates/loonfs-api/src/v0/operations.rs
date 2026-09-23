//! Operation requests and responses for the v0 HTTP API.

use super::ContentToken;
use crate::{
    AbsolutePath, AccessGrants, AccessRevisionNo, ActorId, AttributeKey, AttributeValue,
    AttributesRevisionNo, BindingGeneration, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId,
    ManifestNo, NamespaceGeneration, NamespaceId, PinId, RevisionNo, WriterEpoch, WriterId,
};
use crate::{NamespaceAccess, PrincipalId, PrincipalScope};
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub feature: Option<String>,
    /// Human-readable error message.
    pub message: String,
    /// The invalid JSON Pointer, parameter name, CLI flag, or CLI argument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub param: Option<String>,
    /// The request correlation ID also sent in the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub committed_fingerprint: Option<String>,
    /// The index of the failed operation in the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub operation_index: Option<u32>,
    /// Zero-based position of the failed request precondition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub precondition_index: Option<u32>,
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_writer: Option<WriterId>,
    /// The Unix-millisecond time when the current writer acquired its epoch, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_acquired_at_ms: Option<u64>,
    /// Maximum writer sessions admitted by the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub max_writer_sessions: Option<usize>,
    /// Inode the failed precondition or operation targeted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub inode_id: Option<InodeId>,
    /// The request expected the path to contain this inode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_inode_id: Option<InodeId>,
    /// The path actually contained this inode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_inode_id: Option<InodeId>,
    /// Opaque binding token supplied by the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_binding_generation: Option<BindingGeneration>,
    /// Current binding token; absent for the root, which has no binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_binding_generation: Option<BindingGeneration>,
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
    pub expected_attributes_revision_no: Option<AttributesRevisionNo>,
    /// Attribute revision that is actually current for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_attributes_revision_no: Option<AttributesRevisionNo>,
    /// Access revision the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_access_revision_no: Option<AccessRevisionNo>,
    /// Access revision that is actually current for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_access_revision_no: Option<AccessRevisionNo>,
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
    /// The head sequence required by the request.
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
    /// The access mode, fixed for the namespace's life. Defaults to
    /// unrestricted.
    #[serde(default = "NamespaceAccess::unrestricted")]
    pub access: NamespaceAccess,
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
    /// Fork from this live snapshot instead of the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
}

/// Current state for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Namespace {
    /// The namespace's access mode.
    pub access: NamespaceAccessMode,
    /// Namespace ID.
    pub namespace_id: NamespaceId,
    /// Which generation of its id this namespace is. Recreating a deleted id increments it.
    pub generation: NamespaceGeneration,
    /// Time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// Actor that created the namespace, as supplied by the application.
    pub created_by: ActorId,
    /// Present only for a fork: the source it was forked from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// A namespace's access mode as reported, without its genesis grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceAccessMode {
    /// Every caller holding the deployment credential may do everything.
    Unrestricted {},
    /// Access rows govern operations in this identity domain.
    Acl {
        /// Identity domain the namespace's principal ids belong to.
        principal_scope: PrincipalScope,
    },
}

impl From<&NamespaceAccess> for NamespaceAccessMode {
    fn from(access: &NamespaceAccess) -> Self {
        match access {
            NamespaceAccess::Unrestricted {} => Self::Unrestricted {},
            NamespaceAccess::Acl {
                principal_scope, ..
            } => Self::Acl {
                principal_scope: principal_scope.clone(),
            },
        }
    }
}

/// The source a forked namespace started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceForkBasis {
    /// Namespace the fork was taken from.
    pub source_namespace_id: NamespaceId,
    /// Source sequence the fork captured.
    pub source_head_seq: ChangeSeq,
}

/// Namespace state and storage details used by maintenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceDiagnostics {
    /// Namespace ID.
    pub namespace_id: NamespaceId,
    /// Which generation of its id this namespace is. Recreating a deleted id increments it.
    pub generation: NamespaceGeneration,
    /// Time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// Actor that created the namespace, as supplied by the application.
    pub created_by: ActorId,
    /// Present only for a fork: the source it was forked from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
    /// The namespace's current manifest number.
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

/// Requirements for replacing a move or copy destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DestinationPrecondition {
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_inode_id: Option<InodeId>,
    /// With `replace` behavior and an inode precondition, the required content revision.
    #[serde(
        rename = "expected_destination_revision_no",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_revision_no: Option<RevisionNo>,
}

/// Field-name family used when validating replacement preconditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionFields {
    /// Fields on a file put operation.
    Put,
    /// Destination fields on a move or copy operation.
    Destination,
}

impl PreconditionFields {
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

/// Why a destination precondition is not a valid request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DestinationPreconditionError {
    /// A create-only operation supplied a replacement precondition.
    #[error("destination preconditions require replace behavior")]
    PreconditionsRequireReplace {
        /// First supplied expectation field, with inode before revision.
        field: &'static str,
    },
    /// A revision precondition did not name the inode whose revision it checks.
    #[error(
        "`{revision_field}` names the revision of one inode; pair it with `{inode_field}` so the precondition names which inode"
    )]
    RevisionRequiresInode {
        /// Revision field supplied by the request.
        revision_field: &'static str,
        /// Inode field required beside the revision.
        inode_field: &'static str,
    },
}

impl DestinationPrecondition {
    /// Validates the precondition and returns the required destination state.
    pub fn resolve(
        &self,
        fields: PreconditionFields,
    ) -> Result<Option<ExpectedFileState>, DestinationPreconditionError> {
        if self.behavior == DestinationBehavior::NoReplace
            && !matches!(
                (self.expected_inode_id, self.expected_revision_no),
                (None, None)
            )
        {
            let (revision_field, inode_field) = fields.names();
            return Err(DestinationPreconditionError::PreconditionsRequireReplace {
                field: if self.expected_inode_id.is_some() {
                    inode_field
                } else {
                    revision_field
                },
            });
        }
        let Some(inode_id) = self.expected_inode_id else {
            if self.expected_revision_no.is_some() {
                let (revision_field, inode_field) = fields.names();
                return Err(DestinationPreconditionError::RevisionRequiresInode {
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

/// Rejects an attribute revision precondition without an inode precondition.
pub fn validate_attributes_precondition(
    expected_inode_id: Option<InodeId>,
    expected_attributes_revision_no: Option<AttributesRevisionNo>,
) -> Result<(), DestinationPreconditionError> {
    validate_revision_precondition(
        expected_inode_id,
        expected_attributes_revision_no.is_some(),
        "expected_attributes_revision_no",
    )
}

/// Rejects an access revision precondition without an inode precondition.
pub fn validate_access_precondition(
    expected_inode_id: Option<InodeId>,
    expected_access_revision_no: Option<AccessRevisionNo>,
) -> Result<(), DestinationPreconditionError> {
    validate_revision_precondition(
        expected_inode_id,
        expected_access_revision_no.is_some(),
        "expected_access_revision_no",
    )
}

fn validate_revision_precondition(
    expected_inode_id: Option<InodeId>,
    has_revision: bool,
    revision_field: &'static str,
) -> Result<(), DestinationPreconditionError> {
    if has_revision && expected_inode_id.is_none() {
        return Err(DestinationPreconditionError::RevisionRequiresInode {
            revision_field,
            inode_field: "expected_inode_id",
        });
    }
    Ok(())
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
    /// Create or replace one file from uploaded or inline content.
    /// Requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationPutFile"))]
    PutFile {
        /// Absolute destination path; missing ancestors are created automatically.
        path: AbsolutePath,
        /// Uploaded content covered by a token; mutually exclusive with `inline_content`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// Complete file bytes as base64; mutually exclusive with `content_ref`.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
        /// Whether an existing file may receive a new revision instead of causing a conflict.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// With `replace` behavior, the request requires the path to contain this inode.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// With `replace` behavior and an inode precondition, the request requires this content revision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// Create a file with an unused name under an existing parent inode.
    /// Requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateFileByInode")
    )]
    CreateFileByInode {
        /// Parent directory.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// New file name.
        display_name: DisplayName,
        /// Uploaded content covered by a token; mutually exclusive with `inline_content`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// Complete file bytes as base64; mutually exclusive with `content_ref`.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
    },
    /// Append a revision to a file inode if its current revision matches.
    /// Requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileRevisionByInode")
    )]
    PutFileRevisionByInode {
        /// File to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Uploaded content covered by a token; mutually exclusive with `inline_content`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// Complete file bytes as base64; mutually exclusive with `content_ref`.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
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
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
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
        source_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        destination_path: AbsolutePath,
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
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
        destination_parent_inode_id: InodeId,
        /// New name.
        destination_display_name: DisplayName,
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// Copy one file path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyPath"))]
    CopyPath {
        /// Absolute source path that must resolve to a visible file.
        source_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        destination_path: AbsolutePath,
        /// Replacement behavior and optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
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
        destination_path: Option<AbsolutePath>,
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
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// With an inode precondition, the attribute revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    /// Replace the access row of the inode one path resolves to. The root
    /// path is a valid target.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUpdateAccess"))]
    UpdateAccess {
        /// Absolute path that must resolve to a visible file or directory.
        path: AbsolutePath,
        /// Whether the directory stops inheritance from its ancestors.
        boundary: bool,
        /// The inode's complete direct grants after this update.
        grants: AccessGrants,
        /// The inode that the path must still resolve to before the update.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// With an inode precondition, the access revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
}

impl FilesystemOperation {
    /// Returns the content written by this operation, if any.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::PutFile { content_ref, .. }
            | Self::CreateFileByInode { content_ref, .. }
            | Self::PutFileRevisionByInode { content_ref, .. } => content_ref.as_ref(),
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::UpdateAttributes { .. }
            | Self::UpdateAccess { .. } => None,
        }
    }
}

/// Admission conditions checked against the candidate's pre-state before its operations.
/// The pre-state head sequence is the last admitted commit's sequence in the batch,
/// or the batch's base head sequence when no earlier candidate was admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitPrecondition {
    /// Requires the pre-state head sequence to equal `expected_head_seq`.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionNamespaceHead"))]
    NamespaceHead {
        /// Sequence observed when the caller read its inputs.
        expected_head_seq: ChangeSeq,
    },
    /// Requires a visible inode with the content revision the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionFileRevision"))]
    FileRevision {
        /// Inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Content revision observed by the caller.
        expected_revision_no: RevisionNo,
    },
    /// Requires the path to retain the binding the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathBinding"))]
    PathBinding {
        /// Absolute path to check, including the root.
        path: AbsolutePath,
        /// Inode required at the path.
        #[serde(with = "crate::public_inode_id")]
        expected_inode_id: InodeId,
        /// Detects moves away and back.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_binding_generation: Option<BindingGeneration>,
    },
    /// Requires a visible inode with the attribute revision the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAttributesRevision")
    )]
    AttributesRevision {
        /// Inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Attribute revision observed by the caller.
        expected_attributes_revision_no: AttributesRevisionNo,
    },
    /// Requires a visible inode with the access revision the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAccessRevision")
    )]
    AccessRevision {
        /// Inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Access revision observed by the caller.
        expected_access_revision_no: AccessRevisionNo,
    },
    /// Requires no visible entry at the full path.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathAbsence"))]
    PathAbsence {
        /// Absolute path to check, including the root.
        path: AbsolutePath,
    },
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
    /// The caller annotation that forms part of the commit identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
    /// The proofs for new external content references in this request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ContentToken>,
    /// Ordered admission conditions evaluated before any operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    /// The non-empty ordered operations to commit atomically.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// Sets the admission conditions in caller order.
    pub fn preconditions(mut self, preconditions: Vec<CommitPrecondition>) -> Self {
        self.preconditions = preconditions;
        self
    }

    /// A request carrying exactly one operation.
    pub fn single(
        commit_id: CommitId,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            message,
            content_tokens: Vec::new(),
            preconditions: Vec::new(),
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
    pub committed_by: crate::ActorId,
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// Request to create a durable checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateCheckpointRequest {
    /// The non-unique label recorded on the checkpoint.
    pub name: String,
    /// The checkpoint lifetime in milliseconds, or `None` for an explicit deletion only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
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

/// Identifies the checkpoint record that was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteCheckpointResponse {
    /// Namespace the checkpoint belonged to.
    pub namespace_id: NamespaceId,
    /// Deleted checkpoint record.
    pub checkpoint_id: PinId,
}

/// The owner of a checkpoint record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwnerSummary {
    /// An operator-created pin, deleted by id or by its own expiry.
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
    /// Durable checkpoint id used to address the checkpoint for deletion.
    pub checkpoint_id: PinId,
    /// Who owns the checkpoint, including the label carried by a user pin.
    pub owner: CheckpointOwnerSummary,
    /// Time the checkpoint record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// Expiry in Unix milliseconds; collection waits one further grace window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expires_at_ms: Option<u64>,
    /// Namespace sequence captured by the checkpoint.
    pub captured_seq: ChangeSeq,
    /// Manifest pinned by the checkpoint.
    pub manifest_no: ManifestNo,
}

/// A live snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(as = Snapshot))]
pub struct SnapshotSummary {
    /// Snapshot id.
    pub snapshot_id: PinId,
    /// Namespace whose state the snapshot captured.
    pub namespace_id: NamespaceId,
    /// Snapshot label.
    pub name: String,
    /// Namespace sequence captured by the snapshot.
    pub captured_seq: ChangeSeq,
    /// Time the snapshot record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// When the snapshot expires, in Unix milliseconds.
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
            captured_seq: checkpoint.captured_seq,
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// Identifies the snapshot record that was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteSnapshotResponse {
    /// Namespace the snapshot belonged to.
    pub namespace_id: NamespaceId,
    /// Deleted snapshot record.
    pub snapshot_id: PinId,
}

/// How one WAL flush satisfied its goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlushWalOutcome {
    /// The current manifest already covered the WAL tail; nothing was published.
    AlreadyCurrent,
    /// This call published the next current manifest.
    Published,
    /// Another publisher changed the current manifest before this call could publish.
    ManifestAdvanced,
}

/// The current manifest state after one WAL flush.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FlushWalResponse {
    /// Namespace whose WAL tail was flushed.
    pub namespace_id: NamespaceId,
    /// Head sequence the flush attempted to cover.
    pub target_head_seq: ChangeSeq,
    /// Current manifest number after the operation.
    pub manifest_no: ManifestNo,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Whether this call published the current manifest.
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub grace_window_ms: Option<u64>,
}

/// The candidates inspected but not deleted by one garbage-collection pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// Candidates protected by current references or manifest discovery.
    pub referenced: u64,
    /// Unreachable candidates younger than the grace window by their provider timestamps.
    pub within_grace_window: u64,
    /// Unreachable candidates without provider timestamps.
    pub no_provider_timestamp: u64,
    /// Unrecognized keys retained from object families scanned by garbage collection.
    pub unrecognized_key: u64,
    /// Checkpoint records whose owner or grace window prevents deletion.
    pub checkpoint_not_deletable: u64,
    /// Upload sessions still protected by a lease or grace window.
    pub upload_session_window: u64,
    /// Upload sessions whose deletion safety could not be determined.
    pub upload_session_undecided: u64,
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
    /// Upload-session control objects deleted after the reap window.
    pub upload_sessions: u64,
    /// Content reclaimed through completed upload sessions.
    pub content_objects: u64,
    /// Successful deletion attempts under a retired namespace owner prefix.
    pub retired_content_objects: u64,
}

impl DeletedObjectCounts {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            wal_segments,
            metadata_segments,
            manifests,
            upload_sessions,
            content_objects,
            retired_content_objects,
        } = other;
        self.wal_segments += wal_segments;
        self.metadata_segments += metadata_segments;
        self.manifests += manifests;
        self.upload_sessions += upload_sessions;
        self.content_objects += content_objects;
        self.retired_content_objects += retired_content_objects;
    }
}

/// Checkpoint record counts deleted by one garbage-collection pass, grouped by owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedCheckpointsByOwner {
    /// Fork-owned records deleted because their target namespaces are gone.
    pub fork: u64,
    /// User-owned records deleted after expiry or namespace deletion.
    pub expired: u64,
    /// Snapshot-owned records deleted after expiry or namespace deletion.
    pub snapshot: u64,
    /// Retired records deleted after their generations are reclaimed.
    pub retired: u64,
}

impl DeletedCheckpointsByOwner {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            fork,
            expired,
            snapshot,
            retired,
        } = other;
        self.fork += fork;
        self.expired += expired;
        self.snapshot += snapshot;
        self.retired += retired;
    }
}

/// The result of one stateless garbage-collection call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// Namespace the pass ran against.
    pub namespace_id: NamespaceId,
    /// Objects the pass deleted, split by object family.
    pub deleted: DeletedObjectCounts,
    /// The checkpoint records deleted by the pass, grouped by owner.
    pub deleted_checkpoints_by_owner: DeletedCheckpointsByOwner,
    /// Candidates retained at deletion time, grouped by reason.
    pub retained: RetainedCandidates,
    /// The earliest future retirement deadline, pin deletion time, or upload cleanup time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_reclamation_at_ms: Option<u64>,
    /// The current tombstone's deletion time plus the configured retirement grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub reclaim_after_ms: Option<u64>,
}

impl GcResponse {
    /// An empty report for `namespace_id`, before any candidate is examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted: DeletedObjectCounts::default(),
            deleted_checkpoints_by_owner: DeletedCheckpointsByOwner::default(),
            retained: RetainedCandidates::default(),
            next_reclamation_at_ms: None,
            reclaim_after_ms: None,
        }
    }

    /// Records one retained candidate under the reason that spared it.
    pub fn retain(&mut self, reason: RetainedReason) {
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
    /// Counts into [`RetainedCandidates::unrecognized_key`].
    UnrecognizedKey,
    /// Counts into [`RetainedCandidates::checkpoint_not_deletable`].
    CheckpointNotDeletable,
    /// Counts into [`RetainedCandidates::upload_session_window`].
    UploadSessionWindow,
    /// Counts into [`RetainedCandidates::upload_session_undecided`].
    UploadSessionUndecided,
}

impl RetainedReason {
    fn counter(self, retained: &mut RetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::WithinGraceWindow => &mut retained.within_grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::CheckpointNotDeletable => &mut retained.checkpoint_not_deletable,
            Self::UploadSessionWindow => &mut retained.upload_session_window,
            Self::UploadSessionUndecided => &mut retained.upload_session_undecided,
        }
    }
}

impl RetainedCandidates {
    /// Counts all candidates retained by the pass.
    pub fn total(&self) -> u64 {
        self.by_reason().into_iter().map(|(_, count)| count).sum()
    }

    /// Returns every reason and count in a fixed order.
    pub(crate) fn by_reason(&self) -> [(&'static str, u64); 7] {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            checkpoint_not_deletable,
            upload_session_window,
            upload_session_undecided,
        } = *self;
        [
            ("referenced", referenced),
            ("within_grace_window", within_grace_window),
            ("no_provider_timestamp", no_provider_timestamp),
            ("unrecognized_key", unrecognized_key),
            ("checkpoint_not_deletable", checkpoint_not_deletable),
            ("upload_session_window", upload_session_window),
            ("upload_session_undecided", upload_session_undecided),
        ]
    }

    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            referenced,
            within_grace_window,
            no_provider_timestamp,
            unrecognized_key,
            checkpoint_not_deletable,
            upload_session_window,
            upload_session_undecided,
        } = other;
        self.referenced += referenced;
        self.within_grace_window += within_grace_window;
        self.no_provider_timestamp += no_provider_timestamp;
        self.unrecognized_key += unrecognized_key;
        self.checkpoint_not_deletable += checkpoint_not_deletable;
        self.upload_session_window += upload_session_window;
        self.upload_session_undecided += upload_session_undecided;
    }

    /// The reason with the highest count, and that count. `None` when
    /// nothing was retained. Ties go to the first reason in the fixed table
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
    /// Collects aged, unreferenced objects.
    Gc(GcRequest),
    /// Advances the retention floor to the flushed manifest head.
    Retention(AdvanceRetentionRequest),
    /// Restores a root administrator.
    RecoverAdministrator(RecoverAdministratorRequest),
}

/// Grants `admin` on the root row to one principal, keeping every other
/// root grant, through a commit that checks no subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct RecoverAdministratorRequest {
    /// Principal receiving administrator rights.
    pub principal_id: PrincipalId,
}

/// The committed administrator recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RecoverAdministratorResponse {
    /// Namespace whose root grants changed.
    pub namespace_id: NamespaceId,
    /// Recovery commit id.
    pub commit_id: CommitId,
    /// Sequence assigned to the recovery commit.
    pub committed_seq: ChangeSeq,
    /// Root access revision after recovery.
    pub access_revision_no: AccessRevisionNo,
}

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataMaintenanceRequest {
    /// The WAL-tail threshold for flushing, or `None` for the server default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
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
    /// The step flushed the WAL tail and published the next current manifest.
    Flushed {
        /// Sequence covered by the published manifest.
        manifest_head_seq: ChangeSeq,
    },
    /// The current manifest already covered the captured WAL tail; this step published no manifest.
    AlreadyPublished {
        /// Sequence this step attempted to flush through.
        attempted_seq: ChangeSeq,
        /// The namespace's current manifest number.
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
    #[cfg_attr(feature = "openapi", schema(title = "ReorganizeStepOutcomeNotNeeded"))]
    NotNeeded {},
    /// One family group was merged and a manifest published.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ReorganizeStepOutcomeUnitPublished")
    )]
    UnitPublished {},
    /// A family group needs a streaming compaction. Run the `metadata_compaction` job.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ReorganizeStepOutcomeCompactionRequired")
    )]
    CompactionRequired {},
    /// Another publisher changed the current manifest before this step could publish.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ReorganizeStepOutcomeManifestAdvanced")
    )]
    ManifestAdvanced {},
    /// A newer runtime holds the compactor epoch.
    #[cfg_attr(feature = "openapi", schema(title = "ReorganizeStepOutcomeFenced"))]
    Fenced {},
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
    /// Counts and deadlines from one collection call.
    Gc(GcResponse),
    /// Result of advancing the retention floor.
    Retention(AdvanceRetentionResponse),
    /// The committed administrator recovery.
    RecoverAdministrator(RecoverAdministratorResponse),
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
    /// No eligible family group was available and nothing was published.
    NotNeeded,
    /// The selected window fit a bounded step and this run published it.
    BoundedMergePublished,
    /// The selected run window was replaced in a published manifest.
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
    /// Inputs changed, time ran out, or publication retries were exhausted.
    Abandoned,
    /// Another process claimed the namespace compactor role; nothing was published.
    Fenced,
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
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
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
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::NamespaceGeneration(1),
            crate::ContentId::generate(),
            b"hello",
        );
        let revision = FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(7),
            commit_id: CommitId::parse("c_revision_owner").expect("commit id"),
            committed_at_ms: 1_752_624_000_000,
            committed_by: crate::ActorId::loonfs(),
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
                "committed_by": "loonfs",
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
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::NamespaceGeneration(1),
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id"),
            b"hello",
        )
    }

    #[test]
    fn namespace_wire_shape_has_only_core_state() {
        let namespace = Namespace {
            access: NamespaceAccessMode::Unrestricted {},
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            generation: crate::NamespaceGeneration(2),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
        };
        assert_eq!(
            serde_json::to_value(namespace).expect("serialize namespace"),
            serde_json::json!({
                "namespace_id": "demo",
                "generation": 2,
                "access": {"kind": "unrestricted"},
                "created_at_ms": 1000,
                "created_by": "test",
                "head_seq": 11,
                "retention_floor_seq": 4
            })
        );
    }

    #[test]
    fn namespace_diagnostics_wire_shape_keeps_storage_fields() {
        let diagnostics = NamespaceDiagnostics {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            generation: crate::NamespaceGeneration(2),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
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
                "generation": 2,
                "created_at_ms": 1000,
                "created_by": "test",
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
            source_path: path("/docs/a.txt"),
            destination_path: path("/docs/b.txt"),
            precondition: crate::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "source_path": "/docs/a.txt",
                "destination_path": "/docs/b.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_7",
                "expected_destination_revision_no": 3
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            source_path: path("/docs/a.txt"),
            destination_path: path("/docs/b.txt"),
            precondition: crate::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "source_path": "/docs/a.txt",
                "destination_path": "/docs/b.txt",
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
            expected_attributes_revision_no: Some(AttributesRevisionNo(3)),
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
    fn update_attributes_omits_empty_collections_and_absent_preconditions() {
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
        .expect("remove-only op defaults the set map and both preconditions");
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
                "owner_namespace_id": "demo",
                "owner_generation": 1,
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
            "source_path": "/docs/a.txt",
            "destination_path": "/docs/b.txt"
        }))
        .expect("move op defaults behavior");
        assert_eq!(
            move_path,
            FilesystemOperation::MovePath {
                source_path: path("/docs/a.txt"),
                destination_path: path("/docs/b.txt"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            }
        );

        let copy_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "copy_path",
            "source_path": "/docs/a.txt",
            "destination_path": "/docs/b.txt"
        }))
        .expect("copy op defaults behavior");
        assert_eq!(
            copy_path,
            FilesystemOperation::CopyPath {
                source_path: path("/docs/a.txt"),
                destination_path: path("/docs/b.txt"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            }
        );

        let move_by_inode: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "move_by_inode",
            "inode_id": "ino_7",
            "expected_binding_generation": "aaaa",
            "destination_parent_inode_id": "ino_1",
            "destination_display_name": "b.txt"
        }))
        .expect("inode move defaults behavior");
        assert_eq!(
            move_by_inode,
            FilesystemOperation::MoveByInode {
                inode_id: InodeId(7),
                expected_binding_generation: BindingGeneration::parse("aaaa")
                    .expect("binding generation"),
                destination_parent_inode_id: InodeId(1),
                destination_display_name: DisplayName::parse("b.txt").expect("display name"),
                precondition: DestinationPrecondition::default(),
            }
        );
    }

    #[test]
    fn filesystem_operation_paths_keep_the_plain_string_wire_shape() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::NamespaceGeneration(1),
            ContentId::generate(),
            b"hello",
        );
        let cases = [
            (
                FilesystemOperation::PutFile {
                    path: path("/docs/a.txt"),
                    content_ref: Some(content_ref.clone()),
                    inline_content: None,
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
                    destination_path: Some(path("/docs/restored")),
                },
                serde_json::json!({
                    "kind": "undelete",
                    "inode_id": "ino_7",
                    "deletion_seq": 8,
                    "destination_path": "/docs/restored"
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
                "content_ref": ContentRef::blob_v1(crate::NamespaceId::parse("demo").expect("namespace id"), crate::NamespaceGeneration(1), ContentId::generate(), b"hello")
            }),
            serde_json::json!({"kind": "delete_path", "path": "relative"}),
            serde_json::json!({
                "kind": "move_path",
                "source_path": "relative",
                "destination_path": "/target"
            }),
            serde_json::json!({
                "kind": "copy_path",
                "source_path": "/source",
                "destination_path": "relative"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": "ino_7",
                "deletion_seq": 8,
                "destination_path": "relative"
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
                "deletion_seq": 8,
                "destination_path": "/docs/restored"
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
    fn path_preconditions_reject_ambiguous_shapes() {
        let missing = serde_json::from_value::<CommitPrecondition>(
            serde_json::json!({"kind": "path_binding", "path": "/docs/input"}),
        )
        .expect_err("binding requires an inode");
        assert!(
            missing.to_string().contains("expected_inode_id"),
            "{missing}"
        );
        serde_json::from_value::<CommitPrecondition>(serde_json::json!({
            "kind": "path_binding", "path": "/docs/input", "expected_inode_id": null
        }))
        .expect_err("a null inode is not an absence check");
        let error = serde_json::from_value::<CommitPrecondition>(serde_json::json!({
            "kind": "path_absence", "path": "/docs/input", "expected_inode_id": "ino_42"
        }))
        .expect_err("absence accepts only a path");
        assert!(
            error
                .to_string()
                .contains("unknown field `expected_inode_id`"),
            "{error}"
        );
    }

    #[test]
    fn a_misspelled_precondition_does_not_decode() {
        let put = |precondition: &str| {
            let mut operation = serde_json::json!({
                "kind": "put_file",
                "path": "/docs/a.txt",
                "content_ref": sample_content_ref(),
                "behavior": "replace",
                "expected_inode_id": "ino_7"
            });
            operation[precondition] = serde_json::json!(3);
            serde_json::json!({
                "commit_id": "with_preconditions-put",
                "operations": [operation]
            })
        };

        let spelled: CommitRequest = serde_json::from_value(put("expected_revision_no"))
            .expect("the precondition spelled correctly decodes");
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
                "commit_id": "bounded-revision-precondition",
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

        for (field, operation) in [
            (
                "path",
                serde_json::json!({
                    "kind": "undelete",
                    "inode_id": "ino_7",
                    "deletion_seq": 8,
                    "path": "/docs/restored"
                }),
            ),
            (
                "from_path",
                serde_json::json!({
                    "kind": "move_path",
                    "source_path": "/docs/a.txt",
                    "destination_path": "/docs/b.txt",
                    "from_path": "/docs/a.txt"
                }),
            ),
        ] {
            let mut body = valid();
            body["operations"] = serde_json::json!([operation]);
            let error = serde_json::from_value::<CommitRequest>(body)
                .expect_err("obsolete operation field must be rejected");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{field}`")),
                "{error}"
            );
        }
    }

    #[test]
    fn checkpoint_responses_use_one_checkpoint_wire_object() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let checkpoint = Checkpoint {
            namespace_id: namespace_id.clone(),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwnerSummary::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: Some(1_752_626_600_000),
            captured_seq: ChangeSeq(12),
            manifest_no: ManifestNo(9),
        };
        let checkpoint_json = serde_json::json!({
            "namespace_id": "demo",
            "checkpoint_id": "pin_00000000000000000001-0000000000000001",
            "owner": {"kind": "user", "name": "release"},
            "created_at_ms": 1_752_623_000_000_u64,
            "expires_at_ms": 1_752_626_600_000_u64,
            "captured_seq": 12,
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
            serde_json::to_value(DeleteCheckpointResponse {
                namespace_id,
                checkpoint_id: checkpoint.checkpoint_id,
            })
            .expect("serialize delete checkpoint response"),
            serde_json::json!({
                "namespace_id": "demo",
                "checkpoint_id": "pin_00000000000000000001-0000000000000001",
            }),
        );
    }

    #[test]
    fn optional_response_fields_are_omitted_and_default_when_absent() {
        let checkpoint_json = serde_json::to_value(Checkpoint {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwnerSummary::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: None,
            captured_seq: ChangeSeq(3),
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
        assert!(gc_json.get("reclaim_after_ms").is_none());
        let gc: GcResponse =
            serde_json::from_value(gc_json).expect("decode gc response without optional fields");
        assert_eq!(gc.next_reclamation_at_ms, None);
        assert_eq!(gc.reclaim_after_ms, None);
        let retired = GcResponse {
            reclaim_after_ms: Some(2_000_000),
            ..gc
        };
        let json = serde_json::to_value(&retired).expect("encode retirement");
        assert_eq!(json["reclaim_after_ms"], 2_000_000);
        assert_eq!(
            serde_json::from_value::<GcResponse>(json).expect("decode retirement"),
            retired
        );
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
            serde_json::to_value(ReorganizeStepOutcome::UnitPublished {})
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
                    "grace_window_ms": 600_000
                }),
                Some(RunMaintenanceRequest::Gc(GcRequest {
                    grace_window_ms: Some(600_000),
                })),
            ),
            (
                serde_json::json!({"kind": "retention"}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {})),
            ),
            (serde_json::json!({}), None),
            (serde_json::json!({"kind": "nope"}), None),
            (serde_json::json!({"kind": "gc", "bogus": 1}), None),
            (serde_json::json!({"kind": "gc", "max_objects": 1}), None),
            (serde_json::json!({"kind": "gc", "max_steps": 1}), None),
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
            "namespace_id": "demo",
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
    #[test]
    fn update_access_round_trips_and_requires_boundary_and_grants() {
        let operation = FilesystemOperation::UpdateAccess {
            path: AbsolutePath::parse("/docs/secret").expect("path"),
            boundary: true,
            grants: serde_json::from_value(serde_json::json!({"prn_ada": ["read", "write"]}))
                .expect("grants"),
            expected_inode_id: Some(InodeId(9)),
            expected_access_revision_no: Some(AccessRevisionNo(2)),
        };
        let json = serde_json::json!({
            "kind": "update_access",
            "path": "/docs/secret",
            "boundary": true,
            "grants": {"prn_ada": ["read", "write"]},
            "expected_inode_id": "ino_9",
            "expected_access_revision_no": 2
        });
        assert_eq!(serde_json::to_value(&operation).expect("serialize"), json);
        assert_eq!(
            serde_json::from_value::<FilesystemOperation>(json.clone()).expect("decode"),
            operation
        );
        for field in ["boundary", "grants"] {
            let mut missing = json.clone();
            missing.as_object_mut().expect("object").remove(field);
            assert!(
                serde_json::from_value::<FilesystemOperation>(missing).is_err(),
                "missing {field}"
            );
        }
        assert_eq!(
            serde_json::to_value(CommitPrecondition::AccessRevision {
                inode_id: InodeId(9),
                expected_access_revision_no: AccessRevisionNo(2),
            })
            .expect("serialize precondition"),
            serde_json::json!({"kind": "access_revision", "inode_id": "ino_9", "expected_access_revision_no": 2})
        );
    }
}
