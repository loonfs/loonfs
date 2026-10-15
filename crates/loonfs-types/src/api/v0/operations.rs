//! Operation requests and responses for the v0 HTTP API.

use super::ContentToken;
use crate::options::UndeleteDestination;
use crate::{
    AbsolutePath, AccessGrants, AccessRevisionNo, ActorId, AttributeKey, AttributeValue,
    AttributesRevisionNo, BindingVersion, ChangeSeq, CommitFingerprint, CommitId, ContentRef,
    DisplayName, InodeId, ManifestNo, NamespaceId, PinId, ProbeRunId, RevisionNo, WriterEpoch,
    WriterId,
};
use crate::{NamespaceAccess, NamespaceNaming, PrincipalId, PrincipalScope};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The JSON body that LoonFS APIs return with an HTTP error status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorResponse {
    /// The stable, machine-readable error code.
    pub code: String,
    /// The capability feature that isn't supported, when the code is `not_supported`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub feature: Option<String>,
    /// A human-readable description of the error.
    pub message: String,
    /// The invalid input, as a JSON Pointer, a parameter name, a CLI flag, or a CLI argument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub param: Option<String>,
    /// The request ID. The server also sends this ID in the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub request_id: Option<String>,
    /// Optional machine-readable context for the error code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub details: Option<Box<ErrorDetails>>,
}

/// The machine-readable identifiers and state that explain an error response.
///
/// Every field is optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDetails {
    /// The deleted namespace that caused the operation to fail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub namespace_id: Option<NamespaceId>,
    /// The idempotency key of the commit that the error is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub commit_id: Option<CommitId>,
    /// The sequence that `commit_id` was already committed at. The server includes it when a
    /// durable commit receipt records that commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub committed_seq: Option<ChangeSeq>,
    /// The fingerprint of the mutation that was committed under `commit_id`. This field is present
    /// exactly when `committed_seq` is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, nullable = false))]
    pub committed_fingerprint: Option<CommitFingerprint>,
    /// The index of the failed operation in the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub operation_index: Option<u32>,
    /// The zero-based position of the failed request precondition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub precondition_index: Option<u32>,
    /// The epoch of the failed writer session at the time another writer displaced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fenced_writer_epoch: Option<WriterEpoch>,
    /// The namespace's current writer epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_writer_epoch: Option<WriterEpoch>,
    /// The writer of the current epoch. It's present when the current manifest records a writer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_writer_id: Option<WriterId>,
    /// The time the current writer acquired its epoch, in Unix milliseconds. It's present when the
    /// current manifest records a writer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub active_acquired_at_ms: Option<u64>,
    /// The inode that the failed precondition or operation targeted.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub inode_id: Option<InodeId>,
    /// The inode that the request expected the path to contain.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_inode_id: Option<InodeId>,
    /// The inode that the path actually contained.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_inode_id: Option<InodeId>,
    /// The opaque binding token that the request supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_binding_version: Option<BindingVersion>,
    /// The current binding token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_binding_version: Option<BindingVersion>,
    /// The revision that the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_revision_no: Option<RevisionNo>,
    /// The revision that is actually current. It's absent when the inode has no current revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_revision_no: Option<RevisionNo>,
    /// The attribute revision that the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_attributes_revision_no: Option<AttributesRevisionNo>,
    /// The attribute revision that is actually current for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_attributes_revision_no: Option<AttributesRevisionNo>,
    /// The access revision that the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_access_revision_no: Option<AccessRevisionNo>,
    /// The access revision that is actually current for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_access_revision_no: Option<AccessRevisionNo>,
    /// The `after_seq` value from the change feed request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub after_seq: Option<ChangeSeq>,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_floor_seq: Option<ChangeSeq>,
    /// The deletion sequence that the undelete expected to be active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_deletion_seq: Option<ChangeSeq>,
    /// The deletion sequence that is actually active for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_deletion_seq: Option<ChangeSeq>,
    /// The head sequence that the request required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_head_seq: Option<ChangeSeq>,
    /// The namespace's actual head sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub actual_head_seq: Option<ChangeSeq>,
}

/// A request to create a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// The namespace's access mode, which can't be changed after the namespace is created. The
    /// default is unrestricted.
    #[serde(default = "NamespaceAccess::unrestricted")]
    pub access: NamespaceAccess,
    /// How the namespace compares sibling names. The mode can't be changed after the namespace is
    /// created, and it defaults to `case_insensitive`.
    #[serde(default)]
    pub naming: NamespaceNaming,
    /// The durable ID of the new namespace.
    pub namespace_id: NamespaceId,
}

/// A request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// The durable ID of the new namespace that the fork creates.
    pub new_namespace_id: NamespaceId,
    /// A live snapshot to fork from. Without it, the fork starts from the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
}

/// The current state of one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceMetadata {
    /// The namespace's access mode.
    pub access: NamespaceAccessMode,
    /// How the namespace compares sibling names.
    pub naming: NamespaceNaming,
    /// The namespace ID.
    pub namespace_id: NamespaceId,
    /// The time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The actor that created the namespace, as the application supplied it.
    pub created_by: ActorId,
    /// The namespace and sequence this namespace was forked from. Only a forked namespace has this
    /// field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// The sequence of the namespace's newest visible commit.
    pub head_seq: ChangeSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: ChangeSeq,
}

/// A namespace's access mode, without the grants that the namespace was created with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NamespaceAccessMode {
    /// Every caller with the deployment credential can perform every operation.
    Unrestricted {},
    /// Access rows control the operations that principals in this identity domain can perform.
    Acl {
        /// The identity domain that the namespace's principal IDs belong to.
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

/// The source namespace and sequence that a forked namespace started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceForkBasis {
    /// The namespace that the fork was taken from.
    pub source_namespace_id: NamespaceId,
    /// The source namespace's sequence that the fork captured.
    pub source_head_seq: ChangeSeq,
}

/// A namespace's state and the storage details that maintenance uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceDiagnostics {
    /// The namespace ID.
    pub namespace_id: NamespaceId,
    /// The time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The actor that created the namespace, as the application supplied it.
    pub created_by: ActorId,
    /// The namespace and sequence this namespace was forked from. Only a forked namespace has this
    /// field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// The sequence of the namespace's newest visible commit.
    pub head_seq: ChangeSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: ChangeSeq,
    /// The number of the namespace's current manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub current_manifest_no: Option<ManifestNo>,
    /// The number of visible WAL objects after the current manifest.
    pub wal_tail_objects: u64,
    /// The number of snapshots that hadn't expired when the diagnostics read began.
    pub live_snapshots: u64,
    /// The number of user checkpoints, including expired records that garbage collection hasn't
    /// deleted yet.
    pub live_checkpoints: u64,
}

/// The result of deleting a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteNamespaceResponse {
    /// The deleted namespace.
    pub namespace_id: NamespaceId,
    /// The last committed sequence before the namespace was deleted.
    pub head_seq: ChangeSeq,
}

/// What a path put, move, or copy does when the destination already exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationBehavior {
    /// An operation that fails if the destination path already exists.
    #[default]
    NoReplace,
    /// An operation that replaces the current destination file.
    Replace,
}

/// The requirements for replacing the destination of a move or copy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DestinationPrecondition {
    /// Whether the operation can replace an existing destination file.
    #[serde(default)]
    pub behavior: DestinationBehavior,
    /// The inode that the destination path must contain. It applies only with `replace` behavior.
    #[serde(
        rename = "expected_destination_inode_id",
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expected_inode_id: Option<InodeId>,
    /// The content revision that must be the destination file's current revision. It applies only
    /// with `replace` behavior and requires `expected_destination_inode_id`.
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

/// Whether a delete can remove a non-empty directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DeleteDirectoryBehavior {
    /// A delete that fails if the target is a non-empty directory.
    #[default]
    NonRecursive,
    /// A delete that removes the whole directory subtree.
    Recursive,
}

/// One filesystem operation.
///
/// The server rejects unknown fields, and a variant without fields requires an empty object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FilesystemOperation {
    /// An operation that creates one directory.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectory")
    )]
    CreateDirectory {
        /// The absolute path of the new directory. The operation fails if the path is invalid or
        /// already bound.
        path: AbsolutePath,
        /// Whether the operation creates any missing ancestor directories. The final path component
        /// must still be new.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        parents: bool,
    },
    /// An operation that creates a directory under an existing parent inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectoryByInode")
    )]
    CreateDirectoryByInode {
        /// The parent directory.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// The name of the new directory.
        display_name: DisplayName,
    },
    /// An operation that creates or replaces one file from uploaded or inline content.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationPutFile"))]
    PutFile {
        /// The absolute destination path. The operation creates any missing ancestor directories.
        path: AbsolutePath,
        /// The uploaded content, which a content token must cover. Set this field or
        /// `inline_content`, but not both.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// The complete file bytes as base64. Set this field or `content_ref`, but not both.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes::option"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
        /// Whether the operation adds a new revision to an existing file or fails with a conflict.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// The inode that the path must contain. It applies only with `replace` behavior.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The content revision that must be the file's current revision. It applies only with
        /// `replace` behavior and requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that creates a file with an unused name under an existing parent inode.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateFileByInode")
    )]
    CreateFileByInode {
        /// The parent directory.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// The name of the new file.
        display_name: DisplayName,
        /// The uploaded content, which a content token must cover. Set this field or
        /// `inline_content`, but not both.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// The complete file bytes as base64. Set this field or `content_ref`, but not both.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes::option"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
    },
    /// An operation that adds a revision to a file inode if the inode's current revision is
    /// `expected_revision_no`.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileRevisionByInode")
    )]
    PutFileRevisionByInode {
        /// The file to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The uploaded content, which a content token must cover. Set this field or
        /// `inline_content`, but not both.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
        /// The complete file bytes as base64. Set this field or `content_ref`, but not both.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::base64_bytes::option"
        )]
        #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
        inline_content: Option<Vec<u8>>,
        /// The revision that must be the file's current revision for the write to succeed.
        expected_revision_no: RevisionNo,
    },
    /// An operation that adds bytes to the end of a file as its next revision.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationAppendFile"))]
    AppendFile {
        /// The absolute path, which must resolve to a visible file.
        path: AbsolutePath,
        /// The bytes to add, as base64, from 1 byte to 256 KiB.
        #[serde(with = "crate::base64_bytes")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = Byte))]
        inline_content: Vec<u8>,
        /// The inode that the path must still resolve to.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The content revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that adds bytes to the end of a file inode as its next revision.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationAppendFileByInode")
    )]
    AppendFileByInode {
        /// The file to append to.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The bytes to add, as base64, from 1 byte to 256 KiB.
        #[serde(with = "crate::base64_bytes")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = Byte))]
        inline_content: Vec<u8>,
        /// The content revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that deletes the file or directory at one path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationDeletePath"))]
    DeletePath {
        /// The absolute path, which must resolve to a visible inode.
        path: AbsolutePath,
        /// Whether the operation can recursively delete a non-empty directory.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
        /// The inode that the path must still resolve to before the delete.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
    },
    /// An operation that deletes an inode if its current binding version is
    /// `expected_binding_version`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationDeleteByInode")
    )]
    DeleteByInode {
        /// The inode to delete.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The binding version that the inode must currently have for the delete to succeed.
        expected_binding_version: BindingVersion,
        /// Whether the operation can recursively delete a non-empty directory.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
    },
    /// An operation that moves a file or directory from one path to another.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMovePath"))]
    MovePath {
        /// The absolute source path, which must resolve to a visible inode.
        source_path: AbsolutePath,
        /// The absolute destination path. The destination's parent must be visible and writable.
        destination_path: AbsolutePath,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that moves an inode if its current binding version is
    /// `expected_binding_version`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMoveByInode"))]
    MoveByInode {
        /// The inode to move.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The binding version that the inode must currently have for the move to succeed.
        expected_binding_version: BindingVersion,
        /// The destination directory.
        #[serde(with = "crate::public_inode_id")]
        destination_parent_inode_id: InodeId,
        /// The inode's new name.
        destination_display_name: DisplayName,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that copies a file from one path to another.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyPath"))]
    CopyPath {
        /// The absolute source path, which must resolve to a visible file.
        source_path: AbsolutePath,
        /// The absolute destination path. The destination's parent must be visible and writable.
        destination_path: AbsolutePath,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that copies a file inode to a new name under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyByInode"))]
    CopyByInode {
        /// The file to copy.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The destination directory.
        #[serde(with = "crate::public_inode_id")]
        destination_parent_inode_id: InodeId,
        /// The name of the copy.
        destination_display_name: DisplayName,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that restores the deletion that `inode_id` and `deletion_seq` identify.
    ///
    /// To choose the destination, set `destination_path`, or set `destination_parent_inode_id` and
    /// `destination_display_name`. If you set neither, the undelete uses the parent and name
    /// recorded with the deletion.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUndelete"))]
    Undelete {
        /// The deleted inode to make reachable again.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The deletion sequence that the caller observed. It prevents the undelete from cancelling
        /// a newer deletion of the inode.
        deletion_seq: ChangeSeq,
        /// The absolute path to restore the inode to. It can't be combined with
        /// `destination_parent_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_path: Option<AbsolutePath>,
        /// The directory to restore the inode into. It requires `destination_display_name`.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_parent_inode_id: Option<InodeId>,
        /// The inode's name in `destination_parent_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_display_name: Option<DisplayName>,
    },
    /// An operation that restores an older revision as the current revision of the file at a path.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationRestoreRevision")
    )]
    RestoreRevision {
        /// The absolute path, which must resolve to a visible file.
        path: AbsolutePath,
        /// An existing older revision. The operation copies its content into a new current
        /// revision.
        source_revision_no: RevisionNo,
    },
    /// An operation that restores an older revision as the current revision of a file inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationRestoreRevisionByInode")
    )]
    RestoreRevisionByInode {
        /// The visible file to restore.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// An existing older revision. The operation copies its content into a new current
        /// revision.
        source_revision_no: RevisionNo,
    },
    /// An operation that writes and removes attributes on the inode that a path resolves to.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAttributes")
    )]
    UpdateAttributes {
        /// The absolute path, which must resolve to a visible file or directory.
        path: AbsolutePath,
        /// The attributes to write. A new value replaces the current value of the same key, and
        /// other keys don't change.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// The attribute keys to remove. The server rejects a list that repeats a key.
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
        /// The attribute revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    /// An operation that writes and removes attributes on a visible inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAttributesByInode")
    )]
    UpdateAttributesByInode {
        /// The visible file or directory to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The attributes to write. A new value replaces the current value of the same key, and
        /// other keys don't change.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// The attribute keys to remove. The server rejects a list that repeats a key.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remove: Vec<AttributeKey>,
        /// The attribute revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    /// An operation that replaces the access row of the inode that a path resolves to.
    ///
    /// The root path is a valid target.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUpdateAccess"))]
    UpdateAccess {
        /// The absolute path, which must resolve to a visible file or directory.
        path: AbsolutePath,
        /// Whether the directory stops inheriting grants from its ancestors.
        boundary: bool,
        /// Every direct grant that the inode has after this update.
        grants: AccessGrants,
        /// The inode that the path must still resolve to before the update.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The access revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
    /// An operation that replaces the access row of a visible inode.
    ///
    /// The root inode is a valid target.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAccessByInode")
    )]
    UpdateAccessByInode {
        /// The visible file or directory to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Whether the directory stops inheriting grants from its ancestors.
        boundary: bool,
        /// Every direct grant that the inode has after this update.
        grants: AccessGrants,
        /// The access revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
}

impl FilesystemOperation {
    /// Builds an undelete of the deletion of `inode_id` committed at
    /// `deletion_seq`, bound where `destination` says.
    pub fn undelete(
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        destination: &UndeleteDestination,
    ) -> Self {
        let (destination_path, destination_parent_inode_id, destination_display_name) =
            match destination {
                UndeleteDestination::Recorded => (None, None, None),
                UndeleteDestination::Path(path) => (Some(path.clone()), None, None),
                UndeleteDestination::Name {
                    parent_inode_id,
                    display_name,
                } => (None, Some(*parent_inode_id), Some(display_name.clone())),
            };
        Self::Undelete {
            inode_id,
            deletion_seq,
            destination_path,
            destination_parent_inode_id,
            destination_display_name,
        }
    }

    /// Returns the content written by this operation, if any.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::PutFile { content_ref, .. }
            | Self::CreateFileByInode { content_ref, .. }
            | Self::PutFileRevisionByInode { content_ref, .. } => content_ref.as_ref(),
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::AppendFile { .. }
            | Self::AppendFileByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::CopyByInode { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::RestoreRevisionByInode { .. }
            | Self::UpdateAttributes { .. }
            | Self::UpdateAttributesByInode { .. }
            | Self::UpdateAccess { .. }
            | Self::UpdateAccessByInode { .. } => None,
        }
    }

    /// Returns the bytes this operation adds to the end of a file, if any.
    pub fn appended_content(&self) -> Option<&[u8]> {
        match self {
            Self::AppendFile { inline_content, .. }
            | Self::AppendFileByInode { inline_content, .. } => Some(inline_content),
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::PutFile { .. }
            | Self::CreateFileByInode { .. }
            | Self::PutFileRevisionByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::CopyByInode { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::RestoreRevisionByInode { .. }
            | Self::UpdateAttributes { .. }
            | Self::UpdateAttributesByInode { .. }
            | Self::UpdateAccess { .. }
            | Self::UpdateAccessByInode { .. } => None,
        }
    }
}

/// A condition that a commit must meet before its operations run.
///
/// The server checks each condition against the state before the commit. When several commits are
/// applied as one batch, that state's head sequence is the sequence of the last commit admitted
/// earlier in the batch, or the batch's starting head sequence if no earlier commit was admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitPrecondition {
    /// A requirement that the head sequence before the commit equals `expected_head_seq`.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionNamespaceHead"))]
    NamespaceHead {
        /// The head sequence that the caller observed when it read its inputs.
        expected_head_seq: ChangeSeq,
    },
    /// A requirement that the inode is visible and has the content revision that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionFileRevision"))]
    FileRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The content revision that the caller observed.
        expected_revision_no: RevisionNo,
    },
    /// A requirement that the path still has the binding that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathBinding"))]
    PathBinding {
        /// The absolute path to check, which can be the root.
        path: AbsolutePath,
        /// The inode that the path must be bound to.
        #[serde(with = "crate::public_inode_id")]
        expected_inode_id: InodeId,
        /// The binding version that the path must also have, if set. This check detects an inode
        /// that was moved away from the path and then back.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_binding_version: Option<BindingVersion>,
    },
    /// A requirement that a visible inode still has the binding that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionInodeBinding"))]
    InodeBinding {
        /// The inode whose binding the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The binding version that the caller observed.
        expected_binding_version: BindingVersion,
    },
    /// A requirement that the inode is visible and has the attribute revision that the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAttributesRevision")
    )]
    AttributesRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The attribute revision that the caller observed.
        expected_attributes_revision_no: AttributesRevisionNo,
    },
    /// A requirement that the inode is visible and has the access revision that the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAccessRevision")
    )]
    AccessRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The access revision that the caller observed.
        expected_access_revision_no: AccessRevisionNo,
    },
    /// A requirement that no visible entry exists at the full path.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathAbsence"))]
    PathAbsence {
        /// The absolute path to check, which can be the root.
        path: AbsolutePath,
    },
    /// A requirement that no visible entry with this name exists under the parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionNameAbsence"))]
    NameAbsence {
        /// The directory to check.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// The name to check.
        display_name: DisplayName,
    },
}

/// A request to commit one or more filesystem operations atomically, in order.
///
/// The server rejects unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// The caller's idempotency key for the whole request.
    pub commit_id: CommitId,
    /// An annotation from the caller. The message is part of the commit's identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
    /// One content token for each uploaded `content_ref` that the request uses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ContentToken>,
    /// The conditions that the server checks, in order, before it runs any operation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    /// The operations to commit atomically, in order. The list can't be empty.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// Sets the admission conditions in caller order.
    pub fn preconditions(mut self, preconditions: Vec<CommitPrecondition>) -> Self {
        self.preconditions = preconditions;
        self
    }

    /// Builds a request with exactly one operation.
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

/// One immutable revision of a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FileRevision {
    /// The file inode that this revision belongs to.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The revision number within the file inode.
    pub revision_no: RevisionNo,
    /// The namespace sequence of the commit that created this revision.
    pub committed_seq: ChangeSeq,
    /// The ID of the commit that created this revision.
    pub commit_id: CommitId,
    /// The time the revision was committed, in Unix milliseconds. Commit order comes from
    /// `committed_seq`, not from this time.
    pub committed_at_ms: u64,
    /// The actor responsible for this revision, as the application supplied it.
    pub committed_by: crate::ActorId,
    /// The content stored for this revision.
    pub content_ref: ContentRef,
}

/// One page of a file's revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListFileRevisionsResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The file inode whose revisions were returned.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The namespace head sequence that the read used.
    pub head_seq: ChangeSeq,
    /// The retained revisions, in order.
    pub revisions: Vec<FileRevision>,
    /// An opaque cursor for the next page, if more revisions are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// A request to create a durable checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateCheckpointRequest {
    /// A label to record on the checkpoint. Labels don't need to be unique.
    pub name: String,
    /// The checkpoint's lifetime in milliseconds. Omit it to keep the checkpoint until it's
    /// deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub ttl_ms: Option<u64>,
}

/// A request to create a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateSnapshotRequest {
    /// The snapshot's label, which doesn't need to be unique.
    pub name: String,
    /// The snapshot's lifetime in milliseconds, counted from the server's current time.
    pub ttl_ms: u64,
}

/// A request to extend a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ExtendSnapshotRequest {
    /// The new lifetime in milliseconds, counted from the server's current time.
    pub ttl_ms: u64,
}

/// The checkpoint record that was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteCheckpointResponse {
    /// The namespace that the checkpoint belonged to.
    pub namespace_id: NamespaceId,
    /// The ID of the deleted checkpoint record.
    pub checkpoint_id: PinId,
}

/// The owner of a checkpoint record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwner {
    /// A pin that an operator created. It's deleted by its ID or after it expires.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerUser"))]
    User {
        /// The label that the creator recorded. Labels don't need to be unique.
        name: String,
    },
    /// A pin that keeps the source state that one fork attempt starts from.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerFork"))]
    Fork {
        /// The fork's target namespace. Garbage collection keeps this checkpoint while that
        /// namespace exists.
        target_namespace_id: NamespaceId,
    },
    /// A pin that an application created as a snapshot. It's readable until it expires.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerSnapshot"))]
    Snapshot {
        /// The snapshot's label, which doesn't need to be unique.
        name: String,
    },
    /// An owner kind from a newer server. This variant doesn't include the kind's own fields.
    #[serde(other)]
    Unknown,
}

/// One checkpoint, as its durable record stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Checkpoint {
    /// The namespace that the checkpoint belongs to.
    pub namespace_id: NamespaceId,
    /// The checkpoint's durable ID. Use it to delete the checkpoint.
    pub checkpoint_id: PinId,
    /// The checkpoint's owner. For a user pin, the owner includes the label.
    pub owner: CheckpointOwner,
    /// The time the checkpoint record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The time the checkpoint expires, in Unix milliseconds. Garbage collection waits one more
    /// grace window before it deletes the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expires_at_ms: Option<u64>,
    /// The namespace sequence that the checkpoint captured.
    pub captured_seq: ChangeSeq,
    /// The manifest that the checkpoint pins.
    pub manifest_no: ManifestNo,
}

/// A live snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Snapshot {
    /// The snapshot ID.
    pub snapshot_id: PinId,
    /// The namespace whose state the snapshot captured.
    pub namespace_id: NamespaceId,
    /// The snapshot's label.
    pub name: String,
    /// The namespace sequence that the snapshot captured.
    pub captured_seq: ChangeSeq,
    /// The time the snapshot record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The time the snapshot expires, in Unix milliseconds.
    pub expires_at_ms: u64,
}

impl Snapshot {
    /// Converts a snapshot-owned checkpoint to a snapshot summary.
    ///
    /// Returns `None` for another owner. A snapshot owner always carries the
    /// checkpoint's top-level `expires_at_ms`.
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Option<Self> {
        let CheckpointOwner::Snapshot { name } = checkpoint.owner else {
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

    /// Reports whether the snapshot is still live at `now_ms`.
    pub fn is_live(&self, now_ms: u64) -> bool {
        self.expires_at_ms > now_ms
    }
}

/// One page of checkpoint records, including expired records that garbage collection hasn't deleted
/// yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListCheckpointsResponse {
    /// The namespace that the records belong to.
    pub namespace_id: NamespaceId,
    /// The records, in ascending checkpoint ID order.
    pub checkpoints: Vec<Checkpoint>,
    /// An opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// One page of live snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListSnapshotsResponse {
    /// The namespace that the snapshots belong to.
    pub namespace_id: NamespaceId,
    /// The live snapshot records, in ascending snapshot ID order.
    pub snapshots: Vec<Snapshot>,
    /// An opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// The snapshot record that was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteSnapshotResponse {
    /// The namespace that the snapshot belonged to.
    pub namespace_id: NamespaceId,
    /// The ID of the deleted snapshot record.
    pub snapshot_id: PinId,
}

/// Optional overrides for one garbage collection pass.
///
/// The server rejects unknown fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// The minimum age, in milliseconds, that an object must reach before the pass can delete it.
    /// The value must be at least the server's advertised safety floor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub grace_window_ms: Option<u64>,
}

/// The candidates that one garbage collection pass inspected but didn't delete, counted by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// The candidates that a current root or the shared base filter protected, or that manifest
    /// discovery needed.
    pub referenced: u64,
    /// The unreferenced candidates that were younger than the grace window, according to their
    /// provider timestamps.
    pub within_grace_window: u64,
    /// The unreferenced candidates that had no provider timestamp, so their age was unknown.
    pub no_provider_timestamp: u64,
    /// The keys that garbage collection didn't recognize in the object families it scanned. The
    /// pass keeps these objects.
    pub unrecognized_key: u64,
    /// The checkpoint records that the pass couldn't delete because of their owner or their grace
    /// window.
    pub checkpoint_not_deletable: u64,
    /// The upload sessions that a lease or grace window still protected.
    pub upload_session_window: u64,
    /// The upload sessions that the pass couldn't prove safe to delete.
    pub upload_session_undecided: u64,
}

/// The number of objects that one garbage collection pass deleted, grouped by object family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedObjectCounts {
    /// The unreferenced WAL objects that the pass deleted.
    pub wal_objects: u64,
    /// The unreferenced metadata segments that the pass deleted.
    pub metadata_segments: u64,
    /// The unreferenced manifests that the pass deleted.
    pub manifests: u64,
    /// The upload session records that the pass deleted after their grace windows passed.
    pub upload_sessions: u64,
    /// The content objects the pass deleted because no retained view referenced them and they were
    /// older than the grace window.
    pub content_objects: u64,
    /// The content objects that the pass listed under a retired namespace and deleted.
    pub retired_content_objects: u64,
    /// The store's temporary objects that the pass deleted because they were older than the grace
    /// window.
    pub temporary_objects: u64,
}

impl DeletedObjectCounts {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            wal_objects,
            metadata_segments,
            manifests,
            upload_sessions,
            content_objects,
            retired_content_objects,
            temporary_objects,
        } = other;
        self.wal_objects += wal_objects;
        self.metadata_segments += metadata_segments;
        self.manifests += manifests;
        self.upload_sessions += upload_sessions;
        self.content_objects += content_objects;
        self.retired_content_objects += retired_content_objects;
        self.temporary_objects += temporary_objects;
    }
}

/// The number of checkpoint records that one garbage collection pass deleted, grouped by owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeletedCheckpointsByOwner {
    /// The user pins that the pass deleted after they expired or their namespace was deleted.
    pub user: u64,
    /// The snapshot pins that the pass deleted after they expired or their namespace was deleted.
    pub snapshot: u64,
    /// The fork pins that the pass deleted because their target namespaces no longer exist.
    pub fork: u64,
}

impl DeletedCheckpointsByOwner {
    /// Adds counts from another pass.
    pub fn add(&mut self, other: &Self) {
        let Self {
            user,
            snapshot,
            fork,
        } = other;
        self.user += user;
        self.snapshot += snapshot;
        self.fork += fork;
    }
}

/// The result of one stateless garbage collection call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// The namespace that the pass ran against.
    pub namespace_id: NamespaceId,
    /// The objects that the pass deleted, counted by object family.
    pub deleted: DeletedObjectCounts,
    /// The checkpoint records that the pass deleted, counted by owner.
    pub deleted_checkpoints_by_owner: DeletedCheckpointsByOwner,
    /// The candidates that the pass kept at deletion time, counted by reason.
    pub retained: RetainedCandidates,
    /// The earliest future retirement deadline, pin deletion time, or upload cleanup time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_reclamation_at_ms: Option<u64>,
    /// The deletion time of the current tombstone plus the configured retirement grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub reclaimable_at_ms: Option<u64>,
}

impl GcResponse {
    /// Creates an empty report for `namespace_id`, before any candidate is
    /// examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted: DeletedObjectCounts::default(),
            deleted_checkpoints_by_owner: DeletedCheckpointsByOwner::default(),
            retained: RetainedCandidates::default(),
            next_reclamation_at_ms: None,
            reclaimable_at_ms: None,
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

    /// Returns the reason with the highest count, and that count.
    ///
    /// `None` when nothing was retained. Ties go to the first reason in the
    /// fixed table order, so one pass's report is stable.
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

/// A request to advance the retention floor to the folded manifest head, or to the one target that
/// the request sets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct AdvanceRetentionRequest {
    /// The sequence to advance the floor to. If the folded manifest head is lower, the floor
    /// advances to the head instead. This field can't be combined with `cutoff_at_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub to_seq: Option<ChangeSeq>,
    /// A Unix time in milliseconds. The floor advances to the last commit made at or before this
    /// time. LoonFS reads commits upward from the floor, and the first commit made after this time
    /// ends the advance. This field can't be combined with `to_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub cutoff_at_ms: Option<u64>,
}

/// The result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// The namespace whose retention floor was advanced.
    pub namespace_id: NamespaceId,
    /// The new retention floor, which is the minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// One maintenance job for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunMaintenanceRequest {
    /// A job that folds the WAL tail and runs one bounded compaction step.
    Metadata(MetadataMaintenanceRequest),
    /// A job that runs one unit of metadata compaction, which is either one bounded merge or one
    /// streaming compaction of a family group.
    MetadataCompaction(MetadataCompactionRequest),
    /// A garbage collection job for aged, unreferenced objects.
    Gc(GcRequest),
    /// A garbage collection job for aged, unreferenced grep index objects.
    GrepGc {},
    /// A job that advances the retention floor to the folded manifest head, or to the target that
    /// the request sets.
    Retention(AdvanceRetentionRequest),
    /// A job that restores a root administrator.
    RecoverAdministrator(RecoverAdministratorRequest),
}

/// A request to grant `admin` on the root access row to one principal, through a commit that
/// doesn't check any subject's rights.
///
/// Every other root grant stays in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct RecoverAdministratorRequest {
    /// The principal that receives administrator rights.
    pub principal_id: PrincipalId,
}

/// The committed administrator recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RecoverAdministratorResponse {
    /// The namespace whose root grants changed.
    pub namespace_id: NamespaceId,
    /// The ID of the recovery commit.
    pub commit_id: CommitId,
    /// The sequence of the recovery commit.
    pub committed_seq: ChangeSeq,
    /// The root's access revision after the recovery.
    pub access_revision_no: AccessRevisionNo,
}

/// Overrides for the `metadata` maintenance job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataMaintenanceRequest {
    /// The WAL tail length, in WAL objects, that triggers a fold. Omit it to use the server
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub max_wal_tail_objects: Option<u64>,
}

/// A request for one unit of metadata compaction, with no options.
///
/// To compact every eligible family group, repeat the request while it keeps publishing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataCompactionRequest {}

/// What the WAL fold step of a maintenance pass did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WalFoldStepOutcome {
    /// The tail was below the threshold, so there was nothing to fold.
    NotNeeded,
    /// The step folded the WAL tail and published the next current manifest.
    Folded {
        /// The last sequence that the published manifest includes.
        manifest_head_seq: ChangeSeq,
    },
    /// The current manifest already included the captured WAL tail, so this step didn't publish a
    /// manifest.
    AlreadyPublished {
        /// The sequence that this step tried to fold through.
        attempted_seq: ChangeSeq,
        /// The number of the namespace's current manifest.
        current_manifest_no: ManifestNo,
    },
    /// Concurrent updates prevented every publication attempt.
    RetriesExhausted {
        /// The head sequence observed before the step ran.
        observed_head_seq: ChangeSeq,
    },
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

/// What the bounded compaction step of a maintenance pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CompactionStepOutcome {
    /// No family group had enough delta runs to merge.
    #[cfg_attr(feature = "openapi", schema(title = "CompactionStepOutcomeNotNeeded"))]
    NotNeeded {},
    /// One family group was merged, and a new manifest was published.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CompactionStepOutcomeBoundedMergePublished")
    )]
    BoundedMergePublished {},
    /// A family group's window is too large for one bounded merge.
    ///
    /// Run the `metadata_compaction` job, which compacts the group by streaming.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CompactionStepOutcomeMetadataCompactionRequired")
    )]
    MetadataCompactionRequired {},
    /// Another publisher changed the current manifest before this step could publish.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CompactionStepOutcomeManifestAdvanced")
    )]
    ManifestAdvanced {},
    /// A newer runtime claimed the namespace's compactor epoch, so this step published nothing.
    #[cfg_attr(feature = "openapi", schema(title = "CompactionStepOutcomeFenced"))]
    Fenced {},
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

/// The result of one maintenance job.
///
/// The `kind` matches the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunMaintenanceResponse {
    /// The result of the WAL fold and one bounded compaction step.
    Metadata(MetadataMaintenanceResponse),
    /// The result of one unit of metadata compaction.
    MetadataCompaction(MetadataCompactionResponse),
    /// The counts and deadlines from one garbage collection call.
    Gc(GcResponse),
    /// The counts from one garbage collection pass over the grep index.
    GrepGc {
        /// The namespace whose grep keyspace the pass inspected.
        namespace_id: NamespaceId,
        /// The unreferenced grep segments that the pass deleted because they were older than the
        /// minimum segment age.
        deleted_segments: u64,
        /// The other unreferenced grep objects that the pass deleted after the grace window.
        deleted_other_objects: u64,
        /// Whether the pass deleted the grep extension state of a namespace that is missing or
        /// deleted.
        namespace_reaped: bool,
        /// The candidates that the pass kept because they were referenced, too young, or
        /// unrecognized.
        retained_candidates: u64,
    },
    /// The result of advancing the retention floor.
    Retention(AdvanceRetentionResponse),
    /// The committed administrator recovery.
    RecoverAdministrator(RecoverAdministratorResponse),
}

/// What each step of one `metadata` maintenance job did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceResponse {
    /// The namespace that this run maintained.
    pub namespace_id: NamespaceId,
    /// What the WAL fold did.
    pub wal_fold: WalFoldStepOutcome,
    /// What the bounded compaction step did.
    pub compaction: CompactionStepOutcome,
}

/// What one metadata compaction run did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataCompactionResponse {
    /// The namespace that this run compacted.
    pub namespace_id: NamespaceId,
    /// The outcome of the compaction.
    pub compaction: MetadataCompactionOutcome,
}

/// The outcome of one metadata compaction run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MetadataCompactionOutcome {
    /// No eligible family group was available, so nothing was published.
    NotNeeded,
    /// The selected window fit in a bounded step, and this run published it.
    BoundedMergePublished,
    /// A streaming compaction merged the selected window and published the next manifest.
    Published {
        /// The manifest that the compaction published.
        manifest_no: ManifestNo,
        /// The rows that the compaction read.
        rows_read: u64,
        /// The rows that the compaction wrote.
        rows_written: u64,
        /// The input bytes that the compaction read.
        input_bytes: u64,
        /// The output bytes that the compaction wrote.
        output_bytes: u64,
        /// The output segments that the compaction wrote.
        output_segments: u64,
    },
    /// The run was cancelled, so the current manifest didn't change.
    Cancelled,
    /// The run stopped because its inputs changed, time ran out, or publication retries were
    /// exhausted.
    Abandoned,
    /// Another process claimed the namespace's compactor epoch, so nothing was published.
    Fenced,
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

/// A request for one run of the store contract probe, with an empty body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct StoreProbeRequest {}

/// The results of one store contract probe, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeResponse {
    /// The ID of this probe run. The server generates the ID and writes the probe's temporary
    /// objects under it.
    pub run_id: ProbeRunId,
    /// The check results, in the order that the checks ran.
    pub checks: Vec<StoreProbeCheckResult>,
}

impl StoreProbeResponse {
    /// Returns true when every check passed or reported an unsupported option.
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|check| {
            matches!(
                check.outcome,
                StoreProbeCheckOutcome::Passed | StoreProbeCheckOutcome::Unsupported
            )
        })
    }
}

/// The result of one contract check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeCheckResult {
    /// The check's stable name.
    pub name: String,
    /// Whether the store passed the check, failed it, or doesn't support the capability.
    pub outcome: StoreProbeCheckOutcome,
    /// The expected and actual behavior for a failed check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
}

impl StoreProbeCheckResult {
    /// Formats the check as one report line.
    pub fn check_line(&self) -> String {
        match self.outcome {
            StoreProbeCheckOutcome::Passed => format!("{}: passed", self.name),
            StoreProbeCheckOutcome::Unsupported => format!("{}: unsupported", self.name),
            StoreProbeCheckOutcome::Failed => match &self.message {
                Some(message) => format!("{}: failed: {message}", self.name),
                None => format!("{}: failed", self.name),
            },
        }
    }
}

/// What one contract check found about the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum StoreProbeCheckOutcome {
    /// The store behaved as the contract requires.
    Passed,
    /// The store doesn't support this optional capability.
    Unsupported,
    /// The store violated the contract or the operation failed.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentId;
    use std::assert_matches;

    #[test]
    fn file_revision_provenance_fields_are_pinned_on_the_wire() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
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
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("valid content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        )
    }

    #[test]
    fn namespace_wire_shape_has_only_core_state() {
        let namespace = NamespaceMetadata {
            access: NamespaceAccessMode::Unrestricted {},
            naming: NamespaceNaming::CaseSensitive,
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
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
                "access": {"kind": "unrestricted"},
                "naming": "case_sensitive",
                "created_at_ms": 1000,
                "created_by": "test",
                "head_seq": 11,
                "retention_floor_seq": 4
            })
        );
    }

    #[test]
    fn a_namespace_access_mode_from_a_newer_server_decodes_with_an_unknown_field() {
        let mode: NamespaceAccessMode = serde_json::from_value(serde_json::json!({
            "kind": "acl",
            "principal_scope": "corp",
            "added_by_a_newer_server": true
        }))
        .expect("a response accepts unknown fields");
        assert_eq!(
            mode,
            NamespaceAccessMode::Acl {
                principal_scope: PrincipalScope::parse("corp").expect("principal scope"),
            }
        );
    }

    #[test]
    fn namespace_diagnostics_wire_shape_keeps_storage_fields() {
        let diagnostics = NamespaceDiagnostics {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
            current_manifest_no: Some(ManifestNo(8)),
            wal_tail_objects: 3,
            live_snapshots: 2,
            live_checkpoints: 5,
        };
        assert_eq!(
            serde_json::to_value(diagnostics).expect("serialize namespace diagnostics"),
            serde_json::json!({
                "namespace_id": "demo",
                "created_at_ms": 1000,
                "created_by": "test",
                "head_seq": 11,
                "retention_floor_seq": 4,
                "current_manifest_no": 8,
                "wal_tail_objects": 3,
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
            serde_json::from_value::<FilesystemOperation>(encoded)
                .expect_err("an invalid attribute key or value should be rejected");
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
                "content_id": "con_0123456789abcdef0123456789abcdef",
                "size_bytes": 1,
                "checksum": {
                    "algorithm": "crc64nvme",
                    "value": "aaaaaaaaaaaaaaaa"
                }
            }
        }))
        .expect("put op defaults behavior");
        assert_matches!(
            put,
            FilesystemOperation::PutFile {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
                ..
            }
        );

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
            "expected_binding_version": "aaaa",
            "destination_parent_inode_id": "ino_1",
            "destination_display_name": "b.txt"
        }))
        .expect("inode move defaults behavior");
        assert_eq!(
            move_by_inode,
            FilesystemOperation::MoveByInode {
                inode_id: InodeId(7),
                expected_binding_version: BindingVersion::parse("aaaa").expect("binding version"),
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
            ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
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
                    destination_parent_inode_id: None,
                    destination_display_name: None,
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
    fn appends_carry_their_bytes_as_required_base64() {
        let cases = [
            (
                FilesystemOperation::AppendFile {
                    path: path("/logs/app.log"),
                    inline_content: b"hi".to_vec(),
                    expected_inode_id: Some(InodeId(7)),
                    expected_revision_no: Some(RevisionNo(3)),
                },
                serde_json::json!({
                    "kind": "append_file",
                    "path": "/logs/app.log",
                    "inline_content": "aGk=",
                    "expected_inode_id": "ino_7",
                    "expected_revision_no": 3
                }),
            ),
            (
                FilesystemOperation::AppendFileByInode {
                    inode_id: InodeId(7),
                    inline_content: b"hi".to_vec(),
                    expected_revision_no: None,
                },
                serde_json::json!({
                    "kind": "append_file_by_inode",
                    "inode_id": "ino_7",
                    "inline_content": "aGk="
                }),
            ),
        ];
        for (operation, encoded) in cases {
            assert_eq!(
                serde_json::to_value(&operation).expect("serialize append"),
                encoded
            );
            assert_eq!(
                serde_json::from_value::<FilesystemOperation>(encoded.clone())
                    .expect("deserialize append"),
                operation
            );
            let mut missing = encoded;
            missing
                .as_object_mut()
                .expect("an operation is an object")
                .remove("inline_content");
            serde_json::from_value::<FilesystemOperation>(missing)
                .expect_err("an append without its bytes should be rejected");
        }
    }

    #[test]
    fn filesystem_operation_paths_validate_during_deserialization() {
        for encoded in [
            serde_json::json!({"kind": "create_directory", "path": "relative", "parents": false}),
            serde_json::json!({
                "kind": "put_file",
                "path": "relative",
                "content_ref": ContentRef::blob_v1(crate::NamespaceId::parse("demo").expect("namespace id"), ContentId::generate(), b"hello", crate::ChecksumAlgorithm::Crc64nvme)
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
            serde_json::from_value::<FilesystemOperation>(encoded)
                .expect_err("an invalid path should be rejected");
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
                serde_json::from_value::<FilesystemOperation>(invalid_operation)
                    .expect_err(&format!("{inode_key} accepted an invalid inode ID"));
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
        assert_matches!(
            spelled.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(3)),
                ..
            }]
        );

        for misspelling in ["expected_revsion_no", "expectedRevisionNo"] {
            serde_json::from_value::<CommitRequest>(put(misspelling)).expect_err(&format!(
                "`{misspelling}` decoded instead of failing the request"
            ));
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
        assert_matches!(
            request.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(value)),
                ..
            }] if *value == crate::MAX_PUBLIC_INTEGER
        );

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
            serde_json::from_value::<CommitRequest>(body).expect_err(&format!(
                "an unknown field in {level} decoded instead of failing the request"
            ));
        }
    }

    #[test]
    fn checkpoint_responses_use_one_checkpoint_wire_object() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let checkpoint = Checkpoint {
            namespace_id: namespace_id.clone(),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwner::User {
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
            owner: CheckpointOwner::User {
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
        assert!(gc_json.get("reclaimable_at_ms").is_none());
        assert_eq!(
            gc_json["deleted_checkpoints_by_owner"],
            serde_json::json!({"user": 0, "snapshot": 0, "fork": 0})
        );
        let gc: GcResponse =
            serde_json::from_value(gc_json).expect("decode gc response without optional fields");
        assert_eq!(gc.next_reclamation_at_ms, None);
        assert_eq!(gc.reclaimable_at_ms, None);
        let retired = GcResponse {
            reclaimable_at_ms: Some(2_000_000),
            ..gc
        };
        let json = serde_json::to_value(&retired).expect("encode retirement");
        assert_eq!(json["reclaimable_at_ms"], 2_000_000);
        assert_eq!(
            serde_json::from_value::<GcResponse>(json).expect("decode retirement"),
            retired
        );
    }

    #[test]
    fn maintenance_outcomes_use_the_outcome_tag() {
        assert_eq!(
            serde_json::to_value(WalFoldStepOutcome::Folded {
                manifest_head_seq: ChangeSeq(9),
            })
            .expect("serialize WAL fold outcome"),
            serde_json::json!({"outcome": "folded", "manifest_head_seq": 9})
        );
        assert_eq!(
            serde_json::to_value(CompactionStepOutcome::BoundedMergePublished {})
                .expect("serialize compaction outcome"),
            serde_json::json!({"outcome": "bounded_merge_published"})
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
    fn a_wal_fold_outcome_from_a_newer_server_decodes_as_unknown() {
        let newer = serde_json::json!({"outcome": "deferred", "until_ms": 5});
        let outcome: WalFoldStepOutcome =
            serde_json::from_value(newer.clone()).expect("a newer outcome decodes");
        assert_eq!(outcome, WalFoldStepOutcome::Unknown);
        let outcome: CompactionStepOutcome =
            serde_json::from_value(newer.clone()).expect("a newer outcome decodes");
        assert_eq!(outcome, CompactionStepOutcome::Unknown);
        let outcome: MetadataCompactionOutcome =
            serde_json::from_value(newer).expect("a newer outcome decodes");
        assert_eq!(outcome, MetadataCompactionOutcome::Unknown);
    }

    #[test]
    fn a_checkpoint_owner_from_a_newer_server_decodes_as_unknown() {
        let checkpoint: Checkpoint = serde_json::from_value(serde_json::json!({
            "namespace_id": "demo",
            "checkpoint_id": "pin_00000000000000000001-0000000000000001",
            "owner": {"kind": "retention_policy", "policy_id": "nightly"},
            "created_at_ms": 1,
            "captured_seq": 3,
            "manifest_no": 3
        }))
        .expect("a newer owner decodes");
        assert_eq!(checkpoint.owner, CheckpointOwner::Unknown);
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
                serde_json::json!({"kind": "metadata", "max_wal_tail_objects": 4}),
                Some(RunMaintenanceRequest::Metadata(
                    MetadataMaintenanceRequest {
                        max_wal_tail_objects: Some(4),
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
                Some(RunMaintenanceRequest::Retention(
                    AdvanceRetentionRequest::default(),
                )),
            ),
            (
                serde_json::json!({"kind": "retention", "to_seq": 12}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {
                    to_seq: Some(ChangeSeq(12)),
                    cutoff_at_ms: None,
                })),
            ),
            (
                serde_json::json!({"kind": "retention", "cutoff_at_ms": 1_752_624_000_000u64}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {
                    to_seq: None,
                    cutoff_at_ms: Some(1_752_624_000_000),
                })),
            ),
            (
                serde_json::json!({"kind": "grep_gc"}),
                Some(RunMaintenanceRequest::GrepGc {}),
            ),
            (
                serde_json::json!({"kind": "grep_gc", "max_objects": 8}),
                None,
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
                None => {
                    decoded.expect_err(&format!("invalid maintenance request decoded: {body}"));
                }
            }
        }

        serde_json::from_value::<CreateCheckpointRequest>(
            serde_json::json!({"name": "nightly", "ttl_ms": 60_000}),
        )
        .expect("the same checkpoint body without a typo decodes");
        serde_json::from_value::<CreateCheckpointRequest>(
            serde_json::json!({"name": "nightly", "ttlMs": 60_000}),
        )
        .expect_err("a camel-case field should be rejected");

        // The probe body carries no options yet, so an unknown one is the
        // only thing it can be sent.
        serde_json::from_value::<StoreProbeRequest>(serde_json::json!({}))
            .expect("an empty probe body decodes");
        serde_json::from_value::<StoreProbeRequest>(serde_json::json!({"deep": true}))
            .expect_err("an unknown probe field should be rejected");

        serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
            "namespace_id": "demo",
        }))
        .expect("the same create body without a typo decodes");
        serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
            "namespace_id": "demo",
            "fork_of": "other"
        }))
        .expect_err("a fork field on a create request should be rejected");
        serde_json::from_value::<ForkNamespaceRequest>(serde_json::json!({
            "new_namespace_id": "demo",
            "source_namespace_id": "other"
        }))
        .expect_err("a source namespace field on a fork request should be rejected");
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
            serde_json::from_value::<FilesystemOperation>(missing)
                .expect_err(&format!("missing {field}"));
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
