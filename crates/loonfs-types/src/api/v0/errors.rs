//! The error response body for the v0 HTTP API.

use crate::ErrorCode;
use crate::{
    AccessRevisionNo, AttributesRevisionNo, BindingVersion, ChangeSeq, CommitFingerprint, CommitId,
    InodeId, NamespaceId, RevisionNo, WriterEpoch, WriterId,
};
use serde::{Deserialize, Serialize};

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

impl ErrorResponse {
    /// Builds the body of an error with `code` and `message` and no other fields.
    pub fn new(code: ErrorCode, message: &str) -> Self {
        Self {
            code: code.as_str().to_owned(),
            feature: None,
            message: message.to_owned(),
            param: None,
            request_id: None,
            details: None,
        }
    }
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
    /// For `range_not_satisfiable`, the size in bytes of the file that the range was requested
    /// from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub size_bytes: Option<u64>,
}
