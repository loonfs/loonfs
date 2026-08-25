//! Errors shared by every mutable control-object loader.

use crate::error::StoreFailureClass;
use loonfs_api::NamespaceId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ControlObjectLoadError {
    #[error("missing control object `{object_key}`")]
    MissingObject { object_key: String },
    #[error(
        "metadata root references seq `{root_manifest_head_seq}` beyond the reloaded head seq `{head_seq}`"
    )]
    RootAheadOfHead {
        root_manifest_head_seq: loonfs_api::ChangeSeq,
        head_seq: loonfs_api::ChangeSeq,
    },
    #[error("retention floor seq `{floor_seq}` is beyond the reloaded head seq `{head_seq}`")]
    FloorAheadOfHead {
        floor_seq: loonfs_api::ChangeSeq,
        head_seq: loonfs_api::ChangeSeq,
    },
    #[error(
        "retention floor seq `{floor_seq}` is beyond the seq `{root_manifest_head_seq}` covered by the reloaded metadata root"
    )]
    FloorAheadOfRoot {
        floor_seq: loonfs_api::ChangeSeq,
        root_manifest_head_seq: loonfs_api::ChangeSeq,
    },
    #[error(
        "namespace `{namespace_id}` retention floor stands at `{floor_seq}`, but its metadata root object is missing"
    )]
    MissingRootAfterFloor {
        namespace_id: NamespaceId,
        floor_seq: loonfs_api::ChangeSeq,
    },
    #[error(
        "control object namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    NamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "control object identity mismatch for `{object_key}` field `{field}`: expected `{expected}`, actual `{actual}`"
    )]
    IdentityMismatch {
        object_key: String,
        field: String,
        expected: String,
        actual: String,
    },
    #[error(
        "control object `{object_key}` records a fork basis over a manifest owned by `{namespace_id}`, which is the namespace's own id; a fork basis names the source namespace"
    )]
    ForkBasisOwnerIsSelf {
        object_key: String,
        namespace_id: NamespaceId,
    },
    #[error("invalid control-object key layout `{object_key}` for {expected_family}: {reason}")]
    KeyLayout {
        object_key: String,
        expected_family: String,
        reason: String,
    },
    #[error(
        "control object checksum mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error("control object codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("control object store error for `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
}
