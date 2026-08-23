//! Manifest load errors, classified as corruption versus store trouble.

use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_api::{ManifestNo, ManifestObjectId, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Coarse failure class of a manifest load: corruption versus store trouble.
///
/// Deliberately not named `*ErrorKind` to avoid colliding with the wire-level
/// caller-action concept in [`loonfs_api::ErrorKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestLoadFailureClass {
    Corrupt,
    Store,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ManifestLoadError {
    #[error("missing namespace manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to read namespace manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("namespace manifest codec error for `{object_key}`: {message}")]
    ManifestCodec { object_key: String, message: String },
    #[error(
        "namespace manifest namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "namespace manifest number mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestNoMismatch {
        object_key: String,
        expected: ManifestNo,
        actual: ManifestNo,
    },
    #[error(
        "namespace manifest object id mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestObjectIdMismatch {
        object_key: String,
        expected: ManifestObjectId,
        actual: ManifestObjectId,
    },
    #[error(
        "namespace manifest object conflict for `{object_key}` manifest `{manifest_no}`: the immutable key contains different bytes"
    )]
    ManifestObjectConflict {
        object_key: String,
        manifest_no: ManifestNo,
    },
    #[error(
        "namespace manifest conflict for `{object_key}` manifest `{manifest_no}`: expected payload checksum `{expected_payload_checksum}`, actual `{actual_payload_checksum}`"
    )]
    ManifestConflict {
        object_key: String,
        manifest_no: ManifestNo,
        expected_payload_checksum: String,
        actual_payload_checksum: String,
    },
    #[error("namespace manifest `{object_key}` is missing row family `{family:?}`")]
    MissingRowFamily {
        object_key: String,
        family: MetadataRowFamily,
    },
    #[error("namespace manifest `{object_key}` repeats row family `{family:?}`")]
    DuplicateRowFamily {
        object_key: String,
        family: MetadataRowFamily,
    },
    #[error("namespace manifest `{object_key}` has invalid runs: {message}")]
    RunManifestMismatch { object_key: String, message: String },
    #[error("missing metadata segment `{object_key}`")]
    MissingSegment { object_key: String },
    #[error("failed to read metadata segment `{object_key}`: {message}")]
    ReadSegment { object_key: String, message: String },
    #[error("metadata segment codec error for `{object_key}`: {message}")]
    SegmentCodec { object_key: String, message: String },
    #[error("metadata segment descriptor mismatch for `{object_key}`: {message}")]
    SegmentDescriptorMismatch { object_key: String, message: String },
    #[error(
        "metadata row kind mismatch for `{object_key}` family `{family:?}`: found `{row_kind}`"
    )]
    SegmentRowKindMismatch {
        object_key: String,
        family: MetadataRowFamily,
        row_kind: String,
    },
    #[error(
        "namespace manifest `{object_key}` has duplicate revision rows in `{family:?}` for key `{row_key}`"
    )]
    DuplicateRevisionRow {
        object_key: String,
        family: MetadataRowFamily,
        row_key: String,
    },
    #[error("namespace manifest `{object_key}` revision index does not match canonical revisions")]
    RevisionIndexMismatch { object_key: String },
}

impl ManifestLoadError {
    pub fn failure_class(&self) -> ManifestLoadFailureClass {
        match self {
            Self::ReadManifest { .. } | Self::ReadSegment { .. } => ManifestLoadFailureClass::Store,
            _ => ManifestLoadFailureClass::Corrupt,
        }
    }
}
