//! Manifest load errors, classified as corruption versus store trouble.

use loonfs_api::wire::manifest::{MetadataSegmentKey, MetadataTableFamily};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
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
        "namespace manifest id mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestIdMismatch {
        object_key: String,
        expected: ManifestId,
        actual: ManifestId,
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
        "namespace manifest conflict for `{object_key}` manifest `{manifest_id}`: expected payload checksum `{expected_payload_checksum}`, actual `{actual_payload_checksum}`"
    )]
    ManifestConflict {
        object_key: String,
        manifest_id: ManifestId,
        expected_payload_checksum: String,
        actual_payload_checksum: String,
    },
    #[error("namespace manifest `{object_key}` is not verified")]
    ManifestNotVerified { object_key: String },
    #[error("namespace manifest `{object_key}` is missing table family `{family:?}`")]
    MissingTableFamily {
        object_key: String,
        family: MetadataTableFamily,
    },
    #[error("namespace manifest `{object_key}` repeats table family `{family:?}`")]
    DuplicateTableFamily {
        object_key: String,
        family: MetadataTableFamily,
    },
    #[error("namespace manifest `{object_key}` has invalid runs: {message}")]
    RunManifestMismatch { object_key: String, message: String },
    #[error("missing metadata SST `{object_key}`")]
    MissingSegment { object_key: String },
    #[error("failed to read metadata SST `{object_key}`: {message}")]
    ReadSegment { object_key: String, message: String },
    #[error("metadata SST codec error for `{object_key}`: {message}")]
    SegmentCodec { object_key: String, message: String },
    #[error(
        "metadata SST namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "metadata SST seq mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error(
        "metadata SST family mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentFamilyMismatch {
        object_key: String,
        expected: MetadataTableFamily,
        actual: MetadataTableFamily,
    },
    #[error(
        "metadata SST index mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentIndexMismatch {
        object_key: String,
        expected: u32,
        actual: u32,
    },
    #[error(
        "metadata SST key mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentKeyMismatch {
        object_key: String,
        expected: MetadataSegmentKey,
        actual: MetadataSegmentKey,
    },
    #[error("metadata SST key mismatch for `{object_key}`: expected `{expected}`")]
    SegmentObjectKeyMismatch {
        object_key: String,
        expected: String,
    },
    #[error("metadata SST descriptor mismatch for `{object_key}`: {message}")]
    SegmentDescriptorMismatch { object_key: String, message: String },
    #[error("manifest page shape mismatch for `{object_key}` page {page_index}: {message}")]
    PageShapeMismatch {
        object_key: String,
        page_index: u32,
        message: String,
    },
    #[error(
        "metadata row key mismatch for `{object_key}` page {page_index} row {row_index}: expected `{expected}`, actual `{actual}`"
    )]
    RowKeyMismatch {
        object_key: String,
        page_index: u32,
        row_index: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "metadata row kind mismatch for `{object_key}` family `{family:?}`: found `{row_kind}`"
    )]
    TableRowKindMismatch {
        object_key: String,
        family: MetadataTableFamily,
        row_kind: String,
    },
    #[error(
        "namespace manifest `{object_key}` has duplicate revision rows in `{family:?}` for key `{row_key}`"
    )]
    DuplicateRevisionRow {
        object_key: String,
        family: MetadataTableFamily,
        row_key: String,
    },
    #[error("namespace manifest `{object_key}` revision index does not match canonical revisions")]
    RevisionIndexMismatch { object_key: String },
    #[error("metadata rows do not reproduce authoritative metadata")]
    MetadataMismatch,
}

impl ManifestLoadError {
    pub fn failure_class(&self) -> ManifestLoadFailureClass {
        match self {
            Self::ReadManifest { .. }
            | Self::ReadSegment { .. }
            | Self::ManifestConflict { .. } => ManifestLoadFailureClass::Store,
            _ => ManifestLoadFailureClass::Corrupt,
        }
    }
}
