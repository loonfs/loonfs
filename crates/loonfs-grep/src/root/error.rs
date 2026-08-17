//! Typed failures for grep root state, encoding, loading, and publication.

use loonfs::StoreFailureClass;
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::{IndexSegmentId, NamespaceId};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepManifestStateError {
    #[error("unsupported grep index format version `{found}`: this build supports `{supported}`")]
    UnsupportedIndexFormatVersion { found: u32, supported: u32 },
    #[error("disabled grep root carries query-visible segments")]
    DisabledHasSegments,
    #[error("disabled grep root carries an in-progress reorganization")]
    DisabledHasReorganize,
    #[error("duplicate grep segment id `{segment_id}`")]
    DuplicateSegmentId { segment_id: IndexSegmentId },
    #[error("grep segment `{segment_id}` has a minimum row key after its maximum")]
    InvalidSegmentRange { segment_id: IndexSegmentId },
    #[error(
        "grep segment `{segment_id}` uses run ordinal `{run_ordinal}` but the next ordinal is \
         `{next_run_ordinal}`"
    )]
    UnallocatedSegmentRunOrdinal {
        segment_id: IndexSegmentId,
        run_ordinal: u64,
        next_run_ordinal: u64,
    },
    #[error(
        "grep reorganization uses run ordinal `{run_ordinal}` but the next ordinal is `{next_run_ordinal}`"
    )]
    UnallocatedReorganizeRunOrdinal {
        run_ordinal: u64,
        next_run_ordinal: u64,
    },
    #[error("grep reorganization repeats segment id `{segment_id}`")]
    DuplicateReorganizeSegmentId { segment_id: IndexSegmentId },
    #[error("grep reorganization snapshot references missing segment `{segment_id}`")]
    MissingReorganizeSnapshotSegment { segment_id: IndexSegmentId },
    #[error("grep reorganization output references missing segment `{segment_id}`")]
    MissingReorganizeOutputSegment { segment_id: IndexSegmentId },
    #[error(
        "grep reorganization output `{segment_id}` does not carry the reorganization's level and run ordinal"
    )]
    ReorganizeOutputDescriptorMismatch { segment_id: IndexSegmentId },
}

/// Failure encoding or decoding one grep root pointer or manifest.
///
/// Envelope-shaped failures are the shared vocabulary every durable family
/// reports through; only grep's own payload invariants are named here.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GrepEnvelopeCodecError {
    #[error(transparent)]
    Envelope(#[from] EnvelopeCodecError),
    #[error("invalid grep manifest state: {0}")]
    InvalidState(#[from] GrepManifestStateError),
}

/// Failure to load or publish a grep root.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepRootError {
    #[error("object-store operation failed for grep root `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("grep root `{object_key}` is corrupt: {message}")]
    Corrupt { object_key: String, message: String },
    #[error("grep root `{root_key}` names missing manifest `{manifest_key}`")]
    MissingManifest {
        root_key: String,
        manifest_key: String,
    },
    #[error(
        "grep root `{object_key}` names namespace `{actual}` instead of requested namespace \
         `{expected}`"
    )]
    IdentityMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error("grep root publication conflict for `{object_key}`")]
    Conflict { object_key: String },
    #[error("grep root advance changes namespace from `{expected}` to `{actual}`")]
    AdvanceIdentityMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
}

pub(super) type Result<T> = std::result::Result<T, GrepRootError>;
