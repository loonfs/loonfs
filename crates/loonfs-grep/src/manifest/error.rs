//! Typed failures for grep manifest state, encoding, loading, and publication.

use loonfs::StoreFailureClass;
use loonfs_api::wire::envelope::EnvelopeCodecError;
use loonfs_api::{IndexSegmentId, NamespaceId, RunNo};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepManifestStateError {
    #[error("disabled grep manifest carries query-visible segments")]
    DisabledHasSegments,
    #[error("disabled grep manifest carries an in-progress reorganization")]
    DisabledHasReorganize,
    #[error("duplicate grep segment id `{segment_id}`")]
    DuplicateSegmentId { segment_id: IndexSegmentId },
    #[error("grep segment `{segment_id}` has a minimum row key after its maximum")]
    InvalidSegmentRange { segment_id: IndexSegmentId },
    #[error("grep segment `{segment_id}` carries no rows")]
    EmptySegment { segment_id: IndexSegmentId },
    #[error(
        "grep segment `{segment_id}` uses run `{run_no}` but the next run number is \
         `{next_run_no}`"
    )]
    UnallocatedSegmentRunNo {
        segment_id: IndexSegmentId,
        run_no: RunNo,
        next_run_no: RunNo,
    },
    #[error("grep reorganization uses run `{run_no}` but the next run number is `{next_run_no}`")]
    UnallocatedReorganizeRunNo { run_no: RunNo, next_run_no: RunNo },
    #[error("grep reorganization repeats segment id `{segment_id}`")]
    DuplicateReorganizeSegmentId { segment_id: IndexSegmentId },
    #[error("grep reorganization snapshot references missing segment `{segment_id}`")]
    MissingReorganizeSnapshotSegment { segment_id: IndexSegmentId },
    #[error("grep reorganization output references missing segment `{segment_id}`")]
    MissingReorganizeOutputSegment { segment_id: IndexSegmentId },
    #[error(
        "grep reorganization output `{segment_id}` does not carry the reorganization's level and \
         run number"
    )]
    ReorganizeOutputDescriptorMismatch { segment_id: IndexSegmentId },
}

/// Failure encoding or decoding one grep hint or manifest.
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

/// Failure to load or publish a grep manifest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepManifestError {
    #[error("object-store operation failed for grep state `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("grep state `{object_key}` is corrupt: {message}")]
    Corrupt { object_key: String, message: String },
    #[error(
        "grep state `{object_key}` names namespace `{actual_namespace_id}` instead of requested namespace \
         `{expected_namespace_id}`"
    )]
    IdentityMismatch {
        object_key: String,
        expected_namespace_id: NamespaceId,
        actual_namespace_id: NamespaceId,
    },
    #[error("grep manifest publication conflict for `{object_key}`")]
    Conflict { object_key: String },
}

pub(super) type Result<T> = std::result::Result<T, GrepManifestError>;
