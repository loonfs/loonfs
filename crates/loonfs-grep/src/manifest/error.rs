//! Typed failures for grep manifest state and encoding.

use loonfs_types::format::envelope::EnvelopeCodecError;
use loonfs_types::{IndexSegmentId, RunNo};
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
    #[error("grep reorganization input references missing segment `{segment_id}`")]
    MissingReorganizeInputSegment { segment_id: IndexSegmentId },
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
