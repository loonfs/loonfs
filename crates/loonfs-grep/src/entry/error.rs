//! Why grep's entry in a namespace manifest is invalid.

use loonfs_types::{IndexSegmentId, RunNo};
use thiserror::Error;

/// A rule that grep's entry breaks. Core has already validated the segments
/// that the entry lists, so these are the rules of grep's own members.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum GrepEntryError {
    #[error("grep entry version `{version}` is not supported; this build reads version 2")]
    UnsupportedVersion { version: u32 },
    #[error("grep entry members don't decode: {message}")]
    UndecodableState { message: String },
    #[error("disabled grep entry lists segments")]
    DisabledHasRuns,
    #[error("disabled grep entry has an in-progress reorganization")]
    DisabledHasReorganize,
    #[error("grep reorganization uses run `{run_no}` but the next run number is `{next_run_no}`")]
    UnallocatedReorganizeRunNo { run_no: RunNo, next_run_no: RunNo },
    #[error("grep reorganization repeats segment id `{segment_id}`")]
    DuplicateReorganizeSegmentId { segment_id: IndexSegmentId },
    #[error("grep reorganization input references missing segment `{segment_id}`")]
    MissingReorganizeInputSegment { segment_id: IndexSegmentId },
    #[error("grep reorganization output references missing segment `{segment_id}`")]
    MissingReorganizeOutputSegment { segment_id: IndexSegmentId },
    #[error(
        "grep reorganization output `{segment_id}` isn't in a run with the reorganization's level \
         and run number"
    )]
    ReorganizeOutputDescriptorMismatch { segment_id: IndexSegmentId },
}
