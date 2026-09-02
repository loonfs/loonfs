//! [`CommitHeadPublishError`]: failures of the segment PUT and head
//! compare-and-swap.

use loonfs_api::ErrorCode;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommitHeadPublishError {
    #[error("expected head etag must not be empty")]
    EmptyExpectedHeadEtag,
    #[error("wal segment does not connect at `{field}`: expected `{expected}`, actual `{actual}`")]
    SegmentDoesNotConnect {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("WAL segment contains no records")]
    EmptyWalSegment,
    #[error("sequence number cannot exceed 9007199254740991")]
    SeqOverflow,
    #[error("namespace head changed since the publish view was loaded")]
    StaleHead,
    /// The self-enforced publish budget elapsed between starting the WAL
    /// segment PUT and reaching the head CAS. The segment is abandoned as an
    /// orphan for GC and the commit must be rebuilt as a fresh segment, so
    /// callers retry exactly as they do for `StaleHead`.
    #[error("publish budget exceeded: elapsed {elapsed_ms}ms over budget {budget_ms}ms")]
    PublishBudgetExceeded { elapsed_ms: u64, budget_ms: u64 },
    /// The head compare-and-swap was sent but its outcome was never
    /// observed (for example, a transport failure waiting for the
    /// response). The commit may or may not be visible.
    #[error("head compare-and-swap outcome unknown: {0}")]
    OutcomeUnknown(String),
    #[error("head codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("head object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

impl CommitHeadPublishError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::StaleHead | Self::PublishBudgetExceeded { .. } => ErrorCode::StaleHead,
            Self::OutcomeUnknown(_) => ErrorCode::CommitOutcomeUnknown,
            Self::EmptyExpectedHeadEtag
            | Self::SegmentDoesNotConnect { .. }
            | Self::EmptyWalSegment
            | Self::SeqOverflow
            | Self::Codec { .. }
            | Self::Store { .. } => ErrorCode::ServerError,
        }
    }
}
