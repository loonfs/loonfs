use loonfs_api::{ChangeSeq, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum CommitHeadPublishError {
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
    #[error("expected head etag must not be empty")]
    EmptyExpectedHeadEtag,
    #[error("publish namespace mismatch: head `{head}`, plan `{plan}`")]
    NamespaceMismatch {
        head: NamespaceId,
        plan: NamespaceId,
    },
    #[error("WAL segment namespace mismatch: head `{head}`, wal `{wal}`")]
    WalSegmentNamespaceMismatch { head: NamespaceId, wal: NamespaceId },
    #[error("WAL segment writer epoch mismatch: expected `{expected}`, actual `{actual}`")]
    WalSegmentWriterEpochMismatch {
        expected: WriterEpoch,
        actual: WriterEpoch,
    },
    #[error("WAL segment base head seq mismatch: expected `{expected}`, actual `{actual}`")]
    WalSegmentBaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL segment start seq mismatch: expected `{expected}`, actual `{actual}`")]
    WalSegmentStartSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL segment end seq mismatch: expected `{expected}`, actual `{actual}`")]
    WalSegmentEndSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL segment contains no records")]
    EmptyWalSegment,
    #[error("sequence counter overflow")]
    SeqOverflow,
    #[error("namespace head changed since the publish view was loaded")]
    StaleHead,
    /// The self-enforced publish budget elapsed between starting the WAL
    /// segment PUT and reaching the head CAS. The segment is abandoned as an
    /// orphan for GC and the commit must be rebuilt as a fresh segment, so
    /// callers retry exactly as they do for `StaleHead`.
    #[error("publish budget exceeded: elapsed `{elapsed_ms}` ms over budget `{budget_ms}` ms")]
    PublishBudgetExceeded { elapsed_ms: u64, budget_ms: u64 },
    /// The head compare-and-swap was sent but its outcome was never
    /// observed (for example, a transport failure waiting for the
    /// response). The commit may or may not be visible.
    #[error("head compare-and-swap outcome unknown: {0}")]
    OutcomeUnknown(String),
    #[error("head codec error: {0}")]
    Codec(String),
    #[error("head object store error: {0}")]
    Store(String),
}
