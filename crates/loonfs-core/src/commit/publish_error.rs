use loonfs_api::{ChangeSeq, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CommitHeadPublishError {
    EmptyWriterVersion,
    EmptyExpectedHeadEtag,
    NamespaceMismatch {
        head: NamespaceId,
        plan: NamespaceId,
    },
    WalSegmentNamespaceMismatch {
        head: NamespaceId,
        wal: NamespaceId,
    },
    WalSegmentWriterEpochMismatch {
        expected: WriterEpoch,
        actual: WriterEpoch,
    },
    WalSegmentBaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    WalSegmentStartSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    WalSegmentEndSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    EmptyWalSegment,
    SeqOverflow,
    StaleHead,
    /// The self-enforced publish budget elapsed between starting the WAL
    /// segment PUT and reaching the head CAS. The segment is abandoned as an
    /// orphan for GC and the commit must be rebuilt as a fresh segment, so
    /// callers retry exactly as they do for `StaleHead`.
    PublishBudgetExceeded {
        elapsed_ms: u64,
        budget_ms: u64,
    },
    /// The head compare-and-swap was sent but its outcome was never
    /// observed (for example, a transport failure waiting for the
    /// response). The commit may or may not be visible.
    OutcomeUnknown(String),
    Codec(String),
    Store(String),
}
