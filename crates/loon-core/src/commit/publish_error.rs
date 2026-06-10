use loon_api::{ChangeSeq, NamespaceId};
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
    /// The head compare-and-swap was sent but its outcome was never
    /// observed (for example, a transport failure waiting for the
    /// response). The commit may or may not be visible.
    OutcomeUnknown(String),
    Codec(String),
    Store(String),
}
