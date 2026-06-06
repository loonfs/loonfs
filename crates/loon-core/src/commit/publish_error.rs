use loon_api::{ChangeSeq, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    Codec(String),
    Store(String),
}
