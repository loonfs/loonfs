use crate::invariants::InvariantId;
use crate::metadata::MetadataApplyError;
use loonfs_api::wire::control::{HeadState, WalSegmentPointer};
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, CommitId, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedWalSegment {
    pub object_key: String,
    pub segment_id: String,
    pub envelope: WalSegmentEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<InvariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalBuildError {
    EmptyWriterVersion,
    EmptySegment,
    NamespaceMismatch {
        request: NamespaceId,
        plan: NamespaceId,
    },
    BaseHeadSeqMismatch {
        request: ChangeSeq,
        plan: ChangeSeq,
    },
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    Codec(String),
}

#[derive(Debug, Clone)]
pub(crate) struct WalChainLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) chain_base_seq: ChangeSeq,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) visible_tip: Option<WalSegmentPointer>,
    pub(crate) stop_after_seq: Option<ChangeSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedWalSegment {
    object_key: String,
    envelope: WalSegmentEnvelope,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedWalRecord<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) seq: ChangeSeq,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) commit_id: &'a CommitId,
    pub(crate) semantic_commit_fingerprint: &'a str,
    pub(crate) message: Option<&'a str>,
    pub(crate) deltas: Cow<'a, [WalCommitDelta]>,
}

impl ValidatedWalSegment {
    pub(crate) fn new(object_key: String, envelope: WalSegmentEnvelope) -> Self {
        Self {
            object_key,
            envelope,
        }
    }

    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }

    pub(crate) fn envelope(&self) -> &WalSegmentEnvelope {
        &self.envelope
    }

    pub(crate) fn records(&self) -> &[WalCommitPayload] {
        &self.envelope.payload.records
    }

    pub(crate) fn pointer(&self) -> WalSegmentPointer {
        self.envelope.pointer(self.object_key.clone())
    }

    pub(crate) fn decoded_records(&self) -> impl Iterator<Item = DecodedWalRecord<'_>> {
        let namespace_id = &self.envelope.payload.namespace_id;
        let writer_epoch = self.envelope.payload.writer_epoch;
        self.envelope
            .payload
            .records
            .iter()
            .map(move |record| DecodedWalRecord {
                namespace_id,
                seq: record.seq,
                writer_epoch,
                commit_id: &record.commit_id,
                semantic_commit_fingerprint: &record.semantic_commit_fingerprint,
                message: record.message.as_deref(),
                deltas: Cow::Borrowed(&record.deltas),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedWalChain {
    segments: Vec<ValidatedWalSegment>,
    checked_invariants: Vec<InvariantId>,
}

impl ValidatedWalChain {
    pub(crate) fn new(
        segments: Vec<ValidatedWalSegment>,
        checked_invariants: Vec<InvariantId>,
    ) -> Self {
        Self {
            segments,
            checked_invariants,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            segments: Vec::new(),
            checked_invariants: Vec::new(),
        }
    }

    pub(crate) fn segments(&self) -> &[ValidatedWalSegment] {
        &self.segments
    }

    pub(crate) fn checked_invariants(&self) -> &[InvariantId] {
        &self.checked_invariants
    }

    pub(crate) fn decoded_records(&self) -> impl Iterator<Item = DecodedWalRecord<'_>> {
        self.segments
            .iter()
            .flat_map(ValidatedWalSegment::decoded_records)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalChainLoadError {
    #[error("invalid WAL chain seq range: base `{chain_base_seq:?}` is after head `{head_seq:?}`")]
    InvalidSeqRange {
        chain_base_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    #[error("missing visible WAL tip for seq `{seq:?}` under `{prefix}`")]
    MissingVisibleTip { prefix: String, seq: ChangeSeq },
    #[error("visible WAL tip ends at `{actual:?}`, expected head seq `{expected:?}`")]
    TipEndSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal { object_key: String, message: String },
    #[error("missing WAL object `{object_key}`")]
    MissingWalObject { object_key: String },
    #[error("WAL pointer does not match segment payload for `{object_key}`")]
    PointerMismatch { object_key: String },
    #[error(
        "WAL chain does not reach expected head seq: expected `{expected:?}`, actual `{actual:?}`"
    )]
    HeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL chain suffix does not cover requested cursor `{after_seq:?}`")]
    CursorNotCovered { after_seq: ChangeSeq },
    #[error("wal replay validation failed: {0:?}")]
    Replay(WalReplayError),
}

impl From<WalReplayError> for WalChainLoadError {
    fn from(value: WalReplayError) -> Self {
        Self::Replay(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplayedWalTail {
    pub resulting_head: HeadState,
    pub resulting_metadata_state: crate::metadata::MetadataState,
    pub checked_invariants: Vec<InvariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalReplayError {
    Codec(String),
    ObjectKeyMismatch {
        expected: String,
        actual: String,
    },
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    BaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    WriterEpochMismatch {
        expected_max: WriterEpoch,
        actual: WriterEpoch,
    },
    EmptySegment,
    SegmentSummaryMismatch,
    MetadataApply(MetadataApplyError),
    SeqOverflow,
}
