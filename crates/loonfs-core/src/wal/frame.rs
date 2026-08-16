//! WAL segment and chain framing types, shared by the writer, reader, and
//! replay paths.

use loonfs_api::wire::control::{HeadState, WalSegmentPointer};
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, CommitId, NamespaceId, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedWalSegment {
    pub envelope: WalSegmentEnvelope,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalBuildError {
    #[error("WAL segment contains no records")]
    EmptySegment,
    #[error("WAL segment namespace mismatch: record `{record}`, segment `{segment}`")]
    SegmentNamespaceMismatch {
        record: NamespaceId,
        segment: NamespaceId,
    },
    #[error("WAL build base head seq mismatch: request `{request}`, plan `{plan}`")]
    BaseHeadSeqMismatch { request: ChangeSeq, plan: ChangeSeq },
    #[error("non-contiguous WAL seq: expected `{expected}`, actual `{actual}`")]
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL codec error: {0}")]
    Codec(String),
    #[error("sequence number cannot exceed 9007199254740991")]
    SeqOverflow,
}

#[derive(Debug, Clone)]
pub(crate) struct WalChainLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) chain_base_seq: ChangeSeq,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) visible_tip: Option<WalSegmentPointer>,
    pub(crate) stop_after_seq: Option<ChangeSeq>,
    /// The head's `recent_segments` accelerator, used only to prefetch the
    /// replay gap concurrently. Chain links stay the sole history
    /// authority: wrong or missing hints cost a fallback fetch, never
    /// correctness.
    pub(crate) recent_segments: &'a [WalSegmentPointer],
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
    pub(crate) actor: &'a loonfs_api::ActorRef,
    pub(crate) committed_at_ms: u64,
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
        self.envelope.pointer()
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
                actor: &record.actor,
                committed_at_ms: record.committed_at_ms,
                semantic_commit_fingerprint: &record.semantic_commit_fingerprint,
                message: record.message.as_deref(),
                deltas: Cow::Borrowed(&record.deltas),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedWalChain {
    segments: Vec<ValidatedWalSegment>,
}

impl ValidatedWalChain {
    pub(crate) fn new(segments: Vec<ValidatedWalSegment>) -> Self {
        Self { segments }
    }

    pub(crate) fn empty() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub(crate) fn segments(&self) -> &[ValidatedWalSegment] {
        &self.segments
    }

    pub(crate) fn decoded_records(&self) -> impl Iterator<Item = DecodedWalRecord<'_>> {
        self.segments
            .iter()
            .flat_map(ValidatedWalSegment::decoded_records)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalChainLoadError {
    #[error("invalid WAL chain seq range: base `{chain_base_seq}` is after head `{head_seq}`")]
    InvalidSeqRange {
        chain_base_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    #[error("missing visible WAL tip for namespace `{namespace_id}` at seq `{seq}`")]
    MissingVisibleTip {
        namespace_id: NamespaceId,
        seq: ChangeSeq,
    },
    #[error("visible WAL tip ends at `{actual}`, expected head seq `{expected}`")]
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
        "WAL chain does not reach expected head seq: expected `{expected}`, actual `{actual}`"
    )]
    HeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL chain suffix does not cover requested cursor `{after_seq}`")]
    CursorNotCovered { after_seq: ChangeSeq },
    #[error(
        "namespace head describes `{described_segments}` visible WAL tail segments reaching down \
         to seq `{described_from_seq}`, which does not reach the tail boundary at seq \
         `{boundary_seq}`"
    )]
    TailNotDescribedByHead {
        boundary_seq: ChangeSeq,
        described_segments: u64,
        described_from_seq: ChangeSeq,
    },
    #[error("wal replay validation failed: {0}")]
    Replay(#[from] WalReplayError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayedWalTail {
    pub resulting_head: HeadState,
    pub resulting_metadata_state: crate::metadata::MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalReplayError {
    #[error("WAL codec error: {0}")]
    Codec(String),
    #[error("WAL segment namespace mismatch: expected `{expected}`, actual `{actual}`")]
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error("WAL segment base head seq mismatch: expected `{expected}`, actual `{actual}`")]
    BaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("non-contiguous WAL seq: expected `{expected}`, actual `{actual}`")]
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error(
        "WAL segment writer epoch mismatch: expected at most `{expected_max}`, actual `{actual}`"
    )]
    WriterEpochMismatch {
        expected_max: WriterEpoch,
        actual: WriterEpoch,
    },
    #[error("WAL segment contains no records")]
    EmptySegment,
    #[error("WAL segment summary does not match its records")]
    SegmentSummaryMismatch,
    #[error(
        "WAL segment `{object_key}` is missing its previous visible segment link before seq `{required_seq}`"
    )]
    BrokenChainLink {
        object_key: String,
        required_seq: ChangeSeq,
    },
    #[error("sequence number cannot exceed 9007199254740991")]
    SeqOverflow,
}
