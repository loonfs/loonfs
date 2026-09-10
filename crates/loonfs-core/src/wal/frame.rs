//! WAL segment and chain framing types, shared by the writer, reader, and
//! replay paths.

use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::wal::{
    WalCommitDelta, WalCommitPayload, WalSegmentEnvelope, WalSegmentPayload,
};
use loonfs_api::{ChangeSeq, CommitId, NamespaceId, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use thiserror::Error;

pub(crate) type PreparedWalSegment = loonfs_api::wire::envelope::EncodedEnvelope<WalSegmentPayload>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalSegmentError {
    #[error("WAL segment contains no records")]
    EmptySegment,
    #[error("WAL segment namespace mismatch: expected `{expected}`, actual `{actual}`")]
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error("non-contiguous WAL seq: expected `{expected}`, actual `{actual}`")]
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL codec error: {0}")]
    Codec(String),
    #[error("sequence number cannot exceed 9007199254740991")]
    SeqOverflow,
    #[error("WAL segment base head seq mismatch: expected `{expected}`, actual `{actual}`")]
    BaseHeadSeqMismatch {
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
    #[error("WAL segment summary does not match its records")]
    SegmentSummaryMismatch,
}

#[derive(Debug, Clone)]
pub(crate) struct WalChainLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) chain_base_seq: ChangeSeq,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) base_wal_no: WalNo,
    pub(crate) tip_wal_no: WalNo,
    pub(crate) writer_epoch: WriterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub(crate) committed_by: &'a loonfs_api::ActorId,
    pub(crate) committed_at_ms: u64,
    pub(crate) semantic_commit_fingerprint: &'a loonfs_api::CommitFingerprint,
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

    pub(crate) fn envelope(&self) -> &WalSegmentEnvelope {
        &self.envelope
    }

    pub(crate) fn records(&self) -> &[WalCommitPayload] {
        &self.envelope.payload().records
    }

    pub(crate) fn decoded_records(&self) -> impl Iterator<Item = DecodedWalRecord<'_>> {
        let namespace_id = &self.envelope.payload().namespace_id;
        let writer_epoch = self.envelope.payload().writer_epoch;
        self.envelope
            .payload()
            .records
            .iter()
            .map(move |record| DecodedWalRecord {
                namespace_id,
                seq: record.seq,
                writer_epoch,
                commit_id: &record.commit_id,
                committed_by: &record.committed_by,
                committed_at_ms: record.committed_at_ms,
                semantic_commit_fingerprint: &record.semantic_commit_fingerprint,
                message: record.message.as_deref(),
                deltas: Cow::Borrowed(&record.deltas),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedWalChain {
    segments: Vec<ValidatedWalSegment>,
}

impl ValidatedWalChain {
    pub(crate) fn new(segments: Vec<ValidatedWalSegment>) -> Self {
        Self { segments }
    }

    pub(crate) fn segments(&self) -> &[ValidatedWalSegment] {
        &self.segments
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalChainLoadError {
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal {
        object_key: String,
        message: String,
        class: crate::error::StoreFailureClass,
    },
    #[error("missing WAL object `{object_key}`")]
    MissingWalObject { object_key: String },
    #[error("WAL number does not match object key `{object_key}`")]
    NumberMismatch { object_key: String },
    #[error("WAL does not reach head sequence: expected `{expected}`, actual `{actual}`")]
    HeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL replay validation failed: {0}")]
    Replay(#[from] WalSegmentError),
}

impl WalChainLoadError {
    pub fn code(&self) -> loonfs_api::ErrorCode {
        match self {
            Self::ReadWal {
                class: crate::error::StoreFailureClass::PermissionDenied,
                ..
            } => loonfs_api::ErrorCode::StoragePermissionDenied,
            Self::ReadWal { .. } => loonfs_api::ErrorCode::ServerError,
            _ => loonfs_api::ErrorCode::NamespaceCorrupt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayedWalTail {
    pub resulting_head: NamespaceReadState,
    pub resulting_metadata_state: crate::metadata::MetadataState,
}
