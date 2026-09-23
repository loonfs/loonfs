//! WAL segment and tail framing types, shared by the writer, reader, and
//! replay paths.

use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::wal::{WalSegmentEnvelope, WalSegmentPayload};
use loonfs_api::{ChangeSeq, NamespaceId, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
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
    #[error("activity counter cannot exceed 9007199254740991")]
    ActivityOverflow,
    #[error("WAL number cannot exceed 9007199254740991")]
    NumberOverflow,
    #[error("WAL segment prior head seq mismatch: expected `{expected}`, actual `{actual}`")]
    PriorHeadSeqMismatch {
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
pub(super) struct WalTailLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) base_seq: ChangeSeq,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedWalTail {
    segments: Vec<ValidatedWalSegment>,
}

impl ValidatedWalTail {
    pub(crate) fn new(segments: Vec<ValidatedWalSegment>) -> Self {
        Self { segments }
    }

    pub(crate) fn segments(&self) -> &[ValidatedWalSegment] {
        &self.segments
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalTailLoadError {
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
    #[error(
        "WAL through object `{object_key}` reaches sequence `{actual}`, expected head sequence `{expected}`"
    )]
    HeadSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("WAL object `{object_key}` failed replay validation: {error}")]
    Replay {
        object_key: String,
        error: WalSegmentError,
    },
}

impl WalTailLoadError {
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
    pub projected_tail: super::ProjectedWalTail,
}
