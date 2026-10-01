//! WAL object and tail framing types, shared by the writer, reader, and
//! replay paths.

use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{WalObjectEnvelope, WalObjectPayload};
use loonfs_types::{ChangeSeq, NamespaceId, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) type PreparedWalObject =
    loonfs_types::format::envelope::EncodedEnvelope<WalObjectPayload>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalObjectError {
    #[error("WAL object namespace mismatch: expected `{expected}`, actual `{actual}`")]
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
    #[error(
        "WAL object writer epoch mismatch: expected at most `{expected_max}`, actual `{actual}`"
    )]
    WriterEpochMismatch {
        expected_max: WriterEpoch,
        actual: WriterEpoch,
    },
    #[error("WAL object summary does not match its records")]
    SummaryMismatch,
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
pub(crate) struct ValidatedWalObject {
    object_key: String,
    envelope: WalObjectEnvelope,
}

impl ValidatedWalObject {
    pub(super) fn new(object_key: String, envelope: WalObjectEnvelope) -> Self {
        Self {
            object_key,
            envelope,
        }
    }

    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }

    pub(crate) fn envelope(&self) -> &WalObjectEnvelope {
        &self.envelope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedWalTail {
    objects: Vec<ValidatedWalObject>,
}

impl ValidatedWalTail {
    pub(crate) fn new(objects: Vec<ValidatedWalObject>) -> Self {
        Self { objects }
    }

    pub(crate) fn objects(&self) -> &[ValidatedWalObject] {
        &self.objects
    }

    /// The `committed_at_ms` of the newest commit in the tail. A tail of
    /// fences alone has none.
    pub(crate) fn newest_commit_at_ms(&self) -> Option<u64> {
        self.objects
            .iter()
            .rev()
            .find_map(|object| object.envelope().payload().records.last())
            .map(|commit| commit.committed_at_ms)
    }

    #[allow(
        clippy::disallowed_methods,
        reason = "published WAL framing owns its numbered object key"
    )]
    pub(crate) fn push_published(&mut self, envelope: WalObjectEnvelope) {
        let payload = envelope.payload();
        let object_key =
            loonfs_objectstore::keys::wal_object(&payload.namespace_id, &payload.wal_no);
        self.objects
            .push(ValidatedWalObject::new(object_key, envelope));
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
        error: WalObjectError,
    },
}

impl WalTailLoadError {
    pub fn code(&self) -> loonfs_types::ErrorCode {
        match self {
            Self::ReadWal {
                class: crate::error::StoreFailureClass::PermissionDenied,
                ..
            } => loonfs_types::ErrorCode::StoragePermissionDenied,
            Self::ReadWal { .. } => loonfs_types::ErrorCode::ServerError,
            _ => loonfs_types::ErrorCode::NamespaceCorrupt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayedWalTail {
    pub resulting_head: NamespaceReadState,
    pub projected_tail: super::ProjectedWalTail,
}
