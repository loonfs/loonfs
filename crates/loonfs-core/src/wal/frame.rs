//! WAL object and tail framing types, shared by the writer, reader, and
//! replay paths.

use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{WalObjectEnvelope, WalObjectPayload};
use loonfs_types::{ChangeSeq, NamespaceId, WalNo, WriterEpoch};
use std::sync::Arc;
use thiserror::Error;

pub(crate) type PreparedWalObject =
    loonfs_types::format::envelope::EncodedEnvelope<WalObjectPayload>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
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
    #[error("WAL codec error for `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    #[error("sequence number cannot exceed 9007199254740991")]
    SeqOverflow,
    #[error("activity counter cannot exceed 9007199254740991")]
    ActivityOverflow,
    #[error(
        "WAL object writer epoch mismatch: expected at most `{expected_max}`, actual `{actual}`"
    )]
    WriterEpochMismatch {
        expected_max: WriterEpoch,
        actual: WriterEpoch,
    },
    #[error("WAL object summary does not match its records")]
    SummaryMismatch,
    #[error(
        "WAL object `{wal_no}` lowers the basis floor to `{basis_floor_seq}` from \
         `{prior_basis_floor_seq}` at WAL number `{prior_wal_no}`"
    )]
    BasisFloorDecrease {
        wal_no: WalNo,
        basis_floor_seq: ChangeSeq,
        prior_wal_no: WalNo,
        prior_basis_floor_seq: ChangeSeq,
    },
}

#[derive(Debug, Clone)]
pub(super) struct WalTailLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) base_seq: ChangeSeq,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) base_wal_no: WalNo,
    pub(crate) base_basis_floor_seq: ChangeSeq,
    pub(crate) tip_wal_no: WalNo,
    pub(crate) writer_epoch: WriterEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedWalObject {
    object_key: String,
    envelope: Arc<WalObjectEnvelope>,
}

impl ValidatedWalObject {
    pub(super) fn new(object_key: String, envelope: Arc<WalObjectEnvelope>) -> Self {
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

    pub(crate) fn push_published(&mut self, envelope: WalObjectEnvelope) {
        let payload = envelope.payload();
        let object_key = super::wal_object_key(&payload.namespace_id, &payload.wal_no);
        self.objects
            .push(ValidatedWalObject::new(object_key, Arc::new(envelope)));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum WalTailLoadError {
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal {
        object_key: String,
        message: String,
        kind: loonfs_objectstore::ObjectStoreErrorKind,
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
            Self::ReadWal { kind, .. } => crate::error::classify_store_failure(*kind),
            _ => loonfs_types::ErrorCode::NamespaceCorrupt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayedWalTail {
    pub resulting_head: NamespaceReadState,
    pub projected_tail: super::ProjectedWalTail,
}
