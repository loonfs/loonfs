//! Reads numbered WAL segments: one at a time after a position, or a bounded tail between two heads.
// This module is the physical WAL boundary.
#![allow(clippy::disallowed_methods)]

use super::frame::ReplayedWalTail;
use super::replay::{
    ensure_replayed_head_matches, project_validated_wal_tail, validate_wal_segment_for_replay,
};
use super::{
    ValidatedWalSegment, ValidatedWalTail, WalSegmentError, WalTailLoadError, WalTailLoadRequest,
};
use crate::error::MetadataProjectionLoadError;
use crate::metadata::MetadataState;
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, NamespaceId, WalNo, WriterEpoch};
use loonfs_objectstore::keys::wal_segment;
use loonfs_objectstore::ObjectStore;

// Missing and malformed objects still need their numbered key in caller diagnostics.
pub(super) struct LoadedWalSegment {
    pub(super) object_key: String,
    pub(super) envelope: Result<Option<WalSegmentEnvelope>, WalTailLoadError>,
}

pub(super) async fn load_wal_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> LoadedWalSegment {
    let object_key = wal_segment(namespace_id, &wal_no);
    let envelope = async {
        let Some(bytes) =
            store
                .get(&object_key, None)
                .await
                .map_err(|error| WalTailLoadError::ReadWal {
                    object_key: object_key.clone(),
                    message: error.public_message().into_owned(),
                    class: crate::error::StoreFailureClass::of(&error),
                })?
        else {
            return Ok(None);
        };
        let envelope =
            decode_wal_segment_envelope_zstd(&bytes).map_err(|error| WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error: WalSegmentError::Codec(error.to_string()),
            })?;
        if envelope.payload().wal_no != wal_no {
            return Err(WalTailLoadError::NumberMismatch {
                object_key: object_key.clone(),
            });
        }
        if envelope.payload().namespace_id != *namespace_id {
            return Err(WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error: WalSegmentError::NamespaceMismatch {
                    expected: namespace_id.clone(),
                    actual: envelope.payload().namespace_id.clone(),
                },
            });
        }
        Ok(Some(envelope))
    }
    .await;
    LoadedWalSegment {
        object_key,
        envelope,
    }
}

/// Reads consecutive numbered segments after a position, each validated as
/// the contiguous successor of the one before.
pub(super) struct WalWalk<'a> {
    namespace_id: &'a NamespaceId,
    wal_no: WalNo,
    seq: ChangeSeq,
    tip: Option<WalNo>,
}

impl<'a> WalWalk<'a> {
    /// Reads until the first absent number.
    pub(super) fn after(namespace_id: &'a NamespaceId, wal_no: WalNo, seq: ChangeSeq) -> Self {
        Self {
            namespace_id,
            wal_no,
            seq,
            tip: None,
        }
    }

    /// Reads every number through `tip`; an absent one is missing history.
    pub(super) fn through(self, tip: WalNo) -> Self {
        Self {
            tip: Some(tip),
            ..self
        }
    }

    pub(super) fn seq(&self) -> ChangeSeq {
        self.seq
    }

    pub(super) fn object_key(&self) -> String {
        wal_segment(self.namespace_id, &self.wal_no)
    }

    pub(super) async fn next<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<Option<ValidatedWalSegment>, WalTailLoadError> {
        if self.tip.is_some_and(|tip| self.wal_no >= tip) {
            return Ok(None);
        }
        let Ok(wal_no) = self.wal_no.successor() else {
            return Ok(None);
        };
        let LoadedWalSegment {
            object_key,
            envelope,
        } = load_wal_segment(store, self.namespace_id, wal_no).await;
        let Some(envelope) = envelope? else {
            return match self.tip {
                Some(_) => Err(WalTailLoadError::MissingWalObject { object_key }),
                None => Ok(None),
            };
        };
        validate_wal_segment_for_replay(self.namespace_id, self.seq, &envelope).map_err(
            |error| WalTailLoadError::Replay {
                object_key: object_key.clone(),
                error,
            },
        )?;
        self.wal_no = wal_no;
        self.seq = envelope.payload().end_seq;
        Ok(Some(ValidatedWalSegment::new(object_key, envelope)))
    }
}

pub(super) async fn load_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalTailLoadRequest<'_>,
) -> Result<ValidatedWalTail, WalTailLoadError> {
    let mut walk = WalWalk::after(request.namespace_id, request.base_wal_no, request.base_seq)
        .through(request.tip_wal_no);
    let mut segments = Vec::new();
    let mut epoch = WriterEpoch(0);
    while let Some(segment) = walk.next(store).await? {
        let writer_epoch = segment.envelope().payload().writer_epoch;
        if writer_epoch < epoch || writer_epoch > request.writer_epoch {
            return Err(WalTailLoadError::Replay {
                object_key: segment.object_key().to_owned(),
                error: WalSegmentError::WriterEpochMismatch {
                    expected_max: request.writer_epoch,
                    actual: writer_epoch,
                },
            });
        }
        epoch = writer_epoch;
        segments.push(segment);
    }
    if walk.seq() != request.head_seq {
        return Err(WalTailLoadError::HeadSeqMismatch {
            object_key: walk.object_key(),
            expected: request.head_seq,
            actual: walk.seq(),
        });
    }
    Ok(ValidatedWalTail::new(segments))
}

/// Loads every retained segment, from the retention floor through the head.
pub(crate) async fn load_retained_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    head: &NamespaceReadState,
    retention_floor_seq: ChangeSeq,
) -> Result<ValidatedWalTail, WalTailLoadError> {
    load_wal_tail(
        store,
        WalTailLoadRequest {
            namespace_id: &head.namespace_id,
            base_seq: retention_floor_seq,
            head_seq: head.seq,
            base_wal_no: head.retention_floor_wal_no,
            tip_wal_no: head.wal_no,
            writer_epoch: head.writer_epoch,
        },
    )
    .await
}

pub(crate) async fn load_replayed_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    base_head: &NamespaceReadState,
    current_head: &NamespaceReadState,
    base_metadata_state: &MetadataState,
    expected_writer_epoch: Option<WriterEpoch>,
) -> Result<ReplayedWalTail, MetadataProjectionLoadError> {
    let tail = load_wal_tail(
        store,
        WalTailLoadRequest {
            namespace_id: &current_head.namespace_id,
            base_seq: base_head.seq,
            head_seq: current_head.seq,
            base_wal_no: base_head.wal_no,
            tip_wal_no: current_head.wal_no,
            writer_epoch: current_head.writer_epoch,
        },
    )
    .await?;
    let replayed = {
        let _span =
            tracing::debug_span!("loonfs.phase", phase = "project_metadata_state").entered();
        project_validated_wal_tail(
            base_head,
            &super::ProjectedWalTail::from_rows(base_metadata_state.clone()),
            expected_writer_epoch,
            &tail,
        )?
    };
    ensure_replayed_head_matches(current_head, &replayed.resulting_head)?;
    Ok(replayed)
}
