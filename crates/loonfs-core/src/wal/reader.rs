//! Loads numbered WAL segments and replays the tail between namespace heads.
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
use loonfs_api::{NamespaceId, WalNo, WriterEpoch};
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
        let envelope = decode_wal_segment_envelope_zstd(&bytes)
            .map_err(|error| WalSegmentError::Codec(error.to_string()))?;
        if envelope.payload().wal_no != wal_no {
            return Err(WalTailLoadError::NumberMismatch {
                object_key: object_key.clone(),
            });
        }
        if envelope.payload().namespace_id != *namespace_id {
            return Err(WalSegmentError::NamespaceMismatch {
                expected: namespace_id.clone(),
                actual: envelope.payload().namespace_id.clone(),
            }
            .into());
        }
        Ok(Some(envelope))
    }
    .await;
    LoadedWalSegment {
        object_key,
        envelope,
    }
}

pub(crate) async fn load_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalTailLoadRequest<'_>,
) -> Result<ValidatedWalTail, WalTailLoadError> {
    let mut segments = Vec::new();
    let mut seq = request.base_seq;
    let mut epoch = WriterEpoch(0);
    for previous in request.base_wal_no.0..request.tip_wal_no.0 {
        let wal_no = WalNo(previous + 1);
        let LoadedWalSegment {
            object_key,
            envelope,
        } = load_wal_segment(store, request.namespace_id, wal_no).await;
        let envelope = envelope?.ok_or_else(|| WalTailLoadError::MissingWalObject {
            object_key: object_key.clone(),
        })?;
        validate_wal_segment_for_replay(request.namespace_id, seq, &envelope)?;
        let payload = envelope.payload();
        if payload.writer_epoch < epoch || payload.writer_epoch > request.writer_epoch {
            return Err(WalSegmentError::WriterEpochMismatch {
                expected_max: request.writer_epoch,
                actual: payload.writer_epoch,
            }
            .into());
        }
        seq = payload.end_seq;
        epoch = payload.writer_epoch;
        segments.push(ValidatedWalSegment::new(object_key, envelope));
    }
    if seq != request.head_seq {
        return Err(WalTailLoadError::HeadSeqMismatch {
            expected: request.head_seq,
            actual: seq,
        });
    }
    Ok(ValidatedWalTail::new(segments))
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
        project_validated_wal_tail(base_head, base_metadata_state, expected_writer_epoch, &tail)?
    };
    ensure_replayed_head_matches(current_head, &replayed.resulting_head)?;
    Ok(replayed)
}
