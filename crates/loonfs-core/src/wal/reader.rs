//! Loads and verifies contiguous numbered WAL segments.

use super::replay::validate_wal_segment_for_replay;
use super::{
    ValidatedWalChain, ValidatedWalSegment, WalChainLoadError, WalChainLoadRequest, WalSegmentError,
};
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{NamespaceId, WalNo, WriterEpoch};
use loonfs_objectstore::keys::wal_segment;
use loonfs_objectstore::ObjectStore;

pub(crate) async fn load_wal_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
) -> Result<Option<WalSegmentEnvelope>, WalChainLoadError> {
    let object_key = wal_segment(namespace_id, &wal_no);
    let Some(bytes) =
        store
            .get(&object_key, None)
            .await
            .map_err(|error| WalChainLoadError::ReadWal {
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
        return Err(WalChainLoadError::NumberMismatch { object_key });
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

pub(crate) async fn load_wal_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
) -> Result<ValidatedWalChain, WalChainLoadError> {
    let mut segments = Vec::new();
    let mut seq = request.chain_base_seq;
    let mut epoch = WriterEpoch(0);
    for previous in request.base_wal_no.0..request.tip_wal_no.0 {
        let wal_no = WalNo(previous + 1);
        let object_key = wal_segment(request.namespace_id, &wal_no);
        let envelope = load_wal_segment(store, request.namespace_id, wal_no)
            .await?
            .ok_or_else(|| WalChainLoadError::MissingWalObject {
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
        return Err(WalChainLoadError::HeadSeqMismatch {
            expected: request.head_seq,
            actual: seq,
        });
    }
    Ok(ValidatedWalChain::new(segments))
}
