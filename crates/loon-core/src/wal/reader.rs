use super::replay::{extend_wal_replay_invariants, validate_decoded_replayed_wal, WalReplayError};
use super::{ValidatedWalChain, ValidatedWalSegment, WalChainLoadError, WalChainLoadRequest};
use loon_api::wire::control::WalSegmentPointer;
use loon_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loon_objectstore::ObjectStore;

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "load_validated_wal_chain", key_class = "wal_segment")
)]
pub(crate) fn load_validated_wal_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
) -> Result<ValidatedWalChain, WalChainLoadError> {
    if request.chain_base_seq > request.head_seq {
        return Err(WalChainLoadError::InvalidSeqRange {
            chain_base_seq: request.chain_base_seq,
            head_seq: request.head_seq,
        });
    }
    if request.chain_base_seq == request.head_seq {
        return Ok(ValidatedWalChain::empty());
    }

    let prefix = format!("namespaces/{}/wal/", request.namespace_id.as_str());
    let mut pointer = request
        .visible_tip
        .clone()
        .ok_or(WalChainLoadError::MissingVisibleTip {
            prefix,
            seq: request.head_seq,
        })?;
    if pointer.end_seq != request.head_seq {
        return Err(WalChainLoadError::TipEndSeqMismatch {
            expected: request.head_seq,
            actual: pointer.end_seq,
        });
    }

    let stop_after_seq = request.stop_after_seq.unwrap_or(request.chain_base_seq);
    let mut reversed = Vec::new();
    loop {
        if pointer.end_seq <= stop_after_seq {
            break;
        }

        let object_key = pointer.object_key.clone();
        let encoded_bytes = store
            .get(&object_key, None)
            .map_err(|err| WalChainLoadError::ReadWal {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| WalChainLoadError::MissingWalObject {
                object_key: object_key.clone(),
            })?;
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| WalReplayError::Codec(err.to_string()))?;
        validate_pointer_matches_envelope(&pointer, &object_key, &envelope)?;

        let prev = envelope.payload.prev_visible_segment.clone();
        reversed.push(ValidatedWalSegment::new(object_key, envelope));

        if reversed
            .last()
            .map(|segment| segment.envelope().payload.base_head_seq <= stop_after_seq)
            .unwrap_or(false)
        {
            break;
        }

        pointer = prev.ok_or(WalReplayError::SegmentSummaryMismatch)?;
    }

    reversed.reverse();

    let Some(first_segment) = reversed.first() else {
        return Ok(ValidatedWalChain::empty());
    };
    if let Some(after_seq) = request.stop_after_seq {
        if first_segment.envelope().payload.base_head_seq > after_seq
            || first_segment.envelope().payload.end_seq <= after_seq
        {
            return Err(WalChainLoadError::CursorNotCovered { after_seq });
        }
    }

    let mut expected_base_seq = first_segment.envelope().payload.base_head_seq;
    if request.stop_after_seq.is_none() && expected_base_seq != request.chain_base_seq {
        return Err(WalChainLoadError::HeadSeqMismatch {
            expected: request.chain_base_seq,
            actual: expected_base_seq,
        });
    }
    let mut checked_invariants = Vec::new();
    for segment in &reversed {
        validate_decoded_replayed_wal(
            request.namespace_id,
            expected_base_seq,
            segment.object_key(),
            segment.envelope(),
        )?;
        expected_base_seq = segment.envelope().payload.end_seq;
        extend_wal_replay_invariants(&mut checked_invariants);
    }

    if expected_base_seq != request.head_seq {
        return Err(WalChainLoadError::HeadSeqMismatch {
            expected: request.head_seq,
            actual: expected_base_seq,
        });
    }

    Ok(ValidatedWalChain::new(reversed, checked_invariants))
}

fn validate_pointer_matches_envelope(
    pointer: &WalSegmentPointer,
    object_key: &str,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalChainLoadError> {
    if envelope.pointer(object_key.to_owned()) != *pointer {
        return Err(WalChainLoadError::PointerMismatch {
            object_key: object_key.to_owned(),
        });
    }
    Ok(())
}
