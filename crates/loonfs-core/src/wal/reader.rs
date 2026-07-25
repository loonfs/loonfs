//! Loads and validates the WAL segment chain from a base seq through the
//! head.

use super::replay::{
    extend_wal_replay_invariants, validate_wal_segment_for_replay, WalReplayError,
};
use super::{ValidatedWalChain, ValidatedWalSegment, WalChainLoadError, WalChainLoadRequest};
use bytes::Bytes;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::ChangeSeq;
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

const RECENT_SEGMENT_PREFETCH_CONCURRENCY: usize = 8;

/// Fetches the hinted segments covering the replay gap concurrently.
///
/// Failures and misses are silently dropped: the chain walk re-fetches
/// anything the prefetch did not deliver, so hints can only save latency.
async fn prefetch_recent_segments<S: ObjectStore + ?Sized>(
    store: &S,
    hints: &[WalSegmentPointer],
    stop_after_seq: ChangeSeq,
    head_seq: ChangeSeq,
) -> HashMap<String, Bytes> {
    let in_gap: Vec<String> = hints
        .iter()
        .filter(|pointer| pointer.end_seq > stop_after_seq && pointer.end_seq <= head_seq)
        .map(|pointer| pointer.object_key.clone())
        .collect();
    let mut prefetched = HashMap::new();
    for chunk in in_gap.chunks(RECENT_SEGMENT_PREFETCH_CONCURRENCY) {
        let fetches = chunk.iter().map(|object_key| async move {
            let bytes = store.get(object_key, None).await.ok().flatten()?;
            Some((object_key.clone(), bytes))
        });
        prefetched.extend(
            futures::future::join_all(fetches)
                .await
                .into_iter()
                .flatten(),
        );
    }
    prefetched
}

#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "load_validated_wal_chain", key_class = "wal_segment")
)]
pub(crate) async fn load_validated_wal_chain<S: ObjectStore + ?Sized>(
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

    let mut pointer = request
        .visible_tip
        .clone()
        .ok_or(WalChainLoadError::MissingVisibleTip {
            namespace_id: request.namespace_id.clone(),
            seq: request.head_seq,
        })?;
    if pointer.end_seq != request.head_seq {
        return Err(WalChainLoadError::TipEndSeqMismatch {
            expected: request.head_seq,
            actual: pointer.end_seq,
        });
    }

    let stop_after_seq = request.stop_after_seq.unwrap_or(request.chain_base_seq);
    let mut prefetched = if request.recent_segments.is_empty() {
        HashMap::new()
    } else {
        prefetch_recent_segments(
            store,
            request.recent_segments,
            stop_after_seq,
            request.head_seq,
        )
        .await
    };
    let mut reversed = Vec::new();
    loop {
        if pointer.end_seq <= stop_after_seq {
            break;
        }

        let object_key = pointer.object_key.clone();
        let encoded_bytes = match prefetched.remove(&object_key) {
            Some(bytes) => bytes,
            None => store
                .get(&object_key, None)
                .await
                .map_err(|err| WalChainLoadError::ReadWal {
                    object_key: object_key.clone(),
                    message: err.to_string(),
                })?
                .ok_or_else(|| WalChainLoadError::MissingWalObject {
                    object_key: object_key.clone(),
                })?,
        };
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| WalReplayError::Codec(err.to_string()))?;
        validate_pointer_matches_envelope(&pointer, &object_key, &envelope)?;

        let prev = envelope.payload.prev_visible_segment.clone();
        reversed.push(ValidatedWalSegment::new(object_key.clone(), envelope));

        if reversed
            .last()
            .map(|segment| segment.envelope().payload.base_head_seq <= stop_after_seq)
            .unwrap_or(false)
        {
            break;
        }

        pointer = prev.ok_or_else(|| WalReplayError::BrokenChainLink {
            object_key: object_key.clone(),
            required_seq: stop_after_seq,
        })?;
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
        validate_wal_segment_for_replay(
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

/// Counts the visible WAL tail segments above `chain_base_seq` without
/// loading bodies the head already describes.
///
/// Every head publish prepends its tip pointer to `recent_segments` under
/// the same compare-and-swap that installs `visible_wal_tip`, so a hint run
/// that is contiguous from the tip carries the same authority as the tip
/// itself: those segments are counted from pointer seq ranges alone. Only
/// a tail extending past the hinted window (or an unusable hint list) walks
/// chain links, fetching and pointer-validating one body per unresolved
/// segment.
///
/// Unlike [`load_validated_wal_chain`], hint-covered bodies are neither
/// fetched nor checksum-verified, and the manifest boundary is accepted at
/// `base <= chain_base_seq` rather than exact equality: this serves
/// inspection surfaces (status, maintenance gating) whose callers do not
/// replay the tail. Replay consumers keep loading the validated chain.
#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "count_wal_tail_segments", key_class = "wal_segment")
)]
pub(crate) async fn count_visible_wal_tail_segments<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
) -> Result<u64, WalChainLoadError> {
    if request.chain_base_seq > request.head_seq {
        return Err(WalChainLoadError::InvalidSeqRange {
            chain_base_seq: request.chain_base_seq,
            head_seq: request.head_seq,
        });
    }
    if request.chain_base_seq == request.head_seq {
        return Ok(0);
    }
    let tip = request
        .visible_tip
        .clone()
        .ok_or(WalChainLoadError::MissingVisibleTip {
            namespace_id: request.namespace_id.clone(),
            seq: request.head_seq,
        })?;
    if tip.end_seq != request.head_seq {
        return Err(WalChainLoadError::TipEndSeqMismatch {
            expected: request.head_seq,
            actual: tip.end_seq,
        });
    }
    let stop_after_seq = request.stop_after_seq.unwrap_or(request.chain_base_seq);
    if tip.end_seq <= stop_after_seq {
        return Ok(0);
    }

    let mut count: u64 = 1;
    // Newest pointer whose tail membership is decided but whose predecessor
    // is not; the body walk resumes here when pointer math runs out.
    let mut newest_unresolved = tip.clone();
    if pointer_reaches_base(&newest_unresolved, stop_after_seq) {
        return Ok(count);
    }

    if request.recent_segments.first() == Some(&tip) {
        for pointer in &request.recent_segments[1..] {
            if pointer.end_seq.0 + 1 != newest_unresolved.start_seq.0 {
                // Contiguity break: chain links resume authority below.
                break;
            }
            if pointer.end_seq <= stop_after_seq {
                // Fully folded: the tail ends right above this pointer.
                return Ok(count);
            }
            count += 1;
            newest_unresolved = pointer.clone();
            if pointer_reaches_base(&newest_unresolved, stop_after_seq) {
                return Ok(count);
            }
        }
    }

    loop {
        let object_key = newest_unresolved.object_key.clone();
        let encoded_bytes = store
            .get(&object_key, None)
            .await
            .map_err(|err| WalChainLoadError::ReadWal {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| WalChainLoadError::MissingWalObject {
                object_key: object_key.clone(),
            })?;
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| WalReplayError::Codec(err.to_string()))?;
        validate_pointer_matches_envelope(&newest_unresolved, &object_key, &envelope)?;
        let prev = envelope.payload.prev_visible_segment.clone().ok_or(
            WalReplayError::BrokenChainLink {
                object_key,
                required_seq: stop_after_seq,
            },
        )?;
        if prev.end_seq <= stop_after_seq {
            return Ok(count);
        }
        count += 1;
        newest_unresolved = prev;
        if pointer_reaches_base(&newest_unresolved, stop_after_seq) {
            return Ok(count);
        }
    }
}

/// True when the pointer's base (the seq before its first commit) sits at
/// or below the boundary: every older segment is folded and the tail is
/// fully counted.
fn pointer_reaches_base(pointer: &WalSegmentPointer, stop_after_seq: ChangeSeq) -> bool {
    pointer.start_seq.0.saturating_sub(1) <= stop_after_seq.0
}
