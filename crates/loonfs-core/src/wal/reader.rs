//! Loads and validates the WAL segment chain from a base seq through the
//! head.

use super::replay::{validate_wal_segment_for_replay, WalReplayError};
use super::{ValidatedWalChain, ValidatedWalSegment, WalChainLoadError, WalChainLoadRequest};
use bytes::Bytes;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

/// Hinted segments fetched at once. The head names every segment a legal
/// unflushed tail can hold, so a replay's whole gap is usually hinted and
/// this is what decides how many round trips it costs.
pub(super) const RECENT_SEGMENT_PREFETCH_CONCURRENCY: usize = 32;

/// The hinted segment keys that cover the replay gap, newest first.
fn hints_in_gap(
    hints: &[WalSegmentPointer],
    stop_after_seq: ChangeSeq,
    head_seq: ChangeSeq,
) -> Vec<String> {
    hints
        .iter()
        .filter(|pointer| pointer.end_seq > stop_after_seq && pointer.end_seq <= head_seq)
        .map(|pointer| pointer.object_key.clone())
        .collect()
}

/// Fetches the hinted segments covering the replay gap concurrently.
///
/// Failures and misses do not stop the load: the chain walk fetches anything
/// the prefetch did not deliver, and a real read error surfaces there as
/// [`WalChainLoadError::ReadWal`]. Hints therefore change where the bytes
/// come from, never what the load returns.
///
/// A failed request is still a request, so the caller counts it against the
/// fetch budget and the walk's own fetch of that segment counts again. A
/// flaky store then drains the budget faster than the chain is long, which
/// an operator reads as a budget problem. The failures are reported here,
/// once per load, so the store shows up as the cause.
async fn prefetch_recent_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    in_gap: &[String],
) -> HashMap<String, Bytes> {
    let mut prefetched = HashMap::new();
    let mut failed_requests: usize = 0;
    let mut first_error: Option<String> = None;
    for chunk in in_gap.chunks(RECENT_SEGMENT_PREFETCH_CONCURRENCY) {
        let fetches = chunk
            .iter()
            .map(|object_key| async move { (object_key, store.get(object_key, None).await) });
        for (object_key, result) in futures::future::join_all(fetches).await {
            match result {
                Ok(Some(bytes)) => {
                    prefetched.insert(object_key.clone(), bytes);
                }
                // A miss is not an error here. The object may legitimately
                // be gone, and the walk reports that on its own fetch.
                Ok(None) => {}
                Err(error) => {
                    failed_requests += 1;
                    first_error.get_or_insert_with(|| error.to_string());
                }
            }
        }
    }
    if let Some(first_error) = first_error {
        tracing::info!(
            namespace_id = %namespace_id,
            failed_requests,
            %first_error,
            "WAL segment prefetch requests failed; the walk fetches those segments again out of the same budget"
        );
    }
    prefetched
}

/// One walk down the chain links, from the visible tip towards the base.
struct WalkedChain {
    /// The segments the walk found, oldest first.
    segments: Vec<ValidatedWalSegment>,
    /// How many `get` requests the load issued, prefetch included. A
    /// request that failed or missed counts, because the round trip
    /// happened; a segment the prefetch delivered is not counted a second
    /// time when the walk consumes it. This is never more than the
    /// `max_segment_fetches` the caller gave.
    fetches: usize,
    /// `true` when the walk stopped at its fetch limit instead of reaching
    /// the base. `segments` is then a partial chain. Nothing may replay
    /// from it, and [`finish_chain`] rejects it.
    stopped_at_limit: bool,
}

impl WalkedChain {
    fn reached(segments: Vec<ValidatedWalSegment>, fetches: usize) -> Self {
        Self {
            segments,
            fetches,
            stopped_at_limit: false,
        }
    }

    fn stopped_at_limit(fetches: usize) -> Self {
        Self {
            segments: Vec::new(),
            fetches,
            stopped_at_limit: true,
        }
    }
}

/// What a bounded chain load produced.
///
/// Both outcomes report `requests_issued`: how many `get` requests the load
/// sent for segment bodies, prefetch included. A caller that meters its own
/// reads charges for that number and nothing else, so its budget drains by
/// what the store was actually asked for. It is never more than the limit
/// the caller gave.
pub(crate) enum WalChainLoad {
    /// The whole chain was read inside the fetch limit.
    Complete {
        chain: ValidatedWalChain,
        requests_issued: usize,
    },
    /// The load stopped at the fetch limit. The chain it walked is partial,
    /// so it is dropped rather than returned, but the requests it spent
    /// getting there are reported so the caller can charge for them.
    LimitReached { requests_issued: usize },
}

/// Loads the chain, issuing at most `max_segment_fetches` requests for
/// segment bodies.
///
/// A caller that meters its own reads uses this so that the loader cannot
/// read past what the caller may pay for. The loader itself keeps no
/// budget, because foreground reads and the changefeed share it.
#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "load_wal_chain_within", key_class = "wal_segment")
)]
pub(crate) async fn load_wal_chain_within<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
    max_segment_fetches: usize,
) -> Result<WalChainLoad, WalChainLoadError> {
    let walked = walk_chain(store, &request, max_segment_fetches).await?;
    if walked.stopped_at_limit {
        return Ok(WalChainLoad::LimitReached {
            requests_issued: walked.fetches,
        });
    }
    Ok(WalChainLoad::Complete {
        chain: finish_chain(&request, walked.segments)?,
        requests_issued: walked.fetches,
    })
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
    // No caller here meters its own reads, so the walk runs to the base.
    // A namespace cannot hold `usize::MAX` segments, so the walk never
    // stops at this limit. If it somehow did, `finish_chain` would reject
    // the partial chain rather than return it.
    let walked = walk_chain(store, &request, usize::MAX).await?;
    finish_chain(&request, walked.segments)
}

/// Walks the chain links from the visible tip down to the base, issuing at
/// most `max_segment_fetches` requests for segment bodies.
async fn walk_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: &WalChainLoadRequest<'_>,
    max_segment_fetches: usize,
) -> Result<WalkedChain, WalChainLoadError> {
    if request.chain_base_seq > request.head_seq {
        return Err(WalChainLoadError::InvalidSeqRange {
            chain_base_seq: request.chain_base_seq,
            head_seq: request.head_seq,
        });
    }
    if request.chain_base_seq == request.head_seq {
        return Ok(WalkedChain::reached(Vec::new(), 0));
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
    let in_gap = hints_in_gap(request.recent_segments, stop_after_seq, request.head_seq);
    // The invariant this function maintains: `fetches` counts every `get`
    // request issued for a segment body, prefetch included, and it never
    // exceeds `max_segment_fetches`.
    //
    // The prefetch issues its requests before the walk can decide it has
    // read enough, so it takes its share of the budget up front and the
    // walk spends what is left. Hints are newest first and the walk starts
    // at the tip, so the prefix the budget covers is the part of the gap
    // the walk reaches first. A hint list longer than the budget is cut
    // down to the budget rather than refused: the walk reads what the
    // budget covers and reports what it spent, so a caller that meters its
    // reads is told where its budget went instead of being handed an empty
    // answer that claims to have cost nothing.
    let prefetched_hints = in_gap.len().min(max_segment_fetches);
    let mut fetches = prefetched_hints;
    let mut prefetched = if prefetched_hints == 0 {
        HashMap::new()
    } else {
        prefetch_recent_segments(store, request.namespace_id, &in_gap[..prefetched_hints]).await
    };
    let mut reversed = Vec::new();
    loop {
        if pointer.end_seq <= stop_after_seq {
            break;
        }

        let object_key = pointer.object_key.clone();
        let encoded_bytes = match prefetched.remove(&object_key) {
            // The prefetch already paid for this body, so consuming it
            // costs nothing further.
            Some(bytes) => bytes,
            None => {
                if fetches >= max_segment_fetches {
                    return Ok(WalkedChain::stopped_at_limit(fetches));
                }
                fetches += 1;
                store
                    .get(&object_key, None)
                    .await
                    .map_err(|err| WalChainLoadError::ReadWal {
                        object_key: object_key.clone(),
                        message: err.to_string(),
                    })?
                    .ok_or_else(|| WalChainLoadError::MissingWalObject {
                        object_key: object_key.clone(),
                    })?
            }
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
    Ok(WalkedChain::reached(reversed, fetches))
}

/// Validates one walked chain as a replayable run and hands it back.
///
/// A partial chain does not survive this: its oldest segment does not sit
/// at the requested base, so the base check below rejects it.
fn finish_chain(
    request: &WalChainLoadRequest<'_>,
    segments: Vec<ValidatedWalSegment>,
) -> Result<ValidatedWalChain, WalChainLoadError> {
    let Some(first_segment) = segments.first() else {
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
    for segment in &segments {
        validate_wal_segment_for_replay(
            request.namespace_id,
            expected_base_seq,
            segment.object_key(),
            segment.envelope(),
        )?;
        expected_base_seq = segment.envelope().payload.end_seq;
    }

    if expected_base_seq != request.head_seq {
        return Err(WalChainLoadError::HeadSeqMismatch {
            expected: request.head_seq,
            actual: expected_base_seq,
        });
    }

    Ok(ValidatedWalChain::new(segments))
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

/// Counts the visible WAL tail segments above `chain_base_seq` from the
/// head's chain pointers alone.
///
/// Every head publish prepends its tip pointer to `recent_segments` under
/// the same compare-and-swap that installs `visible_wal_tip`, so a hint run
/// that is contiguous from the tip carries the same authority as the tip
/// itself. The list holds every segment a legal unflushed tail can reach
/// (see [`crate::limits::RECENT_SEGMENTS_LIMIT`]), so that run describes
/// the whole tail and this takes no store parameter: the signature is the
/// guarantee.
///
/// A head that under-describes its own tail is corrupted, and is reported as
/// [`WalChainLoadError::TailNotDescribedByHead`] rather than walked. This
/// serves inspection surfaces (status, maintenance gating), and a serial
/// chain walk of an unbounded tail is not something a foreground call
/// should pay for silently. Replay consumers keep loading the validated
/// chain, which does walk.
///
/// Unlike [`load_validated_wal_chain`], bodies are never fetched or
/// checksum-verified, and the manifest boundary is accepted at
/// `base <= chain_base_seq` rather than exact equality: callers here do not
/// replay the tail.
#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "count_wal_tail_segments", key_class = "wal_segment")
)]
pub(crate) fn count_visible_wal_tail_segments(
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
    // Oldest pointer counted so far: the run has to keep descending from
    // here, contiguously, until it crosses the boundary.
    let mut oldest_counted = &tip;
    if pointer_reaches_base(oldest_counted, stop_after_seq) {
        return Ok(count);
    }

    // A decoded head always begins its accelerator at its tip. This request
    // carries a plain pointer slice rather than a head, and the count reads
    // no segment bodies, so it checks that relationship here rather than
    // assuming it.
    if request.recent_segments.first() == Some(&tip) {
        for pointer in &request.recent_segments[1..] {
            if pointer.end_seq.0 + 1 != oldest_counted.start_seq.0 {
                // Contiguity break: nothing below is described.
                break;
            }
            if pointer.end_seq <= stop_after_seq {
                // Fully folded: the tail ends right above this pointer.
                return Ok(count);
            }
            count += 1;
            oldest_counted = pointer;
            if pointer_reaches_base(oldest_counted, stop_after_seq) {
                return Ok(count);
            }
        }
    }

    Err(WalChainLoadError::TailNotDescribedByHead {
        boundary_seq: stop_after_seq,
        described_segments: count,
        described_from_seq: oldest_counted.start_seq,
    })
}

/// True when the pointer's base (the seq before its first commit) sits at
/// or below the boundary: every older segment is folded and the tail is
/// fully counted.
fn pointer_reaches_base(pointer: &WalSegmentPointer, stop_after_seq: ChangeSeq) -> bool {
    pointer.start_seq.0.saturating_sub(1) <= stop_after_seq.0
}
