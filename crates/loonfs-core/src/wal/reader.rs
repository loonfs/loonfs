//! Loads and validates the WAL segment chain from a base seq through the
//! head.

use super::replay::{validate_wal_segment_for_replay, WalReplayError};
use super::{ValidatedWalChain, ValidatedWalSegment, WalChainLoadError, WalChainLoadRequest};
use bytes::Bytes;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_objectstore::keys::wal_segment;
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, HashSet};

/// Hinted segments fetched at once. The head names every segment a legal
/// unflushed tail can hold, so a replay's whole gap is usually hinted and
/// this is what decides how many round trips it costs.
pub(super) const RECENT_SEGMENT_PREFETCH_CONCURRENCY: usize = 32;

impl WalChainLoadRequest<'_> {
    fn tip_and_stop(&self) -> Result<Option<(WalSegmentPointer, ChangeSeq)>, WalChainLoadError> {
        if self.chain_base_seq > self.head_seq {
            return Err(WalChainLoadError::InvalidSeqRange {
                chain_base_seq: self.chain_base_seq,
                head_seq: self.head_seq,
            });
        }
        if self.chain_base_seq == self.head_seq {
            return Ok(None);
        }
        let tip = self
            .visible_tip
            .clone()
            .ok_or(WalChainLoadError::MissingVisibleTip {
                namespace_id: self.namespace_id.clone(),
                seq: self.head_seq,
            })?;
        if tip.end_seq != self.head_seq {
            return Err(WalChainLoadError::TipEndSeqMismatch {
                expected: self.head_seq,
                actual: tip.end_seq,
            });
        }
        let stop_after_seq = self.stop_after_seq.unwrap_or(self.chain_base_seq);
        if tip.end_seq <= stop_after_seq {
            return Ok(None);
        }
        Ok(Some((tip, stop_after_seq)))
    }
}

/// The hinted segment keys that cover the replay gap, newest first.
fn hints_in_gap(
    namespace_id: &NamespaceId,
    tip: &WalSegmentPointer,
    hints: &[WalSegmentPointer],
    stop_after_seq: ChangeSeq,
    head_seq: ChangeSeq,
) -> Vec<String> {
    let mut seen = HashSet::new();
    std::iter::once(tip)
        .chain(hints)
        .filter(|pointer| pointer.end_seq > stop_after_seq && pointer.end_seq <= head_seq)
        .map(|pointer| wal_segment(namespace_id, &pointer.segment_id))
        .filter(|object_key| seen.insert(object_key.clone()))
        .collect()
}

/// Concurrently fetches hinted WAL segments in the replay range.
///
/// A miss or failed prefetch does not fail the load; the normal chain walk
/// fetches the segment again and reports any final read error. Every
/// prefetch request still counts against the caller's fetch budget, including
/// failures.
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
                    first_error.get_or_insert_with(|| error.public_message().into_owned());
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
    /// Complete segments in oldest-first order. Empty when a limited walk
    /// stops before reaching the base.
    segments: Vec<ValidatedWalSegment>,
    /// How many `get` requests the load issued, prefetch included. A request
    /// that failed or missed counts, because the round trip happened; a
    /// segment the prefetch delivered is not counted again when consumed.
    fetches: usize,
    limit_reached: bool,
}

/// Result of loading a WAL chain with a fetch limit.
///
/// `requests_issued` counts all segment-body requests, including prefetches,
/// misses, and failed requests. It never exceeds the supplied limit.
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
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "load_wal_chain_within", key_class = "wal_segment")
)]
pub(crate) async fn load_wal_chain_within<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
    max_segment_fetches: usize,
) -> Result<WalChainLoad, WalChainLoadError> {
    let walked = walk_chain(store, &request, Some(max_segment_fetches)).await?;
    if walked.limit_reached {
        Ok(WalChainLoad::LimitReached {
            requests_issued: walked.fetches,
        })
    } else {
        Ok(WalChainLoad::Complete {
            chain: finish_chain(&request, walked.segments)?,
            requests_issued: walked.fetches,
        })
    }
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "load_validated_wal_chain", key_class = "wal_segment")
)]
pub(crate) async fn load_validated_wal_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
) -> Result<ValidatedWalChain, WalChainLoadError> {
    let walked = walk_chain(store, &request, None).await?;
    finish_chain(&request, walked.segments)
}

/// Walks the chain links from the visible tip down to the base, optionally
/// limiting requests for segment bodies.
async fn walk_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: &WalChainLoadRequest<'_>,
    max_segment_fetches: Option<usize>,
) -> Result<WalkedChain, WalChainLoadError> {
    let Some((mut pointer, stop_after_seq)) = request.tip_and_stop()? else {
        return Ok(WalkedChain {
            segments: Vec::new(),
            fetches: 0,
            limit_reached: false,
        });
    };
    let in_gap = hints_in_gap(
        request.namespace_id,
        &pointer,
        request.recent_segments,
        stop_after_seq,
        request.head_seq,
    );
    // `fetches` counts every segment-body request, including prefetches, and
    // never exceeds `max_segment_fetches`. Prefetch consumes its share first;
    // the chain walk uses the remaining budget. Because hints are ordered from
    // newest to oldest, a limited prefetch covers the segments nearest the tip.
    let prefetched_hints =
        max_segment_fetches.map_or(in_gap.len(), |limit| in_gap.len().min(limit));
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

        let object_key = wal_segment(request.namespace_id, &pointer.segment_id);
        let encoded_bytes = match prefetched.remove(&object_key) {
            // The prefetch already paid for this body, so consuming it
            // costs nothing further.
            Some(bytes) => bytes,
            None => {
                if max_segment_fetches.is_some_and(|limit| fetches >= limit) {
                    return Ok(WalkedChain {
                        segments: Vec::new(),
                        fetches,
                        limit_reached: true,
                    });
                }
                fetches += 1;
                store
                    .get(&object_key, None)
                    .await
                    .map_err(|err| WalChainLoadError::ReadWal {
                        object_key: object_key.clone(),
                        message: err.public_message().into_owned(),
                    })?
                    .ok_or_else(|| WalChainLoadError::MissingWalObject {
                        object_key: object_key.clone(),
                    })?
            }
        };
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| WalReplayError::Codec(err.to_string()))?;
        validate_pointer_matches_envelope(&pointer, &object_key, &envelope)?;

        let reached_stop = envelope.payload.base_head_seq <= stop_after_seq;
        let prev = envelope.payload.prev_visible_segment.clone();
        reversed.push(ValidatedWalSegment::new(object_key.clone(), envelope));

        if reached_stop {
            break;
        }

        pointer = prev.ok_or_else(|| WalReplayError::BrokenChainLink {
            object_key: object_key.clone(),
            required_seq: stop_after_seq,
        })?;
    }

    reversed.reverse();
    Ok(WalkedChain {
        segments: reversed,
        fetches,
        limit_reached: false,
    })
}

/// Validates one walked chain as a replayable run and hands it back.
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
    if envelope.pointer() != *pointer {
        return Err(WalChainLoadError::PointerMismatch {
            object_key: object_key.to_owned(),
        });
    }
    Ok(())
}

/// Counts visible WAL-tail segments from the pointers stored in the head.
///
/// Head publication updates `visible_wal_tip` and its `recent_segments`
/// predecessor hints in the same compare-and-swap, so those pointers together
/// describe the complete legal tail. The function performs no object-store
/// reads.
///
/// If the head does not describe its complete tail, the namespace is
/// reported as corrupt rather than triggering an unbounded foreground chain
/// walk. Segment bodies and checksums are not validated because this method
/// only counts pointers; replay paths still load the full validated chain.
#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "count_wal_tail_segments", key_class = "wal_segment")
)]
pub(crate) fn count_visible_wal_tail_segments(
    request: &WalChainLoadRequest<'_>,
) -> Result<u64, WalChainLoadError> {
    let Some((tip, stop_after_seq)) = request.tip_and_stop()? else {
        return Ok(0);
    };

    let mut count: u64 = 1;
    // Oldest pointer counted so far: the run has to keep descending from
    // here, contiguously, until it crosses the boundary.
    let mut oldest_counted = &tip;
    if pointer_reaches_base(oldest_counted, stop_after_seq) {
        return Ok(count);
    }

    for pointer in request.recent_segments {
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
