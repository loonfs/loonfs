//! Fetching, decoding, and cache publication for SST data blocks.
//!
//! A load resolves each block that it asks for against the per-view memo and
//! the block cache, and fetches the blocks that neither one has in contiguous
//! ranged GETs. It decodes every block that it fetches and inserts each one
//! into the memo and the block cache.

use super::block_memo::SessionBlockMemo;
use super::cache::SegmentBlockKind;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache};
use super::index_block_load::{load_section_bytes, segment_block_cache_key, segment_codec_error};
use super::segment_block_cache::LoadedSegmentBlock;
use crate::heap_bytes::data_block_heap_bytes;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::manifest::error::ManifestLoadError;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_data_block, DecodedDataBlock, SegmentIndexEntry,
};
use std::sync::Arc;

/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
pub(super) const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

// The SSD tier's write queue fits the blocks of at least one bulk GET.
const _: () = assert!(MAX_BULK_LOAD_BYTES <= crate::limits::SSD_WRITE_QUEUE_BYTES as u64);

pub(crate) fn decoded_data_cache_block(block: DecodedDataBlock) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Data {
        decoded_bytes: data_block_heap_bytes(&block),
        block: Arc::new(block),
    }
}

/// Loads the data blocks of `entries`, which are consecutive blocks of one
/// segment, in order.
///
/// The load resolves each block against the memo and the block cache, groups
/// the blocks that neither one has into spans of at most
/// `MAX_BULK_LOAD_BYTES`, and fetches each span in one ranged GET. It decodes
/// every block of a span and inserts each one into the memo and the block
/// cache. Concurrent loads of a span share one GET through the block cache's
/// single flight on the span's first block.
pub(crate) async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks = vec![None; entries.len()];
    // A load that loses the single flight on a span's first block finds the
    // span's other blocks in the block cache on the next pass, and fetches a
    // block that the cache evicted since the winner inserted it. Each pass
    // resolves at least the first block of every span.
    loop {
        load_cached_data_blocks(segment_cache, memo, descriptor, entries, &mut blocks).await;
        let spans = missing_block_spans(entries, &blocks);
        if spans.is_empty() {
            break;
        }
        let span_loads: Vec<_> = spans
            .iter()
            .map(|&(start, end)| async move {
                let loaded =
                    load_span(store, segment_cache, memo, descriptor, &entries[start..end]).await?;
                Ok::<_, ManifestLoadError>((start, loaded))
            })
            .collect();
        let loaded: Vec<_> = stream::iter(span_loads)
            .buffer_unordered(STORE_READS_IN_FLIGHT)
            .try_collect()
            .await?;
        for (start, loaded) in loaded {
            for (slot, block) in blocks[start..].iter_mut().zip(loaded) {
                *slot = Some(block);
            }
        }
    }
    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block should be resolved above"))
        .collect())
}

/// Loads the blocks of `span` through the block cache's single flight on the
/// span's first block. Returns every block of the span when this load
/// fetched it, and the first block alone when a concurrent load fetched it.
async fn load_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    span: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut decoded = None;
    let fetch = load_and_publish_span(store, segment_cache, memo, descriptor, span, &mut decoded);
    let Some(cache) = segment_cache else {
        fetch.await?;
        return Ok(decoded.expect("a span load without a cache should decode the span"));
    };
    let first_key =
        segment_block_cache_key(descriptor, SegmentBlockKind::Data, span[0].block.offset);
    let first = cache
        .blocks()
        .get_or_load_block(
            &first_key,
            &span[0].block,
            Box::pin(async {
                Ok::<_, ManifestLoadError>(
                    fetch
                        .await?
                        .expect("a cached span should return its first block"),
                )
            }),
        )
        .await?;
    match decoded {
        Some(decoded) => Ok(decoded),
        None => Ok(vec![
            first.into_data(&metadata_segment_object_key(descriptor))?
        ]),
    }
}

/// Fills each empty slot of `blocks` whose block of `entries` the view's memo
/// or the block cache has, and leaves `None` for every block that has to be
/// fetched.
async fn load_cached_data_blocks(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
    blocks: &mut [Option<Arc<DecodedDataBlock>>],
) {
    // One probe key reused across the span: a fresh key per block would
    // build the segment object key once per block on every warm scan.
    let mut probe_key = segment_block_cache_key(descriptor, SegmentBlockKind::Data, 0);
    for (slot, entry) in blocks.iter_mut().zip(entries) {
        if slot.is_some() {
            continue;
        }
        let handle = entry.block;
        probe_key.block_offset = handle.offset;
        if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
            memo.and_then(|memo| memo.get(&probe_key))
        {
            *slot = Some(block);
            continue;
        }
        if let Some(cache) = segment_cache {
            if let Some(DecodedMetadataSegmentBlock::Data {
                block,
                decoded_bytes,
            }) = cache.blocks().get_block(&probe_key, &handle).await
            {
                if let Some(memo) = memo {
                    // Keep shared hits too, so this view's later scans do not
                    // repeat shared-cache recency work.
                    memo.record(
                        &probe_key,
                        &DecodedMetadataSegmentBlock::Data {
                            block: Arc::clone(&block),
                            decoded_bytes,
                        },
                    );
                }
                *slot = Some(block);
            }
        }
    }
}

/// Groups the blocks that no cache had into spans, each of which one ranged
/// GET fetches.
fn missing_block_spans(
    entries: &[SegmentIndexEntry],
    blocks: &[Option<Arc<DecodedDataBlock>>],
) -> Vec<(usize, usize)> {
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut cursor = 0;
    while cursor < entries.len() {
        if blocks[cursor].is_some() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut span_bytes = 0u64;
        while cursor < entries.len()
            && blocks[cursor].is_none()
            && span_bytes + u64::from(entries[cursor].block.stored_bytes) <= MAX_BULK_LOAD_BYTES
        {
            span_bytes += u64::from(entries[cursor].block.stored_bytes);
            cursor += 1;
        }
        // A single block larger than the fetch cap still fetches alone.
        if cursor == start {
            cursor += 1;
        }
        spans.push((start, cursor));
    }
    spans
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, keeps the decodes in `winner_decodes`, and inserts each block into
/// the per-view memo and the block cache. Returns the first block's cache
/// entry, with its stored bytes, for the single-flight cell.
async fn load_and_publish_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    span: &[SegmentIndexEntry],
    winner_decodes: &mut Option<Vec<Arc<DecodedDataBlock>>>,
) -> Result<Option<LoadedSegmentBlock>, ManifestLoadError> {
    let first = &span[0].block;
    let last = &span[span.len() - 1].block;
    let span_len = last.offset + u64::from(last.stored_bytes) - first.offset;
    let object_key = metadata_segment_object_key(descriptor);
    let bytes = Bytes::from(load_section_bytes(store, &object_key, first.offset, span_len).await?);
    let mut first_block = None;
    let mut retained = Vec::with_capacity(span.len());
    for entry in span {
        let handle = entry.block;
        let begin = loonfs_types::usize_from_u64(handle.offset - first.offset);
        let stored = bytes.slice(begin..begin + handle.stored_bytes as usize);
        let decoded = Arc::new(
            decode_data_block(&stored, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        );
        if memo.is_some() || segment_cache.is_some() {
            let cache_key =
                segment_block_cache_key(descriptor, SegmentBlockKind::Data, handle.offset);
            let cache_block = DecodedMetadataSegmentBlock::Data {
                decoded_bytes: data_block_heap_bytes(&decoded),
                block: Arc::clone(&decoded),
            };
            if let Some(memo) = memo {
                memo.record(&cache_key, &cache_block);
            }
            if let Some(cache) = segment_cache {
                // The first block is what the single-flight cell publishes;
                // inserting it here too keeps the whole span uniformly cached.
                cache
                    .blocks()
                    .insert_block(cache_key, cache_block.clone(), &stored);
            }
            if first_block.is_none() {
                first_block = Some(LoadedSegmentBlock {
                    block: cache_block,
                    stored,
                });
            }
        }
        retained.push(decoded);
    }
    *winner_decodes = Some(retained);
    Ok(first_block)
}
