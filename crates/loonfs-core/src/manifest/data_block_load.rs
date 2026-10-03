//! Fetching, decoding, coalescing, and cache publication for SST data blocks.
//!
//! Point loads consult the per-view memo, the decoded block cache, and the
//! node-local cache of stored bytes. Scan loads skip the stored-byte tier
//! because it answers one block per awaited read, turning a wide scan into
//! tens of thousands of serial point reads. Scans use the decoded caches and
//! coalesced store GETs, and they do not populate the stored-byte tier.

use super::block_fetch::{
    load_section_bytes, offer_stored_block, segment_block_cache_key, segment_codec_error,
    stored_block_section,
};
use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentBlockKind, MetadataSegmentCache};
use super::error::ManifestLoadError;
use super::stored_block_cache::StoredMetadataBlockKind;
use crate::heap_bytes::data_block_heap_bytes;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::sst_blocks::{
    decode_data_block, BlockHandle, DecodedDataBlock, SegmentIndexEntry,
};
use std::sync::Arc;

/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
pub(super) const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

pub(super) async fn load_segment_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entry: &SegmentIndexEntry,
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entry.block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
        memo.and_then(|memo| memo.get(&cache_key))
    {
        return Ok(block);
    }
    let fetch = || async {
        if let Some(bytes) = memo.and_then(|memo| memo.stored(&cache_key)) {
            return decode_stored_data(descriptor, &handle, &bytes);
        }
        // Between the decoded cache above and the store below: a local copy
        // of the same stored bytes.
        if let Some(decoded) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Data,
            &handle,
            decode_data_block,
        )
        .await
        {
            return Ok(decoded_data_cache_block(decoded));
        }
        let object_key = metadata_segment_object_key(descriptor);
        let bytes = load_section_bytes(
            store,
            &object_key,
            handle.offset,
            handle.stored_bytes as u64,
        )
        .await?;
        offer_stored_block(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Data,
            &handle,
            &bytes,
        );
        Ok(decoded_data_cache_block(
            decode_data_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        ))
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    if let Some(memo) = memo {
        memo.record(&cache_key, &block);
    }
    block.into_data(&metadata_segment_object_key(descriptor))
}

pub(super) async fn load_segment_data_block_with_readahead<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entries[0].block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
        memo.and_then(|memo| memo.get(&cache_key))
    {
        return Ok(block);
    }
    let fetch = || async {
        if let Some(bytes) = memo.and_then(|memo| memo.stored(&cache_key)) {
            return decode_stored_data(descriptor, &handle, &bytes);
        }
        let read_count = entries
            .iter()
            .take_while(|entry| {
                entry.block.offset == handle.offset
                    || entry.block.offset + u64::from(entry.block.stored_bytes) - handle.offset
                        <= MAX_BULK_LOAD_BYTES
            })
            .count();
        let entries = &entries[..read_count];
        let last = entries[entries.len() - 1].block;
        let object_key = metadata_segment_object_key(descriptor);
        let bytes = load_section_bytes(
            store,
            &object_key,
            handle.offset,
            last.offset + u64::from(last.stored_bytes) - handle.offset,
        )
        .await?;
        if let Some(memo) = memo {
            for entry in &entries[1..] {
                let start = (entry.block.offset - handle.offset) as usize;
                memo.record_stored(
                    &segment_block_cache_key(
                        descriptor,
                        MetadataSegmentBlockKind::Data,
                        entry.block.offset,
                    ),
                    &bytes[start..start + entry.block.stored_bytes as usize],
                );
            }
        }
        let bytes = &bytes[..handle.stored_bytes as usize];
        decode_stored_data(descriptor, &handle, bytes)
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    if let Some(memo) = memo {
        memo.record(&cache_key, &block);
    }
    block.into_data(&metadata_segment_object_key(descriptor))
}

fn decode_stored_data(
    descriptor: &MetadataSegmentRef,
    handle: &BlockHandle,
    bytes: &[u8],
) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
    Ok(decoded_data_cache_block(
        decode_data_block(bytes, handle)
            .map_err(|err| segment_codec_error(&metadata_segment_object_key(descriptor), err))?,
    ))
}

pub(super) fn decoded_data_cache_block(block: DecodedDataBlock) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Data {
        decoded_bytes: data_block_heap_bytes(&block),
        block: Arc::new(block),
    }
}

/// Bulk path for wide selections: resolve each block against the memo and
/// shared decoded cache, group the blocks neither answered into consecutive
/// spans, and fetch each span with coalesced ranged GETs instead of one
/// request per block. Duplicate concurrent span fetches are possible and
/// benign; the narrow path keeps single-flight for the hot point lookups.
pub(super) async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks: Vec<Option<Arc<DecodedDataBlock>>> = vec![None; entries.len()];
    // One probe key reused across the span: a fresh key per block would
    // build the segment object key once per block on every warm scan.
    let mut probe_key = segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, 0);
    for (position, entry) in entries.iter().enumerate() {
        let handle = entry.block;
        probe_key.block_offset = handle.offset;
        if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
            memo.and_then(|memo| memo.get(&probe_key))
        {
            blocks[position] = Some(block);
            continue;
        }
        if let Some(cache) = segment_cache {
            if let Some(DecodedMetadataSegmentBlock::Data {
                block,
                decoded_bytes,
            }) = cache.get(&probe_key)
            {
                if let Some(memo) = memo {
                    // Keep shared hits just like point loads, so this view's
                    // later scans do not repeat shared-cache recency work.
                    memo.record(
                        &probe_key,
                        &DecodedMetadataSegmentBlock::Data {
                            block: Arc::clone(&block),
                            decoded_bytes,
                        },
                    );
                }
                blocks[position] = Some(block);
                continue;
            }
        }
        if let Some(bytes) = memo.and_then(|memo| memo.stored(&probe_key)) {
            let decoded = decode_stored_data(descriptor, &handle, &bytes)?;
            if let Some(memo) = memo {
                memo.record(&probe_key, &decoded);
            }
            if let Some(cache) = segment_cache {
                cache.insert(probe_key.clone(), decoded.clone());
            }
            blocks[position] = Some(decoded.into_data(&metadata_segment_object_key(descriptor))?);
        }
    }

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

    let mut span_decodes = vec![None; spans.len()];
    futures::future::try_join_all(spans.iter().zip(&mut span_decodes).map(
        |((start, end), winner_decodes)| {
            let span = &entries[*start..*end];
            async move {
                // Single-flight on the span's first block: the winning fetch
                // keeps what it decoded and publishes the whole span. A
                // concurrent loser resolves the remaining blocks from caches.
                let first_key = segment_block_cache_key(
                    descriptor,
                    MetadataSegmentBlockKind::Data,
                    span[0].block.offset,
                );
                let fetch = || async move {
                    load_and_publish_span(
                        store,
                        segment_cache,
                        memo,
                        descriptor,
                        span,
                        winner_decodes,
                    )
                    .await
                };
                match segment_cache {
                    Some(cache) => {
                        cache
                            .get_or_load(&first_key, || async {
                                Ok(fetch()
                                    .await?
                                    .expect("a cached span should return its first block"))
                            })
                            .await?;
                    }
                    None => {
                        fetch().await?;
                    }
                }
                Ok::<_, ManifestLoadError>(())
            }
        },
    ))
    .await?;

    for ((start, end), winner_decodes) in spans.iter().zip(span_decodes) {
        let Some(winner_decodes) = winner_decodes else {
            continue;
        };
        assert_eq!(
            winner_decodes.len(),
            end - start,
            "a span winner should retain every block it decoded"
        );
        for (slot, block) in blocks[*start..*end].iter_mut().zip(winner_decodes) {
            *slot = Some(block);
        }
    }

    // Only single-flight losers still need to resolve what the winner
    // published, with the existing point-load fallback on cache eviction.
    for (position, entry) in entries.iter().enumerate() {
        if blocks[position].is_some() {
            continue;
        }
        blocks[position] =
            Some(load_segment_data_block(store, segment_cache, memo, descriptor, entry).await?);
    }

    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block should be resolved above"))
        .collect())
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, keeps the winner's decodes, and publishes them to the per-view memo
/// and (when populating) the shared cache. Returns the first block's cache
/// entry for the single-flight cell.
async fn load_and_publish_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    span: &[SegmentIndexEntry],
    winner_decodes: &mut Option<Vec<Arc<DecodedDataBlock>>>,
) -> Result<Option<DecodedMetadataSegmentBlock>, ManifestLoadError> {
    let first = &span[0].block;
    let last = &span[span.len() - 1].block;
    let span_len = last.offset + u64::from(last.stored_bytes) - first.offset;
    let object_key = metadata_segment_object_key(descriptor);
    let bytes = load_section_bytes(store, &object_key, first.offset, span_len).await?;
    let mut first_block = None;
    let mut retained = Vec::with_capacity(span.len());
    for entry in span {
        let handle = entry.block;
        let begin = (handle.offset - first.offset) as usize;
        let stored = &bytes[begin..begin + handle.stored_bytes as usize];
        let decoded = Arc::new(
            decode_data_block(stored, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        );
        if memo.is_some() || segment_cache.is_some() {
            let cache_key =
                segment_block_cache_key(descriptor, MetadataSegmentBlockKind::Data, handle.offset);
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
                cache.insert(cache_key, cache_block.clone());
            }
            if first_block.is_none() {
                first_block = Some(cache_block);
            }
        }
        retained.push(decoded);
    }
    *winner_decodes = Some(retained);
    Ok(first_block)
}
