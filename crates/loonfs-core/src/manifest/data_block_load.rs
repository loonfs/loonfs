//! Fetching, decoding, coalescing, and cache publication for SST data blocks.
//!
//! Point loads and scan loads consult the per-view memo and the block cache
//! first. A scan load fetches the blocks that neither one has with coalesced
//! ranged GETs.

use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache};
use super::error::ManifestLoadError;
use super::index_block_load::{
    decode_stored_block, load_section_bytes, segment_block_cache_key, segment_codec_error,
};
use super::segment_block_cache::LoadedSegmentBlock;
use crate::block_cache::SegmentBlockKind;
use crate::heap_bytes::data_block_heap_bytes;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_data_block, BlockHandle, DecodedDataBlock, SegmentIndexEntry,
};
use std::sync::Arc;

/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
pub(super) const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

// The SSD tier's write queue fits the blocks of at least one bulk GET.
const _: () = assert!(MAX_BULK_LOAD_BYTES <= crate::limits::SSD_WRITE_QUEUE_BYTES as u64);

pub(super) async fn load_segment_data_block_with_readahead<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entries[0].block;
    let cache_key = segment_block_cache_key(descriptor, SegmentBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Data { block, .. }) =
        memo.and_then(|memo| memo.get(&cache_key))
    {
        return Ok(block);
    }
    let fetch = async {
        if let Some(bytes) = memo.and_then(|memo| memo.stored(&cache_key)) {
            let stored = Bytes::from_owner(bytes);
            return Ok(LoadedSegmentBlock {
                block: decode_stored_data(descriptor, &handle, &stored)?,
                stored,
            });
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
        let bytes = Bytes::from(
            load_section_bytes(
                store,
                &object_key,
                handle.offset,
                last.offset + u64::from(last.stored_bytes) - handle.offset,
            )
            .await?,
        );
        if let Some(memo) = memo {
            for entry in &entries[1..] {
                let start = usize::try_from(entry.block.offset - handle.offset).expect(
                    "the read stops at `MAX_BULK_LOAD_BYTES` past its first block, so a block's \
                     offset in it should fit usize",
                );
                memo.record_stored(
                    &segment_block_cache_key(
                        descriptor,
                        SegmentBlockKind::Data,
                        entry.block.offset,
                    ),
                    &bytes[start..start + entry.block.stored_bytes as usize],
                );
            }
        }
        let stored = bytes.slice(..handle.stored_bytes as usize);
        Ok::<_, ManifestLoadError>(LoadedSegmentBlock {
            block: decode_stored_data(descriptor, &handle, &stored)?,
            stored,
        })
    };
    let block = match segment_cache {
        Some(cache) => {
            cache
                .blocks()
                .get_or_load_block(&cache_key, &handle, Box::pin(fetch))
                .await?
        }
        None => fetch.await?.block,
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
    decode_stored_block(
        &metadata_segment_object_key(descriptor),
        SegmentBlockKind::Data,
        handle,
        bytes,
    )
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
    let mut probe_key = segment_block_cache_key(descriptor, SegmentBlockKind::Data, 0);
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
            }) = cache.blocks().get_block(&probe_key, &handle).await
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
                cache
                    .blocks()
                    .insert_block(probe_key.clone(), decoded.clone(), &bytes);
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
    let span_loads: Vec<_> = spans
        .iter()
        .zip(&mut span_decodes)
        .map(|((start, end), winner_decodes)| {
            let span = &entries[*start..*end];
            async move {
                // Single-flight on the span's first block: the winning fetch
                // keeps what it decoded and publishes the whole span. A
                // concurrent loser resolves the remaining blocks from caches.
                let first_key = segment_block_cache_key(
                    descriptor,
                    SegmentBlockKind::Data,
                    span[0].block.offset,
                );
                let fetch = load_and_publish_span(
                    store,
                    segment_cache,
                    memo,
                    descriptor,
                    span,
                    winner_decodes,
                );
                match segment_cache {
                    Some(cache) => {
                        cache
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
                    }
                    None => {
                        fetch.await?;
                    }
                }
                Ok::<_, ManifestLoadError>(())
            }
        })
        .collect();
    stream::iter(span_loads)
        .buffer_unordered(STORE_READS_IN_FLIGHT)
        .try_collect::<()>()
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
    // published. Each block reads through the decoded cache, and a block
    // evicted since the winner published it is fetched alone.
    for (position, entry) in entries.iter().enumerate() {
        if blocks[position].is_some() {
            continue;
        }
        blocks[position] = Some(
            load_segment_data_block_with_readahead(
                store,
                segment_cache,
                memo,
                descriptor,
                std::slice::from_ref(entry),
            )
            .await?,
        );
    }

    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block should be resolved above"))
        .collect())
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, keeps the winner's decodes, and publishes them to the per-view memo
/// and (when populating) the shared cache. Returns the first block's cache
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
        let begin = usize::try_from(handle.offset - first.offset).expect(
            "`load_section_bytes` holds the whole span in memory, so a block's offset in it \
             should fit usize",
        );
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
