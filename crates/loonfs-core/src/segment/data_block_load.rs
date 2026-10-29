//! Fetching, decoding, coalescing, and cache publication for SST data blocks.
//!
//! Every load consults the per-view memo and the block cache first. A load
//! that reads ahead fetches the read-ahead window of a block that neither one
//! has (see [`read_ahead_window_len`]) and keeps the window's other blocks as
//! stored bytes in the memo. A span load, for compaction and for scans that
//! don't read ahead, fetches the blocks that neither one has with coalesced
//! ranged GETs.

use super::block_memo::SessionBlockMemo;
use super::cache::SegmentBlockKind;
use super::cache::{read_ahead_window_bytes, DecodedMetadataSegmentBlock, MetadataSegmentCache};
use super::index_block_load::{
    decode_stored_block, load_section_bytes, segment_block_cache_key, segment_codec_error,
};
use super::segment_block_cache::LoadedSegmentBlock;
use crate::heap_bytes::data_block_heap_bytes;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::READAHEAD_WINDOW_BYTES;
use crate::manifest::error::ManifestLoadError;
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
// A default read-ahead window is read in one GET.
const _: () = assert!(READAHEAD_WINDOW_BYTES as u64 <= MAX_BULK_LOAD_BYTES);

/// Returns how many blocks of `entries` the read-ahead window of a read that
/// misses `entries[0]` covers.
///
/// The window is the missed block and the blocks after it, as long as their
/// stored bytes stay within `window_bytes` and within `MAX_BULK_LOAD_BYTES`,
/// so one ranged GET reads the window. A block larger than that is a window
/// of its own. For example, a window of 1 MiB over blocks stored in 4 KiB
/// each covers 256 blocks, and over blocks of 300 KiB it covers three.
pub(crate) fn read_ahead_window_len(entries: &[SegmentIndexEntry], window_bytes: usize) -> usize {
    let (first, rest) = entries
        .split_first()
        .expect("a read-ahead window should start at a block");
    let max_bytes = MAX_BULK_LOAD_BYTES.min(window_bytes as u64);
    1 + rest
        .iter()
        .take_while(|entry| {
            entry.block.offset + u64::from(entry.block.stored_bytes) - first.block.offset
                <= max_bytes
        })
        .count()
}

/// Loads the data block of `entries[0]`, and reads ahead over the blocks of
/// `entries` after it in the same ranged GET if neither the memo nor the
/// block cache has the block.
///
/// Every read that reads ahead, a scan or a lookup, loads its blocks one at a
/// time through this function. The GET reads the read-ahead window that
/// [`read_ahead_window_len`] describes, and the memo records the stored bytes
/// of the window's other blocks. A later load by the same read decodes a
/// block from them when the read reaches it, which inserts the block into the
/// block cache. Decoding the whole window here would decode blocks that the
/// read never reaches, and a damaged block that the read doesn't reach would
/// fail the read. And when the block cache keeps fewer blocks than the read
/// has in flight, the cache could evict the window's decoded blocks before the
/// read reaches them, and each later block of the window would then fetch and
/// decode the rest of the window again.
///
/// The memo is the only place that keeps the window's other blocks, so the
/// window also stops at the read working memory that the memo's pool has
/// free. A larger window would make the memo evict its own oldest blocks, the
/// window's among them, to make room for the rest. Without a memo, the GET
/// reads the block alone.
pub(crate) async fn load_segment_data_block_with_readahead<S: ObjectStore + ?Sized>(
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
        let window_bytes = memo.map_or(0, |memo| {
            read_ahead_window_bytes(segment_cache).min(memo.available_bytes())
        });
        let entries = &entries[..read_ahead_window_len(entries, window_bytes)];
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
                let start = loonfs_types::usize_from_u64(entry.block.offset - handle.offset);
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

pub(crate) fn decoded_data_cache_block(block: DecodedDataBlock) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Data {
        decoded_bytes: data_block_heap_bytes(&block),
        block: Arc::new(block),
    }
}

/// Bulk path for wide selections: resolve each block against the memo and
/// shared decoded cache, group the blocks neither had into consecutive
/// spans, and fetch each span with coalesced ranged GETs instead of one
/// request per block. Duplicate concurrent span fetches are possible and
/// benign; the narrow path keeps single-flight for the hot point lookups.
pub(crate) async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks = load_cached_data_blocks(segment_cache, memo, descriptor, entries).await?;
    let spans = missing_block_spans(entries, &blocks);

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

/// Loads the blocks of `entries` that the view's memo or the caches already
/// have, and leaves `None` for every block that has to be fetched.
async fn load_cached_data_blocks(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Option<Arc<DecodedDataBlock>>>, ManifestLoadError> {
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
    Ok(blocks)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Index entries for contiguous blocks with the given stored sizes.
    fn entries(stored_bytes: &[u32]) -> Vec<SegmentIndexEntry> {
        let mut offset = 0;
        stored_bytes
            .iter()
            .map(|stored_bytes| {
                let entry = SegmentIndexEntry {
                    last_row_key: format!("key-{offset:012}"),
                    block: BlockHandle {
                        offset,
                        stored_bytes: *stored_bytes,
                        decoded_bytes: *stored_bytes,
                        crc32c: 0,
                    },
                };
                offset += u64::from(*stored_bytes);
                entry
            })
            .collect()
    }

    #[test]
    fn a_read_ahead_window_covers_the_blocks_whose_stored_bytes_fit_its_size() {
        const KIB: u32 = 1024;
        let window = READAHEAD_WINDOW_BYTES;
        // Each case is the stored size and number of the blocks, the window's
        // size, and the blocks that the window covers.
        for (block_bytes, blocks, window_bytes, covered) in [
            // Small blocks: 256 blocks of 4 KiB fill 1 MiB exactly.
            (4 * KIB, 1_000, window, 256),
            // Large blocks: three blocks of 300 KiB fit, and a fourth doesn't.
            (300 * KIB, 10, window, 3),
            // A block larger than the window is a window of its own.
            (2 * 1024 * KIB, 10, window, 1),
            // A window larger than one GET stops at `MAX_BULK_LOAD_BYTES`.
            (4 * KIB, 10_000, usize::MAX, 1_024),
            (4 * KIB, 10_000, 8 * 1024 * 1024, 1_024),
            // A window stops at the segment's last block.
            (4 * KIB, 10, window, 10),
            // A zero window reads the missed block alone.
            (4 * KIB, 10, 0, 1),
        ] {
            let entries = entries(&vec![block_bytes; blocks]);
            assert_eq!(
                read_ahead_window_len(&entries, window_bytes),
                covered,
                "{blocks} blocks of {block_bytes} bytes, window {window_bytes}"
            );
        }
    }
}
