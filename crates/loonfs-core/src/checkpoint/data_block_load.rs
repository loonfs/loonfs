//! Fetching, decoding, coalescing, and cache publication for SST data blocks.

use super::block_fetch::{load_section_bytes, segment_block_cache_key, segment_codec_error};
use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache};
use super::error::ManifestLoadError;
use crate::metadata::content_ref_evidence_bytes;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::{decode_data_block, DecodedDataBlock, SegmentIndexEntry};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

/// Longest single ranged GET issued while bulk-reading a block span; longer
/// spans split into consecutive requests.
const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

async fn load_segment_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    entry: &SegmentIndexEntry,
) -> Result<Arc<DecodedDataBlock>, ManifestLoadError> {
    let handle = entry.block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, handle.offset);
    if let Some(DecodedMetadataTableBlock::Data { block, .. }) = memo.get(&cache_key) {
        return Ok(block);
    }
    let fetch = || async {
        let bytes = load_section_bytes(
            store,
            &descriptor.object_key,
            handle.offset,
            handle.stored_len as u64,
        )
        .await?;
        Ok(decoded_data_cache_block(
            family,
            decode_data_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        ))
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Data { block, .. } => Ok(block),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-data block for a data key",
        )),
    }
}

pub(super) fn decoded_data_cache_block(
    family: MetadataTableFamily,
    block: DecodedDataBlock,
) -> DecodedMetadataTableBlock {
    DecodedMetadataTableBlock::Data {
        decoded_byte_len: decoded_manifest_block_weight(family, &block.rows),
        block: Arc::new(block),
    }
}

/// Bulk path for wide selections: consult the cache per block, group the
/// misses into consecutive spans, and fetch each span with coalesced ranged
/// GETs instead of one request per block. Duplicate concurrent span fetches
/// are possible and benign; the narrow path keeps single-flight for the hot
/// point lookups.
pub(super) async fn load_segment_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let mut blocks: Vec<Option<Arc<DecodedDataBlock>>> = vec![None; entries.len()];
    // One probe key reused across the span: a fresh key per block would
    // clone the segment checksum once per block on every warm scan.
    let mut probe_key = segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, 0);
    for (position, entry) in entries.iter().enumerate() {
        probe_key.block_offset = entry.block.offset;
        if let Some(DecodedMetadataTableBlock::Data { block, .. }) = memo.get(&probe_key) {
            blocks[position] = Some(block);
            continue;
        }
        if let Some(cache) = table_cache {
            if let Some(DecodedMetadataTableBlock::Data { block, .. }) = cache.get(&probe_key) {
                blocks[position] = Some(block);
            }
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
            && span_bytes + u64::from(entries[cursor].block.stored_len) <= MAX_BULK_LOAD_BYTES
        {
            span_bytes += u64::from(entries[cursor].block.stored_len);
            cursor += 1;
        }
        // A single block larger than the fetch cap still fetches alone.
        if cursor == start {
            cursor += 1;
        }
        spans.push((start, cursor));
    }

    futures::future::try_join_all(spans.iter().map(|(start, end)| {
        let span = &entries[*start..*end];
        async move {
            // Single-flight on the span's first block: the winning fetch
            // decodes and publishes the whole span, so a concurrent scan
            // waiting here finds the remaining blocks in the shared cache
            // instead of re-fetching the span.
            let first_key = segment_block_cache_key(
                descriptor,
                MetadataTableBlockKind::Data,
                span[0].block.offset,
            );
            let fetch = || async {
                load_and_publish_span(store, table_cache, memo, family, descriptor, span).await
            };
            match table_cache {
                Some(cache) => {
                    cache.get_or_load(&first_key, fetch).await?;
                }
                None => {
                    fetch().await?;
                }
            }
            Ok::<_, ManifestLoadError>(())
        }
    }))
    .await?;

    // Everything the winner published (or this call fetched) is resolvable
    // now; blocks that lost a cache race are fetched alone.
    for (position, entry) in entries.iter().enumerate() {
        if blocks[position].is_some() {
            continue;
        }
        blocks[position] = Some(
            load_segment_data_block(store, table_cache, memo, family, descriptor, entry).await?,
        );
    }

    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every selected block should be resolved above"))
        .collect())
}

/// Fetches one contiguous span with a single ranged GET, decodes every
/// block, and publishes them to the per-view memo and (when populating) the
/// shared cache. Returns the first block's cache entry for the
/// single-flight cell.
async fn load_and_publish_span<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    span: &[SegmentIndexEntry],
) -> Result<DecodedMetadataTableBlock, ManifestLoadError> {
    let first = &span[0].block;
    let last = &span[span.len() - 1].block;
    let span_len = last.offset + u64::from(last.stored_len) - first.offset;
    let bytes = load_section_bytes(store, &descriptor.object_key, first.offset, span_len).await?;
    let mut first_block = None;
    for entry in span {
        let handle = entry.block;
        let begin = (handle.offset - first.offset) as usize;
        let stored = &bytes[begin..begin + handle.stored_len as usize];
        let decoded = Arc::new(
            decode_data_block(stored, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        );
        let cache_key =
            segment_block_cache_key(descriptor, MetadataTableBlockKind::Data, handle.offset);
        let cache_block = DecodedMetadataTableBlock::Data {
            decoded_byte_len: decoded_manifest_block_weight(family, &decoded.rows),
            block: decoded,
        };
        memo.record(&cache_key, &cache_block);
        if let Some(cache) = table_cache {
            // The first block is what the single-flight cell publishes;
            // inserting it here too keeps the whole span uniformly cached.
            cache.insert(cache_key, cache_block.clone());
        }
        if first_block.is_none() {
            first_block = Some(cache_block);
        }
    }
    Ok(first_block.expect("a span should always hold at least one block"))
}

pub(super) fn decoded_manifest_block_weight(
    family: MetadataTableFamily,
    rows: &[MetadataRow],
) -> usize {
    // Approximate decoded map/vector bookkeeping beyond owned row payloads.
    const BLOCK_ENTRY_OVERHEAD: usize = 64;
    const BLOCK_ALLOCATION_OVERHEAD: usize = 128;
    let row_weight = rows
        .iter()
        .map(|row| {
            BLOCK_ENTRY_OVERHEAD
                + row.row_key_for_family(family).len()
                + decoded_manifest_row_weight(row)
        })
        .sum::<usize>();
    row_weight.saturating_add(BLOCK_ALLOCATION_OVERHEAD)
}

pub(super) fn decoded_manifest_row_weight(row: &MetadataRow) -> usize {
    // Approximate inline row storage and heap allocation metadata.
    const FIXED_ROW_OVERHEAD: usize = 32;
    const ALLOCATED_ROW_OVERHEAD: usize = 96;
    match row {
        MetadataRow::Inode { .. } => FIXED_ROW_OVERHEAD,
        MetadataRow::DirentryBind {
            name_key,
            display_name,
            ..
        } => ALLOCATED_ROW_OVERHEAD + name_key.as_str().len() + display_name.as_str().len(),
        MetadataRow::DirentryUnbind { name_key, .. } => {
            ALLOCATED_ROW_OVERHEAD + name_key.as_str().len()
        }
        MetadataRow::Revision { content_ref, .. } => {
            ALLOCATED_ROW_OVERHEAD + content_ref_evidence_bytes(content_ref)
        }
        MetadataRow::Tombstone { .. } => FIXED_ROW_OVERHEAD,
        MetadataRow::CommitReceipt {
            commit_id,
            semantic_commit_fingerprint,
            message,
            ..
        } => {
            ALLOCATED_ROW_OVERHEAD
                + commit_id.as_str().len()
                + semantic_commit_fingerprint.len()
                + message.as_ref().map_or(0, String::len)
        }
    }
}
