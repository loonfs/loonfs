//! Cached reads of gram-index segment sections.
//!
//! Index segments share the metadata segments' block grammar and their
//! immutability contract: a decoded section is identified by the segment's
//! payload checksum plus the block kind and offset, so it can never go
//! stale. These loaders resolve a section through the shared
//! [`MetadataTableCache`] — the same cache and byte budget metadata blocks
//! use, with distinct checksums keeping the two families apart — and fall
//! back to a direct ranged fetch when no cache is attached (the embedded
//! unit-test shape), decoding and CRC-verifying identically either way.

use super::cache::{
    DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheKey,
};
use crate::error::{CoreError, Result};
use loonfs_api::wire::index_grams::IndexRow;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

/// The error every index-segment reader reports for a section that fetched
/// but would not decode.
pub(crate) fn index_segment_corrupt(
    object_key: &str,
    what: &str,
    error: &dyn std::fmt::Display,
) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "index segment `{object_key}` carries an unreadable {what}: {error}"
    ))
}

fn index_section_cache_key(
    payload_checksum: &str,
    block_kind: MetadataTableBlockKind,
    handle: &BlockHandle,
) -> MetadataTableCacheKey {
    MetadataTableCacheKey {
        // The same identity convention metadata segments use — the payload
        // checksum verbatim — so both families share one cache without
        // colliding: distinct objects have distinct checksums.
        identity: payload_checksum.to_owned(),
        block_kind,
        block_offset: handle.offset,
    }
}

/// Fetches exactly the byte range a handle names, refusing handles whose
/// bounds do not fit the address space.
async fn fetch_index_section_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    handle: &BlockHandle,
) -> Result<Vec<u8>> {
    let end_exclusive = handle
        .offset
        .checked_add(u64::from(handle.stored_len))
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "index segment `{object_key}` descriptor names bytes past the address space"
            ))
        })?;
    let bytes = store
        .get(
            object_key,
            Some(ByteRange {
                start_inclusive: handle.offset,
                end_exclusive,
            }),
        )
        .await
        .map_err(|error| CoreError::store(object_key, &error))?
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "manifest references missing index segment `{object_key}`"
            ))
        })?;
    Ok(bytes.to_vec())
}

/// Loads an index segment's bloom filter block, decoded once at fetch time
/// and shared through the cache afterward. Callers with an inline filter
/// copy in the descriptor decode it locally and never reach here.
pub(crate) async fn load_index_segment_filter_block<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<SegmentFilter>> {
    let cache_key =
        index_section_cache_key(payload_checksum, MetadataTableBlockKind::Filter, handle);
    let fetch = || async {
        let bytes = fetch_index_section_bytes(store, object_key, handle).await?;
        let filter = decode_filter_block(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?;
        Ok::<_, CoreError>(DecodedMetadataTableBlock::Filter {
            decoded_byte_len: handle.decoded_len as usize,
            filter: Arc::new(filter),
        })
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    match block {
        DecodedMetadataTableBlock::Filter { filter, .. } => Ok(filter),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::IndexData { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(index_segment_corrupt(
            object_key,
            "filter block",
            &"cache returned a non-filter block for a filter key",
        )),
    }
}

/// Loads an index segment's index block: the block directory a probe
/// narrows before touching any data block.
pub(crate) async fn load_index_segment_index_block<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    let cache_key =
        index_section_cache_key(payload_checksum, MetadataTableBlockKind::Index, handle);
    let fetch = || async {
        let bytes = fetch_index_section_bytes(store, object_key, handle).await?;
        let entries = decode_index_block(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?;
        Ok::<_, CoreError>(DecodedMetadataTableBlock::Index {
            decoded_byte_len: handle.decoded_len as usize,
            entries: Arc::new(entries),
        })
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    match block {
        DecodedMetadataTableBlock::Index { entries, .. } => Ok(entries),
        DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::IndexData { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(index_segment_corrupt(
            object_key,
            "index block",
            &"cache returned a non-index block for an index key",
        )),
    }
}

/// Loads one of an index segment's data blocks, rows decoded as the gram
/// family's [`IndexRow`].
pub(crate) async fn load_index_segment_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<DecodedDataBlock<IndexRow>>> {
    let cache_key = index_section_cache_key(payload_checksum, MetadataTableBlockKind::Data, handle);
    let fetch = || async {
        let bytes = fetch_index_section_bytes(store, object_key, handle).await?;
        let block = decode_data_block_rows::<IndexRow>(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?;
        Ok::<_, CoreError>(DecodedMetadataTableBlock::IndexData {
            decoded_byte_len: decoded_index_block_weight(&block),
            block: Arc::new(block),
        })
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_fetch(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    match block {
        DecodedMetadataTableBlock::IndexData { block, .. } => Ok(block),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(index_segment_corrupt(
            object_key,
            "data block",
            &"cache returned a non-data block for a data key",
        )),
    }
}

/// In-memory weight of a decoded gram data block for the cache's byte
/// budget: row keys plus each row's packed posting bytes, with the same
/// per-row and per-block overheads the metadata weight heuristic charges.
fn decoded_index_block_weight(block: &DecodedDataBlock<IndexRow>) -> usize {
    let row_weight = block
        .row_keys
        .iter()
        .zip(&block.rows)
        .map(|(key, row)| {
            let IndexRow::GramPostings { postings, .. } = row;
            64 + key.len() + postings.len()
        })
        .sum::<usize>();
    row_weight.saturating_add(128)
}
