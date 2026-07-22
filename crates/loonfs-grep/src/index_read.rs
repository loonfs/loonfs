//! Cached reads of gram-index segment filter, index, and data blocks.

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind};
use loonfs_api::wire::index_grams::IndexRow;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_core::{Error as CoreError, Result, StoreFailureClass};
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

pub(crate) fn index_segment_corrupt(
    object_key: &str,
    what: &str,
    error: &dyn std::fmt::Display,
) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "index segment `{object_key}` carries an unreadable {what}: {error}"
    ))
}

fn cache_key(
    payload_checksum: &str,
    block_kind: GrepBlockKind,
    handle: &BlockHandle,
) -> GrepBlockCacheKey {
    GrepBlockCacheKey {
        payload_checksum: payload_checksum.to_owned(),
        block_kind,
        block_offset: handle.offset,
    }
}

async fn load_index_section_bytes<S: ObjectStore + ?Sized>(
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
        .map_err(|error| CoreError::Store {
            object_key: object_key.to_owned(),
            message: error.message(),
            class: StoreFailureClass::of(&error),
        })?
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "manifest references missing index segment `{object_key}`"
            ))
        })?;
    Ok(bytes.to_vec())
}

pub(crate) async fn load_filter_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<SegmentFilter>> {
    let key = cache_key(payload_checksum, GrepBlockKind::Filter, handle);
    if let Some(DecodedGrepBlock::Filter(filter)) = cache.get(&key) {
        return Ok(filter);
    }
    let bytes = load_index_section_bytes(store, object_key, handle).await?;
    let filter = Arc::new(
        decode_filter_block(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
    );
    cache.insert(key, DecodedGrepBlock::Filter(Arc::clone(&filter)));
    Ok(filter)
}

pub(crate) async fn load_index_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    let key = cache_key(payload_checksum, GrepBlockKind::Index, handle);
    if let Some(DecodedGrepBlock::Index(entries)) = cache.get(&key) {
        return Ok(entries);
    }
    let bytes = load_index_section_bytes(store, object_key, handle).await?;
    let entries = Arc::new(
        decode_index_block(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
    );
    cache.insert(key, DecodedGrepBlock::Index(Arc::clone(&entries)));
    Ok(entries)
}

pub(crate) async fn load_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    payload_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<DecodedDataBlock<IndexRow>>> {
    let key = cache_key(payload_checksum, GrepBlockKind::Data, handle);
    if let Some(DecodedGrepBlock::Data(block)) = cache.get(&key) {
        return Ok(block);
    }
    let bytes = load_index_section_bytes(store, object_key, handle).await?;
    let block = Arc::new(
        decode_data_block_rows::<IndexRow>(&bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?,
    );
    cache.insert(key, DecodedGrepBlock::Data(Arc::clone(&block)));
    Ok(block)
}
