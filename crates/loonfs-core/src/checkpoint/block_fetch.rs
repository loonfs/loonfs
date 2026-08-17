//! Fetching, decoding, and cache publication for SST index and filter blocks.
//!
//! Index, filter, and point data loads consult the per-view memo, the decoded
//! block cache, and the node-local cache of stored bytes. Their fetched
//! sections, including whole-small-segment loads, are offered to the local
//! tier. Wide data spans skip that tier because its awaited point reads lose
//! the coalescing provided by ranged store GETs.

use super::block_load::SessionBlockMemo;
use super::cache::{
    DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheKey,
};
use super::data_block_load::decoded_data_cache_block;
use super::error::ManifestLoadError;
use super::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
use bytes::Bytes;
use loonfs_api::wire::hex::hex_decode_bytes;
use loonfs_api::wire::manifest::MetadataFileRef;
use loonfs_api::wire::sst_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, BlockHandle, SegmentFilter,
    SegmentIndexEntry, SstBlockCodecError,
};
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

pub(super) fn segment_block_cache_key(
    descriptor: &MetadataFileRef,
    block_kind: MetadataTableBlockKind,
    block_offset: u64,
) -> MetadataTableCacheKey {
    MetadataTableCacheKey {
        identity: descriptor.payload_checksum.clone(),
        block_kind,
        block_offset,
    }
}

/// The local stored-block cache's key for one section of a segment.
///
/// The identity is the segment's payload checksum, the same immutable bytes
/// the decoded cache keys by, and the handle's offset locates the section
/// inside the object.
fn stored_block_key(
    descriptor: &MetadataFileRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        payload_checksum: descriptor.payload_checksum.clone(),
        kind,
        offset: handle.offset,
    }
}

/// The local stored-block cache this read may consult, which exists exactly
/// where the decoded cache does: a path carrying no table cache — every
/// maintenance path — reaches neither tier.
fn stored_block_cache(
    table_cache: Option<&MetadataTableCache>,
) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
    table_cache?.stored_block_cache()
}

/// Answers one section from the local stored-block cache, when it holds
/// bytes for it that decode.
///
/// The bytes go to the same decoder the store's bytes go to, so a hit is
/// checksummed and structure-checked exactly as a fetch is. A hit that does
/// not decode says nothing about the segment — object storage still holds
/// the authority — so the entry is dropped and the caller falls through to
/// one ordinary fetch, which reports corruption if the store's bytes fail
/// too. Nothing retries the local tier.
pub(super) async fn stored_block_section<T>(
    table_cache: Option<&MetadataTableCache>,
    descriptor: &MetadataFileRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    decode: impl FnOnce(&[u8], &BlockHandle) -> Result<T, SstBlockCodecError>,
) -> Option<T> {
    let cache = stored_block_cache(table_cache)?;
    let key = stored_block_key(descriptor, kind, handle);
    let bytes = cache.get(&key).await?;
    // The decoder checks the stored length too; checking it first keeps a
    // truncated entry from reaching the decompressor at all.
    let decoded = if bytes.len() == handle.stored_len as usize {
        decode(&bytes, handle).map_err(|error| error.to_string())
    } else {
        Err(format!(
            "entry holds {} bytes, expected {}",
            bytes.len(),
            handle.stored_len
        ))
    };
    match decoded {
        Ok(decoded) => Some(decoded),
        Err(reason) => {
            // The read still succeeds from the store, but bytes this tier
            // wrote and read back no longer decode: the local disk is
            // returning something other than what it was given. That is a
            // hardware or filesystem problem an operator has to know about,
            // so it is a warning rather than a debug line.
            tracing::warn!(
                object_key = %descriptor.object_key,
                "local block cache entry did not decode, refetching from the store: {reason}"
            );
            cache.invalidate(&key);
            None
        }
    }
}

/// Offers one section's stored bytes to the local stored-block cache.
///
/// Every section a fetch produced is offered under its own key, so a later
/// read of any one of them needs no GET. The bytes are copied rather than
/// sliced out of the fetch buffer: a slice would keep the whole fetched span
/// alive for as long as the cache held any one section of it.
pub(super) fn offer_stored_block(
    table_cache: Option<&MetadataTableCache>,
    descriptor: &MetadataFileRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    stored: &[u8],
) {
    let Some(cache) = stored_block_cache(table_cache) else {
        return;
    };
    cache.insert(
        stored_block_key(descriptor, kind, handle),
        Bytes::copy_from_slice(stored),
    );
}

/// Fetches exactly the byte range a handle names.
pub(super) async fn load_section_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, ManifestLoadError> {
    let Some(bytes) = store
        .get(
            object_key,
            Some(ByteRange {
                start_inclusive: offset,
                end_exclusive: offset + len,
            }),
        )
        .await
        .map_err(|err| ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        })?
    else {
        return Err(ManifestLoadError::MissingSegment {
            object_key: object_key.to_owned(),
        });
    };
    if bytes.len() as u64 != len {
        return Err(ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: format!("ranged read returned {} bytes, expected {len}", bytes.len()),
        });
    }
    Ok(bytes.to_vec())
}

pub(super) fn segment_codec_error(
    object_key: &str,
    err: impl std::fmt::Display,
) -> ManifestLoadError {
    ManifestLoadError::SegmentCodec {
        object_key: object_key.to_owned(),
        message: err.to_string(),
    }
}

/// Largest segment object fetched whole on first touch, in stored bytes.
/// Below this, splitting the index and data reads into separate ranged GETs
/// costs more round-trips than the whole object costs bytes; one GET
/// publishes every section to the memo and shared cache. Sized to catch
/// delta-run segments (one or two data blocks) while leaving base segments
/// on the per-section path.
const WHOLE_SEGMENT_LOAD_MAX_BYTES: u64 = 128 * 1024;

/// A segment object's total stored length: the index block is the last
/// section, so it ends the object.
pub(super) fn segment_object_len(descriptor: &MetadataFileRef) -> u64 {
    descriptor.index_block.offset + u64::from(descriptor.index_block.stored_len)
}

/// Fetches the byte span that answers one filter or index load with a single
/// GET, decoding and publishing every section the span covers: the whole
/// object when it is small (index, filter, and all data blocks), otherwise
/// everything from the requested section to the end of the object — for a
/// filter that is the filter plus the index that directly follows it
/// (manifest loading rejects any other layout), and for an index exactly the
/// index, which ends the object. Returns the requested block.
///
/// Every section the span covers is also offered to the local stored-block
/// cache in its stored form, so what one GET produced is what a later read
/// finds there.
async fn load_and_publish_segment_sections<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
    want: MetadataTableBlockKind,
) -> Result<DecodedMetadataTableBlock, ManifestLoadError> {
    let filter_handle = descriptor.filter_block;
    let index_handle = descriptor.index_block;
    let object_len = segment_object_len(descriptor);
    let fetch_whole_object = object_len <= WHOLE_SEGMENT_LOAD_MAX_BYTES;
    let fetch_offset = match want {
        MetadataTableBlockKind::Data | MetadataTableBlockKind::Manifest => {
            return Err(segment_codec_error(
                &descriptor.object_key,
                "segment section fetch supports only filter and index blocks",
            ));
        }
        _ if fetch_whole_object => 0,
        MetadataTableBlockKind::Filter => filter_handle.offset,
        MetadataTableBlockKind::Index => index_handle.offset,
    };
    let bytes = load_section_bytes(
        store,
        &descriptor.object_key,
        fetch_offset,
        object_len - fetch_offset,
    )
    .await?;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset.checked_sub(fetch_offset)?).ok()?;
        bytes.get(start..start + handle.stored_len as usize)
    };

    let index_entries = match section(&index_handle) {
        Some(stored) => {
            offer_stored_block(
                table_cache,
                descriptor,
                StoredMetadataBlockKind::Index,
                &index_handle,
                stored,
            );
            let entries = Arc::new(
                decode_index_block(stored, &index_handle)
                    .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
            );
            let block = DecodedMetadataTableBlock::Index {
                decoded_bytes: index_handle.decoded_len as usize,
                entries: Arc::clone(&entries),
            };
            publish_segment_block(
                table_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataTableBlockKind::Index,
                    index_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    let filter_block = match section(&filter_handle) {
        Some(stored) => {
            offer_stored_block(
                table_cache,
                descriptor,
                StoredMetadataBlockKind::Filter,
                &filter_handle,
                stored,
            );
            let filter = decode_filter_block(stored, &filter_handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?;
            let block = DecodedMetadataTableBlock::Filter {
                decoded_bytes: filter_handle.decoded_len as usize,
                filter: Arc::new(filter),
            };
            publish_segment_block(
                table_cache,
                memo,
                segment_block_cache_key(
                    descriptor,
                    MetadataTableBlockKind::Filter,
                    filter_handle.offset,
                ),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    if fetch_whole_object {
        if let Some(DecodedMetadataTableBlock::Index { entries, .. }) = &index_entries {
            for entry in entries.iter() {
                let Some(stored) = section(&entry.block) else {
                    return Err(segment_codec_error(
                        &descriptor.object_key,
                        "data block outside the segment object bounds",
                    ));
                };
                offer_stored_block(
                    table_cache,
                    descriptor,
                    StoredMetadataBlockKind::Data,
                    &entry.block,
                    stored,
                );
                let decoded = decode_data_block(stored, &entry.block)
                    .map_err(|err| segment_codec_error(&descriptor.object_key, err))?;
                let block = decoded_data_cache_block(descriptor.family, decoded);
                publish_segment_block(
                    table_cache,
                    memo,
                    segment_block_cache_key(
                        descriptor,
                        MetadataTableBlockKind::Data,
                        entry.block.offset,
                    ),
                    &block,
                );
            }
        }
    }

    let wanted = match want {
        MetadataTableBlockKind::Filter => filter_block,
        _ => index_entries,
    };
    wanted.ok_or_else(|| {
        segment_codec_error(
            &descriptor.object_key,
            "requested section outside the segment object bounds",
        )
    })
}

fn publish_segment_block(
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    cache_key: MetadataTableCacheKey,
    block: &DecodedMetadataTableBlock,
) {
    memo.record(&cache_key, block);
    if let Some(cache) = table_cache {
        cache.insert(cache_key, block.clone());
    }
}

pub(super) async fn load_segment_index<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, table_cache, memo, descriptor, true).await
}

/// Loads only the index section even for a small segment. Reorganization
/// uses this to account data-block decoded bytes before any row payload is
/// decoded; the normal lookup path keeps its whole-small-segment shortcut.
pub(super) async fn load_segment_index_for_reorganization<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, table_cache, memo, descriptor, false).await
}

async fn load_segment_index_inner<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
    load_small_segment_whole: bool,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    let handle = descriptor.index_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Index, handle.offset);
    if let Some(DecodedMetadataTableBlock::Index { entries, .. }) = memo.get(&cache_key) {
        return Ok(entries);
    }
    let fetch = || async {
        // Between the decoded cache above and the store below: a local copy
        // of the same stored bytes. A hit answers this section and nothing
        // else, so the sibling sections a fetch would have published are
        // published only when a fetch happens.
        if let Some(entries) = stored_block_section(
            table_cache,
            descriptor,
            StoredMetadataBlockKind::Index,
            &handle,
            decode_index_block,
        )
        .await
        {
            return Ok(DecodedMetadataTableBlock::Index {
                decoded_bytes: handle.decoded_len as usize,
                entries: Arc::new(entries),
            });
        }
        if load_small_segment_whole {
            load_and_publish_segment_sections(
                store,
                table_cache,
                memo,
                descriptor,
                MetadataTableBlockKind::Index,
            )
            .await
        } else {
            let stored = load_section_bytes(
                store,
                &descriptor.object_key,
                handle.offset,
                u64::from(handle.stored_len),
            )
            .await?;
            let entries = decode_index_block(&stored, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?;
            Ok(DecodedMetadataTableBlock::Index {
                decoded_bytes: handle.decoded_len as usize,
                entries: Arc::new(entries),
            })
        }
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Index { entries, .. } => Ok(entries),
        DecodedMetadataTableBlock::Filter { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-index block for an index key",
        )),
    }
}

/// Loads a segment's bloom filter block: the cheap pre-index check a lookup
/// consults to skip the segment entirely. A copy inlined in the manifest
/// descriptor answers without any object fetch; otherwise the local
/// stored-block cache answers if it holds the section, and only failing that
/// does one ranged GET cover the filter together with the adjacent index
/// block (and the whole object when it is small), so an admitted lookup does
/// not pay a second round-trip for the index.
pub(super) async fn load_segment_filter<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
) -> Result<Arc<SegmentFilter>, ManifestLoadError> {
    let handle = descriptor.filter_block;
    let cache_key =
        segment_block_cache_key(descriptor, MetadataTableBlockKind::Filter, handle.offset);
    if let Some(DecodedMetadataTableBlock::Filter { filter, .. }) = memo.get(&cache_key) {
        return Ok(filter);
    }
    if let Some(inline) = &descriptor.filter_inline {
        let bytes = hex_decode_bytes(inline).map_err(|err| {
            segment_codec_error(&descriptor.object_key, format!("inline filter: {err}"))
        })?;
        // The handle names and verifies the durable filter block; decoding
        // the inline copy against it proves the two are byte-identical.
        let filter = Arc::new(
            decode_filter_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&descriptor.object_key, err))?,
        );
        let block = DecodedMetadataTableBlock::Filter {
            decoded_bytes: handle.decoded_len as usize,
            filter: Arc::clone(&filter),
        };
        publish_segment_block(table_cache, memo, cache_key, &block);
        return Ok(filter);
    }
    let fetch = || async {
        // Reached only when the descriptor carried no inline copy: an
        // inlined filter is answered above and never touches this tier.
        if let Some(filter) = stored_block_section(
            table_cache,
            descriptor,
            StoredMetadataBlockKind::Filter,
            &handle,
            decode_filter_block,
        )
        .await
        {
            return Ok(DecodedMetadataTableBlock::Filter {
                decoded_bytes: handle.decoded_len as usize,
                filter: Arc::new(filter),
            });
        }
        load_and_publish_segment_sections(
            store,
            table_cache,
            memo,
            descriptor,
            MetadataTableBlockKind::Filter,
        )
        .await
    };
    let block = match table_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    match block {
        DecodedMetadataTableBlock::Filter { filter, .. } => Ok(filter),
        DecodedMetadataTableBlock::Index { .. }
        | DecodedMetadataTableBlock::Data { .. }
        | DecodedMetadataTableBlock::Manifest { .. } => Err(segment_codec_error(
            &descriptor.object_key,
            "cache returned a non-filter block",
        )),
    }
}
