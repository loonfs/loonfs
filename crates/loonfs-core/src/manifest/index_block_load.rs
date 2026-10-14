//! Loading, decoding, and cache publication for segment index and filter blocks.
//!
//! Index, filter, and point data loads consult the per-view memo, the decoded
//! block cache, and the node-local cache of stored bytes. Their fetched
//! sections, including whole-small-segment loads, are offered to the local
//! tier. Wide data spans skip that tier because its awaited point reads lose
//! the coalescing provided by ranged store GETs.

use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey};
use super::data_block_load::decoded_data_cache_block;
use super::error::ManifestLoadError;
use super::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
use crate::block_cache::SegmentBlockKind;
use crate::heap_bytes::index_block_heap_bytes;
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::{ByteRange, ObjectStore};
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, BlockHandle,
    SegmentBlockCodecError, SegmentFilter, SegmentIndexEntry,
};
use std::sync::Arc;

pub(super) fn segment_block_cache_key(
    descriptor: &MetadataSegmentRef,
    block_kind: SegmentBlockKind,
    block_offset: u64,
) -> MetadataSegmentCacheKey {
    MetadataSegmentCacheKey {
        identity: metadata_segment_object_key(descriptor),
        block_kind,
        block_offset,
    }
}

fn stored_block_key(
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        owner_namespace_id: descriptor.owner_namespace_id.clone(),
        segment_id: descriptor.segment_id.clone(),
        kind,
        offset: handle.offset,
    }
}

/// Returns the stored-block cache associated with the decoded segment cache.
/// A compaction reads its input with no segment cache, so it uses neither cache.
fn stored_block_cache(
    segment_cache: Option<&MetadataSegmentCache>,
) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
    segment_cache?.stored_block_cache()
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
    segment_cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    decode: impl FnOnce(&[u8], &BlockHandle) -> Result<T, SegmentBlockCodecError>,
) -> Option<T> {
    let cache = stored_block_cache(segment_cache)?;
    let key = stored_block_key(descriptor, kind, handle);
    let bytes = cache.get(&key).await?;
    // The decoder checks the stored length too; checking it first keeps a
    // truncated entry from reaching the decompressor at all.
    let decoded = if bytes.len() == handle.stored_bytes as usize {
        decode(&bytes, handle).map_err(|error| error.to_string())
    } else {
        Err(format!(
            "entry holds {} bytes, expected {}",
            bytes.len(),
            handle.stored_bytes
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
                object_key = %metadata_segment_object_key(descriptor),
                error = %reason,
                "local block cache entry did not decode, refetching from the store"
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
    segment_cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    handle: &BlockHandle,
    stored: &[u8],
) {
    let Some(cache) = stored_block_cache(segment_cache) else {
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
    size_bytes: u64,
) -> Result<Vec<u8>, ManifestLoadError> {
    let end_exclusive = offset.checked_add(size_bytes).ok_or_else(|| {
        ManifestLoadError::SegmentDescriptorMismatch {
            object_key: object_key.to_owned(),
            message: "the named section runs past the address space".to_owned(),
        }
    })?;
    let Some(bytes) = store
        .get(
            object_key,
            Some(ByteRange {
                start_inclusive: offset,
                end_exclusive,
            }),
        )
        .await
        .map_err(|err| ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: err.public_message().into_owned(),
            kind: loonfs_objectstore::ObjectStoreErrorKind::of(&err),
        })?
    else {
        return Err(ManifestLoadError::MissingSegment {
            object_key: object_key.to_owned(),
        });
    };
    if bytes.len() as u64 != size_bytes {
        return Err(ManifestLoadError::ReadSegment {
            object_key: object_key.to_owned(),
            message: format!(
                "ranged read returned {} bytes, expected {size_bytes}",
                bytes.len()
            ),
            kind: loonfs_objectstore::ObjectStoreErrorKind::Other,
        });
    }
    Ok(bytes.to_vec())
}

pub(super) fn segment_codec_error(
    object_key: &str,
    error: impl std::fmt::Display,
) -> ManifestLoadError {
    ManifestLoadError::SegmentCodec {
        object_key: object_key.to_owned(),
        message: error.to_string(),
    }
}

/// Largest segment object fetched whole on first touch, in stored bytes.
/// Below this, splitting the index and data reads into separate ranged GETs
/// costs more round-trips than the whole object costs bytes; one GET
/// publishes every section to the memo and shared cache. Sized to catch
/// delta-run segments (one or two data blocks) while leaving base segments
/// on the per-section path.
const MAX_WHOLE_SEGMENT_LOAD_BYTES: u64 = 256 * 1024;

/// A segment object's total stored length: the index block is the last
/// section, so it ends the object.
pub(super) fn segment_object_bytes(descriptor: &MetadataSegmentRef) -> u64 {
    descriptor.index_block.offset + u64::from(descriptor.index_block.stored_bytes)
}

// Small objects cost one round trip. Retain their data bytes so an index
// lookup does not decode unrelated rows before another read can start.
async fn load_and_publish_segment_sections<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    want: SegmentBlockKind,
) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
    let object_key = metadata_segment_object_key(descriptor);
    let object_bytes = segment_object_bytes(descriptor);
    let fetch_offset = match want {
        SegmentBlockKind::Data | SegmentBlockKind::Manifest => {
            return Err(segment_codec_error(
                &object_key,
                "segment section fetch supports only filter and index blocks",
            ));
        }
        _ if object_bytes <= MAX_WHOLE_SEGMENT_LOAD_BYTES => 0,
        SegmentBlockKind::Filter => descriptor.filter_block.offset,
        SegmentBlockKind::Index => descriptor.index_block.offset,
    };
    let bytes = load_section_bytes(
        store,
        &object_key,
        fetch_offset,
        object_bytes - fetch_offset,
    )
    .await?;
    let (index_block, filter_block) =
        publish_segment_sections(segment_cache, memo, descriptor, &bytes, fetch_offset)?;
    let wanted = match want {
        SegmentBlockKind::Filter => filter_block,
        _ => index_block,
    };
    wanted.ok_or_else(|| {
        segment_codec_error(
            &object_key,
            "requested section outside the segment object bounds",
        )
    })
}

/// Inserts the sections of a segment that this process wrote into
/// `segment_cache`, and offers them to its node-local tier. These are the
/// sections that the first fetch of the segment would insert: the index and
/// filter blocks, and the data blocks of a segment small enough to fetch whole.
/// Returns an error if a section of the segment doesn't decode.
pub(super) fn insert_written_segment(
    segment_cache: &MetadataSegmentCache,
    descriptor: &MetadataSegmentRef,
    bytes: &[u8],
) -> Result<(), ManifestLoadError> {
    publish_segment_sections(Some(segment_cache), None, descriptor, bytes, 0)?;
    Ok(())
}

/// Decodes and publishes the index and filter sections in `bytes`, which
/// start at `fetch_offset` in the segment object, and the data sections too
/// when `bytes` is the whole of a small object. Returns the index and filter
/// blocks, or `None` for a section outside `bytes`.
fn publish_segment_sections(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    bytes: &[u8],
    fetch_offset: u64,
) -> Result<
    (
        Option<DecodedMetadataSegmentBlock>,
        Option<DecodedMetadataSegmentBlock>,
    ),
    ManifestLoadError,
> {
    let object_key = metadata_segment_object_key(descriptor);
    let filter_handle = descriptor.filter_block;
    let index_handle = descriptor.index_block;
    let fetch_whole_object = segment_object_bytes(descriptor) <= MAX_WHOLE_SEGMENT_LOAD_BYTES;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset.checked_sub(fetch_offset)?).ok()?;
        let end = start.checked_add(handle.stored_bytes as usize)?;
        bytes.get(start..end)
    };

    let index_entries = match section(&index_handle) {
        Some(stored) => {
            offer_stored_block(
                segment_cache,
                descriptor,
                StoredMetadataBlockKind::Index,
                &index_handle,
                stored,
            );
            let block = index_cache_block(
                decode_index_block(stored, &index_handle)
                    .map_err(|err| segment_codec_error(&object_key, err))?,
            );
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(descriptor, SegmentBlockKind::Index, index_handle.offset),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    let filter_block = match section(&filter_handle) {
        Some(stored) => {
            offer_stored_block(
                segment_cache,
                descriptor,
                StoredMetadataBlockKind::Filter,
                &filter_handle,
                stored,
            );
            let filter = decode_filter_block(stored, &filter_handle)
                .map_err(|err| segment_codec_error(&object_key, err))?;
            let block = filter_cache_block(filter);
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(descriptor, SegmentBlockKind::Filter, filter_handle.offset),
                &block,
            );
            Some(block)
        }
        None => None,
    };
    if fetch_whole_object {
        if let Some(DecodedMetadataSegmentBlock::Index { entries, .. }) = &index_entries {
            for entry in entries.iter() {
                let Some(stored) = section(&entry.block) else {
                    return Err(segment_codec_error(
                        &object_key,
                        "data block outside the segment object bounds",
                    ));
                };
                offer_stored_block(
                    segment_cache,
                    descriptor,
                    StoredMetadataBlockKind::Data,
                    &entry.block,
                    stored,
                );
                if let Some(memo) = memo {
                    memo.record_stored(
                        &segment_block_cache_key(
                            descriptor,
                            SegmentBlockKind::Data,
                            entry.block.offset,
                        ),
                        stored,
                    );
                    continue;
                }
                let decoded = decode_data_block(stored, &entry.block)
                    .map_err(|err| segment_codec_error(&object_key, err))?;
                let block = decoded_data_cache_block(decoded);
                publish_segment_block(
                    segment_cache,
                    memo,
                    segment_block_cache_key(descriptor, SegmentBlockKind::Data, entry.block.offset),
                    &block,
                );
            }
        }
    }

    Ok((index_entries, filter_block))
}

fn filter_cache_block(filter: SegmentFilter) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Filter {
        decoded_bytes: filter.decoded_bytes() + crate::heap_bytes::arc_bytes::<()>(),
        filter: Arc::new(filter),
    }
}

fn index_cache_block(entries: Vec<SegmentIndexEntry>) -> DecodedMetadataSegmentBlock {
    DecodedMetadataSegmentBlock::Index {
        decoded_bytes: index_block_heap_bytes(&entries),
        entries: Arc::new(entries),
    }
}

fn publish_segment_block(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    cache_key: MetadataSegmentCacheKey,
    block: &DecodedMetadataSegmentBlock,
) {
    if let Some(memo) = memo {
        memo.record(&cache_key, block);
    }
    if let Some(cache) = segment_cache {
        cache.insert(cache_key, block.clone());
    }
}

pub(super) async fn load_segment_index<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, segment_cache, Some(memo), descriptor, true).await
}

/// Loads only the index section even for a small segment. Compaction
/// uses this to account data-block decoded bytes before any row payload is
/// decoded; the normal lookup path keeps its whole-small-segment shortcut.
pub(super) async fn load_segment_index_for_compaction<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    load_segment_index_inner(store, segment_cache, memo, descriptor, false).await
}

async fn load_segment_index_inner<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    load_small_segment_whole: bool,
) -> Result<Arc<Vec<SegmentIndexEntry>>, ManifestLoadError> {
    let handle = descriptor.index_block;
    let cache_key = segment_block_cache_key(descriptor, SegmentBlockKind::Index, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Index { entries, .. }) =
        memo.and_then(|memo| memo.get(&cache_key))
    {
        return Ok(entries);
    }
    let fetch = || async {
        // Between the decoded cache above and the store below: a local copy
        // of the same stored bytes. A hit answers this section and nothing
        // else, so the sibling sections a fetch would have published are
        // published only when a fetch happens.
        if let Some(entries) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Index,
            &handle,
            decode_index_block,
        )
        .await
        {
            return Ok(index_cache_block(entries));
        }
        if load_small_segment_whole {
            load_and_publish_segment_sections(
                store,
                segment_cache,
                memo,
                descriptor,
                SegmentBlockKind::Index,
            )
            .await
        } else {
            let object_key = metadata_segment_object_key(descriptor);
            let stored = load_section_bytes(
                store,
                &object_key,
                handle.offset,
                u64::from(handle.stored_bytes),
            )
            .await?;
            let entries = decode_index_block(&stored, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?;
            Ok(index_cache_block(entries))
        }
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    if let Some(memo) = memo {
        memo.record(&cache_key, &block);
    }
    block.into_index(&metadata_segment_object_key(descriptor))
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
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
) -> Result<Arc<SegmentFilter>, ManifestLoadError> {
    let handle = descriptor.filter_block;
    let cache_key = segment_block_cache_key(descriptor, SegmentBlockKind::Filter, handle.offset);
    if let Some(DecodedMetadataSegmentBlock::Filter { filter, .. }) = memo.get(&cache_key) {
        return Ok(filter);
    }
    if let Some(inline) = &descriptor.filter_inline {
        let object_key = metadata_segment_object_key(descriptor);
        let bytes = hex_decode_bytes(inline)
            .map_err(|err| segment_codec_error(&object_key, format!("inline filter: {err}")))?;
        // The handle names and verifies the durable filter block; decoding
        // the inline copy against it proves the two are byte-identical.
        let block = filter_cache_block(
            decode_filter_block(&bytes, &handle)
                .map_err(|err| segment_codec_error(&object_key, err))?,
        );
        publish_segment_block(segment_cache, Some(memo), cache_key, &block);
        return block.into_filter(&object_key);
    }
    let fetch = || async {
        // Reached only when the descriptor carried no inline copy: an
        // inlined filter is answered above and never touches this tier.
        if let Some(filter) = stored_block_section(
            segment_cache,
            descriptor,
            StoredMetadataBlockKind::Filter,
            &handle,
            decode_filter_block,
        )
        .await
        {
            return Ok(filter_cache_block(filter));
        }
        load_and_publish_segment_sections(
            store,
            segment_cache,
            Some(memo),
            descriptor,
            SegmentBlockKind::Filter,
        )
        .await
    };
    let block = match segment_cache {
        Some(cache) => cache.get_or_load(&cache_key, fetch).await?,
        None => fetch().await?,
    };
    memo.record(&cache_key, &block);
    block.into_filter(&metadata_segment_object_key(descriptor))
}
