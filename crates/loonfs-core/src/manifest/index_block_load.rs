//! Loading, decoding, and cache publication for segment index and filter blocks.
//!
//! Index and filter loads consult the per-view memo and the block cache
//! before they fetch from object storage.

use super::block_load::SessionBlockMemo;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey};
use super::data_block_load::decoded_data_cache_block;
use super::error::ManifestLoadError;
use super::segment_block_cache::LoadedSegmentBlock;
use crate::block_cache::SegmentBlockKind;
use crate::heap_bytes::index_block_heap_bytes;
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::{ByteRange, ObjectStore};
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, BlockHandle, SegmentFilter,
    SegmentIndexEntry,
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

/// Checks a block's stored bytes against its handle and decodes them into
/// the block that the block cache keeps.
///
/// A block loaded from the store and a block read from local SSD are both
/// decoded here, so a block from disk passes the same check.
pub fn decode_stored_block(
    object_key: &str,
    kind: SegmentBlockKind,
    handle: &BlockHandle,
    stored: &[u8],
) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
    let codec_error = |error| segment_codec_error(object_key, error);
    match kind {
        SegmentBlockKind::Index => Ok(index_cache_block(
            decode_index_block(stored, handle).map_err(codec_error)?,
        )),
        SegmentBlockKind::Filter => Ok(filter_cache_block(
            decode_filter_block(stored, handle).map_err(codec_error)?,
        )),
        SegmentBlockKind::Data => Ok(decoded_data_cache_block(
            decode_data_block(stored, handle).map_err(codec_error)?,
        )),
        SegmentBlockKind::Manifest => Err(segment_codec_error(
            object_key,
            "a manifest isn't a segment block",
        )),
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
) -> Result<LoadedSegmentBlock, ManifestLoadError> {
    let object_key = metadata_segment_object_key(descriptor);
    let object_bytes = segment_object_bytes(descriptor);
    let whole_object = object_bytes <= MAX_WHOLE_SEGMENT_LOAD_BYTES;
    let fetch_offset = match want {
        SegmentBlockKind::Data | SegmentBlockKind::Manifest => {
            return Err(segment_codec_error(
                &object_key,
                "segment section fetch supports only filter and index blocks",
            ));
        }
        _ if whole_object => 0,
        SegmentBlockKind::Filter => descriptor.filter_block.offset,
        SegmentBlockKind::Index => descriptor.index_block.offset,
    };
    let bytes = Bytes::from(
        load_section_bytes(
            store,
            &object_key,
            fetch_offset,
            object_bytes - fetch_offset,
        )
        .await?,
    );
    let (index_block, filter_block) = publish_segment_sections(
        segment_cache,
        memo,
        descriptor,
        &bytes,
        fetch_offset,
        whole_object,
    )?;
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

/// The kind of process that wrote a segment.
///
/// A fold and a compaction insert different blocks of the segments they write
/// into the segment cache, as [`insert_written_segment`] describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SegmentWriter {
    /// A fold, which writes the rows of the writer's own WAL tail.
    Fold,
    /// A compaction, which rewrites rows that are already in segments.
    Compaction,
}

/// Inserts the blocks of a segment that this process wrote into
/// `segment_cache`, and returns an error if one of those blocks doesn't
/// decode.
///
/// A fold inserts every block of its segments, and a compaction inserts each
/// segment's index and filter blocks, and its data blocks only when the
/// segment is small enough to fetch whole. The writer that just folded is the
/// process most likely to read the folded rows next. No read asked for the
/// rows that a compaction rewrites, so a compaction inserts only the blocks
/// that the first fetch of the segment would insert. Each inserted block is
/// charged to the cache's budget and evicted like any other block.
pub(super) fn insert_written_segment(
    segment_cache: &MetadataSegmentCache,
    descriptor: &MetadataSegmentRef,
    bytes: &Bytes,
    writer: SegmentWriter,
) -> Result<(), ManifestLoadError> {
    let data_blocks = match writer {
        SegmentWriter::Fold => true,
        SegmentWriter::Compaction => {
            segment_object_bytes(descriptor) <= MAX_WHOLE_SEGMENT_LOAD_BYTES
        }
    };
    publish_segment_sections(Some(segment_cache), None, descriptor, bytes, 0, data_blocks)?;
    Ok(())
}

/// Decodes and publishes the index and filter sections in `bytes`, which
/// start at `fetch_offset` in the segment object, and the data sections too
/// when `data_blocks` is set, which requires `bytes` to be the whole object.
/// Returns the index and filter blocks with their stored bytes, or `None`
/// for a section outside `bytes`.
fn publish_segment_sections(
    segment_cache: Option<&MetadataSegmentCache>,
    memo: Option<&SessionBlockMemo>,
    descriptor: &MetadataSegmentRef,
    bytes: &Bytes,
    fetch_offset: u64,
    data_blocks: bool,
) -> Result<(Option<LoadedSegmentBlock>, Option<LoadedSegmentBlock>), ManifestLoadError> {
    let object_key = metadata_segment_object_key(descriptor);
    let filter_handle = descriptor.filter_block;
    let index_handle = descriptor.index_block;
    let section = |handle: &BlockHandle| -> Option<Bytes> {
        let start = usize::try_from(handle.offset.checked_sub(fetch_offset)?).ok()?;
        let end = start.checked_add(handle.stored_bytes as usize)?;
        (end <= bytes.len()).then(|| bytes.slice(start..end))
    };

    let index_entries = match section(&index_handle) {
        Some(stored) => {
            let block =
                decode_stored_block(&object_key, SegmentBlockKind::Index, &index_handle, &stored)?;
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(descriptor, SegmentBlockKind::Index, index_handle.offset),
                &block,
                &stored,
            );
            Some(LoadedSegmentBlock { block, stored })
        }
        None => None,
    };
    let filter_block = match section(&filter_handle) {
        Some(stored) => {
            let block = decode_stored_block(
                &object_key,
                SegmentBlockKind::Filter,
                &filter_handle,
                &stored,
            )?;
            publish_segment_block(
                segment_cache,
                memo,
                segment_block_cache_key(descriptor, SegmentBlockKind::Filter, filter_handle.offset),
                &block,
                &stored,
            );
            Some(LoadedSegmentBlock { block, stored })
        }
        None => None,
    };
    if data_blocks {
        if let Some(LoadedSegmentBlock {
            block: DecodedMetadataSegmentBlock::Index { entries, .. },
            ..
        }) = &index_entries
        {
            for entry in entries.iter() {
                let Some(stored) = section(&entry.block) else {
                    return Err(segment_codec_error(
                        &object_key,
                        "data block outside the segment object bounds",
                    ));
                };
                if let Some(memo) = memo {
                    memo.record_stored(
                        &segment_block_cache_key(
                            descriptor,
                            SegmentBlockKind::Data,
                            entry.block.offset,
                        ),
                        &stored,
                    );
                    continue;
                }
                let block = decode_stored_block(
                    &object_key,
                    SegmentBlockKind::Data,
                    &entry.block,
                    &stored,
                )?;
                publish_segment_block(
                    segment_cache,
                    memo,
                    segment_block_cache_key(descriptor, SegmentBlockKind::Data, entry.block.offset),
                    &block,
                    &stored,
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
    stored: &[u8],
) {
    if let Some(memo) = memo {
        memo.record(&cache_key, block);
    }
    if let Some(cache) = segment_cache {
        cache
            .blocks()
            .insert_block(cache_key, block.clone(), stored);
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
    let fetch = async {
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
            let stored = Bytes::from(
                load_section_bytes(
                    store,
                    &object_key,
                    handle.offset,
                    u64::from(handle.stored_bytes),
                )
                .await?,
            );
            Ok(LoadedSegmentBlock {
                block: decode_stored_block(&object_key, SegmentBlockKind::Index, &handle, &stored)?,
                stored,
            })
        }
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
    block.into_index(&metadata_segment_object_key(descriptor))
}

/// Loads a segment's bloom filter block: the cheap pre-index check a lookup
/// consults to skip the segment entirely. A copy inlined in the manifest
/// descriptor answers without any object fetch; otherwise one ranged GET
/// covers the filter together with the adjacent index block (and the whole
/// object when it is small), so an admitted lookup does not pay a second
/// round-trip for the index.
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
        let block = decode_stored_block(&object_key, SegmentBlockKind::Filter, &handle, &bytes)?;
        publish_segment_block(segment_cache, Some(memo), cache_key, &block, &bytes);
        return block.into_filter(&object_key);
    }
    let fetch = load_and_publish_segment_sections(
        store,
        segment_cache,
        Some(memo),
        descriptor,
        SegmentBlockKind::Filter,
    );
    let block = match segment_cache {
        Some(cache) => {
            cache
                .blocks()
                .get_or_load_block(&cache_key, &handle, Box::pin(fetch))
                .await?
        }
        None => fetch.await?.block,
    };
    memo.record(&cache_key, &block);
    block.into_filter(&metadata_segment_object_key(descriptor))
}
