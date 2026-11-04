//! Cached reads of gram-index segment filter, index, and data blocks.

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey};
use crate::codec::IndexRow;
use crate::{GrepError, Result};
use loonfs::engine::SegmentBlockKind;
use loonfs_objectstore::ObjectStoreErrorKind;
use loonfs_objectstore::{ByteRange, ObjectStore};
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_types::IndexSegmentId;
use std::sync::Arc;

/// Largest index segment object fetched whole on first touch, in stored bytes.
/// Below this, one GET costs less than separate ranged reads of the filter,
/// index, and data blocks; 128 KiB covers a segment of one or two 64 KiB data
/// blocks.
pub(crate) const MAX_WHOLE_SEGMENT_LOAD_BYTES: u64 = 128 * 1024;
// Matches the private metadata span limit in segment/data_block_load.rs.
const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) fn index_segment_corrupt(
    object_key: &str,
    what: &str,
    error: &dyn std::fmt::Display,
) -> GrepError {
    GrepError::IndexCorrupt {
        message: format!("index segment `{object_key}` carries an unreadable {what}: {error}"),
    }
}

fn cache_key(
    segment_id: &IndexSegmentId,
    block_kind: SegmentBlockKind,
    handle: &BlockHandle,
) -> GrepBlockCacheKey {
    GrepBlockCacheKey {
        identity: segment_id.to_string(),
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
        .checked_add(u64::from(handle.stored_bytes))
        .ok_or_else(|| GrepError::IndexCorrupt {
            message: format!(
                "index segment `{object_key}` descriptor names bytes past the address space"
            ),
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
        .map_err(|error| GrepError::store(object_key, &error))?
        .ok_or_else(|| GrepError::IndexCorrupt {
            message: format!("grep's entry lists missing index segment `{object_key}`"),
        })?;
    if bytes.len() != handle.stored_bytes as usize {
        return Err(GrepError::StoreUnavailable {
            object_key: object_key.to_owned(),
            message: format!(
                "ranged read returned {} bytes, expected {}",
                bytes.len(),
                handle.stored_bytes
            ),
            kind: ObjectStoreErrorKind::Other,
        });
    }
    Ok(bytes.to_vec())
}

pub(crate) fn segment_object_bytes(
    object_key: &str,
    descriptor: &ExtensionSegmentRef,
) -> Result<u64> {
    descriptor
        .index_block
        .offset
        .checked_add(u64::from(descriptor.index_block.stored_bytes))
        .ok_or_else(|| GrepError::IndexCorrupt {
            message: format!(
                "index segment `{object_key}` descriptor names bytes past the address space"
            ),
        })
}

/// Loads the filter block of `descriptor`'s segment through `cache` when it
/// is set (see [`load_section`]).
pub(crate) async fn load_filter_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    descriptor: &ExtensionSegmentRef,
) -> Result<Arc<SegmentFilter>> {
    load_section(
        store,
        cache,
        object_key,
        descriptor,
        SegmentBlockKind::Filter,
    )
    .await?
    .filter()
    .ok_or_else(|| cache_kind_corrupt(object_key, "filter"))
}

/// Loads the index block of `descriptor`'s segment through `cache` when it
/// is set (see [`load_section`]).
pub(crate) async fn load_index_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    descriptor: &ExtensionSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    load_section(
        store,
        cache,
        object_key,
        descriptor,
        SegmentBlockKind::Index,
    )
    .await?
    .index()
    .ok_or_else(|| cache_kind_corrupt(object_key, "index"))
}

/// Loads the `want` block of `descriptor`'s segment, its filter block or its
/// index block, through `cache` when it is set.
///
/// The index block ends the segment object and the filter block comes just
/// before it, so one ranged read from the wanted block to the end of the
/// object covers the filter together with the index block, and a lookup that
/// the filter admits reads nothing more before its data blocks. Through a
/// cache, a segment object of at most [`MAX_WHOLE_SEGMENT_LOAD_BYTES`] is read
/// whole. The blocks that the read covers go into the cache.
async fn load_section<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    descriptor: &ExtensionSegmentRef,
    want: SegmentBlockKind,
) -> Result<DecodedGrepBlock> {
    let wanted = match want {
        SegmentBlockKind::Filter => &descriptor.filter_block,
        _ => &descriptor.index_block,
    };
    let load = || async {
        let object_bytes = segment_object_bytes(object_key, descriptor)?;
        let whole_object = cache.is_some() && object_bytes <= MAX_WHOLE_SEGMENT_LOAD_BYTES;
        let start = if whole_object { 0 } else { wanted.offset };
        let read = BlockHandle {
            offset: start,
            stored_bytes: object_bytes
                .checked_sub(start)
                .and_then(|bytes| u32::try_from(bytes).ok())
                .ok_or_else(|| GrepError::IndexCorrupt {
                    message: format!(
                        "index segment `{object_key}` descriptor names a block past the end of \
                         its object"
                    ),
                })?,
            decoded_bytes: 0,
            crc32c: 0,
        };
        let bytes = load_index_section_bytes(store, object_key, &read).await?;
        let decode = |kind, handle: &BlockHandle| {
            Ok::<_, GrepError>((
                kind,
                *handle,
                decode_section(object_key, &bytes, start, kind, handle)?,
            ))
        };
        let mut blocks = vec![decode(SegmentBlockKind::Index, &descriptor.index_block)?];
        if descriptor.filter_block.offset >= start {
            blocks.push(decode(SegmentBlockKind::Filter, &descriptor.filter_block)?);
        }
        if whole_object {
            let entries = blocks[0]
                .2
                .clone()
                .index()
                .expect("the first block is the index");
            for entry in entries.iter() {
                blocks.push(decode(SegmentBlockKind::Data, &entry.block)?);
            }
        }
        let mut found = None;
        for (kind, handle, block) in blocks {
            match cache {
                _ if kind == want => found = Some(block),
                Some(cache) => {
                    cache.insert(cache_key(&descriptor.segment_id, kind, &handle), block);
                }
                None => {}
            }
        }
        Ok::<_, GrepError>(found.expect("the read covers the wanted block"))
    };
    match cache {
        Some(cache) => {
            let key = cache_key(&descriptor.segment_id, want, wanted);
            cache.get_or_load(&key, load).await
        }
        None => load().await,
    }
}

/// Decodes the `kind` block at `handle` from `bytes`, which a read of the
/// segment object from offset `start` returned.
fn decode_section(
    object_key: &str,
    bytes: &[u8],
    start: u64,
    kind: SegmentBlockKind,
    handle: &BlockHandle,
) -> Result<DecodedGrepBlock> {
    let stored = handle
        .offset
        .checked_sub(start)
        .and_then(|offset| usize::try_from(offset).ok())
        .and_then(|offset| bytes.get(offset..offset.checked_add(handle.stored_bytes as usize)?))
        .ok_or_else(|| GrepError::IndexCorrupt {
            message: format!(
                "index segment `{object_key}` {kind:?} block exceeds the object bounds"
            ),
        })?;
    let decoded_bytes = handle.decoded_bytes as usize;
    match kind {
        SegmentBlockKind::Filter => Ok(DecodedGrepBlock::Filter {
            filter: Arc::new(
                decode_filter_block(stored, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
            ),
            decoded_bytes,
        }),
        SegmentBlockKind::Index => Ok(DecodedGrepBlock::Index {
            entries: Arc::new(
                decode_index_block(stored, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
            ),
            decoded_bytes,
        }),
        _ => decode_data_cache_block(object_key, stored, handle),
    }
}

pub(crate) async fn load_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    segment_id: &IndexSegmentId,
    handle: &BlockHandle,
) -> Result<Arc<DecodedDataBlock<IndexRow>>> {
    let load = || async {
        let bytes = load_index_section_bytes(store, object_key, handle).await?;
        decode_data_cache_block(object_key, &bytes, handle)
    };
    let decoded = match cache {
        Some(cache) => {
            let key = cache_key(segment_id, SegmentBlockKind::Data, handle);
            cache.get_or_load(&key, load).await?
        }
        None => load().await?,
    };
    decoded
        .data()
        .ok_or_else(|| cache_kind_corrupt(object_key, "data"))
}

fn decode_data_cache_block(
    object_key: &str,
    bytes: &[u8],
    handle: &BlockHandle,
) -> Result<DecodedGrepBlock> {
    let block = Arc::new(
        decode_data_block_rows::<IndexRow>(bytes, handle)
            .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?,
    );
    Ok(DecodedGrepBlock::Data {
        block,
        decoded_bytes: handle.decoded_bytes as usize,
    })
}

/// Loads the data blocks of `entries` through `cache` when it is set, and
/// reads each run of adjacent blocks that the cache doesn't have in one
/// ranged read of at most `MAX_BULK_LOAD_BYTES`.
pub(crate) async fn load_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    segment_id: &IndexSegmentId,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock<IndexRow>>>> {
    let key =
        |entry: &SegmentIndexEntry| cache_key(segment_id, SegmentBlockKind::Data, &entry.block);
    let mut blocks = entries
        .iter()
        .map(
            |entry| match cache.and_then(|cache| cache.get(&key(entry))) {
                Some(cached) => cached
                    .data()
                    .map(Some)
                    .ok_or_else(|| cache_kind_corrupt(object_key, "data")),
                None => Ok(None),
            },
        )
        .collect::<Result<Vec<_>>>()?;
    let mut cursor = 0;
    while cursor < entries.len() {
        if blocks[cursor].is_some() {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut handle = entries[start].block;
        let mut span_bytes = u64::from(handle.stored_bytes);
        cursor += 1;
        while cursor < entries.len() && blocks[cursor].is_none() {
            let next = entries[cursor].block;
            let next_span_bytes = span_bytes + u64::from(next.stored_bytes);
            if handle.offset.checked_add(span_bytes) != Some(next.offset)
                || next_span_bytes > MAX_BULK_LOAD_BYTES
            {
                break;
            }
            span_bytes = next_span_bytes;
            cursor += 1;
        }
        handle.stored_bytes = u32::try_from(span_bytes)
            .expect("a span should fit the byte limit or contain one u32-sized block");
        let bytes = load_index_section_bytes(store, object_key, &handle).await?;
        for (entry, slot) in entries[start..cursor]
            .iter()
            .zip(&mut blocks[start..cursor])
        {
            let offset = loonfs_types::usize_from_u64(entry.block.offset - handle.offset);
            let stored = &bytes[offset..offset + entry.block.stored_bytes as usize];
            let block = Arc::new(
                decode_data_block_rows::<IndexRow>(stored, &entry.block)
                    .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?,
            );
            if let Some(cache) = cache {
                cache.insert(
                    key(entry),
                    DecodedGrepBlock::Data {
                        block: Arc::clone(&block),
                        decoded_bytes: entry.block.decoded_bytes as usize,
                    },
                );
            }
            *slot = Some(block);
        }
    }
    Ok(blocks
        .into_iter()
        .map(|block| block.expect("every block should be cached or loaded"))
        .collect())
}

fn cache_kind_corrupt(object_key: &str, expected: &str) -> GrepError {
    GrepError::IndexCorrupt {
        message: format!(
            "index segment `{object_key}` resolved its {expected} block to a different cache kind"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cache_key, load_data_block_span, load_index_section_bytes, BlockHandle, GrepError,
        IndexRow, MAX_BULK_LOAD_BYTES,
    };
    use crate::cache::GrepBlockCache;
    use crate::codec::Gram;
    use bytes::Bytes;
    use loonfs::engine::{DecodedBlockCacheConfig, SegmentBlockKind};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ByteRange, ObjectStore, PutMode, Result as StoreResult};
    use loonfs_test_support::stores::{
        delegate_object_store, ConcurrencyWatchStore, KeyPredicate, RecordingStore,
    };
    use loonfs_types::format::segment_blocks::{decode_index_block, SegmentBlocksBuilder};
    use loonfs_types::IndexSegmentId;
    use std::assert_matches;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Checks that a span read stays within the byte limit, and that a load
    /// through a cache reads only the blocks that the cache doesn't have, in
    /// spans of adjacent blocks.
    #[tokio::test]
    async fn data_spans_obey_the_byte_limit_and_skip_cached_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let watched = Arc::new(ConcurrencyWatchStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let store = RecordingStore::new(Arc::clone(&watched), KeyPredicate::any());
        let object_key = "segments/span";
        let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
        let mut rows = Vec::new();
        let mut state = 1u64;
        for gram in 0..12 {
            // Gaps of up to 2^24 take about four bytes each, so a row is
            // about 750 KiB and a span reads five rows.
            let mut ordinal = 0;
            let postings = (0..196_608)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    ordinal += 1 + (state & 0xff_ffff);
                    ordinal
                })
                .collect::<Vec<_>>();
            let row =
                IndexRow::gram_postings(Gram([0, 0, gram]), &postings).expect("valid postings");
            builder
                .push(&row.row_key(), &row.filter_key(), &row)
                .expect("append row");
            rows.push(row);
        }
        let built = builder.build().expect("build segment");
        let entries = decode_index_block(
            &built.bytes[loonfs_types::usize_from_u64(built.index.offset)..],
            &built.index,
        )
        .expect("decode index");
        assert_eq!(entries.len(), rows.len());
        assert!(built.filter.offset > MAX_BULK_LOAD_BYTES);
        store
            .put(
                object_key,
                Bytes::from(built.bytes),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write segment");

        let cache = GrepBlockCache::new(DecodedBlockCacheConfig::new(64 << 20));
        let segment_id = IndexSegmentId::generate();
        let blocks = load_data_block_span(&store, Some(&cache), object_key, &segment_id, &entries)
            .await
            .expect("load spans");
        assert_eq!(watched.reads().peak_in_flight, 1);
        assert_eq!(
            blocks
                .iter()
                .flat_map(|block| &block.rows)
                .collect::<Vec<_>>(),
            rows.iter().collect::<Vec<_>>()
        );
        let gets = store.take_gets();
        assert!((2..=3).contains(&gets.len()), "{gets:?}");
        for (_, range) in &gets {
            let (start, end) = range.expect("span reads should be ranged");
            assert!(end - start <= MAX_BULK_LOAD_BYTES);
        }
        for entry in &entries {
            let reads = gets
                .iter()
                .filter(|(_, range)| {
                    range.is_some_and(|(start, end)| {
                        start <= entry.block.offset
                            && end >= entry.block.offset + u64::from(entry.block.stored_bytes)
                    })
                })
                .count();
            assert_eq!(reads, 1);
        }

        for position in [1, 2, 5] {
            cache.invalidate(&cache_key(
                &segment_id,
                SegmentBlockKind::Data,
                &entries[position].block,
            ));
        }
        let reloaded =
            load_data_block_span(&store, Some(&cache), object_key, &segment_id, &entries)
                .await
                .expect("reload spans");
        assert_eq!(
            reloaded
                .iter()
                .flat_map(|block| &block.rows)
                .collect::<Vec<_>>(),
            rows.iter().collect::<Vec<_>>()
        );
        let end = |position: usize| {
            entries[position].block.offset + u64::from(entries[position].block.stored_bytes)
        };
        assert_eq!(
            store
                .take_gets()
                .into_iter()
                .map(|(_, range)| range)
                .collect::<Vec<_>>(),
            [
                Some((entries[1].block.offset, end(2))),
                Some((entries[5].block.offset, end(5)))
            ]
        );
    }

    #[derive(Debug)]
    struct ShortReadStore {
        inner: LocalFsStore,
    }

    #[async_trait::async_trait]
    impl ObjectStore for ShortReadStore {
        delegate_object_store!(self => self.inner; except get);

        async fn get(&self, _key: &str, _range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            Ok(Some(Bytes::from_static(b"truncated")))
        }
    }

    #[tokio::test]
    async fn a_short_ranged_read_is_a_store_failure_not_index_corruption() {
        let temp_dir = tempdir().expect("tempdir");
        let store = ShortReadStore {
            inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        };
        let handle = BlockHandle {
            offset: 0,
            stored_bytes: 64,
            decoded_bytes: 64,
            crc32c: 0,
        };
        let error = load_index_section_bytes(&store, "segments/one", &handle)
            .await
            .expect_err("a short ranged read should not decode");
        assert_matches!(
            error,
            GrepError::StoreUnavailable { .. },
            "expected a store failure, got {error:?}"
        );
    }
}
