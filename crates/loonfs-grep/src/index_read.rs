//! Cached reads of gram-index segment filter, index, and data blocks.

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind};
use crate::codec::IndexRow;
use crate::manifest::GrepSegmentRef;
use crate::{GrepError, Result};
use loonfs::StoreFailureClass;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_api::IndexSegmentId;
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

/// Largest index segment object fetched whole on first touch, in stored bytes.
/// Below this, one GET costs less than separate ranged reads of the filter,
/// index, and data blocks; 128 KiB covers a segment of one or two 64 KiB data
/// blocks.
const WHOLE_SEGMENT_LOAD_MAX_BYTES: u64 = 128 * 1024;
// Matches the private metadata span limit in manifest/data_block_load.rs.
const MAX_BULK_LOAD_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) fn index_segment_corrupt(
    object_key: &str,
    what: &str,
    error: &dyn std::fmt::Display,
) -> GrepError {
    GrepError::CorruptIndex {
        message: format!("index segment `{object_key}` carries an unreadable {what}: {error}"),
    }
}

fn cache_key(
    segment_id: &IndexSegmentId,
    block_kind: GrepBlockKind,
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
        .ok_or_else(|| GrepError::CorruptIndex {
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
        .ok_or_else(|| GrepError::CorruptIndex {
            message: format!("manifest references missing index segment `{object_key}`"),
        })?;
    if bytes.len() != handle.stored_bytes as usize {
        return Err(GrepError::StoreUnavailable {
            object_key: object_key.to_owned(),
            message: format!(
                "ranged read returned {} bytes, expected {}",
                bytes.len(),
                handle.stored_bytes
            ),
            class: StoreFailureClass::Other,
        });
    }
    Ok(bytes.to_vec())
}

struct WholeSegment {
    filter: Arc<SegmentFilter>,
    entries: Arc<Vec<SegmentIndexEntry>>,
}

pub(crate) fn segment_object_len(object_key: &str, descriptor: &GrepSegmentRef) -> Result<u64> {
    descriptor
        .index_block
        .offset
        .checked_add(u64::from(descriptor.index_block.stored_bytes))
        .ok_or_else(|| GrepError::CorruptIndex {
            message: format!(
                "index segment `{object_key}` descriptor names bytes past the address space"
            ),
        })
}

async fn load_and_publish_segment_sections<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    descriptor: &GrepSegmentRef,
    object_len: u64,
) -> Result<WholeSegment> {
    let whole_handle = BlockHandle {
        offset: 0,
        stored_bytes: object_len as u32,
        decoded_bytes: 0,
        crc32c: 0,
    };
    let bytes = load_index_section_bytes(store, object_key, &whole_handle).await?;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset).ok()?;
        let end = start.checked_add(handle.stored_bytes as usize)?;
        bytes.get(start..end)
    };
    let index_bytes = section(&descriptor.index_block).ok_or_else(|| GrepError::CorruptIndex {
        message: format!("index segment `{object_key}` index block exceeds the object bounds"),
    })?;
    let entries = Arc::new(
        decode_index_block(index_bytes, &descriptor.index_block)
            .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
    );
    let filter_bytes =
        section(&descriptor.filter_block).ok_or_else(|| GrepError::CorruptIndex {
            message: format!("index segment `{object_key}` filter block exceeds the object bounds"),
        })?;
    let filter = Arc::new(
        decode_filter_block(filter_bytes, &descriptor.filter_block)
            .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
    );
    for entry in entries.iter() {
        let stored = section(&entry.block).ok_or_else(|| GrepError::CorruptIndex {
            message: format!("index segment `{object_key}` data block exceeds the object bounds"),
        })?;
        let block = Arc::new(
            decode_data_block_rows::<IndexRow>(stored, &entry.block)
                .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?,
        );
        cache.insert(
            cache_key(&descriptor.segment_id, GrepBlockKind::Data, &entry.block),
            DecodedGrepBlock::Data {
                block,
                decoded_bytes: entry.block.decoded_bytes as usize,
            },
        );
    }
    Ok(WholeSegment { filter, entries })
}

pub(crate) async fn load_filter_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    descriptor: &GrepSegmentRef,
) -> Result<Arc<SegmentFilter>> {
    let handle = &descriptor.filter_block;
    let key = cache_key(&descriptor.segment_id, GrepBlockKind::Filter, handle);
    let decoded = cache
        .get_or_load(&key, || async {
            let object_len = segment_object_len(object_key, descriptor)?;
            if object_len <= WHOLE_SEGMENT_LOAD_MAX_BYTES {
                let whole = load_and_publish_segment_sections(
                    store, cache, object_key, descriptor, object_len,
                )
                .await?;
                cache.insert(
                    cache_key(
                        &descriptor.segment_id,
                        GrepBlockKind::Index,
                        &descriptor.index_block,
                    ),
                    DecodedGrepBlock::Index {
                        entries: whole.entries,
                        decoded_bytes: descriptor.index_block.decoded_bytes as usize,
                    },
                );
                return Ok(DecodedGrepBlock::Filter {
                    filter: whole.filter,
                    decoded_bytes: handle.decoded_bytes as usize,
                });
            }
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let filter = Arc::new(
                decode_filter_block(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Filter {
                filter,
                decoded_bytes: handle.decoded_bytes as usize,
            })
        })
        .await?;
    decoded
        .filter()
        .ok_or_else(|| cache_kind_corrupt(object_key, "filter"))
}

pub(crate) async fn load_index_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&GrepBlockCache>,
    object_key: &str,
    descriptor: &GrepSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    let handle = &descriptor.index_block;
    let load = || async {
        if let Some(cache) = cache {
            let object_len = segment_object_len(object_key, descriptor)?;
            if object_len <= WHOLE_SEGMENT_LOAD_MAX_BYTES {
                let whole = load_and_publish_segment_sections(
                    store, cache, object_key, descriptor, object_len,
                )
                .await?;
                cache.insert(
                    cache_key(
                        &descriptor.segment_id,
                        GrepBlockKind::Filter,
                        &descriptor.filter_block,
                    ),
                    DecodedGrepBlock::Filter {
                        filter: whole.filter,
                        decoded_bytes: descriptor.filter_block.decoded_bytes as usize,
                    },
                );
                return Ok(DecodedGrepBlock::Index {
                    entries: whole.entries,
                    decoded_bytes: handle.decoded_bytes as usize,
                });
            }
        }
        let bytes = load_index_section_bytes(store, object_key, handle).await?;
        let entries = Arc::new(
            decode_index_block(&bytes, handle)
                .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
        );
        Ok::<_, GrepError>(DecodedGrepBlock::Index {
            entries,
            decoded_bytes: handle.decoded_bytes as usize,
        })
    };
    let decoded = match cache {
        Some(cache) => {
            let key = cache_key(&descriptor.segment_id, GrepBlockKind::Index, handle);
            cache.get_or_load(&key, load).await?
        }
        None => load().await?,
    };
    decoded
        .index()
        .ok_or_else(|| cache_kind_corrupt(object_key, "index"))
}

pub(crate) async fn load_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    segment_id: &IndexSegmentId,
    handle: &BlockHandle,
) -> Result<Arc<DecodedDataBlock<IndexRow>>> {
    let key = cache_key(segment_id, GrepBlockKind::Data, handle);
    let decoded = cache
        .get_or_load(&key, || async {
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            decode_data_cache_block(object_key, &bytes, handle)
        })
        .await?;
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

pub(crate) async fn load_data_block_span<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    entries: &[SegmentIndexEntry],
) -> Result<Vec<Arc<DecodedDataBlock<IndexRow>>>> {
    let mut blocks = Vec::with_capacity(entries.len());
    let mut cursor = 0;
    while cursor < entries.len() {
        let start = cursor;
        let mut handle = entries[start].block;
        let mut span_bytes = u64::from(handle.stored_bytes);
        cursor += 1;
        while cursor < entries.len() {
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
        for entry in &entries[start..cursor] {
            let offset = (entry.block.offset - handle.offset) as usize;
            let stored = &bytes[offset..offset + entry.block.stored_bytes as usize];
            let block = decode_data_block_rows::<IndexRow>(stored, &entry.block)
                .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?;
            blocks.push(Arc::new(block));
        }
    }
    Ok(blocks)
}

fn cache_kind_corrupt(object_key: &str, expected: &str) -> GrepError {
    GrepError::CorruptIndex {
        message: format!(
            "index segment `{object_key}` resolved its {expected} block to a different cache kind"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        load_data_block_span, load_index_section_bytes, BlockHandle, GrepError, IndexRow,
        MAX_BULK_LOAD_BYTES,
    };
    use crate::codec::{Gram, GramPosting};
    use bytes::Bytes;
    use loonfs_api::wire::sst_blocks::{decode_index_block, SegmentBlocksBuilder};
    use loonfs_api::{InodeId, RevisionNo};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ByteRange, ObjectStore, PutMode, Result as StoreResult};
    use loonfs_test_support::stores::{
        delegate_object_store, ConcurrencyWatchStore, KeyPredicate, RecordingStore,
    };
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn data_spans_obey_the_byte_limit() {
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
            let mut inode_id = InodeId(0);
            let postings = (0..65_536)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    inode_id = InodeId(inode_id.0 + (state & 0xffff) + 1);
                    GramPosting {
                        inode_id,
                        revision_no: RevisionNo((state >> 32).max(1)),
                    }
                })
                .collect::<Vec<_>>();
            let row =
                IndexRow::gram_postings(Gram([0, 0, gram]), &postings).expect("valid postings");
            builder
                .push(&row.row_key(), &row.filter_key(), &row)
                .expect("append row");
            rows.push(row);
        }
        let built = builder.finish().expect("build segment");
        let entries = decode_index_block(&built.bytes[built.index.offset as usize..], &built.index)
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

        let blocks = load_data_block_span(&store, object_key, &entries)
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
        assert!(
            matches!(error, GrepError::StoreUnavailable { .. }),
            "expected a store failure, got {error:?}"
        );
    }
}
