//! Cached reads of gram-index segment filter, index, and data blocks.

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind};
use crate::codec::IndexRow;
use crate::root::GrepSegmentRef;
use crate::{GrepError, Result};
use loonfs::StoreFailureClass;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

const WHOLE_SEGMENT_LOAD_MAX_BYTES: u64 = 128 * 1024;

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
    object_checksum: &str,
    block_kind: GrepBlockKind,
    handle: &BlockHandle,
) -> GrepBlockCacheKey {
    GrepBlockCacheKey {
        identity: object_checksum.to_owned(),
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
        .map_err(|error| GrepError::StoreUnavailable {
            object_key: object_key.to_owned(),
            message: error.public_message().into_owned(),
            class: StoreFailureClass::of(&error),
        })?
        .ok_or_else(|| GrepError::CorruptIndex {
            message: format!("manifest references missing index segment `{object_key}`"),
        })?;
    if bytes.len() != handle.stored_len as usize {
        return Err(GrepError::StoreUnavailable {
            object_key: object_key.to_owned(),
            message: format!(
                "ranged read returned {} bytes, expected {}",
                bytes.len(),
                handle.stored_len
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

fn segment_object_len(object_key: &str, descriptor: &GrepSegmentRef) -> Result<u64> {
    descriptor
        .index_block
        .offset
        .checked_add(u64::from(descriptor.index_block.stored_len))
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
        stored_len: object_len as u32,
        decoded_len: 0,
        crc32c: 0,
    };
    let bytes = load_index_section_bytes(store, object_key, &whole_handle).await?;
    let section = |handle: &BlockHandle| -> Option<&[u8]> {
        let start = usize::try_from(handle.offset).ok()?;
        let end = start.checked_add(handle.stored_len as usize)?;
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
            cache_key(
                &descriptor.object_checksum,
                GrepBlockKind::Data,
                &entry.block,
            ),
            DecodedGrepBlock::Data {
                block,
                decoded_bytes: entry.block.decoded_len as usize,
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
    let key = cache_key(&descriptor.object_checksum, GrepBlockKind::Filter, handle);
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
                        &descriptor.object_checksum,
                        GrepBlockKind::Index,
                        &descriptor.index_block,
                    ),
                    DecodedGrepBlock::Index {
                        entries: whole.entries,
                        decoded_bytes: descriptor.index_block.decoded_len as usize,
                    },
                );
                return Ok(DecodedGrepBlock::Filter {
                    filter: whole.filter,
                    decoded_bytes: handle.decoded_len as usize,
                });
            }
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let filter = Arc::new(
                decode_filter_block(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Filter {
                filter,
                decoded_bytes: handle.decoded_len as usize,
            })
        })
        .await?;
    match decoded {
        DecodedGrepBlock::Filter { filter, .. } => Ok(filter),
        _ => Err(cache_kind_corrupt(object_key, "filter")),
    }
}

pub(crate) async fn load_index_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    descriptor: &GrepSegmentRef,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    let handle = &descriptor.index_block;
    let key = cache_key(&descriptor.object_checksum, GrepBlockKind::Index, handle);
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
                        &descriptor.object_checksum,
                        GrepBlockKind::Filter,
                        &descriptor.filter_block,
                    ),
                    DecodedGrepBlock::Filter {
                        filter: whole.filter,
                        decoded_bytes: descriptor.filter_block.decoded_len as usize,
                    },
                );
                return Ok(DecodedGrepBlock::Index {
                    entries: whole.entries,
                    decoded_bytes: handle.decoded_len as usize,
                });
            }
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let entries = Arc::new(
                decode_index_block(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Index {
                entries,
                decoded_bytes: handle.decoded_len as usize,
            })
        })
        .await?;
    match decoded {
        DecodedGrepBlock::Index { entries, .. } => Ok(entries),
        _ => Err(cache_kind_corrupt(object_key, "index")),
    }
}

pub(crate) async fn load_data_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    object_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<DecodedDataBlock<IndexRow>>> {
    let key = cache_key(object_checksum, GrepBlockKind::Data, handle);
    let decoded = cache
        .get_or_load(&key, || async {
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let block = Arc::new(
                decode_data_block_rows::<IndexRow>(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "data block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Data {
                block,
                decoded_bytes: handle.decoded_len as usize,
            })
        })
        .await?;
    match decoded {
        DecodedGrepBlock::Data { block, .. } => Ok(block),
        _ => Err(cache_kind_corrupt(object_key, "data")),
    }
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
    #![allow(clippy::unreachable)]
    // Only `get` is used by this test store.

    use super::{load_index_section_bytes, BlockHandle, GrepError};
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode, Result as StoreResult,
    };

    #[derive(Debug)]
    struct ShortReadStore;

    #[async_trait::async_trait]
    impl ObjectStore for ShortReadStore {
        async fn head(&self, _key: &str) -> StoreResult<Option<ObjectMetadata>> {
            unreachable!()
        }

        async fn get_with_metadata(&self, _key: &str) -> StoreResult<Option<ObjectBody>> {
            unreachable!()
        }

        async fn get(&self, _key: &str, _range: Option<ByteRange>) -> StoreResult<Option<Bytes>> {
            Ok(Some(Bytes::from_static(b"truncated")))
        }

        async fn put(
            &self,
            _key: &str,
            _bytes: Bytes,
            _mode: PutMode,
        ) -> StoreResult<ObjectMetadata> {
            unreachable!()
        }

        async fn delete(&self, _key: &str) -> StoreResult<()> {
            unreachable!()
        }

        fn list_prefix_from_stream(
            &self,
            _prefix: &str,
            _start_after: Option<&str>,
        ) -> BoxStream<'static, StoreResult<String>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn a_short_ranged_read_is_a_store_failure_not_index_corruption() {
        let handle = BlockHandle {
            offset: 0,
            stored_len: 64,
            decoded_len: 64,
            crc32c: 0,
        };
        let error = load_index_section_bytes(&ShortReadStore, "segments/one", &handle)
            .await
            .expect_err("a short ranged read should not decode");
        assert!(
            matches!(error, GrepError::StoreUnavailable { .. }),
            "expected a store failure, got {error:?}"
        );
    }
}
