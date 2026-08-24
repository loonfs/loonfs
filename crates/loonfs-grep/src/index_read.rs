//! Cached reads of gram-index segment filter, index, and data blocks.

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, GrepBlockKind};
use crate::codec::IndexRow;
use crate::{GrepError, Result};
use loonfs::StoreFailureClass;
use loonfs_api::wire::sst_blocks::{
    decode_data_block_rows, decode_filter_block, decode_index_block, BlockHandle, DecodedDataBlock,
    SegmentFilter, SegmentIndexEntry,
};
use loonfs_objectstore::{ByteRange, ObjectStore};
use std::sync::Arc;

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

pub(crate) async fn load_filter_block<S: ObjectStore + ?Sized>(
    store: &S,
    cache: &GrepBlockCache,
    object_key: &str,
    object_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<SegmentFilter>> {
    let key = cache_key(object_checksum, GrepBlockKind::Filter, handle);
    let decoded = cache
        .get_or_load(&key, || async {
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let filter = Arc::new(
                decode_filter_block(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "filter block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Filter {
                filter,
                decoded_byte_len: handle.decoded_len as usize,
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
    object_checksum: &str,
    handle: &BlockHandle,
) -> Result<Arc<Vec<SegmentIndexEntry>>> {
    let key = cache_key(object_checksum, GrepBlockKind::Index, handle);
    let decoded = cache
        .get_or_load(&key, || async {
            let bytes = load_index_section_bytes(store, object_key, handle).await?;
            let entries = Arc::new(
                decode_index_block(&bytes, handle)
                    .map_err(|error| index_segment_corrupt(object_key, "index block", &error))?,
            );
            Ok::<_, GrepError>(DecodedGrepBlock::Index {
                entries,
                decoded_byte_len: handle.decoded_len as usize,
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
                decoded_byte_len: handle.decoded_len as usize,
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
    // The stub store below serves one ranged read and reaches nothing else.

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
