//! SST block-range selection and per-view block memoization.

#[cfg(test)]
use super::block_fetch::load_segment_index;
use super::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCache, MetadataSegmentCacheKey};
use super::data_block_load::{
    load_segment_data_block_span, load_segment_data_block_with_readahead,
};
use super::error::ManifestLoadError;
use super::scan::Readahead;
#[cfg(test)]
use super::validate::validate_manifest_row_seq_range;
use bytes::Bytes;
#[cfg(test)]
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
#[cfg(test)]
use loonfs_types::format::manifest::MetadataRow;
use loonfs_types::format::manifest::MetadataSegmentRef;
#[cfg(test)]
use loonfs_types::format::sst_blocks::index_blocks_for_key_range;
use loonfs_types::format::sst_blocks::DecodedDataBlock;
#[cfg(test)]
use loonfs_types::ChangeSeq;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Default for the decoded and stored data-block bytes one view keeps.
/// 64 MiB holds one page's working set plus read-ahead, and a runaway scan
/// cannot hold gigabytes through the memo.
pub(crate) const DEFAULT_BLOCK_MEMO_BYTES: usize = 64 * 1024 * 1024;

/// The data blocks of one segment that can hold keys in
/// `[lower_bound, upper_bound)`, shared straight from the decoded-block
/// memo and caches. Row access borrows from the blocks rather than building
/// an owned row set, which would clone every row and key of every touched
/// block on every scan.
#[cfg(test)]
pub(crate) struct SegmentKeyRangeBlocks {
    blocks: Vec<Arc<DecodedDataBlock>>,
}

#[cfg(test)]
impl SegmentKeyRangeBlocks {
    /// Rows whose keys fall in `[lower_bound, upper_bound)`, in row order,
    /// found by binary search over each block's decode-validated key order.
    /// Express a prefix scan as `[prefix, string_prefix_upper_bound(prefix))`.
    pub(super) fn rows_in_key_range<'a>(
        &'a self,
        lower_bound: &'a str,
        upper_bound: Option<&'a str>,
    ) -> impl Iterator<Item = (&'a str, &'a MetadataRow)> + 'a {
        self.blocks.iter().flat_map(move |block| {
            let start = block
                .row_keys
                .partition_point(|key| key.as_str() < lower_bound);
            let end = upper_bound.map_or(block.row_keys.len(), |upper_bound| {
                block
                    .row_keys
                    .partition_point(|key| key.as_str() < upper_bound)
            });
            let range = start..end.max(start);
            block.row_keys[range.clone()]
                .iter()
                .zip(&block.rows[range])
                .map(|(key, row)| (key.as_str(), row))
        })
    }

    #[cfg(test)]
    pub(super) fn rows(&self) -> impl Iterator<Item = &MetadataRow> {
        self.blocks.iter().flat_map(|block| block.rows.iter())
    }

    #[cfg(test)]
    pub(super) fn row_keys(&self) -> impl Iterator<Item = &String> {
        self.blocks.iter().flat_map(|block| block.row_keys.iter())
    }
}

/// This memo stays separate because it uses FIFO eviction for one operation.
#[derive(Debug)]
pub(super) struct SessionBlockMemo {
    max_data_bytes: usize,
    inner: Mutex<SessionBlockMemoInner>,
}

impl Default for SessionBlockMemo {
    fn default() -> Self {
        Self::new(DEFAULT_BLOCK_MEMO_BYTES)
    }
}

#[derive(Debug, Default)]
struct SessionBlockMemoInner {
    blocks: HashMap<Arc<MetadataSegmentCacheKey>, MemoBlock>,
    data_insertion_order: VecDeque<Arc<MetadataSegmentCacheKey>>,
    data_bytes: usize,
}

#[derive(Debug, Clone)]
enum MemoBlock {
    Decoded(DecodedMetadataSegmentBlock),
    Stored(Bytes),
}

impl MemoBlock {
    fn data_bytes(&self) -> usize {
        match self {
            Self::Decoded(DecodedMetadataSegmentBlock::Data { decoded_bytes, .. }) => {
                *decoded_bytes
            }
            Self::Decoded(_) => 0,
            Self::Stored(bytes) => bytes.len(),
        }
    }
}

impl SessionBlockMemo {
    pub(super) fn new(max_data_bytes: usize) -> Self {
        Self {
            max_data_bytes,
            inner: Mutex::default(),
        }
    }

    pub(super) fn get(
        &self,
        cache_key: &MetadataSegmentCacheKey,
    ) -> Option<DecodedMetadataSegmentBlock> {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .blocks
            .get(cache_key)
            .and_then(|block| match block {
                MemoBlock::Decoded(block) => Some(block.clone()),
                MemoBlock::Stored(_) => None,
            })
    }

    pub(super) fn stored(&self, cache_key: &MetadataSegmentCacheKey) -> Option<Bytes> {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .blocks
            .get(cache_key)
            .and_then(|block| match block {
                MemoBlock::Stored(bytes) => Some(bytes.clone()),
                MemoBlock::Decoded(_) => None,
            })
    }

    pub(super) fn record_stored(&self, cache_key: &MetadataSegmentCacheKey, bytes: &[u8]) {
        self.record_block(cache_key, MemoBlock::Stored(Bytes::copy_from_slice(bytes)));
    }

    pub(super) fn record(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        block: &DecodedMetadataSegmentBlock,
    ) {
        self.record_block(cache_key, MemoBlock::Decoded(block.clone()));
    }

    fn record_block(&self, cache_key: &MetadataSegmentCacheKey, block: MemoBlock) {
        let data_bytes = block.data_bytes();
        let cache_key = Arc::new(cache_key.clone());
        if data_bytes == 0 {
            self.inner
                .lock()
                .expect("session block memo lock should not be poisoned")
                .blocks
                .insert(cache_key, block);
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("session block memo lock should not be poisoned");
        let previous = inner.blocks.insert(Arc::clone(&cache_key), block);
        if let Some(previous) = previous {
            inner.data_bytes = inner.data_bytes.saturating_sub(previous.data_bytes());
        } else {
            inner.data_insertion_order.push_back(cache_key);
        }
        inner.data_bytes = inner.data_bytes.saturating_add(data_bytes);
        while inner.data_bytes > self.max_data_bytes {
            let oldest = inner
                .data_insertion_order
                .pop_front()
                .expect("accounted data blocks should have an insertion-order entry");
            let evicted = inner
                .blocks
                .remove(&oldest)
                .expect("session block memo queue and map should stay one-to-one");
            inner.data_bytes = inner.data_bytes.saturating_sub(evicted.data_bytes());
        }
    }
}

// Adjacent lookups share fetched bytes within one aligned window.
const RANGE_SCAN_READAHEAD_BLOCKS: usize = 32;
/// Loads the rows of one segment whose keys can fall in
/// `[lower_bound, upper_bound)`: index first, then only the data blocks the
/// index says can match. Callers trim edge blocks with
/// [`SegmentKeyRangeBlocks::rows_in_key_range`].
#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the segment scan inputs stay explicit at the shared load boundary"
)]
pub(super) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    max_seq: ChangeSeq,
    lower_bound: &str,
    upper_bound: Option<&str>,
    row_limit: usize,
    readahead: Readahead,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let index = load_segment_index(store, segment_cache, memo, descriptor).await?;
    let needed = index_blocks_for_key_range(&index, lower_bound, upper_bound);

    let mut result = SegmentKeyRangeBlocks { blocks: Vec::new() };
    let mut remaining_rows = row_limit;
    let mut start = needed.start;
    while start < needed.end && remaining_rows > 0 {
        let required_end = if readahead == Readahead::Stored {
            start + 1
        } else if row_limit == usize::MAX {
            needed.end
        } else {
            (start + 1)
                .div_ceil(RANGE_SCAN_READAHEAD_BLOCKS)
                .saturating_mul(RANGE_SCAN_READAHEAD_BLOCKS)
                .min(needed.end)
        };
        let blocks = load_segment_blocks_with_readahead(
            store,
            segment_cache,
            memo,
            descriptor,
            &index,
            start..required_end,
            readahead,
        )
        .await?;
        let batch = SegmentKeyRangeBlocks { blocks };
        if row_limit != usize::MAX {
            remaining_rows = remaining_rows
                .saturating_sub(batch.rows_in_key_range(lower_bound, upper_bound).count());
        }
        result.blocks.extend(batch.blocks);
        start = required_end;
    }

    validate_manifest_row_seq_range(
        &metadata_segment_object_key(descriptor),
        result.blocks.iter().flat_map(|block| block.rows.iter()),
        max_seq,
    )?;
    Ok(result)
}

pub(super) async fn load_segment_blocks_with_readahead<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataSegmentRef,
    index: &[loonfs_types::format::sst_blocks::SegmentIndexEntry],
    range: std::ops::Range<usize>,
    readahead: Readahead,
) -> Result<Vec<Arc<DecodedDataBlock>>, ManifestLoadError> {
    let extended_end = if readahead != Readahead::Disabled {
        range
            .end
            .div_ceil(RANGE_SCAN_READAHEAD_BLOCKS)
            .saturating_mul(RANGE_SCAN_READAHEAD_BLOCKS)
            .min(index.len())
    } else {
        range.end
    };
    if readahead == Readahead::Stored {
        let mut blocks = Vec::new();
        for position in range {
            blocks.push(
                load_segment_data_block_with_readahead(
                    store,
                    segment_cache,
                    Some(memo),
                    descriptor,
                    &index[position..extended_end],
                )
                .await?,
            );
        }
        Ok(blocks)
    } else {
        Ok(load_segment_data_block_span(
            store,
            segment_cache,
            Some(memo),
            descriptor,
            &index[range.start..extended_end],
        )
        .await?
        .into_iter()
        .take(range.len())
        .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::MetadataSegmentBlockKind;
    use super::*;
    use loonfs_types::format::sst_blocks::{decode_filter_block, SegmentBlocksBuilder};

    fn key(kind: MetadataSegmentBlockKind, offset: u64) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: format!("memo-{offset}"),
            block_kind: kind,
            block_offset: offset,
        }
    }

    fn data_block(decoded_bytes: usize) -> DecodedMetadataSegmentBlock {
        DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    fn filter_block() -> DecodedMetadataSegmentBlock {
        let mut builder = SegmentBlocksBuilder::default();
        builder
            .push("key", "key", &0_u8)
            .expect("filter fixture row should encode");
        let built = builder.finish().expect("filter fixture should finish");
        let start = built.filter.offset as usize;
        let end = start + built.filter.stored_bytes as usize;
        let filter = decode_filter_block(&built.bytes[start..end], &built.filter)
            .expect("filter fixture should decode");
        DecodedMetadataSegmentBlock::Filter {
            filter: Arc::new(filter),
            decoded_bytes: 1,
        }
    }

    fn manifest_block() -> DecodedMetadataSegmentBlock {
        let manifest = loonfs_types::format::manifest::decode_namespace_manifest_json(
            include_bytes!("../../../loonfs-types/tests/golden/manifest.v1.json"),
        )
        .expect("valid manifest fixture");
        DecodedMetadataSegmentBlock::Manifest {
            manifest: (Arc::new(manifest), Arc::new(Vec::new()), 0),
            decoded_bytes: 1,
        }
    }

    #[test]
    fn data_budget_evicts_oldest_data_and_preserves_metadata_entries() {
        let memo = SessionBlockMemo::default();
        let index_key = key(MetadataSegmentBlockKind::Index, 1);
        let filter_key = key(MetadataSegmentBlockKind::Filter, 2);
        let manifest_key = key(MetadataSegmentBlockKind::Manifest, 3);
        memo.record(
            &index_key,
            &DecodedMetadataSegmentBlock::Index {
                entries: Arc::new(Vec::new()),
                decoded_bytes: 1,
            },
        );
        memo.record(&filter_key, &filter_block());
        memo.record(&manifest_key, &manifest_block());

        let oldest_data_key = key(MetadataSegmentBlockKind::Data, 4);
        let newer_data_key = key(MetadataSegmentBlockKind::Data, 5);
        let newest_data_key = key(MetadataSegmentBlockKind::Data, 6);
        memo.record(&oldest_data_key, &data_block(DEFAULT_BLOCK_MEMO_BYTES / 2));
        memo.record(&newer_data_key, &data_block(DEFAULT_BLOCK_MEMO_BYTES / 2));
        memo.record(&newest_data_key, &data_block(1));

        assert!(memo.get(&oldest_data_key).is_none());
        assert!(memo.get(&newer_data_key).is_some());
        assert!(memo.get(&newest_data_key).is_some());
        assert!(memo.get(&index_key).is_some());
        assert!(memo.get(&filter_key).is_some());
        assert!(memo.get(&manifest_key).is_some());
    }
}
