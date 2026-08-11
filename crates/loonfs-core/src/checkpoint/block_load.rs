//! SST block-range selection and per-view decoded-block memoization.

use super::block_fetch::load_segment_index;
use super::cache::{DecodedMetadataTableBlock, MetadataTableCache, MetadataTableCacheKey};
use super::data_block_load::load_segment_data_block_span;
use super::error::ManifestLoadError;
use super::scan::Readahead;
use super::validate::validate_manifest_row_seq_range;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow};
use loonfs_api::wire::sst_blocks::{index_blocks_for_key_range, DecodedDataBlock};
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Bounds decoded data retained by one view. 64 MiB comfortably holds one
/// page's working set plus read-ahead, while a runaway scan cannot pin
/// gigabytes through its memo.
const SESSION_BLOCK_MEMO_DATA_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// The data blocks of one segment that can hold keys in
/// `[lower_bound, upper_bound)`, shared straight from the decoded-block
/// memo and caches. Row access borrows from the blocks rather than building
/// an owned row set, which would clone every row and key of every touched
/// block on every scan.
pub(crate) struct SegmentKeyRangeBlocks {
    blocks: Vec<Arc<DecodedDataBlock>>,
}

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

/// Per-view memo of decoded blocks. Index, filter, and manifest entries stay
/// for the view's lifetime, while data entries have a decoded-byte budget and
/// leave in insertion order. This is a dedupe map, not an owner:
/// [`SegmentKeyRangeBlocks`] holds its own block [`Arc`]s, so eviction cannot
/// invalidate borrowed rows. Its worst case is one extra shared-cache lookup.
#[derive(Debug, Default)]
pub(super) struct SessionBlockMemo {
    inner: Mutex<SessionBlockMemoInner>,
}

#[derive(Debug, Default)]
struct SessionBlockMemoInner {
    blocks: HashMap<MetadataTableCacheKey, DecodedMetadataTableBlock>,
    data_insertion_order: VecDeque<MetadataTableCacheKey>,
    data_decoded_bytes: usize,
}

impl SessionBlockMemo {
    pub(super) fn get(
        &self,
        cache_key: &MetadataTableCacheKey,
    ) -> Option<DecodedMetadataTableBlock> {
        self.inner
            .lock()
            .expect("session block memo lock should not be poisoned")
            .blocks
            .get(cache_key)
            .cloned()
    }

    pub(super) fn record(
        &self,
        cache_key: &MetadataTableCacheKey,
        block: &DecodedMetadataTableBlock,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("session block memo lock should not be poisoned");
        let previous = inner.blocks.insert(cache_key.clone(), block.clone());
        if let Some(DecodedMetadataTableBlock::Data {
            decoded_byte_len, ..
        }) = previous
        {
            inner.data_decoded_bytes = inner.data_decoded_bytes.saturating_sub(decoded_byte_len);
        } else if matches!(block, DecodedMetadataTableBlock::Data { .. }) {
            inner.data_insertion_order.push_back(cache_key.clone());
        }
        if let DecodedMetadataTableBlock::Data {
            decoded_byte_len, ..
        } = block
        {
            inner.data_decoded_bytes = inner.data_decoded_bytes.saturating_add(*decoded_byte_len);
        }
        while inner.data_decoded_bytes > SESSION_BLOCK_MEMO_DATA_BUDGET_BYTES {
            let oldest = inner
                .data_insertion_order
                .pop_front()
                .expect("accounted data blocks should have an insertion-order entry");
            let Some(DecodedMetadataTableBlock::Data {
                decoded_byte_len, ..
            }) = inner.blocks.get(&oldest)
            else {
                continue;
            };
            let decoded_byte_len = *decoded_byte_len;
            inner.blocks.remove(&oldest);
            inner.data_decoded_bytes = inner.data_decoded_bytes.saturating_sub(decoded_byte_len);
        }
    }
}

/// Blocks a range scan reads ahead within a segment. Paged scans ask for a
/// couple of blocks at a time while marching through whole segments; without
/// readahead every page pays its own small GETs, and request counts scale
/// with pages instead of bytes. Read-ahead blocks land in the per-view memo
/// and shared cache, so the following pages are memory hits. 32 blocks of
/// the 8 KiB target is a ~256 KiB ranged GET.
const RANGE_SCAN_READAHEAD_BLOCKS: usize = 32;
/// Loads the rows of one segment whose keys can fall in
/// `[lower_bound, upper_bound)`: index first, then only the data blocks the
/// index says can match. Callers trim edge blocks with
/// [`SegmentKeyRangeBlocks::rows_in_key_range`].
pub(super) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    descriptor: &MetadataFileRef,
    lower_bound: &str,
    upper_bound: Option<&str>,
    readahead: Readahead,
) -> Result<SegmentKeyRangeBlocks, ManifestLoadError> {
    let index = load_segment_index(store, table_cache, memo, descriptor).await?;
    let needed = index_blocks_for_key_range(&index, lower_bound, upper_bound);

    // A paged scan marches onward through the segment: read ahead so the
    // following pages are served from the memo instead of their own GETs.
    let extended_end = if readahead == Readahead::Enabled {
        needed
            .start
            .saturating_add(RANGE_SCAN_READAHEAD_BLOCKS)
            .max(needed.end)
            .min(index.len())
    } else {
        needed.end
    };
    let blocks: Vec<_> = load_segment_data_block_span(
        store,
        table_cache,
        memo,
        descriptor,
        &index[needed.start..extended_end],
    )
    .await?
    .into_iter()
    .take(needed.len())
    .collect();

    validate_manifest_row_seq_range(
        &descriptor.object_key,
        blocks.iter().flat_map(|block| block.rows.iter()),
        descriptor.run_seq,
    )?;
    Ok(SegmentKeyRangeBlocks { blocks })
}

#[cfg(test)]
mod tests {
    use super::super::cache::MetadataTableBlockKind;
    use super::*;
    use loonfs_api::wire::manifest::{
        NamespaceManifestEnvelope, NamespaceManifestKind, NamespaceManifestPayload,
        NAMESPACE_MANIFEST_FORMAT_VERSION,
    };
    use loonfs_api::wire::sst_blocks::{decode_filter_block, SegmentBlocksBuilder};
    use loonfs_api::{
        ChangeSeq, CommitId, InodeId, ManifestId, ManifestObjectId, NamespaceId, WriterEpoch,
    };

    fn key(kind: MetadataTableBlockKind, offset: u64) -> MetadataTableCacheKey {
        MetadataTableCacheKey {
            identity: format!("memo-{offset}"),
            block_kind: kind,
            block_offset: offset,
        }
    }

    fn data_block(decoded_byte_len: usize) -> DecodedMetadataTableBlock {
        DecodedMetadataTableBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_byte_len,
        }
    }

    fn filter_block() -> DecodedMetadataTableBlock {
        let mut builder = SegmentBlocksBuilder::default();
        builder
            .push("key", "key", &0_u8)
            .expect("filter fixture row should encode");
        let built = builder.finish().expect("filter fixture should finish");
        let start = built.filter.offset as usize;
        let end = start + built.filter.stored_len as usize;
        let filter = decode_filter_block(&built.bytes[start..end], &built.filter)
            .expect("filter fixture should decode");
        DecodedMetadataTableBlock::Filter {
            filter: Arc::new(filter),
            decoded_byte_len: 1,
        }
    }

    fn manifest_block() -> DecodedMetadataTableBlock {
        let manifest = NamespaceManifestEnvelope {
            kind: NamespaceManifestKind::NamespaceManifest,
            format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
            payload_checksum: String::new(),
            payload: NamespaceManifestPayload {
                namespace_id: NamespaceId::parse("memo").expect("namespace id"),
                manifest_id: ManifestId(0),
                manifest_object_id: ManifestObjectId::parse(
                    "00000000000000000000-0000000000000000",
                )
                .expect("manifest object id"),
                head_seq: ChangeSeq(0),
                head_commit_id: CommitId::parse("c_00000000000000000000000000000000")
                    .expect("commit id"),
                base_seq: ChangeSeq(0),
                writer_epoch: WriterEpoch(0),
                next_inode_id: InodeId(1),
                retention_floor_seq: ChangeSeq(0),
                metadata_files: Vec::new(),
            },
        };
        DecodedMetadataTableBlock::Manifest {
            manifest: Arc::new(manifest),
            scan_runs: Arc::new(Vec::new()),
            decoded_byte_len: 1,
        }
    }

    #[test]
    fn data_budget_evicts_oldest_data_and_preserves_metadata_entries() {
        let memo = SessionBlockMemo::default();
        let index_key = key(MetadataTableBlockKind::Index, 1);
        let filter_key = key(MetadataTableBlockKind::Filter, 2);
        let manifest_key = key(MetadataTableBlockKind::Manifest, 3);
        memo.record(
            &index_key,
            &DecodedMetadataTableBlock::Index {
                entries: Arc::new(Vec::new()),
                decoded_byte_len: 1,
            },
        );
        memo.record(&filter_key, &filter_block());
        memo.record(&manifest_key, &manifest_block());

        let oldest_data_key = key(MetadataTableBlockKind::Data, 4);
        let newer_data_key = key(MetadataTableBlockKind::Data, 5);
        let newest_data_key = key(MetadataTableBlockKind::Data, 6);
        memo.record(
            &oldest_data_key,
            &data_block(SESSION_BLOCK_MEMO_DATA_BUDGET_BYTES / 2),
        );
        memo.record(
            &newer_data_key,
            &data_block(SESSION_BLOCK_MEMO_DATA_BUDGET_BYTES / 2),
        );
        memo.record(&newest_data_key, &data_block(1));

        assert!(memo.get(&oldest_data_key).is_none());
        assert!(memo.get(&newer_data_key).is_some());
        assert!(memo.get(&newest_data_key).is_some());
        assert!(memo.get(&index_key).is_some());
        assert!(memo.get(&filter_key).is_some());
        assert!(memo.get(&manifest_key).is_some());
    }
}
