//! SST block-range selection and per-view decoded-block memoization.

use super::block_fetch::load_segment_index;
use super::cache::{DecodedMetadataTableBlock, MetadataTableCache, MetadataTableCacheKey};
use super::data_block_load::load_segment_data_block_span;
use super::error::ManifestLoadError;
use super::scan::Readahead;
use super::validate::validate_manifest_row_seq_range;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::{index_blocks_for_key_range, DecodedDataBlock};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

/// Per-view memo of decoded blocks, so one operation never re-fetches or
/// re-decodes a block it already saw — the reuse that keeps cache-disabled
/// paths (cold boot, diagnostics) linear in the blocks they touch. Entries
/// share the decoded allocations with the shared cache and concurrent
/// readers; the memo retains pointers, not copies, for the view's lifetime.
#[derive(Debug, Default)]
pub(super) struct SessionBlockMemo {
    blocks: Mutex<HashMap<MetadataTableCacheKey, DecodedMetadataTableBlock>>,
}

impl SessionBlockMemo {
    pub(super) fn get(
        &self,
        cache_key: &MetadataTableCacheKey,
    ) -> Option<DecodedMetadataTableBlock> {
        self.blocks
            .lock()
            .expect("session block memo lock should not be poisoned")
            .get(cache_key)
            .cloned()
    }

    pub(super) fn record(
        &self,
        cache_key: &MetadataTableCacheKey,
        block: &DecodedMetadataTableBlock,
    ) {
        self.blocks
            .lock()
            .expect("session block memo lock should not be poisoned")
            .insert(cache_key.clone(), block.clone());
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
#[allow(clippy::too_many_arguments)]
pub(super) async fn load_manifest_segment_rows_in_key_range_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    memo: &SessionBlockMemo,
    family: MetadataTableFamily,
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
        family,
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
