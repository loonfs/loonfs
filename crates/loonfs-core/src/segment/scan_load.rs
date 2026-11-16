//! Cached block loading for metadata scans.

use super::block_memo::SessionBlockMemo;
use super::cache::MetadataSegmentCache;
use super::data_block_load::{load_segment_data_block_refill, load_segment_data_block_span};
use super::index_block_load::load_segment_index;
use super::scan::BlocksPerRefill;
use crate::compaction::run_iterator::SegmentBlockLoader;
use crate::limits::MAX_SCAN_REFILL_BYTES;
use crate::manifest::error::ManifestLoadError;
use crate::manifest::validate::validate_manifest_row_seq_range;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{
    index_blocks_for_key_range, DecodedDataBlock, SegmentIndexEntry,
};
use loonfs_types::CommitSeq;
use std::sync::Arc;

/// One segment of a scan, with the sequence of the run that lists it, which
/// bounds the sequence of every row in the segment.
#[derive(Clone, Copy)]
pub(crate) struct ScanDescriptor<'a> {
    pub(crate) descriptor: &'a MetadataSegmentRef,
    pub(crate) max_seq: CommitSeq,
}

pub(super) struct ScanLoader<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) segment_cache: Option<&'a MetadataSegmentCache>,
    pub(super) block_memo: &'a SessionBlockMemo,
    pub(super) blocks_per_refill: BlocksPerRefill,
    pub(super) upper_bound: Option<&'a str>,
}

impl<'d, S: ObjectStore + ?Sized> SegmentBlockLoader<MetadataRow, ScanDescriptor<'d>>
    for ScanLoader<'_, S>
{
    type Error = ManifestLoadError;

    /// Asks for every block that the refill may take: the blocks of the key
    /// range, or for a read-ahead scan with a block cache, every block left
    /// in the segment. [`Self::load_data_blocks`] stops at the caps.
    fn data_block_count(&self, entries: &[SegmentIndexEntry], _max_decoded_bytes: usize) -> usize {
        match self.blocks_per_refill {
            BlocksPerRefill::One => entries.len().min(1),
            // Without a block cache, the blocks past the range would be
            // fetched, decoded, and dropped.
            BlocksPerRefill::ReadAhead if self.segment_cache.is_some() => entries.len(),
            BlocksPerRefill::KeyRange | BlocksPerRefill::ReadAhead => {
                index_blocks_for_key_range(entries, "", self.upper_bound).end
            }
        }
    }

    async fn load_index(
        &self,
        segment: ScanDescriptor<'d>,
    ) -> Result<(Arc<Vec<SegmentIndexEntry>>, ScanDescriptor<'d>), Self::Error> {
        let index = load_segment_index(
            self.store,
            self.segment_cache,
            self.block_memo,
            segment.descriptor,
        )
        .await?;
        Ok((index, segment))
    }

    /// Loads one block through the memo for a lookup. A range refill takes
    /// the longest prefix in which the blocks that the block cache doesn't
    /// have fit in `MAX_SCAN_REFILL_BYTES` stored and the segment's share of
    /// the scan's decoded budget, and reads ahead only when it fetches a
    /// block of the key range. The memo doesn't record a range refill's
    /// blocks, because the scan keeps them until it has consumed them.
    async fn load_data_blocks(
        &self,
        segment: ScanDescriptor<'d>,
        entries: Vec<SegmentIndexEntry>,
        max_decoded_bytes: usize,
    ) -> Result<Vec<Arc<DecodedDataBlock<MetadataRow>>>, Self::Error> {
        let blocks = match self.blocks_per_refill {
            BlocksPerRefill::One => {
                load_segment_data_block_span(
                    self.store,
                    self.segment_cache,
                    Some(self.block_memo),
                    segment.descriptor,
                    &entries,
                )
                .await?
            }
            BlocksPerRefill::KeyRange | BlocksPerRefill::ReadAhead => {
                let range_blocks = index_blocks_for_key_range(&entries, "", self.upper_bound).end;
                load_segment_data_block_refill(
                    self.store,
                    self.segment_cache,
                    segment.descriptor,
                    &entries,
                    range_blocks,
                    MAX_SCAN_REFILL_BYTES,
                    max_decoded_bytes,
                )
                .await?
            }
        };
        validate_manifest_row_seq_range(
            &metadata_segment_object_key(segment.descriptor),
            blocks.iter().flat_map(|block| block.rows.iter()),
            segment.max_seq,
        )?;
        Ok(blocks)
    }
}
