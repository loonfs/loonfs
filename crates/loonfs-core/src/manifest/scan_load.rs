//! Cached block loading for ordered metadata scans.

use super::block_fetch::load_segment_index;
use super::block_load::load_segment_blocks_with_readahead;
use super::compaction_merge::SegmentBlockLoader;
use super::data_block_load::MAX_BULK_LOAD_BYTES;
use super::error::ManifestLoadError;
use super::scan::{Readahead, VerifiedMetadataSegments};
use super::validate::validate_manifest_row_seq_range;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::sst_blocks::{
    index_blocks_for_key_range, DecodedDataBlock, SegmentIndexEntry,
};
use loonfs_types::ChangeSeq;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) struct ScanDescriptor<'a> {
    pub(super) descriptor: &'a MetadataSegmentRef,
    pub(super) max_seq: ChangeSeq,
}

pub(super) struct ScanLoader<'a, 's, S: ObjectStore + ?Sized> {
    pub(super) segments: &'a VerifiedMetadataSegments<'s, S>,
    pub(super) readahead: Readahead,
    pub(super) upper_bound: Option<&'a str>,
}

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<MetadataRow, ScanDescriptor<'_>>
    for ScanLoader<'_, '_, S>
{
    type Error = ManifestLoadError;

    fn data_block_count(&self, entries: &[SegmentIndexEntry], _decoded_byte_limit: usize) -> usize {
        if self.readahead != Readahead::Disabled {
            return entries.len().min(1);
        }
        let needed = index_blocks_for_key_range(entries, "", self.upper_bound);
        let mut stored_bytes = 0;
        entries[needed]
            .iter()
            .take_while(|entry| {
                let next_bytes = stored_bytes + u64::from(entry.block.stored_bytes);
                if stored_bytes > 0 && next_bytes > MAX_BULK_LOAD_BYTES {
                    return false;
                }
                stored_bytes = next_bytes;
                true
            })
            .count()
    }

    async fn load_index(
        &self,
        segment: ScanDescriptor<'_>,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>, Self::Error> {
        load_segment_index(
            self.segments.store,
            self.segments.segment_cache,
            &self.segments.block_memo,
            segment.descriptor,
        )
        .await
    }

    async fn load_data_blocks(
        &self,
        segment: ScanDescriptor<'_>,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<Vec<Arc<DecodedDataBlock>>, Self::Error> {
        let index = self.load_index(segment).await?;
        let first = entries.first().expect("a refill should request a block");
        let start = index.partition_point(|entry| entry.block.offset < first.block.offset);
        let blocks = load_segment_blocks_with_readahead(
            self.segments.store,
            self.segments.segment_cache,
            &self.segments.block_memo,
            segment.descriptor,
            &index,
            start..start + entries.len(),
            self.readahead,
        )
        .await?;
        validate_manifest_row_seq_range(
            &metadata_segment_object_key(segment.descriptor),
            blocks.iter().flat_map(|block| block.rows.iter()),
            segment.max_seq,
        )?;
        Ok(blocks)
    }
}
