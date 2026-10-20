//! Cached block loading for metadata scans, and a scan of each segment's own
//! rows without a merge.

use super::block_load::{load_segment_blocks_with_readahead, SessionBlockMemo};
use super::cache::{read_working_memory, MetadataSegmentCache};
use super::compaction_merge::{refill_iterators, SegmentBlockLoader, SegmentRowIterator};
use super::data_block_load::MAX_BULK_LOAD_BYTES;
use super::error::ManifestLoadError;
use super::index_block_load::load_segment_index;
use super::scan::Readahead;
use super::validate::validate_manifest_row_seq_range;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{
    index_blocks_for_key_range, key_range_may_intersect, DecodedDataBlock, SegmentIndexEntry,
};
use loonfs_types::ChangeSeq;
use std::ops::ControlFlow;
use std::sync::Arc;

/// One segment of a scan, with the sequence of the run that lists it, which
/// bounds the sequence of every row in the segment.
#[derive(Clone, Copy)]
pub(crate) struct ScanDescriptor<'a> {
    pub(crate) descriptor: &'a MetadataSegmentRef,
    pub(crate) max_seq: ChangeSeq,
}

pub(super) struct ScanLoader<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) segment_cache: Option<&'a MetadataSegmentCache>,
    pub(super) block_memo: &'a SessionBlockMemo,
    pub(super) readahead: Readahead,
    pub(super) upper_bound: Option<&'a str>,
}

/// Visits each row of `segments` whose key is in `[lower_bound, upper_bound)`,
/// one segment at a time, without merging the rows of different segments.
///
/// The rows of one segment arrive in key order, and a key that two segments
/// list arrives once from each. Up to `STORE_READS_IN_FLIGHT` segments are
/// open and loading through `segment_cache` at once. The blocks of each refill
/// are charged to the cache's read working memory while they load, and the
/// charge is released before `visit` sees their rows.
pub(crate) async fn visit_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    segments: &[ScanDescriptor<'_>],
    lower_bound: &str,
    upper_bound: Option<&str>,
    mut visit: impl FnMut(MetadataRow) -> crate::error::Result<ControlFlow<()>>,
) -> crate::error::Result<ControlFlow<()>> {
    let mut pending = segments.iter().copied().filter(|segment| {
        key_range_may_intersect(
            &segment.descriptor.min_row_key,
            &segment.descriptor.max_row_key,
            segment.descriptor.row_count,
            lower_bound,
            upper_bound,
        )
    });
    let mut iterators = Vec::new();
    loop {
        iterators.extend(
            pending
                .by_ref()
                .take(STORE_READS_IN_FLIGHT - iterators.len())
                .map(|segment| {
                    SegmentRowIterator::new((), vec![segment], Some(lower_bound.to_owned()))
                }),
        );
        if iterators.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }
        {
            // The memo is dropped before `visit` runs, so the scan's blocks
            // don't take the read working memory that the caller's sets need.
            let block_memo = SessionBlockMemo::new(read_working_memory(segment_cache));
            let loader = ScanLoader {
                store,
                segment_cache,
                block_memo: &block_memo,
                readahead: Readahead::Enabled,
                upper_bound,
            };
            refill_iterators(&loader, &mut iterators, 1).await?;
        }
        for iterator in &mut iterators {
            while let Some((key, _)) = iterator.head() {
                if upper_bound.is_some_and(|upper| key >= upper) {
                    break;
                }
                if visit(iterator.take_head())?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
        }
        iterators.retain(|iterator| {
            let Some(last_key) = iterator.loaded_through_key() else {
                return false;
            };
            iterator.head().is_none()
                && iterator
                    .current_segment()
                    .is_some_and(|segment| last_key < segment.descriptor.max_row_key.as_str())
                && upper_bound.is_none_or(|upper| last_key < upper)
        });
    }
}

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<MetadataRow, ScanDescriptor<'_>>
    for ScanLoader<'_, S>
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
            self.store,
            self.segment_cache,
            self.block_memo,
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
            self.store,
            self.segment_cache,
            self.block_memo,
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
