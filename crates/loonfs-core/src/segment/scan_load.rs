//! Cached block loading for metadata scans, and a scan of each segment's own
//! rows without a merge.

use super::block_memo::SessionBlockMemo;
use super::cache::{read_working_memory, MetadataSegmentCache};
use super::data_block_load::load_segment_data_block_span;
use super::index_block_load::load_segment_index;
use super::scan::BlocksPerRefill;
use crate::compaction::merge_iterator::{
    refill_iterators, LoadedBlocks, SegmentBlockLoader, SegmentRowIterator,
};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::MAX_SCAN_REFILL_BYTES;
use crate::manifest::error::ManifestLoadError;
use crate::manifest::validate::validate_manifest_row_seq_range;
use crate::read_working_memory::{ReadReservation, ReadWorkingMemory};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{
    index_blocks_for_key_range, key_range_may_intersect, SegmentIndexEntry,
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
    /// The read working memory that a key-range refill reserves its blocks'
    /// decoded bytes from.
    pub(super) pool: &'a Arc<ReadWorkingMemory>,
    pub(super) blocks_per_refill: BlocksPerRefill,
    pub(super) upper_bound: Option<&'a str>,
}

/// Visits each row of `segments` whose key is in `[lower_bound, upper_bound)`,
/// one segment at a time, without merging the rows of different segments.
///
/// The rows of one segment arrive in key order, and a key that two segments
/// list arrives once from each. Up to `STORE_READS_IN_FLIGHT` segments are
/// open and loading through `segment_cache` at once. Each refill loads the
/// next blocks of each open segment's key range, up to
/// `MAX_SCAN_REFILL_BYTES` of stored bytes and the decoded bytes that it
/// reserves from the cache's read working memory, and inserts each block that
/// it fetches into the block cache. The memo's index and filter blocks are
/// released before `visit` sees the rows, and a refill's reservation stays
/// until `visit` has seen every row of the refill's blocks.
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
    let block_memo = SessionBlockMemo::new(read_working_memory(segment_cache));
    let loader = ScanLoader {
        store,
        segment_cache,
        block_memo: &block_memo,
        pool: block_memo.pool(),
        blocks_per_refill: BlocksPerRefill::KeyRange,
        upper_bound,
    };
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
        refill_iterators(&loader, &mut iterators, loader.pool.available_bytes()).await?;
        // The memo releases its charge before `visit` runs, so the caller's
        // sets can take the rest of the read working memory.
        block_memo.clear();
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

    fn data_block_count(&self, entries: &[SegmentIndexEntry], max_decoded_bytes: usize) -> usize {
        if self.blocks_per_refill == BlocksPerRefill::One {
            return entries.len().min(1);
        }
        let needed = index_blocks_for_key_range(entries, "", self.upper_bound);
        let mut stored_bytes = 0;
        let mut decoded_bytes = 0;
        entries[needed]
            .iter()
            .take_while(|entry| {
                let next_stored_bytes = stored_bytes + u64::from(entry.block.stored_bytes);
                let next_decoded_bytes = decoded_bytes + entry.block.decoded_bytes as usize;
                // A refill holds its first block whatever its size.
                if stored_bytes > 0
                    && (next_stored_bytes > MAX_SCAN_REFILL_BYTES
                        || next_decoded_bytes > max_decoded_bytes)
                {
                    return false;
                }
                stored_bytes = next_stored_bytes;
                decoded_bytes = next_decoded_bytes;
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
    ) -> Result<LoadedBlocks<MetadataRow>, Self::Error> {
        let (blocks, reservation) = match self.blocks_per_refill {
            BlocksPerRefill::One => {
                let blocks = load_segment_data_block_span(
                    self.store,
                    self.segment_cache,
                    Some(self.block_memo),
                    segment.descriptor,
                    &entries,
                )
                .await?;
                (blocks, None)
            }
            BlocksPerRefill::KeyRange => {
                let (entries, reservation) = self.reserve_refill(&entries);
                let blocks = load_segment_data_block_span(
                    self.store,
                    self.segment_cache,
                    None,
                    segment.descriptor,
                    entries,
                )
                .await?;
                (blocks, Some(reservation))
            }
        };
        validate_manifest_row_seq_range(
            &metadata_segment_object_key(segment.descriptor),
            blocks.iter().flat_map(|block| block.rows.iter()),
            segment.max_seq,
        )?;
        Ok(LoadedBlocks {
            blocks,
            reservation,
        })
    }
}

impl<S: ObjectStore + ?Sized> ScanLoader<'_, S> {
    /// Reserves the decoded bytes of the longest prefix of `entries` that the
    /// read working memory has room for, and always of the first entry, and
    /// returns that prefix with its reservation.
    ///
    /// The scan keeps the reservation until it has consumed the prefix's
    /// blocks. The first block is reserved even above the limit, so the
    /// scans that share the read working memory exceed its limit by at most
    /// one block for each segment that they have open.
    fn reserve_refill<'e>(
        &self,
        entries: &'e [SegmentIndexEntry],
    ) -> (&'e [SegmentIndexEntry], ReadReservation) {
        let decoded_bytes = |entry: &SegmentIndexEntry| entry.block.decoded_bytes as usize;
        let mut reservation = self.pool.reserve(
            entries.iter().map(decoded_bytes).sum(),
            decoded_bytes(&entries[0]),
        );
        let mut kept_bytes = decoded_bytes(&entries[0]);
        let kept = 1 + entries[1..]
            .iter()
            .take_while(|entry| {
                let next_bytes = kept_bytes + decoded_bytes(entry);
                if next_bytes > reservation.bytes() {
                    return false;
                }
                kept_bytes = next_bytes;
                true
            })
            .count();
        reservation.shrink_to(kept_bytes);
        (&entries[..kept], reservation)
    }
}
