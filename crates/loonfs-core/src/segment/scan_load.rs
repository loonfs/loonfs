//! Cached block loading for metadata scans.

use super::block_memo::SessionBlockMemo;
use super::cache::MetadataSegmentCache;
use super::data_block_load::load_segment_data_block_span;
use super::index_block_load::load_segment_index;
use super::scan::BlocksPerRefill;
use crate::compaction::run_iterator::{LoadedBlocks, SegmentBlockLoader};
use crate::limits::MAX_SCAN_REFILL_BYTES;
use crate::manifest::error::ManifestLoadError;
use crate::manifest::validate::validate_manifest_row_seq_range;
use crate::read_working_memory::{ReadReservation, ReadWorkingMemory};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{index_blocks_for_key_range, SegmentIndexEntry};
use loonfs_types::ChangeSeq;
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
    pub(super) read_working_memory: &'a Arc<ReadWorkingMemory>,
    pub(super) blocks_per_refill: BlocksPerRefill,
    pub(super) upper_bound: Option<&'a str>,
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
        let mut reservation = self.read_working_memory.reserve(
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
