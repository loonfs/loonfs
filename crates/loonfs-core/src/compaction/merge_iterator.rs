//! Streams rows from multiple runs in row-key order with bounded buffering.

use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::manifest::validate::validate_manifest_row_seq_range;
use crate::read_working_memory::ReadReservation;
use crate::segment::block_memo::SessionBlockMemo;
use crate::segment::data_block_load::load_segment_data_block_span;
use crate::segment::index_block_load::load_segment_index_for_compaction;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{DecodedDataBlock, SegmentIndexEntry};
use loonfs_types::ChangeSeq;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

/// Stored bytes one refill asks for in a single ranged read; at object-store
/// latency the round trip, not the bytes, is what a refill costs.
pub(crate) const ITERATOR_FETCH_TARGET_BYTES: usize = 2 * 1024 * 1024;

/// Defines which adjacent rows a retention rule processes together.
///
/// Groups use the shortest shared row-key prefix required by the rule: a
/// binding edge, a deletion identity, an inode, or a single row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalityGrouping {
    /// Every row is judged on its own.
    Row,
    /// Rows sharing the first `n` hyphen-separated components after their
    /// family's row-key prefix are judged together. Every component of the
    /// grammar is digits or lowercase hex, and a hyphen sorts below both, so
    /// rows of one grouping are contiguous in row-key order and grouping
    /// order is row-key order.
    LeadingKeyComponents(usize),
}

/// Returns the row's locality group after removing the family prefix.
pub(crate) fn locality_of(
    family: MetadataRowFamily,
    row_key: &str,
    locality: LocalityGrouping,
) -> &str {
    let prefix_len = family.row_key_prefix().len();
    let tail = row_key.get(prefix_len..).unwrap_or(row_key);
    let LocalityGrouping::LeadingKeyComponents(components) = locality else {
        return tail;
    };
    let mut end = 0;
    for component in 0..components {
        if component > 0 {
            end += 1;
        }
        match tail[end..].find('-') {
            Some(offset) => end += offset,
            None => return tail,
        }
    }
    &tail[..end]
}

/// The data blocks that one refill of a segment row iterator loaded, with the
/// read working memory reserved for those blocks.
pub struct LoadedBlocks<Row> {
    /// One block for each of a prefix of the requested entries, in order.
    pub blocks: Vec<Arc<DecodedDataBlock<Row>>>,
    /// The bytes reserved for `blocks`, or `None` when the loader bounds its
    /// blocks by its own budget.
    pub reservation: Option<ReadReservation>,
}

/// Loads the index and data blocks for one segment row iterator.
pub trait SegmentBlockLoader<Row, Segment> {
    type Error;

    /// Selects a prefix of the remaining index; zero ends the segment.
    fn data_block_count(&self, entries: &[SegmentIndexEntry], max_decoded_bytes: usize) -> usize {
        let mut count = 0;
        let mut stored_bytes = 0;
        let mut decoded_bytes = 0;
        let target_bytes = ITERATOR_FETCH_TARGET_BYTES.min(max_decoded_bytes);
        for entry in entries {
            let next_decoded_bytes = decoded_bytes + entry.block.decoded_bytes as usize;
            // A block cannot be split, even when it exceeds the iterator's share.
            if count > 0 && (stored_bytes >= target_bytes || next_decoded_bytes > max_decoded_bytes)
            {
                break;
            }
            stored_bytes += entry.block.stored_bytes as usize;
            decoded_bytes = next_decoded_bytes;
            count += 1;
        }
        count
    }

    fn load_index(
        &self,
        segment: Segment,
    ) -> impl Future<Output = std::result::Result<Arc<Vec<SegmentIndexEntry>>, Self::Error>>;

    /// Loads the blocks of a prefix of `entries` that holds at least one
    /// block, in order.
    fn load_data_blocks(
        &self,
        segment: Segment,
        entries: Vec<SegmentIndexEntry>,
    ) -> impl Future<Output = std::result::Result<LoadedBlocks<Row>, Self::Error>>;
}

/// Walks ordered segment rows with bounded decoded-block residency.
pub struct SegmentRowIterator<Row, Segment, SortKey> {
    sort_key: SortKey,
    segments: Vec<Segment>,
    next_segment: usize,
    index: Option<Arc<Vec<SegmentIndexEntry>>>,
    next_block: usize,
    blocks: VecDeque<Arc<DecodedDataBlock<Row>>>,
    /// The read working memory reserved for the blocks of the last refill,
    /// which the next refill releases once `blocks` is empty.
    batch_reservation: Option<ReadReservation>,
    row: usize,
    lower: Option<String>,
}

impl<Row, Segment, SortKey> SegmentRowIterator<Row, Segment, SortKey> {
    /// Creates an iterator over segments already in their run's list order.
    pub fn new(sort_key: SortKey, segments: Vec<Segment>, lower: Option<String>) -> Self {
        Self {
            sort_key,
            segments,
            next_segment: 0,
            index: None,
            next_block: 0,
            blocks: VecDeque::new(),
            batch_reservation: None,
            row: 0,
            lower,
        }
    }

    /// Returns the source ordering key.
    pub fn sort_key(&self) -> &SortKey {
        &self.sort_key
    }

    /// Returns the segment that owns the current row.
    pub fn current_segment(&self) -> Option<&Segment> {
        self.next_segment
            .checked_sub(1)
            .and_then(|position| self.segments.get(position))
    }

    /// Borrows the next row key and row.
    pub fn head(&self) -> Option<(&str, &Row)> {
        let block = self.blocks.front()?;
        Some((block.row_keys[self.row].as_str(), &block.rows[self.row]))
    }

    pub(crate) fn loaded_through_key(&self) -> Option<&str> {
        self.index
            .as_ref()?
            .get(self.next_block.checked_sub(1)?)
            .map(|entry| entry.last_row_key.as_str())
    }

    /// Removes and returns the next row.
    pub fn take_head(&mut self) -> Row
    where
        Row: Clone,
    {
        let row = self
            .blocks
            .front()
            .expect("an iterator with a head row should have a front block")
            .rows[self.row]
            .clone();
        self.row += 1;
        while self
            .blocks
            .front()
            .is_some_and(|block| self.row >= block.rows.len())
        {
            self.blocks.pop_front();
            self.row = 0;
        }
        row
    }

    fn needs_fill(&self) -> bool {
        self.blocks.is_empty() && !self.is_exhausted()
    }

    fn is_exhausted(&self) -> bool {
        self.blocks.is_empty()
            && self.next_segment >= self.segments.len()
            && self
                .index
                .as_ref()
                .is_none_or(|index| self.next_block >= index.len())
    }

    fn resident_blocks(&self) -> usize {
        self.blocks.len()
    }

    async fn fill<Loader>(
        &mut self,
        loader: &Loader,
        max_decoded_bytes: usize,
    ) -> std::result::Result<(), Loader::Error>
    where
        Segment: Clone,
        Loader: SegmentBlockLoader<Row, Segment>,
    {
        while self.blocks.is_empty() {
            self.batch_reservation = None;
            let index = match &self.index {
                Some(index) if self.next_block < index.len() => Arc::clone(index),
                _ => {
                    self.index = None;
                    let Some(segment) = self.segments.get(self.next_segment).cloned() else {
                        return Ok(());
                    };
                    self.next_segment += 1;
                    let index = loader.load_index(segment).await?;
                    self.next_block = self.lower.as_deref().map_or(0, |lower| {
                        index.partition_point(|entry| entry.last_row_key.as_str() < lower)
                    });
                    self.index = Some(Arc::clone(&index));
                    index
                }
            };
            let segment = self.segments[self.next_segment - 1].clone();
            let count = loader.data_block_count(&index[self.next_block..], max_decoded_bytes);
            if count == 0 {
                self.next_block = index.len();
                continue;
            }
            let entries = index[self.next_block..self.next_block + count].to_vec();
            let loaded = loader.load_data_blocks(segment, entries).await?;
            assert!(
                !loaded.blocks.is_empty() && loaded.blocks.len() <= count,
                "a refill should load a non-empty prefix of the blocks it asked for"
            );
            self.next_block += loaded.blocks.len();
            self.batch_reservation = loaded.reservation;
            for block in loaded.blocks {
                let next_row = self.lower.as_deref().map_or(0, |lower| {
                    block
                        .row_keys
                        .partition_point(|row_key| row_key.as_str() < lower)
                });
                if next_row < block.rows.len() {
                    if self.blocks.is_empty() {
                        self.row = next_row;
                    }
                    self.blocks.push_back(block);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct MetadataSegmentInput {
    descriptor: MetadataSegmentRef,
    max_seq: ChangeSeq,
}

pub(crate) type MetadataSegmentRowIterator =
    SegmentRowIterator<MetadataRow, MetadataSegmentInput, MetadataRowFamily>;

impl MetadataSegmentRowIterator {
    pub(crate) fn metadata(
        family: MetadataRowFamily,
        max_seq: ChangeSeq,
        segments: Vec<MetadataSegmentRef>,
    ) -> Self {
        Self::new(
            family,
            segments
                .into_iter()
                .map(|descriptor| MetadataSegmentInput {
                    descriptor,
                    max_seq,
                })
                .collect(),
            None,
        )
    }
}

pub(crate) struct MetadataSegmentBlockLoader<'a, S: ?Sized> {
    store: &'a S,
    index_memo: Option<&'a SessionBlockMemo>,
}

impl<'a, S: ?Sized> MetadataSegmentBlockLoader<'a, S> {
    pub(crate) fn new(store: &'a S, index_memo: Option<&'a SessionBlockMemo>) -> Self {
        Self { store, index_memo }
    }
}

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<MetadataRow, MetadataSegmentInput>
    for MetadataSegmentBlockLoader<'_, S>
{
    type Error = crate::error::CoreError;

    async fn load_index(
        &self,
        segment: MetadataSegmentInput,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        load_segment_index_for_compaction(self.store, None, self.index_memo, &segment.descriptor)
            .await
            .map_err(CoreError::from)
    }

    async fn load_data_blocks(
        &self,
        segment: MetadataSegmentInput,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<LoadedBlocks<MetadataRow>> {
        let blocks =
            load_segment_data_block_span(self.store, None, None, &segment.descriptor, &entries)
                .await?;
        validate_manifest_row_seq_range(
            &metadata_segment_object_key(&segment.descriptor),
            blocks.iter().flat_map(|block| block.rows.iter()),
            segment.max_seq,
        )?;
        // Compaction bounds the blocks it keeps by its step's decoded input
        // budget, not by the read working memory.
        Ok(LoadedBlocks {
            blocks,
            reservation: None,
        })
    }
}

/// Fills every iterator that has run out of rows, with a bounded number of
/// fills in flight, and returns the decoded blocks they then keep together.
///
/// The iterators share `max_decoded_input_bytes` equally; each share caps a
/// refill below the fetch target when many inputs are open.
pub async fn refill_iterators<Row, Segment, SortKey, Loader>(
    loader: &Loader,
    iterators: &mut [SegmentRowIterator<Row, Segment, SortKey>],
    max_decoded_input_bytes: usize,
) -> std::result::Result<usize, Loader::Error>
where
    Segment: Clone,
    Loader: SegmentBlockLoader<Row, Segment>,
{
    let max_decoded_bytes = max_decoded_input_bytes / iterators.len().max(1);
    let fills: Vec<_> = iterators
        .iter_mut()
        .filter(|iterator| iterator.needs_fill())
        .map(|iterator| Box::pin(iterator.fill(loader, max_decoded_bytes)))
        .collect();
    stream::iter(fills)
        .buffer_unordered(STORE_READS_IN_FLIGHT)
        .try_collect::<()>()
        .await?;
    Ok(iterators
        .iter()
        .map(SegmentRowIterator::resident_blocks)
        .sum())
}

/// The iterator whose next row sorts first, or `None` when every iterator is
/// spent.
///
/// A linear scan rather than a heap: an iterator is opened per run per family
/// of one cluster, which is a handful, and the scan compares borrowed key
/// slices while a heap would have to own them.
pub fn select_next_iterator<Row, Segment, SortKey, Locality>(
    iterators: &[SegmentRowIterator<Row, Segment, SortKey>],
    mut locality: Locality,
) -> Option<usize>
where
    SortKey: Ord,
    Locality: for<'a> FnMut(&SortKey, &'a str) -> &'a str,
{
    let mut best: Option<(usize, &str, &str)> = None;
    for (position, iterator) in iterators.iter().enumerate() {
        let Some((row_key, _)) = iterator.head() else {
            continue;
        };
        let candidate = locality(iterator.sort_key(), row_key);
        let take = match best {
            // Locality first so a group's rows arrive together whatever
            // family they come from, then family, then key: within one
            // family that is row-key order, which is the order a segment
            // builder demands.
            Some((best_position, best_locality, best_key)) => {
                (candidate, iterators[position].sort_key(), row_key)
                    < (best_locality, iterators[best_position].sort_key(), best_key)
            }
            None => true,
        };
        if take {
            best = Some((position, candidate, row_key));
        }
    }
    best.map(|(position, _, _)| position)
}

#[cfg(test)]
mod tests {
    use super::{
        locality_of, refill_iterators, LoadedBlocks, LocalityGrouping, SegmentBlockLoader,
        SegmentRowIterator,
    };
    use crate::read_working_memory::ReadWorkingMemory;
    use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily};
    use loonfs_types::format::segment_blocks::{BlockHandle, DecodedDataBlock, SegmentIndexEntry};
    use loonfs_types::{ChangeSeq, DisplayName, InodeId, NameKey};
    use std::sync::{Arc, Mutex};

    fn bind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(u128::from(parent)),
            name_key: NameKey::parse(name).expect("name key"),
            state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                display_name: DisplayName::parse(name).expect("display name"),
                revision: Some(crate::test_support::revision_summary()),
            },
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq),
            delta_index: 0,
            commit_id: crate::test_support::binding_commit_id(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 0,
            bind_position: loonfs_types::format::manifest::DeltaPosition {
                seq: ChangeSeq(bind_seq),
                delta_index: 0,
            },
        })
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
            parent_inode_id: InodeId(u128::from(parent)),
            name_key: NameKey::parse(name).expect("name key"),
            child_inode_id: InodeId(42),
            child_kind: loonfs_types::InodeKind::File,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(bind_seq + 1),
            delta_index: 0,
            commit_id: crate::test_support::binding_commit_id(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 0,
            bind_position: loonfs_types::format::manifest::DeltaPosition {
                seq: ChangeSeq(bind_seq + 1),
                delta_index: 0,
            },
            state: loonfs_types::format::manifest::DirentryBindingState::Unbound,
        })
    }

    const SLOT_AND_CHILD: LocalityGrouping = LocalityGrouping::LeadingKeyComponents(3);

    #[test]
    fn changes_to_one_slot_and_child_share_a_locality_group() {
        let bound = bind(7, "report.txt", 11);
        let retired = unbind(7, "report.txt", 11);
        let bind_key = bound.row_key_for_family(MetadataRowFamily::DirentryBinds);
        let unbind_key = retired.row_key_for_family(MetadataRowFamily::DirentryBinds);

        assert_eq!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, SLOT_AND_CHILD),
            locality_of(
                MetadataRowFamily::DirentryBinds,
                &unbind_key,
                SLOT_AND_CHILD
            ),
        );
        let regenerated =
            bind(7, "report.txt", 12).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_eq!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, SLOT_AND_CHILD),
            locality_of(
                MetadataRowFamily::DirentryBinds,
                &regenerated,
                SLOT_AND_CHILD
            ),
        );
        // Another name under the same parent is a different group: the rules
        // read one binding, not one directory.
        let other = bind(7, "other.txt", 11).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, SLOT_AND_CHILD),
            locality_of(MetadataRowFamily::DirentryBinds, &other, SLOT_AND_CHILD),
        );
        // And so is the same name under another parent.
        let elsewhere =
            bind(8, "report.txt", 11).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, SLOT_AND_CHILD),
            locality_of(MetadataRowFamily::DirentryBinds, &elsewhere, SLOT_AND_CHILD),
        );
    }

    #[test]
    fn locality_order_follows_row_key_order() {
        let mut keys: Vec<String> = ["a", "ab", "b", "a-b"]
            .into_iter()
            .flat_map(|name| {
                [11u64, 12].map(|seq| {
                    bind(7, name, seq).row_key_for_family(MetadataRowFamily::DirentryBinds)
                })
            })
            .collect();
        keys.sort();

        let localities: Vec<&str> = keys
            .iter()
            .map(|key| locality_of(MetadataRowFamily::DirentryBinds, key, SLOT_AND_CHILD))
            .collect();
        let mut runs = localities.clone();
        runs.dedup();
        let mut distinct = runs.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            runs.len(),
            distinct.len(),
            "a group must not reappear after another group began: {localities:?}"
        );
        assert!(
            runs.windows(2).all(|pair| pair[0] < pair[1]),
            "grouping order must be row-key order: {runs:?}"
        );
    }

    /// A loader of one in-memory segment of two-row blocks that returns at
    /// most two blocks of each request, reserves each batch's decoded bytes,
    /// and records how many blocks each request asked for.
    struct ShortBatches {
        blocks: Vec<Arc<DecodedDataBlock<String>>>,
        index: Arc<Vec<SegmentIndexEntry>>,
        pool: Arc<ReadWorkingMemory>,
        requests: Mutex<Vec<usize>>,
    }

    impl ShortBatches {
        fn new(block_count: u64) -> Self {
            let row_key = |row: u64| format!("key-{row:03}");
            let blocks = (0..block_count)
                .map(|block| {
                    let row_keys = vec![row_key(2 * block), row_key(2 * block + 1)];
                    Arc::new(DecodedDataBlock {
                        rows: row_keys.clone(),
                        row_keys,
                    })
                })
                .collect();
            let index = (0..block_count)
                .map(|block| SegmentIndexEntry {
                    last_row_key: row_key(2 * block + 1),
                    block: BlockHandle {
                        offset: block * 100,
                        stored_bytes: 100,
                        decoded_bytes: 100,
                        crc32c: 0,
                    },
                })
                .collect();
            Self {
                blocks,
                index: Arc::new(index),
                pool: Arc::new(ReadWorkingMemory::new(usize::MAX, None)),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl SegmentBlockLoader<String, ()> for ShortBatches {
        type Error = ();

        async fn load_index(&self, _segment: ()) -> Result<Arc<Vec<SegmentIndexEntry>>, ()> {
            Ok(Arc::clone(&self.index))
        }

        async fn load_data_blocks(
            &self,
            _segment: (),
            entries: Vec<SegmentIndexEntry>,
        ) -> Result<LoadedBlocks<String>, ()> {
            self.requests.lock().expect("requests").push(entries.len());
            let first = usize::try_from(entries[0].block.offset / 100).expect("block number");
            let count = entries.len().min(2);
            Ok(LoadedBlocks {
                blocks: self.blocks[first..first + count].to_vec(),
                reservation: Some(self.pool.reserve(100 * count, 100)),
            })
        }
    }

    #[tokio::test]
    async fn a_refill_that_loads_a_shorter_prefix_loads_the_rest_on_the_next_refill() {
        let loader = ShortBatches::new(5);
        let mut iterator = SegmentRowIterator::new((), vec![()], Some("key-001".to_owned()));
        let mut rows = Vec::new();
        loop {
            refill_iterators(&loader, std::slice::from_mut(&mut iterator), usize::MAX)
                .await
                .expect("refill");
            if iterator.head().is_none() {
                break;
            }
            // Only the batch that the iterator keeps is reserved.
            assert!(loader.pool.in_use() <= 200);
            while iterator.head().is_some() {
                rows.push(iterator.take_head());
            }
        }
        let expected: Vec<_> = (1..10).map(|row| format!("key-{row:03}")).collect();
        assert_eq!(rows, expected);
        assert_eq!(*loader.requests.lock().expect("requests"), [5, 3, 1]);
        drop(iterator);
        assert_eq!(loader.pool.in_use(), 0);
    }
}
