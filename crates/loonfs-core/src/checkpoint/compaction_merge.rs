//! Streams rows from multiple runs in row-key order with bounded buffering.

use super::block_fetch::load_segment_index_for_reorganization;
use super::block_load::SessionBlockMemo;
use super::data_block_load::load_segment_data_block_span;
use super::streaming_compaction::manifest_load_failure;
use super::validate::validate_manifest_row_seq_range;
use crate::error::Result;
use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentIndexEntry};
use loonfs_api::ChangeSeq;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

/// Decoded data blocks one iterator holds at a time. An iterator refills only
/// when it has none left, so this is also the most it ever holds, and a
/// merge's input residency is this times the number of open iterators.
const BLOCKS_PER_ITERATOR_FETCH: usize = 2;

/// Iterators refilled in one wave. Refills are a merge's only bulk reads, so
/// this is the width of its fan-out at the store.
const ITERATOR_FETCH_CONCURRENCY: usize = 8;

/// Defines which adjacent rows a retention rule processes together.
///
/// Groups use the shortest shared row-key prefix required by the rule: a
/// binding generation, deletion identity, inode, or single row. Binding
/// generations keep memory bounded when a name is reused many times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalityGrouping {
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
pub(super) fn locality_of(
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

/// Loads the index and data blocks for one segment row iterator.
pub trait SegmentBlockLoader<Row, Segment> {
    type Error;

    fn load_index(
        &self,
        segment: Segment,
    ) -> impl Future<Output = std::result::Result<Arc<Vec<SegmentIndexEntry>>, Self::Error>>;

    fn load_data_blocks(
        &self,
        segment: Segment,
        entries: Vec<SegmentIndexEntry>,
    ) -> impl Future<Output = std::result::Result<Vec<Arc<DecodedDataBlock<Row>>>, Self::Error>>;
}

/// Walks ordered segment rows with bounded decoded-block residency.
pub struct SegmentRowIterator<Row, Segment, SortKey> {
    sort_key: SortKey,
    segments: Vec<Segment>,
    next_segment: usize,
    index: Option<Arc<Vec<SegmentIndexEntry>>>,
    next_block: usize,
    blocks: VecDeque<Arc<DecodedDataBlock<Row>>>,
    row: usize,
    lower: Option<String>,
}

impl<Row, Segment, SortKey> SegmentRowIterator<Row, Segment, SortKey> {
    /// Creates an iterator over segments already ordered by segment index.
    pub fn new(sort_key: SortKey, segments: Vec<Segment>, lower: Option<String>) -> Self {
        Self {
            sort_key,
            segments,
            next_segment: 0,
            index: None,
            next_block: 0,
            blocks: VecDeque::new(),
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

    async fn fill<Loader>(&mut self, loader: &Loader) -> std::result::Result<(), Loader::Error>
    where
        Segment: Clone,
        Loader: SegmentBlockLoader<Row, Segment>,
    {
        while self.blocks.is_empty() {
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
            let end = (self.next_block + BLOCKS_PER_ITERATOR_FETCH).min(index.len());
            let entries = index[self.next_block..end].to_vec();
            let blocks = loader.load_data_blocks(segment, entries).await?;
            self.next_block = end;
            for block in blocks {
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
pub(super) struct MetadataSegmentInput {
    descriptor: MetadataSegmentRef,
    max_seq: ChangeSeq,
}

pub(super) type MetadataSegmentRowIterator =
    SegmentRowIterator<MetadataRow, MetadataSegmentInput, MetadataRowFamily>;

impl MetadataSegmentRowIterator {
    pub(super) fn metadata(
        family: MetadataRowFamily,
        max_seq: ChangeSeq,
        mut segments: Vec<MetadataSegmentRef>,
    ) -> Self {
        segments.sort_by_key(|descriptor| descriptor.segment_index);
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

pub(super) struct MetadataSegmentBlockLoader<'a, S: ?Sized> {
    store: &'a S,
}

impl<'a, S: ?Sized> MetadataSegmentBlockLoader<'a, S> {
    pub(super) fn new(store: &'a S) -> Self {
        Self { store }
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
        load_segment_index_for_reorganization(
            self.store,
            None,
            &SessionBlockMemo::default(),
            &segment.descriptor,
        )
        .await
        .map_err(manifest_load_failure)
    }

    async fn load_data_blocks(
        &self,
        segment: MetadataSegmentInput,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<Vec<Arc<DecodedDataBlock>>> {
        let blocks = load_segment_data_block_span(
            self.store,
            None,
            &SessionBlockMemo::default(),
            &segment.descriptor,
            &entries,
        )
        .await
        .map_err(manifest_load_failure)?;
        validate_manifest_row_seq_range(
            &metadata_segment_object_key(&segment.descriptor),
            blocks.iter().flat_map(|block| block.rows.iter()),
            segment.max_seq,
        )
        .map_err(manifest_load_failure)?;
        Ok(blocks)
    }
}

/// Fills every iterator that has run out of rows, a bounded wave at a time,
/// and answers with the decoded blocks they then hold together.
pub async fn refill_iterators<Row, Segment, SortKey, Loader>(
    loader: &Loader,
    iterators: &mut [SegmentRowIterator<Row, Segment, SortKey>],
) -> std::result::Result<usize, Loader::Error>
where
    Segment: Clone,
    Loader: SegmentBlockLoader<Row, Segment>,
{
    let mut hungry: Vec<&mut SegmentRowIterator<Row, Segment, SortKey>> = iterators
        .iter_mut()
        .filter(|iterator| iterator.needs_fill())
        .collect();
    for wave in hungry.chunks_mut(ITERATOR_FETCH_CONCURRENCY) {
        futures::future::try_join_all(
            wave.iter_mut()
                .map(|iterator| Box::pin(iterator.fill(loader))),
        )
        .await?;
    }
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
    use super::{locality_of, LocalityGrouping};
    use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily};
    use loonfs_api::{ChangeSeq, DisplayName, InodeId, NameKey};

    fn bind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryBind {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
        }
    }

    fn unbind(parent: u64, name: &str, bind_seq: u64) -> MetadataRow {
        MetadataRow::DirentryUnbind {
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("name key"),
            display_name: DisplayName::parse(name).expect("display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(bind_seq + 1),
            unbind_delta_index: 0,
        }
    }

    /// The grouping the bindings cluster merges by.
    const GENERATION: LocalityGrouping = LocalityGrouping::LeadingKeyComponents(4);

    #[test]
    fn a_bind_and_its_unbind_name_one_locality_group() {
        let bound = bind(7, "report.txt", 11);
        let retired = unbind(7, "report.txt", 11);
        let bind_key = bound.row_key_for_family(MetadataRowFamily::DirentryBinds);
        let unbind_key = retired.row_key_for_family(MetadataRowFamily::DirentryUnbinds);

        assert_eq!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, GENERATION),
            locality_of(MetadataRowFamily::DirentryUnbinds, &unbind_key, GENERATION),
        );
        // Another generation of the same name is a different group, which is
        // what keeps a slot with any number of generations bounded.
        let regenerated =
            bind(7, "report.txt", 12).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, GENERATION),
            locality_of(MetadataRowFamily::DirentryBinds, &regenerated, GENERATION),
        );
        // Another name under the same parent is a different group: the rules
        // read one binding, not one directory.
        let other = bind(7, "other.txt", 11).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, GENERATION),
            locality_of(MetadataRowFamily::DirentryBinds, &other, GENERATION),
        );
        // And so is the same name under another parent.
        let elsewhere =
            bind(8, "report.txt", 11).row_key_for_family(MetadataRowFamily::DirentryBinds);
        assert_ne!(
            locality_of(MetadataRowFamily::DirentryBinds, &bind_key, GENERATION),
            locality_of(MetadataRowFamily::DirentryBinds, &elsewhere, GENERATION),
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
            .map(|key| locality_of(MetadataRowFamily::DirentryBinds, key, GENERATION))
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
}
