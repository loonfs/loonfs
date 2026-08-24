//! Streams rows from multiple runs in row-key order with bounded buffering.

use super::block_fetch::load_segment_index_for_reorganization;
use super::block_load::SessionBlockMemo;
use super::data_block_load::load_segment_data_block_span;
use super::streaming_compaction::manifest_load_failure;
use super::validate::validate_manifest_row_seq_range;
use crate::error::Result;
use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_api::wire::sst_blocks::{DecodedDataBlock, SegmentIndexEntry};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use std::collections::VecDeque;
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

/// Reads one family's rows out of one run, one bounded span of data blocks at
/// a time.
///
/// A run's segments for one family are ascending and non-overlapping (manifest
/// load enforces it), so walking them in index order walks the run's rows in
/// row-key order. Only the current segment's index is held, and only the
/// blocks not yet consumed.
pub(super) struct SegmentRowIterator {
    pub(super) family: MetadataRowFamily,
    segments: Vec<MetadataSegmentRef>,
    next_segment: usize,
    index: Option<Arc<Vec<SegmentIndexEntry>>>,
    next_block: usize,
    blocks: VecDeque<Arc<DecodedDataBlock>>,
    row: usize,
}

impl SegmentRowIterator {
    pub(super) fn new(family: MetadataRowFamily, mut segments: Vec<MetadataSegmentRef>) -> Self {
        segments.sort_by_key(|descriptor| descriptor.segment_index);
        Self {
            family,
            segments,
            next_segment: 0,
            index: None,
            next_block: 0,
            blocks: VecDeque::new(),
            row: 0,
        }
    }

    pub(super) fn head(&self) -> Option<(&str, &MetadataRow)> {
        let block = self.blocks.front()?;
        Some((block.row_keys[self.row].as_str(), &block.rows[self.row]))
    }

    pub(super) fn take_head(&mut self) -> MetadataRow {
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

    /// Fetches the next span of data blocks, opening the next segment first
    /// when the current one is spent. A segment with no rows left is closed
    /// and its index dropped before the next one is read, so one iterator
    /// holds one index at a time.
    async fn fill<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        while self.blocks.is_empty() {
            let index = match &self.index {
                Some(index) if self.next_block < index.len() => Arc::clone(index),
                _ => {
                    self.index = None;
                    let Some(descriptor) = self.segments.get(self.next_segment) else {
                        return Ok(());
                    };
                    self.next_segment += 1;
                    self.next_block = 0;
                    let index = load_segment_index_for_reorganization(
                        store,
                        None,
                        &SessionBlockMemo::default(),
                        descriptor,
                    )
                    .await
                    .map_err(manifest_load_failure)?;
                    self.index = Some(Arc::clone(&index));
                    index
                }
            };
            let descriptor = &self.segments[self.next_segment - 1];
            let end = (self.next_block + BLOCKS_PER_ITERATOR_FETCH).min(index.len());
            // Compaction bypasses the shared segment cache. This temporary memo
            // retains decoded blocks only for the current load.
            let blocks = load_segment_data_block_span(
                store,
                None,
                &SessionBlockMemo::default(),
                descriptor,
                &index[self.next_block..end],
            )
            .await
            .map_err(manifest_load_failure)?;
            self.next_block = end;
            validate_manifest_row_seq_range(
                &metadata_segment_object_key(descriptor),
                blocks.iter().flat_map(|block| block.rows.iter()),
                descriptor.run_seq,
            )
            .map_err(manifest_load_failure)?;
            self.row = 0;
            self.blocks
                .extend(blocks.into_iter().filter(|block| !block.rows.is_empty()));
        }
        Ok(())
    }
}

/// Fills every iterator that has run out of rows, a bounded wave at a time,
/// and answers with the decoded blocks they then hold together.
pub(super) async fn refill_iterators<S: ObjectStore + ?Sized>(
    store: &S,
    iterators: &mut [SegmentRowIterator],
) -> Result<usize> {
    let mut hungry: Vec<&mut SegmentRowIterator> = iterators
        .iter_mut()
        .filter(|iterator| iterator.needs_fill())
        .collect();
    for wave in hungry.chunks_mut(ITERATOR_FETCH_CONCURRENCY) {
        futures::future::try_join_all(
            wave.iter_mut()
                .map(|iterator| Box::pin(iterator.fill(store))),
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
pub(super) fn select_next_iterator(
    iterators: &[SegmentRowIterator],
    locality: LocalityGrouping,
) -> Option<usize> {
    let mut best: Option<(usize, &str, &str)> = None;
    for (position, iterator) in iterators.iter().enumerate() {
        let Some((row_key, _)) = iterator.head() else {
            continue;
        };
        let candidate = locality_of(iterator.family, row_key, locality);
        let take = match best {
            // Locality first so a group's rows arrive together whatever
            // family they come from, then family, then key: within one
            // family that is row-key order, which is the order a segment
            // builder demands.
            Some((best_position, best_locality, best_key)) => {
                (candidate, iterators[position].family, row_key)
                    < (best_locality, iterators[best_position].family, best_key)
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
