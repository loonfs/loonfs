//! The unfolded WAL's rows of one listed family in row-key order: the order
//! of a state's rows, and [`TailRows`], one merge over a view's states.
//!
//! Within one key, a row with a higher sequence comes first, and then the row
//! of the newer state, in the order of `MetadataView::row_states`.

use super::rows::trash_record_from_tombstone;
use super::{DirentryBindingRecord, InodeRecord, MetadataState, RevisionRecord};
use loonfs_types::format::manifest::{lookup_keys, SubtreeTombstoneRecord, TrashRecord};
use loonfs_types::CommitSeq;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::size_of;

/// A family that a listing reads from the unfolded WAL: `Stored` is the
/// record that a state keeps, and the family's own type is the record that
/// the listing takes.
pub(super) trait TailFamily: Sized + 'static {
    type Stored: 'static;

    fn rows(state: &MetadataState) -> &[Self::Stored];

    fn order(orders: &RowOrders) -> &[u32];

    fn row_key(row: &Self::Stored) -> String;

    fn committed_seq(row: &Self::Stored) -> CommitSeq;

    fn listed(row: &Self::Stored) -> Self;
}

impl TailFamily for InodeRecord {
    type Stored = Self;

    fn rows(state: &MetadataState) -> &[Self] {
        state.inodes()
    }

    fn order(orders: &RowOrders) -> &[u32] {
        &orders.inodes
    }

    fn row_key(row: &Self) -> String {
        lookup_keys::inode_key(row.inode_id)
    }

    fn committed_seq(row: &Self) -> CommitSeq {
        row.committed_seq
    }

    fn listed(row: &Self) -> Self {
        row.clone()
    }
}

impl TailFamily for DirentryBindingRecord {
    type Stored = Self;

    fn rows(state: &MetadataState) -> &[Self] {
        state.direntry_binds()
    }

    fn order(orders: &RowOrders) -> &[u32] {
        &orders.direntry_binds
    }

    fn row_key(row: &Self) -> String {
        lookup_keys::direntry_bind_row_key(
            row.parent_inode_id,
            &row.name_key,
            row.child_inode_id,
            row.committed_seq,
            row.delta_index,
        )
    }

    fn committed_seq(row: &Self) -> CommitSeq {
        row.committed_seq
    }

    fn listed(row: &Self) -> Self {
        row.clone()
    }
}

impl TailFamily for RevisionRecord {
    type Stored = Self;

    fn rows(state: &MetadataState) -> &[Self] {
        state.revisions()
    }

    fn order(orders: &RowOrders) -> &[u32] {
        &orders.revisions
    }

    fn row_key(row: &Self) -> String {
        lookup_keys::revision_row_key(
            row.inode_id,
            row.revision_no,
            row.committed_seq,
            row.delta_index,
        )
    }

    fn committed_seq(row: &Self) -> CommitSeq {
        row.committed_seq
    }

    fn listed(row: &Self) -> Self {
        row.clone()
    }
}

/// The trash rows that a state's tombstone events derive.
impl TailFamily for TrashRecord {
    type Stored = SubtreeTombstoneRecord;

    fn rows(state: &MetadataState) -> &[SubtreeTombstoneRecord] {
        state.subtree_tombstones()
    }

    fn order(orders: &RowOrders) -> &[u32] {
        &orders.trash
    }

    fn row_key(row: &SubtreeTombstoneRecord) -> String {
        trash_record_from_tombstone(row).row_key()
    }

    fn committed_seq(row: &SubtreeTombstoneRecord) -> CommitSeq {
        row.committed_seq
    }

    fn listed(row: &SubtreeTombstoneRecord) -> Self {
        trash_record_from_tombstone(row)
    }
}

/// The positions of a tail chunk's rows of each listed family, in the order
/// that [`TailRows`] merges them. A position indexes the family's rows in the
/// chunk's state.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RowOrders {
    inodes: Box<[u32]>,
    direntry_binds: Box<[u32]>,
    revisions: Box<[u32]>,
    trash: Box<[u32]>,
}

impl RowOrders {
    pub(crate) fn new(state: &MetadataState) -> Self {
        Self {
            inodes: sorted_positions::<InodeRecord>(state).into(),
            direntry_binds: sorted_positions::<DirentryBindingRecord>(state).into(),
            revisions: sorted_positions::<RevisionRecord>(state).into(),
            trash: sorted_positions::<TrashRecord>(state).into(),
        }
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        (self.inodes.len() + self.direntry_binds.len() + self.revisions.len() + self.trash.len())
            * size_of::<u32>()
    }
}

/// Returns the positions of `state`'s rows of `F` sorted by row key, and the
/// higher sequence first within one key. Each key is formatted once.
fn sorted_positions<F: TailFamily>(state: &MetadataState) -> Vec<u32> {
    let rows = F::rows(state);
    let len = u32::try_from(rows.len())
        .expect("a state should hold fewer rows of one family than a `u32` position counts");
    let mut positions: Vec<u32> = (0..len).collect();
    positions.sort_by_cached_key(|position| {
        let row = &rows[*position as usize];
        (F::row_key(row), Reverse(F::committed_seq(row)))
    });
    positions
}

/// One state's rows of `F` before `end`, in the state's order, from `next`.
struct StateRows<'a, F: TailFamily> {
    rows: &'a [F::Stored],
    order: Cow<'a, [u32]>,
    next: usize,
    end: usize,
}

impl<'a, F: TailFamily> StateRows<'a, F> {
    fn row(&self, index: usize) -> &'a F::Stored {
        &self.rows[self.order[index] as usize]
    }

    /// Returns the index of the first row from `from` whose key is at or
    /// after `bound`, with a binary search that formats the keys it compares.
    fn first_at_or_after(&self, from: usize, bound: &str) -> usize {
        from + self.order[from..self.end]
            .partition_point(|position| F::row_key(&self.rows[*position as usize]).as_str() < bound)
    }
}

/// The key and sequence of a state's next row, and the state's place in the
/// merge, in the order that the merge yields rows.
type NextRow = (String, Reverse<CommitSeq>, usize);

/// A merge of several states' rows of `F` in row-key order, read on demand,
/// that yields only the rows at or below a view's sequence.
pub(super) struct TailRows<'a, F: TailFamily> {
    /// The states in the view's order, newest first.
    states: Vec<StateRows<'a, F>>,
    /// For each state with rows left, the key and sequence of its next row,
    /// and the state's place in `states`.
    heap: BinaryHeap<Reverse<NextRow>>,
    visible_seq: CommitSeq,
}

impl<'a, F: TailFamily> TailRows<'a, F> {
    /// Starts a merge of `states`' rows at or below `visible_seq` whose keys
    /// are at or after `lower_bound` and before `upper_bound`. A state that
    /// comes with its order uses it, and the merge sorts the rows of a state
    /// without one.
    pub(super) fn new(
        states: impl IntoIterator<Item = (&'a MetadataState, Option<&'a RowOrders>)>,
        lower_bound: &str,
        upper_bound: Option<&str>,
        visible_seq: CommitSeq,
    ) -> Self {
        let states: Vec<_> = states
            .into_iter()
            .map(|(state, orders)| {
                let order = match orders {
                    Some(orders) => Cow::Borrowed(F::order(orders)),
                    None => Cow::Owned(sorted_positions::<F>(state)),
                };
                let mut rows = StateRows {
                    rows: F::rows(state),
                    end: order.len(),
                    order,
                    next: 0,
                };
                if let Some(upper_bound) = upper_bound {
                    rows.end = rows.first_at_or_after(0, upper_bound);
                }
                rows.next = rows.first_at_or_after(0, lower_bound);
                rows
            })
            .collect();
        let mut merge = Self {
            heap: BinaryHeap::with_capacity(states.len()),
            states,
            visible_seq,
        };
        for state_no in 0..merge.states.len() {
            merge.push_next(state_no);
        }
        merge
    }

    /// Returns the key of the next row that the merge yields.
    pub(super) fn peek_key(&mut self) -> Option<&str> {
        while let Some(Reverse((_, _, state_no))) = self.heap.peek() {
            let rows = &self.states[*state_no];
            if F::committed_seq(rows.row(rows.next)) <= self.visible_seq {
                break;
            }
            self.pop();
        }
        self.heap
            .peek()
            .map(|Reverse((row_key, ..))| row_key.as_str())
    }

    /// Returns the next row that the merge yields, with its row key.
    pub(super) fn next(&mut self) -> Option<(String, F)> {
        self.peek_key()?;
        self.pop().map(|(row_key, row)| (row_key, F::listed(row)))
    }

    /// Skips every row before `lower_bound`. Only the states whose next key
    /// is before `lower_bound` move, each with a binary search.
    pub(super) fn seek(&mut self, lower_bound: &str) {
        while self
            .heap
            .peek()
            .is_some_and(|Reverse((row_key, ..))| row_key.as_str() < lower_bound)
        {
            let Reverse((_, _, state_no)) = self.heap.pop().expect("the heap holds what it peeked");
            let rows = &mut self.states[state_no];
            rows.next = rows.first_at_or_after(rows.next, lower_bound);
            self.push_next(state_no);
        }
    }

    fn pop(&mut self) -> Option<(String, &'a F::Stored)> {
        let Reverse((row_key, _, state_no)) = self.heap.pop()?;
        let rows = &mut self.states[state_no];
        let row = rows.row(rows.next);
        rows.next += 1;
        self.push_next(state_no);
        Some((row_key, row))
    }

    fn push_next(&mut self, state_no: usize) {
        let rows = &self.states[state_no];
        if rows.next < rows.end {
            let row = rows.row(rows.next);
            self.heap.push(Reverse((
                F::row_key(row),
                Reverse(F::committed_seq(row)),
                state_no,
            )));
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::metadata::{MetadataStateBuilder, TombstoneRowAction};
    use crate::wal::ProjectedWalTail;
    use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition};
    use loonfs_types::{ActorId, CommitId, ContentRef, InodeId, InodeKind, NameKey, RevisionNo};

    /// Returns the inode row of `inode_id` at `seq`, tagged with `tag` as its
    /// commit time.
    pub(in crate::metadata) fn tagged_inode(inode_id: u128, seq: u64, tag: u64) -> InodeRecord {
        InodeRecord {
            inode_id: InodeId(inode_id),
            inode_kind: InodeKind::File,
            committed_seq: CommitSeq(seq),
            commit_id: CommitId::parse("c_tail_rows").expect("commit id"),
            committed_by: ActorId::loonfs(),
            committed_at_ms: tag,
        }
    }

    pub(in crate::metadata) fn inode(inode_id: u128, seq: u64) -> InodeRecord {
        tagged_inode(inode_id, seq, 0)
    }

    pub(in crate::metadata) fn state(inodes: &[InodeRecord]) -> MetadataState {
        let mut builder = MetadataStateBuilder::default();
        for inode in inodes {
            builder.push_inode(inode.clone());
        }
        builder.build()
    }

    fn key(inode_id: u128) -> String {
        lookup_keys::inode_key(InodeId(inode_id))
    }

    fn read_all(rows: &mut TailRows<'_, InodeRecord>) -> Vec<(u128, u64, u64)> {
        std::iter::from_fn(|| rows.next())
            .map(|(row_key, row)| {
                assert_eq!(row_key, key(row.inode_id.0));
                (row.inode_id.0, row.committed_seq.0, row.committed_at_ms)
            })
            .collect()
    }

    /// Checks that the merge yields the rows of several states in key order,
    /// and of one key the higher sequence first and then the newer state's.
    #[test]
    fn the_merge_yields_rows_by_key_then_higher_sequence_then_newer_state() {
        let newest = state(&[tagged_inode(5, 3, 0), inode(9, 3), inode(1, 3)]);
        let middle = state(&[tagged_inode(5, 4, 1), inode(4, 4)]);
        let oldest = state(&[inode(2, 2), tagged_inode(5, 4, 2), inode(7, 2)]);
        let oldest_orders = RowOrders::new(&oldest);
        let mut rows = TailRows::<InodeRecord>::new(
            [
                (&newest, None),
                (&middle, None),
                (&oldest, Some(&oldest_orders)),
            ],
            &key(0),
            None,
            CommitSeq(4),
        );
        assert_eq!(
            read_all(&mut rows),
            [
                (1, 3, 0),
                (2, 2, 0),
                (4, 4, 0),
                (5, 4, 1),
                (5, 4, 2),
                (5, 3, 0),
                (7, 2, 0),
                (9, 3, 0),
            ]
        );
    }

    /// Checks that a seek skips the rows before its bound in every state,
    /// that the end key stops the merge, and that the filter drops rows.
    #[test]
    fn a_seek_skips_rows_in_every_state_and_the_end_key_stops_the_merge() {
        let first = state(&[inode(1, 1), inode(4, 1), inode(6, 3), inode(9, 1)]);
        let second = state(&[inode(2, 2), inode(3, 2), inode(5, 2), inode(8, 2)]);
        let mut rows = TailRows::<InodeRecord>::new(
            [(&first, None), (&second, None)],
            &key(2),
            Some(&key(8)),
            CommitSeq(2),
        );
        assert_eq!(rows.next().map(|(_, row)| row.inode_id), Some(InodeId(2)));
        rows.seek(&key(5));
        assert_eq!(read_all(&mut rows), [(5, 2, 0)]);
        assert_eq!(rows.peek_key(), None);
    }

    /// Checks that each family's order in a chunk sorts its rows by row key,
    /// and of one key the higher sequence first.
    #[test]
    fn a_chunks_orders_sort_each_family_by_key_then_higher_sequence() {
        let mut builder = MetadataStateBuilder::default();
        for (number, seq) in [(9, 1), (3, 5), (7, 2), (3, 6), (1, 4)] {
            builder.push_inode(inode(number, seq));
            builder.push_direntry_binding(crate::test_support::binding_row(
                1,
                &format!("name-{}", number % 4),
                u64::try_from(number).expect("small"),
                seq,
            ));
            builder.push_revision(RevisionRecord {
                inode_id: InodeId(number % 4),
                revision_no: RevisionNo(seq % 3),
                committed_seq: CommitSeq(seq),
                commit_id: CommitId::parse("c_tail_rows").expect("commit id"),
                committed_by: ActorId::loonfs(),
                committed_at_ms: 0,
                delta_index: 0,
                content_ref: ContentRef::blob_v1(
                    loonfs_types::ContentId::parse(format!("con_{seq:026}")).expect("content id"),
                    b"value",
                    loonfs_types::ChecksumAlgorithm::Crc64nvme,
                ),
            });
            let action = if seq % 2 == 0 {
                TombstoneRowAction::Revoke {
                    target: DeltaPosition {
                        seq: CommitSeq(1),
                        delta_index: 0,
                    },
                }
            } else {
                TombstoneRowAction::Set {
                    root_kind: InodeKind::File,
                    deleted_binding: DeletedBinding {
                        parent_inode_id: InodeId(1),
                        name_key: NameKey::parse("gone").expect("name key"),
                        display_name: loonfs_types::DisplayName::parse("gone")
                            .expect("display name"),
                    },
                }
            };
            builder.push_subtree_tombstone(SubtreeTombstoneRecord {
                root_inode_id: InodeId(number % 2),
                committed_seq: CommitSeq(seq),
                delta_index: 0,
                commit_id: CommitId::parse("c_tail_rows").expect("commit id"),
                action,
                committed_by: ActorId::loonfs(),
                committed_at_ms: 0,
            });
        }
        let tail = ProjectedWalTail::from_rows(builder.build());
        let chunk = &tail.chunks()[0];
        assert_sorted::<InodeRecord>(&chunk.rows, &chunk.orders.inodes);
        assert_sorted::<DirentryBindingRecord>(&chunk.rows, &chunk.orders.direntry_binds);
        assert_sorted::<RevisionRecord>(&chunk.rows, &chunk.orders.revisions);
        assert_sorted::<TrashRecord>(&chunk.rows, &chunk.orders.trash);
        assert_eq!(chunk.orders.heap_bytes(), 4 * 5 * size_of::<u32>());
    }

    fn assert_sorted<F: TailFamily>(state: &MetadataState, order: &[u32]) {
        let rows = F::rows(state);
        let mut positions = order.to_vec();
        positions.sort_unstable();
        assert_eq!(positions, (0..5).collect::<Vec<u32>>());
        let sort_keys: Vec<_> = order
            .iter()
            .map(|position| {
                let row = &rows[*position as usize];
                (F::row_key(row), Reverse(F::committed_seq(row)))
            })
            .collect();
        assert!(sort_keys.is_sorted(), "{sort_keys:?}");
    }
}
