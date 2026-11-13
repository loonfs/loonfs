//! A merge's size bounds, and the split hint of a merge that's too large for
//! one of them
//! ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#the-split-hint)).

use super::memory::PlanningMemory;
use super::net_change::{fork_layout, inode_change, touched_inodes, InodeChange};
use super::operations::{
    access_update, attributes_update, binding_change, create_plan, revision_update, InodePlan,
};
use super::state::StateReader;
use super::MergeAllowance;
use crate::commit::wal_size::merge_operation_bytes;
use crate::error::{CoreError, MergeBound, Result};
use crate::metadata::MetadataView;
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitSeq, InodeId, NamespaceId};

/// How many times the split hint bisects the fork's commit sequences. With
/// the probe of the first unmerged commit alone, the hint measures at most
/// four ranges, each with one computation of the fork's net change.
const SPLIT_BISECTIONS: usize = 3;

/// Returns `result` with the split hint added to its refusal for one WAL
/// object or `allowance`, when the refusal has none.
///
/// `result` is a size check of a merge of the fork's commits after the
/// first sequence of `range` through the second (see [`split_hint`]).
pub(super) async fn with_split_hint<T, S: ObjectStore + ?Sized>(
    result: Result<T>,
    fork_view: &LoadedMetadataView<'_, S>,
    (after_seq, through_seq): (CommitSeq, CommitSeq),
    fields_bytes: usize,
    allowance: &MergeAllowance,
) -> Result<T> {
    match result {
        Err(CoreError::MergeTooLarge {
            bound: bound @ (MergeBound::WalObject | MergeBound::MergeAllowance),
            estimated_bytes,
            max_bytes,
            split_at_seq: None,
        }) => Err(CoreError::MergeTooLarge {
            bound,
            estimated_bytes,
            max_bytes,
            split_at_seq: split_hint(
                fork_view,
                after_seq,
                through_seq,
                fields_bytes,
                &allowance.unshared_copy(),
            )
            .await?,
        }),
        result => result,
    }
}

/// Returns the split hint of a merge of the fork's commits after `after_seq`
/// through `through_seq` that doesn't fit one WAL object or `allowance`: the
/// largest fork sequence that a probe showed a merge through it to fit (see
/// [`net_change_fits`]), or `None` when no probe fit. `fields_bytes` bounds
/// the merge commit's fields other than its operations.
async fn split_hint<S: ObjectStore + ?Sized>(
    fork_view: &LoadedMetadataView<'_, S>,
    after_seq: CommitSeq,
    through_seq: CommitSeq,
    fields_bytes: usize,
    allowance: &MergeAllowance,
) -> Result<Option<CommitSeq>> {
    let mut search = SplitSearch::new(after_seq, through_seq);
    // The search ends after at most four probes.
    while let Some(probe_seq) = search.next_probe() {
        let fits =
            net_change_fits(fork_view, after_seq, probe_seq, fields_bytes, allowance).await?;
        search.record(probe_seq, fits);
    }
    Ok(search.hint())
}

/// The search for a split hint over the fork's commits after `after_seq`.
///
/// The search bisects the fork's commit sequences [`SPLIT_BISECTIONS`]
/// times, between the largest sequence known to fit, which starts at the
/// range's start, and the smallest sequence known not to, which starts at
/// the refused range's end. When no bisection probe fit, it probes the
/// first commit after `after_seq` alone, so that a range shorter than the
/// smallest bisection probe still gets a hint, unless a bisection probe
/// already showed that commit not to fit. So it probes at most four ranges.
struct SplitSearch {
    after_seq: u64,
    fitting: u64,
    too_large: u64,
    probes: usize,
}

impl SplitSearch {
    fn new(after_seq: CommitSeq, through_seq: CommitSeq) -> Self {
        assert!(
            after_seq < through_seq,
            "a merge's range should end after the sequence that it starts after"
        );
        Self {
            after_seq: after_seq.0,
            fitting: after_seq.0,
            too_large: through_seq.0,
            probes: 0,
        }
    }

    /// Returns the sequence that the next probe's range ends at, or `None`
    /// when the search is over.
    fn next_probe(&self) -> Option<CommitSeq> {
        let probe = if self.probes < SPLIT_BISECTIONS {
            self.fitting + (self.too_large - self.fitting) / 2
        } else if self.probes == SPLIT_BISECTIONS && self.fitting == self.after_seq {
            self.after_seq + 1
        } else {
            return None;
        };
        (probe > self.fitting && probe < self.too_large).then_some(CommitSeq(probe))
    }

    /// Records whether the range that ends at `probe_seq` fits.
    fn record(&mut self, probe_seq: CommitSeq, fits: bool) {
        if fits {
            self.fitting = probe_seq.0;
        } else {
            self.too_large = probe_seq.0;
        }
        self.probes += 1;
    }

    /// Returns the largest sequence that a probe showed to fit.
    fn hint(&self) -> Option<CommitSeq> {
        (self.fitting > self.after_seq).then_some(CommitSeq(self.fitting))
    }
}

/// Returns whether the fork's net change over its commits after `after_seq`
/// through `through_seq` fits one WAL object and `allowance`, measured from
/// the fork alone as a merge's plan measures its planned bytes and its
/// working bytes (see [`MergeAllowance::check`]). `fields_bytes` bounds the
/// commit's fields other than its operations, as it does for the merge.
///
/// The measure is the plan of a merge of the net change into a namespace
/// whose rows are the fork's own at the range's start. A merge into another
/// namespace leaves out each row whose value is the same as that namespace's
/// current value, so the measure bounds the merge's planned bytes. It stops
/// at the first inode that takes it over a bound, and it keeps one inode's
/// plan at a time.
async fn net_change_fits<S: ObjectStore + ?Sized>(
    fork_view: &LoadedMetadataView<'_, S>,
    after_seq: CommitSeq,
    through_seq: CommitSeq,
    fields_bytes: usize,
    allowance: &MergeAllowance,
) -> Result<bool> {
    match measure_net_change(fork_view, after_seq, through_seq, fields_bytes, allowance).await {
        Ok(()) => Ok(true),
        Err(CoreError::MergeTooLarge {
            bound: MergeBound::WalObject | MergeBound::MergeAllowance,
            ..
        }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Measures the fork's net change for [`net_change_fits`], and returns
/// `merge_too_large` at the first inode that takes the measure over a bound.
async fn measure_net_change<S: ObjectStore + ?Sized>(
    fork_view: &LoadedMetadataView<'_, S>,
    after_seq: CommitSeq,
    through_seq: CommitSeq,
    fields_bytes: usize,
    allowance: &MergeAllowance,
) -> Result<()> {
    let fork = &fork_view.head().namespace_id;
    let end = fork_view.metadata_view_at(through_seq);
    let start = fork_view.metadata_view_at(after_seq);
    let memory = PlanningMemory::new(allowance.clone());
    let (mut ours, mut before) = (
        StateReader::new(end, &memory),
        StateReader::new(start, &memory),
    );
    let mut planned_bytes = fields_bytes;
    for inode_id in touched_inodes(end, after_seq, &memory).await? {
        let plan = match inode_change(&mut ours, &mut before, fork, inode_id).await? {
            Some(change) => net_change_plan(start, fork, inode_id, &change).await?,
            None => None,
        };
        for operation in plan.iter().flat_map(InodePlan::operations) {
            let layout_extents = match operation.content_ref() {
                Some(content_ref) => Some(fork_layout(end, fork, content_ref).await?.extents.len()),
                None => None,
            };
            planned_bytes =
                planned_bytes.saturating_add(merge_operation_bytes(&operation, layout_extents));
        }
        memory.check_planned(planned_bytes)?;
    }
    Ok(())
}

/// Returns the plan of a merge of the fork's change to one inode into a
/// namespace whose rows are the fork's own at the range's start, which
/// `start` reads: the create of an inode that the fork created in the range,
/// or the inode's move, delete, or restore, which the planner's
/// [`binding_change`] chooses, then its revision, attribute, and access
/// updates. The plan's preconditions are the rows of `fork` at the range's
/// start.
async fn net_change_plan<S: ObjectStore + ?Sized>(
    start: MetadataView<'_, '_, S>,
    fork: &NamespaceId,
    inode_id: InodeId,
    change: &InodeChange,
) -> Result<Option<InodePlan>> {
    let InodeChange {
        ours,
        before,
        changed,
    } = change;
    let Some(before) = before else {
        return create_plan(inode_id, ours);
    };
    // A restore reverses a deletion of the inode itself that hid it at the
    // start. An inode that only an ancestor's deletion hid comes back with
    // the ancestor's restore, with no change of its own.
    let deletion_seq = match (&before.visible, ours.edge()) {
        (None, Some(_)) => start
            .hiding_subtree_tombstone(inode_id)
            .await?
            .map(|deletion| deletion.committed_seq),
        _ => None,
    };
    let binding = binding_change(fork, ours, before, deletion_seq)?;
    let mut updates = Vec::new();
    if ours.visible.is_some() {
        if let (true, Some(row), Some(start_row)) =
            (changed.content, &ours.revision, &before.revision)
        {
            updates.push(revision_update(inode_id, row, start_row));
        }
        if changed.attributes {
            updates.push(attributes_update(inode_id, ours, Some(before)));
        }
        if changed.access {
            updates.push(access_update(inode_id, ours, Some(before)));
        }
    }
    Ok(Some(InodePlan {
        inode_id,
        kind: ours.kind,
        binding,
        updates,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that the split hint is the largest sequence that a probe showed
    /// to fit, that the search measures the first commit alone only when no
    /// bisection probe fit and no probe showed that commit too large, and
    /// that it measures at most four ranges.
    #[test]
    fn the_split_hint_is_the_largest_fitting_probe_of_at_most_four() {
        // The range's start and end, the last sequence through which the
        // fork's change fits, the hint, and the sequences that the search
        // measures, in order.
        let cases = [
            (100, 200, 199, Some(187), &[150, 175, 187][..]),
            (100, 200, 160, Some(150), &[150, 175, 162]),
            (100, 200, 105, Some(101), &[150, 125, 112, 101]),
            (100, 200, 100, None, &[150, 125, 112, 101]),
            (100, 101, 100, None, &[]),
            (100, 102, 101, Some(101), &[101]),
            (100, 102, 100, None, &[101]),
            (100, 104, 100, None, &[102, 101]),
        ];
        for (after_seq, through_seq, fits_through, hint, probes) in cases {
            let mut search = SplitSearch::new(CommitSeq(after_seq), CommitSeq(through_seq));
            let mut measured = Vec::new();
            while let Some(probe_seq) = search.next_probe() {
                measured.push(probe_seq.0);
                search.record(probe_seq, probe_seq.0 <= fits_through);
            }
            let case = (after_seq, through_seq, fits_through);
            assert_eq!(search.hint(), hint.map(CommitSeq), "{case:?}");
            assert_eq!(measured, probes, "{case:?}");
            assert!(measured.len() <= 4, "{case:?}");
        }
    }
}
