//! A rebase's replay commit: the merge of a fork into a new fork of the
//! fork's source, classified against the new fork's head with the source's
//! history read through the new fork's snapshot, which skips the changes that
//! conflict and reports them
//! ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).

use super::classify::{Classifier, SubtreeCheck};
use super::operations::BindingChange;
use super::state::StateReader;
use super::{
    ensure_range_start_retained, fork_point, merge_start, range_start, rebase_basis,
    MergeAllowance, MergeRequest,
};
use crate::commit::engine::CommitCandidate;
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, Result};
use crate::metadata::AncestorWalk;
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ForkBasis;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{AbsolutePath, CommitSeq, InodeId, InodeKind, MergeConflict, MergeConflictKind};
use std::collections::{BTreeSet, HashMap, HashSet};

/// A replay commit planned against the head of a rebase's new fork, and the
/// conflict report of the changes that it leaves out.
#[derive(Debug)]
pub struct ReplayPlan {
    candidate: CommitCandidate,
    record: MergeRecord,
    conflicts: Vec<MergeConflict>,
}

impl ReplayPlan {
    /// Returns the replay commit's merge record, with the range of the
    /// fork's commits that the replay commit examines.
    pub fn record(&self) -> &MergeRecord {
        &self.record
    }

    /// Returns the conflict report, with one entry for each inode whose
    /// change the replay commit leaves out.
    pub fn conflicts(&self) -> &[MergeConflict] {
        &self.conflicts
    }

    /// Returns the replay commit.
    pub fn into_candidate(self) -> CommitCandidate {
        self.candidate
    }
}

/// What a replay commit's classification has beyond a merge's: the source
/// through the new fork's snapshot, and the conflict report.
pub(super) struct Replay<'v, S: ObjectStore + ?Sized> {
    /// The namespace that the rebase forked, read through the new fork's snapshot,
    /// whose history at or below the new fork's fork point the
    /// classification reads.
    pub(super) snapshot: &'v LoadedMetadataView<'v, S>,
    /// Every conflict, in the order that the classification finds them.
    pub(super) report: Vec<MergeConflict>,
}

/// Where a replay commit's range starts, and the fork basis of the new fork
/// that the range start was checked against.
struct ReplayRange<'a> {
    basis: &'a ForkBasis,
    fork_point_seq: CommitSeq,
    source_after_seq: CommitSeq,
}

/// One part of the history after the fork point that the subtree check
/// reads with one diff: the merged-into namespace's history for a merge, and
/// for a replay commit, the source's part through the new fork's snapshot or the
/// new fork's own part above its fork point.
pub(super) struct HistoryPart<'v, S: ObjectStore + ?Sized> {
    pub(super) view: &'v LoadedMetadataView<'v, S>,
    pub(super) from_seq: CommitSeq,
    pub(super) to_seq: CommitSeq,
    /// Each checked directory's path at the end of the part, with the
    /// directory's index in the checks. A directory with no path there is
    /// left out.
    pub(super) paths: Vec<(usize, AbsolutePath)>,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns the fork point of `fork`, read at `fork_context`, after it
    /// checks that `fork` is a fork of this namespace, read at `context`, that
    /// the fork point is at or above this namespace's retention floor, and
    /// that the start of the fork's unmerged range is at or above the fork's
    /// own retention floor. A rebase makes these checks before it creates its
    /// new fork.
    pub async fn fork_point_for_rebase<F>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
    ) -> Result<CommitSeq> {
        let main = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        let fork_point_seq = fork_point(fork_view.head(), &main.head().namespace_id)?;
        let start = merge_start(
            &main.metadata_view(),
            main.head(),
            &fork_view.head().namespace_id,
            fork_point_seq,
        )
        .await??;
        ensure_range_start_retained(&fork_view, start.after_seq)?;
        Ok(fork_point_seq)
    }

    /// Checks that this namespace, read at `context`, is a rebase's new fork
    /// for `fork`, read at `fork_context`, and that the replay commit's range
    /// starts at or above both floors, as [`Self::plan_replay`] does, and
    /// returns this namespace's fork basis, which identifies the snapshot that
    /// the plan reads the source through. A caller runs the checks before it
    /// folds the fork, so a request that would fail them folds nothing.
    pub async fn check_replay<F>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
        request: &MergeRequest,
    ) -> Result<ForkBasis> {
        let new_fork = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        let range = replay_range_start(&new_fork, &fork_view, request).await?;
        Ok(range.basis.clone())
    }

    /// Plans a replay commit of `fork`, read at `fork_context`, into this
    /// namespace, a rebase's new fork read at `context`. `source`, read at
    /// `source_context`, is the namespace that the rebase forked, read
    /// through this namespace's fork snapshot, and a context at another manifest
    /// returns `namespace_corrupt`, as a context of another namespace does
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).
    ///
    /// The plan takes the range's start from this namespace's own replay
    /// rows or `resume_after_seq`, and checks it against both floors, as
    /// [`Self::plan_merge`] does for a merge. It classifies the fork's net
    /// change against this namespace's head, which supplies the current
    /// values, the preconditions, the name and visibility checks, and the
    /// order. It reads whether a row at or below this namespace's fork point
    /// is the fork's own, the base version, and the source's part of a
    /// subtree check through the snapshot, and the rest from this namespace. It
    /// fails with `merge_too_large` when the commit wouldn't fit one WAL
    /// object or `allowance`, skips each conflicting change and each change
    /// that depends on a skipped one, and reports them. Like a merge's
    /// commit, a replay commit that deletes or moves a directory, or moves an
    /// inode through a temporary name, expects this namespace's head,
    /// because another writer can commit into a rebase's new fork while the
    /// rebase runs.
    #[allow(
        clippy::too_many_arguments,
        reason = "a replay reads three namespaces, each at its own context, as a merge reads two"
    )]
    pub async fn plan_replay<F, P>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
        source: &NamespaceEngine<S, P>,
        source_context: &RuntimeReadContext,
        request: &MergeRequest,
        prepared_at_ms: u64,
        allowance: MergeAllowance,
    ) -> Result<ReplayPlan> {
        let new_fork = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        let ReplayRange {
            basis,
            fork_point_seq,
            source_after_seq,
        } = replay_range_start(&new_fork, &fork_view, request).await?;
        let snapshot = source.namespace.load_metadata_view(source_context).await?;
        if snapshot.head().namespace_id != basis.manifest.owner_namespace_id
            || snapshot.anchor.manifest_no != basis.manifest.manifest_no
        {
            return Err(CoreError::NamespaceCorrupt(format!(
                "a replay commit reads `{}` at manifest `{}` through the new fork's snapshot, but \
                 its context is at manifest `{}` of `{}`",
                basis.manifest.owner_namespace_id,
                basis.manifest.manifest_no,
                snapshot.anchor.manifest_no,
                snapshot.head().namespace_id
            )));
        }
        let planned = self
            .plan_merge_commit(
                &new_fork,
                Some(&snapshot),
                fork,
                &fork_view,
                (fork_point_seq, source_after_seq),
                request,
                prepared_at_ms,
                &allowance,
            )
            .await?;
        Ok(ReplayPlan {
            candidate: planned.candidate,
            record: planned.record,
            conflicts: planned.report.unwrap_or_default(),
        })
    }
}

/// Returns the fork point and the start of a replay commit's range into
/// `new_fork`, which has to be a rebase's new fork for `fork_view`'s
/// namespace, with the new fork's basis. The fork point is the sequence of
/// the namespace that the rebase forked, and the range starts as
/// [`range_start`] says, with the new fork's own replay rows or
/// `resume_after_seq`.
async fn replay_range_start<'a, S: ObjectStore + ?Sized>(
    new_fork: &'a LoadedMetadataView<'_, S>,
    fork_view: &LoadedMetadataView<'_, S>,
    request: &MergeRequest,
) -> Result<ReplayRange<'a>> {
    let fork_id = &fork_view.head().namespace_id;
    let Some((basis, _)) = rebase_basis(new_fork.head(), fork_id) else {
        return Err(CoreError::InvalidMerge {
            param: "source_namespace_id",
            message: format!(
                "namespace `{}` isn't a rebase of fork `{fork_id}`",
                new_fork.head().namespace_id
            ),
        });
    };
    let (fork_point_seq, source_after_seq) = range_start(
        new_fork,
        fork_view,
        request,
        &basis.manifest.owner_namespace_id,
    )
    .await?;
    Ok(ReplayRange {
        basis,
        fork_point_seq,
        source_after_seq,
    })
}

impl<'v, S: ObjectStore + ?Sized> Classifier<'v, S> {
    /// Returns the parts of the history after the fork point that the
    /// subtree check of `checks` reads: the merged-into namespace's history
    /// up to its head for a merge, and for a replay commit, the source's
    /// history through the snapshot up to the new fork's fork point, at each
    /// directory's path there, and the new fork's own history above it, at
    /// each directory's current path.
    pub(super) async fn history_parts(
        &self,
        checks: &[SubtreeCheck],
    ) -> Result<Vec<HistoryPart<'v, S>>> {
        let main = self.main;
        let current_paths = || {
            checks
                .iter()
                .enumerate()
                .map(|(index, check)| (index, check.path.clone()))
                .collect()
        };
        let Some(snapshot) = self.replay.as_ref().map(|replay| replay.snapshot) else {
            return Ok(vec![HistoryPart {
                view: main,
                from_seq: self.fork_point_seq,
                to_seq: main.head().seq,
                paths: current_paths(),
            }]);
        };
        let own_fork_point_seq = snapshot.head().seq;
        let mut parts = Vec::with_capacity(2);
        // A directory that an earlier replay commit created has no path at
        // the new fork's fork point, and nothing below it changed before.
        let mut snapshot_head = StateReader::new(snapshot.metadata_view(), &self.memory);
        let mut snapshot_paths = Vec::with_capacity(checks.len());
        for (index, check) in checks.iter().enumerate() {
            let path = snapshot_head
                .state(check.directory)
                .await?
                .and_then(|state| state.visible)
                .map(|visible| visible.path);
            snapshot_paths.extend(path.map(|path| (index, path)));
        }
        if !snapshot_paths.is_empty() {
            parts.push(HistoryPart {
                view: snapshot,
                from_seq: self.fork_point_seq,
                to_seq: own_fork_point_seq,
                paths: snapshot_paths,
            });
        }
        if main.head().seq > own_fork_point_seq {
            parts.push(HistoryPart {
                view: main,
                from_seq: own_fork_point_seq,
                to_seq: main.head().seq,
                paths: current_paths(),
            });
        }
        Ok(parts)
    }

    /// Skips the plans of the conflicting inodes, and records a `dependency`
    /// conflict for each change that depends on a skipped change, in the
    /// order that it finds them:
    ///
    /// - a create, a restore, or a move into a directory whose create or
    ///   restore it skips, as the merge's target check treats a restored
    ///   directory like a created one,
    /// - the delete of a directory that still contains an inode whose move
    ///   out of it was skipped, which the delete would delete in the new
    ///   fork, and
    /// - a create or a move that takes a name that a skipped change would
    ///   have freed, which is a `name` conflict.
    ///
    /// Each pass records at least one conflict, so the loop runs at most
    /// once for each plan. A skipped inode's ancestors are read once, when
    /// it's first skipped, because a later pass only skips more deletes.
    pub(super) async fn skip_conflicts(
        &mut self,
        holders: &HashMap<InodeId, InodeId>,
    ) -> Result<()> {
        let created_directories: HashSet<_> = self
            .plans
            .iter()
            .filter(|plan| {
                plan.kind == InodeKind::Directory
                    && matches!(
                        plan.binding,
                        Some(BindingChange::Create { .. } | BindingChange::RestoreDeleted { .. })
                    )
            })
            .map(|plan| plan.inode_id)
            .collect();
        let deleted_directories: HashSet<_> = self
            .plans
            .iter()
            .filter(|plan| {
                plan.kind == InodeKind::Directory
                    && matches!(plan.binding, Some(BindingChange::Delete { .. }))
            })
            .map(|plan| plan.inode_id)
            .collect();
        let mut examined = HashSet::new();
        loop {
            let mut recorded = self.block_takers(holders).await?;
            recorded += self
                .skip_creates_in_skipped_directories(&created_directories)
                .await?;
            recorded += self
                .skip_deletes_of_skipped_moves(&deleted_directories, &mut examined)
                .await?;
            if recorded == 0 {
                break;
            }
        }
        let conflicted = &self.conflicted;
        self.plans
            .retain(|plan| !conflicted.contains(&plan.inode_id));
        Ok(())
    }

    /// Records a `dependency` conflict for each create, restore, or move
    /// into a directory in `created_directories` whose create or restore is
    /// skipped, and returns how many it recorded. `theirs` is the directory's
    /// version in the fork.
    async fn skip_creates_in_skipped_directories(
        &mut self,
        created_directories: &HashSet<InodeId>,
    ) -> Result<usize> {
        let mut recorded = 0;
        for index in 0..self.plans.len() {
            let plan = &self.plans[index];
            let inode_id = plan.inode_id;
            let Some((parent_inode_id, _)) =
                plan.binding.as_ref().and_then(BindingChange::destination)
            else {
                continue;
            };
            if self.conflicted.contains(&inode_id)
                || !created_directories.contains(&parent_inode_id)
                || !self.conflicted.contains(&parent_inode_id)
            {
                continue;
            }
            let theirs = self
                .ours
                .state(parent_inode_id)
                .await?
                .and_then(|state| state.conflict_version());
            self.target_conflict(inode_id, MergeConflictKind::Dependency, theirs)
                .await?;
            recorded += 1;
        }
        Ok(recorded)
    }

    /// Records a `dependency` conflict for each delete of a directory in
    /// `deleted_directories` that, in the new fork, contains a skipped inode
    /// that's visible in the fork, and returns how many it recorded.
    /// `theirs` is the skipped inode's version in the fork.
    ///
    /// The fork deleted the directory, so a skipped inode that's visible in
    /// the fork moved out of it there, and the directory's recursive delete
    /// would delete the inode in the new fork. `examined` lists the skipped
    /// inodes whose ancestors an earlier pass read.
    async fn skip_deletes_of_skipped_moves(
        &mut self,
        deleted_directories: &HashSet<InodeId>,
        examined: &mut HashSet<InodeId>,
    ) -> Result<usize> {
        let mut recorded = 0;
        let skipped: BTreeSet<_> = self.conflicted.difference(examined).copied().collect();
        for inode_id in skipped {
            examined.insert(inode_id);
            let Some(ours) = self
                .ours
                .state(inode_id)
                .await?
                .and_then(|state| state.conflict_version())
            else {
                continue;
            };
            let mut walk = AncestorWalk::new(inode_id);
            while let Some(ancestor) = walk.next(self.theirs.session()).await? {
                if deleted_directories.contains(&ancestor) && !self.conflicted.contains(&ancestor) {
                    self.target_conflict(
                        ancestor,
                        MergeConflictKind::Dependency,
                        Some(ours.clone()),
                    )
                    .await?;
                    recorded += 1;
                }
            }
        }
        Ok(recorded)
    }
}
