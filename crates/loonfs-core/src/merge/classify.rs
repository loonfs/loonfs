//! Classifies a fork's net change over a range against the head of the
//! namespace being merged into, under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)),
//! and plans what the merge's commit does to each inode.

use super::memory::PlanningMemory;
use super::net_change::{fork_layout_extents, inode_change, touched_inodes, Changed, InodeChange};
use super::operations::{
    access_update, attributes_update, create_plan, delete_change, move_change, restore_change,
    revision_update, BindingChange, InodePlan,
};
use super::replay::Replay;
use super::state::{InodeState, StateReader};
use crate::authorize::{require, Absence, Authorizer};
use crate::commit::wal_size::{conflict_bytes, merge_operation_bytes};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::name_keys;
use crate::path::read::LoadedMetadataView;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::options::DiffOptions;
use loonfs_types::{
    AbsolutePath, AccessRight, CommitSeq, ContentRef, InodeId, InodeKind, MergeConflict,
    MergeConflictKind, MergeConflictVersion, NameKey,
};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// A directory that the fork deleted or moved, whose subtree in the
/// merged-into namespace a merge checks for changes that aren't the fork's
/// own, with the versions that a `subtree` conflict reports.
pub(super) struct SubtreeCheck {
    pub(super) directory: InodeId,
    /// The directory's path at the merged-into namespace's head.
    pub(super) path: AbsolutePath,
    ours: InodeState,
    theirs: Option<MergeConflictVersion>,
}

/// The reads and the results of one merge's classification.
pub(super) struct Classifier<'v, S: ObjectStore + ?Sized> {
    /// The merged-into namespace's view, whose diff the subtree check reads.
    pub(super) main: &'v LoadedMetadataView<'v, S>,
    /// The merged-into namespace's state at its head.
    pub(super) theirs: StateReader<'v, 'v, S>,
    /// The merged-into namespace's state at the fork point, which a
    /// conflict reports as its base.
    pub(super) base: StateReader<'v, 'v, S>,
    /// The fork's state at the end of the range.
    pub(super) ours: StateReader<'v, 'v, S>,
    /// The fork's WAL tail, whose pieces the merge writes for its content
    /// references.
    pub(super) fork_tail: &'v ProjectedWalTail,
    /// The fork's state at the start of the range.
    pub(super) before: StateReader<'v, 'v, S>,
    /// The subject's authority on the fork, which has to read each inode
    /// whose change the merge applies.
    pub(super) fork_authorizer: Authorizer<'v>,
    pub(super) record: &'v MergeRecord,
    pub(super) fork_point_seq: CommitSeq,
    /// The memory that the plan uses, which the running size bound checks
    /// against the merge allowance with the planned bytes.
    pub(super) memory: Arc<PlanningMemory>,
    /// The sequences above the fork point whose commits the check of a row's
    /// provenance read, and whether each commit is a merge from this fork.
    /// It grows with the rows that the merge replaces.
    pub(super) own_seqs: HashMap<CommitSeq, bool>,
    pub(super) plans: Vec<InodePlan>,
    /// An upper bound on the commit's planned WAL record so far.
    pub(super) planned_bytes: usize,
    /// The directories that the fork deleted or moved with no other
    /// conflict, which [`Self::check_subtrees`] checks after the inodes are
    /// classified.
    pub(super) subtree_checks: Vec<SubtreeCheck>,
    /// The inodes whose change conflicts, each counted once.
    pub(super) conflicted: HashSet<InodeId>,
    /// The conflicting inode with the smallest ID.
    pub(super) first_conflict: Option<MergeConflict>,
    /// What a rebase's replay commit reads and reports beyond a merge, or
    /// `None` for a merge, which reports its first conflict only.
    pub(super) replay: Option<Replay<'v, S>>,
}

impl<S: ObjectStore + ?Sized> Classifier<'_, S> {
    /// Classifies each inode that the fork's commits in the range touched,
    /// in inode ID order, and checks the merge allowance against the plan's
    /// memory after each inode.
    pub(super) async fn classify(&mut self) -> Result<()> {
        let touched =
            touched_inodes(self.ours.view(), self.record.source_after_seq, &self.memory).await?;
        for inode_id in touched {
            self.classify_inode(inode_id).await?;
            self.memory.check()?;
        }
        self.check_subtrees().await
    }

    async fn classify_inode(&mut self, inode_id: InodeId) -> Result<()> {
        let Some(InodeChange { ours, changed, .. }) = inode_change(
            &mut self.ours,
            &mut self.before,
            &self.record.source_namespace_id,
            inode_id,
        )
        .await?
        else {
            return Ok(());
        };
        if ours.is_visible() {
            require(
                &self.fork_authorizer,
                self.ours.session(),
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        }
        match self.theirs.state(inode_id).await? {
            None => self.plan_create(inode_id, &ours).await,
            Some(theirs) => self.plan_change(inode_id, &ours, &theirs, &changed).await,
        }
    }

    /// Plans the create of an inode that the merged-into namespace doesn't
    /// have, with its attributes and access.
    async fn plan_create(&mut self, inode_id: InodeId, ours: &InodeState) -> Result<()> {
        match create_plan(inode_id, ours)? {
            Some(plan) => self.add_plan(plan).await,
            None => Ok(()),
        }
    }

    /// Plans the change of an inode that the merged-into namespace has, and
    /// records a conflict when a row that the change replaces isn't the
    /// fork's own (see [`Self::is_own`]).
    ///
    /// The plan applies the inode's state at the range's end in the fork.
    /// Whether the inode and its destination are reachable after the merge
    /// is checked once all the plans are known (see
    /// [`Self::check_targets`]).
    async fn plan_change(
        &mut self,
        inode_id: InodeId,
        ours: &InodeState,
        theirs: &InodeState,
        changed: &Changed,
    ) -> Result<()> {
        let main = self.main;
        let namespace_id = &main.head().namespace_id;
        let mut plan = InodePlan {
            inode_id,
            kind: ours.kind,
            binding: None,
            updates: Vec::new(),
        };
        let mut conflict = None;
        if !ours.is_visible() {
            // The fork shows nothing of the inode at the range's end, so the
            // merge deletes it where the merged-into namespace has it bound.
            // The target check drops the delete when a deletion that the
            // merge doesn't undo hides the inode there (see
            // [`Self::check_targets`]), and the commit's order leaves out a
            // delete that a directory's recursive delete covers there.
            plan.binding = theirs
                .edge()
                .map(|from| delete_change(namespace_id, theirs, from));
            if let Some(binding) = &plan.binding {
                conflict = self
                    .binding_conflict(inode_id, ours, theirs, binding)
                    .await?;
            }
        } else if ours.edge().is_some() && theirs.edge().is_none() {
            // The merged-into namespace deleted the inode itself. A deletion
            // that an earlier merge of this fork made, and that the fork has
            // since undone, is undone by this merge too, while the deletion
            // is in the trash here. Any other deletion conflicts.
            let Some(deletion_seq) = self.restored_deletion(inode_id).await? else {
                self.conflict(inode_id, MergeConflictKind::Visibility, ours, None)
                    .await?;
                return Ok(());
            };
            plan.binding = restore_change(deletion_seq, ours)?;
            conflict = self.plan_updates(&mut plan, ours, theirs, changed).await?;
        } else {
            if changed.binding {
                plan.binding = move_change(namespace_id, ours, theirs)?;
                if let Some(binding) = &plan.binding {
                    conflict = self
                        .binding_conflict(inode_id, ours, theirs, binding)
                        .await?;
                }
            }
            conflict = conflict.or(self.plan_updates(&mut plan, ours, theirs, changed).await?);
        }
        if let Some(kind) = conflict {
            self.conflict(inode_id, kind, ours, theirs.conflict_version())
                .await?;
        }
        self.add_plan(plan).await
    }

    /// Returns the sequence of the deletion of `inode_id` itself in the
    /// merged-into namespace, when the merge can undo it: the deletion is
    /// the fork's own (see [`Self::is_own`]), which an earlier merge of the
    /// fork made, and it's still in the trash there.
    async fn restored_deletion(&mut self, inode_id: InodeId) -> Result<Option<CommitSeq>> {
        let Some(deletion) = self.theirs.view().tombstone_in_trash(inode_id).await? else {
            return Ok(None);
        };
        Ok(self
            .is_own(deletion.committed_seq)
            .await?
            .then_some(deletion.committed_seq))
    }

    /// Returns the kind of conflict of `binding`, the move or the delete of
    /// an inode whose own row is bound in the merged-into namespace, when the
    /// row or the revision that it replaces isn't the fork's own (see
    /// [`Self::is_own`]), and adds a directory with no such conflict to the
    /// subtree checks.
    async fn binding_conflict(
        &mut self,
        inode_id: InodeId,
        ours: &InodeState,
        theirs: &InodeState,
        binding: &BindingChange,
    ) -> Result<Option<MergeConflictKind>> {
        let their_edge = theirs
            .edge()
            .expect("a move or a delete replaces a bound row");
        let mut conflict = None;
        if !self.is_own(their_edge.binding.seq).await? {
            conflict = Some(MergeConflictKind::Binding);
        }
        if let (BindingChange::Delete { .. }, Some(revision)) = (binding, &theirs.revision) {
            if conflict.is_none() && !self.is_own(revision.committed_seq).await? {
                conflict = Some(MergeConflictKind::Content);
            }
        }
        // The subtree check reads the directory's path at the head. A
        // directory that a deletion hides there has none, and nothing below
        // it can change while it's hidden, so it needs no check.
        if let (None, InodeKind::Directory, Some(path)) = (conflict, theirs.kind, &theirs.path) {
            self.subtree_checks.push(SubtreeCheck {
                directory: inode_id,
                path: path.clone(),
                ours: ours.clone(),
                theirs: theirs.conflict_version(),
            });
        }
        Ok(conflict)
    }

    /// Plans the revision, attribute, and access updates of a visible inode
    /// whose components the fork changed, and returns the kind of the first
    /// conflict.
    async fn plan_updates(
        &mut self,
        plan: &mut InodePlan,
        ours: &InodeState,
        theirs: &InodeState,
        changed: &Changed,
    ) -> Result<Option<MergeConflictKind>> {
        let inode_id = plan.inode_id;
        let mut conflict = None;
        if changed.content && !ours.same_content(theirs) {
            if let (Some(row), Some(their_row)) = (&ours.revision, &theirs.revision) {
                if !self.is_own(their_row.committed_seq).await? {
                    conflict = conflict.or(Some(MergeConflictKind::Content));
                }
                plan.updates.push(revision_update(inode_id, row, their_row));
            }
        }
        if changed.attributes && !ours.same_attributes(theirs) {
            if let Some(row) = &theirs.attributes {
                if !self.is_own(row.committed_seq).await? {
                    conflict = conflict.or(Some(MergeConflictKind::Attributes));
                }
            }
            plan.updates
                .push(attributes_update(inode_id, ours, Some(theirs)));
        }
        if changed.access && !ours.same_access(theirs) {
            if let Some(row) = &theirs.access {
                if !self.is_own(row.committed_seq).await? {
                    conflict = conflict.or(Some(MergeConflictKind::Access));
                }
            }
            plan.updates
                .push(access_update(inode_id, ours, Some(theirs)));
        }
        Ok(conflict)
    }

    /// Adds an inode's plan, and adds its operations and the layouts that
    /// they record to the commit's running size bound.
    ///
    /// The running bound stops the merge with `merge_too_large` as soon as
    /// the plans so far exceed one WAL object or the merge allowance (see
    /// [`MergeAllowance::check`](super::MergeAllowance::check)), so the
    /// plans keep at most about that much memory, and a merge that admission
    /// would refuse isn't planned in full. It adds operations up as the merge
    /// commit's own bound does. The commit's bound also includes the
    /// temporary moves that ordering the operations can add, and the planner
    /// checks the ordered commit against the same bounds.
    async fn add_plan(&mut self, plan: InodePlan) -> Result<()> {
        let operations = plan.operations();
        if operations.is_empty() {
            return Ok(());
        }
        for operation in &operations {
            let layout_extents = match operation.content_ref() {
                Some(content_ref) => Some(self.layout_extents(content_ref).await?),
                None => None,
            };
            self.planned_bytes = self
                .planned_bytes
                .saturating_add(merge_operation_bytes(operation, layout_extents));
        }
        self.memory.check_planned(self.planned_bytes)?;
        self.plans.push(plan);
        Ok(())
    }

    /// Returns the number of extents in the layout that the commit's proof
    /// of `content_ref` records (see [`fork_layout_extents`]).
    async fn layout_extents(&self, content_ref: &ContentRef) -> Result<usize> {
        fork_layout_extents(
            self.ours.view(),
            self.fork_tail,
            &self.main.head().pool_id,
            content_ref,
        )
        .await
    }

    /// Returns whether the row committed at `seq` is the fork's own. A row is
    /// the fork's own when it was committed at or below the fork point, or by
    /// a merge from this fork. A replay commit reads the commit at or below
    /// the new fork's fork point through the snapshot.
    pub(super) async fn is_own(&mut self, seq: CommitSeq) -> Result<bool> {
        if seq <= self.fork_point_seq {
            return Ok(true);
        }
        if let Some(own) = self.own_seqs.get(&seq) {
            return Ok(*own);
        }
        let history = match &self.replay {
            Some(replay) if seq <= replay.snapshot.head().seq => replay.snapshot.metadata_view(),
            _ => self.theirs.view(),
        };
        let commit = history.commit_at_seq(seq).await?.ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "a row was committed at sequence `{seq}`, where the namespace keeps no commit"
            ))
        })?;
        let own = commit
            .merge
            .as_ref()
            .is_some_and(|merge| merge.source_namespace_id == self.record.source_namespace_id);
        self.own_seqs.insert(seq, own);
        Ok(own)
    }

    /// Records a `subtree` conflict for each directory that the fork deleted
    /// or moved when something below the directory changed in the
    /// merged-into namespace after the fork point through a row that isn't
    /// the fork's own (see [`Self::is_own`]).
    ///
    /// One diff over the root for each part of the history after the fork
    /// point that [`Self::history_parts`] lists finds the changes. An entry
    /// counts for each checked directory that its path at either end of the
    /// part is at or below, other than its own, and an entry below none of
    /// them is skipped. Each diff's pages advance through the commits of its
    /// part, so each loop ends at the end of the part.
    async fn check_subtrees(&mut self) -> Result<()> {
        if self.subtree_checks.is_empty() {
            return Ok(());
        }
        let checks = std::mem::take(&mut self.subtree_checks);
        let mut changed = vec![false; checks.len()];
        for part in self.history_parts(&checks).await? {
            let by_path: HashMap<Vec<NameKey>, usize> = part
                .paths
                .iter()
                .map(|(index, path)| (name_keys(path), *index))
                .collect();
            let mut options = DiffOptions {
                path: AbsolutePath::root(),
                to_seq: Some(part.to_seq),
                ..DiffOptions::default()
            };
            loop {
                let page = crate::diff::diff_page(part.view, part.from_seq, &options).await?;
                for entry in &page.response.entries {
                    let below: BTreeSet<usize> =
                        [entry.path.as_ref(), entry.previous_path.as_ref()]
                            .into_iter()
                            .flatten()
                            .flat_map(|path| {
                                let keys = name_keys(path);
                                (0..=keys.len())
                                    .filter_map(|length| by_path.get(&keys[..length]).copied())
                                    .collect::<Vec<_>>()
                            })
                            .filter(|index| {
                                !changed[*index] && checks[*index].directory != entry.inode_id
                            })
                            .collect();
                    if !below.is_empty() && self.has_foreign_row(entry.inode_id).await? {
                        for index in below {
                            changed[index] = true;
                        }
                    }
                }
                match page.response.next_after_seq {
                    Some(after_seq) => options.after_seq = Some(after_seq),
                    None => break,
                }
            }
        }
        for (check, changed) in checks.into_iter().zip(changed) {
            if changed {
                self.conflict(
                    check.directory,
                    MergeConflictKind::Subtree,
                    &check.ours,
                    check.theirs,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Returns whether one of the inode's newest rows in the merged-into
    /// namespace isn't the fork's own (see [`Self::is_own`]): its binding, its
    /// deletion, or its revision,
    /// attribute, or access row.
    async fn has_foreign_row(&mut self, inode_id: InodeId) -> Result<bool> {
        let Some(state) = self.theirs.state(inode_id).await? else {
            return Ok(false);
        };
        let deletion = self
            .theirs
            .view()
            .newest_subtree_tombstone(inode_id)
            .await?;
        let seqs = [
            state.edge().map(|edge| edge.binding.seq),
            deletion.map(|row| row.committed_seq),
            state.revision.as_ref().map(|row| row.committed_seq),
            state.attributes.as_ref().map(|row| row.committed_seq),
            state.access.as_ref().map(|row| row.committed_seq),
        ];
        for seq in seqs.into_iter().flatten() {
            if !self.is_own(seq).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Counts a conflict, keeps it as the first when its inode has the
    /// smallest ID so far, and adds it to a replay commit's report, with its
    /// bytes checked against the allowance as a plan's are, because the
    /// replay commit's merge record stores the report. `theirs` is the
    /// merged-into namespace's version
    /// that the conflict reports. An inode's later conflicts change nothing.
    pub(super) async fn conflict(
        &mut self,
        inode_id: InodeId,
        kind: MergeConflictKind,
        ours: &InodeState,
        theirs: Option<MergeConflictVersion>,
    ) -> Result<()> {
        if !self.conflicted.insert(inode_id) {
            return Ok(());
        }
        let first = self
            .first_conflict
            .as_ref()
            .is_none_or(|first| inode_id < first.inode_id);
        if !first && self.replay.is_none() {
            return Ok(());
        }
        let base = self
            .base
            .state(inode_id)
            .await?
            .and_then(|state| state.conflict_version());
        let conflict = MergeConflict {
            inode_id,
            kind,
            base,
            ours: ours.conflict_version(),
            theirs,
        };
        if let Some(replay) = &mut self.replay {
            self.planned_bytes = self
                .planned_bytes
                .saturating_add(conflict_bytes(&conflict, replay.report.is_empty()));
            self.memory.check_planned(self.planned_bytes)?;
            replay.report.push(conflict.clone());
        }
        if first {
            self.first_conflict = Some(conflict);
        }
        Ok(())
    }
}
