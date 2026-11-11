//! Classifies a fork's net change over a range against the head of the
//! namespace being merged into, under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)),
//! and plans what the merge's commit does to each inode.

use super::memory::PlanningMemory;
use super::net_change::{fork_layout, inode_change, touched_inodes, Changed, InodeChange};
use super::operations::{
    access_update, attributes_update, binding_change, create_plan, revision_update, BindingChange,
    InodePlan,
};
use super::replay::Replay;
use super::state::{Edge, InodeState, StateReader};
use crate::authorize::{require, Absence, Authorizer};
use crate::commit::wal_size::merge_operation_bytes;
use crate::error::{CoreError, Result};
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::options::DiffOptions;
use loonfs_types::{
    AbsolutePath, AccessRight, CommitSeq, ContentLayout, ContentRef, DisplayName, InodeId,
    InodeKind, MergeConflict, MergeConflictKind, MergeConflictVersion, NameKey, MAX_PATH_DEPTH,
    ROOT_INODE_ID,
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

/// Returns the name keys of `path`'s components, which compare names as
/// path lookups do.
fn name_keys(path: &AbsolutePath) -> Vec<NameKey> {
    path.components()
        .iter()
        .map(|component| NameKey::for_display_name(&component.to_display_name()))
        .collect()
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
    /// The layout of each content reference that the plans write, from the
    /// fork's view after the fork's fold.
    pub(super) layouts: HashMap<ContentRef, ContentLayout>,
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
        if ours.visible.is_some() {
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
    async fn plan_change(
        &mut self,
        inode_id: InodeId,
        ours: &InodeState,
        theirs: &InodeState,
        changed: &Changed,
    ) -> Result<()> {
        let mut plan = InodePlan {
            inode_id,
            kind: ours.kind,
            binding: None,
            updates: Vec::new(),
        };
        let mut conflict = None;
        if theirs.visible.is_none() {
            let Some(edge) = ours.edge() else {
                return Ok(());
            };
            // A deletion that an earlier merge of this fork made, and that
            // the fork has since undone, is undone by this merge too, while
            // the deletion is recoverable here. Any other deletion conflicts.
            let deletion_seq = self.restored_deletion(inode_id).await?;
            if deletion_seq.is_none() && !self.hidden_by_restored_ancestor(inode_id, edge).await? {
                self.conflict(inode_id, MergeConflictKind::Visibility, ours, None)
                    .await?;
                return Ok(());
            }
            plan.binding =
                binding_change(&self.main.head().namespace_id, ours, theirs, deletion_seq)?;
        } else if changed.binding {
            plan.binding = binding_change(&self.main.head().namespace_id, ours, theirs, None)?;
            if let Some(binding) = &plan.binding {
                conflict = self
                    .binding_conflict(inode_id, ours, theirs, binding)
                    .await?;
            }
        }
        if ours.visible.is_some() {
            conflict = conflict.or(self.plan_updates(&mut plan, ours, theirs, changed).await?);
        }
        if let Some(kind) = conflict {
            self.conflict(inode_id, kind, ours, theirs.conflict_version())
                .await?;
        }
        self.add_plan(plan).await
    }

    /// Returns the sequence of the deletion that hides `inode_id` in the
    /// merged-into namespace, when the merge undoes the deletion. That's a
    /// deletion of the inode itself that's the fork's own (see
    /// [`Self::is_own`]), which an earlier merge of the fork made, that's
    /// still recoverable there, and that the fork undid, so the inode is
    /// visible in the fork.
    async fn restored_deletion(&mut self, inode_id: InodeId) -> Result<Option<CommitSeq>> {
        let Some(deletion) = self.theirs.view().recoverable_deletion(inode_id).await? else {
            return Ok(None);
        };
        if !self.is_own(deletion.committed_seq).await? {
            return Ok(None);
        }
        let visible_in_fork = self
            .ours
            .state(inode_id)
            .await?
            .is_some_and(|state| state.visible.is_some());
        Ok(visible_in_fork.then_some(deletion.committed_seq))
    }

    /// Returns whether `inode_id` is hidden in the merged-into namespace
    /// only by deletions of its ancestors that the merge undoes (see
    /// [`Self::restored_deletion`]), and keeps there the parent and the name that
    /// `edge` gives it in the fork. Undoing those deletions shows the inode
    /// again without a change of its own.
    async fn hidden_by_restored_ancestor(
        &mut self,
        inode_id: InodeId,
        edge: &Edge,
    ) -> Result<bool> {
        let view = self.theirs.view();
        let Some(binding) = view.current_parent_binding_for_child(inode_id).await? else {
            return Ok(false);
        };
        if binding.parent_inode_id != edge.parent_inode_id
            || binding.display_name().map(DisplayName::as_str) != Some(edge.display_name.as_str())
        {
            return Ok(false);
        }
        let mut restored = false;
        let mut ancestor = binding.parent_inode_id;
        // A binding cycle would be corrupt, and the bound ends the walk.
        for _ in 0..=MAX_PATH_DEPTH {
            if view.hiding_subtree_tombstone(ancestor).await?.is_some() {
                if self.restored_deletion(ancestor).await?.is_none() {
                    return Ok(false);
                }
                restored = true;
            }
            if ancestor == ROOT_INODE_ID {
                return Ok(restored);
            }
            let Some(binding) = view.current_parent_binding_for_child(ancestor).await? else {
                return Ok(false);
            };
            ancestor = binding.parent_inode_id;
        }
        Ok(false)
    }

    /// Returns the kind of conflict of `binding`, the move or the delete of
    /// a visible inode, when the binding or the revision that it replaces
    /// isn't the fork's own (see [`Self::is_own`]), and adds a directory
    /// with no such conflict to the subtree checks.
    async fn binding_conflict(
        &mut self,
        inode_id: InodeId,
        ours: &InodeState,
        theirs: &InodeState,
        binding: &BindingChange,
    ) -> Result<Option<MergeConflictKind>> {
        let their_edge = theirs
            .edge()
            .expect("an inode whose binding differs from a visible one's isn't the root");
        let mut conflict = None;
        if !self.is_own(their_edge.binding.seq).await? {
            conflict = Some(MergeConflictKind::Binding);
        }
        if let (BindingChange::Delete { .. }, Some(revision)) = (binding, &theirs.revision) {
            if conflict.is_none() && !self.is_own(revision.committed_seq).await? {
                conflict = Some(MergeConflictKind::Content);
            }
        }
        if conflict.is_none() && theirs.kind == InodeKind::Directory {
            let path = theirs
                .visible
                .as_ref()
                .expect("a visible inode has a path")
                .path
                .clone();
            self.subtree_checks.push(SubtreeCheck {
                directory: inode_id,
                path,
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
                Some(content_ref) => Some(self.layout(content_ref).await?.extents.len()),
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

    /// Returns the layout of `content_ref` in the fork's view, which the
    /// commit's proofs record.
    async fn layout(&mut self, content_ref: &ContentRef) -> Result<&ContentLayout> {
        if !self.layouts.contains_key(content_ref) {
            let layout = fork_layout(
                self.ours.view(),
                &self.record.source_namespace_id,
                content_ref,
            )
            .await?;
            self.layouts.insert(content_ref.clone(), layout);
        }
        Ok(&self.layouts[content_ref])
    }

    /// Returns whether the row committed at `seq` is the fork's own. A row is
    /// the fork's own when it was committed at or below the fork point, or by
    /// a merge from this fork. A replay commit reads the commit at or below
    /// the new fork's fork point through the pin.
    pub(super) async fn is_own(&mut self, seq: CommitSeq) -> Result<bool> {
        if seq <= self.fork_point_seq {
            return Ok(true);
        }
        if let Some(own) = self.own_seqs.get(&seq) {
            return Ok(*own);
        }
        let history = match &self.replay {
            Some(replay) if seq <= replay.pinned.head().seq => replay.pinned.metadata_view(),
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
    /// smallest ID so far, and adds it to a replay commit's report. `theirs`
    /// is the merged-into namespace's version that the conflict reports. An
    /// inode's later conflicts change nothing.
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
            replay.report.push(conflict.clone());
        }
        if first {
            self.first_conflict = Some(conflict);
        }
        Ok(())
    }
}
