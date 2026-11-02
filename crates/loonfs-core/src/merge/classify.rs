//! Classifies a fork's net change over a range against the head of the
//! namespace being merged into, under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)),
//! and plans what the merge's commit does to each inode.

use super::operations::{access_update, attributes_update, display_name, BindingChange, InodePlan};
use super::replay::Replay;
use super::state::{Edge, InodeState, StateReader};
use super::MergeAllowance;
use crate::authorize::{require, Absence, Authorizer};
use crate::commit::wal_size::{
    merge_operation_bytes, MAX_MERGED_INODES, MIN_MERGE_OPERATION_BYTES,
};
use crate::error::{CoreError, MergeBound, Result};
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{MergeRecord, MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use loonfs_types::options::DiffOptions;
use loonfs_types::{
    AbsolutePath, AccessRight, ChangeSeq, ContentLayout, ContentRef, DisplayName,
    FilesystemOperation, InodeId, InodeKind, MergeConflict, MergeConflictKind,
    MergeConflictVersion, NameKey, MAX_PATH_DEPTH, ROOT_INODE_ID,
};
use std::collections::{BTreeSet, HashMap, HashSet};

/// The most of the fork's commits that one read of a merge's range returns.
/// A page includes each commit's deltas, so it stays at tens of commits.
const MERGE_COMMITS_PAGE: usize = 64;

/// Which components of an inode's state the fork changed in the range.
struct Changed {
    binding: bool,
    content: bool,
    attributes: bool,
    access: bool,
}

impl Changed {
    /// Compares the inode's state at the range's start, or `None` when the
    /// fork created it in the range, with its state at the end.
    fn between(before: Option<&InodeState>, after: &InodeState) -> Option<Self> {
        let changed = match before {
            None => Self {
                binding: true,
                content: true,
                attributes: true,
                access: true,
            },
            Some(before) => Self {
                binding: !before.same_binding(after),
                content: !before.same_content(after),
                attributes: !before.same_attributes(after),
                access: !before.same_access(after),
            },
        };
        (changed.binding || changed.content || changed.attributes || changed.access)
            .then_some(changed)
    }
}

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
    pub(super) fork_point_seq: ChangeSeq,
    /// The publisher's merge allowance, which the running size bound checks
    /// too.
    pub(super) allowance: MergeAllowance,
    /// The sequences above the fork point whose commits the check of a row's
    /// provenance read, and whether each commit is a merge from this fork.
    /// It grows with the rows that the merge replaces.
    pub(super) own_seqs: HashMap<ChangeSeq, bool>,
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
    /// in inode ID order.
    pub(super) async fn classify(&mut self) -> Result<()> {
        for inode_id in self.touched_inodes().await? {
            self.classify_inode(inode_id).await?;
        }
        self.check_subtrees().await
    }

    /// Lists the inodes that the fork's commits in the range touched.
    ///
    /// [`MAX_MERGED_INODES`] bounds the set. A range that touched more inodes
    /// than one WAL object could contain operations for fails with
    /// `merge_too_large`, even if some of those inodes drop out of the net
    /// change. The error's `split_at_seq` is the last commit before the one
    /// that crossed the bound, so the caller can merge the range through
    /// that commit, unless the first commit of the range crossed it.
    async fn touched_inodes(&self) -> Result<BTreeSet<InodeId>> {
        let view = self.ours.view();
        let mut touched = BTreeSet::new();
        let mut after_seq = self.record.source_after_seq;
        // Each pass reads the next page of the range's commits, and the view
        // ends at the range's end, so the loop ends when a page is empty.
        loop {
            let page = view.commits_after(after_seq, MERGE_COMMITS_PAGE).await?;
            if page.is_empty() {
                return Ok(touched);
            }
            for commit in &page {
                touched.extend(crate::diff::touched_inodes(&commit.deltas));
                if touched.len() > MAX_MERGED_INODES {
                    return Err(CoreError::MergeTooLarge {
                        bound: MergeBound::TouchedInodes,
                        estimated_bytes: touched.len().saturating_mul(MIN_MERGE_OPERATION_BYTES),
                        max_bytes: MAX_WAL_OBJECT_BYTES,
                        split_at_seq: (after_seq > self.record.source_after_seq)
                            .then_some(after_seq),
                    });
                }
                after_seq = commit.committed_seq;
            }
        }
    }

    async fn classify_inode(&mut self, inode_id: InodeId) -> Result<()> {
        let ours = self.ours.state(inode_id).await?.ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "a commit of fork `{}` touched inode `{inode_id}`, which the fork doesn't have",
                self.record.source_namespace_id
            ))
        })?;
        let before = self.before.state(inode_id).await?;
        let Some(changed) = Changed::between(before.as_ref(), &ours) else {
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
        let Some(edge) = ours.edge() else {
            return Ok(());
        };
        let content_ref = ours.revision.as_ref().map(|row| row.content_ref.clone());
        let plan = InodePlan {
            inode_id,
            kind: ours.kind,
            binding: Some(BindingChange::Create {
                parent_inode_id: edge.parent_inode_id,
                display_name: display_name(&edge.display_name)?,
                content_ref,
            }),
            updates: Vec::new(),
        };
        self.add_plan(plan, ours, None).await
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
            if let Some(deletion_seq) = self.undeletion(inode_id).await? {
                plan.binding = Some(BindingChange::Undelete {
                    deletion_seq,
                    parent_inode_id: edge.parent_inode_id,
                    display_name: display_name(&edge.display_name)?,
                });
            } else if !self.hidden_by_undeleted_ancestor(inode_id, edge).await? {
                self.conflict(inode_id, MergeConflictKind::Visibility, ours, None)
                    .await?;
                return Ok(());
            }
        } else if changed.binding && !ours.same_binding(theirs) {
            let (binding, kind) = self.plan_binding(inode_id, ours, theirs).await?;
            plan.binding = Some(binding);
            conflict = conflict.or(kind);
        }
        if ours.visible.is_some() {
            conflict = conflict.or(self.plan_updates(&mut plan, ours, theirs, changed).await?);
        }
        if let Some(kind) = conflict {
            self.conflict(inode_id, kind, ours, theirs.conflict_version())
                .await?;
        }
        self.add_plan(plan, ours, Some(theirs)).await
    }

    /// Returns the sequence of the deletion that hides `inode_id` in the
    /// merged-into namespace, when the merge undoes the deletion. That's a
    /// deletion of the inode itself that's the fork's own (see
    /// [`Self::is_own`]), which an earlier merge of the fork made, that's
    /// still recoverable there, and that the fork undid, so the inode is
    /// visible in the fork.
    async fn undeletion(&mut self, inode_id: InodeId) -> Result<Option<ChangeSeq>> {
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
    /// [`Self::undeletion`]), and keeps there the parent and the name that
    /// `edge` gives it in the fork. Undoing those deletions shows the inode
    /// again without a change of its own.
    async fn hidden_by_undeleted_ancestor(
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
        let mut undeleted = false;
        let mut ancestor = binding.parent_inode_id;
        // A binding cycle would be corrupt, and the bound ends the walk.
        for _ in 0..=MAX_PATH_DEPTH {
            if view.hiding_subtree_tombstone(ancestor).await?.is_some() {
                if self.undeletion(ancestor).await?.is_none() {
                    return Ok(false);
                }
                undeleted = true;
            }
            if ancestor == ROOT_INODE_ID {
                return Ok(undeleted);
            }
            let Some(binding) = view.current_parent_binding_for_child(ancestor).await? else {
                return Ok(false);
            };
            ancestor = binding.parent_inode_id;
        }
        Ok(false)
    }

    /// Plans a move or a delete of a visible inode, and returns the kind of
    /// conflict when the binding or revision that it replaces isn't the fork's
    /// own (see [`Self::is_own`]), or
    /// something below a directory changed.
    async fn plan_binding(
        &mut self,
        inode_id: InodeId,
        ours: &InodeState,
        theirs: &InodeState,
    ) -> Result<(BindingChange, Option<MergeConflictKind>)> {
        let their_edge = theirs
            .edge()
            .expect("an inode whose binding differs from a visible one's isn't the root");
        let expected_binding_version =
            crate::binding_version::encode(their_edge.binding, &self.main.head().namespace_id);
        let mut conflict = None;
        if !self.is_own(their_edge.binding.seq).await? {
            conflict = Some(MergeConflictKind::Binding);
        }
        let binding = match ours.edge() {
            Some(edge) => BindingChange::Move {
                expected_binding_version,
                parent_inode_id: edge.parent_inode_id,
                display_name: display_name(&edge.display_name)?,
            },
            None => {
                if let Some(revision) = &theirs.revision {
                    if conflict.is_none() && !self.is_own(revision.committed_seq).await? {
                        conflict = Some(MergeConflictKind::Content);
                    }
                }
                BindingChange::Delete {
                    expected_binding_version,
                    expected_revision_no: theirs.revision.as_ref().map(|row| row.revision_no),
                }
            }
        };
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
        Ok((binding, conflict))
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
                plan.updates
                    .push(FilesystemOperation::PutFileRevisionByInode {
                        inode_id,
                        content: loonfs_types::FileContent::Ref {
                            content_ref: row.content_ref.clone(),
                            source_namespace_id: None,
                        },
                        expected_revision_no: their_row.revision_no,
                    });
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

    /// Adds an inode's plan, with the attributes and access of an inode that
    /// the merge creates, and adds its operations and the layouts that they
    /// record to the commit's running size bound.
    ///
    /// The running bound stops the merge with `merge_too_large` as soon as
    /// the plans so far exceed one WAL object or the merge allowance, so the
    /// plans keep at most about that much memory, and a merge that admission
    /// would refuse isn't planned in full. It adds operations up as the merge
    /// commit's own bound does. The commit's bound also includes the
    /// temporary moves that ordering the operations can add, and admission
    /// checks that bound.
    async fn add_plan(
        &mut self,
        mut plan: InodePlan,
        ours: &InodeState,
        theirs: Option<&InodeState>,
    ) -> Result<()> {
        if theirs.is_none() {
            if ours.attributes.is_some() {
                plan.updates
                    .push(attributes_update(plan.inode_id, ours, None));
            }
            if ours.access.is_some() {
                plan.updates.push(access_update(plan.inode_id, ours, None));
            }
        }
        let operations: Vec<_> = plan
            .binding
            .as_ref()
            .map(|binding| binding.operation(plan.inode_id, plan.kind))
            .into_iter()
            .chain(plan.updates.iter().cloned())
            .collect();
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
        let document_bytes = self.planned_bytes.saturating_add(WAL_OBJECT_OVERHEAD_BYTES);
        if document_bytes > MAX_WAL_OBJECT_BYTES {
            return Err(CoreError::MergeTooLarge {
                bound: MergeBound::WalObject,
                estimated_bytes: document_bytes,
                max_bytes: MAX_WAL_OBJECT_BYTES,
                split_at_seq: None,
            });
        }
        self.allowance.check(self.planned_bytes)?;
        self.plans.push(plan);
        Ok(())
    }

    /// Returns the layout of `content_ref` in the fork's view. The fork's
    /// fold before planning wrote a layout for each content reference that
    /// the range's commits made.
    async fn layout(&mut self, content_ref: &ContentRef) -> Result<&ContentLayout> {
        if !self.layouts.contains_key(content_ref) {
            let layout = self
                .ours
                .view()
                .content_layout(&content_ref.content_id)
                .await?
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "fork `{}` has no layout for content `{}` after its fold",
                        self.record.source_namespace_id, content_ref.content_id
                    ))
                })?
                .layout;
            self.layouts.insert(content_ref.clone(), layout);
        }
        Ok(&self.layouts[content_ref])
    }

    /// Returns whether the row committed at `seq` is the fork's own. A row is
    /// the fork's own when it was committed at or below the fork point, or by
    /// a merge from this fork. A replay commit reads the commit at or below
    /// the new fork's fork point through the pin.
    pub(super) async fn is_own(&mut self, seq: ChangeSeq) -> Result<bool> {
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
