//! Classifies a fork's net change over a range against the head of the
//! namespace being merged into, under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)),
//! and plans what the merge's commit does to each inode.

use super::operations::{access_update, attributes_update, display_name, BindingChange, InodePlan};
use super::state::{InodeState, StateReader};
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
    AbsolutePath, AccessRight, ChangeSeq, ContentLayout, ContentRef, FilesystemOperation, InodeId,
    InodeKind, MergeConflict, MergeConflictKind, MergeConflictVersion,
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
    /// The inodes whose change conflicts, each counted once.
    pub(super) conflicted: HashSet<InodeId>,
    /// The conflicting inode with the smallest ID.
    pub(super) first_conflict: Option<MergeConflict>,
}

impl<S: ObjectStore + ?Sized> Classifier<'_, S> {
    /// Classifies each inode that the fork's commits in the range touched,
    /// in inode ID order.
    pub(super) async fn classify(&mut self) -> Result<()> {
        for inode_id in self.touched_inodes().await? {
            self.classify_inode(inode_id).await?;
        }
        Ok(())
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
        if theirs.visible.is_none() {
            if ours.visible.is_some() {
                self.conflict(inode_id, MergeConflictKind::Visibility, ours, None)
                    .await?;
            }
            return Ok(());
        }
        let mut plan = InodePlan {
            inode_id,
            kind: ours.kind,
            binding: None,
            updates: Vec::new(),
        };
        let mut conflict = None;
        if changed.binding && !ours.same_binding(theirs) {
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
                }
            }
        };
        if conflict.is_none() && theirs.kind == InodeKind::Directory {
            let path = &theirs
                .visible
                .as_ref()
                .expect("a visible inode has a path")
                .path;
            if self.subtree_changed(inode_id, path).await? {
                conflict = Some(MergeConflictKind::Subtree);
            }
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
    /// a merge from this fork.
    pub(super) async fn is_own(&mut self, seq: ChangeSeq) -> Result<bool> {
        if seq <= self.fork_point_seq {
            return Ok(true);
        }
        if let Some(own) = self.own_seqs.get(&seq) {
            return Ok(*own);
        }
        let commit = self
            .theirs
            .view()
            .commit_at_seq(seq)
            .await?
            .ok_or_else(|| {
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

    /// Returns whether something below `directory`, at `path`, changed in
    /// the merged-into namespace after the fork point through a row that
    /// isn't the fork's own, from the namespace's diff over the directory.
    ///
    /// The diff's pages advance through the commits after the fork point, so
    /// the loop ends after the namespace's head.
    async fn subtree_changed(&mut self, directory: InodeId, path: &AbsolutePath) -> Result<bool> {
        let mut options = DiffOptions {
            path: path.clone(),
            to_seq: Some(self.main.head().seq),
            ..DiffOptions::default()
        };
        loop {
            let page = crate::diff::diff_page(self.main, self.fork_point_seq, &options).await?;
            for entry in &page.response.entries {
                if entry.inode_id != directory && self.has_foreign_row(entry.inode_id).await? {
                    return Ok(true);
                }
            }
            match page.response.next_after_seq {
                Some(after_seq) => options.after_seq = Some(after_seq),
                None => return Ok(false),
            }
        }
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

    /// Counts a conflict, and keeps it as the first when its inode has the
    /// smallest ID so far. `theirs` is the merged-into namespace's version
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
        if self
            .first_conflict
            .as_ref()
            .is_some_and(|first| first.inode_id < inode_id)
        {
            return Ok(());
        }
        let base = self
            .base
            .state(inode_id)
            .await?
            .and_then(|state| state.conflict_version());
        self.first_conflict = Some(MergeConflict {
            inode_id,
            kind,
            base,
            ours: ours.conflict_version(),
            theirs,
        });
        Ok(())
    }
}
