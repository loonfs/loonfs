//! Shared direntry visibility rules.
//!
//! [`MetadataVisibilityReads`] defines the required storage lookups. The
//! composite methods and free functions apply the same visibility rules to
//! in-memory state, manifest-backed views, and mutation previews.

use super::queries::{ResolvedVisiblePath, VisiblePathError};
use super::{
    AccessRevisionRecord, DirentryBindingRecord, InodeRecord, MetadataState, SubtreeTombstoneRecord,
};
use futures::FutureExt;
use loonfs_types::{AbsolutePath, ChangeSeq, InodeId, InodeKind, NameKey, ROOT_INODE_ID};
use std::collections::BTreeSet;
use std::future::Future;

/// Storage lookups scoped to one read sequence.
///
/// Required `find_*` methods return raw rows. Provided methods apply
/// visibility rules and may be overridden only for equivalent cached or
/// indexed paths.
pub(crate) trait MetadataVisibilityReads {
    type Error: From<VisiblePathError>;

    /// The retention floor of the view's manifest. A deletion at or below it
    /// has expired in the view
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    fn retention_floor_seq(&self) -> ChangeSeq;

    /// The inode record created at or before the read seq, if any.
    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error>;

    /// Returns the latest slot value, including an unbound value.
    async fn find_latest_slot_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error>;

    /// Returns the latest child-index value, including an unbound value.
    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error>;

    /// Latest tombstone event rooted at `root_inode_id` at the read seq, when
    /// that event hides the subtree (a `set`).
    async fn find_hiding_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error>;

    /// Newest access row of `inode_id` at the read seq, if any.
    async fn find_access_row(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<AccessRevisionRecord>, Self::Error>;

    /// Composite rule; see [`current_parent_binding_for_child`].
    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error>
    where
        Self: Sized,
    {
        current_parent_binding_for_child(self, child_inode_id).await
    }

    /// Composite rule; see [`active_child_binding`].
    async fn active_child_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error>
    where
        Self: Sized,
    {
        active_child_binding(self, parent_inode_id, name_key).await
    }

    /// Composite rule; see [`covering_subtree_tombstone`].
    async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error>
    where
        Self: Sized,
    {
        covering_subtree_tombstone(self, inode_id).await
    }

    /// Composite rule; see [`visible_inode`].
    async fn visible_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error>
    where
        Self: Sized,
    {
        visible_inode(self, inode_id).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbsentVisibilityLeg {
    /// The parent inode is not visible at the read seq.
    ParentInode,
    /// The parent inode is visible but is not a directory, so it binds no
    /// children under any name.
    ParentNotDirectory,
    /// No bind row exists under `(parent, name_key)`.
    ForwardBinding,
    /// The newest visible slot value is unbound.
    BindingUnbound,
    /// The bound child's inode is not visible at the read seq.
    ChildInode,
}

impl AbsentVisibilityLeg {
    /// The leg's stable name for the `leg` field.
    ///
    /// Every name is a `&'static str`, so naming a leg allocates nothing.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::ParentInode => "parent_inode",
            Self::ParentNotDirectory => "parent_not_directory",
            Self::ForwardBinding => "forward_binding",
            Self::BindingUnbound => "binding_unbound",
            Self::ChildInode => "child_inode",
        }
    }
}

/// Says which leg made `(parent, name_key)` absent, once per absence.
///
/// Ids only, no names. The events around this one log namespace ids, object
/// keys, and content ids, and none of them logs a path or a display name; a
/// name key is derived from user text, so it stays out of the record too.
/// The child fields are absent when no bind row was found, because there was
/// no child to name.
fn trace_absent_leg(
    leg: AbsentVisibilityLeg,
    parent_inode_id: InodeId,
    direntry: Option<&DirentryBindingRecord>,
) {
    tracing::debug!(
        leg = leg.name(),
        parent_inode_id = parent_inode_id.0,
        child_inode_id = direntry.map(|direntry| direntry.child_inode_id.0),
        committed_seq = direntry.map(|direntry| direntry.committed_seq.0),
        "visibility walk found no child"
    );
}

pub(crate) async fn current_parent_binding_for_child<R: MetadataVisibilityReads>(
    reads: &mut R,
    child_inode_id: InodeId,
) -> Result<Option<DirentryBindingRecord>, R::Error> {
    if child_inode_id == ROOT_INODE_ID {
        return Ok(None);
    }
    let Some(direntry) = reads
        .find_latest_parent_binding_for_child(child_inode_id)
        .await?
    else {
        return Ok(None);
    };
    if !direntry.is_bound() {
        return Ok(None);
    }
    Ok(Some(direntry))
}

pub(crate) async fn active_child_binding<R: MetadataVisibilityReads>(
    reads: &mut R,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<Option<DirentryBindingRecord>, R::Error> {
    let Some(direntry) = reads
        .find_latest_slot_binding(parent_inode_id, name_key)
        .await?
    else {
        trace_absent_leg(AbsentVisibilityLeg::ForwardBinding, parent_inode_id, None);
        return Ok(None);
    };
    if !direntry.is_bound() {
        trace_absent_leg(
            AbsentVisibilityLeg::BindingUnbound,
            parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// A walk up the current parent bindings from one inode, which reports a
/// revisited inode as a parent-binding cycle.
///
/// Stored bindings form a tree, so a walk that revisits an inode found
/// corrupt stored metadata. The visited set bounds the walk by the
/// namespace's inode count.
pub(crate) struct AncestorWalk {
    next: Option<NextAncestor>,
    visited: BTreeSet<InodeId>,
    followed: Option<DirentryBindingRecord>,
}

enum NextAncestor {
    Inode(InodeId),
    ParentOf(InodeId),
}

impl AncestorWalk {
    /// Starts a walk whose first inode is `inode_id`.
    pub(crate) fn new(inode_id: InodeId) -> Self {
        Self {
            next: Some(NextAncestor::Inode(inode_id)),
            visited: BTreeSet::new(),
            followed: None,
        }
    }

    /// Returns the walk's next inode: the start inode first, then the parent
    /// of the inode returned last, or `None` when that inode has no current
    /// parent binding.
    pub(crate) async fn next<R: MetadataVisibilityReads>(
        &mut self,
        reads: &mut R,
    ) -> Result<Option<InodeId>, R::Error> {
        let inode_id = match self.next.take() {
            None => return Ok(None),
            Some(NextAncestor::Inode(inode_id)) => {
                self.followed = None;
                inode_id
            }
            Some(NextAncestor::ParentOf(child_inode_id)) => {
                let Some(binding) = reads
                    .current_parent_binding_for_child(child_inode_id)
                    .await?
                else {
                    self.followed = None;
                    return Ok(None);
                };
                let parent_inode_id = binding.parent_inode_id;
                self.followed = Some(binding);
                parent_inode_id
            }
        };
        if !self.visited.insert(inode_id) {
            return Err(VisiblePathError::ParentBindingCycle { inode_id }.into());
        }
        self.next = Some(NextAncestor::ParentOf(inode_id));
        Ok(Some(inode_id))
    }

    /// Continues the walk at `inode_id` after it ended, for a parent that a
    /// deleted inode records instead of a current binding.
    pub(crate) fn continue_at(&mut self, inode_id: InodeId) {
        self.next = Some(NextAncestor::Inode(inode_id));
    }

    /// Takes the binding that the walk followed to the inode it returned
    /// last, which is `None` for the start inode and for an inode the walk
    /// continued at.
    pub(crate) fn take_followed_binding(&mut self) -> Option<DirentryBindingRecord> {
        self.followed.take()
    }
}

/// The first hiding subtree tombstone rooted at `inode_id` or at any of its
/// current ancestors (following current parent bindings upward). A
/// recoverable deletion and an expired one both hide their subtree, so the
/// rule doesn't read the retention floor. A parent-binding cycle is corrupt
/// stored metadata: answering "not covered" could show a deleted subtree.
pub(crate) async fn covering_subtree_tombstone<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Option<SubtreeTombstoneRecord>, R::Error> {
    let mut walk = AncestorWalk::new(inode_id);
    while let Some(ancestor) = walk.next(reads).await? {
        if let Some(tombstone) = reads.find_hiding_subtree_tombstone(ancestor).await? {
            return Ok(Some(tombstone));
        }
    }
    Ok(None)
}

/// Whether the ancestry of `inode_id` allows a read by inode ID
/// ([format section 4.4](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#44-paths-listings-and-revision-reads)).
///
/// The walk follows current bound edges up from the inode and stops at the
/// first tombstone that hides the inode or an ancestor (a `set`; a `revoke`
/// hides nothing, so the walk passes it). A deletion at or below the view's
/// retention floor has expired, which refuses the read. A deletion above the
/// floor allows it, because a recoverable deletion's rights are judged on
/// its saved ancestry, so the walk doesn't continue above a recoverable
/// root. Reaching the root inode without a tombstone allows the read.
/// Reaching an inode with no bound edge before any tombstone means the
/// ancestry is disconnected, which refuses the read. Every live inode has
/// bound edges up to the root, and every deleted inode has them up to its
/// deletion's root, which has a tombstone, so a chain that breaks before
/// either belongs to nothing the namespace can show. And base compactions
/// remove an expired subtree's rows a family group at a time, so the
/// bindings that lead to the expired root can be gone while the revisions
/// remain. A parent-binding cycle is corrupt stored metadata.
pub(crate) async fn ancestry_allows_read<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<bool, R::Error> {
    let mut walk = AncestorWalk::new(inode_id);
    while let Some(ancestor) = walk.next(reads).await? {
        if ancestor == ROOT_INODE_ID {
            return Ok(true);
        }
        if let Some(tombstone) = reads.find_hiding_subtree_tombstone(ancestor).await? {
            return Ok(tombstone.committed_seq > reads.retention_floor_seq());
        }
    }
    Ok(false)
}

/// Whether binding `inode_id` under `new_parent_inode_id` would make the
/// directory graph cyclic: true iff `inode_id` is `new_parent_inode_id` or
/// one of its current ancestors.
pub(crate) async fn would_create_directory_cycle<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
    new_parent_inode_id: InodeId,
) -> Result<bool, R::Error> {
    let mut walk = AncestorWalk::new(new_parent_inode_id);
    while let Some(ancestor) = walk.next(reads).await? {
        if ancestor == inode_id {
            return Ok(true);
        }
    }
    Ok(false)
}

/// An inode is visible iff it exists at the read seq and no subtree
/// tombstone covers it or any of its current ancestors.
pub(crate) async fn visible_inode<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>, R::Error> {
    let Some(inode) = reads.find_inode(inode_id).await? else {
        return Ok(None);
    };
    if reads.covering_subtree_tombstone(inode_id).await?.is_some() {
        return Ok(None);
    }
    Ok(Some(inode))
}

/// The visible child under `(parent, name_key)`: the parent must be a
/// visible directory, the name must have an active binding, and the bound
/// child inode must itself be visible.
pub(crate) async fn visible_child<R: MetadataVisibilityReads>(
    reads: &mut R,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<Option<DirentryBindingRecord>, R::Error> {
    let Some(parent) = reads.visible_inode(parent_inode_id).await? else {
        trace_absent_leg(AbsentVisibilityLeg::ParentInode, parent_inode_id, None);
        return Ok(None);
    };
    if parent.inode_kind != InodeKind::Directory {
        trace_absent_leg(
            AbsentVisibilityLeg::ParentNotDirectory,
            parent_inode_id,
            None,
        );
        return Ok(None);
    }

    let Some(direntry) = reads
        .active_child_binding(parent_inode_id, name_key)
        .await?
    else {
        // No record here. The binding rule names its own leg, and one
        // absence is one record.
        return Ok(None);
    };
    if reads
        .visible_inode(direntry.child_inode_id)
        .await?
        .is_none()
    {
        trace_absent_leg(
            AbsentVisibilityLeg::ChildInode,
            parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// For a page or walk that already checked its parent is a visible directory, the
/// newest slot binding at the read sequence is equivalent to [`visible_child`]
/// without an inode lookup: commit validation creates the child's inode row
/// in the same commit as its first binding, and retention removes an inode row
/// only when an expired deletion hides the inode, which no visible directory
/// binds. A bound binding under a visible directory therefore implies the
/// child inode exists at that sequence, leaving a covering subtree tombstone
/// as the only way it can be invisible.
pub(crate) async fn visible_page_child<R: MetadataVisibilityReads>(
    reads: &mut R,
    direntry: DirentryBindingRecord,
) -> Result<Option<DirentryBindingRecord>, R::Error> {
    if !direntry.is_bound() {
        trace_absent_leg(
            AbsentVisibilityLeg::BindingUnbound,
            direntry.parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    if reads
        .covering_subtree_tombstone(direntry.child_inode_id)
        .await?
        .is_some()
    {
        trace_absent_leg(
            AbsentVisibilityLeg::ChildInode,
            direntry.parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// Resolves `absolute_path` component by component through visible
/// directories and visible child bindings, starting at the canonical root
/// inode. Each component folds into its name key.
pub(crate) async fn resolve_visible_path<R>(
    reads: &mut R,
    absolute_path: &AbsolutePath,
) -> Result<ResolvedVisiblePath, R::Error>
where
    R: MetadataVisibilityReads,
    R::Error: From<VisiblePathError>,
{
    let root_inode_id = ROOT_INODE_ID;
    if absolute_path.is_root() {
        let root = reads
            .visible_inode(root_inode_id)
            .await?
            .ok_or(VisiblePathError::RootMissing)?;
        return Ok(ResolvedVisiblePath {
            absolute_path: AbsolutePath::root(),
            inode_id: root_inode_id,
            inode_kind: root.inode_kind,
            created_by: root.committed_by,
            created_at_ms: root.committed_at_ms,
            parent_inode_id: None,
            display_name: String::new(),
            binding_version: None,
        });
    }

    let mut last: Option<DirentryBindingRecord> = None;
    let mut current_absolute_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let (parent_inode_id, parent_kind) = match &last {
            None => (root_inode_id, InodeKind::Directory),
            Some(binding) => (binding.child_inode_id, binding.child_kind),
        };
        if parent_kind != InodeKind::Directory {
            return Err(VisiblePathError::PathComponentNotDirectory {
                absolute_path: current_absolute_path.to_string(),
                inode_id: parent_inode_id,
                inode_kind: parent_kind,
            }
            .into());
        }

        let display_name = component.to_display_name();
        let not_found = || VisiblePathError::PathNotFound {
            absolute_path: current_absolute_path.join(&display_name).to_string(),
        };
        let name_key = NameKey::for_display_name(&display_name);
        let direntry = reads
            .active_child_binding(parent_inode_id, &name_key)
            .await?
            .ok_or_else(not_found)?;
        let direntry = visible_page_child(reads, direntry)
            .await?
            .ok_or_else(not_found)?;

        current_absolute_path = current_absolute_path.join(
            direntry
                .display_name()
                .expect("visible binding should be bound"),
        );
        last = Some(direntry);
    }

    let last = last.expect("a non-root path should have a component");
    let display_name = last
        .display_name()
        .expect("visible binding should be bound")
        .to_string();
    let binding_version = Some(last.position());
    Ok(ResolvedVisiblePath {
        absolute_path: current_absolute_path,
        inode_id: last.child_inode_id,
        inode_kind: last.child_kind,
        created_by: last.child_created_by,
        created_at_ms: last.child_created_at_ms,
        parent_inode_id: Some(last.parent_inode_id),
        display_name,
        binding_version,
    })
}

/// Drives a visibility future built over in-memory reads to completion
/// without an executor.
///
/// Every [`MetadataVisibilityReads`] method on the in-memory implementors
/// returns without awaiting, so the composed future finishes on its first
/// poll; `now_or_never` performs exactly that single poll (the same pattern
/// commit validation uses to stay synchronous over its in-memory preview).
pub(crate) fn resolve_in_memory_read<T>(future: impl Future<Output = T>) -> T {
    future
        .now_or_never()
        .expect("in-memory metadata visibility reads should never await")
}

/// [`MetadataState`] reads scoped to `base_seq`.
pub(crate) struct MetadataStateReads<'a> {
    state: &'a MetadataState,
    base_seq: ChangeSeq,
}

impl MetadataState {
    pub(crate) fn reads_at_seq(&self, base_seq: ChangeSeq) -> MetadataStateReads<'_> {
        MetadataStateReads {
            state: self,
            base_seq,
        }
    }
}

impl MetadataVisibilityReads for MetadataStateReads<'_> {
    type Error = VisiblePathError;

    /// In-memory state has no manifest, so like a new namespace's first
    /// manifest, it has a floor of 0 and no deletion in it has expired.
    fn retention_floor_seq(&self) -> ChangeSeq {
        ChangeSeq(0)
    }

    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.inode(inode_id)
        } else {
            self.state.inode_at_seq_scan(inode_id, self.base_seq)
        })
    }

    async fn find_latest_slot_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.latest_bind(parent_inode_id, name_key)
        } else {
            self.state
                .bound_child_at_seq_scan(parent_inode_id, name_key, self.base_seq)
        })
    }

    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error> {
        Ok(self.state.latest_parent_binding_for_child_at_seq(
            child_inode_id,
            self.base_seq.min(self.state.indexed_seq()),
        ))
    }

    async fn find_hiding_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.hiding_tombstone(root_inode_id)
        } else {
            self.state
                .hiding_subtree_tombstone_scan(root_inode_id, self.base_seq)
        })
    }

    async fn find_access_row(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<AccessRevisionRecord>, Self::Error> {
        Ok(self.state.newest_access_row_at_seq(inode_id, self.base_seq))
    }

    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error> {
        if self.base_seq >= self.state.indexed_seq() {
            return Ok(self.state.indexes.active_parent_for_child(child_inode_id));
        }
        current_parent_binding_for_child(self, child_inode_id).await
    }

    async fn active_child_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>, Self::Error> {
        if self.base_seq >= self.state.indexed_seq() {
            return Ok(self.state.indexes.active_child(parent_inode_id, name_key));
        }
        active_child_binding(self, parent_inode_id, name_key).await
    }
}
