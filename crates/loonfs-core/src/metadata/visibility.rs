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
use loonfs_types::{
    AbsolutePath, CommitSeq, DisplayName, InodeId, InodeKind, NameKey, ROOT_INODE_ID,
};
use std::collections::BTreeSet;
use std::future::Future;

/// Storage lookups scoped to one read sequence.
///
/// Required `find_*` methods return raw rows. Provided methods apply
/// visibility rules and may be overridden only for equivalent cached or
/// indexed paths.
pub(crate) trait MetadataVisibilityReads {
    type Error: From<VisiblePathError>;

    /// The view's retention floor. A deletion at or below it has expired in
    /// the view
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    fn retention_floor_seq(&self) -> CommitSeq;

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

    /// Composite rule; see [`ancestry`].
    async fn ancestry(&mut self, inode_id: InodeId) -> Result<Ancestry, Self::Error>
    where
        Self: Sized,
    {
        ancestry(self, inode_id).await
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
        parent_inode_id = %parent_inode_id,
        child_inode_id = direntry.map(|direntry| tracing::field::display(direntry.child_inode_id)),
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
/// corrupt stored metadata. The seen set bounds the walk by the
/// namespace's inode count.
pub(crate) struct AncestorWalk {
    next: Option<NextAncestor>,
    seen: BTreeSet<InodeId>,
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
            seen: BTreeSet::new(),
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
        if !self.seen.insert(inode_id) {
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

/// The end of a walk up the current parent bindings from an inode
/// ([format section 4.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#43-visible-metadata)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ancestry {
    /// The walk reached the root inode without meeting a hiding tombstone,
    /// so the inode is visible if it exists.
    Root,
    /// The first hiding tombstone (a `set`) that the walk met, on the inode
    /// or on one of its ancestors.
    Hidden(SubtreeTombstoneRecord),
    /// The walk reached an inode with no current parent binding before it
    /// reached the root or a hiding tombstone.
    Disconnected,
}

/// Walks the current bound parent bindings up from `inode_id` to the root
/// or the first hiding tombstone, whichever comes first.
///
/// A `revoke` hides nothing, so the walk passes it. A deletion in the trash
/// and an expired one both hide their subtree, so the walk doesn't read the
/// retention floor, and the walk doesn't continue above the inode that a
/// deletion's tombstone is on.
///
/// An inode that isn't deleted has bound edges up to the root, and a deleted
/// inode has them up to the inode that its deletion's tombstone is on, so a
/// chain that breaks before either occurs only below an expired deletion.
/// Base compactions remove an expired subtree's rows a family group at a
/// time, so the bindings that lead to the deleted directory can be gone while
/// the inode rows and revisions below it remain. Every visibility check
/// therefore treats an inode whose ancestry is [`Ancestry::Disconnected`] as
/// hidden.
///
/// The root inode can't be deleted, so the walk stops there without reading
/// its tombstones. A parent-binding cycle is corrupt stored metadata, and the
/// walk returns an error for it, because returning any variant could show a
/// deleted subtree.
pub(crate) async fn ancestry<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Ancestry, R::Error> {
    let mut walk = AncestorWalk::new(inode_id);
    while let Some(ancestor) = walk.next(reads).await? {
        if ancestor == ROOT_INODE_ID {
            return Ok(Ancestry::Root);
        }
        if let Some(tombstone) = reads.find_hiding_subtree_tombstone(ancestor).await? {
            return Ok(Ancestry::Hidden(tombstone));
        }
    }
    Ok(Ancestry::Disconnected)
}

/// Whether the ancestry of `inode_id` allows a read by inode ID
/// ([format section 4.4](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#44-paths-listings-and-revision-reads)).
///
/// The read is refused when the walk meets a deletion at or below the view's
/// retention floor, which has expired. The read is allowed when the walk
/// meets a deletion above the floor, because the rights on an item under a
/// deletion in the trash are judged on the deletion's saved ancestry. The
/// read is allowed when the walk reaches the root, and refused when the
/// ancestry is disconnected (see [`ancestry`]).
pub(crate) async fn ancestry_allows_read<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<bool, R::Error> {
    Ok(match reads.ancestry(inode_id).await? {
        Ancestry::Root => true,
        Ancestry::Hidden(tombstone) => tombstone.committed_seq > reads.retention_floor_seq(),
        Ancestry::Disconnected => false,
    })
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

/// An inode is visible iff it exists at the read seq and its current parent
/// bindings lead to the root without a hiding tombstone ([`Ancestry::Root`]).
pub(crate) async fn visible_inode<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Option<InodeRecord>, R::Error> {
    let Some(inode) = reads.find_inode(inode_id).await? else {
        return Ok(None);
    };
    if reads.ancestry(inode_id).await? != Ancestry::Root {
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
/// child inode exists at that sequence, leaving the child's ancestry as the
/// only way it can be invisible.
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
    if reads.ancestry(direntry.child_inode_id).await? != Ancestry::Root {
        trace_absent_leg(
            AbsentVisibilityLeg::ChildInode,
            direntry.parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// How far [`walk_visible_path`] got down a path.
pub(crate) enum VisiblePathWalk {
    /// Every component of the path is visible.
    Found(ResolvedVisiblePath),
    /// A component of the path has no visible child under the directory above
    /// it.
    Missing {
        /// The deepest directory on the path that is visible, which is the
        /// directory above the first missing component.
        deepest_existing: InodeId,
        /// The path of that directory, as the namespace stores it.
        deepest_path: AbsolutePath,
        /// The path's components from the first missing one to the last one,
        /// as the path spells them.
        missing: Vec<DisplayName>,
    },
}

impl VisiblePathWalk {
    /// Returns the path's inode, or [`VisiblePathError::PathNotFound`] for
    /// the first component that has no visible child.
    pub(crate) fn into_resolved(self) -> Result<ResolvedVisiblePath, VisiblePathError> {
        match self {
            Self::Found(resolved) => Ok(resolved),
            Self::Missing {
                deepest_path,
                missing,
                ..
            } => Err(VisiblePathError::PathNotFound {
                absolute_path: deepest_path.join(&missing[0]).to_string(),
            }),
        }
    }
}

/// Walks `absolute_path` component by component through visible directories
/// and visible child bindings, starting at the canonical root inode, until
/// it reaches the path's inode or a component that has no visible child.
/// Each component folds into its name key.
pub(crate) async fn walk_visible_path<R>(
    reads: &mut R,
    absolute_path: &AbsolutePath,
) -> Result<VisiblePathWalk, R::Error>
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
        return Ok(VisiblePathWalk::Found(ResolvedVisiblePath {
            absolute_path: AbsolutePath::root(),
            inode_id: root_inode_id,
            inode_kind: root.inode_kind,
            created_by: root.committed_by,
            created_at_ms: root.committed_at_ms,
            parent_inode_id: None,
            display_name: String::new(),
            binding_version: None,
        }));
    }

    let mut last: Option<DirentryBindingRecord> = None;
    let mut current_absolute_path = AbsolutePath::root();

    let components = absolute_path.components();
    for (depth, component) in components.iter().enumerate() {
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

        let name_key = NameKey::for_display_name(&component.to_display_name());
        let direntry = match reads
            .active_child_binding(parent_inode_id, &name_key)
            .await?
        {
            Some(direntry) => visible_page_child(reads, direntry).await?,
            None => None,
        };
        let Some(direntry) = direntry else {
            return Ok(VisiblePathWalk::Missing {
                deepest_existing: parent_inode_id,
                deepest_path: current_absolute_path,
                missing: components[depth..]
                    .iter()
                    .map(|component| component.to_display_name())
                    .collect(),
            });
        };

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
    let binding_version = Some(last.bind_position);
    Ok(VisiblePathWalk::Found(ResolvedVisiblePath {
        absolute_path: current_absolute_path,
        inode_id: last.child_inode_id,
        inode_kind: last.child_kind,
        created_by: last.child_created_by,
        created_at_ms: last.child_created_at_ms,
        parent_inode_id: Some(last.parent_inode_id),
        display_name,
        binding_version,
    }))
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
    base_seq: CommitSeq,
}

impl MetadataState {
    pub(crate) fn reads_at_seq(&self, base_seq: CommitSeq) -> MetadataStateReads<'_> {
        MetadataStateReads {
            state: self,
            base_seq,
        }
    }
}

impl MetadataVisibilityReads for MetadataStateReads<'_> {
    type Error = VisiblePathError;

    /// In-memory state has no manifest and no floor object, so like a new
    /// namespace's first manifest, it has a floor of 0 and no deletion in it
    /// has expired.
    fn retention_floor_seq(&self) -> CommitSeq {
        CommitSeq(0)
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
