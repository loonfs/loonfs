//! Shared direntry visibility rules.
//!
//! [`MetadataVisibilityReads`] defines the required storage lookups. The
//! composite methods and free functions apply the same visibility rules to
//! in-memory state, manifest-backed views, and mutation previews.
//! [`BindingIdentity`] provides the common bind/unbind comparison.

use super::queries::{ResolvedVisiblePath, VisiblePathError};
use super::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState, SubtreeTombstoneRecord,
};
use crate::binding_generation::BindingGeneration;
use futures::FutureExt;
use loonfs_api::{AbsolutePath, ChangeSeq, InodeId, InodeKind, NameKey, ROOT_INODE_ID};
use std::collections::BTreeSet;
use std::future::Future;

/// The identity key of a direntry binding event.
///
/// Two records describe the same binding event iff all five fields agree.
/// Unbind records carry the identity of the bind they revoke, so an unbind
/// matches a bind through the same comparison. `display_name` is
/// presentation, not identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BindingIdentity {
    pub(crate) parent_inode_id: InodeId,
    pub(crate) name_key: NameKey,
    pub(crate) child_inode_id: InodeId,
    pub(crate) bind_seq: ChangeSeq,
    pub(crate) bind_delta_index: u32,
}

impl From<&DirentryBindRecord> for BindingIdentity {
    fn from(record: &DirentryBindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }
}

impl From<&DirentryUnbindRecord> for BindingIdentity {
    fn from(record: &DirentryUnbindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }
}

pub(crate) fn same_binding(left: &DirentryBindRecord, right: &DirentryBindRecord) -> bool {
    BindingIdentity::from(left) == BindingIdentity::from(right)
}

pub(crate) fn binding_generation(record: &DirentryBindRecord) -> BindingGeneration {
    BindingGeneration {
        bind_seq: record.bind_seq,
        bind_delta_index: record.bind_delta_index,
    }
}

/// True iff `unbind` revokes exactly the binding event `direntry`.
pub(crate) fn unbind_matches_binding(
    unbind: &DirentryUnbindRecord,
    direntry: &DirentryBindRecord,
) -> bool {
    BindingIdentity::from(unbind) == BindingIdentity::from(direntry)
}

/// Storage lookups scoped to one read sequence.
///
/// Required `find_*` methods return raw rows. Provided methods apply
/// visibility rules and may be overridden only for equivalent cached or
/// indexed paths.
pub(crate) trait MetadataVisibilityReads {
    type Error;

    /// The inode record created at or before the read seq, if any.
    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error>;

    /// Latest bind for `(parent, name_key)` at the read seq, regardless of
    /// whether it has since been unbound.
    async fn find_latest_bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>;

    /// Latest bind whose child is `child_inode_id` at the read seq,
    /// regardless of whether it has since been unbound.
    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>;

    /// Latest subtree tombstone rooted at `root_inode_id` at the read seq.
    async fn find_active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error>;

    /// Whether an unbind revoking exactly this binding event exists at the
    /// read seq.
    async fn is_binding_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, Self::Error>;

    /// Composite rule; see [`current_parent_binding_for_child`].
    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>
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
    ) -> Result<Option<DirentryBindRecord>, Self::Error>
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

    /// Composite rule; see [`visible_child`].
    async fn visible_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error>
    where
        Self: Sized,
    {
        visible_child(self, parent_inode_id, name_key).await
    }
}

/// Which leg of the child-visibility walk answered no.
///
/// A visible child needs three durable families to agree: the forward bind
/// row under `(parent, name_key)`, the child's own latest parent binding,
/// and the inode rows for the parent and the child. When any one of them
/// comes back empty, the walk answers "no child", and from the outside every
/// leg looks the same. This names the leg that stopped the walk, so a read
/// that answers not-found says which lookup produced the absence instead of
/// only that there was one.
///
/// Only absences are named. A visible child is the ordinary answer and says
/// nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbsentVisibilityLeg {
    /// The parent inode is not visible at the read seq.
    ParentInode,
    /// The parent inode is visible but is not a directory, so it binds no
    /// children under any name.
    ParentNotDirectory,
    /// No bind row exists under `(parent, name_key)`.
    ForwardBinding,
    /// A bind row exists and an unbind revokes it.
    BindingUnbound,
    /// The bound child has no current parent binding of its own.
    ReverseIndex,
    /// The child's current parent binding is a different bind event: the
    /// child was bound elsewhere and this name is the stale one.
    BindingSuperseded,
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
            Self::ReverseIndex => "reverse_index",
            Self::BindingSuperseded => "binding_superseded",
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
    direntry: Option<&DirentryBindRecord>,
) {
    tracing::debug!(
        leg = leg.name(),
        parent_inode_id = parent_inode_id.0,
        child_inode_id = direntry.map(|direntry| direntry.child_inode_id.0),
        bind_seq = direntry.map(|direntry| direntry.bind_seq.0),
        "visibility walk found no child"
    );
}

/// The child's current parent binding: the latest binding for the child that
/// has not been unbound. Returns `None` when the latest binding was revoked,
/// even if an older un-revoked binding row still exists — bindings are
/// superseded by later ones, never resurrected.
pub(crate) async fn current_parent_binding_for_child<R: MetadataVisibilityReads>(
    reads: &mut R,
    child_inode_id: InodeId,
) -> Result<Option<DirentryBindRecord>, R::Error> {
    let Some(direntry) = reads
        .find_latest_parent_binding_for_child(child_inode_id)
        .await?
    else {
        return Ok(None);
    };
    if reads.is_binding_unbound(&direntry).await? {
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// The ACTIVE binding for `(parent, name_key)`: the latest bind under that
/// name that (1) has not been unbound and (2) is also the child inode's
/// current binding — a child renamed elsewhere leaves its old name bound in
/// the rows but inactive.
///
/// Condition (2) is checked by comparing identities against
/// [`current_parent_binding_for_child`], which already folds in the unbound
/// check.
pub(crate) async fn active_child_binding<R: MetadataVisibilityReads>(
    reads: &mut R,
    parent_inode_id: InodeId,
    name_key: &NameKey,
) -> Result<Option<DirentryBindRecord>, R::Error> {
    let Some(direntry) = reads
        .find_latest_bound_child(parent_inode_id, name_key)
        .await?
    else {
        trace_absent_leg(AbsentVisibilityLeg::ForwardBinding, parent_inode_id, None);
        return Ok(None);
    };
    if reads.is_binding_unbound(&direntry).await? {
        trace_absent_leg(
            AbsentVisibilityLeg::BindingUnbound,
            parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    let Some(latest_binding) = reads
        .current_parent_binding_for_child(direntry.child_inode_id)
        .await?
    else {
        trace_absent_leg(
            AbsentVisibilityLeg::ReverseIndex,
            parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    };
    if !same_binding(&latest_binding, &direntry) {
        trace_absent_leg(
            AbsentVisibilityLeg::BindingSuperseded,
            parent_inode_id,
            Some(&direntry),
        );
        return Ok(None);
    }
    Ok(Some(direntry))
}

/// The first active subtree tombstone rooted at `inode_id` or at any of its
/// current ancestors (following current parent bindings upward). The visited
/// set terminates the walk instead of looping on parent-binding cycles.
pub(crate) async fn covering_subtree_tombstone<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
) -> Result<Option<SubtreeTombstoneRecord>, R::Error> {
    let mut current = Some(inode_id);
    let mut visited = BTreeSet::new();

    while let Some(candidate_inode_id) = current {
        if !visited.insert(candidate_inode_id.0) {
            break;
        }
        if let Some(tombstone) = reads
            .find_active_subtree_tombstone(candidate_inode_id)
            .await?
        {
            return Ok(Some(tombstone));
        }
        current = reads
            .current_parent_binding_for_child(candidate_inode_id)
            .await?
            .map(|direntry| direntry.parent_inode_id);
    }

    Ok(None)
}

/// Whether binding `inode_id` under `new_parent_inode_id` would make the
/// directory graph cyclic: true iff `inode_id` is `new_parent_inode_id` or
/// one of its current ancestors. Same ancestor walk (and cycle guard) as
/// [`covering_subtree_tombstone`], visiting for identity instead of
/// tombstones.
pub(crate) async fn would_create_directory_cycle<R: MetadataVisibilityReads>(
    reads: &mut R,
    inode_id: InodeId,
    new_parent_inode_id: InodeId,
) -> Result<bool, R::Error> {
    let mut current = Some(new_parent_inode_id);
    let mut visited = BTreeSet::new();

    while let Some(candidate_inode_id) = current {
        if !visited.insert(candidate_inode_id.0) {
            break;
        }
        if candidate_inode_id == inode_id {
            return Ok(true);
        }
        current = reads
            .current_parent_binding_for_child(candidate_inode_id)
            .await?
            .map(|direntry| direntry.parent_inode_id);
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
) -> Result<Option<DirentryBindRecord>, R::Error> {
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

/// Resolves `absolute_path` component by component through visible
/// directories and visible child bindings, starting at the canonical root
/// inode.
pub(crate) async fn resolve_visible_path<R>(
    reads: &mut R,
    absolute_path: &AbsolutePath,
) -> Result<ResolvedVisiblePath, R::Error>
where
    R: MetadataVisibilityReads,
    R::Error: From<VisiblePathError>,
{
    let root_inode_id = ROOT_INODE_ID;
    let root = reads
        .visible_inode(root_inode_id)
        .await?
        .ok_or(VisiblePathError::RootMissing)?;
    if absolute_path.is_root() {
        return Ok(ResolvedVisiblePath {
            absolute_path: "/".to_owned(),
            inode_id: root_inode_id,
            inode_kind: root.inode_kind,
            created_by: root.created_by,
            created_at_ms: root.created_at_ms,
            parent_inode_id: None,
            display_name: String::new(),
            binding_generation: None,
        });
    }

    let mut current_inode_id = root_inode_id;
    let mut current_absolute_path = "/".to_owned();
    let mut current_parent_inode_id = None;
    let mut current_display_name = String::new();
    let mut current_binding_generation = None;

    for component in absolute_path.components() {
        let current_inode = reads
            .visible_inode(current_inode_id)
            .await?
            .ok_or_else(|| VisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            })?;
        if current_inode.inode_kind != InodeKind::Directory {
            return Err(VisiblePathError::PathComponentNotDirectory {
                absolute_path: current_absolute_path,
                inode_id: current_inode_id,
                inode_kind: current_inode.inode_kind,
            }
            .into());
        }

        let requested_absolute_path = join_display_path(&current_absolute_path, component.as_str());
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(&display_name);
        let direntry = reads
            .visible_child(current_inode_id, &name_key)
            .await?
            .ok_or(VisiblePathError::PathNotFound {
                absolute_path: requested_absolute_path,
            })?;

        current_inode_id = direntry.child_inode_id;
        current_parent_inode_id = Some(direntry.parent_inode_id);
        current_absolute_path =
            join_display_path(&current_absolute_path, direntry.display_name.as_str());
        current_display_name = direntry.display_name.to_string();
        current_binding_generation = Some(binding_generation(&direntry));
    }

    let inode = reads
        .visible_inode(current_inode_id)
        .await?
        .ok_or_else(|| VisiblePathError::PathNotFound {
            absolute_path: current_absolute_path.clone(),
        })?;
    Ok(ResolvedVisiblePath {
        absolute_path: current_absolute_path,
        inode_id: current_inode_id,
        inode_kind: inode.inode_kind,
        created_by: inode.created_by,
        created_at_ms: inode.created_at_ms,
        parent_inode_id: current_parent_inode_id,
        display_name: current_display_name,
        binding_generation: current_binding_generation,
    })
}

fn join_display_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
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
pub(super) struct MetadataStateReads<'a> {
    state: &'a MetadataState,
    base_seq: ChangeSeq,
}

impl MetadataState {
    pub(super) fn reads_at_seq(&self, base_seq: ChangeSeq) -> MetadataStateReads<'_> {
        MetadataStateReads {
            state: self,
            base_seq,
        }
    }
}

impl MetadataVisibilityReads for MetadataStateReads<'_> {
    type Error = VisiblePathError;

    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.inode(inode_id)
        } else {
            self.state.inode_at_seq_scan(inode_id, self.base_seq)
        })
    }

    async fn find_latest_bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
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
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        Ok(self.state.latest_parent_binding_for_child_at_seq(
            child_inode_id,
            self.base_seq.min(self.state.indexed_seq()),
        ))
    }

    async fn find_active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.active_tombstone(root_inode_id)
        } else {
            self.state
                .active_subtree_tombstone_scan(root_inode_id, self.base_seq)
        })
    }

    async fn is_binding_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, Self::Error> {
        Ok(if self.base_seq >= self.state.indexed_seq() {
            self.state.indexes.is_unbound(direntry)
        } else {
            self.state
                .is_direntry_unbound_at_seq_scan(direntry, self.base_seq)
        })
    }

    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        if self.base_seq >= self.state.indexed_seq() {
            return Ok(self.state.indexes.active_parent_for_child(child_inode_id));
        }
        current_parent_binding_for_child(self, child_inode_id).await
    }

    async fn active_child_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        if self.base_seq >= self.state.indexed_seq() {
            return Ok(self.state.indexes.active_child(parent_inode_id, name_key));
        }
        active_child_binding(self, parent_inode_id, name_key).await
    }
}
