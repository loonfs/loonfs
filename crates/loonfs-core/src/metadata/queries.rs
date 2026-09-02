//! Seq-gated reads over [`MetadataState`]: record lookups, visibility
//! checks, and path resolution.
//!
//! Primitive reads use indexes at or above [`MetadataState::indexed_seq`]
//! and scan historical rows below it. Composite visibility decisions live in
//! [`super::visibility`].

use super::visibility::{
    self, resolve_in_memory_read, unbind_matches_binding, MetadataVisibilityReads,
};
use super::{DirentryBindRecord, InodeRecord, MetadataState, SubtreeTombstoneRecord};
use crate::binding_generation::BindingGeneration;
use loonfs_api::{AbsolutePath, ActorRef, ChangeSeq, InodeId, InodeKind, NameKey};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedVisiblePath {
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_by: ActorRef,
    pub created_at_ms: u64,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub binding_generation: Option<BindingGeneration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisiblePathError {
    #[error("canonical root inode is missing")]
    RootMissing,
    #[error("visible path not found: `{absolute_path}`")]
    PathNotFound { absolute_path: String },
    #[error(
        "path component traversal expected directory at `{absolute_path}` but found inode `{inode_id}` kind `{inode_kind}`"
    )]
    PathComponentNotDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
}

impl MetadataState {
    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        read_now(self.reads_at_seq(base_seq).find_inode(inode_id))
    }

    pub(super) fn inode_at_seq_scan(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<InodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    /// Latest bind for `(parent, name)` at or before `base_seq`, regardless
    /// of whether it has since been unbound.
    #[cfg(test)]
    pub(crate) fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        read_now(
            self.reads_at_seq(base_seq)
                .find_latest_bound_child(parent_inode_id, name_key),
        )
    }

    pub(super) fn bound_child_at_seq_scan(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == *name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
    }

    pub fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        read_now(
            self.reads_at_seq(base_seq)
                .current_parent_binding_for_child(child_inode_id),
        )
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        read_now(
            self.reads_at_seq(base_seq)
                .find_active_subtree_tombstone(root_inode_id),
        )
    }

    pub(super) fn active_subtree_tombstone_scan(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        super::rows::active_tombstone_from_records(
            self.subtree_tombstones
                .iter()
                .filter(|tombstone| tombstone.root_inode_id == root_inode_id)
                .cloned(),
            base_seq,
        )
    }

    pub fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        read_now(visibility::covering_subtree_tombstone(
            &mut self.reads_at_seq(base_seq),
            inode_id,
        ))
    }

    pub fn visible_inode(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        read_now(visibility::visible_inode(
            &mut self.reads_at_seq(base_seq),
            inode_id,
        ))
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        read_now(visibility::visible_child(
            &mut self.reads_at_seq(base_seq),
            parent_inode_id,
            name_key,
        ))
    }

    pub fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        resolve_in_memory_read(visibility::resolve_visible_path(
            &mut self.reads_at_seq(base_seq),
            absolute_path,
        ))
    }

    pub(super) fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn is_direntry_unbound_at_seq(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        read_now(self.reads_at_seq(base_seq).is_binding_unbound(direntry))
    }

    pub(super) fn is_direntry_unbound_at_seq_scan(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        self.direntry_unbinds
            .iter()
            .any(|unbind| unbind.unbind_seq <= base_seq && unbind_matches_binding(unbind, direntry))
    }

    pub fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        read_now(visibility::would_create_directory_cycle(
            &mut self.reads_at_seq(base_seq),
            inode_id,
            new_parent_inode_id,
        ))
    }
}

/// Drives an in-memory visibility read and unwraps its uninhabited error
/// arm: of the [`super::visibility`] rules only `resolve_visible_path`
/// constructs a [`VisiblePathError`], and it is not routed through here.
fn read_now<T>(future: impl Future<Output = Result<T, VisiblePathError>>) -> T {
    resolve_in_memory_read(future).expect("seq-scoped metadata state reads should be infallible")
}
