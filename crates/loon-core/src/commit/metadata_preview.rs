use crate::invariants::InvariantId;
use crate::metadata::{
    DirentryBindRecord, InodeRecord, MetadataApplyError, MetadataState, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loon_api::wire::wal::WalDelta;
use loon_api::{ChangeSeq, InodeId, InodeKind, RevisionNo};
use std::collections::BTreeSet;

pub(super) struct MetadataPreview<'a> {
    base: &'a MetadataState,
    rows: MetadataState,
}

impl<'a> MetadataPreview<'a> {
    pub(super) fn new(base: &'a MetadataState) -> Self {
        Self {
            base,
            rows: MetadataState::default(),
        }
    }

    pub(super) fn apply_committed_wal_deltas_mut(
        &mut self,
        committed_seq: ChangeSeq,
        deltas: &[WalDelta],
    ) -> Result<Vec<InvariantId>, MetadataApplyError> {
        self.rows
            .apply_committed_wal_deltas_mut(committed_seq, deltas)
    }

    pub(super) fn inode_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<InodeRecord> {
        self.rows
            .inode_at_seq(inode_id, base_seq)
            .or_else(|| self.base_inode_at_seq(inode_id, base_seq))
    }

    pub(super) fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.rows_latest_revision_head_at_seq(inode_id, base_seq)
            .into_iter()
            .chain(self.base_latest_revision_head_at_seq(inode_id, base_seq))
            .max_by_key(|revision| {
                (
                    revision.revision_no,
                    revision.committed_seq,
                    revision.revision_delta_index,
                )
            })
    }

    pub(super) fn revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.rows_revision_at_seq(inode_id, revision_no, base_seq)
            .into_iter()
            .chain(self.base_revision_at_seq(inode_id, revision_no, base_seq))
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index))
    }

    fn rows_latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.rows
            .revisions()
            .iter()
            .filter(|revision| revision.inode_id == inode_id && revision.committed_seq <= base_seq)
            .max_by_key(|revision| {
                (
                    revision.revision_no,
                    revision.committed_seq,
                    revision.revision_delta_index,
                )
            })
            .cloned()
    }

    fn rows_revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.rows
            .revisions()
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= base_seq
            })
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index))
            .cloned()
    }

    pub(super) fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        let direntry = self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        Some(direntry)
    }

    pub(super) fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id, base_seq) {
                return Some(tombstone);
            }

            current = self
                .current_parent_binding_for_child(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    pub(super) fn visible_inode(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<InodeRecord> {
        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
            return None;
        }

        Some(inode)
    }

    pub(super) fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub(super) fn visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<DirentryBindRecord> {
        let Some(parent) = self.visible_inode(parent_inode_id, base_seq) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .rows
            .direntry_binds()
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.bind_seq <= base_seq
            })
            .cloned()
            .chain(self.base_visible_children(parent_inode_id, base_seq))
            .filter(|direntry| {
                self.active_child_binding_at_seq(parent_inode_id, &direntry.name_key, base_seq)
                    .map(|active| {
                        active.child_inode_id == direntry.child_inode_id
                            && active.bind_seq == direntry.bind_seq
                            && active.bind_delta_index == direntry.bind_delta_index
                    })
                    .unwrap_or(false)
            })
            .filter(|direntry| {
                self.visible_inode(direntry.child_inode_id, base_seq)
                    .is_some()
            })
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    pub(super) fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        let mut current = Some(new_parent_inode);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == inode_id {
                return true;
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
    }

    fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.rows
            .direntry_binds()
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
            .into_iter()
            .chain(self.base_bound_child_at_seq(parent_inode_id, name_key, base_seq))
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
    }

    fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        self.rows
            .subtree_tombstones()
            .iter()
            .filter(|tombstone| {
                tombstone.root_inode_id == root_inode_id && tombstone.tombstone_seq <= base_seq
            })
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index))
            .cloned()
            .into_iter()
            .chain(self.base_active_subtree_tombstone(root_inode_id, base_seq))
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index))
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        let latest_binding =
            self.latest_parent_binding_for_child_at_seq(direntry.child_inode_id, base_seq)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_delta_index != direntry.bind_delta_index
            || self.is_direntry_unbound_at_seq(&latest_binding, base_seq)
        {
            return None;
        }

        Some(direntry)
    }

    fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.rows
            .direntry_binds()
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
            .into_iter()
            .chain(self.base_current_parent_binding_for_child(child_inode_id, base_seq))
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
    }

    fn is_direntry_unbound_at_seq(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        self.rows.direntry_unbinds().iter().any(|unbind| {
            unbind.unbind_seq <= base_seq
                && unbind.parent_inode_id == direntry.parent_inode_id
                && unbind.name_key == direntry.name_key
                && unbind.child_inode_id == direntry.child_inode_id
                && unbind.bind_seq == direntry.bind_seq
                && unbind.bind_delta_index == direntry.bind_delta_index
        }) || self.base_is_direntry_unbound_at_seq(direntry, base_seq)
    }

    fn base_inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.inode_at_head(inode_id)
        } else {
            self.base.inode_at_seq(inode_id, base_seq)
        }
    }

    fn base_bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.visible_child_at_head(parent_inode_id, name_key)
        } else {
            self.base.visible_child(parent_inode_id, name_key, base_seq)
        }
    }

    fn base_visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<DirentryBindRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.visible_children_at_head(parent_inode_id)
        } else {
            self.base.visible_children(parent_inode_id, base_seq)
        }
    }

    fn base_current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base
                .current_parent_binding_for_child_at_head(child_inode_id)
        } else {
            self.base
                .current_parent_binding_for_child(child_inode_id, base_seq)
        }
    }

    fn base_active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.active_subtree_tombstone_at_head(root_inode_id)
        } else {
            self.base.active_subtree_tombstone(root_inode_id, base_seq)
        }
    }

    fn base_latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.latest_revision_at_head(inode_id)
        } else {
            self.base.latest_revision_head_at_seq(inode_id, base_seq)
        }
    }

    fn base_revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.base.indexed_seq() {
            self.base.revision_at_head(inode_id, revision_no)
        } else {
            self.base.revision_at_seq(inode_id, revision_no, base_seq)
        }
    }

    fn base_is_direntry_unbound_at_seq(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        if base_seq >= self.base.indexed_seq() {
            self.base.is_direntry_unbound_at_head(direntry)
        } else {
            self.base.is_direntry_unbound_at_seq(direntry, base_seq)
        }
    }
}
