//! Seq-gated reads over [`MetadataState`]: record lookups, visibility
//! checks, and path resolution.
//!
//! Every seq-parameterized query routes to its `*_at_head` twin once
//! `base_seq` reaches [`MetadataState::indexed_seq`], and to a historical
//! row scan below it.

use super::{
    DirentryBindRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_api::{AbsolutePath, ChangeSeq, InodeId, InodeKind, NameKey, NamePolicy, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedVisiblePath {
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisiblePathError {
    #[error("invalid absolute path `{absolute_path}`")]
    InvalidAbsolutePath { absolute_path: String },
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
        if base_seq >= self.indexed_seq() {
            return self.inode_at_head(inode_id);
        }
        self.inode_at_seq_scan(inode_id, base_seq)
    }

    pub fn inode_at_head(&self, inode_id: InodeId) -> Option<InodeRecord> {
        self.indexes.inode(inode_id)
    }

    fn inode_at_seq_scan(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    pub fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.indexed_seq() {
            return self.latest_revision_at_head(inode_id);
        }
        self.latest_revision_head_at_seq_scan(inode_id, base_seq)
    }

    pub fn latest_revision_at_head(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.indexes.latest_revision(inode_id)
    }

    fn latest_revision_head_at_seq_scan(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
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

    pub fn revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.indexed_seq() {
            return self.revision_at_head(inode_id, revision_no);
        }
        self.revision_at_seq_scan(inode_id, revision_no, base_seq)
    }

    pub fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Option<RevisionRecord> {
        self.indexes.revision(inode_id, revision_no)
    }

    fn revision_at_seq_scan(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= base_seq
            })
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index))
            .cloned()
    }

    /// Latest bind for `(parent, name)` at or before `base_seq`, regardless
    /// of whether it has since been unbound.
    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.indexes.latest_bind(parent_inode_id, name_key);
        }
        self.bound_child_at_seq_scan(parent_inode_id, name_key, base_seq)
    }

    fn bound_child_at_seq_scan(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == name_key
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
        if base_seq >= self.indexed_seq() {
            return self.current_parent_binding_for_child_at_head(child_inode_id);
        }
        let direntry = self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        Some(direntry)
    }

    pub fn current_parent_binding_for_child_at_head(
        &self,
        child_inode_id: InodeId,
    ) -> Option<DirentryBindRecord> {
        self.indexes.active_parent_for_child(child_inode_id)
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        if base_seq >= self.indexed_seq() {
            return self.active_subtree_tombstone_at_head(root_inode_id);
        }
        self.active_subtree_tombstone_scan(root_inode_id, base_seq)
    }

    pub fn active_subtree_tombstone_at_head(
        &self,
        root_inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        self.indexes.active_tombstone(root_inode_id)
    }

    fn active_subtree_tombstone_scan(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        self.subtree_tombstones
            .iter()
            .filter(|tombstone| {
                tombstone.root_inode_id == root_inode_id && tombstone.tombstone_seq <= base_seq
            })
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index))
            .cloned()
    }

    pub fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        if base_seq >= self.indexed_seq() {
            return self.covering_subtree_tombstone_at_head(inode_id);
        }

        walk_ancestor_chain(
            inode_id,
            |candidate| self.active_subtree_tombstone(candidate, base_seq),
            |candidate| {
                self.current_parent_binding_for_child(candidate, base_seq)
                    .map(|direntry| direntry.parent_inode_id)
            },
        )
    }

    pub fn covering_subtree_tombstone_at_head(
        &self,
        inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        walk_ancestor_chain(
            inode_id,
            |candidate| self.active_subtree_tombstone_at_head(candidate),
            |candidate| {
                self.current_parent_binding_for_child_at_head(candidate)
                    .map(|direntry| direntry.parent_inode_id)
            },
        )
    }

    pub fn visible_inode(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_inode_at_head(inode_id);
        }

        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
            return None;
        }

        Some(inode)
    }

    pub fn visible_inode_at_head(&self, inode_id: InodeId) -> Option<InodeRecord> {
        let inode = self.inode_at_head(inode_id)?;
        if self.covering_subtree_tombstone_at_head(inode_id).is_some() {
            return None;
        }

        Some(inode)
    }

    pub fn current_revision_head(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.visible_inode(inode_id, base_seq)?;
        self.latest_revision_head_at_seq(inode_id, base_seq)
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_child_at_head(parent_inode_id, name_key);
        }

        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn visible_child_at_head(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Option<DirentryBindRecord> {
        let parent = self.visible_inode_at_head(parent_inode_id)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.indexes.active_child(parent_inode_id, name_key)?;
        self.visible_inode_at_head(direntry.child_inode_id)?;
        Some(direntry)
    }

    pub fn visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_children_at_head(parent_inode_id);
        }

        let Some(parent) = self.visible_inode(parent_inode_id, base_seq) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.bind_seq <= base_seq
            })
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
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    pub fn visible_children_page_by_name_key(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Vec<DirentryBindRecord> {
        if limit == 0 {
            return Vec::new();
        }
        if base_seq >= self.indexed_seq() {
            return self.visible_children_page_by_name_key_at_head(
                parent_inode_id,
                start_after_name_key,
                limit,
            );
        }

        let mut children = self.visible_children(parent_inode_id, base_seq);
        children.sort_by(|left, right| left.name_key.cmp(&right.name_key));
        children
            .into_iter()
            .filter(|child| {
                start_after_name_key
                    .map(|last_name_key| child.name_key.as_str() > last_name_key)
                    .unwrap_or(true)
            })
            .take(limit)
            .collect()
    }

    pub fn visible_children_page_by_name_key_at_head(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Vec<DirentryBindRecord> {
        if limit == 0 {
            return Vec::new();
        }
        let Some(parent) = self.visible_inode_at_head(parent_inode_id) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = Vec::with_capacity(limit);
        for direntry in self
            .indexes
            .active_children_after_name_key(parent_inode_id, start_after_name_key)
        {
            if self
                .visible_inode_at_head(direntry.child_inode_id)
                .is_none()
            {
                continue;
            }
            children.push(direntry.clone());
            if children.len() == limit {
                break;
            }
        }
        children
    }

    pub fn visible_children_at_head(&self, parent_inode_id: InodeId) -> Vec<DirentryBindRecord> {
        let Some(parent) = self.visible_inode_at_head(parent_inode_id) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .indexes
            .active_children(parent_inode_id)
            .into_iter()
            .filter(|direntry| {
                self.visible_inode_at_head(direntry.child_inode_id)
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

    pub fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
        name_policy: NamePolicy,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id, base_seq)
            .ok_or(VisiblePathError::RootMissing)?;
        if absolute_path.is_root() {
            return Ok(ResolvedVisiblePath {
                absolute_path: "/".to_owned(),
                inode_id: root_inode_id,
                inode_kind: root.inode_kind,
                parent_inode_id: None,
                display_name: String::new(),
            });
        }

        let mut current_inode_id = root_inode_id;
        let mut current_absolute_path = "/".to_owned();
        let mut current_parent_inode_id = None;
        let mut current_display_name = String::new();

        for component in absolute_path.components() {
            let current_inode = self.visible_inode(current_inode_id, base_seq).ok_or(
                VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                },
            )?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                });
            }

            let requested_absolute_path =
                join_display_path(&current_absolute_path, component.as_str());
            let display_name = component.to_display_name();
            let name_key = NameKey::for_display_name(name_policy, &display_name);
            let direntry = self
                .visible_child(current_inode_id, name_key.as_str(), base_seq)
                .ok_or(VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path =
                join_display_path(&current_absolute_path, &direntry.display_name);
        }

        let inode = self
            .visible_inode(current_inode_id, base_seq)
            .ok_or_else(|| VisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            })?;
        Ok(ResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.indexes.active_child(parent_inode_id, name_key);
        }

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
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
    }

    pub fn is_direntry_unbound_at_seq(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        if base_seq >= self.indexed_seq() {
            return self.is_direntry_unbound_at_head(direntry);
        }
        self.is_direntry_unbound_at_seq_scan(direntry, base_seq)
    }

    pub(crate) fn is_direntry_unbound_at_head(&self, direntry: &DirentryBindRecord) -> bool {
        self.indexes.is_unbound(direntry)
    }

    fn is_direntry_unbound_at_seq_scan(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        self.direntry_unbinds.iter().any(|unbind| {
            unbind.unbind_seq <= base_seq
                && unbind.parent_inode_id == direntry.parent_inode_id
                && unbind.name_key == direntry.name_key
                && unbind.child_inode_id == direntry.child_inode_id
                && unbind.bind_seq == direntry.bind_seq
                && unbind.bind_delta_index == direntry.bind_delta_index
        })
    }

    pub fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        if base_seq >= self.indexed_seq() {
            return self.would_create_directory_cycle_at_head(inode_id, new_parent_inode_id);
        }

        walk_ancestor_chain(
            new_parent_inode_id,
            |candidate| (candidate == inode_id).then_some(()),
            |candidate| {
                self.current_parent_binding_for_child(candidate, base_seq)
                    .map(|direntry| direntry.parent_inode_id)
            },
        )
        .is_some()
    }

    pub fn would_create_directory_cycle_at_head(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> bool {
        walk_ancestor_chain(
            new_parent_inode_id,
            |candidate| (candidate == inode_id).then_some(()),
            |candidate| {
                self.current_parent_binding_for_child_at_head(candidate)
                    .map(|direntry| direntry.parent_inode_id)
            },
        )
        .is_some()
    }
}

fn join_display_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}

/// Walks from `start` up the chain of parents produced by `parent_of`,
/// returning the first `visit` hit.
///
/// The visited set guards the walk against parent-binding cycles: revisiting
/// an inode terminates the walk instead of looping. The covering-tombstone
/// and directory-cycle checks are both this walk parameterized by their
/// per-step lookups.
fn walk_ancestor_chain<T>(
    start: InodeId,
    visit: impl Fn(InodeId) -> Option<T>,
    parent_of: impl Fn(InodeId) -> Option<InodeId>,
) -> Option<T> {
    let mut current = Some(start);
    let mut visited = BTreeSet::new();

    while let Some(candidate_inode_id) = current {
        if !visited.insert(candidate_inode_id.0) {
            break;
        }
        if let Some(found) = visit(candidate_inode_id) {
            return Some(found);
        }
        current = parent_of(candidate_inode_id);
    }

    None
}
