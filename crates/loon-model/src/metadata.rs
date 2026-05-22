use loon_api::{
    name_key_for_display_name, ChangeSeq, ContentRef, InodeId, InodeKind, NamePolicy, RevisionNo,
    WalDelta,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataState {
    #[serde(default)]
    pub inodes: Vec<InodeRecord>,
    #[serde(default)]
    pub direntry_binds: Vec<DirentryBindRecord>,
    #[serde(default)]
    pub direntry_unbinds: Vec<DirentryUnbindRecord>,
    #[serde(default)]
    pub revisions: Vec<RevisionRecord>,
    #[serde(default)]
    pub subtree_tombstones: Vec<SubtreeTombstoneRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryBindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryUnbindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
    pub unbind_seq: ChangeSeq,
    pub unbind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    pub revision_delta_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    pub tombstone_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

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
        "path component traversal expected directory at `{absolute_path}` but found inode `{inode_id:?}` kind `{inode_kind:?}`"
    )]
    PathComponentNotDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataApplyError {
    RevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
}

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        deltas: &[WalDelta],
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for delta in deltas {
            match delta {
                WalDelta::CreateInode {
                    delta_index: _,
                    inode_id,
                    inode_kind,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: inode_kind.clone(),
                        created_seq: committed_seq,
                    });
                    push_unique_invariant(&mut checked_invariants, "create_inode_writes_inode_row");
                }
                WalDelta::BindDirentry {
                    delta_index,
                    parent_inode,
                    display_name,
                    child_inode,
                } => {
                    metadata_state.direntry_binds.push(DirentryBindRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key_for_display_name(NamePolicy::default(), display_name),
                        display_name: display_name.clone(),
                        child_inode_id: *child_inode,
                        bind_seq: committed_seq,
                        bind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "bind_direntry_writes_direntry_bind_row",
                    );
                }
                WalDelta::UnbindDirentry {
                    delta_index,
                    parent_inode,
                    name_key,
                    child_inode,
                    bind_seq,
                    bind_delta_index,
                } => {
                    metadata_state.direntry_unbinds.push(DirentryUnbindRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key.clone(),
                        child_inode_id: *child_inode,
                        bind_seq: *bind_seq,
                        bind_delta_index: *bind_delta_index,
                        unbind_seq: committed_seq,
                        unbind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "unbind_direntry_writes_unbind_row",
                    );
                }
                WalDelta::AppendFileRevision {
                    delta_index,
                    inode_id,
                    revision_no,
                    content_ref,
                } => {
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: *revision_no,
                        committed_seq,
                        revision_delta_index: *delta_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "append_file_revision_writes_revision_row",
                    );
                }
                WalDelta::TombstoneSubtree {
                    delta_index,
                    root_inode,
                } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode,
                            tombstone_seq: committed_seq,
                            tombstone_delta_index: *delta_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "tombstone_subtree_writes_tombstone_row",
                    );
                }
            }
        }

        Ok(AppliedMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
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

    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        let canonical_name_key = name_key_for_display_name(NamePolicy::default(), name_key);
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == canonical_name_key
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
        let direntry = self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        Some(direntry)
    }

    pub fn active_subtree_tombstone(
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

    pub fn visible_inode(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
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
        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn visible_children(
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

    pub fn resolve_visible_path(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        let components = parse_absolute_path_components(absolute_path)?;
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id, base_seq)
            .ok_or(VisiblePathError::RootMissing)?;
        if components.is_empty() {
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

        for component in components {
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

            let requested_absolute_path = join_absolute_path(&current_absolute_path, &component);
            let direntry = self
                .visible_child(current_inode_id, &component, base_seq)
                .ok_or(VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path =
                join_absolute_path(&current_absolute_path, &direntry.display_name);
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
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

fn parse_absolute_path_components(absolute_path: &str) -> Result<Vec<String>, VisiblePathError> {
    if !absolute_path.starts_with('/') {
        return Err(VisiblePathError::InvalidAbsolutePath {
            absolute_path: absolute_path.to_owned(),
        });
    }

    let mut components = Vec::new();
    for component in absolute_path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(VisiblePathError::InvalidAbsolutePath {
                absolute_path: absolute_path.to_owned(),
            });
        }
        components.push(component.to_owned());
    }
    Ok(components)
}

fn join_absolute_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
    }
}
