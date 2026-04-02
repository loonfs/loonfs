use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo, WalOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataState {
    #[serde(default)]
    pub inodes: Vec<InodeRecord>,
    #[serde(default)]
    pub direntries: Vec<DirentryRecord>,
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
pub struct DirentryRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    #[serde(default)]
    pub bind_op_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    #[serde(default)]
    pub revision_op_index: u32,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    #[serde(default)]
    pub tombstone_op_index: u32,
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
        base_revision: RevisionNo,
    },
    RestoreSourceRevisionMissing {
        inode_id: InodeId,
        restore_from_revision: RevisionNo,
    },
}

impl MetadataState {
    pub fn apply_committed_wal_ops(
        &self,
        committed_seq: ChangeSeq,
        ops: &[WalOp],
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for op in ops {
            match op {
                WalOp::CreateDir {
                    op_index,
                    inode_id,
                    parent_inode,
                    display_name,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::Dir,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *parent_inode,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_dir_writes_inode_and_direntry_rows",
                    );
                }
                WalOp::CreateFile {
                    op_index,
                    inode_id,
                    parent_inode,
                    display_name,
                    content_manifest_digest,
                } => {
                    metadata_state.inodes.push(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::File,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *parent_inode,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: RevisionNo(1),
                        committed_seq,
                        revision_op_index: *op_index,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_file_writes_inode_direntry_and_initial_revision",
                    );
                }
                WalOp::ReplaceFile {
                    op_index,
                    inode_id,
                    base_revision,
                    content_manifest_digest,
                } => {
                    let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                        MetadataApplyError::RevisionOverflow {
                            inode_id: *inode_id,
                            base_revision: *base_revision,
                        },
                    )?;
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision,
                        committed_seq,
                        revision_op_index: *op_index,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "replace_file_appends_new_revision_head",
                    );
                }
                WalOp::DeleteFile { op_index, inode_id } => {
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_op_index: *op_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_file_writes_tombstone_row",
                    );
                }
                WalOp::Rename {
                    op_index,
                    inode_id,
                    new_parent_inode,
                    new_display_name,
                } => {
                    metadata_state.direntries.push(DirentryRecord {
                        parent_inode_id: *new_parent_inode,
                        name_key: new_display_name.clone(),
                        display_name: new_display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: *op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "rename_appends_new_direntry_binding",
                    );
                }
                WalOp::DeleteSubtree { .. } => {
                    let WalOp::DeleteSubtree {
                        op_index,
                        root_inode,
                    } = op
                    else {
                        unreachable!();
                    };
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode,
                            tombstone_seq: committed_seq,
                            tombstone_op_index: *op_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_subtree_writes_tombstone_row",
                    );
                }
                WalOp::RestoreRevision {
                    op_index,
                    inode_id,
                    base_revision,
                    restore_from_revision,
                } => {
                    let next_revision = base_revision.0.checked_add(1).map(RevisionNo).ok_or(
                        MetadataApplyError::RevisionOverflow {
                            inode_id: *inode_id,
                            base_revision: *base_revision,
                        },
                    )?;
                    let source_revision = metadata_state
                        .revision_at_seq(*inode_id, *restore_from_revision, committed_seq)
                        .ok_or(MetadataApplyError::RestoreSourceRevisionMissing {
                            inode_id: *inode_id,
                            restore_from_revision: *restore_from_revision,
                        })?;
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision,
                        committed_seq,
                        revision_op_index: *op_index,
                        content_manifest_digest: source_revision.content_manifest_digest,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "restore_creates_new_revision_head",
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
                    revision.revision_op_index,
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
            .max_by_key(|revision| (revision.committed_seq, revision.revision_op_index))
            .cloned()
    }

    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_op_index))
            .cloned()
    }

    pub fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)
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
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_op_index))
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
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
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
    ) -> Option<DirentryRecord> {
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
    ) -> Vec<DirentryRecord> {
        let Some(parent) = self.visible_inode(parent_inode_id, base_seq) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .direntries
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.bind_seq <= base_seq
            })
            .filter(|direntry| {
                self.active_child_binding_at_seq(parent_inode_id, &direntry.name_key, base_seq)
                    .map(|active| {
                        active.child_inode_id == direntry.child_inode_id
                            && active.bind_seq == direntry.bind_seq
                            && active.bind_op_index == direntry.bind_op_index
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

            let next_absolute_path = join_absolute_path(&current_absolute_path, &component);
            let direntry = self
                .visible_child(current_inode_id, &component, base_seq)
                .ok_or_else(|| VisiblePathError::PathNotFound {
                    absolute_path: next_absolute_path.clone(),
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path = next_absolute_path;
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
    ) -> Option<DirentryRecord> {
        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        let latest_binding =
            self.latest_parent_binding_for_child_at_seq(direntry.child_inode_id, base_seq)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_op_index != direntry.bind_op_index
        {
            return None;
        }

        Some(direntry)
    }

    fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_op_index))
            .cloned()
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
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
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

#[cfg(test)]
mod tests {
    use super::{
        DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
        VisiblePathError,
    };
    use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo, WalOp};

    #[test]
    fn apply_committed_wal_ops_appends_create_dir_rows() {
        let applied = MetadataState::default()
            .apply_committed_wal_ops(
                ChangeSeq(42),
                &[WalOp::CreateDir {
                    op_index: 0,
                    inode_id: InodeId(501),
                    parent_inode: InodeId(2),
                    display_name: "drafts".to_owned(),
                }],
            )
            .expect("apply create_dir");

        assert_eq!(
            applied.metadata_state.inodes,
            vec![InodeRecord {
                inode_id: InodeId(501),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied.metadata_state.direntries,
            vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "drafts".to_owned(),
                display_name: "drafts".to_owned(),
                child_inode_id: InodeId(501),
                bind_seq: ChangeSeq(42),
                bind_op_index: 0,
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"create_dir_writes_inode_and_direntry_rows".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_appends_create_file_rows() {
        let applied = MetadataState::default()
            .apply_committed_wal_ops(
                ChangeSeq(42),
                &[WalOp::CreateFile {
                    op_index: 0,
                    inode_id: InodeId(501),
                    parent_inode: InodeId(2),
                    display_name: "note.txt".to_owned(),
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                }],
            )
            .expect("apply create_file");

        assert_eq!(
            applied.metadata_state.inodes,
            vec![InodeRecord {
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied.metadata_state.direntries,
            vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(501),
                bind_seq: ChangeSeq(42),
                bind_op_index: 0,
            }]
        );
        assert_eq!(
            applied.metadata_state.revisions,
            vec![RevisionRecord {
                inode_id: InodeId(501),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(42),
                revision_op_index: 0,
                content_manifest_digest: "sha256:note-v1".to_owned(),
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"create_file_writes_inode_direntry_and_initial_revision".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_appends_replace_file_revision() {
        let applied = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(17),
            }],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(17),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(17),
                committed_seq: ChangeSeq(41),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v17".to_owned(),
            }],
            subtree_tombstones: vec![SubtreeTombstoneRecord {
                root_inode_id: InodeId(99),
                tombstone_seq: ChangeSeq(40),
                tombstone_op_index: 0,
            }],
        }
        .apply_committed_wal_ops(
            ChangeSeq(42),
            &[WalOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(42),
                base_revision: RevisionNo(17),
                content_manifest_digest: "sha256:report-v18".to_owned(),
            }],
        )
        .expect("apply replace_file");

        assert_eq!(
            applied.metadata_state.revisions.last(),
            Some(&RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(18),
                committed_seq: ChangeSeq(42),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v18".to_owned(),
            })
        );
        assert!(applied
            .checked_invariants
            .contains(&"replace_file_appends_new_revision_head".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_appends_delete_subtree_tombstone() {
        let applied = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v1".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
        .apply_committed_wal_ops(
            ChangeSeq(42),
            &[WalOp::DeleteSubtree {
                op_index: 0,
                root_inode: InodeId(7),
            }],
        )
        .expect("apply delete_subtree");

        assert_eq!(
            applied.metadata_state.subtree_tombstones,
            vec![SubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(42),
                tombstone_op_index: 0,
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"delete_subtree_writes_tombstone_row".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_restores_historical_revision_as_new_head() {
        let applied = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(9),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(42),
                bind_seq: ChangeSeq(9),
                bind_op_index: 0,
            }],
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(3),
                    committed_seq: ChangeSeq(17),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v3".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(5),
                    committed_seq: ChangeSeq(52),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report-v5".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        }
        .apply_committed_wal_ops(
            ChangeSeq(53),
            &[WalOp::RestoreRevision {
                op_index: 0,
                inode_id: InodeId(42),
                base_revision: RevisionNo(5),
                restore_from_revision: RevisionNo(3),
            }],
        )
        .expect("apply restore_revision");

        assert_eq!(
            applied.metadata_state.revisions.last(),
            Some(&RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(6),
                committed_seq: ChangeSeq(53),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v3".to_owned(),
            })
        );
        assert!(applied
            .checked_invariants
            .contains(&"restore_creates_new_revision_head".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_appends_rename_binding_and_hides_old_visible_name() {
        let applied = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(5),
                committed_seq: ChangeSeq(52),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report-v5".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
        .apply_committed_wal_ops(
            ChangeSeq(53),
            &[WalOp::Rename {
                op_index: 0,
                inode_id: InodeId(42),
                new_parent_inode: InodeId(7),
                new_display_name: "report-renamed.txt".to_owned(),
            }],
        )
        .expect("apply rename");

        assert_eq!(
            applied
                .metadata_state
                .visible_child(InodeId(2), "report.txt", ChangeSeq(53)),
            None
        );
        assert_eq!(
            applied
                .metadata_state
                .visible_child(InodeId(7), "report-renamed.txt", ChangeSeq(53))
                .expect("renamed visible child")
                .child_inode_id,
            InodeId(42)
        );
        assert!(applied
            .checked_invariants
            .contains(&"rename_appends_new_direntry_binding".to_owned()));
    }

    #[test]
    fn visible_child_prefers_latest_slot_binding_when_name_is_reused() {
        let metadata_state = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(10),
                },
                InodeRecord {
                    inode_id: InodeId(77),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(30),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(10),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "archive.txt".to_owned(),
                    display_name: "archive.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(20),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(77),
                    bind_seq: ChangeSeq(30),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(10),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(77),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(30),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:note-v2".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        };

        assert_eq!(
            metadata_state
                .visible_child(InodeId(2), "note.txt", ChangeSeq(30))
                .expect("latest visible note.txt binding")
                .child_inode_id,
            InodeId(77)
        );
        let old_child_binding = metadata_state
            .current_parent_binding_for_child(InodeId(42), ChangeSeq(30))
            .expect("latest binding for renamed-away inode");
        assert_eq!(old_child_binding.parent_inode_id, InodeId(2));
        assert_eq!(old_child_binding.name_key, "archive.txt");
    }

    #[test]
    fn resolve_visible_path_accepts_root_and_nested_file() {
        let metadata_state = sample_path_metadata();

        let root = metadata_state
            .resolve_visible_path("/", ChangeSeq(3))
            .expect("resolve root");
        assert_eq!(root.absolute_path, "/");
        assert_eq!(root.inode_id, InodeId(1));
        assert_eq!(root.inode_kind, InodeKind::Dir);

        let file = metadata_state
            .resolve_visible_path("/docs/report.txt", ChangeSeq(3))
            .expect("resolve file");
        assert_eq!(file.absolute_path, "/docs/report.txt");
        assert_eq!(file.inode_id, InodeId(3));
        assert_eq!(file.inode_kind, InodeKind::File);
    }

    #[test]
    fn visible_children_only_returns_active_visible_bindings_in_order() {
        let metadata_state = sample_path_metadata();
        let children = metadata_state.visible_children(InodeId(1), ChangeSeq(3));
        let names = children
            .into_iter()
            .map(|child| child.display_name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["archive", "docs"]);
    }

    #[test]
    fn resolve_visible_path_rejects_invalid_and_non_directory_components() {
        let metadata_state = sample_path_metadata();

        let invalid = metadata_state
            .resolve_visible_path("/docs/../secret.txt", ChangeSeq(3))
            .expect_err("invalid path should fail");
        assert!(matches!(
            invalid,
            VisiblePathError::InvalidAbsolutePath { .. }
        ));

        let non_directory = metadata_state
            .resolve_visible_path("/docs/report.txt/child", ChangeSeq(3))
            .expect_err("file component should fail");
        assert!(matches!(
            non_directory,
            VisiblePathError::PathComponentNotDirectory { inode_id, .. } if inode_id == InodeId(3)
        ));
    }

    fn sample_path_metadata() -> MetadataState {
        MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(4),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(2),
                },
            ],
            direntries: vec![
                DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(3),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "archive".to_owned(),
                    display_name: "archive".to_owned(),
                    child_inode_id: InodeId(4),
                    bind_seq: ChangeSeq(2),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(3),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(1),
                revision_op_index: 0,
                content_manifest_digest: "sha256:report".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
    }
}
