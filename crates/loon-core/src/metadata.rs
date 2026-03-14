use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo, WalOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataApplyError {
    UnsupportedWalOp {
        op_name: String,
    },
    RevisionOverflow {
        inode_id: InodeId,
        base_revision: RevisionNo,
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
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_dir_writes_inode_and_direntry_rows",
                    );
                }
                WalOp::CreateFile {
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
                    });
                    metadata_state.revisions.push(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: RevisionNo(1),
                        committed_seq,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_file_writes_inode_direntry_and_initial_revision",
                    );
                }
                WalOp::ReplaceFile {
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
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "replace_file_appends_new_revision_head",
                    );
                }
                WalOp::Rename { .. } => {
                    return Err(MetadataApplyError::UnsupportedWalOp {
                        op_name: "rename".to_owned(),
                    });
                }
                WalOp::DeleteSubtree { .. } => {
                    let WalOp::DeleteSubtree { root_inode } = op else {
                        unreachable!();
                    };
                    metadata_state
                        .subtree_tombstones
                        .push(SubtreeTombstoneRecord {
                            root_inode_id: *root_inode,
                            tombstone_seq: committed_seq,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_subtree_writes_tombstone_row",
                    );
                }
                WalOp::RestoreRevision { .. } => {
                    return Err(MetadataApplyError::UnsupportedWalOp {
                        op_name: "restore_revision".to_owned(),
                    });
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
            .max_by_key(|revision| revision.revision_no)
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
            .max_by_key(|direntry| direntry.bind_seq)
            .cloned()
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
            .max_by_key(|tombstone| tombstone.tombstone_seq)
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

        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
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
            .max_by_key(|direntry| direntry.bind_seq)
            .cloned()
    }
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DirentryRecord, InodeRecord, MetadataApplyError, MetadataState, RevisionRecord,
        SubtreeTombstoneRecord,
    };
    use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo, WalOp};

    #[test]
    fn apply_committed_wal_ops_appends_create_dir_rows() {
        let applied = MetadataState::default()
            .apply_committed_wal_ops(
                ChangeSeq(42),
                &[WalOp::CreateDir {
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
            }]
        );
        assert_eq!(
            applied.metadata_state.revisions,
            vec![RevisionRecord {
                inode_id: InodeId(501),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(42),
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
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(17),
                committed_seq: ChangeSeq(41),
                content_manifest_digest: "sha256:report-v17".to_owned(),
            }],
            subtree_tombstones: vec![SubtreeTombstoneRecord {
                root_inode_id: InodeId(99),
                tombstone_seq: ChangeSeq(40),
            }],
        }
        .apply_committed_wal_ops(
            ChangeSeq(42),
            &[WalOp::ReplaceFile {
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
                },
                DirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                },
            ],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                content_manifest_digest: "sha256:report-v1".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
        .apply_committed_wal_ops(
            ChangeSeq(42),
            &[WalOp::DeleteSubtree {
                root_inode: InodeId(7),
            }],
        )
        .expect("apply delete_subtree");

        assert_eq!(
            applied.metadata_state.subtree_tombstones,
            vec![SubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(42),
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"delete_subtree_writes_tombstone_row".to_owned()));
    }

    #[test]
    fn apply_committed_wal_ops_rejects_unsupported_op() {
        let error = MetadataState::default()
            .apply_committed_wal_ops(
                ChangeSeq(42),
                &[WalOp::Rename {
                    inode_id: InodeId(42),
                    new_parent_inode: InodeId(2),
                    new_display_name: "renamed.txt".to_owned(),
                }],
            )
            .expect_err("rename should be unsupported");

        assert_eq!(
            error,
            MetadataApplyError::UnsupportedWalOp {
                op_name: "rename".to_owned(),
            }
        );
    }
}
