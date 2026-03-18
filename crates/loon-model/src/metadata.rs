use crate::{
    AppliedModelMetadataState, ModelDirentryRecord, ModelInodeRecord, ModelMetadataApplyError,
    ModelMetadataMutation, ModelMetadataPreconditionError, ModelMetadataState, ModelRevisionRecord,
    ModelSubtreeTombstoneRecord,
};
use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo};
use std::collections::BTreeSet;

impl ModelMetadataState {
    pub fn apply_committed_mutations(
        &self,
        committed_seq: ChangeSeq,
        mutations: &[ModelMetadataMutation],
    ) -> Result<AppliedModelMetadataState, ModelMetadataApplyError> {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for (op_index, mutation) in mutations.iter().enumerate() {
            let op_index = u32::try_from(op_index).expect("mutation index should fit in u32");
            match mutation {
                ModelMetadataMutation::CreateDir {
                    inode_id,
                    parent_inode_id,
                    display_name,
                } => {
                    metadata_state.inodes.push(ModelInodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::Dir,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_dir_writes_inode_and_direntry_rows",
                    );
                }
                ModelMetadataMutation::CreateFile {
                    inode_id,
                    parent_inode_id,
                    display_name,
                    content_manifest_digest,
                } => {
                    metadata_state.inodes.push(ModelInodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::File,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: op_index,
                    });
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: RevisionNo(1),
                        committed_seq,
                        revision_op_index: op_index,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_file_writes_inode_direntry_and_initial_revision",
                    );
                }
                ModelMetadataMutation::ReplaceFile {
                    inode_id,
                    base_revision_no,
                    content_manifest_digest,
                } => {
                    let next_revision_no =
                        base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                            ModelMetadataApplyError::RevisionOverflow {
                                inode_id: *inode_id,
                                base_revision_no: *base_revision_no,
                            },
                        )?;
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision_no,
                        committed_seq,
                        revision_op_index: op_index,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "replace_file_appends_new_revision_head",
                    );
                }
                ModelMetadataMutation::Rename {
                    inode_id,
                    new_parent_inode_id,
                    new_display_name,
                } => {
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *new_parent_inode_id,
                        name_key: new_display_name.clone(),
                        display_name: new_display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                        bind_op_index: op_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "rename_appends_new_direntry_binding",
                    );
                }
                ModelMetadataMutation::RestoreRevision {
                    inode_id,
                    base_revision_no,
                    restore_from_revision_no,
                } => {
                    let next_revision_no =
                        base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                            ModelMetadataApplyError::RevisionOverflow {
                                inode_id: *inode_id,
                                base_revision_no: *base_revision_no,
                            },
                        )?;
                    let source_revision = metadata_state
                        .revision_at_seq(*inode_id, *restore_from_revision_no, committed_seq)
                        .ok_or(ModelMetadataApplyError::RestoreSourceRevisionMissing {
                            inode_id: *inode_id,
                            restore_from_revision_no: *restore_from_revision_no,
                        })?;
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision_no,
                        committed_seq,
                        revision_op_index: op_index,
                        content_manifest_digest: source_revision.content_manifest_digest,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "restore_creates_new_revision_head",
                    );
                }
                ModelMetadataMutation::DeleteSubtree { root_inode_id } => {
                    metadata_state
                        .subtree_tombstones
                        .push(ModelSubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                            tombstone_op_index: op_index,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_subtree_writes_tombstone_row",
                    );
                }
            }
        }

        Ok(AppliedModelMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<ModelInodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    pub fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelRevisionRecord> {
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
    ) -> Option<ModelRevisionRecord> {
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
    ) -> Option<ModelDirentryRecord> {
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
    ) -> Option<ModelDirentryRecord> {
        self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelSubtreeTombstoneRecord> {
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
    ) -> Option<ModelSubtreeTombstoneRecord> {
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

    pub fn visible_inode(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelInodeRecord> {
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
    ) -> Option<ModelRevisionRecord> {
        self.visible_inode(inode_id, base_seq)?;
        self.latest_revision_head_at_seq(inode_id, base_seq)
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn ensure_child_name_absent(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let parent = self
            .inode_at_seq(parent_inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::ParentMissing { parent_inode_id })?;
        if parent.inode_kind != InodeKind::Dir {
            return Err(ModelMetadataPreconditionError::ParentNotDirectory {
                parent_inode_id,
                actual_kind: parent.inode_kind,
            });
        }

        if let Some(existing) = self.visible_child(parent_inode_id, name_key, base_seq) {
            return Err(ModelMetadataPreconditionError::ChildNameCollision {
                parent_inode_id,
                name_key: name_key.to_owned(),
                child_inode_id: existing.child_inode_id,
            });
        }

        Ok(())
    }

    pub fn ensure_inode_revision_is(
        &self,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::File {
            return Err(ModelMetadataPreconditionError::InodeNotFile {
                inode_id,
                actual_kind: inode.inode_kind,
            });
        }

        let actual_revision_no = self
            .latest_revision_head_at_seq(inode_id, base_seq)
            .map(|revision| revision.revision_no);
        if actual_revision_no != Some(expected_revision_no) {
            return Err(ModelMetadataPreconditionError::InodeRevisionMismatch {
                inode_id,
                expected: expected_revision_no,
                actual: actual_revision_no,
            });
        }

        Ok(())
    }

    pub fn ensure_inode_is_directory(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::Dir {
            return Err(ModelMetadataPreconditionError::InodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            });
        }

        Ok(())
    }

    pub fn ensure_restore_source_revision_exists(
        &self,
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        restore_from_revision: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        if self
            .revision_at_seq(inode_id, restore_from_revision, base_seq)
            .is_none()
        {
            return Err(ModelMetadataPreconditionError::SourceRevisionMissing {
                inode_id,
                restore_from_revision,
            });
        }
        if restore_from_revision >= base_revision_no {
            return Err(
                ModelMetadataPreconditionError::SourceRevisionNotHistorical {
                    inode_id,
                    base_revision_no,
                    restore_from_revision,
                },
            );
        }

        Ok(())
    }

    pub fn ensure_rename_source_binding_exists(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        self.inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if self
            .current_parent_binding_for_child(inode_id, base_seq)
            .is_none()
        {
            return Err(ModelMetadataPreconditionError::SourceBindingMissing { inode_id });
        }

        Ok(())
    }

    pub fn ensure_rename_does_not_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::Dir {
            return Ok(());
        }
        if self.is_ancestor_of(inode_id, new_parent_inode_id, base_seq) {
            return Err(ModelMetadataPreconditionError::RenameWouldCycle {
                inode_id,
                new_parent_inode_id,
            });
        }

        Ok(())
    }

    pub fn ensure_ancestors_not_subtree_deleted(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        if let Some(tombstone) = self.covering_subtree_tombstone(inode_id, base_seq) {
            return Err(
                ModelMetadataPreconditionError::AncestorCoveredBySubtreeTombstone {
                    inode_id,
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.tombstone_seq,
                },
            );
        }

        Ok(())
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
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
    ) -> Option<ModelDirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_op_index))
            .cloned()
    }

    fn is_ancestor_of(
        &self,
        ancestor_inode_id: InodeId,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == ancestor_inode_id {
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
