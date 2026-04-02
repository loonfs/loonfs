use crate::{
    AppliedModelMetadataState, ModelDirentryRecord, ModelInodeRecord, ModelMetadataApplyError,
    ModelMetadataMutation, ModelMetadataPreconditionError, ModelMetadataState,
    ModelResolvedVisibleCreateTarget, ModelResolvedVisiblePath, ModelRevisionRecord,
    ModelSubtreeTombstoneRecord, ModelVisiblePathError, ModelVisiblePathMutationError,
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

    pub fn visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<ModelDirentryRecord> {
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
    ) -> Result<ModelResolvedVisiblePath, ModelVisiblePathError> {
        let components = parse_absolute_path_components(absolute_path)?;
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id, base_seq)
            .ok_or(ModelVisiblePathError::RootMissing)?;
        if components.is_empty() {
            return Ok(ModelResolvedVisiblePath {
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
                ModelVisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                },
            )?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(ModelVisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                });
            }

            let next_absolute_path = join_absolute_path(&current_absolute_path, &component);
            let direntry = self
                .visible_child(current_inode_id, &component, base_seq)
                .ok_or(ModelVisiblePathError::PathNotFound {
                    absolute_path: next_absolute_path.clone(),
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path = next_absolute_path;
        }

        let inode = self.visible_inode(current_inode_id, base_seq).ok_or(
            ModelVisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            },
        )?;
        Ok(ModelResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    pub fn resolve_visible_create_target(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<ModelResolvedVisibleCreateTarget, ModelVisiblePathMutationError> {
        let (parent_absolute_path, display_name) =
            split_parent_and_display_name(absolute_path).map_err(map_split_path_error)?;
        let parent = self
            .resolve_visible_path(&parent_absolute_path, base_seq)
            .map_err(ModelVisiblePathMutationError::VisiblePath)?;
        if let Some(existing) = self.visible_child(parent.inode_id, &display_name, base_seq) {
            let existing_inode = self
                .visible_inode(existing.child_inode_id, base_seq)
                .expect("visible child should have visible inode");
            return Err(ModelVisiblePathMutationError::DestinationOccupied {
                absolute_path: normalize_absolute_path(absolute_path)
                    .expect("validated create target should normalize"),
                inode_id: existing.child_inode_id,
                inode_kind: existing_inode.inode_kind,
            });
        }

        Ok(ModelResolvedVisibleCreateTarget {
            absolute_path: normalize_absolute_path(absolute_path)
                .expect("validated create target should normalize"),
            parent_absolute_path,
            parent_inode_id: parent.inode_id,
            display_name,
        })
    }

    pub fn resolve_visible_file_target(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<ModelResolvedVisiblePath, ModelVisiblePathMutationError> {
        let resolved = self
            .resolve_visible_path(absolute_path, base_seq)
            .map_err(ModelVisiblePathMutationError::VisiblePath)?;
        if resolved.absolute_path == "/" {
            return Err(ModelVisiblePathMutationError::RootPathRejected {
                absolute_path: resolved.absolute_path,
            });
        }
        if resolved.inode_kind != InodeKind::File {
            return Err(ModelVisiblePathMutationError::FileRequired {
                absolute_path: resolved.absolute_path,
                inode_id: resolved.inode_id,
                inode_kind: resolved.inode_kind,
            });
        }

        Ok(resolved)
    }

    pub fn ensure_distinct_visible_paths(
        &self,
        source_absolute_path: &str,
        destination_absolute_path: &str,
    ) -> Result<(), ModelVisiblePathMutationError> {
        let source = normalize_absolute_path(source_absolute_path)
            .map_err(ModelVisiblePathMutationError::VisiblePath)?;
        let destination = normalize_absolute_path(destination_absolute_path)
            .map_err(ModelVisiblePathMutationError::VisiblePath)?;
        if source == destination {
            return Err(
                ModelVisiblePathMutationError::IdenticalSourceAndDestination {
                    absolute_path: source,
                },
            );
        }

        Ok(())
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

fn parse_absolute_path_components(
    absolute_path: &str,
) -> Result<Vec<String>, ModelVisiblePathError> {
    if !absolute_path.starts_with('/') {
        return Err(ModelVisiblePathError::InvalidAbsolutePath {
            absolute_path: absolute_path.to_owned(),
        });
    }

    let mut components = Vec::new();
    for component in absolute_path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(ModelVisiblePathError::InvalidAbsolutePath {
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

fn normalize_absolute_path(absolute_path: &str) -> Result<String, ModelVisiblePathError> {
    let components = parse_absolute_path_components(absolute_path)?;
    if components.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

enum SplitPathError {
    RootPathRejected { absolute_path: String },
    VisiblePath(ModelVisiblePathError),
}

fn map_split_path_error(error: SplitPathError) -> ModelVisiblePathMutationError {
    match error {
        SplitPathError::RootPathRejected { absolute_path } => {
            ModelVisiblePathMutationError::RootPathRejected { absolute_path }
        }
        SplitPathError::VisiblePath(error) => ModelVisiblePathMutationError::VisiblePath(error),
    }
}

fn split_parent_and_display_name(absolute_path: &str) -> Result<(String, String), SplitPathError> {
    let normalized = normalize_absolute_path(absolute_path).map_err(SplitPathError::VisiblePath)?;
    if normalized == "/" {
        return Err(SplitPathError::RootPathRejected {
            absolute_path: normalized,
        });
    }

    let mut components =
        parse_absolute_path_components(&normalized).map_err(SplitPathError::VisiblePath)?;
    let display_name = components
        .pop()
        .expect("non-root normalized absolute path should have leaf");
    let parent_absolute_path = if components.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", components.join("/"))
    };
    Ok((parent_absolute_path, display_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModelDirentryRecord, ModelInodeRecord, ModelMetadataState, ModelRevisionRecord};

    #[test]
    fn resolve_visible_path_accepts_root_and_nested_file() {
        let metadata = sample_metadata();

        let root = metadata
            .resolve_visible_path("/", ChangeSeq(3))
            .expect("resolve root");
        assert_eq!(root.absolute_path, "/");
        assert_eq!(root.inode_id, InodeId(1));
        assert_eq!(root.inode_kind, InodeKind::Dir);

        let file = metadata
            .resolve_visible_path("/docs/report.txt", ChangeSeq(3))
            .expect("resolve file");
        assert_eq!(file.absolute_path, "/docs/report.txt");
        assert_eq!(file.inode_id, InodeId(3));
        assert_eq!(file.inode_kind, InodeKind::File);
        assert_eq!(file.parent_inode_id, Some(InodeId(2)));
    }

    #[test]
    fn visible_children_only_returns_active_visible_bindings_in_order() {
        let metadata = sample_metadata();
        let children = metadata.visible_children(InodeId(1), ChangeSeq(3));
        let names = children
            .into_iter()
            .map(|child| child.display_name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["archive", "docs"]);
    }

    #[test]
    fn resolve_visible_path_rejects_invalid_and_non_directory_components() {
        let metadata = sample_metadata();

        let invalid = metadata
            .resolve_visible_path("/docs/../secret.txt", ChangeSeq(3))
            .expect_err("invalid path should fail");
        assert!(matches!(
            invalid,
            ModelVisiblePathError::InvalidAbsolutePath { .. }
        ));

        let non_directory = metadata
            .resolve_visible_path("/docs/report.txt/child", ChangeSeq(3))
            .expect_err("file component should fail");
        assert!(matches!(
            non_directory,
            ModelVisiblePathError::PathComponentNotDirectory { inode_id, .. } if inode_id == InodeId(3)
        ));
    }

    #[test]
    fn resolve_visible_create_target_accepts_absent_leaf_under_visible_directory() {
        let metadata = sample_metadata();

        let target = metadata
            .resolve_visible_create_target("/docs/draft.txt", ChangeSeq(3))
            .expect("resolve create target");

        assert_eq!(target.absolute_path, "/docs/draft.txt");
        assert_eq!(target.parent_absolute_path, "/docs");
        assert_eq!(target.parent_inode_id, InodeId(2));
        assert_eq!(target.display_name, "draft.txt");
    }

    #[test]
    fn resolve_visible_create_target_rejects_root_and_occupied_destinations() {
        let metadata = sample_metadata();

        let root = metadata
            .resolve_visible_create_target("/", ChangeSeq(3))
            .expect_err("root create target should fail");
        assert!(matches!(
            root,
            ModelVisiblePathMutationError::RootPathRejected { .. }
        ));

        let occupied = metadata
            .resolve_visible_create_target("/docs/report.txt", ChangeSeq(3))
            .expect_err("occupied create target should fail");
        assert!(matches!(
            occupied,
            ModelVisiblePathMutationError::DestinationOccupied { inode_id, .. }
                if inode_id == InodeId(3)
        ));
    }

    #[test]
    fn ensure_distinct_visible_paths_rejects_identical_normalized_paths() {
        let metadata = sample_metadata();
        let error = metadata
            .ensure_distinct_visible_paths("/docs//report.txt", "/docs/report.txt")
            .expect_err("identical normalized paths should fail");
        assert!(matches!(
            error,
            ModelVisiblePathMutationError::IdenticalSourceAndDestination { absolute_path }
                if absolute_path == "/docs/report.txt"
        ));
    }

    #[test]
    fn resolve_visible_file_target_accepts_visible_file_and_rejects_root_or_directory() {
        let metadata = sample_metadata();

        let file = metadata
            .resolve_visible_file_target("/docs/report.txt", ChangeSeq(3))
            .expect("resolve visible file target");
        assert_eq!(file.inode_id, InodeId(3));
        assert_eq!(file.absolute_path, "/docs/report.txt");

        let root = metadata
            .resolve_visible_file_target("/", ChangeSeq(3))
            .expect_err("root should be rejected");
        assert!(matches!(
            root,
            ModelVisiblePathMutationError::RootPathRejected { absolute_path }
                if absolute_path == "/"
        ));

        let directory = metadata
            .resolve_visible_file_target("/docs", ChangeSeq(3))
            .expect_err("directory should be rejected");
        assert!(matches!(
            directory,
            ModelVisiblePathMutationError::FileRequired { inode_id, inode_kind, .. }
                if inode_id == InodeId(2) && inode_kind == InodeKind::Dir
        ));
    }

    fn sample_metadata() -> ModelMetadataState {
        ModelMetadataState {
            inodes: vec![
                ModelInodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                ModelInodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(4),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(2),
                },
            ],
            direntries: vec![
                ModelDirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(3),
                    bind_seq: ChangeSeq(1),
                    bind_op_index: 0,
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "archive".to_owned(),
                    display_name: "archive".to_owned(),
                    child_inode_id: InodeId(4),
                    bind_seq: ChangeSeq(2),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![ModelRevisionRecord {
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
