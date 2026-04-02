use crate::commit::CommitOp;
use loon_types::{ChangeSeq, InodeId, InodeKind, RevisionNo, WalOp};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
pub struct ResolvedVisibleCreateTarget {
    pub absolute_path: String,
    pub parent_absolute_path: String,
    pub parent_inode_id: InodeId,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleSubtreeDirectory {
    pub relative_path: String,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub parent_inode_id: InodeId,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleSubtreeFile {
    pub relative_path: String,
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub parent_inode_id: InodeId,
    pub display_name: String,
    pub revision_no: RevisionNo,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleDirectorySubtree {
    pub root: ResolvedVisiblePath,
    #[serde(default)]
    pub directories: Vec<VisibleSubtreeDirectory>,
    #[serde(default)]
    pub files: Vec<VisibleSubtreeFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisiblePathMutationError {
    #[error(transparent)]
    VisiblePath(VisiblePathError),
    #[error("mutation target must not be root path `{absolute_path}`")]
    RootPathRejected { absolute_path: String },
    #[error(
        "mutation path `{absolute_path}` must resolve to visible file inode `{inode_id:?}` kind `{inode_kind:?}`"
    )]
    FileRequired {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error(
        "destination path `{absolute_path}` is already occupied by inode `{inode_id:?}` kind `{inode_kind:?}`"
    )]
    DestinationOccupied {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error("source and destination resolve to identical path `{absolute_path}`")]
    IdenticalSourceAndDestination { absolute_path: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisibleSubtreeError {
    #[error(transparent)]
    VisiblePath(VisiblePathError),
    #[error(
        "recursive subtree root must be a directory at `{absolute_path}` (inode `{inode_id:?}` kind `{inode_kind:?}`)"
    )]
    RootNotDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error(
        "recursive subtree walk does not support descendant kind `{inode_kind:?}` at `{absolute_path}` (inode `{inode_id:?}`)"
    )]
    UnsupportedDescendant {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[error("visible file at `{absolute_path}` is missing its current revision head")]
    FileRevisionMissing {
        absolute_path: String,
        inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursivePutLocalEntry {
    pub relative_path: String,
    pub inode_kind: InodeKind,
    pub content_manifest_digest: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursivePutDirectory {
    pub relative_path: String,
    pub absolute_path: String,
    pub parent_relative_path: Option<String>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursivePutFile {
    pub relative_path: String,
    pub absolute_path: String,
    pub parent_relative_path: String,
    pub display_name: String,
    pub content_manifest_digest: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursivePutPlan {
    pub target: ResolvedVisibleCreateTarget,
    #[serde(default)]
    pub directories: Vec<RecursivePutDirectory>,
    #[serde(default)]
    pub files: Vec<RecursivePutFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum RecursivePutPlanError {
    #[error(transparent)]
    VisiblePathMutation(VisiblePathMutationError),
    #[error("recursive put local subtree is missing its root directory entry")]
    RootEntryMissing,
    #[error("recursive put root local entry must be a directory")]
    RootEntryMustBeDirectory,
    #[error("recursive put entry has invalid relative path `{relative_path}`")]
    InvalidRelativePath { relative_path: String },
    #[error(
        "recursive put does not support local kind `{inode_kind:?}` at relative path `{relative_path}`"
    )]
    UnsupportedLocalKind {
        relative_path: String,
        inode_kind: InodeKind,
    },
    #[error("recursive put local subtree contains duplicate relative path `{relative_path}`")]
    DuplicateRelativePath { relative_path: String },
    #[error(
        "recursive put local subtree entry `{relative_path}` is missing parent `{parent_relative_path}`"
    )]
    ParentEntryMissing {
        relative_path: String,
        parent_relative_path: String,
    },
    #[error(
        "recursive put local subtree entry `{relative_path}` has non-directory parent `{parent_relative_path}`"
    )]
    ParentEntryNotDirectory {
        relative_path: String,
        parent_relative_path: String,
    },
    #[error("recursive put file entry `{relative_path}` is missing content metadata")]
    FileEntryMissingContent { relative_path: String },
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

    pub fn resolve_visible_create_target(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisibleCreateTarget, VisiblePathMutationError> {
        let (parent_absolute_path, display_name) =
            split_parent_and_display_name(absolute_path).map_err(map_split_path_error)?;
        let parent = self
            .resolve_visible_path(&parent_absolute_path, base_seq)
            .map_err(VisiblePathMutationError::VisiblePath)?;
        if let Some(existing) = self.visible_child(parent.inode_id, &display_name, base_seq) {
            let existing_inode = self
                .visible_inode(existing.child_inode_id, base_seq)
                .expect("visible child should have visible inode");
            return Err(VisiblePathMutationError::DestinationOccupied {
                absolute_path: normalize_absolute_path(absolute_path)
                    .expect("validated create target should normalize"),
                inode_id: existing.child_inode_id,
                inode_kind: existing_inode.inode_kind,
            });
        }

        Ok(ResolvedVisibleCreateTarget {
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
    ) -> Result<ResolvedVisiblePath, VisiblePathMutationError> {
        let resolved = self
            .resolve_visible_path(absolute_path, base_seq)
            .map_err(VisiblePathMutationError::VisiblePath)?;
        if resolved.absolute_path == "/" {
            return Err(VisiblePathMutationError::RootPathRejected {
                absolute_path: resolved.absolute_path,
            });
        }
        if resolved.inode_kind != InodeKind::File {
            return Err(VisiblePathMutationError::FileRequired {
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
    ) -> Result<(), VisiblePathMutationError> {
        let source = normalize_absolute_path(source_absolute_path)
            .map_err(VisiblePathMutationError::VisiblePath)?;
        let destination = normalize_absolute_path(destination_absolute_path)
            .map_err(VisiblePathMutationError::VisiblePath)?;
        if source == destination {
            return Err(VisiblePathMutationError::IdenticalSourceAndDestination {
                absolute_path: source,
            });
        }

        Ok(())
    }

    pub fn collect_visible_directory_subtree(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
    ) -> Result<VisibleDirectorySubtree, VisibleSubtreeError> {
        let root = self
            .resolve_visible_path(absolute_path, base_seq)
            .map_err(VisibleSubtreeError::VisiblePath)?;
        if root.inode_kind != InodeKind::Dir {
            return Err(VisibleSubtreeError::RootNotDirectory {
                absolute_path: root.absolute_path,
                inode_id: root.inode_id,
                inode_kind: root.inode_kind,
            });
        }

        let mut directories = Vec::new();
        let mut files = Vec::new();
        self.collect_visible_subtree_children(
            &root.absolute_path,
            &root.absolute_path,
            root.inode_id,
            base_seq,
            &mut directories,
            &mut files,
        )?;

        Ok(VisibleDirectorySubtree {
            root,
            directories,
            files,
        })
    }

    pub fn plan_recursive_put_subtree(
        &self,
        absolute_path: &str,
        base_seq: ChangeSeq,
        local_entries: &[RecursivePutLocalEntry],
    ) -> Result<RecursivePutPlan, RecursivePutPlanError> {
        let target = self
            .resolve_visible_create_target(absolute_path, base_seq)
            .map_err(RecursivePutPlanError::VisiblePathMutation)?;
        let normalized = normalize_recursive_put_entries(local_entries)?;
        let root = normalized
            .get("")
            .ok_or(RecursivePutPlanError::RootEntryMissing)?;
        if root.inode_kind != InodeKind::Dir {
            return Err(RecursivePutPlanError::RootEntryMustBeDirectory);
        }

        let mut directories = Vec::new();
        let mut files = Vec::new();
        for (relative_path, entry) in &normalized {
            if relative_path.is_empty() {
                directories.push(RecursivePutDirectory {
                    relative_path: String::new(),
                    absolute_path: target.absolute_path.clone(),
                    parent_relative_path: None,
                    display_name: target.display_name.clone(),
                });
                continue;
            }

            let parent_relative_path = parent_relative_path(relative_path).ok_or_else(|| {
                RecursivePutPlanError::ParentEntryMissing {
                    relative_path: relative_path.clone(),
                    parent_relative_path: String::new(),
                }
            })?;
            let parent_entry = normalized.get(&parent_relative_path).ok_or_else(|| {
                RecursivePutPlanError::ParentEntryMissing {
                    relative_path: relative_path.clone(),
                    parent_relative_path: parent_relative_path.clone(),
                }
            })?;
            if parent_entry.inode_kind != InodeKind::Dir {
                return Err(RecursivePutPlanError::ParentEntryNotDirectory {
                    relative_path: relative_path.clone(),
                    parent_relative_path,
                });
            }

            let absolute_path = join_absolute_path(&target.absolute_path, relative_path);
            let display_name = leaf_display_name(relative_path).expect("non-root relative path");
            match entry.inode_kind {
                InodeKind::Dir => directories.push(RecursivePutDirectory {
                    relative_path: relative_path.clone(),
                    absolute_path,
                    parent_relative_path: Some(parent_relative_path),
                    display_name,
                }),
                InodeKind::File => {
                    let content_manifest_digest = entry
                        .content_manifest_digest
                        .clone()
                        .ok_or_else(|| RecursivePutPlanError::FileEntryMissingContent {
                            relative_path: relative_path.clone(),
                        })?;
                    let size_bytes = entry.size_bytes.ok_or_else(|| {
                        RecursivePutPlanError::FileEntryMissingContent {
                            relative_path: relative_path.clone(),
                        }
                    })?;
                    files.push(RecursivePutFile {
                        relative_path: relative_path.clone(),
                        absolute_path,
                        parent_relative_path,
                        display_name,
                        content_manifest_digest,
                        size_bytes,
                    });
                }
                InodeKind::Symlink | InodeKind::Mount => {
                    return Err(RecursivePutPlanError::UnsupportedLocalKind {
                        relative_path: relative_path.clone(),
                        inode_kind: entry.inode_kind.clone(),
                    });
                }
            }
        }

        Ok(RecursivePutPlan {
            target,
            directories,
            files,
        })
    }

    pub fn build_recursive_put_commit_ops(
        &self,
        plan: &RecursivePutPlan,
        next_inode_id: InodeId,
    ) -> Vec<CommitOp> {
        let mut ops = Vec::new();
        let mut current_inode = next_inode_id;
        let mut directory_inode_ids = BTreeMap::new();

        for directory in &plan.directories {
            let parent_inode = match &directory.parent_relative_path {
                None => plan.target.parent_inode_id,
                Some(parent_relative_path) => *directory_inode_ids
                    .get(parent_relative_path)
                    .expect("directory parent should be allocated earlier"),
            };
            ops.push(CommitOp::CreateDir {
                parent_inode,
                display_name: directory.display_name.clone(),
            });
            directory_inode_ids.insert(directory.relative_path.clone(), current_inode);
            current_inode = InodeId(
                current_inode
                    .0
                    .checked_add(1)
                    .expect("recursive put inode allocation should not overflow"),
            );
        }

        for file in &plan.files {
            let parent_inode = *directory_inode_ids
                .get(&file.parent_relative_path)
                .expect("file parent directory should be allocated earlier");
            ops.push(CommitOp::CreateFile {
                parent_inode,
                display_name: file.display_name.clone(),
                content_manifest_digest: file.content_manifest_digest.clone(),
            });
            current_inode = InodeId(
                current_inode
                    .0
                    .checked_add(1)
                    .expect("recursive put inode allocation should not overflow"),
            );
        }

        ops
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

    fn collect_visible_subtree_children(
        &self,
        subtree_root_absolute_path: &str,
        parent_absolute_path: &str,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
        directories: &mut Vec<VisibleSubtreeDirectory>,
        files: &mut Vec<VisibleSubtreeFile>,
    ) -> Result<(), VisibleSubtreeError> {
        for child in self.visible_children(parent_inode_id, base_seq) {
            let child_absolute_path = join_absolute_path(parent_absolute_path, &child.display_name);
            let child_inode = self
                .visible_inode(child.child_inode_id, base_seq)
                .ok_or_else(|| {
                    VisibleSubtreeError::VisiblePath(VisiblePathError::PathNotFound {
                        absolute_path: child_absolute_path.clone(),
                    })
                })?;
            let relative_path =
                relative_path_from_root(subtree_root_absolute_path, &child_absolute_path);
            match child_inode.inode_kind {
                InodeKind::Dir => {
                    directories.push(VisibleSubtreeDirectory {
                        relative_path,
                        absolute_path: child_absolute_path.clone(),
                        inode_id: child.child_inode_id,
                        parent_inode_id: child.parent_inode_id,
                        display_name: child.display_name.clone(),
                    });
                    self.collect_visible_subtree_children(
                        subtree_root_absolute_path,
                        &child_absolute_path,
                        child.child_inode_id,
                        base_seq,
                        directories,
                        files,
                    )?;
                }
                InodeKind::File => {
                    let revision = self
                        .latest_revision_head_at_seq(child.child_inode_id, base_seq)
                        .ok_or(VisibleSubtreeError::FileRevisionMissing {
                            absolute_path: child_absolute_path.clone(),
                            inode_id: child.child_inode_id,
                        })?;
                    files.push(VisibleSubtreeFile {
                        relative_path,
                        absolute_path: child_absolute_path,
                        inode_id: child.child_inode_id,
                        parent_inode_id: child.parent_inode_id,
                        display_name: child.display_name,
                        revision_no: revision.revision_no,
                        content_manifest_digest: revision.content_manifest_digest,
                    });
                }
                InodeKind::Symlink | InodeKind::Mount => {
                    return Err(VisibleSubtreeError::UnsupportedDescendant {
                        absolute_path: child_absolute_path,
                        inode_id: child.child_inode_id,
                        inode_kind: child_inode.inode_kind,
                    });
                }
            }
        }

        Ok(())
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

fn relative_path_from_root(root_absolute_path: &str, absolute_path: &str) -> String {
    if root_absolute_path == "/" {
        absolute_path.trim_start_matches('/').to_owned()
    } else {
        absolute_path
            .strip_prefix(&format!("{root_absolute_path}/"))
            .expect("descendant path should be rooted under subtree path")
            .to_owned()
    }
}

fn normalize_recursive_put_entries(
    local_entries: &[RecursivePutLocalEntry],
) -> Result<BTreeMap<String, RecursivePutLocalEntry>, RecursivePutPlanError> {
    let mut normalized = BTreeMap::new();
    for entry in local_entries {
        let relative_path = normalize_relative_path(&entry.relative_path)?;
        if matches!(entry.inode_kind, InodeKind::Symlink | InodeKind::Mount) {
            return Err(RecursivePutPlanError::UnsupportedLocalKind {
                relative_path,
                inode_kind: entry.inode_kind.clone(),
            });
        }
        let normalized_entry = RecursivePutLocalEntry {
            relative_path: relative_path.clone(),
            inode_kind: entry.inode_kind.clone(),
            content_manifest_digest: entry.content_manifest_digest.clone(),
            size_bytes: entry.size_bytes,
        };
        if normalized
            .insert(relative_path.clone(), normalized_entry)
            .is_some()
        {
            return Err(RecursivePutPlanError::DuplicateRelativePath { relative_path });
        }
    }
    Ok(normalized)
}

fn normalize_relative_path(relative_path: &str) -> Result<String, RecursivePutPlanError> {
    if relative_path.is_empty() {
        return Ok(String::new());
    }

    let mut components = Vec::new();
    for component in relative_path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(RecursivePutPlanError::InvalidRelativePath {
                relative_path: relative_path.to_owned(),
            });
        }
        components.push(component);
    }
    Ok(components.join("/"))
}

fn parent_relative_path(relative_path: &str) -> Option<String> {
    if relative_path.is_empty() {
        return None;
    }
    match relative_path.rsplit_once('/') {
        Some((parent, _)) => Some(parent.to_owned()),
        None => Some(String::new()),
    }
}

fn leaf_display_name(relative_path: &str) -> Option<String> {
    relative_path
        .rsplit('/')
        .next()
        .map(std::borrow::ToOwned::to_owned)
}

fn normalize_absolute_path(absolute_path: &str) -> Result<String, VisiblePathError> {
    let components = parse_absolute_path_components(absolute_path)?;
    if components.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

enum SplitPathError {
    RootPathRejected { absolute_path: String },
    VisiblePath(VisiblePathError),
}

fn map_split_path_error(error: SplitPathError) -> VisiblePathMutationError {
    match error {
        SplitPathError::RootPathRejected { absolute_path } => {
            VisiblePathMutationError::RootPathRejected { absolute_path }
        }
        SplitPathError::VisiblePath(error) => VisiblePathMutationError::VisiblePath(error),
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
    use super::{
        DirentryRecord, InodeRecord, MetadataState, RecursivePutLocalEntry, RevisionRecord,
        SubtreeTombstoneRecord, VisiblePathError, VisiblePathMutationError, VisibleSubtreeError,
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

    #[test]
    fn resolve_visible_create_target_accepts_absent_leaf_under_visible_directory() {
        let metadata_state = sample_path_metadata();

        let target = metadata_state
            .resolve_visible_create_target("/docs/draft.txt", ChangeSeq(3))
            .expect("resolve create target");

        assert_eq!(target.absolute_path, "/docs/draft.txt");
        assert_eq!(target.parent_absolute_path, "/docs");
        assert_eq!(target.parent_inode_id, InodeId(2));
        assert_eq!(target.display_name, "draft.txt");
    }

    #[test]
    fn resolve_visible_create_target_rejects_root_and_occupied_destinations() {
        let metadata_state = sample_path_metadata();

        let root = metadata_state
            .resolve_visible_create_target("/", ChangeSeq(3))
            .expect_err("root create target should fail");
        assert!(matches!(
            root,
            VisiblePathMutationError::RootPathRejected { .. }
        ));

        let occupied = metadata_state
            .resolve_visible_create_target("/docs/report.txt", ChangeSeq(3))
            .expect_err("occupied create target should fail");
        assert!(matches!(
            occupied,
            VisiblePathMutationError::DestinationOccupied { inode_id, .. }
                if inode_id == InodeId(3)
        ));
    }

    #[test]
    fn ensure_distinct_visible_paths_rejects_identical_normalized_paths() {
        let metadata_state = sample_path_metadata();
        let error = metadata_state
            .ensure_distinct_visible_paths("/docs//report.txt", "/docs/report.txt")
            .expect_err("identical normalized paths should fail");
        assert!(matches!(
            error,
            VisiblePathMutationError::IdenticalSourceAndDestination { absolute_path }
                if absolute_path == "/docs/report.txt"
        ));
    }

    #[test]
    fn resolve_visible_file_target_accepts_visible_file_and_rejects_root_or_directory() {
        let metadata_state = sample_path_metadata();

        let file = metadata_state
            .resolve_visible_file_target("/docs/report.txt", ChangeSeq(3))
            .expect("resolve visible file target");
        assert_eq!(file.inode_id, InodeId(3));
        assert_eq!(file.absolute_path, "/docs/report.txt");

        let root = metadata_state
            .resolve_visible_file_target("/", ChangeSeq(3))
            .expect_err("root should be rejected");
        assert!(matches!(
            root,
            VisiblePathMutationError::RootPathRejected { absolute_path } if absolute_path == "/"
        ));

        let directory = metadata_state
            .resolve_visible_file_target("/docs", ChangeSeq(3))
            .expect_err("directory should be rejected");
        assert!(matches!(
            directory,
            VisiblePathMutationError::FileRequired { inode_id, inode_kind, .. }
                if inode_id == InodeId(2) && inode_kind == InodeKind::Dir
        ));
    }

    #[test]
    fn collect_visible_directory_subtree_returns_deterministic_relative_entries() {
        let metadata_state = sample_path_metadata();

        let subtree = metadata_state
            .collect_visible_directory_subtree("/docs", ChangeSeq(3))
            .expect("collect subtree");

        assert_eq!(subtree.root.absolute_path, "/docs");
        assert_eq!(
            subtree
                .directories
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["drafts"]
        );
        assert_eq!(
            subtree
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["drafts/note.txt", "report.txt"]
        );
    }

    #[test]
    fn collect_visible_directory_subtree_rejects_unsupported_descendants() {
        let mut metadata_state = sample_path_metadata();
        metadata_state.inodes.push(InodeRecord {
            inode_id: InodeId(7),
            inode_kind: InodeKind::Symlink,
            created_seq: ChangeSeq(3),
        });
        metadata_state.direntries.push(DirentryRecord {
            parent_inode_id: InodeId(2),
            name_key: "shortcut".to_owned(),
            display_name: "shortcut".to_owned(),
            child_inode_id: InodeId(7),
            bind_seq: ChangeSeq(3),
            bind_op_index: 0,
        });

        let error = metadata_state
            .collect_visible_directory_subtree("/docs", ChangeSeq(3))
            .expect_err("unsupported descendant should fail");
        assert!(matches!(
            error,
            VisibleSubtreeError::UnsupportedDescendant { absolute_path, inode_id, inode_kind }
                if absolute_path == "/docs/shortcut"
                    && inode_id == InodeId(7)
                    && inode_kind == InodeKind::Symlink
        ));
    }

    #[test]
    fn collect_visible_directory_subtree_rejects_files_missing_revision_heads() {
        let mut metadata_state = sample_path_metadata();
        metadata_state
            .revisions
            .retain(|revision| revision.inode_id != InodeId(6));

        let error = metadata_state
            .collect_visible_directory_subtree("/docs", ChangeSeq(3))
            .expect_err("missing file revision should fail");
        assert!(matches!(
            error,
            VisibleSubtreeError::FileRevisionMissing { absolute_path, inode_id }
                if absolute_path == "/docs/drafts/note.txt" && inode_id == InodeId(6)
        ));
    }

    #[test]
    fn plan_recursive_put_subtree_builds_deterministic_entries_and_commit_ops() {
        let metadata_state = sample_path_metadata();
        let plan = metadata_state
            .plan_recursive_put_subtree(
                "/archive/imported",
                ChangeSeq(3),
                &[
                    RecursivePutLocalEntry {
                        relative_path: String::new(),
                        inode_kind: InodeKind::Dir,
                        content_manifest_digest: None,
                        size_bytes: None,
                    },
                    RecursivePutLocalEntry {
                        relative_path: "b-dir".to_owned(),
                        inode_kind: InodeKind::Dir,
                        content_manifest_digest: None,
                        size_bytes: None,
                    },
                    RecursivePutLocalEntry {
                        relative_path: "a-dir".to_owned(),
                        inode_kind: InodeKind::Dir,
                        content_manifest_digest: None,
                        size_bytes: None,
                    },
                    RecursivePutLocalEntry {
                        relative_path: "a-dir/alpha.txt".to_owned(),
                        inode_kind: InodeKind::File,
                        content_manifest_digest: Some("sha256:alpha".to_owned()),
                        size_bytes: Some(11),
                    },
                    RecursivePutLocalEntry {
                        relative_path: "b-dir/bravo.txt".to_owned(),
                        inode_kind: InodeKind::File,
                        content_manifest_digest: Some("sha256:bravo".to_owned()),
                        size_bytes: Some(12),
                    },
                ],
            )
            .expect("plan recursive put");

        assert_eq!(
            plan.directories
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["", "a-dir", "b-dir"]
        );
        assert_eq!(
            plan.files
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["a-dir/alpha.txt", "b-dir/bravo.txt"]
        );

        let ops = metadata_state.build_recursive_put_commit_ops(&plan, InodeId(501));
        assert_eq!(
            ops,
            vec![
                crate::commit::CommitOp::CreateDir {
                    parent_inode: InodeId(4),
                    display_name: "imported".to_owned(),
                },
                crate::commit::CommitOp::CreateDir {
                    parent_inode: InodeId(501),
                    display_name: "a-dir".to_owned(),
                },
                crate::commit::CommitOp::CreateDir {
                    parent_inode: InodeId(501),
                    display_name: "b-dir".to_owned(),
                },
                crate::commit::CommitOp::CreateFile {
                    parent_inode: InodeId(502),
                    display_name: "alpha.txt".to_owned(),
                    content_manifest_digest: "sha256:alpha".to_owned(),
                },
                crate::commit::CommitOp::CreateFile {
                    parent_inode: InodeId(503),
                    display_name: "bravo.txt".to_owned(),
                    content_manifest_digest: "sha256:bravo".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn plan_recursive_put_subtree_rejects_invalid_local_shapes() {
        let metadata_state = sample_path_metadata();

        let missing_root = metadata_state
            .plan_recursive_put_subtree(
                "/archive/imported",
                ChangeSeq(3),
                &[RecursivePutLocalEntry {
                    relative_path: "docs".to_owned(),
                    inode_kind: InodeKind::Dir,
                    content_manifest_digest: None,
                    size_bytes: None,
                }],
            )
            .expect_err("missing root should fail");
        assert!(matches!(
            missing_root,
            super::RecursivePutPlanError::RootEntryMissing
        ));

        let unsupported = metadata_state
            .plan_recursive_put_subtree(
                "/archive/imported",
                ChangeSeq(3),
                &[
                    RecursivePutLocalEntry {
                        relative_path: String::new(),
                        inode_kind: InodeKind::Dir,
                        content_manifest_digest: None,
                        size_bytes: None,
                    },
                    RecursivePutLocalEntry {
                        relative_path: "link".to_owned(),
                        inode_kind: InodeKind::Symlink,
                        content_manifest_digest: None,
                        size_bytes: None,
                    },
                ],
            )
            .expect_err("unsupported local kind should fail");
        assert!(matches!(
            unsupported,
            super::RecursivePutPlanError::UnsupportedLocalKind { relative_path, inode_kind }
                if relative_path == "link" && inode_kind == InodeKind::Symlink
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
                InodeRecord {
                    inode_id: InodeId(5),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(2),
                },
                InodeRecord {
                    inode_id: InodeId(6),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(3),
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
                DirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "drafts".to_owned(),
                    display_name: "drafts".to_owned(),
                    child_inode_id: InodeId(5),
                    bind_seq: ChangeSeq(2),
                    bind_op_index: 0,
                },
                DirentryRecord {
                    parent_inode_id: InodeId(5),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(6),
                    bind_seq: ChangeSeq(3),
                    bind_op_index: 0,
                },
            ],
            revisions: vec![
                RevisionRecord {
                    inode_id: InodeId(3),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(1),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:report".to_owned(),
                },
                RevisionRecord {
                    inode_id: InodeId(6),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(3),
                    revision_op_index: 0,
                    content_manifest_digest: "sha256:note".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        }
    }
}
