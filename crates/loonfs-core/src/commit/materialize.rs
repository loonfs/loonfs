use super::{PreparedCommit, ResolvedBinding, ValidatedOp};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{ContentRef, InodeId, InodeKind, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommitDelta {
    pub semantic_op_index: u32,
    pub delta_index: u32,
    pub wal_delta: WalDelta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommit {
    pub prepared: PreparedCommit,
    pub deltas: Vec<MaterializedCommitDelta>,
    pub results: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitOpResult {
    CreateDirectory {
        op_index: u32,
        inode_id: InodeId,
    },
    CreateFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    ReplaceFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    RestoreRevision {
        op_index: u32,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    DeleteFile {
        op_index: u32,
        inode_id: InodeId,
    },
    Rename {
        op_index: u32,
        inode_id: InodeId,
    },
    DeleteSubtree {
        op_index: u32,
        root_inode_id: InodeId,
    },
}

pub fn materialize_commit(prepared: PreparedCommit) -> MaterializedCommit {
    let mut deltas = Vec::new();
    let mut results = Vec::with_capacity(prepared.plan.validated_ops.len());
    for op in &prepared.plan.validated_ops {
        let (mut op_deltas, result) = materialize_validated_op(op);
        deltas.append(&mut op_deltas);
        results.push(result);
    }

    MaterializedCommit {
        prepared,
        deltas,
        results,
    }
}

fn materialize_validated_op(op: &ValidatedOp) -> (Vec<MaterializedCommitDelta>, CommitOpResult) {
    let mut deltas = Vec::new();
    let result = match op {
        ValidatedOp::CreateDir {
            op_index,
            parent_inode_id,
            display_name,
            name_key,
            child_inode_id,
            create_inode_delta_index,
            bind_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::CreateInode {
                    delta_index: *create_inode_delta_index,
                    inode_id: *child_inode_id,
                    inode_kind: InodeKind::Dir,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode_id: *child_inode_id,
                },
            );
            CommitOpResult::CreateDirectory {
                op_index: *op_index,
                inode_id: *child_inode_id,
            }
        }
        ValidatedOp::CreateFile {
            op_index,
            parent_inode_id,
            display_name,
            name_key,
            child_inode_id,
            content_ref,
            create_inode_delta_index,
            bind_delta_index,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::CreateInode {
                    delta_index: *create_inode_delta_index,
                    inode_id: *child_inode_id,
                    inode_kind: InodeKind::File,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode_id: *child_inode_id,
                },
            );
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *child_inode_id,
                    revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::CreateFile {
                op_index: *op_index,
                inode_id: *child_inode_id,
                revision_no: RevisionNo(1),
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::ReplaceFile {
            op_index,
            inode_id,
            revision_no,
            content_ref,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *inode_id,
                    revision_no: *revision_no,
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::ReplaceFile {
                op_index: *op_index,
                inode_id: *inode_id,
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::RestoreRevision {
            op_index,
            inode_id,
            source_revision_no,
            revision_no,
            content_ref,
            revision_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendFileRevision {
                    delta_index: *revision_delta_index,
                    inode_id: *inode_id,
                    revision_no: *revision_no,
                    content_ref: content_ref.clone(),
                },
            );
            CommitOpResult::RestoreRevision {
                op_index: *op_index,
                inode_id: *inode_id,
                source_revision_no: *source_revision_no,
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }
        }
        ValidatedOp::DeleteFile {
            op_index,
            inode_id,
            source_binding,
            unbind_delta_index,
            tombstone_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::TombstoneSubtree {
                    delta_index: *tombstone_delta_index,
                    root_inode_id: *inode_id,
                },
            );
            CommitOpResult::DeleteFile {
                op_index: *op_index,
                inode_id: *inode_id,
            }
        }
        ValidatedOp::Rename {
            op_index,
            inode_id,
            new_parent_inode_id,
            new_display_name,
            new_name_key,
            source_binding,
            unbind_delta_index,
            bind_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::BindDirentry {
                    delta_index: *bind_delta_index,
                    parent_inode_id: *new_parent_inode_id,
                    name_key: new_name_key.clone(),
                    display_name: new_display_name.clone(),
                    child_inode_id: *inode_id,
                },
            );
            CommitOpResult::Rename {
                op_index: *op_index,
                inode_id: *inode_id,
            }
        }
        ValidatedOp::DeleteSubtree {
            op_index,
            root_inode_id,
            source_binding,
            unbind_delta_index,
            tombstone_delta_index,
        } => {
            push_unbind_delta(&mut deltas, *op_index, *unbind_delta_index, source_binding);
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::TombstoneSubtree {
                    delta_index: *tombstone_delta_index,
                    root_inode_id: *root_inode_id,
                },
            );
            CommitOpResult::DeleteSubtree {
                op_index: *op_index,
                root_inode_id: *root_inode_id,
            }
        }
    };

    (deltas, result)
}

fn push_unbind_delta(
    deltas: &mut Vec<MaterializedCommitDelta>,
    semantic_op_index: u32,
    delta_index: u32,
    binding: &ResolvedBinding,
) {
    push_delta(
        deltas,
        semantic_op_index,
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode_id: binding.parent_inode_id,
            name_key: binding.name_key.clone(),
            child_inode_id: binding.child_inode_id,
            bind_seq: binding.bind_seq,
            bind_delta_index: binding.bind_delta_index,
        },
    )
}

fn push_delta(
    deltas: &mut Vec<MaterializedCommitDelta>,
    semantic_op_index: u32,
    wal_delta: WalDelta,
) {
    let delta_index = wal_delta_index(&wal_delta);
    deltas.push(MaterializedCommitDelta {
        semantic_op_index,
        delta_index,
        wal_delta,
    });
}

fn wal_delta_index(wal_delta: &WalDelta) -> u32 {
    match wal_delta {
        WalDelta::CreateInode { delta_index, .. }
        | WalDelta::BindDirentry { delta_index, .. }
        | WalDelta::UnbindDirentry { delta_index, .. }
        | WalDelta::AppendFileRevision { delta_index, .. }
        | WalDelta::TombstoneSubtree { delta_index, .. } => *delta_index,
    }
}
