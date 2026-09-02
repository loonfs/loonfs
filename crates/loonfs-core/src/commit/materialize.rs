//! Materializes a prepared commit into ordered WAL deltas.

use super::{CommitPlan, ResolvedBinding, ValidatedOp};
use loonfs_api::wire::manifest::DeletedDirentry;
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{InodeKind, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedCommitDelta {
    pub semantic_op_index: u32,
    pub delta_index: u32,
    pub wal_delta: WalDelta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedCommit {
    pub commit: CommitPlan,
    /// Observational wall-clock stamp from the publishing request context,
    /// carried into the durable WAL payload. Not part of the semantic
    /// identity: two materializations of one prepared commit under
    /// different clocks share a fingerprint.
    pub committed_at_ms: u64,
    pub deltas: Vec<MaterializedCommitDelta>,
}

pub(crate) fn materialize_commit(commit: CommitPlan, committed_at_ms: u64) -> MaterializedCommit {
    let mut deltas = Vec::new();
    for op in &commit.validated_ops {
        deltas.append(&mut materialize_validated_op(op));
    }

    MaterializedCommit {
        commit,
        committed_at_ms,
        deltas,
    }
}

pub(super) fn materialize_validated_op(op: &ValidatedOp) -> Vec<MaterializedCommitDelta> {
    let mut deltas = Vec::new();
    match op {
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
                    inode_kind: InodeKind::Directory,
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
        }
        ValidatedOp::RestoreRevision {
            op_index,
            inode_id,
            // The source is what validation resolved the content from; the
            // delta records only the revision this op appends.
            source_revision_no: _,
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
                    deleted_direntry: deleted_direntry(source_binding),
                },
            );
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
                    deleted_direntry: deleted_direntry(source_binding),
                },
            );
        }
        ValidatedOp::Undelete {
            op_index,
            inode_id,
            parent_inode_id,
            display_name,
            name_key,
            target,
            revoke_tombstone_delta_index,
            bind_delta_index,
        } => {
            // The mirror of delete's unbind-plus-tombstone: revoke the
            // exact deletion generation validation resolved, then bind the
            // recovered inode at its new home.
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::RevokeSubtreeTombstone {
                    delta_index: *revoke_tombstone_delta_index,
                    root_inode_id: *inode_id,
                    target: *target,
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
                    child_inode_id: *inode_id,
                },
            );
        }
        ValidatedOp::UpdateAttributes {
            op_index,
            inode_id,
            attributes_revision_no,
            attributes,
            attributes_delta_index,
        } => {
            push_delta(
                &mut deltas,
                *op_index,
                WalDelta::AppendAttributesRevision {
                    delta_index: *attributes_delta_index,
                    inode_id: *inode_id,
                    attributes_revision_no: *attributes_revision_no,
                    attributes: attributes.clone(),
                },
            );
        }
    }

    deltas
}

/// The binding a delete retires, as the tombstone records it: the same
/// three fields the unbind delta carries, minus the ones that identify the
/// exact bind generation being retired.
fn deleted_direntry(binding: &ResolvedBinding) -> DeletedDirentry {
    DeletedDirentry {
        parent_inode_id: binding.parent_inode_id,
        name_key: binding.name_key.clone(),
        display_name: binding.display_name.clone(),
    }
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
            display_name: binding.display_name.clone(),
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
        | WalDelta::TombstoneSubtree { delta_index, .. }
        | WalDelta::RevokeSubtreeTombstone { delta_index, .. }
        | WalDelta::AppendAttributesRevision { delta_index, .. } => *delta_index,
    }
}
