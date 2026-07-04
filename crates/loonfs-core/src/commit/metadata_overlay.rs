use super::ValidatedOp;
use crate::metadata::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::{ChangeSeq, InodeId, InodeKind, RevisionNo};

pub(super) struct CommitOverlayRows {
    rows: MetadataState,
}

impl CommitOverlayRows {
    pub(super) fn new() -> Self {
        Self {
            rows: MetadataState::default(),
        }
    }

    pub(super) fn from_rows(rows: &MetadataState) -> Self {
        Self { rows: rows.clone() }
    }

    pub(super) fn rows(&self) -> &MetadataState {
        &self.rows
    }

    pub(super) fn apply_validated_op_mut(&mut self, committed_seq: ChangeSeq, op: &ValidatedOp) {
        match op {
            ValidatedOp::CreateDir {
                parent_inode_id,
                display_name,
                name_key,
                child_inode_id,
                create_inode_delta_index: _,
                bind_delta_index,
                ..
            } => {
                self.push_inode(*child_inode_id, InodeKind::Dir, committed_seq);
                self.push_bind(
                    *parent_inode_id,
                    name_key.clone(),
                    display_name.clone(),
                    *child_inode_id,
                    committed_seq,
                    *bind_delta_index,
                );
            }
            ValidatedOp::CreateFile {
                parent_inode_id,
                display_name,
                name_key,
                child_inode_id,
                content_ref,
                create_inode_delta_index: _,
                bind_delta_index,
                revision_delta_index,
                ..
            } => {
                self.push_inode(*child_inode_id, InodeKind::File, committed_seq);
                self.push_bind(
                    *parent_inode_id,
                    name_key.clone(),
                    display_name.clone(),
                    *child_inode_id,
                    committed_seq,
                    *bind_delta_index,
                );
                self.push_revision(
                    *child_inode_id,
                    RevisionNo(1),
                    committed_seq,
                    *revision_delta_index,
                    content_ref.clone(),
                );
            }
            ValidatedOp::ReplaceFile {
                inode_id,
                revision_no,
                content_ref,
                revision_delta_index,
                ..
            }
            | ValidatedOp::RestoreRevision {
                inode_id,
                revision_no,
                content_ref,
                revision_delta_index,
                ..
            } => {
                self.push_revision(
                    *inode_id,
                    *revision_no,
                    committed_seq,
                    *revision_delta_index,
                    content_ref.clone(),
                );
            }
            ValidatedOp::DeleteFile {
                inode_id,
                source_binding,
                unbind_delta_index,
                tombstone_delta_index,
                ..
            } => {
                self.push_unbind(source_binding, committed_seq, *unbind_delta_index);
                self.push_tombstone(*inode_id, committed_seq, *tombstone_delta_index);
            }
            ValidatedOp::Rename {
                inode_id,
                source_binding,
                new_parent_inode_id,
                new_display_name,
                new_name_key,
                unbind_delta_index,
                bind_delta_index,
                ..
            } => {
                self.push_unbind(source_binding, committed_seq, *unbind_delta_index);
                self.push_bind(
                    *new_parent_inode_id,
                    new_name_key.clone(),
                    new_display_name.clone(),
                    *inode_id,
                    committed_seq,
                    *bind_delta_index,
                );
            }
            ValidatedOp::DeleteSubtree {
                root_inode_id,
                source_binding,
                unbind_delta_index,
                tombstone_delta_index,
                ..
            } => {
                self.push_unbind(source_binding, committed_seq, *unbind_delta_index);
                self.push_tombstone(*root_inode_id, committed_seq, *tombstone_delta_index);
            }
        }
    }

    fn push_inode(&mut self, inode_id: InodeId, inode_kind: InodeKind, created_seq: ChangeSeq) {
        self.rows.push_inode_record(InodeRecord {
            inode_id,
            inode_kind,
            created_seq,
        });
    }

    fn push_bind(
        &mut self,
        parent_inode_id: InodeId,
        name_key: String,
        display_name: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) {
        self.rows.push_direntry_bind_record(DirentryBindRecord {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        });
    }

    fn push_unbind(
        &mut self,
        source_binding: &super::ResolvedBinding,
        unbind_seq: ChangeSeq,
        unbind_delta_index: u32,
    ) {
        self.rows.push_direntry_unbind_record(DirentryUnbindRecord {
            parent_inode_id: source_binding.parent_inode_id,
            name_key: source_binding.name_key.clone(),
            child_inode_id: source_binding.child_inode_id,
            bind_seq: source_binding.bind_seq,
            bind_delta_index: source_binding.bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        });
    }

    fn push_revision(
        &mut self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        revision_delta_index: u32,
        content_ref: loonfs_api::ContentRef,
    ) {
        self.rows.push_revision_record(RevisionRecord {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        });
    }

    fn push_tombstone(
        &mut self,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
        tombstone_delta_index: u32,
    ) {
        self.rows
            .push_subtree_tombstone_record(SubtreeTombstoneRecord {
                root_inode_id,
                tombstone_seq,
                tombstone_delta_index,
            });
    }
}
