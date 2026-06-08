use crate::metadata::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord, SubtreeTombstoneRecord,
};
use loon_api::wire::checkpoint::CheckpointRow;

pub(super) fn inode_from_checkpoint_row(row: CheckpointRow) -> Option<InodeRecord> {
    match row {
        CheckpointRow::Inode {
            inode_id,
            inode_kind,
            created_seq,
        } => Some(InodeRecord {
            inode_id,
            inode_kind,
            created_seq,
        }),
        _ => None,
    }
}

pub(super) fn direntry_bind_from_checkpoint_row(row: CheckpointRow) -> Option<DirentryBindRecord> {
    match row {
        CheckpointRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        } => Some(DirentryBindRecord {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        }),
        _ => None,
    }
}

pub(super) fn direntry_unbind_from_checkpoint_row(
    row: CheckpointRow,
) -> Option<DirentryUnbindRecord> {
    match row {
        CheckpointRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        } => Some(DirentryUnbindRecord {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        }),
        _ => None,
    }
}

pub(super) fn revision_from_checkpoint_row(row: CheckpointRow) -> Option<RevisionRecord> {
    match row {
        CheckpointRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        } => Some(RevisionRecord {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        }),
        _ => None,
    }
}

pub(super) fn tombstone_from_checkpoint_row(row: CheckpointRow) -> Option<SubtreeTombstoneRecord> {
    match row {
        CheckpointRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
        } => Some(SubtreeTombstoneRecord {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
        }),
        _ => None,
    }
}

pub(super) fn unbind_matches_binding(
    unbind: &DirentryUnbindRecord,
    direntry: &DirentryBindRecord,
) -> bool {
    unbind.parent_inode_id == direntry.parent_inode_id
        && unbind.name_key == direntry.name_key
        && unbind.child_inode_id == direntry.child_inode_id
        && unbind.bind_seq == direntry.bind_seq
        && unbind.bind_delta_index == direntry.bind_delta_index
}
