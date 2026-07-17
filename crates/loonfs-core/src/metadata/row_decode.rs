//! Decodes durable manifest rows into the in-memory metadata record types.

use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::MetadataRow;

pub(super) fn inode_from_manifest_row(row: MetadataRow) -> Option<InodeRecord> {
    match row {
        MetadataRow::Inode {
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

pub(super) fn direntry_bind_from_manifest_row(row: MetadataRow) -> Option<DirentryBindRecord> {
    match row {
        MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        } => Some(DirentryBindRecord {
            parent_inode_id,
            name_key: name_key.as_str().to_owned(),
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        }),
        _ => None,
    }
}

pub(super) fn direntry_unbind_from_manifest_row(row: MetadataRow) -> Option<DirentryUnbindRecord> {
    match row {
        MetadataRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        } => Some(DirentryUnbindRecord {
            parent_inode_id,
            name_key: name_key.as_str().to_owned(),
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        }),
        _ => None,
    }
}

pub(super) fn revision_from_manifest_row(row: MetadataRow) -> Option<RevisionRecord> {
    match row {
        MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            committed_at_ms,
            revision_delta_index,
            content_ref,
        } => Some(RevisionRecord {
            inode_id,
            revision_no,
            committed_seq,
            committed_at_ms,
            revision_delta_index,
            content_ref,
        }),
        _ => None,
    }
}

fn subtree_tombstone_action(
    action: &loonfs_api::wire::manifest::TombstoneRowAction,
) -> super::rows::SubtreeTombstoneAction {
    use super::rows::SubtreeTombstoneAction;
    use loonfs_api::wire::manifest::TombstoneRowAction;
    match action {
        TombstoneRowAction::Set => SubtreeTombstoneAction::Set,
        TombstoneRowAction::Revoke {
            target_seq,
            target_delta_index,
        } => SubtreeTombstoneAction::Revoke {
            target_seq: *target_seq,
            target_delta_index: *target_delta_index,
        },
    }
}

pub(super) fn tombstone_from_manifest_row(row: MetadataRow) -> Option<SubtreeTombstoneRecord> {
    match row {
        MetadataRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
            action,
        } => Some(SubtreeTombstoneRecord {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
            action: subtree_tombstone_action(&action),
        }),
        _ => None,
    }
}

pub(super) fn commit_receipt_from_manifest_row(row: MetadataRow) -> Option<CommitReceiptRecord> {
    match row {
        MetadataRow::CommitReceipt {
            commit_id,
            semantic_commit_fingerprint,
            committed_seq,
            committed_at_ms,
            message,
        } => Some(CommitReceiptRecord {
            commit_id,
            semantic_commit_fingerprint,
            committed_seq,
            committed_at_ms,
            message,
        }),
        _ => None,
    }
}
