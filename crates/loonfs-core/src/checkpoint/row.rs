//! Converts in-memory metadata state into manifest rows, per family and in
//! row-key order.

use crate::metadata::MetadataState;
use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
use loonfs_api::ChangeSeq;

#[cfg(test)]
use super::runs::CHECKPOINT_TABLE_FAMILIES;

#[cfg(test)]
pub(super) fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    CHECKPOINT_TABLE_FAMILIES.into_iter().all(|family| {
        manifest_rows_for_family(left, family) == manifest_rows_for_family(right, family)
    })
}

fn tombstone_row_action(
    action: &crate::metadata::SubtreeTombstoneAction,
) -> loonfs_api::wire::manifest::TombstoneRowAction {
    use crate::metadata::SubtreeTombstoneAction;
    use loonfs_api::wire::manifest::TombstoneRowAction;
    match action {
        SubtreeTombstoneAction::Set => TombstoneRowAction::Set,
        SubtreeTombstoneAction::Revoke {
            target_seq,
            target_delta_index,
        } => TombstoneRowAction::Revoke {
            target_seq: *target_seq,
            target_delta_index: *target_delta_index,
        },
    }
}

pub(super) fn manifest_rows_for_family(
    metadata_state: &MetadataState,
    family: MetadataTableFamily,
) -> Vec<MetadataRow> {
    let mut rows = match family {
        MetadataTableFamily::Inodes => metadata_state
            .inodes()
            .iter()
            .map(|inode| MetadataRow::Inode {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind,
                created_seq: inode.created_seq,
            })
            .collect::<Vec<_>>(),
        MetadataTableFamily::DirentryBinds | MetadataTableFamily::DirentryChildBinds => {
            metadata_state
                .direntry_binds()
                .iter()
                .map(|direntry| MetadataRow::DirentryBind {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                    child_inode_id: direntry.child_inode_id,
                    bind_seq: direntry.bind_seq,
                    bind_delta_index: direntry.bind_delta_index,
                })
                .collect::<Vec<_>>()
        }
        MetadataTableFamily::DirentryUnbinds => metadata_state
            .direntry_unbinds()
            .iter()
            .map(|unbind| MetadataRow::DirentryUnbind {
                parent_inode_id: unbind.parent_inode_id,
                name_key: unbind.name_key.clone(),
                child_inode_id: unbind.child_inode_id,
                bind_seq: unbind.bind_seq,
                bind_delta_index: unbind.bind_delta_index,
                unbind_seq: unbind.unbind_seq,
                unbind_delta_index: unbind.unbind_delta_index,
            })
            .collect::<Vec<_>>(),
        MetadataTableFamily::Revisions | MetadataTableFamily::RevisionsByInodeDesc => {
            metadata_state
                .revisions()
                .iter()
                .map(|revision| MetadataRow::Revision {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    committed_seq: revision.committed_seq,
                    committed_at_ms: revision.committed_at_ms,
                    revision_delta_index: revision.revision_delta_index,
                    content_ref: revision.content_ref.clone(),
                })
                .collect::<Vec<_>>()
        }
        MetadataTableFamily::Tombstones => metadata_state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| MetadataRow::Tombstone {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_delta_index: tombstone.tombstone_delta_index,
                action: tombstone_row_action(&tombstone.action),
            })
            .collect::<Vec<_>>(),
        MetadataTableFamily::CommitReceipts => metadata_state
            .commit_receipts()
            .iter()
            .map(|record| MetadataRow::CommitReceipt {
                commit_id: record.commit_id.clone(),
                semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
                committed_seq: record.committed_seq,
                committed_at_ms: record.committed_at_ms,
                message: record.message.clone(),
            })
            .collect::<Vec<_>>(),
    };
    rows.sort_by_key(|row| row.row_key_for_family(family));
    rows
}

pub(super) fn manifest_rows_for_family_after_seq(
    metadata_state: &MetadataState,
    family: MetadataTableFamily,
    after_seq: ChangeSeq,
) -> Vec<MetadataRow> {
    manifest_rows_for_family(metadata_state, family)
        .into_iter()
        .filter(|row| manifest_row_commit_seq(row) > after_seq)
        .collect()
}

pub(super) fn manifest_row_commit_seq(row: &MetadataRow) -> ChangeSeq {
    match row {
        MetadataRow::Inode { created_seq, .. } => *created_seq,
        MetadataRow::DirentryBind { bind_seq, .. } => *bind_seq,
        MetadataRow::DirentryUnbind { unbind_seq, .. } => *unbind_seq,
        MetadataRow::Revision { committed_seq, .. } => *committed_seq,
        MetadataRow::Tombstone { tombstone_seq, .. } => *tombstone_seq,
        MetadataRow::CommitReceipt { committed_seq, .. } => *committed_seq,
    }
}

#[cfg(test)]
pub(super) fn manifest_row_kind(row: &MetadataRow) -> &'static str {
    match row {
        MetadataRow::Inode { .. } => "inode",
        MetadataRow::DirentryBind { .. } => "direntry_bind",
        MetadataRow::DirentryUnbind { .. } => "direntry_unbind",
        MetadataRow::Revision { .. } => "revision",
        MetadataRow::Tombstone { .. } => "tombstone",
        MetadataRow::CommitReceipt { .. } => "commit_receipt",
    }
}
