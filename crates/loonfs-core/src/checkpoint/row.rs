//! Converts in-memory metadata state into manifest rows, per family and in
//! row-key order.

use crate::metadata::{active_deletion_from_tombstone, ActiveDeletionAction, MetadataState};
use loonfs_api::wire::manifest::{
    ActiveDeletionRowAction, MetadataRow, MetadataTableFamily, TombstoneRowAction,
};
use loonfs_api::ChangeSeq;

#[cfg(test)]
use super::runs::CHECKPOINT_TABLE_FAMILIES;

#[cfg(test)]
pub(super) fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    CHECKPOINT_TABLE_FAMILIES.into_iter().all(|family| {
        manifest_rows_for_family(left, family) == manifest_rows_for_family(right, family)
    })
}

fn tombstone_row_action(action: &crate::metadata::SubtreeTombstoneAction) -> TombstoneRowAction {
    use crate::metadata::SubtreeTombstoneAction;
    match action {
        SubtreeTombstoneAction::Set { deleted_direntry } => TombstoneRowAction::Set {
            deleted_direntry: deleted_direntry.clone(),
        },
        SubtreeTombstoneAction::Revoke { target } => TombstoneRowAction::Revoke { target: *target },
    }
}

/// Projects one tombstone event onto its derived active-deletion row. The
/// reducer itself lives beside the tombstone rules in `metadata::rows`; this
/// is only the record-to-wire half.
fn active_deletion_row(tombstone: &crate::metadata::SubtreeTombstoneRecord) -> MetadataRow {
    let record = active_deletion_from_tombstone(tombstone);
    MetadataRow::ActiveDeletion {
        root_inode_id: record.root_inode_id,
        deleted_at_seq: record.deleted_at_seq,
        action: match record.action {
            ActiveDeletionAction::Listed {
                deleted_at_ms,
                deleted_by,
                deleted_direntry,
            } => ActiveDeletionRowAction::Listed {
                deleted_at_ms,
                deleted_by,
                deleted_direntry,
            },
            ActiveDeletionAction::Removed { revoked_at_seq } => {
                ActiveDeletionRowAction::Removed { revoked_at_seq }
            }
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
                commit_id: inode.commit_id.clone(),
                created_by: inode.created_by.clone(),
                created_at_ms: inode.created_at_ms,
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
                display_name: unbind.display_name.clone(),
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
                    commit_id: revision.commit_id.clone(),
                    committed_at_ms: revision.committed_at_ms,
                    actor: revision.actor.clone(),
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
                generation: tombstone.generation,
                commit_id: tombstone.commit_id.clone(),
                action: tombstone_row_action(&tombstone.action),
                deleted_at_ms: tombstone.deleted_at_ms,
                actor: tombstone.actor.clone(),
            })
            .collect::<Vec<_>>(),
        MetadataTableFamily::ActiveDeletions => metadata_state
            .subtree_tombstones()
            .iter()
            .map(active_deletion_row)
            .collect::<Vec<_>>(),
        MetadataTableFamily::CommitReceipts => metadata_state
            .commit_receipts()
            .iter()
            .map(|record| MetadataRow::CommitReceipt {
                commit_id: record.commit_id.clone(),
                actor: record.actor.clone(),
                semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
                committed_seq: record.committed_seq,
                committed_at_ms: record.committed_at_ms,
                message: record.message.clone(),
            })
            .collect::<Vec<_>>(),
        MetadataTableFamily::Attributes => metadata_state
            .attributes_revisions()
            .iter()
            .map(|record| MetadataRow::AttributesRevision {
                inode_id: record.inode_id,
                attributes_revision_no: record.attributes_revision_no,
                committed_seq: record.committed_seq,
                commit_id: record.commit_id.clone(),
                delta_index: record.delta_index,
                actor: record.actor.clone(),
                updated_at_ms: record.updated_at_ms,
                attributes: record.attributes.clone(),
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
        MetadataRow::Tombstone { generation, .. } => generation.seq,
        // A removal marker belongs to the run of the undelete that produced
        // it, not to the run of the deletion whose key it repeats.
        MetadataRow::ActiveDeletion {
            deleted_at_seq,
            action,
            ..
        } => match action {
            ActiveDeletionRowAction::Listed { .. } => *deleted_at_seq,
            ActiveDeletionRowAction::Removed { revoked_at_seq } => *revoked_at_seq,
        },
        MetadataRow::CommitReceipt { committed_seq, .. }
        | MetadataRow::AttributesRevision { committed_seq, .. } => *committed_seq,
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
        MetadataRow::ActiveDeletion { .. } => "active_deletion",
        MetadataRow::CommitReceipt { .. } => "commit_receipt",
        MetadataRow::AttributesRevision { .. } => "attributes_revision",
    }
}
