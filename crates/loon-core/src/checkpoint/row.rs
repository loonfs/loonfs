use super::CHECKPOINT_TABLE_FAMILIES;
use crate::metadata::MetadataState;
use loon_api::wire::checkpoint::{CheckpointRow, CheckpointTableFamily};
use loon_api::ChangeSeq;
use loon_objectstore::keys::CheckpointTableFamily as ObjectStoreCheckpointTableFamily;

pub(super) fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    CHECKPOINT_TABLE_FAMILIES.into_iter().all(|family| {
        checkpoint_rows_for_family(left, family) == checkpoint_rows_for_family(right, family)
    })
}

pub(super) fn checkpoint_rows_for_family(
    metadata_state: &MetadataState,
    family: CheckpointTableFamily,
) -> Vec<CheckpointRow> {
    let mut rows = match family {
        CheckpointTableFamily::Inodes => metadata_state
            .inodes()
            .iter()
            .map(|inode| CheckpointRow::Inode {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::DirentryBinds | CheckpointTableFamily::DirentryChildBinds => {
            metadata_state
                .direntry_binds()
                .iter()
                .map(|direntry| CheckpointRow::DirentryBind {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                    child_inode_id: direntry.child_inode_id,
                    bind_seq: direntry.bind_seq,
                    bind_delta_index: direntry.bind_delta_index,
                })
                .collect::<Vec<_>>()
        }
        CheckpointTableFamily::DirentryUnbinds => metadata_state
            .direntry_unbinds()
            .iter()
            .map(|unbind| CheckpointRow::DirentryUnbind {
                parent_inode_id: unbind.parent_inode_id,
                name_key: unbind.name_key.clone(),
                child_inode_id: unbind.child_inode_id,
                bind_seq: unbind.bind_seq,
                bind_delta_index: unbind.bind_delta_index,
                unbind_seq: unbind.unbind_seq,
                unbind_delta_index: unbind.unbind_delta_index,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Revisions => metadata_state
            .revisions()
            .iter()
            .map(|revision| CheckpointRow::Revision {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                revision_delta_index: revision.revision_delta_index,
                content_ref: revision.content_ref.clone(),
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Tombstones => metadata_state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| CheckpointRow::Tombstone {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_delta_index: tombstone.tombstone_delta_index,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::CommitReceipts => metadata_state
            .commit_receipts()
            .iter()
            .map(|record| CheckpointRow::CommitReceipt {
                commit_id: record.commit_id.clone(),
                semantic_commit_fingerprint_sha256: record
                    .semantic_commit_fingerprint_sha256
                    .clone(),
                committed_seq: record.committed_seq,
                results: record.results.clone(),
            })
            .collect::<Vec<_>>(),
    };
    rows.sort_by_key(|row| row.row_key_for_family(family));
    rows
}

pub(super) fn checkpoint_rows_for_family_after_seq(
    metadata_state: &MetadataState,
    family: CheckpointTableFamily,
    after_seq: ChangeSeq,
) -> Vec<CheckpointRow> {
    checkpoint_rows_for_family(metadata_state, family)
        .into_iter()
        .filter(|row| checkpoint_row_commit_seq(row) > after_seq)
        .collect()
}

pub(super) fn checkpoint_row_commit_seq(row: &CheckpointRow) -> ChangeSeq {
    match row {
        CheckpointRow::Inode { created_seq, .. } => *created_seq,
        CheckpointRow::DirentryBind { bind_seq, .. } => *bind_seq,
        CheckpointRow::DirentryUnbind { unbind_seq, .. } => *unbind_seq,
        CheckpointRow::Revision { committed_seq, .. } => *committed_seq,
        CheckpointRow::Tombstone { tombstone_seq, .. } => *tombstone_seq,
        CheckpointRow::CommitReceipt { committed_seq, .. } => *committed_seq,
    }
}

pub(super) fn checkpoint_table_family(
    family: CheckpointTableFamily,
) -> ObjectStoreCheckpointTableFamily {
    match family {
        CheckpointTableFamily::Inodes => ObjectStoreCheckpointTableFamily::Inodes,
        CheckpointTableFamily::DirentryBinds => ObjectStoreCheckpointTableFamily::DirentryBinds,
        CheckpointTableFamily::DirentryChildBinds => {
            ObjectStoreCheckpointTableFamily::DirentryChildBinds
        }
        CheckpointTableFamily::DirentryUnbinds => ObjectStoreCheckpointTableFamily::DirentryUnbinds,
        CheckpointTableFamily::Revisions => ObjectStoreCheckpointTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => ObjectStoreCheckpointTableFamily::Tombstones,
        CheckpointTableFamily::CommitReceipts => ObjectStoreCheckpointTableFamily::CommitReceipts,
    }
}

pub(super) fn checkpoint_row_kind(row: &CheckpointRow) -> &'static str {
    match row {
        CheckpointRow::Inode { .. } => "inode",
        CheckpointRow::DirentryBind { .. } => "direntry_bind",
        CheckpointRow::DirentryUnbind { .. } => "direntry_unbind",
        CheckpointRow::Revision { .. } => "revision",
        CheckpointRow::Tombstone { .. } => "tombstone",
        CheckpointRow::CommitReceipt { .. } => "commit_receipt",
    }
}

pub(super) fn checkpoint_row_matches_family(
    row: &CheckpointRow,
    family: CheckpointTableFamily,
) -> bool {
    matches!(
        (family, row),
        (CheckpointTableFamily::Inodes, CheckpointRow::Inode { .. })
            | (
                CheckpointTableFamily::DirentryBinds,
                CheckpointRow::DirentryBind { .. }
            )
            | (
                CheckpointTableFamily::DirentryChildBinds,
                CheckpointRow::DirentryBind { .. }
            )
            | (
                CheckpointTableFamily::DirentryUnbinds,
                CheckpointRow::DirentryUnbind { .. }
            )
            | (
                CheckpointTableFamily::Revisions,
                CheckpointRow::Revision { .. }
            )
            | (
                CheckpointTableFamily::Tombstones,
                CheckpointRow::Tombstone { .. }
            )
            | (
                CheckpointTableFamily::CommitReceipts,
                CheckpointRow::CommitReceipt { .. }
            )
    )
}
