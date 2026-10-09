//! Converts in-memory metadata state into manifest rows, per family and in
//! row-key order.

use crate::metadata::{active_deletion_from_tombstone, MetadataState};
use loonfs_types::format::manifest::{ActiveDeletionRowAction, MetadataRow, MetadataRowFamily};
use loonfs_types::{ChangeSeq, ContentId, Sha256State};
use std::collections::HashMap;

#[cfg(test)]
use super::runs::MANIFEST_ROW_FAMILIES;

#[cfg(test)]
pub(super) fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    MANIFEST_ROW_FAMILIES.into_iter().all(|family| {
        manifest_rows_for_family(left, family) == manifest_rows_for_family(right, family)
    })
}

pub(super) fn manifest_rows_for_family(
    metadata_state: &MetadataState,
    family: MetadataRowFamily,
) -> Vec<MetadataRow> {
    let mut rows = match family {
        MetadataRowFamily::Inodes => metadata_state
            .inodes()
            .iter()
            .cloned()
            .map(MetadataRow::Inode)
            .collect::<Vec<_>>(),
        MetadataRowFamily::DirentryBinds | MetadataRowFamily::DirentryChildBinds => metadata_state
            .direntry_binds()
            .iter()
            .cloned()
            .map(MetadataRow::DirentryBinding)
            .collect::<Vec<_>>(),
        MetadataRowFamily::Revisions => metadata_state
            .revisions()
            .iter()
            .cloned()
            .map(MetadataRow::FileRevision)
            .collect::<Vec<_>>(),
        MetadataRowFamily::Tombstones => metadata_state
            .subtree_tombstones()
            .iter()
            .cloned()
            .map(MetadataRow::Tombstone)
            .collect::<Vec<_>>(),
        MetadataRowFamily::ActiveDeletions => metadata_state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| {
                let inode = metadata_state
                    .inode_at_seq(tombstone.root_inode_id, tombstone.committed_seq)
                    .expect("manifest projection should include deletion root inodes");
                active_deletion_from_tombstone(tombstone, inode.inode_kind)
            })
            .map(MetadataRow::ActiveDeletion)
            .collect::<Vec<_>>(),
        MetadataRowFamily::ContentPublications => metadata_state
            .content_publications()
            .iter()
            .cloned()
            .map(MetadataRow::ContentPublication)
            .collect::<Vec<_>>(),
        MetadataRowFamily::CommitReceipts => metadata_state
            .commit_receipts()
            .iter()
            .cloned()
            .map(MetadataRow::CommitReceipt)
            .collect::<Vec<_>>(),
        MetadataRowFamily::Commits => metadata_state
            .commits()
            .iter()
            .cloned()
            .map(MetadataRow::Commit)
            .collect::<Vec<_>>(),
        MetadataRowFamily::Attributes => metadata_state
            .attributes_revisions()
            .iter()
            .cloned()
            .map(MetadataRow::AttributesRevision)
            .collect::<Vec<_>>(),
        MetadataRowFamily::Access => metadata_state
            .access_revisions()
            .iter()
            .cloned()
            .map(MetadataRow::AccessRevision)
            .collect::<Vec<_>>(),
    };
    rows.sort_by_key(|row| row.row_key_for_family(family));
    rows
}

pub(super) fn manifest_rows_for_family_after_seq(
    metadata_state: &MetadataState,
    family: MetadataRowFamily,
    after_seq: ChangeSeq,
) -> Vec<MetadataRow> {
    manifest_rows_for_family(metadata_state, family)
        .into_iter()
        .filter(|row| manifest_row_commit_seq(row) > after_seq)
        .collect()
}

/// Writes a fold's SHA-256 state for a content id into each publication row
/// of that id whose size the state covers and whose delta recorded none.
pub(super) fn with_hash_states(
    mut rows: Vec<MetadataRow>,
    hash_states: &HashMap<ContentId, Sha256State>,
) -> Vec<MetadataRow> {
    for row in &mut rows {
        if let MetadataRow::ContentPublication(record) = row {
            if record.hash_state.is_none() {
                record.hash_state = hash_states
                    .get(&record.content_id)
                    .filter(|state| state.length() == record.size_bytes)
                    .cloned();
            }
        }
    }
    rows
}

pub(super) fn manifest_row_commit_seq(row: &MetadataRow) -> ChangeSeq {
    match row {
        MetadataRow::Inode(record) => record.committed_seq,
        MetadataRow::DirentryBinding(record) => record.committed_seq,
        MetadataRow::FileRevision(record) => record.committed_seq,
        MetadataRow::Tombstone(record) => record.committed_seq,
        // A removal marker belongs to the run of the undelete that produced
        // it, not to the run of the deletion whose key it repeats.
        MetadataRow::ActiveDeletion(record) => match &record.action {
            ActiveDeletionRowAction::Listed { .. } => record.deletion_seq,
            ActiveDeletionRowAction::Removed { revocation_seq } => *revocation_seq,
        },
        MetadataRow::CommitReceipt(record) => record.committed_seq,
        MetadataRow::Commit(record) => record.committed_seq,
        MetadataRow::ContentPublication(record) => record.committed_seq,
        MetadataRow::AttributesRevision(record) => record.committed_seq,
        MetadataRow::AccessRevision(record) => record.committed_seq,
    }
}

#[cfg(test)]
pub(super) fn manifest_row_kind(row: &MetadataRow) -> &'static str {
    match row {
        MetadataRow::Inode(_) => "inode",
        MetadataRow::DirentryBinding(_) => "direntry_binding",
        MetadataRow::FileRevision(_) => "file_revision",
        MetadataRow::Tombstone(_) => "tombstone",
        MetadataRow::ActiveDeletion(_) => "active_deletion",
        MetadataRow::CommitReceipt(_) => "commit_receipt",
        MetadataRow::Commit(_) => "commit",
        MetadataRow::ContentPublication(_) => "content_publication",
        MetadataRow::AttributesRevision(_) => "attributes_revision",
        MetadataRow::AccessRevision(_) => "access_revision",
    }
}
