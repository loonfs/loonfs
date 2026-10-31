//! Converts in-memory metadata state into manifest rows, per family and in
//! row-key order.

use crate::metadata::{active_deletion_from_tombstone, MetadataState};
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{
    ActiveDeletionRowAction, DirentryBindingRecord, MetadataRow, MetadataRowFamily,
};
use loonfs_types::{ChangeSeq, ContentId, InodeId, InodeKind, NameKey};
use std::collections::HashMap;

#[cfg(test)]
#[cfg(test)]
pub(crate) fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    MetadataRowFamily::ALL.into_iter().all(|family| {
        manifest_rows_for_family(left, family) == manifest_rows_for_family(right, family)
    })
}

pub(crate) fn manifest_rows_for_family(
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
        MetadataRowFamily::DirentryBinds => metadata_state
            .direntry_binds()
            .iter()
            .cloned()
            .map(MetadataRow::DirentryBinding)
            .collect::<Vec<_>>(),
        // The child index stores the binding events, without the slot
        // index's summary and subtree rows.
        MetadataRowFamily::DirentryChildBinds => metadata_state
            .direntry_binds()
            .iter()
            .filter(|binding| binding.is_binding_event())
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
            .map(active_deletion_from_tombstone)
            .map(MetadataRow::ActiveDeletion)
            .collect::<Vec<_>>(),
        MetadataRowFamily::ContentLayouts => metadata_state
            .content_layouts()
            .iter()
            .cloned()
            .map(MetadataRow::ContentLayout)
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
    if family == MetadataRowFamily::ContentLayouts {
        rows.dedup_by_key(|row| row.row_key_for_family(family));
    }
    if family == MetadataRowFamily::DirentryBinds {
        keep_newest_subtree_rows(&mut rows);
    }
    rows
}

/// Keeps the newest subtree row of each directory edge among `rows`, which
/// are in row-key order, and drops the edge's older subtree rows.
///
/// A fold writes the rows of the commits it folds, so a directory edge keeps
/// its subtree sequence as of the fold's last commit. A read at a sequence
/// inside the fold's range can then find an older subtree sequence
/// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
/// The binding events and the summary rows stay.
fn keep_newest_subtree_rows(rows: &mut Vec<MetadataRow>) {
    let mut kept_edge: Option<(InodeId, NameKey, InodeId)> = None;
    rows.retain(|row| {
        let MetadataRow::DirentryBinding(binding) = row else {
            return true;
        };
        if !is_subtree_row(binding) {
            return true;
        }
        let edge = (
            binding.parent_inode_id,
            binding.name_key.clone(),
            binding.child_inode_id,
        );
        // One edge's rows are adjacent and newest first, so the first
        // subtree row of an edge is its newest.
        if kept_edge.as_ref() == Some(&edge) {
            return false;
        }
        kept_edge = Some(edge);
        true
    });
}

/// Returns whether an `advance_subtree` delta wrote `binding`, which is true
/// of a directory row that isn't a binding event.
fn is_subtree_row(binding: &DirentryBindingRecord) -> bool {
    binding.child_kind == InodeKind::Directory && !binding.is_binding_event()
}

pub(crate) fn manifest_rows_for_family_after_seq(
    metadata_state: &MetadataState,
    family: MetadataRowFamily,
    after_seq: ChangeSeq,
) -> Vec<MetadataRow> {
    manifest_rows_for_family(metadata_state, family)
        .into_iter()
        .filter(|row| manifest_row_commit_seq(row) > after_seq)
        .collect()
}

/// Replaces tail layouts for chains the fold wrote and restores row-key order.
pub(super) fn with_layouts(
    family: MetadataRowFamily,
    mut rows: Vec<MetadataRow>,
    layouts: &HashMap<ContentId, ContentLayoutRecord>,
) -> Vec<MetadataRow> {
    if family == MetadataRowFamily::ContentLayouts {
        rows.retain(|row| !matches!(row, MetadataRow::ContentLayout(record) if layouts.contains_key(&record.content_id)));
        rows.extend(layouts.values().cloned().map(MetadataRow::ContentLayout));
        rows.sort_by_key(MetadataRow::row_key);
    }
    rows
}

pub(crate) fn manifest_row_commit_seq(row: &MetadataRow) -> ChangeSeq {
    match row {
        MetadataRow::Inode(record) => record.committed_seq,
        MetadataRow::DirentryBinding(record) => record.committed_seq,
        MetadataRow::FileRevision(record) => record.committed_seq,
        MetadataRow::Tombstone(record) => record.committed_seq,
        // A removal marker belongs to the run of the undelete that produced
        // it, not to the run of the deletion whose key it repeats.
        MetadataRow::ActiveDeletion(record) => match &record.action {
            ActiveDeletionRowAction::Listed { .. } => record.deletion_seq,
            ActiveDeletionRowAction::Removed { removed_seq } => *removed_seq,
        },
        MetadataRow::CommitReceipt(record) => record.committed_seq,
        MetadataRow::Commit(record) => record.committed_seq,
        MetadataRow::ContentLayout(record) => record.committed_seq,
        MetadataRow::AttributesRevision(record) => record.committed_seq,
        MetadataRow::AccessRevision(record) => record.committed_seq,
    }
}

#[cfg(test)]
pub(crate) fn manifest_row_kind(row: &MetadataRow) -> &'static str {
    match row {
        MetadataRow::Inode(_) => "inode",
        MetadataRow::DirentryBinding(_) => "direntry_binding",
        MetadataRow::FileRevision(_) => "file_revision",
        MetadataRow::Tombstone(_) => "tombstone",
        MetadataRow::ActiveDeletion(_) => "active_deletion",
        MetadataRow::CommitReceipt(_) => "commit_receipt",
        MetadataRow::Commit(_) => "commit",
        MetadataRow::ContentLayout(_) => "content_layout",
        MetadataRow::AttributesRevision(_) => "attributes_revision",
        MetadataRow::AccessRevision(_) => "access_revision",
    }
}
