//! Decodes durable manifest rows into the in-memory metadata record types.
//!
//! Every table scan is family-scoped, so a row of any other kind in the
//! result is namespace corruption, not a case to skip: each decoder
//! hard-rejects foreign rows instead of filtering them out.

use crate::error::CoreError;
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::MetadataRow;

/// The scanned table can only hold `expected_kind` rows; the foreign row's
/// self-keyed row key names its actual kind and identity.
fn foreign_row(expected_kind: &str, row: &MetadataRow) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "manifest table scan expected `{expected_kind}` rows but found foreign row `{}`",
        row.row_key()
    ))
}

pub(crate) fn inode_from_manifest_row(row: MetadataRow) -> Result<InodeRecord, CoreError> {
    match row {
        MetadataRow::Inode {
            inode_id,
            inode_kind,
            created_seq,
        } => Ok(InodeRecord {
            inode_id,
            inode_kind,
            created_seq,
        }),
        other => Err(foreign_row("inode", &other)),
    }
}

pub(crate) fn direntry_bind_from_manifest_row(
    row: MetadataRow,
) -> Result<DirentryBindRecord, CoreError> {
    match row {
        MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        } => Ok(DirentryBindRecord {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        }),
        other => Err(foreign_row("direntry_bind", &other)),
    }
}

pub(crate) fn direntry_unbind_from_manifest_row(
    row: MetadataRow,
) -> Result<DirentryUnbindRecord, CoreError> {
    match row {
        MetadataRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        } => Ok(DirentryUnbindRecord {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        }),
        other => Err(foreign_row("direntry_unbind", &other)),
    }
}

pub(crate) fn revision_from_manifest_row(row: MetadataRow) -> Result<RevisionRecord, CoreError> {
    match row {
        MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            committed_at_ms,
            revision_delta_index,
            content_ref,
        } => Ok(RevisionRecord {
            inode_id,
            revision_no,
            committed_seq,
            committed_at_ms,
            revision_delta_index,
            content_ref,
        }),
        other => Err(foreign_row("revision", &other)),
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

pub(crate) fn tombstone_from_manifest_row(
    row: MetadataRow,
) -> Result<SubtreeTombstoneRecord, CoreError> {
    match row {
        MetadataRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
            action,
        } => Ok(SubtreeTombstoneRecord {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
            action: subtree_tombstone_action(&action),
        }),
        other => Err(foreign_row("tombstone", &other)),
    }
}

pub(crate) fn commit_receipt_from_manifest_row(
    row: MetadataRow,
) -> Result<CommitReceiptRecord, CoreError> {
    match row {
        MetadataRow::CommitReceipt {
            commit_id,
            semantic_commit_fingerprint,
            committed_seq,
            committed_at_ms,
            message,
        } => Ok(CommitReceiptRecord {
            commit_id,
            semantic_commit_fingerprint,
            committed_seq,
            committed_at_ms,
            message,
        }),
        other => Err(foreign_row("commit_receipt", &other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{ChangeSeq, InodeId};

    fn foreign() -> MetadataRow {
        MetadataRow::Inode {
            inode_id: InodeId(7),
            inode_kind: loonfs_api::InodeKind::File,
            created_seq: ChangeSeq(3),
        }
    }

    #[test]
    fn wrong_kind_rows_are_namespace_corruption() {
        let error =
            direntry_bind_from_manifest_row(foreign()).expect_err("foreign row must be rejected");
        assert!(
            matches!(&error, CoreError::NamespaceCorrupt(_)),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("`direntry_bind`"), "{message}");
        assert!(message.contains("inode-00000000000000000007"), "{message}");

        assert!(direntry_unbind_from_manifest_row(foreign()).is_err());
        assert!(revision_from_manifest_row(foreign()).is_err());
        assert!(tombstone_from_manifest_row(foreign()).is_err());
        assert!(commit_receipt_from_manifest_row(foreign()).is_err());
        let tombstone = MetadataRow::Tombstone {
            root_inode_id: InodeId(1),
            tombstone_seq: ChangeSeq(1),
            tombstone_delta_index: 0,
            action: loonfs_api::wire::manifest::TombstoneRowAction::Set,
        };
        assert!(inode_from_manifest_row(tombstone).is_err());
    }
}
