//! Decodes durable manifest rows into the in-memory metadata record types.
//!
//! Every table scan is family-scoped, so a row of any other kind in the
//! result is namespace corruption, not a case to skip: each decoder
//! hard-rejects foreign rows instead of filtering them out.

use crate::error::CoreError;
use crate::metadata::{
    ActiveDeletionAction, ActiveDeletionRecord, AttributesRevisionRecord, CommitReceiptRecord,
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord, SubtreeTombstoneAction,
    SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::{ActiveDeletionRowAction, MetadataRow, TombstoneRowAction};

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
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        } => Ok(DirentryUnbindRecord {
            parent_inode_id,
            name_key,
            display_name,
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

fn subtree_tombstone_action(action: TombstoneRowAction) -> SubtreeTombstoneAction {
    match action {
        TombstoneRowAction::Set { deleted_direntry } => {
            SubtreeTombstoneAction::Set { deleted_direntry }
        }
        TombstoneRowAction::Revoke { target } => SubtreeTombstoneAction::Revoke { target },
    }
}

pub(crate) fn tombstone_from_manifest_row(
    row: MetadataRow,
) -> Result<SubtreeTombstoneRecord, CoreError> {
    match row {
        MetadataRow::Tombstone {
            root_inode_id,
            generation,
            action,
            deleted_at_ms,
        } => Ok(SubtreeTombstoneRecord {
            root_inode_id,
            generation,
            deleted_at_ms,
            action: subtree_tombstone_action(action),
        }),
        other => Err(foreign_row("tombstone", &other)),
    }
}

pub(crate) fn active_deletion_from_manifest_row(
    row: MetadataRow,
) -> Result<ActiveDeletionRecord, CoreError> {
    match row {
        MetadataRow::ActiveDeletion {
            root_inode_id,
            deleted_at_seq,
            action,
        } => Ok(ActiveDeletionRecord {
            root_inode_id,
            deleted_at_seq,
            action: match action {
                ActiveDeletionRowAction::Listed {
                    deleted_at_ms,
                    deleted_direntry,
                } => ActiveDeletionAction::Listed {
                    deleted_at_ms,
                    deleted_direntry,
                },
                ActiveDeletionRowAction::Removed { revoked_at_seq } => {
                    ActiveDeletionAction::Removed { revoked_at_seq }
                }
            },
        }),
        other => Err(foreign_row("active_deletion", &other)),
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

pub(crate) fn attributes_revision_from_manifest_row(
    row: MetadataRow,
) -> Result<AttributesRevisionRecord, CoreError> {
    match row {
        MetadataRow::AttributesRevision {
            inode_id,
            attributes_revision_no,
            committed_seq,
            delta_index,
            attributes,
        } => Ok(AttributesRevisionRecord {
            inode_id,
            attributes_revision_no,
            committed_seq,
            delta_index,
            attributes,
        }),
        other => Err(foreign_row("attributes_revision", &other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::manifest::TombstoneGeneration;
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
        assert!(attributes_revision_from_manifest_row(foreign()).is_err());
        let tombstone = MetadataRow::Tombstone {
            root_inode_id: InodeId(1),
            generation: TombstoneGeneration {
                seq: ChangeSeq(1),
                delta_index: 0,
            },
            action: TombstoneRowAction::Set {
                deleted_direntry: None,
            },
            deleted_at_ms: 4_000,
        };
        assert!(inode_from_manifest_row(tombstone).is_err());
    }
}
