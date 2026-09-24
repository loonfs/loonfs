//! Decodes durable manifest rows into the in-memory metadata record types.
//!
//! Every segment scan is family-scoped, so a row of any other kind in the
//! result is namespace corruption, not a case to skip: each decoder
//! hard-rejects foreign rows instead of filtering them out.

use crate::error::CoreError;
use crate::metadata::{
    AccessRevisionRecord, ActiveDeletionRecord, AttributesRevisionRecord, CommitReceiptRecord,
    ContentPublicationRecord, DirentryBindingRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::MetadataRow;
use loonfs_api::wire::wal::WalCommitPayload;

/// The scanned segment can only hold `expected_kind` rows; the foreign row's
/// self-keyed row key names its actual kind and identity.
fn foreign_row(expected_kind: &str, row: &MetadataRow) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "manifest segment scan expected `{expected_kind}` rows but found foreign row `{}`",
        row.row_key()
    ))
}

pub(crate) fn inode_from_manifest_row(row: MetadataRow) -> Result<InodeRecord, CoreError> {
    match row {
        MetadataRow::Inode(record) => Ok(record),
        other => Err(foreign_row("inode", &other)),
    }
}

pub(crate) fn direntry_binding_from_manifest_row(
    row: MetadataRow,
) -> Result<DirentryBindingRecord, CoreError> {
    match row {
        MetadataRow::DirentryBinding(record) => Ok(record),
        other => Err(foreign_row("direntry_binding", &other)),
    }
}

pub(crate) fn revision_from_manifest_row(row: MetadataRow) -> Result<RevisionRecord, CoreError> {
    match row {
        MetadataRow::FileRevision(record) => Ok(record),
        other => Err(foreign_row("file_revision", &other)),
    }
}

pub(crate) fn tombstone_from_manifest_row(
    row: MetadataRow,
) -> Result<SubtreeTombstoneRecord, CoreError> {
    match row {
        MetadataRow::Tombstone(record) => Ok(record),
        other => Err(foreign_row("tombstone", &other)),
    }
}

pub(crate) fn active_deletion_from_manifest_row(
    row: MetadataRow,
) -> Result<ActiveDeletionRecord, CoreError> {
    match row {
        MetadataRow::ActiveDeletion(record) => Ok(record),
        other => Err(foreign_row("active_deletion", &other)),
    }
}

pub(crate) fn content_publication_from_manifest_row(
    row: MetadataRow,
) -> Result<ContentPublicationRecord, CoreError> {
    match row {
        MetadataRow::ContentPublication(record) => Ok(record),
        other => Err(foreign_row("content_publication", &other)),
    }
}

pub(crate) fn commit_receipt_from_manifest_row(
    row: MetadataRow,
) -> Result<CommitReceiptRecord, CoreError> {
    match row {
        MetadataRow::CommitReceipt(record) => Ok(record),
        other => Err(foreign_row("commit_receipt", &other)),
    }
}

pub(crate) fn commit_from_manifest_row(row: MetadataRow) -> Result<WalCommitPayload, CoreError> {
    match row {
        MetadataRow::Commit(record) if record.inline_content.is_empty() => Ok(record),
        MetadataRow::Commit(record) => Err(CoreError::NamespaceCorrupt(format!(
            "commit row at sequence `{}` carries inline content",
            record.seq
        ))),
        other => Err(foreign_row("commit", &other)),
    }
}

pub(crate) fn attributes_revision_from_manifest_row(
    row: MetadataRow,
) -> Result<AttributesRevisionRecord, CoreError> {
    match row {
        MetadataRow::AttributesRevision(record) => Ok(record),
        other => Err(foreign_row("attributes_revision", &other)),
    }
}

pub(crate) fn access_revision_from_manifest_row(
    row: MetadataRow,
) -> Result<AccessRevisionRecord, CoreError> {
    match row {
        MetadataRow::AccessRevision(record) => Ok(record),
        other => Err(foreign_row("access_revision", &other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::manifest::{DeltaPosition, TombstoneRowAction};
    use loonfs_api::{ChangeSeq, CommitId, InodeId};

    fn foreign() -> MetadataRow {
        MetadataRow::Inode(InodeRecord {
            inode_id: InodeId(7),
            inode_kind: loonfs_api::InodeKind::File,
            committed_seq: ChangeSeq(3),
            commit_id: CommitId::parse("c_foreign_inode").expect("commit id"),
            committed_by: loonfs_api::ActorId::loonfs(),
            committed_at_ms: 4_000,
        })
    }

    #[test]
    fn wrong_kind_rows_are_namespace_corruption() {
        let error = direntry_binding_from_manifest_row(foreign())
            .expect_err("foreign row must be rejected");
        assert!(
            matches!(&error, CoreError::NamespaceCorrupt(_)),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("`direntry_binding`"), "{message}");
        assert!(message.contains("inode-00000000000000000007"), "{message}");

        assert!(revision_from_manifest_row(foreign()).is_err());
        assert!(tombstone_from_manifest_row(foreign()).is_err());
        assert!(commit_receipt_from_manifest_row(foreign()).is_err());
        assert!(attributes_revision_from_manifest_row(foreign()).is_err());
        let tombstone = MetadataRow::Tombstone(SubtreeTombstoneRecord {
            root_inode_id: InodeId(1),
            generation: DeltaPosition {
                seq: ChangeSeq(1),
                delta_index: 0,
            },
            commit_id: CommitId::parse("c_foreign_tombstone").expect("commit id"),
            action: TombstoneRowAction::Set {
                deleted_binding: loonfs_api::wire::manifest::DeletedBinding {
                    parent_inode_id: InodeId(1),
                    name_key: loonfs_api::NameKey::parse("foreign").expect("valid name key"),
                    display_name: loonfs_api::DisplayName::parse("foreign")
                        .expect("valid display name"),
                },
            },
            committed_at_ms: 4_000,
            committed_by: loonfs_api::ActorId::loonfs(),
        });
        assert!(inode_from_manifest_row(tombstone).is_err());
    }
}
