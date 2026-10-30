//! Durable row keys, lookup prefixes, and Bloom filter probes for metadata
//! rows ([metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys)).

use crate::metadata_rows::{
    ActiveDeletionRecord, ActiveDeletionRowAction, MetadataRow, MetadataRowFamily,
};

impl MetadataRowFamily {
    /// Returns the fixed prefix before the first variable component in this
    /// family's row keys.
    ///
    /// Compaction uses the remaining components to group rows for retention.
    pub const fn row_key_prefix(self) -> &'static str {
        match self {
            Self::Inodes => lookup_keys::INODE_ROW_PREFIX,
            Self::DirentryBinds => lookup_keys::DIRENTRY_BIND_ROW_PREFIX,
            Self::DirentryChildBinds => lookup_keys::DIRENTRY_CHILD_BIND_ROW_PREFIX,
            Self::DirentryRevisions => lookup_keys::DIRENTRY_REVISION_ROW_PREFIX,
            Self::Revisions => lookup_keys::REVISION_ROW_PREFIX,
            Self::Tombstones => lookup_keys::TOMBSTONE_ROW_PREFIX,
            Self::ActiveDeletions => lookup_keys::ACTIVE_DELETION_ROW_PREFIX,
            Self::CommitReceipts => lookup_keys::COMMIT_RECEIPT_ROW_PREFIX,
            Self::Commits => lookup_keys::COMMIT_ROW_PREFIX,
            Self::ContentLayouts => lookup_keys::CONTENT_LAYOUT_ROW_PREFIX,
            Self::Attributes => lookup_keys::ATTRIBUTE_ROW_PREFIX,
            Self::Access => lookup_keys::ACCESS_ROW_PREFIX,
        }
    }
}

impl ActiveDeletionRowAction {
    /// The row-key component that orders a removal ahead of the row it
    /// removes.
    fn sort_rank(&self) -> u32 {
        match self {
            Self::Removed { .. } => lookup_keys::ACTIVE_DELETION_RANK_REMOVED,
            Self::Listed { .. } => lookup_keys::ACTIVE_DELETION_RANK_LISTED,
        }
    }
}

impl ActiveDeletionRecord {
    /// Builds this row's durable key in trash-listing order.
    pub fn row_key(&self) -> String {
        lookup_keys::active_deletion_row_key(
            self.deletion_seq,
            self.root_inode_id,
            self.action.sort_rank(),
        )
    }
}

impl MetadataRow {
    /// Builds this row's canonical durable key in its primary row family.
    ///
    /// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
    pub fn row_key(&self) -> String {
        self.row_key_for_family(match self {
            Self::Inode(_) => MetadataRowFamily::Inodes,
            Self::DirentryBinding(_) => MetadataRowFamily::DirentryBinds,
            Self::DirentryRevision(_) => MetadataRowFamily::DirentryRevisions,
            Self::FileRevision(_) => MetadataRowFamily::Revisions,
            Self::Tombstone(_) => MetadataRowFamily::Tombstones,
            Self::ActiveDeletion(_) => MetadataRowFamily::ActiveDeletions,
            Self::CommitReceipt(_) => MetadataRowFamily::CommitReceipts,
            Self::Commit(_) => MetadataRowFamily::Commits,
            Self::ContentLayout(_) => MetadataRowFamily::ContentLayouts,
            Self::AttributesRevision(_) => MetadataRowFamily::Attributes,
            Self::AccessRevision(_) => MetadataRowFamily::Access,
        })
    }

    /// Builds this row's durable key using the selected primary or secondary ordering.
    ///
    /// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
    pub fn row_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode(record) => lookup_keys::inode_key(record.inode_id),
            Self::DirentryBinding(record) => match family {
                MetadataRowFamily::DirentryBinds => Some(lookup_keys::direntry_bind_row_key(
                    record.parent_inode_id,
                    &record.name_key,
                    record.child_inode_id,
                    record.committed_seq,
                    record.delta_index,
                )),
                MetadataRowFamily::DirentryChildBinds => {
                    Some(lookup_keys::direntry_child_bind_row_key(
                        record.child_inode_id,
                        record.parent_inode_id,
                        record.name_key.as_str(),
                        record.committed_seq,
                        record.delta_index,
                    ))
                }
                MetadataRowFamily::Inodes
                | MetadataRowFamily::DirentryRevisions
                | MetadataRowFamily::Revisions
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::CommitReceipts
                | MetadataRowFamily::Commits
                | MetadataRowFamily::ContentLayouts
                | MetadataRowFamily::Attributes
                | MetadataRowFamily::Access => None,
            }
            .expect("a binding row should use a binding family"),
            Self::DirentryRevision(record) => lookup_keys::direntry_revision_row_key(
                record.parent_inode_id,
                &record.name_key,
                record.child_inode_id,
                record.committed_seq,
                record.delta_index,
            ),
            Self::FileRevision(record) => lookup_keys::revision_row_key(
                record.inode_id,
                record.revision_no,
                record.committed_seq,
                record.delta_index,
            ),
            Self::Tombstone(record) => lookup_keys::tombstone_row_key(
                record.root_inode_id,
                record.committed_seq,
                record.delta_index,
            ),
            Self::ActiveDeletion(record) => lookup_keys::active_deletion_row_key(
                record.deletion_seq,
                record.root_inode_id,
                record.action.sort_rank(),
            ),
            Self::CommitReceipt(record) => {
                lookup_keys::commit_receipt_row_key(&record.commit_id, record.committed_seq)
            }
            Self::Commit(record) => lookup_keys::commit_row_key(record.committed_seq),
            Self::ContentLayout(record) => lookup_keys::content_layout_row_key(
                &record.content_id,
                record.size_bytes,
                record.committed_seq,
            ),
            Self::AttributesRevision(record) => lookup_keys::attributes_row_key(
                record.inode_id,
                record.attributes_revision_no,
                record.committed_seq,
                record.delta_index,
            ),
            Self::AccessRevision(record) => lookup_keys::access_row_key(
                record.inode_id,
                record.access_revision_no,
                record.committed_seq,
                record.delta_index,
            ),
        }
    }

    /// Returns the Bloom filter key for this row in `family`.
    pub fn filter_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode(_) => self.row_key_for_family(family),
            Self::DirentryBinding(record) => match family {
                MetadataRowFamily::DirentryBinds => Some(lookup_keys::direntry_bind_probe(
                    record.parent_inode_id,
                    &record.name_key,
                )),
                MetadataRowFamily::DirentryChildBinds => {
                    Some(lookup_keys::direntry_child_probe(record.child_inode_id))
                }
                MetadataRowFamily::Inodes
                | MetadataRowFamily::DirentryRevisions
                | MetadataRowFamily::Revisions
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::CommitReceipts
                | MetadataRowFamily::Commits
                | MetadataRowFamily::ContentLayouts
                | MetadataRowFamily::Attributes
                | MetadataRowFamily::Access => None,
            }
            .expect("a binding row should use a binding family"),
            Self::DirentryRevision(record) => {
                lookup_keys::direntry_revision_probe(record.parent_inode_id, &record.name_key)
            }
            Self::FileRevision(record) => lookup_keys::revision_probe(record.inode_id),
            Self::Tombstone(record) => lookup_keys::tombstone_probe(record.root_inode_id),
            // The family is only ever range-scanned in key order, never
            // probed for one deletion, so the filter key is the row key.
            Self::ActiveDeletion(_) => self.row_key_for_family(family),
            Self::CommitReceipt(record) => lookup_keys::commit_receipt_probe(&record.commit_id),
            Self::Commit(_) => self.row_key_for_family(family),
            Self::ContentLayout(record) => lookup_keys::content_layout_probe(&record.content_id),
            Self::AttributesRevision(record) => lookup_keys::attributes_probe(record.inode_id),
            Self::AccessRevision(record) => lookup_keys::access_probe(record.inode_id),
        }
    }
}

/// Encodes an arbitrary string so it can occupy one component of a durable row key.
///
/// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
pub fn hex_encode_row_key_component(value: &str) -> String {
    crate::hex::hex_encode_bytes(value.as_bytes())
}

/// Builders for metadata row keys, lookup prefixes, and Bloom filter probes.
///
/// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
pub mod lookup_keys {
    use super::hex_encode_row_key_component;
    use crate::{
        AccessRevisionNo, AttributesRevisionNo, ChangeSeq, CommitId, ContentId, InodeId, NameKey,
        RevisionNo,
    };

    /// Prefix for inode row keys.
    pub const INODE_ROW_PREFIX: &str = "inode-";

    /// Prefix for revision row keys.
    pub const REVISION_ROW_PREFIX: &str = "revision-";

    /// Prefix for commit row keys.
    pub const COMMIT_ROW_PREFIX: &str = "commit-";

    pub(super) const DIRENTRY_BIND_ROW_PREFIX: &str = "direntry-bind-";
    pub(super) const DIRENTRY_CHILD_BIND_ROW_PREFIX: &str = "direntry-child-bind-";
    pub(super) const DIRENTRY_REVISION_ROW_PREFIX: &str = "direntry-revision-";
    pub(super) const TOMBSTONE_ROW_PREFIX: &str = "tombstone-";
    pub(super) const CONTENT_LAYOUT_ROW_PREFIX: &str = "content-layout-";
    pub(super) const COMMIT_RECEIPT_ROW_PREFIX: &str = "commit-receipt-";
    pub(super) const ATTRIBUTE_ROW_PREFIX: &str = "attribute-";
    pub(super) const ACCESS_ROW_PREFIX: &str = "access-";

    /// Builds the exclusive lower bound after `row_key`.
    pub fn after_row_key(row_key: &str) -> String {
        format!("{row_key}\0")
    }

    /// Builds a commit row key.
    pub fn commit_row_key(seq: ChangeSeq) -> String {
        format!("{COMMIT_ROW_PREFIX}{:020}", seq.0)
    }

    /// Builds an inode row key.
    pub fn inode_key(inode_id: InodeId) -> String {
        format!("{INODE_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds a scan bound immediately after an inode row.
    pub fn inode_key_after(inode_id: InodeId) -> String {
        after_row_key(&inode_key(inode_id))
    }

    /// Builds the prefix for directory bindings under one parent.
    pub fn direntry_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("{DIRENTRY_BIND_ROW_PREFIX}{:020}-", parent_inode_id.0)
    }

    /// Builds the Bloom filter probe for a parent/name binding.
    pub fn direntry_bind_probe(parent_inode_id: InodeId, name_key: &NameKey) -> String {
        format!(
            "{}{}",
            direntry_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key.as_str())
        )
    }

    /// Builds the prefix for every version of a parent/name slot.
    pub fn direntry_bind_prefix(parent_inode_id: InodeId, name_key: &NameKey) -> String {
        format!("{}-", direntry_bind_probe(parent_inode_id, name_key))
    }

    /// Builds a row key for one version of a child's binding in a parent/name slot.
    pub fn direntry_bind_row_key(
        parent_inode_id: InodeId,
        name_key: &NameKey,
        child_inode_id: InodeId,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{delta_index:010}",
            direntry_bind_prefix(parent_inode_id, name_key),
            child_inode_id.0,
            committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for bindings to one child inode.
    pub fn direntry_child_probe(child_inode_id: InodeId) -> String {
        format!("{DIRENTRY_CHILD_BIND_ROW_PREFIX}{:020}", child_inode_id.0)
    }

    /// Builds the reverse-index prefix for bindings to one child inode.
    pub fn direntry_child_prefix(child_inode_id: InodeId) -> String {
        format!("{}-", direntry_child_probe(child_inode_id))
    }

    /// Builds a reverse-index row key for one version of a child's binding in a parent/name slot.
    pub(super) fn direntry_child_bind_row_key(
        child_inode_id: InodeId,
        parent_inode_id: InodeId,
        name_key: &str,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{}-{:020}-{delta_index:010}",
            direntry_child_prefix(child_inode_id),
            parent_inode_id.0,
            hex_encode_row_key_component(name_key),
            committed_seq.0
        )
    }

    /// Builds the prefix for the revisions stored under one parent's edges.
    pub fn direntry_revision_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("{DIRENTRY_REVISION_ROW_PREFIX}{:020}-", parent_inode_id.0)
    }

    /// Builds the Bloom filter probe for the revisions stored under one
    /// parent/name slot.
    pub fn direntry_revision_probe(parent_inode_id: InodeId, name_key: &NameKey) -> String {
        format!(
            "{}{}",
            direntry_revision_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key.as_str())
        )
    }

    /// Builds the prefix for the revisions stored under one parent/name slot.
    pub fn direntry_revision_prefix(parent_inode_id: InodeId, name_key: &NameKey) -> String {
        format!("{}-", direntry_revision_probe(parent_inode_id, name_key))
    }

    /// Builds a row key for one version of the revision stored under a
    /// file's edge.
    pub fn direntry_revision_row_key(
        parent_inode_id: InodeId,
        name_key: &NameKey,
        child_inode_id: InodeId,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{delta_index:010}",
            direntry_revision_prefix(parent_inode_id, name_key),
            child_inode_id.0,
            committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for one tombstone root.
    pub fn tombstone_probe(root_inode_id: InodeId) -> String {
        format!("{TOMBSTONE_ROW_PREFIX}{:020}", root_inode_id.0)
    }

    /// Builds the prefix for a root inode's tombstone history.
    pub fn tombstone_prefix(root_inode_id: InodeId) -> String {
        format!("{}-", tombstone_probe(root_inode_id))
    }

    /// Builds a row key for one tombstone event.
    ///
    /// The action is stored in the value, so delete and revoke rows for one
    /// position share a key.
    pub(super) fn tombstone_row_key(
        root_inode_id: InodeId,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:010}",
            tombstone_prefix(root_inode_id),
            committed_seq.0,
            delta_index
        )
    }

    /// Prefix for active-deletion row keys.
    pub const ACTIVE_DELETION_ROW_PREFIX: &str = "active-deletion-";

    /// Rank of an undelete's removal marker within one deletion position.
    /// It is the lowest rank on purpose: an ascending scan sees the removal
    /// before the row it removes, so a page never lists a deletion whose
    /// marker was going to arrive one page later.
    pub(super) const ACTIVE_DELETION_RANK_REMOVED: u32 = 0;

    /// Rank of the listed row within one deletion position, and the highest
    /// rank the family defines.
    pub(super) const ACTIVE_DELETION_RANK_LISTED: u32 = 1;

    /// Builds an active-deletion row key.
    pub(super) fn active_deletion_row_key(
        deletion_seq: ChangeSeq,
        root_inode_id: InodeId,
        sort_rank: u32,
    ) -> String {
        format!(
            "{ACTIVE_DELETION_ROW_PREFIX}{:020}-{:020}-{sort_rank:010}",
            deletion_seq.0, root_inode_id.0
        )
    }

    /// Builds a trash scan bound after one deletion position.
    pub fn active_deletion_key_after(deletion_seq: ChangeSeq, root_inode_id: InodeId) -> String {
        after_row_key(&active_deletion_row_key(
            deletion_seq,
            root_inode_id,
            ACTIVE_DELETION_RANK_LISTED,
        ))
    }

    /// Builds a scan bound after every active-deletion row whose deletion
    /// sequence is at or below `deletion_seq`.
    pub fn active_deletion_key_after_seq(deletion_seq: ChangeSeq) -> String {
        // `.` is the byte after the `-` that ends the sequence component, so
        // the bound sorts after every key of this sequence and before every
        // key of the next one.
        format!("{ACTIVE_DELETION_ROW_PREFIX}{:020}.", deletion_seq.0)
    }

    /// Selects one chain in the Bloom filter.
    pub fn content_layout_probe(content_id: &ContentId) -> String {
        format!("{CONTENT_LAYOUT_ROW_PREFIX}{content_id}")
    }

    /// Selects the rows for one content identity.
    pub fn content_layout_prefix(content_id: &ContentId) -> String {
        format!("{}-", content_layout_probe(content_id))
    }

    /// Orders the longest layout first, then the newest commit.
    pub fn content_layout_row_key(
        content_id: &ContentId,
        size_bytes: u64,
        committed_seq: ChangeSeq,
    ) -> String {
        format!(
            "{}{:020}-{:020}",
            content_layout_prefix(content_id),
            u64::MAX - size_bytes,
            u64::MAX - committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for one commit ID.
    pub fn commit_receipt_probe(commit_id: &CommitId) -> String {
        format!(
            "{COMMIT_RECEIPT_ROW_PREFIX}{}",
            hex_encode_row_key_component(commit_id.as_str())
        )
    }

    /// Builds the prefix for receipts with one commit ID.
    pub fn commit_receipt_prefix(commit_id: &CommitId) -> String {
        format!("{}-", commit_receipt_probe(commit_id))
    }

    /// Builds a commit receipt row key.
    pub(super) fn commit_receipt_row_key(commit_id: &CommitId, committed_seq: ChangeSeq) -> String {
        format!(
            "{}{:020}",
            commit_receipt_prefix(commit_id),
            committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for an inode's revisions.
    pub fn revision_probe(inode_id: InodeId) -> String {
        format!("{REVISION_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first revisions.
    pub fn revision_prefix(inode_id: InodeId) -> String {
        format!("{}-", revision_probe(inode_id))
    }

    /// Builds a prefix for one revision number within an inode.
    pub fn revision_no_prefix(inode_id: InodeId, revision_no: RevisionNo) -> String {
        format!(
            "{}{:020}-",
            revision_prefix(inode_id),
            u64::MAX - revision_no.0
        )
    }

    /// Builds a newest-first revision row key.
    pub fn revision_row_key(
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:010}",
            revision_no_prefix(inode_id, revision_no),
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }

    /// Builds the Bloom filter probe for an inode's attribute revisions.
    pub fn attributes_probe(inode_id: InodeId) -> String {
        format!("{ATTRIBUTE_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first attribute revisions.
    pub fn attributes_prefix(inode_id: InodeId) -> String {
        format!("{}-", attributes_probe(inode_id))
    }

    /// Builds a row key for an attribute revision.
    pub(super) fn attributes_row_key(
        inode_id: InodeId,
        attributes_revision_no: AttributesRevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            attributes_prefix(inode_id),
            u64::MAX - attributes_revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }

    /// Builds the Bloom filter probe for an inode's access revisions.
    pub fn access_probe(inode_id: InodeId) -> String {
        format!("{ACCESS_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first access revisions.
    pub fn access_prefix(inode_id: InodeId) -> String {
        format!("{}-", access_probe(inode_id))
    }

    /// Builds a row key for an access revision.
    pub(super) fn access_row_key(
        inode_id: InodeId,
        access_revision_no: AccessRevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            access_prefix(inode_id),
            u64::MAX - access_revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }
}

#[cfg(test)]
mod tests {
    use super::lookup_keys;
    use crate::metadata_rows::{
        AccessRevisionRecord, ActiveDeletionRecord, ActiveDeletionRowAction,
        AttributesRevisionRecord, CommitReceiptRecord, ContentLayoutRecord, DeletedBinding,
        DirentryBindingRecord, DirentryBindingState, DirentryRevisionRecord, InodeRecord,
        MetadataRow, MetadataRowFamily, RevisionRecord, SubtreeTombstoneRecord, TombstoneRowAction,
    };
    use crate::{ChangeSeq, CommitId, InodeId, NameKey, NamespaceId};

    fn row_commit_id() -> CommitId {
        CommitId::parse("c_metadata_row").expect("commit id")
    }

    fn direntry_revision_row(actor: crate::ActorId) -> MetadataRow {
        MetadataRow::DirentryRevision(DirentryRevisionRecord {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            child_inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            content_ref: crate::ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                    .expect("valid content id"),
                b"direntry revision sample",
                crate::ChecksumAlgorithm::Crc64nvme,
            ),
            revision_committed_by: actor,
            revision_committed_at_ms: 12_000,
            committed_seq: ChangeSeq(17),
            delta_index: 3,
        })
    }

    fn deleted_binding() -> DeletedBinding {
        DeletedBinding {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: crate::DisplayName::parse("report.txt").expect("valid display name"),
        }
    }

    #[test]
    fn inode_row_keys_sort_by_ascending_inode_id() {
        // The inode family's durable order IS ascending inode id, which is
        // what lets a whole-namespace file walk resume from one bound.
        let ids = [9_u64, 1, 100, 10, 2];
        let key_of = |id: u64| lookup_keys::inode_key(InodeId(id));
        let mut keys: Vec<String> = ids.iter().copied().map(key_of).collect();
        keys.sort();

        let mut ascending_ids = ids;
        ascending_ids.sort_unstable();
        assert_eq!(
            keys,
            ascending_ids
                .iter()
                .copied()
                .map(key_of)
                .collect::<Vec<_>>(),
            "row-key order must agree with inode-id order"
        );
        assert!(keys
            .iter()
            .all(|key| key.starts_with(lookup_keys::INODE_ROW_PREFIX)));
    }

    #[test]
    fn the_inode_resume_bound_skips_its_own_row_and_nothing_after_it() {
        let resume = lookup_keys::inode_key_after(InodeId(7));
        assert!(resume > lookup_keys::inode_key(InodeId(7)));
        assert!(resume < lookup_keys::inode_key(InodeId(8)));
    }

    #[test]
    fn direntry_bind_row_key_supports_parent_and_child_indexes() {
        let row = MetadataRow::DirentryBinding(DirentryBindingRecord {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            state: DirentryBindingState::Bound {
                display_name: crate::DisplayName::parse("Report.txt").expect("valid display name"),
            },
            child_inode_id: InodeId(42),
            child_kind: crate::InodeKind::File,
            child_created_by: crate::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(17),
            delta_index: 3,
        });

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryBinds),
            "direntry-bind-00000000000000000009-7265706f72742e747874-00000000000000000042-00000000000000000017-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryChildBinds),
            "direntry-child-bind-00000000000000000042-00000000000000000009-7265706f72742e747874-00000000000000000017-0000000003"
        );
    }

    #[test]
    fn direntry_revision_row_keys_follow_the_slot_index_order() {
        let row = direntry_revision_row(crate::ActorId::loonfs());
        let key = row.row_key();
        assert_eq!(
            key,
            "direntry-revision-00000000000000000009-7265706f72742e747874-00000000000000000042-00000000000000000017-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryRevisions),
            key
        );
        let name_key = NameKey::parse("report.txt").expect("valid name key");
        assert!(key.starts_with(&lookup_keys::direntry_revision_parent_prefix(InodeId(9))));
        assert!(key.starts_with(&lookup_keys::direntry_revision_prefix(
            InodeId(9),
            &name_key
        )));
        assert_eq!(
            row.filter_key_for_family(MetadataRowFamily::DirentryRevisions),
            lookup_keys::direntry_revision_probe(InodeId(9), &name_key)
        );
        // The key after the family prefix is the slot index's key for the
        // same edge and position, so both families sort one parent's edges
        // the same way.
        let bind_key = lookup_keys::direntry_bind_row_key(
            InodeId(9),
            &name_key,
            InodeId(42),
            ChangeSeq(17),
            3,
        );
        assert_eq!(
            key.strip_prefix(MetadataRowFamily::DirentryRevisions.row_key_prefix()),
            bind_key.strip_prefix(MetadataRowFamily::DirentryBinds.row_key_prefix())
        );
    }

    #[test]
    fn row_keys_hex_encode_dash_containing_variable_components() {
        let row = MetadataRow::DirentryBinding(DirentryBindingRecord {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report-2024").expect("valid name key"),
            state: DirentryBindingState::Bound {
                display_name: crate::DisplayName::parse("report-2024").expect("valid display name"),
            },
            child_inode_id: InodeId(42),
            child_kind: crate::InodeKind::File,
            child_created_by: crate::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(17),
            delta_index: 3,
        });

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryBinds),
            "direntry-bind-00000000000000000009-7265706f72742d32303234-00000000000000000042-00000000000000000017-0000000003"
        );
    }

    #[test]
    fn revision_row_key_orders_newest_first_within_each_inode() {
        let row = MetadataRow::FileRevision(RevisionRecord {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorId::loonfs(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                    .expect("valid content id"),
                b"row key sample",
                crate::ChecksumAlgorithm::Crc64nvme,
            ),
        });

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::Revisions),
            "revision-00000000000000000042-18446744073709551608-18446744073709551603-4294967292"
        );
    }

    #[test]
    fn whole_state_row_keys_sort_newest_revision_first_under_the_inode_prefix() {
        let row_of = |revision: u64, seq: u64, delta_index: u32| {
            MetadataRow::AttributesRevision(AttributesRevisionRecord {
                inode_id: InodeId(42),
                attributes_revision_no: crate::AttributesRevisionNo(revision),
                committed_seq: ChangeSeq(seq),
                commit_id: row_commit_id(),
                delta_index,
                committed_by: crate::ActorId::loonfs(),
                committed_at_ms: 12_000 + seq,
                attributes: crate::Attributes::default(),
            })
        };
        let newest = row_of(3, 12, 1);
        let older = row_of(2, 11, 0);

        assert_eq!(
            newest.row_key_for_family(MetadataRowFamily::Attributes),
            "attribute-00000000000000000042-18446744073709551612-18446744073709551603-4294967294"
        );
        assert_eq!(
            newest.row_key(),
            newest.row_key_for_family(MetadataRowFamily::Attributes)
        );
        assert!(
            newest.row_key() < older.row_key(),
            "an ascending scan must reach the newest revision first"
        );
        let prefix = lookup_keys::attributes_prefix(InodeId(42));
        assert!(newest.row_key().starts_with(&prefix));
        assert!(older.row_key().starts_with(&prefix));
        // A point lookup probes the filter with the inode's shared key, and
        // the writer stores exactly that key.
        assert_eq!(
            newest.filter_key_for_family(MetadataRowFamily::Attributes),
            lookup_keys::attributes_probe(InodeId(42))
        );
        // Another inode's rows sort outside the prefix.
        assert!(!row_of(3, 12, 1)
            .row_key()
            .starts_with(&lookup_keys::attributes_prefix(InodeId(43))));

        let access_row = |revision, seq, delta_index| {
            MetadataRow::AccessRevision(AccessRevisionRecord {
                inode_id: InodeId(42),
                access_revision_no: crate::AccessRevisionNo(revision),
                committed_seq: ChangeSeq(seq),
                commit_id: crate::CommitId::parse("c_access").expect("commit"),
                delta_index,
                committed_by: crate::ActorId::loonfs(),
                committed_at_ms: 1_000,
                boundary: false,
                grants: crate::AccessGrants::default(),
            })
        };
        let newest = access_row(3, 12, 1);
        let older = access_row(2, 11, 0);
        assert_eq!(
            newest.row_key(),
            "access-00000000000000000042-18446744073709551612-18446744073709551603-4294967294"
        );
        assert!(newest.row_key() < older.row_key());
        assert_eq!(
            newest.filter_key_for_family(MetadataRowFamily::Access),
            lookup_keys::access_probe(InodeId(42))
        );
    }

    #[test]
    fn row_key_prefixes_match_the_row_keys_they_front() {
        let name_key = NameKey::parse("report.txt").expect("valid name key");
        let display_name = crate::DisplayName::parse("report.txt").expect("valid display name");
        let bind = MetadataRow::DirentryBinding(DirentryBindingRecord {
            parent_inode_id: InodeId(9),
            name_key: name_key.clone(),
            state: DirentryBindingState::Bound {
                display_name: display_name.clone(),
            },
            child_inode_id: InodeId(42),
            child_kind: crate::InodeKind::File,
            child_created_by: crate::ActorId::loonfs(),
            child_created_at_ms: 4_200,
            committed_seq: ChangeSeq(17),
            delta_index: 3,
        });
        let revision = MetadataRow::FileRevision(RevisionRecord {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorId::loonfs(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                    .expect("valid content id"),
                b"row key prefix sample",
                crate::ChecksumAlgorithm::Crc64nvme,
            ),
        });
        let rows: [(MetadataRowFamily, MetadataRow); 10] = [
            (
                MetadataRowFamily::Inodes,
                MetadataRow::Inode(InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: crate::InodeKind::File,
                    committed_seq: ChangeSeq(3),
                    commit_id: row_commit_id(),
                    committed_by: crate::ActorId::loonfs(),
                    committed_at_ms: 3_000,
                }),
            ),
            (MetadataRowFamily::DirentryBinds, bind.clone()),
            (MetadataRowFamily::DirentryChildBinds, bind),
            (
                MetadataRowFamily::DirentryRevisions,
                direntry_revision_row(crate::ActorId::loonfs()),
            ),
            (MetadataRowFamily::Revisions, revision),
            (
                MetadataRowFamily::ContentLayouts,
                MetadataRow::ContentLayout(ContentLayoutRecord {
                    owner_namespace_id: NamespaceId::parse("demo").expect("namespace"),
                    content_id: crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                        .expect("content id"),
                    committed_seq: ChangeSeq(12),
                    size_bytes: 5,
                    layout: crate::ContentLayout {
                        extents: vec![crate::ContentExtent {
                            owner_namespace_id: NamespaceId::parse("demo").expect("namespace"),
                            content_id: crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                                .expect("content id"),
                            object: crate::ExtentObject::Whole,
                            offset: 0,
                            length: 5,
                        }],
                    },
                }),
            ),
            (
                MetadataRowFamily::Tombstones,
                MetadataRow::Tombstone(SubtreeTombstoneRecord {
                    root_inode_id: InodeId(42),
                    committed_seq: ChangeSeq(12),
                    delta_index: 0,
                    commit_id: row_commit_id(),
                    action: TombstoneRowAction::Set {
                        deleted_binding: deleted_binding(),
                    },
                    committed_at_ms: 12_000,
                    committed_by: crate::ActorId::loonfs(),
                }),
            ),
            (
                MetadataRowFamily::ActiveDeletions,
                MetadataRow::ActiveDeletion(ActiveDeletionRecord {
                    root_inode_id: InodeId(42),
                    deletion_seq: ChangeSeq(12),
                    action: ActiveDeletionRowAction::Removed {
                        removed_seq: ChangeSeq(15),
                    },
                }),
            ),
            (
                MetadataRowFamily::CommitReceipts,
                MetadataRow::CommitReceipt(CommitReceiptRecord {
                    commit_id: CommitId::parse("c_00000000000000000000000000000001")
                        .expect("commit id"),
                    committed_seq: ChangeSeq(12),
                }),
            ),
            (
                MetadataRowFamily::Attributes,
                MetadataRow::AttributesRevision(AttributesRevisionRecord {
                    inode_id: InodeId(42),
                    attributes_revision_no: crate::AttributesRevisionNo(3),
                    committed_seq: ChangeSeq(12),
                    commit_id: row_commit_id(),
                    delta_index: 0,
                    committed_by: crate::ActorId::loonfs(),
                    committed_at_ms: 12_000,
                    attributes: crate::Attributes::default(),
                }),
            ),
        ];

        for (family, row) in rows {
            let row_key = row.row_key_for_family(family);
            let prefix = family.row_key_prefix();
            assert!(
                !prefix.is_empty(),
                "`{family:?}` declares no row-key prefix"
            );
            assert!(
                row_key.starts_with(prefix),
                "row key `{row_key}` for `{family:?}` does not start with `{prefix}`"
            );
        }
    }

    #[test]
    fn attribution_values_never_change_row_or_index_keys() {
        fn rows(actor: crate::ActorId) -> Vec<(MetadataRowFamily, MetadataRow)> {
            vec![
                (
                    MetadataRowFamily::Inodes,
                    MetadataRow::Inode(InodeRecord {
                        inode_id: InodeId(42),
                        inode_kind: crate::InodeKind::File,
                        committed_seq: ChangeSeq(3),
                        commit_id: row_commit_id(),
                        committed_by: actor.clone(),
                        committed_at_ms: 3_000,
                    }),
                ),
                (
                    MetadataRowFamily::DirentryRevisions,
                    direntry_revision_row(actor.clone()),
                ),
                (
                    MetadataRowFamily::Revisions,
                    MetadataRow::FileRevision(RevisionRecord {
                        inode_id: InodeId(42),
                        revision_no: crate::RevisionNo(7),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        committed_at_ms: 12_000,
                        committed_by: actor.clone(),
                        delta_index: 3,
                        content_ref: crate::ContentRef::blob_v1(
                            crate::NamespaceId::parse("demo").expect("namespace id"),
                            crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff")
                                .expect("content id"),
                            b"attribution key test",
                            crate::ChecksumAlgorithm::Crc64nvme,
                        ),
                    }),
                ),
                (
                    MetadataRowFamily::Tombstones,
                    MetadataRow::Tombstone(SubtreeTombstoneRecord {
                        root_inode_id: InodeId(42),
                        committed_seq: ChangeSeq(12),
                        delta_index: 3,
                        commit_id: row_commit_id(),
                        action: TombstoneRowAction::Set {
                            deleted_binding: deleted_binding(),
                        },
                        committed_at_ms: 12_000,
                        committed_by: actor.clone(),
                    }),
                ),
                (
                    MetadataRowFamily::ActiveDeletions,
                    MetadataRow::ActiveDeletion(ActiveDeletionRecord {
                        root_inode_id: InodeId(42),
                        deletion_seq: ChangeSeq(12),
                        action: ActiveDeletionRowAction::Listed {
                            inode_kind: crate::InodeKind::File,
                            deleted_at_ms: 12_000,
                            deleted_by: actor.clone(),
                            deleted_binding: deleted_binding(),
                        },
                    }),
                ),
                (
                    MetadataRowFamily::Attributes,
                    MetadataRow::AttributesRevision(AttributesRevisionRecord {
                        inode_id: InodeId(42),
                        attributes_revision_no: crate::AttributesRevisionNo(2),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        delta_index: 3,
                        committed_by: actor,
                        committed_at_ms: 12_000,
                        attributes: crate::Attributes::default(),
                    }),
                ),
            ]
        }

        let actors = [
            crate::ActorId::parse("auth0|x").expect("actor id"),
            crate::ActorId::parse("x".repeat(256)).expect("256-byte actor id"),
            crate::ActorId::parse("external|actor").expect("external actor id"),
        ];
        let baseline = rows(actors[0].clone());
        for actor in actors.into_iter().skip(1) {
            let changed = rows(actor);
            for ((family, baseline), (changed_family, changed)) in baseline.iter().zip(&changed) {
                assert_eq!(family, changed_family);
                assert_eq!(
                    baseline.row_key_for_family(*family),
                    changed.row_key_for_family(*family)
                );
                assert_eq!(
                    baseline.filter_key_for_family(*family),
                    changed.filter_key_for_family(*family)
                );
            }
        }
    }
}
