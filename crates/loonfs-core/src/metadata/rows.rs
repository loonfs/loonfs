//! Metadata row records plus the append and accounting plumbing that keeps
//! [`MetadataState`]'s derived indexes and decoded-size totals in step with
//! its append-only rows.

use super::indexes::MetadataIndexes;
use loonfs_api::wire::manifest::lookup_keys;
use loonfs_api::{
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::mem::{size_of, size_of_val};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataState {
    #[serde(default)]
    pub(super) inodes: Vec<InodeRecord>,
    #[serde(default)]
    pub(super) direntry_binds: Vec<DirentryBindRecord>,
    #[serde(default)]
    pub(super) direntry_unbinds: Vec<DirentryUnbindRecord>,
    #[serde(default)]
    pub(super) revisions: Vec<RevisionRecord>,
    #[serde(default)]
    pub(super) subtree_tombstones: Vec<SubtreeTombstoneRecord>,
    #[serde(default)]
    pub(super) commit_receipts: Vec<CommitReceiptRecord>,
    #[serde(skip)]
    pub(super) row_count: usize,
    #[serde(skip)]
    pub(super) decoded_bytes: usize,
    #[serde(skip)]
    pub(super) indexes: MetadataIndexes,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
struct MetadataStateRows {
    #[serde(default)]
    inodes: Vec<InodeRecord>,
    #[serde(default)]
    direntry_binds: Vec<DirentryBindRecord>,
    #[serde(default)]
    direntry_unbinds: Vec<DirentryUnbindRecord>,
    #[serde(default)]
    revisions: Vec<RevisionRecord>,
    #[serde(default)]
    subtree_tombstones: Vec<SubtreeTombstoneRecord>,
    #[serde(default)]
    commit_receipts: Vec<CommitReceiptRecord>,
}

impl Default for MetadataState {
    fn default() -> Self {
        Self::from_rows(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
}

impl<'de> Deserialize<'de> for MetadataState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rows = MetadataStateRows::deserialize(deserializer)?;
        Ok(Self::from_rows(
            rows.inodes,
            rows.direntry_binds,
            rows.direntry_unbinds,
            rows.revisions,
            rows.subtree_tombstones,
            rows.commit_receipts,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryBindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryUnbindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: NameKey,
    /// User-facing spelling the retired binding carried: while the unbind
    /// row is retained, it is the durable home of a deleted name.
    pub display_name: DisplayName,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
    pub unbind_seq: ChangeSeq,
    pub unbind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    /// Observational wall-clock stamp of the owning commit; never a
    /// validity input — `committed_seq` is the order.
    pub committed_at_ms: u64,
    pub revision_delta_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    /// The recording event's sequence: the delete's committed seq for a
    /// `Set`, the undelete's committed seq for a `Revoke`.
    pub tombstone_seq: ChangeSeq,
    pub tombstone_delta_index: u32,
    /// Wall-clock stamp of the recording commit.
    pub deleted_at_ms: u64,
    /// Deleted-binding identity for `Set` rows from path deletes; the
    /// tombstone row is immortal, so the deleted name lives here after
    /// unbind rows age out.
    pub parent_inode_id: Option<InodeId>,
    pub name_key: Option<NameKey>,
    pub display_name: Option<DisplayName>,
    /// What this event did. Newest-by-(seq, delta) wins at every read
    /// site: a `Set` newest means that deletion is active, a `Revoke`
    /// newest means none is, and a later re-delete supersedes the revoke.
    pub action: SubtreeTombstoneAction,
}

/// The tombstone family's event vocabulary — an explicit state machine
/// instead of a boolean, so every reader matches exhaustively and a revoke
/// names the exact deletion generation it cancels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtreeTombstoneAction {
    /// The subtree rooted here is deleted.
    Set,
    /// The deletion recorded at `(target_seq, target_delta_index)` is
    /// revoked. Validation guarantees the target was the active generation
    /// when the revoke committed.
    Revoke {
        target_seq: ChangeSeq,
        target_delta_index: u32,
    },
}

/// The newest-event-wins active-tombstone rule, shared by every aggregation
/// site: among records at or below `visible_seq`, the newest by
/// `(tombstone_seq, tombstone_delta_index)` speaks for the root — a `Set`
/// newest means that deletion is active, a `Revoke` newest means none is.
/// The newest event is authoritative WITHOUT consulting the revoke's
/// target: commit validation guarantees a revoke only ever lands against
/// the generation that was active, so for valid histories the two rules
/// agree, and the recorded target serves as audit metadata and the
/// projection contract (change-feed consumers reduce with it and can flag
/// a mismatch as corruption). Keep every reader on this helper — a site
/// with its own copy of the rule is how visibility splits from the durable
/// truth.
pub(crate) fn active_tombstone_from_records(
    records: impl IntoIterator<Item = SubtreeTombstoneRecord>,
    visible_seq: ChangeSeq,
) -> Option<SubtreeTombstoneRecord> {
    records
        .into_iter()
        .filter(|tombstone| tombstone.tombstone_seq <= visible_seq)
        .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index))
        .filter(|tombstone| matches!(tombstone.action, SubtreeTombstoneAction::Set))
}

/// One row of the derived `ActiveDeletions` family: the current-state view of
/// a single deletion generation.
///
/// Nothing appends these — [`active_deletion_from_tombstone`] derives one from
/// every tombstone event, so the family is a projection of the tombstone
/// family and never a second source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveDeletionRecord {
    pub(crate) root_inode_id: InodeId,
    /// The deletion's committed sequence. Together with `root_inode_id` this
    /// is the handle `undelete` addresses and the trash entry renders.
    pub(crate) deleted_at_seq: ChangeSeq,
    pub(crate) action: ActiveDeletionAction,
}

/// What one active-deletion row says about its generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActiveDeletionAction {
    /// The deletion is recoverable, and the trash lists it with these
    /// details.
    Listed {
        deleted_at_ms: u64,
        parent_inode_id: Option<InodeId>,
        name_key: Option<NameKey>,
        display_name: Option<DisplayName>,
    },
    /// An undelete cancelled the deletion, so the listing skips the key.
    Removed { revoked_at_seq: ChangeSeq },
}

/// The `ActiveDeletions` reducer, and the only mapping from a tombstone event
/// to its derived row: a `set` adds the deletion to the listing, and a
/// `revoke` removes the exact generation it names.
///
/// It reduces target-aware where the newest-event-wins rule in
/// [`active_tombstone_from_records`] reduces target-blind. Commit validation
/// only ever lands a revoke against the generation that was active, so the two
/// agree on every history a writer can produce; the target is what lets a
/// removal be derived one event at a time instead of by re-reading a root's
/// whole history.
pub(crate) fn active_deletion_from_tombstone(
    tombstone: &SubtreeTombstoneRecord,
) -> ActiveDeletionRecord {
    match &tombstone.action {
        SubtreeTombstoneAction::Set => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deleted_at_seq: tombstone.tombstone_seq,
            action: ActiveDeletionAction::Listed {
                deleted_at_ms: tombstone.deleted_at_ms,
                parent_inode_id: tombstone.parent_inode_id,
                name_key: tombstone.name_key.clone(),
                display_name: tombstone.display_name.clone(),
            },
        },
        SubtreeTombstoneAction::Revoke { target_seq, .. } => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deleted_at_seq: *target_seq,
            action: ActiveDeletionAction::Removed {
                revoked_at_seq: tombstone.tombstone_seq,
            },
        },
    }
}

impl ActiveDeletionRecord {
    /// This row's durable key, which is also its position in the trash
    /// listing's order.
    pub(crate) fn row_key(&self) -> String {
        lookup_keys::active_deletion_row_key(
            self.deleted_at_seq,
            self.root_inode_id,
            match &self.action {
                ActiveDeletionAction::Listed { .. } => lookup_keys::ACTIVE_DELETION_RANK_LISTED,
                ActiveDeletionAction::Removed { .. } => lookup_keys::ACTIVE_DELETION_RANK_REMOVED,
            },
        )
    }

    /// The deletion this row lists, or `None` when the row is an undelete's
    /// removal marker.
    pub(crate) fn into_recoverable(self) -> Option<RecoverableDeletion> {
        match self.action {
            ActiveDeletionAction::Listed {
                deleted_at_ms,
                parent_inode_id,
                name_key,
                display_name,
            } => Some(RecoverableDeletion {
                root_inode_id: self.root_inode_id,
                deleted_at_seq: self.deleted_at_seq,
                deleted_at_ms,
                parent_inode_id,
                name_key,
                display_name,
            }),
            ActiveDeletionAction::Removed { .. } => None,
        }
    }
}

/// One recoverable deletion as the trash listing renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoverableDeletion {
    pub(crate) root_inode_id: InodeId,
    pub(crate) deleted_at_seq: ChangeSeq,
    pub(crate) deleted_at_ms: u64,
    pub(crate) parent_inode_id: Option<InodeId>,
    pub(crate) name_key: Option<NameKey>,
    pub(crate) display_name: Option<DisplayName>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceiptRecord {
    pub commit_id: CommitId,
    pub semantic_commit_fingerprint: String,
    pub committed_seq: ChangeSeq,
    /// Observational wall-clock stamp of the commit; never a validity
    /// input — `committed_seq` is the order.
    pub committed_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl MetadataState {
    pub(crate) fn from_rows(
        inodes: Vec<InodeRecord>,
        direntry_binds: Vec<DirentryBindRecord>,
        direntry_unbinds: Vec<DirentryUnbindRecord>,
        revisions: Vec<RevisionRecord>,
        subtree_tombstones: Vec<SubtreeTombstoneRecord>,
        commit_receipts: Vec<CommitReceiptRecord>,
    ) -> Self {
        let mut state = Self {
            inodes,
            direntry_binds,
            direntry_unbinds,
            revisions,
            subtree_tombstones,
            commit_receipts,
            row_count: 0,
            decoded_bytes: 0,
            indexes: MetadataIndexes::default(),
        };
        state.rebuild_indexes();
        state
    }

    /// Highest sequence carried by any indexed record.
    ///
    /// No record carries a larger seq, so a query at `base_seq >=
    /// indexed_seq()` passes every `seq <= base_seq` filter and is equivalent
    /// to an at-head query. The seq-gated read methods below rely on this to
    /// route to the indexes; only queries strictly below `indexed_seq()` need
    /// the historical scans.
    pub fn indexed_seq(&self) -> ChangeSeq {
        self.indexes.indexed_seq()
    }

    pub fn inodes(&self) -> &[InodeRecord] {
        &self.inodes
    }

    pub fn direntry_binds(&self) -> &[DirentryBindRecord] {
        &self.direntry_binds
    }

    pub fn direntry_unbinds(&self) -> &[DirentryUnbindRecord] {
        &self.direntry_unbinds
    }

    pub fn revisions(&self) -> &[RevisionRecord] {
        &self.revisions
    }

    pub fn subtree_tombstones(&self) -> &[SubtreeTombstoneRecord] {
        &self.subtree_tombstones
    }

    pub fn commit_receipts(&self) -> &[CommitReceiptRecord] {
        &self.commit_receipts
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn decoded_bytes(&self) -> usize {
        self.decoded_bytes
    }

    pub fn find_commit_receipt(&self, commit_id: &CommitId) -> Option<&CommitReceiptRecord> {
        self.indexes.commit_receipt(commit_id)
    }

    fn rebuild_indexes(&mut self) {
        self.row_count = metadata_row_count(
            &self.inodes,
            &self.direntry_binds,
            &self.direntry_unbinds,
            &self.revisions,
            &self.subtree_tombstones,
            &self.commit_receipts,
        );
        self.decoded_bytes = metadata_decoded_bytes(
            &self.inodes,
            &self.direntry_binds,
            &self.direntry_unbinds,
            &self.revisions,
            &self.subtree_tombstones,
            &self.commit_receipts,
        );
        self.indexes = MetadataIndexes::rebuild(
            &self.inodes,
            &self.direntry_binds,
            &self.direntry_unbinds,
            &self.revisions,
            &self.subtree_tombstones,
            &self.commit_receipts,
        );
    }

    pub(crate) fn push_inode_record(&mut self, record: InodeRecord) {
        self.indexes.record_inode(&record);
        self.record_row_weight(size_of::<InodeRecord>());
        self.inodes.push(record);
    }

    pub(crate) fn push_direntry_bind_record(&mut self, record: DirentryBindRecord) {
        self.indexes.record_bind(&record);
        self.record_row_weight(direntry_bind_decoded_bytes(&record));
        self.direntry_binds.push(record);
    }

    pub(crate) fn push_direntry_unbind_record(&mut self, record: DirentryUnbindRecord) {
        self.indexes.record_unbind(&record);
        self.record_row_weight(direntry_unbind_decoded_bytes(&record));
        self.direntry_unbinds.push(record);
    }

    pub(crate) fn push_revision_record(&mut self, record: RevisionRecord) {
        self.indexes.record_revision(&record);
        self.record_row_weight(revision_decoded_bytes(&record));
        self.revisions.push(record);
    }

    pub(crate) fn push_subtree_tombstone_record(&mut self, record: SubtreeTombstoneRecord) {
        self.indexes.record_tombstone(&record);
        self.record_row_weight(size_of::<SubtreeTombstoneRecord>());
        self.subtree_tombstones.push(record);
    }

    pub(crate) fn push_commit_receipt_record(&mut self, record: CommitReceiptRecord) {
        self.indexes.record_commit_receipt(&record);
        self.record_row_weight(commit_receipt_decoded_bytes(&record));
        self.commit_receipts.push(record);
    }

    fn record_row_weight(&mut self, decoded_bytes: usize) {
        self.row_count = self.row_count.saturating_add(1);
        self.decoded_bytes = self.decoded_bytes.saturating_add(decoded_bytes);
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct MetadataStateBuilder {
    state: MetadataState,
}

#[cfg(test)]
impl MetadataStateBuilder {
    pub(crate) fn push_inode(&mut self, record: InodeRecord) {
        self.state.push_inode_record(record);
    }

    pub(crate) fn push_direntry_bind(&mut self, record: DirentryBindRecord) {
        self.state.push_direntry_bind_record(record);
    }

    pub(crate) fn push_direntry_unbind(&mut self, record: DirentryUnbindRecord) {
        self.state.push_direntry_unbind_record(record);
    }

    pub(crate) fn push_revision(&mut self, record: RevisionRecord) {
        self.state.push_revision_record(record);
    }

    pub(crate) fn push_subtree_tombstone(&mut self, record: SubtreeTombstoneRecord) {
        self.state.push_subtree_tombstone_record(record);
    }

    pub(crate) fn push_commit_receipt(&mut self, record: CommitReceiptRecord) {
        self.state.push_commit_receipt_record(record);
    }

    pub(crate) fn finish(mut self) -> MetadataState {
        self.state.rebuild_indexes();
        self.state
    }
}

fn metadata_row_count(
    inodes: &[InodeRecord],
    direntry_binds: &[DirentryBindRecord],
    direntry_unbinds: &[DirentryUnbindRecord],
    revisions: &[RevisionRecord],
    subtree_tombstones: &[SubtreeTombstoneRecord],
    commit_receipts: &[CommitReceiptRecord],
) -> usize {
    inodes
        .len()
        .saturating_add(direntry_binds.len())
        .saturating_add(direntry_unbinds.len())
        .saturating_add(revisions.len())
        .saturating_add(subtree_tombstones.len())
        .saturating_add(commit_receipts.len())
}

fn metadata_decoded_bytes(
    inodes: &[InodeRecord],
    direntry_binds: &[DirentryBindRecord],
    direntry_unbinds: &[DirentryUnbindRecord],
    revisions: &[RevisionRecord],
    subtree_tombstones: &[SubtreeTombstoneRecord],
    commit_receipts: &[CommitReceiptRecord],
) -> usize {
    size_of_val(inodes)
        .saturating_add(
            direntry_binds
                .iter()
                .map(direntry_bind_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(
            direntry_unbinds
                .iter()
                .map(direntry_unbind_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(revisions.iter().map(revision_decoded_bytes).sum::<usize>())
        .saturating_add(size_of_val(subtree_tombstones))
        .saturating_add(
            commit_receipts
                .iter()
                .map(commit_receipt_decoded_bytes)
                .sum::<usize>(),
        )
}

fn direntry_bind_decoded_bytes(record: &DirentryBindRecord) -> usize {
    size_of::<DirentryBindRecord>()
        + record.name_key.as_str().len()
        + record.display_name.as_str().len()
}

fn direntry_unbind_decoded_bytes(record: &DirentryUnbindRecord) -> usize {
    size_of::<DirentryUnbindRecord>() + record.name_key.as_str().len()
}

fn revision_decoded_bytes(record: &RevisionRecord) -> usize {
    size_of::<RevisionRecord>() + content_ref_decoded_bytes(&record.content_ref)
}

fn commit_receipt_decoded_bytes(record: &CommitReceiptRecord) -> usize {
    size_of::<CommitReceiptRecord>()
        + record.commit_id.as_str().len()
        + record.semantic_commit_fingerprint.len()
        + record.message.as_ref().map_or(0, String::len)
}

fn content_ref_decoded_bytes(content_ref: &ContentRef) -> usize {
    size_of::<ContentRef>() + content_ref_evidence_bytes(content_ref)
}

/// Heap bytes a content reference owns beyond its struct: the identity and
/// the checksum strings.
pub(crate) fn content_ref_evidence_bytes(content_ref: &ContentRef) -> usize {
    content_ref.content_id.as_str().len()
        + content_ref.storage_checksum.value.len()
        + content_ref
            .whole_file_sha256
            .as_ref()
            .map_or(0, String::len)
}
