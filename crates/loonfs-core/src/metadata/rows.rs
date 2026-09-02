//! Metadata state plus the append and accounting logic that keeps its
//! indexes and decoded-size totals in step with its rows.

use super::indexes::MetadataIndexes;
use crate::checkpoint::DecodedRowWeight;
use loonfs_api::wire::manifest::{
    ActiveDeletionRecord, ActiveDeletionRowAction, AttributesRevisionRecord, CommitReceiptRecord,
    DeletedDirentry, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, RevisionRecord,
    SubtreeTombstoneRecord, TombstoneRowAction,
};
use loonfs_api::{ActorRef, ChangeSeq, CommitId, InodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataState {
    pub(super) inodes: Vec<InodeRecord>,
    pub(super) direntry_binds: Vec<DirentryBindRecord>,
    pub(super) direntry_unbinds: Vec<DirentryUnbindRecord>,
    pub(super) revisions: Vec<RevisionRecord>,
    pub(super) subtree_tombstones: Vec<SubtreeTombstoneRecord>,
    pub(super) commit_receipts: Vec<CommitReceiptRecord>,
    pub(super) attributes_revisions: Vec<AttributesRevisionRecord>,
    pub(super) row_count: usize,
    pub(super) decoded_bytes: usize,
    pub(super) indexes: MetadataIndexes,
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
            Vec::new(),
        )
    }
}

/// The newest-event-wins active-tombstone rule, shared by every aggregation
/// site: among records at or below `visible_seq`, the newest generation
/// speaks for the root — a `Set` newest means that deletion is active, a
/// `Revoke` newest means none is.
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
        .filter(|tombstone| tombstone.generation.seq <= visible_seq)
        .max_by_key(|tombstone| tombstone.generation)
        .filter(|tombstone| matches!(tombstone.action, TombstoneRowAction::Set { .. }))
}

/// Converts a tombstone event into its derived `ActiveDeletions` row. A `set`
/// adds a deletion to the listing, and a `revoke` removes that generation.
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
        TombstoneRowAction::Set { deleted_direntry } => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deletion_seq: tombstone.generation.seq,
            action: ActiveDeletionRowAction::Listed {
                deleted_at_ms: tombstone.deleted_at_ms,
                deleted_by: tombstone.deleted_by.clone(),
                deleted_direntry: deleted_direntry.clone(),
            },
        },
        TombstoneRowAction::Revoke { target } => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deletion_seq: target.seq,
            action: ActiveDeletionRowAction::Removed {
                revocation_seq: tombstone.generation.seq,
            },
        },
    }
}

pub(crate) fn recoverable_deletion_from_active_record(
    record: ActiveDeletionRecord,
) -> Option<RecoverableDeletion> {
    match record.action {
        ActiveDeletionRowAction::Listed {
            deleted_at_ms,
            deleted_by,
            deleted_direntry,
        } => Some(RecoverableDeletion {
            root_inode_id: record.root_inode_id,
            deletion_seq: record.deletion_seq,
            deleted_at_ms,
            deleted_by,
            deleted_direntry,
        }),
        ActiveDeletionRowAction::Removed { .. } => None,
    }
}

/// One recoverable deletion as the trash listing renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoverableDeletion {
    pub(crate) root_inode_id: InodeId,
    pub(crate) deletion_seq: ChangeSeq,
    pub(crate) deleted_at_ms: u64,
    pub(crate) deleted_by: ActorRef,
    /// The binding the delete removed and an in-place undelete restores.
    pub(crate) deleted_direntry: DeletedDirentry,
}

impl MetadataState {
    #[allow(
        clippy::too_many_arguments,
        reason = "the constructor names every durable metadata row family explicitly"
    )]
    pub(crate) fn from_rows(
        inodes: Vec<InodeRecord>,
        direntry_binds: Vec<DirentryBindRecord>,
        direntry_unbinds: Vec<DirentryUnbindRecord>,
        revisions: Vec<RevisionRecord>,
        subtree_tombstones: Vec<SubtreeTombstoneRecord>,
        commit_receipts: Vec<CommitReceiptRecord>,
        attributes_revisions: Vec<AttributesRevisionRecord>,
    ) -> Self {
        let mut state = Self {
            inodes,
            direntry_binds,
            direntry_unbinds,
            revisions,
            subtree_tombstones,
            commit_receipts,
            attributes_revisions,
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

    pub fn attributes_revisions(&self) -> &[AttributesRevisionRecord] {
        &self.attributes_revisions
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
        self.row_count = metadata_row_count(self);
        self.decoded_bytes = metadata_decoded_bytes(self);
        self.indexes = MetadataIndexes::rebuild(self);
    }

    pub(crate) fn push_inode_record(&mut self, record: InodeRecord) {
        self.indexes.record_inode(&record);
        self.record_row_weight(record.decoded_weight());
        self.inodes.push(record);
    }

    pub(crate) fn push_direntry_bind_record(&mut self, record: DirentryBindRecord) {
        self.indexes.record_bind(&record);
        self.record_row_weight(record.decoded_weight());
        self.direntry_binds.push(record);
    }

    pub(crate) fn push_direntry_unbind_record(&mut self, record: DirentryUnbindRecord) {
        self.indexes.record_unbind(&record);
        self.record_row_weight(record.decoded_weight());
        self.direntry_unbinds.push(record);
    }

    pub(crate) fn push_revision_record(&mut self, record: RevisionRecord) {
        self.indexes.record_revision(&record);
        self.record_row_weight(record.decoded_weight());
        self.revisions.push(record);
    }

    pub(crate) fn push_subtree_tombstone_record(&mut self, record: SubtreeTombstoneRecord) {
        self.indexes.record_tombstone(&record);
        self.record_row_weight(record.decoded_weight());
        self.subtree_tombstones.push(record);
    }

    pub(crate) fn push_commit_receipt_record(&mut self, record: CommitReceiptRecord) {
        self.indexes.record_commit_receipt(&record);
        self.record_row_weight(record.decoded_weight());
        self.commit_receipts.push(record);
    }

    pub(crate) fn push_attributes_revision_record(&mut self, record: AttributesRevisionRecord) {
        self.indexes.record_attributes_revision(&record);
        self.record_row_weight(record.decoded_weight());
        self.attributes_revisions.push(record);
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

    pub(crate) fn push_attributes_revision(&mut self, record: AttributesRevisionRecord) {
        self.state.push_attributes_revision_record(record);
    }

    pub(crate) fn finish(mut self) -> MetadataState {
        self.state.rebuild_indexes();
        self.state
    }
}

fn metadata_row_count(state: &MetadataState) -> usize {
    state
        .inodes
        .len()
        .saturating_add(state.direntry_binds.len())
        .saturating_add(state.direntry_unbinds.len())
        .saturating_add(state.revisions.len())
        .saturating_add(state.subtree_tombstones.len())
        .saturating_add(state.commit_receipts.len())
        .saturating_add(state.attributes_revisions.len())
}

fn metadata_decoded_bytes(state: &MetadataState) -> usize {
    fn total<R: DecodedRowWeight>(records: &[R]) -> usize {
        records
            .iter()
            .map(DecodedRowWeight::decoded_weight)
            .fold(0, usize::saturating_add)
    }
    total(&state.inodes)
        .saturating_add(total(&state.direntry_binds))
        .saturating_add(total(&state.direntry_unbinds))
        .saturating_add(total(&state.revisions))
        .saturating_add(total(&state.subtree_tombstones))
        .saturating_add(total(&state.commit_receipts))
        .saturating_add(total(&state.attributes_revisions))
}
