//! Metadata row records plus the append and accounting plumbing that keeps
//! [`MetadataState`]'s derived indexes and decoded-size totals in step with
//! its append-only rows.

use super::indexes::MetadataIndexes;
use loonfs_api::wire::manifest::{lookup_keys, DeletedDirentry, TombstoneGeneration};
use loonfs_api::{
    ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName,
    InodeId, InodeKind, NameKey, RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::mem::{size_of, size_of_val};

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
    /// This event's own generation: the delete's committed position for a
    /// `Set`, the undelete's for a `Revoke`.
    pub generation: TombstoneGeneration,
    /// Wall-clock stamp of the recording commit.
    pub deleted_at_ms: u64,
    /// What this event did. Newest generation wins at every read site: a
    /// `Set` newest means that deletion is active, a `Revoke` newest means
    /// none is, and a later re-delete supersedes the revoke.
    pub action: SubtreeTombstoneAction,
}

/// The tombstone family's event vocabulary — an explicit state machine
/// instead of a boolean, so every reader matches exhaustively and a revoke
/// names the exact deletion generation it cancels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtreeTombstoneAction {
    /// The subtree rooted here is deleted, removing the binding described
    /// here when the delete came by path. The tombstone row is immortal, so
    /// this is where a deleted name lives after unbind rows age out.
    Set {
        deleted_direntry: Option<DeletedDirentry>,
    },
    /// The deletion recorded at `target` is revoked. Validation guarantees
    /// the target was the active generation when the revoke committed.
    Revoke { target: TombstoneGeneration },
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
        .filter(|tombstone| matches!(tombstone.action, SubtreeTombstoneAction::Set { .. }))
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
        deleted_direntry: Option<DeletedDirentry>,
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
        SubtreeTombstoneAction::Set { deleted_direntry } => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deleted_at_seq: tombstone.generation.seq,
            action: ActiveDeletionAction::Listed {
                deleted_at_ms: tombstone.deleted_at_ms,
                deleted_direntry: deleted_direntry.clone(),
            },
        },
        SubtreeTombstoneAction::Revoke { target } => ActiveDeletionRecord {
            root_inode_id: tombstone.root_inode_id,
            deleted_at_seq: target.seq,
            action: ActiveDeletionAction::Removed {
                revoked_at_seq: tombstone.generation.seq,
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
                deleted_direntry,
            } => Some(RecoverableDeletion {
                root_inode_id: self.root_inode_id,
                deleted_at_seq: self.deleted_at_seq,
                deleted_at_ms,
                deleted_direntry,
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
    /// The binding the delete removed, and so the one undelete restores in
    /// place; `None` when the deletion recorded no binding.
    pub(crate) deleted_direntry: Option<DeletedDirentry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceiptRecord {
    pub commit_id: CommitId,
    pub actor: ActorRef,
    pub semantic_commit_fingerprint: String,
    pub committed_seq: ChangeSeq,
    /// Observational wall-clock stamp of the commit; never a validity
    /// input — `committed_seq` is the order.
    pub committed_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// One inode's complete attribute map at one revision.
///
/// The record is whole state, so a reader takes the newest one for an inode
/// and needs nothing older. An inode with no record is at revision 0 with an
/// empty map, which is why nothing is written until a caller writes an
/// attribute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributesRevisionRecord {
    pub inode_id: InodeId,
    pub attributes_revision_no: AttributeRevisionNo,
    pub committed_seq: ChangeSeq,
    pub delta_index: u32,
    /// The inode's attributes after this update. An empty map is the cleared
    /// state, not an absent record.
    pub attributes: Attributes,
}

impl MetadataState {
    #[allow(clippy::too_many_arguments)]
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

    pub(crate) fn push_attributes_revision_record(&mut self, record: AttributesRevisionRecord) {
        self.indexes.record_attributes_revision(&record);
        self.record_row_weight(attributes_revision_decoded_bytes(&record));
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
    size_of_val(state.inodes.as_slice())
        .saturating_add(
            state
                .direntry_binds
                .iter()
                .map(direntry_bind_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(
            state
                .direntry_unbinds
                .iter()
                .map(direntry_unbind_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(
            state
                .revisions
                .iter()
                .map(revision_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(size_of_val(state.subtree_tombstones.as_slice()))
        .saturating_add(
            state
                .commit_receipts
                .iter()
                .map(commit_receipt_decoded_bytes)
                .sum::<usize>(),
        )
        .saturating_add(
            state
                .attributes_revisions
                .iter()
                .map(attributes_revision_decoded_bytes)
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
        + record.actor.kind.as_str().len()
        + record.actor.id.as_str().len()
        + record.semantic_commit_fingerprint.len()
        + record.message.as_ref().map_or(0, String::len)
}

/// The record's struct plus the key and value bytes its map owns. Attribute
/// maps are caller-sized, so the map's own bytes are what this row weighs.
fn attributes_revision_decoded_bytes(record: &AttributesRevisionRecord) -> usize {
    size_of::<AttributesRevisionRecord>() + record.attributes.logical_bytes()
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
