mod indexes;

use crate::invariants::InvariantId;
use indexes::MetadataIndexes;
use loonfs_api::wire::wal::{WalCommitPayload, WalDelta};
use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, ContentRef, InodeId, InodeKind, NameKey, NamePolicy,
    RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::mem::{size_of, size_of_val};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataState {
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
    #[serde(skip)]
    row_count: usize,
    #[serde(skip)]
    decoded_bytes: usize,
    #[serde(skip)]
    indexes: MetadataIndexes,
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
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
    pub bind_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirentryUnbindRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
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
    pub revision_delta_index: u32,
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
    pub tombstone_delta_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceiptRecord {
    pub commit_id: CommitId,
    pub semantic_commit_fingerprint: String,
    pub committed_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<InvariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedVisiblePath {
    pub absolute_path: String,
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum VisiblePathError {
    #[error("invalid absolute path `{absolute_path}`")]
    InvalidAbsolutePath { absolute_path: String },
    #[error("canonical root inode is missing")]
    RootMissing,
    #[error("visible path not found: `{absolute_path}`")]
    PathNotFound { absolute_path: String },
    #[error(
        "path component traversal expected directory at `{absolute_path}` but found inode `{inode_id:?}` kind `{inode_kind:?}`"
    )]
    PathComponentNotDirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataApplyError {
    RevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
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

    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        deltas: &[WalDelta],
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut metadata_state = self.clone();
        let checked_invariants =
            metadata_state.apply_committed_wal_deltas_mut(committed_seq, deltas)?;

        Ok(AppliedMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn apply_committed_wal_deltas_mut(
        &mut self,
        committed_seq: ChangeSeq,
        deltas: &[WalDelta],
    ) -> Result<Vec<InvariantId>, MetadataApplyError> {
        let mut checked_invariants = Vec::new();

        for delta in deltas {
            match delta {
                WalDelta::CreateInode {
                    delta_index: _,
                    inode_id,
                    inode_kind,
                } => {
                    self.push_inode_record(InodeRecord {
                        inode_id: *inode_id,
                        inode_kind: inode_kind.clone(),
                        created_seq: committed_seq,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        InvariantId::CreateInodeWritesInodeRow,
                    );
                }
                WalDelta::BindDirentry {
                    delta_index,
                    parent_inode,
                    name_key,
                    display_name,
                    child_inode,
                } => {
                    self.push_direntry_bind_record(DirentryBindRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *child_inode,
                        bind_seq: committed_seq,
                        bind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        InvariantId::BindDirentryWritesDirentryBindRow,
                    );
                }
                WalDelta::UnbindDirentry {
                    delta_index,
                    parent_inode,
                    name_key,
                    child_inode,
                    bind_seq,
                    bind_delta_index,
                } => {
                    self.push_direntry_unbind_record(DirentryUnbindRecord {
                        parent_inode_id: *parent_inode,
                        name_key: name_key.clone(),
                        child_inode_id: *child_inode,
                        bind_seq: *bind_seq,
                        bind_delta_index: *bind_delta_index,
                        unbind_seq: committed_seq,
                        unbind_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        InvariantId::UnbindDirentryWritesUnbindRow,
                    );
                }
                WalDelta::AppendFileRevision {
                    delta_index,
                    inode_id,
                    revision_no,
                    content_ref,
                } => {
                    self.push_revision_record(RevisionRecord {
                        inode_id: *inode_id,
                        revision_no: *revision_no,
                        committed_seq,
                        revision_delta_index: *delta_index,
                        content_ref: content_ref.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        InvariantId::AppendFileRevisionWritesRevisionRow,
                    );
                }
                WalDelta::TombstoneSubtree {
                    delta_index,
                    root_inode,
                } => {
                    self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                        root_inode_id: *root_inode,
                        tombstone_seq: committed_seq,
                        tombstone_delta_index: *delta_index,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        InvariantId::TombstoneSubtreeWritesTombstoneRow,
                    );
                }
            }
        }

        Ok(checked_invariants)
    }

    pub fn apply_committed_wal_record(
        &self,
        record: &WalCommitPayload,
    ) -> Result<AppliedMetadataState, MetadataApplyError> {
        let mut metadata_state = self.clone();
        let checked_invariants = metadata_state.apply_committed_wal_record_mut(record)?;

        Ok(AppliedMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn apply_committed_wal_record_mut(
        &mut self,
        record: &WalCommitPayload,
    ) -> Result<Vec<InvariantId>, MetadataApplyError> {
        let deltas = record
            .deltas
            .iter()
            .map(|delta| delta.delta.clone())
            .collect::<Vec<_>>();
        let mut checked_invariants = self.apply_committed_wal_deltas_mut(record.seq, &deltas)?;
        self.push_commit_receipt_record(CommitReceiptRecord {
            commit_id: record.commit_id.clone(),
            semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
            committed_seq: record.seq,
            message: record.message.clone(),
        });
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::WalReplayRecordsCommitReceipt,
        );
        Ok(checked_invariants)
    }

    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        if base_seq >= self.indexed_seq() {
            return self.inode_at_head(inode_id);
        }
        self.inode_at_seq_scan(inode_id, base_seq)
    }

    pub fn inode_at_head(&self, inode_id: InodeId) -> Option<InodeRecord> {
        self.indexes.inode(inode_id)
    }

    fn inode_at_seq_scan(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    pub fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.indexed_seq() {
            return self.latest_revision_at_head(inode_id);
        }
        self.latest_revision_head_at_seq_scan(inode_id, base_seq)
    }

    pub fn latest_revision_at_head(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.indexes.latest_revision(inode_id)
    }

    fn latest_revision_head_at_seq_scan(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| revision.inode_id == inode_id && revision.committed_seq <= base_seq)
            .max_by_key(|revision| {
                (
                    revision.revision_no,
                    revision.committed_seq,
                    revision.revision_delta_index,
                )
            })
            .cloned()
    }

    pub fn revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        if base_seq >= self.indexed_seq() {
            return self.revision_at_head(inode_id, revision_no);
        }
        self.revision_at_seq_scan(inode_id, revision_no, base_seq)
    }

    pub fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Option<RevisionRecord> {
        self.indexes.revision(inode_id, revision_no)
    }

    fn revision_at_seq_scan(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= base_seq
            })
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index))
            .cloned()
    }

    /// Latest bind for `(parent, name)` at or before `base_seq`, regardless
    /// of whether it has since been unbound.
    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.indexes.latest_bind(parent_inode_id, name_key);
        }
        self.bound_child_at_seq_scan(parent_inode_id, name_key, base_seq)
    }

    fn bound_child_at_seq_scan(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
    }

    pub fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.current_parent_binding_for_child_at_head(child_inode_id);
        }
        let direntry = self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        Some(direntry)
    }

    pub fn current_parent_binding_for_child_at_head(
        &self,
        child_inode_id: InodeId,
    ) -> Option<DirentryBindRecord> {
        self.indexes.active_parent_for_child(child_inode_id)
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        if base_seq >= self.indexed_seq() {
            return self.active_subtree_tombstone_at_head(root_inode_id);
        }
        self.active_subtree_tombstone_scan(root_inode_id, base_seq)
    }

    pub fn active_subtree_tombstone_at_head(
        &self,
        root_inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        self.indexes.active_tombstone(root_inode_id)
    }

    fn active_subtree_tombstone_scan(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        self.subtree_tombstones
            .iter()
            .filter(|tombstone| {
                tombstone.root_inode_id == root_inode_id && tombstone.tombstone_seq <= base_seq
            })
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index))
            .cloned()
    }

    pub fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<SubtreeTombstoneRecord> {
        if base_seq >= self.indexed_seq() {
            return self.covering_subtree_tombstone_at_head(inode_id);
        }

        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id, base_seq) {
                return Some(tombstone);
            }

            current = self
                .current_parent_binding_for_child(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    pub fn covering_subtree_tombstone_at_head(
        &self,
        inode_id: InodeId,
    ) -> Option<SubtreeTombstoneRecord> {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone_at_head(candidate_inode_id) {
                return Some(tombstone);
            }

            current = self
                .current_parent_binding_for_child_at_head(candidate_inode_id)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    pub fn visible_inode(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<InodeRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_inode_at_head(inode_id);
        }

        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
            return None;
        }

        Some(inode)
    }

    pub fn visible_inode_at_head(&self, inode_id: InodeId) -> Option<InodeRecord> {
        let inode = self.inode_at_head(inode_id)?;
        if self.covering_subtree_tombstone_at_head(inode_id).is_some() {
            return None;
        }

        Some(inode)
    }

    pub fn current_revision_head(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<RevisionRecord> {
        self.visible_inode(inode_id, base_seq)?;
        self.latest_revision_head_at_seq(inode_id, base_seq)
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_child_at_head(parent_inode_id, name_key);
        }

        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn visible_child_at_head(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Option<DirentryBindRecord> {
        let parent = self.visible_inode_at_head(parent_inode_id)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.indexes.active_child(parent_inode_id, name_key)?;
        self.visible_inode_at_head(direntry.child_inode_id)?;
        Some(direntry)
    }

    pub fn visible_children(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Vec<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.visible_children_at_head(parent_inode_id);
        }

        let Some(parent) = self.visible_inode(parent_inode_id, base_seq) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.bind_seq <= base_seq
            })
            .filter(|direntry| {
                self.active_child_binding_at_seq(parent_inode_id, &direntry.name_key, base_seq)
                    .map(|active| {
                        active.child_inode_id == direntry.child_inode_id
                            && active.bind_seq == direntry.bind_seq
                            && active.bind_delta_index == direntry.bind_delta_index
                    })
                    .unwrap_or(false)
            })
            .filter(|direntry| {
                self.visible_inode(direntry.child_inode_id, base_seq)
                    .is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    pub fn visible_children_page_by_name_key(
        &self,
        parent_inode_id: InodeId,
        base_seq: ChangeSeq,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Vec<DirentryBindRecord> {
        if limit == 0 {
            return Vec::new();
        }
        if base_seq >= self.indexed_seq() {
            return self.visible_children_page_by_name_key_at_head(
                parent_inode_id,
                start_after_name_key,
                limit,
            );
        }

        let mut children = self.visible_children(parent_inode_id, base_seq);
        children.sort_by(|left, right| left.name_key.cmp(&right.name_key));
        children
            .into_iter()
            .filter(|child| {
                start_after_name_key
                    .map(|last_name_key| child.name_key.as_str() > last_name_key)
                    .unwrap_or(true)
            })
            .take(limit)
            .collect()
    }

    pub fn visible_children_page_by_name_key_at_head(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Vec<DirentryBindRecord> {
        if limit == 0 {
            return Vec::new();
        }
        let Some(parent) = self.visible_inode_at_head(parent_inode_id) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = Vec::with_capacity(limit);
        for direntry in self
            .indexes
            .active_children_after_name_key(parent_inode_id, start_after_name_key)
        {
            if self
                .visible_inode_at_head(direntry.child_inode_id)
                .is_none()
            {
                continue;
            }
            children.push(direntry.clone());
            if children.len() == limit {
                break;
            }
        }
        children
    }

    pub fn visible_children_at_head(&self, parent_inode_id: InodeId) -> Vec<DirentryBindRecord> {
        let Some(parent) = self.visible_inode_at_head(parent_inode_id) else {
            return Vec::new();
        };
        if parent.inode_kind != InodeKind::Dir {
            return Vec::new();
        }

        let mut children = self
            .indexes
            .active_children(parent_inode_id)
            .into_iter()
            .filter(|direntry| {
                self.visible_inode_at_head(direntry.child_inode_id)
                    .is_some()
            })
            .collect::<Vec<_>>();
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        children
    }

    pub fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
        name_policy: NamePolicy,
        base_seq: ChangeSeq,
    ) -> Result<ResolvedVisiblePath, VisiblePathError> {
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id, base_seq)
            .ok_or(VisiblePathError::RootMissing)?;
        if absolute_path.is_root() {
            return Ok(ResolvedVisiblePath {
                absolute_path: "/".to_owned(),
                inode_id: root_inode_id,
                inode_kind: root.inode_kind,
                parent_inode_id: None,
                display_name: String::new(),
            });
        }

        let mut current_inode_id = root_inode_id;
        let mut current_absolute_path = "/".to_owned();
        let mut current_parent_inode_id = None;
        let mut current_display_name = String::new();

        for component in absolute_path.components() {
            let current_inode = self.visible_inode(current_inode_id, base_seq).ok_or(
                VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                },
            )?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                });
            }

            let requested_absolute_path =
                join_display_path(&current_absolute_path, component.as_str());
            let display_name = component.to_display_name();
            let name_key = NameKey::for_display_name(name_policy, &display_name);
            let direntry = self
                .visible_child(current_inode_id, name_key.as_str(), base_seq)
                .ok_or(VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;
            current_inode_id = direntry.child_inode_id;
            current_parent_inode_id = Some(direntry.parent_inode_id);
            current_display_name = direntry.display_name.clone();
            current_absolute_path =
                join_display_path(&current_absolute_path, &direntry.display_name);
        }

        let inode = self
            .visible_inode(current_inode_id, base_seq)
            .ok_or_else(|| VisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            })?;
        Ok(ResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        if base_seq >= self.indexed_seq() {
            return self.indexes.active_child(parent_inode_id, name_key);
        }

        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        if self.is_direntry_unbound_at_seq(&direntry, base_seq) {
            return None;
        }
        let latest_binding =
            self.latest_parent_binding_for_child_at_seq(direntry.child_inode_id, base_seq)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_delta_index != direntry.bind_delta_index
            || self.is_direntry_unbound_at_seq(&latest_binding, base_seq)
        {
            return None;
        }

        Some(direntry)
    }

    fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<DirentryBindRecord> {
        self.direntry_binds
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned()
    }

    pub fn is_direntry_unbound_at_seq(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        if base_seq >= self.indexed_seq() {
            return self.is_direntry_unbound_at_head(direntry);
        }
        self.is_direntry_unbound_at_seq_scan(direntry, base_seq)
    }

    pub(crate) fn is_direntry_unbound_at_head(&self, direntry: &DirentryBindRecord) -> bool {
        self.indexes.is_unbound(direntry)
    }

    fn is_direntry_unbound_at_seq_scan(
        &self,
        direntry: &DirentryBindRecord,
        base_seq: ChangeSeq,
    ) -> bool {
        self.direntry_unbinds.iter().any(|unbind| {
            unbind.unbind_seq <= base_seq
                && unbind.parent_inode_id == direntry.parent_inode_id
                && unbind.name_key == direntry.name_key
                && unbind.child_inode_id == direntry.child_inode_id
                && unbind.bind_seq == direntry.bind_seq
                && unbind.bind_delta_index == direntry.bind_delta_index
        })
    }

    pub fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        if base_seq >= self.indexed_seq() {
            return self.would_create_directory_cycle_at_head(inode_id, new_parent_inode);
        }

        let mut current = Some(new_parent_inode);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == inode_id {
                return true;
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
    }

    pub fn would_create_directory_cycle_at_head(
        &self,
        inode_id: InodeId,
        new_parent_inode: InodeId,
    ) -> bool {
        let mut current = Some(new_parent_inode);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == inode_id {
                return true;
            }
            current = self
                .current_parent_binding_for_child_at_head(candidate_inode_id)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
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

fn push_unique_invariant(invariants: &mut Vec<InvariantId>, id: InvariantId) {
    if !invariants.contains(&id) {
        invariants.push(id);
    }
}

fn join_display_path(base: &str, component: &str) -> String {
    if base == "/" {
        format!("/{component}")
    } else {
        format!("{base}/{component}")
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
    size_of::<DirentryBindRecord>() + record.name_key.len() + record.display_name.len()
}

fn direntry_unbind_decoded_bytes(record: &DirentryUnbindRecord) -> usize {
    size_of::<DirentryUnbindRecord>() + record.name_key.len()
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
    size_of::<ContentRef>() + content_ref.digest.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_direntry_replay_uses_persisted_name_key() {
        let applied = MetadataState::default()
            .apply_committed_wal_deltas(
                ChangeSeq(1),
                &[WalDelta::BindDirentry {
                    delta_index: 7,
                    parent_inode: InodeId(1),
                    name_key: "persisted-key".to_owned(),
                    display_name: "Report.TXT".to_owned(),
                    child_inode: InodeId(2),
                }],
            )
            .expect("apply bind delta");

        assert_eq!(applied.metadata_state.direntry_binds().len(), 1);
        let bind = &applied.metadata_state.direntry_binds()[0];
        assert_eq!(bind.name_key, "persisted-key");
        assert_eq!(bind.display_name, "Report.TXT");
        assert_eq!(bind.bind_delta_index, 7);
    }

    #[test]
    fn child_lookup_uses_persisted_name_key_without_recanonicalizing() {
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            vec![DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: "persisted-key".to_owned(),
                display_name: "Report.TXT".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert!(metadata_state
            .visible_child(InodeId(1), "persisted-key", ChangeSeq(1))
            .is_some());
        assert!(metadata_state
            .visible_child(InodeId(1), "Report.TXT", ChangeSeq(1))
            .is_none());
    }

    #[test]
    fn maintained_indexes_track_bind_unbind_rename_and_tombstone() {
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(2),
                },
            ],
            vec![
                DirentryBindRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_delta_index: 0,
                },
                DirentryBindRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(3),
                    bind_seq: ChangeSeq(2),
                    bind_delta_index: 0,
                },
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(metadata_state.indexed_seq(), ChangeSeq(2));
        assert!(metadata_state.inode_at_head(InodeId(2)).is_some());
        assert_eq!(
            metadata_state
                .visible_child_at_head(InodeId(1), "docs")
                .expect("docs visible")
                .child_inode_id,
            InodeId(2)
        );
        assert_eq!(
            metadata_state
                .current_parent_binding_for_child_at_head(InodeId(2))
                .expect("parent binding")
                .parent_inode_id,
            InodeId(1)
        );

        let metadata_state = metadata_state
            .apply_committed_wal_deltas(
                ChangeSeq(3),
                &[WalDelta::UnbindDirentry {
                    delta_index: 0,
                    parent_inode: InodeId(1),
                    name_key: "docs".to_owned(),
                    child_inode: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_delta_index: 0,
                }],
            )
            .expect("unbind")
            .metadata_state;
        assert!(metadata_state
            .visible_child_at_head(InodeId(1), "docs")
            .is_none());
        assert!(metadata_state
            .current_parent_binding_for_child_at_head(InodeId(2))
            .is_none());

        let metadata_state = metadata_state
            .apply_committed_wal_deltas(
                ChangeSeq(4),
                &[WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode: InodeId(1),
                    name_key: "renamed".to_owned(),
                    display_name: "renamed".to_owned(),
                    child_inode: InodeId(2),
                }],
            )
            .expect("rebind")
            .metadata_state;
        assert!(metadata_state
            .visible_child_at_head(InodeId(1), "docs")
            .is_none());
        assert_eq!(
            metadata_state
                .visible_child_at_head(InodeId(1), "renamed")
                .expect("renamed visible")
                .child_inode_id,
            InodeId(2)
        );

        let metadata_state = metadata_state
            .apply_committed_wal_deltas(
                ChangeSeq(5),
                &[WalDelta::TombstoneSubtree {
                    delta_index: 0,
                    root_inode: InodeId(2),
                }],
            )
            .expect("tombstone")
            .metadata_state;
        assert!(metadata_state
            .visible_child_at_head(InodeId(1), "renamed")
            .is_none());
        assert_eq!(
            metadata_state
                .covering_subtree_tombstone_at_head(InodeId(3))
                .expect("descendant tombstone")
                .root_inode_id,
            InodeId(2)
        );
    }

    #[test]
    fn rebuilt_indexes_answer_current_head_queries_after_deserialize() {
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            vec![DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: "file.txt".to_owned(),
                display_name: "file.txt".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let encoded = serde_json::to_string(&metadata_state).expect("encode metadata");
        assert!(!encoded.contains("indexes"));
        assert!(!encoded.contains("row_count"));
        assert!(!encoded.contains("decoded_bytes"));
        let decoded: MetadataState = serde_json::from_str(&encoded).expect("decode metadata");

        assert_eq!(decoded.row_count(), metadata_state.row_count());
        assert_eq!(decoded.decoded_bytes(), metadata_state.decoded_bytes());
        assert_eq!(decoded.indexed_seq(), ChangeSeq(1));
        assert_eq!(
            decoded
                .visible_child_at_head(InodeId(1), "file.txt")
                .expect("indexed child")
                .child_inode_id,
            InodeId(2)
        );
    }

    #[test]
    fn stale_binding_is_not_active_after_newer_bind_claims_same_name() {
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(0),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(3),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(2),
                },
            ],
            vec![
                DirentryBindRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "report".to_owned(),
                    display_name: "report".to_owned(),
                    child_inode_id: InodeId(2),
                    bind_seq: ChangeSeq(1),
                    bind_delta_index: 0,
                },
                DirentryBindRecord {
                    parent_inode_id: InodeId(1),
                    name_key: "report".to_owned(),
                    display_name: "report".to_owned(),
                    child_inode_id: InodeId(3),
                    bind_seq: ChangeSeq(2),
                    bind_delta_index: 0,
                },
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(
            metadata_state
                .visible_child_at_head(InodeId(1), "report")
                .expect("latest child")
                .child_inode_id,
            InodeId(3)
        );
        assert!(metadata_state
            .current_parent_binding_for_child_at_head(InodeId(2))
            .is_none());
    }

    #[test]
    fn resolve_visible_path_uses_explicit_name_policy_and_stored_display_name() {
        let metadata_state = MetadataState::from_rows(
            vec![
                InodeRecord {
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(1),
                },
            ],
            vec![DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: "report.txt".to_owned(),
                display_name: "Report.TXT".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let resolved = metadata_state
            .resolve_visible_path(
                &AbsolutePath::parse("/REPORT.txt").expect("path"),
                NamePolicy::NfcCasefoldV0,
                ChangeSeq(1),
            )
            .expect("resolve path");

        assert_eq!(resolved.inode_id, InodeId(2));
        assert_eq!(resolved.absolute_path, "/Report.TXT");
        assert_eq!(resolved.display_name, "Report.TXT");
    }

    #[test]
    fn metadata_state_serialized_shape_preserves_row_field_names() {
        let metadata_state = MetadataState::from_rows(
            vec![InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        let encoded = serde_json::to_value(&metadata_state).expect("encode metadata state");
        assert_eq!(
            encoded,
            serde_json::json!({
                "inodes": [{
                    "inode_id": 1,
                    "inode_kind": "dir",
                    "created_seq": 0
                }],
                "direntry_binds": [],
                "direntry_unbinds": [],
                "revisions": [],
                "subtree_tombstones": [],
                "commit_receipts": []
            })
        );
    }

    #[test]
    fn metadata_state_accessors_expose_rows_read_only() {
        let metadata_state = MetadataState::from_rows(
            vec![InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(0),
            }],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );

        assert_eq!(metadata_state.inodes().len(), 1);
        assert!(metadata_state.direntry_binds().is_empty());
        assert!(metadata_state.direntry_unbinds().is_empty());
        assert!(metadata_state.revisions().is_empty());
        assert!(metadata_state.subtree_tombstones().is_empty());
        assert!(metadata_state.commit_receipts().is_empty());
    }

    #[test]
    fn find_commit_receipt_returns_latest_matching_receipt() {
        let commit_id = CommitId::parse("same-commit").expect("valid commit id");
        let metadata_state = MetadataState::from_rows(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![
                CommitReceiptRecord {
                    commit_id: commit_id.clone(),
                    semantic_commit_fingerprint: "old".to_owned(),
                    committed_seq: ChangeSeq(1),
                    message: Some("old message".to_owned()),
                },
                CommitReceiptRecord {
                    commit_id: CommitId::parse("other-commit").expect("valid commit id"),
                    semantic_commit_fingerprint: "other".to_owned(),
                    committed_seq: ChangeSeq(3),
                    message: None,
                },
                CommitReceiptRecord {
                    commit_id: commit_id.clone(),
                    semantic_commit_fingerprint: "new".to_owned(),
                    committed_seq: ChangeSeq(2),
                    message: Some("new message".to_owned()),
                },
            ],
        );

        let receipt = metadata_state
            .find_commit_receipt(&commit_id)
            .expect("receipt");
        assert_eq!(receipt.committed_seq, ChangeSeq(2));
        assert_eq!(receipt.semantic_commit_fingerprint, "new");
    }

    #[test]
    fn revision_and_receipt_indexes_rebuild_and_update_incrementally() {
        let commit_id = CommitId::parse("indexed-commit").expect("valid commit id");
        let content_ref = ContentRef {
            kind: loonfs_api::ContentRefKind::WholeFileV0,
            digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            size_bytes: 12,
        };
        let replacement_ref = ContentRef {
            kind: loonfs_api::ContentRefKind::WholeFileV0,
            digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            size_bytes: 24,
        };

        let mut builder = MetadataStateBuilder::default();
        builder.push_inode(InodeRecord {
            inode_id: InodeId(7),
            inode_kind: InodeKind::File,
            created_seq: ChangeSeq(1),
        });
        builder.push_revision(RevisionRecord {
            inode_id: InodeId(7),
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(2),
            revision_delta_index: 0,
            content_ref: content_ref.clone(),
        });
        builder.push_revision(RevisionRecord {
            inode_id: InodeId(7),
            revision_no: RevisionNo(2),
            committed_seq: ChangeSeq(3),
            revision_delta_index: 0,
            content_ref: replacement_ref.clone(),
        });
        builder.push_commit_receipt(CommitReceiptRecord {
            commit_id: commit_id.clone(),
            semantic_commit_fingerprint: "fingerprint".to_owned(),
            committed_seq: ChangeSeq(3),
            message: Some("replace indexed file".to_owned()),
        });
        let metadata_state = builder.finish();

        assert_eq!(metadata_state.row_count(), 4);
        assert!(metadata_state.decoded_bytes() >= metadata_state.row_count());
        assert_eq!(
            metadata_state
                .latest_revision_at_head(InodeId(7))
                .expect("latest revision")
                .revision_no,
            RevisionNo(2)
        );
        assert_eq!(
            metadata_state
                .revision_at_head(InodeId(7), RevisionNo(1))
                .expect("first revision")
                .content_ref,
            content_ref
        );
        assert_eq!(
            metadata_state
                .find_commit_receipt(&commit_id)
                .expect("commit receipt")
                .committed_seq,
            ChangeSeq(3)
        );

        let decoded: MetadataState =
            serde_json::from_value(serde_json::to_value(&metadata_state).expect("encode"))
                .expect("decode");
        assert_eq!(
            decoded
                .latest_revision_at_head(InodeId(7))
                .expect("latest revision after decode")
                .content_ref,
            replacement_ref
        );
        assert_eq!(
            decoded
                .find_commit_receipt(&commit_id)
                .expect("receipt after decode")
                .semantic_commit_fingerprint,
            "fingerprint"
        );
    }

    /// Binding churn under one parent:
    /// - seq 1: child 2 bound at `contested`, child 4 bound at `deleted`
    /// - seq 2: child 2 renamed to `renamed-away`, child 4 unbound for good
    /// - seq 3: child 3 takes over `contested`
    ///
    /// At head this leaves `contested` rebound, `renamed-away` active, and
    /// `deleted` with only a dead binding.
    fn churned_binding_state() -> MetadataState {
        let mut state = MetadataState::default();
        state
            .apply_committed_wal_deltas_mut(
                ChangeSeq(0),
                &[WalDelta::CreateInode {
                    delta_index: 0,
                    inode_id: InodeId(1),
                    inode_kind: InodeKind::Dir,
                }],
            )
            .expect("seed root");
        state
            .apply_committed_wal_deltas_mut(
                ChangeSeq(1),
                &[
                    WalDelta::CreateInode {
                        delta_index: 0,
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::Dir,
                    },
                    WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode: InodeId(1),
                        name_key: "contested".to_owned(),
                        display_name: "contested".to_owned(),
                        child_inode: InodeId(2),
                    },
                    WalDelta::CreateInode {
                        delta_index: 2,
                        inode_id: InodeId(4),
                        inode_kind: InodeKind::Dir,
                    },
                    WalDelta::BindDirentry {
                        delta_index: 3,
                        parent_inode: InodeId(1),
                        name_key: "deleted".to_owned(),
                        display_name: "deleted".to_owned(),
                        child_inode: InodeId(4),
                    },
                ],
            )
            .expect("bind children 2 and 4");
        state
            .apply_committed_wal_deltas_mut(
                ChangeSeq(2),
                &[
                    WalDelta::UnbindDirentry {
                        delta_index: 0,
                        parent_inode: InodeId(1),
                        name_key: "contested".to_owned(),
                        child_inode: InodeId(2),
                        bind_seq: ChangeSeq(1),
                        bind_delta_index: 1,
                    },
                    WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode: InodeId(1),
                        name_key: "renamed-away".to_owned(),
                        display_name: "renamed-away".to_owned(),
                        child_inode: InodeId(2),
                    },
                    WalDelta::UnbindDirentry {
                        delta_index: 2,
                        parent_inode: InodeId(1),
                        name_key: "deleted".to_owned(),
                        child_inode: InodeId(4),
                        bind_seq: ChangeSeq(1),
                        bind_delta_index: 3,
                    },
                ],
            )
            .expect("rename child 2, unbind child 4");
        state
            .apply_committed_wal_deltas_mut(
                ChangeSeq(3),
                &[
                    WalDelta::CreateInode {
                        delta_index: 0,
                        inode_id: InodeId(3),
                        inode_kind: InodeKind::Dir,
                    },
                    WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode: InodeId(1),
                        name_key: "contested".to_owned(),
                        display_name: "contested".to_owned(),
                        child_inode: InodeId(3),
                    },
                ],
            )
            .expect("bind child 3");
        state
    }

    /// Rebuilds the churned state from its rows, so the `from_rows` index
    /// construction path is pinned against the incremental one.
    fn churned_binding_state_rebuilt() -> MetadataState {
        let incremental = churned_binding_state();
        MetadataState::from_rows(
            incremental.inodes().to_vec(),
            incremental.direntry_binds().to_vec(),
            incremental.direntry_unbinds().to_vec(),
            incremental.revisions().to_vec(),
            incremental.subtree_tombstones().to_vec(),
            incremental.commit_receipts().to_vec(),
        )
    }

    #[test]
    fn bound_child_at_head_sees_latest_bind_including_dead_bindings() {
        let state = churned_binding_state();
        let head = state.indexed_seq();
        assert_eq!(head, ChangeSeq(3));

        // The latest bind at the contested name is child 3.
        let head_bind = state
            .bound_child_at_seq(InodeId(1), "contested", head)
            .expect("bind at head");
        assert_eq!(head_bind.child_inode_id, InodeId(3));
        assert_eq!(head_bind.bind_seq, ChangeSeq(3));

        // The deleted name still answers with its dead binding: the bind is
        // unbound but tombstone-ancestry walks must see it.
        let dead_bind = state
            .bound_child_at_seq(InodeId(1), "deleted", head)
            .expect("dead binding visible at head");
        assert_eq!(dead_bind.child_inode_id, InodeId(4));
        assert!(state.is_direntry_unbound_at_seq(&dead_bind, head));
        assert!(state.visible_child(InodeId(1), "deleted", head).is_none());
    }

    #[test]
    fn bound_child_below_indexed_seq_still_scans_history() {
        let state = churned_binding_state();

        // At seq 2 the contested name's latest bind is still child 2's
        // (unbound) binding; the rebind at seq 3 is not visible yet.
        let historical = state
            .bound_child_at_seq(InodeId(1), "contested", ChangeSeq(2))
            .expect("historical bind");
        assert_eq!(historical.child_inode_id, InodeId(2));
        assert_eq!(historical.bind_seq, ChangeSeq(1));
    }

    #[test]
    fn incremental_and_rebuilt_indexes_agree_on_latest_binds() {
        let incremental = churned_binding_state();
        let rebuilt = churned_binding_state_rebuilt();

        for name_key in ["contested", "renamed-away", "deleted", "never-bound"] {
            assert_eq!(
                rebuilt.bound_child_at_seq(InodeId(1), name_key, ChangeSeq(3)),
                incremental.bound_child_at_seq(InodeId(1), name_key, ChangeSeq(3)),
                "latest bind for `{name_key}` diverges between construction paths"
            );
        }
    }

    /// Queries above `indexed_seq()` are at-head queries: commit validation
    /// probes the materialization at the next assigned seq and must hit the indexes.
    #[test]
    fn queries_above_indexed_seq_match_at_head_results() {
        let state = churned_binding_state();
        let beyond_head = ChangeSeq(state.indexed_seq().0 + 1);

        assert_eq!(
            state.bound_child_at_seq(InodeId(1), "contested", beyond_head),
            state.indexes.latest_bind(InodeId(1), "contested"),
        );
        assert_eq!(
            state.visible_child(InodeId(1), "contested", beyond_head),
            state.visible_child_at_head(InodeId(1), "contested"),
        );
        assert_eq!(
            state.current_parent_binding_for_child(InodeId(2), beyond_head),
            state.current_parent_binding_for_child_at_head(InodeId(2)),
        );
        assert_eq!(
            state.visible_inode(InodeId(3), beyond_head),
            state.visible_inode_at_head(InodeId(3)),
        );
    }
}
