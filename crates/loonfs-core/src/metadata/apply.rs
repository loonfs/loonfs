//! Application of committed WAL deltas and commit records onto
//! [`MetadataState`] rows.

use super::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    RevisionRecord, SubtreeTombstoneAction, SubtreeTombstoneRecord,
};
use crate::invariants::{push_unique_invariant, InvariantId};
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_api::{ChangeSeq, CommitId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMetadataState {
    pub metadata_state: MetadataState,
    pub checked_invariants: Vec<InvariantId>,
}

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) -> AppliedMetadataState {
        let mut metadata_state = self.clone();
        let checked_invariants =
            metadata_state.apply_committed_wal_deltas_mut(committed_seq, committed_at_ms, deltas);

        AppliedMetadataState {
            metadata_state,
            checked_invariants,
        }
    }

    pub fn apply_committed_wal_deltas_mut(
        &mut self,
        committed_seq: ChangeSeq,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) -> Vec<InvariantId> {
        let mut checked_invariants = Vec::new();

        for delta in deltas {
            let checked_invariant =
                self.apply_committed_wal_delta_mut(committed_seq, committed_at_ms, delta);
            push_unique_invariant(&mut checked_invariants, checked_invariant);
        }

        checked_invariants
    }

    /// Appends the metadata row encoded by one committed WAL delta and
    /// returns the invariant that append witnesses.
    ///
    /// This is the only WAL-delta to metadata-row mapping in the crate:
    /// durable replay ([`Self::apply_committed_wal_deltas_mut`]) and the
    /// commit validation overlay (`commit::metadata_overlay`) both append
    /// rows through it, so the effects a batch validates against cannot
    /// diverge from what replay later persists.
    pub(crate) fn apply_committed_wal_delta_mut(
        &mut self,
        committed_seq: ChangeSeq,
        committed_at_ms: u64,
        delta: &WalDelta,
    ) -> InvariantId {
        match delta {
            WalDelta::CreateInode {
                delta_index: _,
                inode_id,
                inode_kind,
            } => {
                self.push_inode_record(InodeRecord {
                    inode_id: *inode_id,
                    inode_kind: *inode_kind,
                    created_seq: committed_seq,
                });
                InvariantId::CreateInodeWritesInodeRow
            }
            WalDelta::BindDirentry {
                delta_index,
                parent_inode_id,
                name_key,
                display_name,
                child_inode_id,
            } => {
                self.push_direntry_bind_record(DirentryBindRecord {
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode_id: *child_inode_id,
                    bind_seq: committed_seq,
                    bind_delta_index: *delta_index,
                });
                InvariantId::BindDirentryWritesDirentryBindRow
            }
            WalDelta::UnbindDirentry {
                delta_index,
                parent_inode_id,
                name_key,
                display_name,
                child_inode_id,
                bind_seq,
                bind_delta_index,
            } => {
                self.push_direntry_unbind_record(DirentryUnbindRecord {
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    child_inode_id: *child_inode_id,
                    bind_seq: *bind_seq,
                    bind_delta_index: *bind_delta_index,
                    unbind_seq: committed_seq,
                    unbind_delta_index: *delta_index,
                });
                InvariantId::UnbindDirentryWritesUnbindRow
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
                    committed_at_ms,
                    revision_delta_index: *delta_index,
                    content_ref: content_ref.clone(),
                });
                InvariantId::AppendFileRevisionWritesRevisionRow
            }
            WalDelta::TombstoneSubtree {
                delta_index,
                root_inode_id,
                parent_inode_id,
                name_key,
                display_name,
            } => {
                self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    tombstone_seq: committed_seq,
                    tombstone_delta_index: *delta_index,
                    deleted_at_ms: committed_at_ms,
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    display_name: display_name.clone(),
                    action: SubtreeTombstoneAction::Set,
                });
                InvariantId::TombstoneSubtreeWritesTombstoneRow
            }
            WalDelta::RevokeSubtreeTombstone {
                delta_index,
                root_inode_id,
                target_seq,
                target_delta_index,
            } => {
                self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    tombstone_seq: committed_seq,
                    tombstone_delta_index: *delta_index,
                    deleted_at_ms: committed_at_ms,
                    parent_inode_id: None,
                    name_key: None,
                    display_name: None,
                    action: SubtreeTombstoneAction::Revoke {
                        target_seq: *target_seq,
                        target_delta_index: *target_delta_index,
                    },
                });
                InvariantId::RevokeSubtreeTombstoneWritesRevokeRow
            }
        }
    }

    pub fn apply_committed_wal_record_mut(
        &mut self,
        record: &WalCommitPayload,
    ) -> Vec<InvariantId> {
        self.apply_committed_wal_record_parts_mut(
            record.seq,
            record.committed_at_ms,
            &record.commit_id,
            &record.semantic_commit_fingerprint,
            record.message.as_deref(),
            &record.deltas,
        )
    }

    pub(crate) fn apply_committed_wal_record_parts_mut(
        &mut self,
        seq: ChangeSeq,
        committed_at_ms: u64,
        commit_id: &CommitId,
        semantic_commit_fingerprint: &str,
        message: Option<&str>,
        deltas: &[WalCommitDelta],
    ) -> Vec<InvariantId> {
        let mut checked_invariants = Vec::new();
        for delta in deltas {
            let checked_invariant =
                self.apply_committed_wal_delta_mut(seq, committed_at_ms, &delta.delta);
            push_unique_invariant(&mut checked_invariants, checked_invariant);
        }
        self.push_commit_receipt_record(CommitReceiptRecord {
            commit_id: commit_id.clone(),
            semantic_commit_fingerprint: semantic_commit_fingerprint.to_owned(),
            committed_seq: seq,
            committed_at_ms,
            message: message.map(str::to_owned),
        });
        push_unique_invariant(
            &mut checked_invariants,
            InvariantId::WalReplayRecordsCommitReceipt,
        );
        checked_invariants
    }
}
