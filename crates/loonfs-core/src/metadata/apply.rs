//! Application of committed WAL deltas and commit records onto
//! [`MetadataState`] rows.

use super::{
    AttributesRevisionRecord, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneAction, SubtreeTombstoneRecord,
};
use loonfs_api::wire::manifest::TombstoneGeneration;
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_api::{ActorRef, ChangeSeq};

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        actor: &ActorRef,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) -> MetadataState {
        let mut metadata_state = self.clone();
        metadata_state.apply_committed_wal_deltas_mut(
            committed_seq,
            actor,
            committed_at_ms,
            deltas,
        );
        metadata_state
    }

    pub fn apply_committed_wal_deltas_mut(
        &mut self,
        committed_seq: ChangeSeq,
        actor: &ActorRef,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) {
        for delta in deltas {
            self.apply_committed_wal_delta_mut(committed_seq, actor, committed_at_ms, delta);
        }
    }

    /// Appends the metadata row encoded by one committed WAL delta.
    ///
    /// This is the only WAL-delta to metadata-row mapping in the crate:
    /// durable replay ([`Self::apply_committed_wal_deltas_mut`]) and the
    /// commit validation overlay (`commit::metadata_overlay`) both append
    /// rows through it, so the effects a batch validates against cannot
    /// diverge from what replay later persists.
    pub(crate) fn apply_committed_wal_delta_mut(
        &mut self,
        committed_seq: ChangeSeq,
        actor: &ActorRef,
        committed_at_ms: u64,
        delta: &WalDelta,
    ) {
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
                    created_by: actor.clone(),
                    created_at_ms: committed_at_ms,
                });
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
                    actor: actor.clone(),
                    revision_delta_index: *delta_index,
                    content_ref: content_ref.clone(),
                });
            }
            WalDelta::TombstoneSubtree {
                delta_index,
                root_inode_id,
                deleted_direntry,
            } => {
                self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    generation: TombstoneGeneration {
                        seq: committed_seq,
                        delta_index: *delta_index,
                    },
                    deleted_at_ms: committed_at_ms,
                    actor: actor.clone(),
                    action: SubtreeTombstoneAction::Set {
                        deleted_direntry: deleted_direntry.clone(),
                    },
                });
            }
            WalDelta::RevokeSubtreeTombstone {
                delta_index,
                root_inode_id,
                target,
            } => {
                self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    generation: TombstoneGeneration {
                        seq: committed_seq,
                        delta_index: *delta_index,
                    },
                    deleted_at_ms: committed_at_ms,
                    actor: actor.clone(),
                    action: SubtreeTombstoneAction::Revoke { target: *target },
                });
            }
            WalDelta::AppendAttributesRevision {
                delta_index,
                inode_id,
                attributes_revision_no,
                attributes,
            } => {
                self.push_attributes_revision_record(AttributesRevisionRecord {
                    inode_id: *inode_id,
                    attributes_revision_no: *attributes_revision_no,
                    committed_seq,
                    delta_index: *delta_index,
                    actor: actor.clone(),
                    updated_at_ms: committed_at_ms,
                    attributes: attributes.clone(),
                });
            }
        }
    }

    pub fn apply_committed_wal_record_mut(&mut self, record: &WalCommitPayload) {
        self.apply_committed_wal_record_parts_mut(
            CommitReceiptRecord {
                commit_id: record.commit_id.clone(),
                actor: record.actor.clone(),
                semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
                committed_seq: record.seq,
                committed_at_ms: record.committed_at_ms,
                message: record.message.clone(),
            },
            &record.deltas,
        )
    }

    pub(crate) fn apply_committed_wal_record_parts_mut(
        &mut self,
        receipt: CommitReceiptRecord,
        deltas: &[WalCommitDelta],
    ) {
        for delta in deltas {
            self.apply_committed_wal_delta_mut(
                receipt.committed_seq,
                &receipt.actor,
                receipt.committed_at_ms,
                &delta.delta,
            );
        }
        self.push_commit_receipt_record(receipt);
    }
}
