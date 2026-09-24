//! Application of committed WAL deltas and commit records onto
//! [`MetadataState`] rows.

use super::{
    AccessRevisionRecord, AttributesRevisionRecord, CommitReceiptRecord, ContentPublicationRecord,
    DirentryBindingRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
    TombstoneRowAction,
};
use loonfs_api::wire::manifest::{DeltaPosition, DirentryBindingState};
use loonfs_api::wire::wal::{WalCommitPayload, WalDelta};
use loonfs_api::{ActorId, ChangeSeq, CommitId};

impl MetadataState {
    pub fn apply_committed_wal_deltas(
        &self,
        committed_seq: ChangeSeq,
        commit_id: &CommitId,
        actor: &ActorId,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) -> MetadataState {
        let mut metadata_state = self.clone();
        metadata_state.apply_committed_wal_deltas_mut(
            committed_seq,
            commit_id,
            actor,
            committed_at_ms,
            deltas,
        );
        metadata_state
    }

    pub fn apply_committed_wal_deltas_mut(
        &mut self,
        committed_seq: ChangeSeq,
        commit_id: &CommitId,
        actor: &ActorId,
        committed_at_ms: u64,
        deltas: &[WalDelta],
    ) {
        for delta in deltas {
            self.apply_committed_wal_delta_mut(
                committed_seq,
                commit_id,
                actor,
                committed_at_ms,
                delta,
            );
        }
    }

    /// Appends the metadata row encoded by one committed WAL delta.
    ///
    /// This is the only WAL-delta to metadata-row mapping in the crate:
    /// durable replay ([`Self::apply_committed_wal_deltas_mut`]) and the
    /// commit validation overlay (`commit::validate::view`) both append
    /// rows through it, so the effects a batch validates against cannot
    /// diverge from what replay later persists.
    pub(crate) fn apply_committed_wal_delta_mut(
        &mut self,
        committed_seq: ChangeSeq,
        commit_id: &CommitId,
        actor: &ActorId,
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
                    committed_seq,
                    commit_id: commit_id.clone(),
                    committed_by: actor.clone(),
                    committed_at_ms,
                });
            }
            WalDelta::BindDirentry {
                delta_index,
                parent_inode_id,
                name_key,
                display_name,
                child_inode_id,
            } => {
                self.push_direntry_binding_record(DirentryBindingRecord {
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    child_inode_id: *child_inode_id,
                    committed_seq,
                    delta_index: *delta_index,
                    state: DirentryBindingState::Bound {
                        display_name: display_name.clone(),
                    },
                });
            }
            WalDelta::UnbindDirentry {
                delta_index,
                parent_inode_id,
                name_key,
                child_inode_id,
                ..
            } => {
                self.push_direntry_binding_record(DirentryBindingRecord {
                    parent_inode_id: *parent_inode_id,
                    name_key: name_key.clone(),
                    child_inode_id: *child_inode_id,
                    committed_seq,
                    delta_index: *delta_index,
                    state: DirentryBindingState::Unbound,
                });
            }
            WalDelta::AppendFileRevision {
                delta_index,
                inode_id,
                revision_no,
                content_ref,
            } => {
                if self.find_content_publication(&content_ref.content_id) != Some(committed_seq) {
                    self.push_content_publication_record(ContentPublicationRecord {
                        content_id: content_ref.content_id.clone(),
                        committed_seq,
                        delta_index: *delta_index,
                    });
                }
                self.push_revision_record(RevisionRecord {
                    inode_id: *inode_id,
                    revision_no: *revision_no,
                    committed_seq,
                    commit_id: commit_id.clone(),
                    committed_at_ms,
                    committed_by: actor.clone(),
                    delta_index: *delta_index,
                    content_ref: content_ref.clone(),
                });
            }
            WalDelta::TombstoneSubtree {
                delta_index,
                root_inode_id,
                deleted_binding,
            } => {
                self.push_subtree_tombstone_record(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    generation: DeltaPosition {
                        seq: committed_seq,
                        delta_index: *delta_index,
                    },
                    commit_id: commit_id.clone(),
                    committed_at_ms,
                    committed_by: actor.clone(),
                    action: TombstoneRowAction::Set {
                        deleted_binding: deleted_binding.clone(),
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
                    generation: DeltaPosition {
                        seq: committed_seq,
                        delta_index: *delta_index,
                    },
                    commit_id: commit_id.clone(),
                    committed_at_ms,
                    committed_by: actor.clone(),
                    action: TombstoneRowAction::Revoke { target: *target },
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
                    commit_id: commit_id.clone(),
                    delta_index: *delta_index,
                    committed_by: actor.clone(),
                    committed_at_ms,
                    attributes: attributes.clone(),
                });
            }
            WalDelta::AppendAccessRevision {
                delta_index,
                inode_id,
                access_revision_no,
                boundary,
                grants,
            } => {
                self.push_access_revision_record(AccessRevisionRecord {
                    inode_id: *inode_id,
                    access_revision_no: *access_revision_no,
                    committed_seq,
                    commit_id: commit_id.clone(),
                    delta_index: *delta_index,
                    committed_by: actor.clone(),
                    committed_at_ms,
                    boundary: *boundary,
                    grants: grants.clone(),
                });
            }
        }
    }

    pub fn apply_committed_wal_record_mut(&mut self, record: &WalCommitPayload) {
        for delta in &record.deltas {
            self.apply_committed_wal_delta_mut(
                record.seq,
                &record.commit_id,
                &record.committed_by,
                record.committed_at_ms,
                &delta.delta,
            );
        }
        self.push_commit_record(WalCommitPayload {
            seq: record.seq,
            commit_id: record.commit_id.clone(),
            committed_by: record.committed_by.clone(),
            semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
            committed_at_ms: record.committed_at_ms,
            message: record.message.clone(),
            deltas: record.deltas.clone(),
            inline_content: Vec::new(),
        });
        self.push_commit_receipt_record(CommitReceiptRecord {
            commit_id: record.commit_id.clone(),
            committed_seq: record.seq,
            semantic_commit_fingerprint: record.semantic_commit_fingerprint.clone(),
        });
    }
}
