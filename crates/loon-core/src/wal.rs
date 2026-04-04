use crate::commit::{CommitOp, CommitPlan, CommitRequest, Precondition};
use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::{
    decode_wal_commit_envelope_zstd, encode_wal_commit_envelope_zstd, ChangeSeq, HeadState,
    InodeId, NamespaceId, WalCommitEnvelope, WalCommitPayload, WalOp, WalPrecondition,
};
use loon_objectstore::keys::wal_commit;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedWalCommit {
    pub object_key: String,
    pub commit_id: String,
    pub envelope: WalCommitEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalBuildError {
    EmptyWriterVersion,
    WalWriteNotRequired,
    NamespaceMismatch {
        request: NamespaceId,
        plan: NamespaceId,
    },
    BaseHeadSeqMismatch {
        request: ChangeSeq,
        plan: ChangeSeq,
    },
    AllocatedInodeCountMismatch {
        request_create_ops: usize,
        plan_allocated_count: usize,
    },
    Codec(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredWalObject {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalCommit {
    pub object_key: String,
    pub envelope: WalCommitEnvelope,
    pub resulting_head: HeadState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalCommitWithMetadata {
    pub object_key: String,
    pub envelope: WalCommitEnvelope,
    pub resulting_head: HeadState,
    pub resulting_metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalTail {
    pub resulting_head: HeadState,
    pub resulting_metadata_state: MetadataState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalReplayError {
    Codec(String),
    ObjectKeyMismatch {
        expected: String,
        actual: String,
    },
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    BaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    MetadataApply(MetadataApplyError),
    SeqOverflow,
}

pub fn prepare_wal_commit(
    request: &CommitRequest,
    plan: &CommitPlan,
    writer_version: &str,
) -> Result<PreparedWalCommit, WalBuildError> {
    if writer_version.trim().is_empty() {
        return Err(WalBuildError::EmptyWriterVersion);
    }

    if !plan.wal_object_must_be_written {
        return Err(WalBuildError::WalWriteNotRequired);
    }

    if request.namespace_id != plan.namespace_id {
        return Err(WalBuildError::NamespaceMismatch {
            request: request.namespace_id.clone(),
            plan: plan.namespace_id.clone(),
        });
    }

    if request.planned_head_seq != plan.base_head_seq {
        return Err(WalBuildError::BaseHeadSeqMismatch {
            request: request.planned_head_seq,
            plan: plan.base_head_seq,
        });
    }

    let payload = WalCommitPayload {
        namespace_id: plan.namespace_id.clone(),
        seq: plan.next_seq,
        base_head_seq: plan.base_head_seq,
        commit_id: plan.commit_id.clone(),
        request_id: request.request_id.clone(),
        writer_id: request.writer_id.clone(),
        writer_fence_token: request.writer_fence_token,
        ops: build_wal_ops(request, plan)?,
        preconditions: request
            .preconditions
            .iter()
            .map(WalPrecondition::from)
            .collect(),
    };
    let envelope = WalCommitEnvelope::from_payload(writer_version, payload)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let encoded_bytes = encode_wal_commit_envelope_zstd(&envelope)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;

    Ok(PreparedWalCommit {
        object_key: wal_commit(plan.namespace_id.as_str(), plan.next_seq.0, &plan.commit_id),
        commit_id: plan.commit_id.clone(),
        envelope,
        encoded_bytes,
        checked_invariants: vec![
            "wal_payload_checksum_matches_payload".to_owned(),
            "wal_key_matches_committed_seq".to_owned(),
            "head_publish_requires_durable_wal".to_owned(),
        ],
    })
}

pub fn replay_wal_commit(
    current_head: &HeadState,
    wal_object: &StoredWalObject,
) -> Result<ReplayedWalCommit, WalReplayError> {
    let envelope = decode_and_validate_replayed_wal(current_head, wal_object)?;

    Ok(ReplayedWalCommit {
        object_key: wal_object.object_key.clone(),
        envelope: envelope.clone(),
        resulting_head: HeadState {
            namespace_id: current_head.namespace_id.clone(),
            seq: envelope.payload.seq,
            active_fence_token: envelope.payload.writer_fence_token,
            next_inode_id: replay_next_inode_id(current_head.next_inode_id, &envelope.payload.ops),
            snapshot_hint_seq: current_head.snapshot_hint_seq,
            retention_floor_seq: current_head.retention_floor_seq,
        },
        checked_invariants: vec![
            "wal_payload_checksum_matches_payload".to_owned(),
            "wal_key_matches_committed_seq".to_owned(),
            "wal_replay_requires_matching_namespace".to_owned(),
            "wal_replay_requires_matching_base_head_seq".to_owned(),
            "wal_tail_seq_is_contiguous".to_owned(),
        ],
    })
}

pub fn replay_wal_commit_with_metadata(
    current_head: &HeadState,
    current_metadata_state: &MetadataState,
    wal_object: &StoredWalObject,
) -> Result<ReplayedWalCommitWithMetadata, WalReplayError> {
    let replayed = replay_wal_commit(current_head, wal_object)?;
    let applied_metadata = current_metadata_state
        .apply_committed_wal_ops(replayed.resulting_head.seq, &replayed.envelope.payload.ops)
        .map_err(WalReplayError::MetadataApply)?;
    let mut checked_invariants = replayed.checked_invariants.clone();
    push_invariant(&mut checked_invariants, "wal_replay_applies_metadata_rows");
    extend_invariants(
        &mut checked_invariants,
        &applied_metadata.checked_invariants,
    );

    Ok(ReplayedWalCommitWithMetadata {
        object_key: replayed.object_key,
        envelope: replayed.envelope,
        resulting_head: replayed.resulting_head,
        resulting_metadata_state: applied_metadata.metadata_state,
        checked_invariants,
    })
}

pub fn replay_wal_tail(
    basis_head: &HeadState,
    wal_tail: &[StoredWalObject],
) -> Result<HeadState, WalReplayError> {
    let mut current_head = basis_head.clone();

    for wal_object in wal_tail {
        current_head = replay_wal_commit(&current_head, wal_object)?.resulting_head;
    }

    Ok(current_head)
}

pub fn replay_wal_tail_with_metadata(
    basis_head: &HeadState,
    basis_metadata_state: &MetadataState,
    wal_tail: &[StoredWalObject],
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut current_head = basis_head.clone();
    let mut current_metadata_state = basis_metadata_state.clone();
    let mut checked_invariants = Vec::new();

    for wal_object in wal_tail {
        let replayed =
            replay_wal_commit_with_metadata(&current_head, &current_metadata_state, wal_object)?;
        current_head = replayed.resulting_head;
        current_metadata_state = replayed.resulting_metadata_state;
        extend_invariants(&mut checked_invariants, &replayed.checked_invariants);
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
        checked_invariants,
    })
}

impl From<&PreparedWalCommit> for StoredWalObject {
    fn from(value: &PreparedWalCommit) -> Self {
        Self {
            object_key: value.object_key.clone(),
            encoded_bytes: value.encoded_bytes.clone(),
        }
    }
}

fn build_wal_ops(request: &CommitRequest, plan: &CommitPlan) -> Result<Vec<WalOp>, WalBuildError> {
    let request_create_ops = request
        .ops
        .iter()
        .filter(|op| matches!(op, CommitOp::CreateDir { .. } | CommitOp::CreateFile { .. }))
        .count();
    if request_create_ops != plan.allocated_inode_ids.len() {
        return Err(WalBuildError::AllocatedInodeCountMismatch {
            request_create_ops,
            plan_allocated_count: plan.allocated_inode_ids.len(),
        });
    }

    let mut allocated_inode_ids = plan.allocated_inode_ids.iter().copied();
    let mut wal_ops = Vec::with_capacity(request.ops.len());

    for (op_index, op) in request.ops.iter().enumerate() {
        let op_index = u32::try_from(op_index)
            .map_err(|_| WalBuildError::Codec("op index overflow".to_owned()))?;
        let wal_op = match op {
            CommitOp::CreateDir {
                parent_inode,
                display_name,
            } => WalOp::CreateDir {
                op_index,
                inode_id: allocated_inode_ids.next().ok_or(
                    WalBuildError::AllocatedInodeCountMismatch {
                        request_create_ops,
                        plan_allocated_count: plan.allocated_inode_ids.len(),
                    },
                )?,
                parent_inode: *parent_inode,
                display_name: display_name.clone(),
            },
            CommitOp::CreateFile {
                parent_inode,
                display_name,
                content_manifest_digest,
            } => WalOp::CreateFile {
                op_index,
                inode_id: allocated_inode_ids.next().ok_or(
                    WalBuildError::AllocatedInodeCountMismatch {
                        request_create_ops,
                        plan_allocated_count: plan.allocated_inode_ids.len(),
                    },
                )?,
                parent_inode: *parent_inode,
                display_name: display_name.clone(),
                content_manifest_digest: content_manifest_digest.clone(),
            },
            CommitOp::ReplaceFile {
                inode_id,
                base_revision,
                content_manifest_digest,
            } => WalOp::ReplaceFile {
                op_index,
                inode_id: *inode_id,
                base_revision: *base_revision,
                content_manifest_digest: content_manifest_digest.clone(),
            },
            CommitOp::DeleteFile { inode_id } => WalOp::DeleteFile {
                op_index,
                inode_id: *inode_id,
            },
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
            } => WalOp::Rename {
                op_index,
                inode_id: *inode_id,
                new_parent_inode: *new_parent_inode,
                new_display_name: new_display_name.clone(),
            },
            CommitOp::DeleteSubtree { root_inode } => WalOp::DeleteSubtree {
                op_index,
                root_inode: *root_inode,
            },
            CommitOp::RestoreRevision {
                inode_id,
                base_revision,
                restore_from_revision,
            } => WalOp::RestoreRevision {
                op_index,
                inode_id: *inode_id,
                base_revision: *base_revision,
                restore_from_revision: *restore_from_revision,
            },
        };
        wal_ops.push(wal_op);
    }

    Ok(wal_ops)
}

fn decode_and_validate_replayed_wal(
    current_head: &HeadState,
    wal_object: &StoredWalObject,
) -> Result<WalCommitEnvelope, WalReplayError> {
    let envelope = decode_wal_commit_envelope_zstd(&wal_object.encoded_bytes)
        .map_err(|err| WalReplayError::Codec(err.to_string()))?;
    let expected_object_key = wal_commit(
        envelope.payload.namespace_id.as_str(),
        envelope.payload.seq.0,
        &envelope.payload.commit_id,
    );

    if wal_object.object_key != expected_object_key {
        return Err(WalReplayError::ObjectKeyMismatch {
            expected: expected_object_key,
            actual: wal_object.object_key.clone(),
        });
    }

    if envelope.payload.namespace_id != current_head.namespace_id {
        return Err(WalReplayError::NamespaceMismatch {
            expected: current_head.namespace_id.clone(),
            actual: envelope.payload.namespace_id.clone(),
        });
    }

    if envelope.payload.base_head_seq != current_head.seq {
        return Err(WalReplayError::BaseHeadSeqMismatch {
            expected: current_head.seq,
            actual: envelope.payload.base_head_seq,
        });
    }

    let expected_seq = current_head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(WalReplayError::SeqOverflow)?;

    if envelope.payload.seq != expected_seq {
        return Err(WalReplayError::NonContiguousSeq {
            expected: expected_seq,
            actual: envelope.payload.seq,
        });
    }

    Ok(envelope)
}

fn replay_next_inode_id(current_next_inode_id: InodeId, ops: &[WalOp]) -> InodeId {
    ops.iter()
        .fold(current_next_inode_id, |next_inode_id, op| match op {
            WalOp::CreateDir { inode_id, .. } | WalOp::CreateFile { inode_id, .. } => {
                InodeId(next_inode_id.0.max(inode_id.0.saturating_add(1)))
            }
            WalOp::ReplaceFile { .. }
            | WalOp::DeleteFile { .. }
            | WalOp::Rename { .. }
            | WalOp::DeleteSubtree { .. }
            | WalOp::RestoreRevision { .. } => next_inode_id,
        })
}

fn extend_invariants(checked_invariants: &mut Vec<String>, new_invariants: &[String]) {
    for invariant in new_invariants {
        push_invariant(checked_invariants, invariant);
    }
}

fn push_invariant(checked_invariants: &mut Vec<String>, invariant: &str) {
    if !checked_invariants
        .iter()
        .any(|existing| existing == invariant)
    {
        checked_invariants.push(invariant.to_owned());
    }
}

impl From<&Precondition> for WalPrecondition {
    fn from(value: &Precondition) -> Self {
        match value {
            Precondition::HeadSeqIs(seq) => Self::HeadSeqIs(*seq),
            Precondition::InodeRevisionIs { inode_id, revision } => Self::InodeRevisionIs {
                inode_id: *inode_id,
                revision: *revision,
            },
            Precondition::AncestorsNotSubtreeDeleted { inode_id } => {
                Self::AncestorsNotSubtreeDeleted {
                    inode_id: *inode_id,
                }
            }
            Precondition::ChildNameAbsent {
                parent_inode,
                name_key,
            } => Self::ChildNameAbsent {
                parent_inode: *parent_inode,
                name_key: name_key.clone(),
            },
        }
    }
}
