use crate::commit::{CommitOp, CommitPlan, CommitRequest, Precondition};
use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, v0::CommitOpResult,
    ChangeSeq, HeadState, InodeId, NamespaceId, WalCommitPayload, WalOp, WalPrecondition,
    WalSegmentEnvelope, WalSegmentPayload, WalSegmentPointer,
};
use loon_objectstore::keys::wal_segment;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedWalRecord {
    pub request: CommitRequest,
    pub plan: CommitPlan,
    pub results: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedWalSegment {
    pub object_key: String,
    pub segment_id: String,
    pub envelope: WalSegmentEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalBuildError {
    EmptyWriterVersion,
    WalWriteNotRequired,
    EmptySegment,
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
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    Codec(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredWalObject {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalSegment {
    pub object_key: String,
    pub envelope: WalSegmentEnvelope,
    pub resulting_head: HeadState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayedWalSegmentWithMetadata {
    pub object_key: String,
    pub envelope: WalSegmentEnvelope,
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
    EmptySegment,
    SegmentSummaryMismatch,
    MetadataApply(MetadataApplyError),
    SeqOverflow,
}

pub fn prepare_wal_segment(
    namespace_id: NamespaceId,
    prev_visible_segment: Option<WalSegmentPointer>,
    records: &[PreparedWalRecord],
    writer_version: &str,
) -> Result<PreparedWalSegment, WalBuildError> {
    if writer_version.trim().is_empty() {
        return Err(WalBuildError::EmptyWriterVersion);
    }
    if records.is_empty() {
        return Err(WalBuildError::EmptySegment);
    }

    let mut payload_records: Vec<WalCommitPayload> = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        if !record.plan.wal_object_must_be_written {
            return Err(WalBuildError::WalWriteNotRequired);
        }
        if record.request.namespace_id != record.plan.namespace_id {
            return Err(WalBuildError::NamespaceMismatch {
                request: record.request.namespace_id.clone(),
                plan: record.plan.namespace_id.clone(),
            });
        }
        if record.plan.namespace_id != namespace_id {
            return Err(WalBuildError::NamespaceMismatch {
                request: record.plan.namespace_id.clone(),
                plan: namespace_id,
            });
        }
        if index > 0 {
            let expected = payload_records[index - 1]
                .seq
                .0
                .checked_add(1)
                .map(ChangeSeq)
                .ok_or_else(|| WalBuildError::Codec("seq overflow".to_owned()))?;
            if record.plan.next_seq != expected {
                return Err(WalBuildError::NonContiguousSeq {
                    expected,
                    actual: record.plan.next_seq,
                });
            }
            if record.plan.base_head_seq != payload_records[index - 1].seq {
                return Err(WalBuildError::BaseHeadSeqMismatch {
                    request: record.plan.base_head_seq,
                    plan: payload_records[index - 1].seq,
                });
            }
        }
        payload_records.push(WalCommitPayload {
            namespace_id: record.plan.namespace_id.clone(),
            seq: record.plan.next_seq,
            base_head_seq: record.plan.base_head_seq,
            commit_id: record.plan.commit_id.clone(),
            request_id: record.request.request_id.clone(),
            request_checksum_sha256: record
                .request
                .request_checksum_sha256()
                .map_err(|err| WalBuildError::Codec(err.to_string()))?,
            semantic_fingerprint_sha256: record
                .request
                .semantic_fingerprint_sha256()
                .map_err(|err| WalBuildError::Codec(err.to_string()))?,
            source_request_checksum_sha256: record.request.source_request_checksum_sha256.clone(),
            writer_id: record.request.writer_id.clone(),
            writer_fence_token: record.request.writer_fence_token,
            message: record.request.message.clone(),
            annotations: record.request.annotations.clone(),
            ops: build_wal_ops(&record.request, &record.plan)?,
            preconditions: record
                .request
                .preconditions
                .iter()
                .map(WalPrecondition::from)
                .collect(),
            results: record.results.clone(),
        });
    }

    let start_seq = payload_records
        .first()
        .map(|record| record.seq)
        .ok_or(WalBuildError::EmptySegment)?;
    let end_seq = payload_records
        .last()
        .map(|record| record.seq)
        .ok_or(WalBuildError::EmptySegment)?;
    let segment_id = format!("seg_{}", Uuid::new_v4().simple());
    let payload = WalSegmentPayload {
        namespace_id,
        segment_id: segment_id.clone(),
        prev_visible_segment,
        base_head_seq: payload_records[0].base_head_seq,
        start_seq,
        end_seq,
        records: payload_records,
    };
    let envelope = WalSegmentEnvelope::from_payload(writer_version, payload)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let encoded_bytes = encode_wal_segment_envelope_zstd(&envelope)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let object_key = wal_segment(
        envelope.payload.namespace_id.as_str(),
        start_seq.0,
        end_seq.0,
        &segment_id,
    );

    Ok(PreparedWalSegment {
        object_key,
        segment_id,
        envelope,
        encoded_bytes,
        checked_invariants: vec![
            "wal_payload_checksum_matches_payload".to_owned(),
            "wal_key_matches_segment_seq_range".to_owned(),
            "head_publish_requires_durable_wal".to_owned(),
        ],
    })
}

pub fn replay_wal_segment(
    current_head: &HeadState,
    wal_object: &StoredWalObject,
) -> Result<ReplayedWalSegment, WalReplayError> {
    let envelope = decode_and_validate_replayed_wal(current_head, wal_object)?;
    let mut resulting_head = current_head.clone();
    for record in &envelope.payload.records {
        resulting_head.seq = record.seq;
        resulting_head.next_inode_id =
            replay_next_inode_id(resulting_head.next_inode_id, &record.ops);
    }
    resulting_head.visible_wal_tip = Some(envelope.pointer(wal_object.object_key.clone()));

    Ok(ReplayedWalSegment {
        object_key: wal_object.object_key.clone(),
        envelope,
        resulting_head,
        checked_invariants: vec![
            "wal_payload_checksum_matches_payload".to_owned(),
            "wal_key_matches_segment_seq_range".to_owned(),
            "wal_replay_requires_matching_namespace".to_owned(),
            "wal_replay_requires_matching_base_head_seq".to_owned(),
            "wal_tail_seq_is_contiguous".to_owned(),
        ],
    })
}

pub fn replay_wal_segment_with_metadata(
    current_head: &HeadState,
    current_metadata_state: &MetadataState,
    wal_object: &StoredWalObject,
) -> Result<ReplayedWalSegmentWithMetadata, WalReplayError> {
    let replayed = replay_wal_segment(current_head, wal_object)?;
    let mut current_metadata_state = current_metadata_state.clone();
    let mut checked_invariants = replayed.checked_invariants.clone();
    for record in &replayed.envelope.payload.records {
        let applied = current_metadata_state
            .apply_committed_wal_record(record)
            .map_err(WalReplayError::MetadataApply)?;
        current_metadata_state = applied.metadata_state;
        push_invariant(&mut checked_invariants, "wal_replay_applies_metadata_rows");
        extend_invariants(&mut checked_invariants, &applied.checked_invariants);
    }

    Ok(ReplayedWalSegmentWithMetadata {
        object_key: replayed.object_key,
        envelope: replayed.envelope,
        resulting_head: replayed.resulting_head,
        resulting_metadata_state: current_metadata_state,
        checked_invariants,
    })
}

pub fn replay_wal_tail(
    basis_head: &HeadState,
    wal_tail: &[StoredWalObject],
) -> Result<HeadState, WalReplayError> {
    let mut current_head = basis_head.clone();

    for wal_object in wal_tail {
        current_head = replay_wal_segment(&current_head, wal_object)?.resulting_head;
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
            replay_wal_segment_with_metadata(&current_head, &current_metadata_state, wal_object)?;
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

impl From<&PreparedWalSegment> for StoredWalObject {
    fn from(value: &PreparedWalSegment) -> Self {
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
        let resolved_restore_content_ref = plan
            .resolved_restore_content_refs
            .get(op_index as usize)
            .and_then(|content_ref| content_ref.as_ref());
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
                content_ref,
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
                content_ref: content_ref.clone(),
            },
            CommitOp::ReplaceFile {
                inode_id,
                base_revision,
                content_ref,
            } => WalOp::ReplaceFile {
                op_index,
                inode_id: *inode_id,
                base_revision: *base_revision,
                content_ref: content_ref.clone(),
            },
            CommitOp::RestoreRevision {
                inode_id,
                source_revision,
                base_revision,
            } => WalOp::RestoreRevision {
                op_index,
                inode_id: *inode_id,
                source_revision_no: *source_revision,
                base_revision: *base_revision,
                content_ref: resolved_restore_content_ref
                    .ok_or_else(|| {
                        WalBuildError::Codec(format!(
                            "missing resolved restore content ref for op index {op_index}"
                        ))
                    })?
                    .clone(),
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
        };
        wal_ops.push(wal_op);
    }

    Ok(wal_ops)
}

fn decode_and_validate_replayed_wal(
    current_head: &HeadState,
    wal_object: &StoredWalObject,
) -> Result<WalSegmentEnvelope, WalReplayError> {
    let envelope = decode_wal_segment_envelope_zstd(&wal_object.encoded_bytes)
        .map_err(|err| WalReplayError::Codec(err.to_string()))?;
    let expected_object_key = wal_segment(
        envelope.payload.namespace_id.as_str(),
        envelope.payload.start_seq.0,
        envelope.payload.end_seq.0,
        &envelope.payload.segment_id,
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

    let expected_start = current_head
        .seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(WalReplayError::SeqOverflow)?;

    if envelope.payload.start_seq != expected_start {
        return Err(WalReplayError::NonContiguousSeq {
            expected: expected_start,
            actual: envelope.payload.start_seq,
        });
    }
    if envelope.payload.records.is_empty() {
        return Err(WalReplayError::EmptySegment);
    }
    if envelope.payload.records.first().map(|record| record.seq) != Some(envelope.payload.start_seq)
        || envelope.payload.records.last().map(|record| record.seq)
            != Some(envelope.payload.end_seq)
    {
        return Err(WalReplayError::SegmentSummaryMismatch);
    }
    for (offset, record) in envelope.payload.records.iter().enumerate() {
        let expected = envelope
            .payload
            .start_seq
            .0
            .checked_add(offset as u64)
            .map(ChangeSeq)
            .ok_or(WalReplayError::SeqOverflow)?;
        if record.seq != expected {
            return Err(WalReplayError::NonContiguousSeq {
                expected,
                actual: record.seq,
            });
        }
        if record.namespace_id != current_head.namespace_id {
            return Err(WalReplayError::NamespaceMismatch {
                expected: current_head.namespace_id.clone(),
                actual: record.namespace_id.clone(),
            });
        }
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
            | WalOp::RestoreRevision { .. }
            | WalOp::DeleteFile { .. }
            | WalOp::Rename { .. }
            | WalOp::DeleteSubtree { .. } => next_inode_id,
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
