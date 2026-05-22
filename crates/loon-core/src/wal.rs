use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, generate_wal_segment_id,
    validate_wal_segment_id, ChangeSeq, HeadState, InodeId, NamespaceId, WalCommitPayload, WalOp,
    WalSegmentEnvelope, WalSegmentPayload, WalSegmentPointer,
};
use loon_objectstore::keys::wal_segment;
use serde::{Deserialize, Serialize};

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
    EmptySegment,
    NamespaceMismatch {
        request: NamespaceId,
        plan: NamespaceId,
    },
    BaseHeadSeqMismatch {
        request: ChangeSeq,
        plan: ChangeSeq,
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
    records: &[MaterializedCommit],
    writer_version: &str,
) -> Result<PreparedWalSegment, WalBuildError> {
    if writer_version.trim().is_empty() {
        return Err(WalBuildError::EmptyWriterVersion);
    }
    if records.is_empty() {
        return Err(WalBuildError::EmptySegment);
    }

    let mut payload_records: Vec<WalCommitPayload> = Vec::with_capacity(records.len());
    for record in records {
        let payload_record = wal_payload_from_materialized_commit(record)?;
        if payload_record.namespace_id != namespace_id {
            return Err(WalBuildError::NamespaceMismatch {
                request: payload_record.namespace_id.clone(),
                plan: namespace_id.clone(),
            });
        }
        if let Some(previous) = payload_records.last() {
            let expected = previous
                .seq
                .0
                .checked_add(1)
                .map(ChangeSeq)
                .ok_or_else(|| WalBuildError::Codec("seq overflow".to_owned()))?;
            if payload_record.seq != expected {
                return Err(WalBuildError::NonContiguousSeq {
                    expected,
                    actual: payload_record.seq,
                });
            }
            if payload_record.apply_after_seq != previous.seq {
                return Err(WalBuildError::BaseHeadSeqMismatch {
                    request: payload_record.apply_after_seq,
                    plan: previous.seq,
                });
            }
        }
        payload_records.push(payload_record);
    }

    let start_seq = payload_records
        .first()
        .map(|record| record.seq)
        .ok_or(WalBuildError::EmptySegment)?;
    let end_seq = payload_records
        .last()
        .map(|record| record.seq)
        .ok_or(WalBuildError::EmptySegment)?;
    // WAL segment IDs are collision-resistant namespace-incarnation IDs. They
    // are intentionally not derived from the seq range, so losing writers can
    // leave harmless orphan segments without creating reusable object names.
    let segment_id = generate_wal_segment_id();
    let payload = WalSegmentPayload {
        namespace_id,
        segment_id: segment_id.clone(),
        prev_visible_segment,
        base_head_seq: payload_records[0].apply_after_seq,
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

fn decode_and_validate_replayed_wal(
    current_head: &HeadState,
    wal_object: &StoredWalObject,
) -> Result<WalSegmentEnvelope, WalReplayError> {
    let envelope = decode_wal_segment_envelope_zstd(&wal_object.encoded_bytes)
        .map_err(|err| WalReplayError::Codec(err.to_string()))?;
    validate_wal_segment_id(&envelope.payload.segment_id)
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
            WalOp::CreateInode { inode_id, .. } => {
                InodeId(next_inode_id.0.max(inode_id.0.saturating_add(1)))
            }
            WalOp::BindDirentry { .. }
            | WalOp::UnbindDirentry { .. }
            | WalOp::AppendFileRevision { .. }
            | WalOp::TombstoneSubtree { .. } => next_inode_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{materialize_commit, CommitOp, CommitPlan, CommitRequest, PreparedCommit};
    use loon_api::{CommitId, FenceToken};

    #[test]
    fn build_wal_record_payload_matches_segment_record_payload() {
        let namespace_id = NamespaceId::from("demo");
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from("c_wal_payload"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: Some("create docs".to_owned()),
            annotations: None,
        };
        let plan = CommitPlan {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from("c_wal_payload"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resolved_source_bindings: vec![None],
            resulting_next_inode_id: InodeId(3),
            metadata_preconditions: Vec::new(),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        let record = materialize_commit(prepared).expect("materialize commit");

        let segment = prepare_wal_segment(
            namespace_id,
            None,
            std::slice::from_ref(&record),
            "test-writer",
        )
        .expect("prepare wal segment");
        let payload = wal_payload_from_materialized_commit(&record).expect("build commit payload");

        assert_eq!(payload, segment.envelope.payload.records[0]);
    }

    #[test]
    fn prepared_wal_segments_use_unique_segment_ids_and_object_keys() {
        let namespace_id = NamespaceId::from("demo");
        let record = materialized_create_dir(
            &namespace_id,
            "c_wal_unique",
            "docs",
            ChangeSeq(0),
            ChangeSeq(1),
        );

        let first = prepare_wal_segment(
            namespace_id.clone(),
            None,
            std::slice::from_ref(&record),
            "test-writer",
        )
        .expect("prepare first wal segment");
        let second = prepare_wal_segment(
            namespace_id,
            None,
            std::slice::from_ref(&record),
            "test-writer",
        )
        .expect("prepare second wal segment");

        assert_ne!(first.segment_id, second.segment_id);
        assert_ne!(first.object_key, second.object_key);
        validate_wal_segment_id(&first.segment_id).expect("first segment id shape");
        validate_wal_segment_id(&second.segment_id).expect("second segment id shape");
    }

    fn materialized_create_dir(
        namespace_id: &NamespaceId,
        commit_id: &str,
        display_name: &str,
        apply_after_seq: ChangeSeq,
        assigned_seq: ChangeSeq,
    ) -> MaterializedCommit {
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from(commit_id),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(1),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: display_name.to_owned(),
            }],
            preconditions: Vec::new(),
            message: None,
            annotations: None,
        };
        let plan = CommitPlan {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::from(commit_id),
            apply_after_seq,
            assigned_seq,
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resolved_source_bindings: vec![None],
            resulting_next_inode_id: InodeId(3),
            metadata_preconditions: Vec::new(),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        materialize_commit(prepared).expect("materialize commit")
    }
}
