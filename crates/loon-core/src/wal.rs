use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::metadata::{MetadataApplyError, MetadataState};
use loon_api::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, generate_wal_segment_id,
    validate_wal_segment_id, ChangeSeq, HeadState, InodeId, NamespaceId, WalCommitDelta,
    WalCommitPayload, WalDelta, WalSegmentEnvelope, WalSegmentPayload, WalSegmentPointer,
};
use loon_objectstore::keys::wal_segment;
use loon_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[derive(Debug, Clone)]
pub(crate) struct WalChainLoadRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) chain_base_seq: ChangeSeq,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) visible_tip: Option<WalSegmentPointer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedWalSegment {
    object_key: String,
    envelope: WalSegmentEnvelope,
}

impl ValidatedWalSegment {
    pub(crate) fn object_key(&self) -> &str {
        &self.object_key
    }

    pub(crate) fn envelope(&self) -> &WalSegmentEnvelope {
        &self.envelope
    }

    pub(crate) fn records(&self) -> &[WalCommitPayload] {
        &self.envelope.payload.records
    }

    fn pointer(&self) -> WalSegmentPointer {
        self.envelope.pointer(self.object_key.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedWalChain {
    segments: Vec<ValidatedWalSegment>,
    checked_invariants: Vec<String>,
}

impl ValidatedWalChain {
    pub(crate) fn segments(&self) -> &[ValidatedWalSegment] {
        &self.segments
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum WalChainLoadError {
    #[error("invalid WAL chain seq range: base `{chain_base_seq:?}` is after head `{head_seq:?}`")]
    InvalidSeqRange {
        chain_base_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    #[error("missing visible WAL tip for seq `{seq:?}` under `{prefix}`")]
    MissingVisibleTip { prefix: String, seq: ChangeSeq },
    #[error("visible WAL tip ends at `{actual:?}`, expected head seq `{expected:?}`")]
    TipEndSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("failed to read WAL object `{object_key}`: {message}")]
    ReadWal { object_key: String, message: String },
    #[error("missing WAL object `{object_key}`")]
    MissingWalObject { object_key: String },
    #[error("WAL pointer does not match segment payload for `{object_key}`")]
    PointerMismatch { object_key: String },
    #[error(
        "WAL chain does not reach expected head seq: expected `{expected:?}`, actual `{actual:?}`"
    )]
    HeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("wal replay validation failed: {0:?}")]
    Replay(WalReplayError),
}

impl From<WalReplayError> for WalChainLoadError {
    fn from(value: WalReplayError) -> Self {
        Self::Replay(value)
    }
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

pub(crate) fn load_validated_wal_chain<S: ObjectStore + ?Sized>(
    store: &S,
    request: WalChainLoadRequest<'_>,
) -> Result<ValidatedWalChain, WalChainLoadError> {
    if request.chain_base_seq > request.head_seq {
        return Err(WalChainLoadError::InvalidSeqRange {
            chain_base_seq: request.chain_base_seq,
            head_seq: request.head_seq,
        });
    }
    if request.chain_base_seq == request.head_seq {
        return Ok(ValidatedWalChain {
            segments: Vec::new(),
            checked_invariants: Vec::new(),
        });
    }

    let prefix = format!("namespaces/{}/wal/", request.namespace_id.as_str());
    let mut pointer = request
        .visible_tip
        .clone()
        .ok_or(WalChainLoadError::MissingVisibleTip {
            prefix,
            seq: request.head_seq,
        })?;
    if pointer.end_seq != request.head_seq {
        return Err(WalChainLoadError::TipEndSeqMismatch {
            expected: request.head_seq,
            actual: pointer.end_seq,
        });
    }

    let mut reversed = Vec::new();
    loop {
        if pointer.end_seq <= request.chain_base_seq {
            break;
        }

        let object_key = pointer.object_key.clone();
        let encoded_bytes = store
            .get(&object_key, None)
            .map_err(|err| WalChainLoadError::ReadWal {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| WalChainLoadError::MissingWalObject {
                object_key: object_key.clone(),
            })?;
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| WalReplayError::Codec(err.to_string()))?;
        validate_pointer_matches_envelope(&pointer, &object_key, &envelope)?;

        let prev = envelope.payload.prev_visible_segment.clone();
        reversed.push(ValidatedWalSegment {
            object_key,
            envelope,
        });

        if reversed
            .last()
            .map(|segment| segment.envelope.payload.base_head_seq <= request.chain_base_seq)
            .unwrap_or(false)
        {
            break;
        }

        pointer = prev.ok_or(WalReplayError::SegmentSummaryMismatch)?;
    }

    reversed.reverse();

    let mut expected_base_seq = request.chain_base_seq;
    let mut checked_invariants = Vec::new();
    for segment in &reversed {
        validate_decoded_replayed_wal(
            request.namespace_id,
            expected_base_seq,
            segment.object_key(),
            segment.envelope(),
        )?;
        expected_base_seq = segment.envelope.payload.end_seq;
        extend_wal_replay_invariants(&mut checked_invariants);
    }

    if expected_base_seq != request.head_seq {
        return Err(WalChainLoadError::HeadSeqMismatch {
            expected: request.head_seq,
            actual: expected_base_seq,
        });
    }

    Ok(ValidatedWalChain {
        segments: reversed,
        checked_invariants,
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
            replay_next_inode_id_from_commit_deltas(resulting_head.next_inode_id, &record.deltas);
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

pub(crate) fn replay_validated_wal_tail_with_metadata(
    basis_head: &HeadState,
    basis_metadata_state: &MetadataState,
    wal_tail: &[ValidatedWalSegment],
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut current_head = basis_head.clone();
    let mut current_metadata_state = basis_metadata_state.clone();
    let mut checked_invariants = Vec::new();

    for wal_segment in wal_tail {
        validate_decoded_replayed_wal(
            &current_head.namespace_id,
            current_head.seq,
            wal_segment.object_key(),
            wal_segment.envelope(),
        )?;
        for record in wal_segment.records() {
            current_head.seq = record.seq;
            current_head.next_inode_id =
                replay_next_inode_id_from_commit_deltas(current_head.next_inode_id, &record.deltas);
            let applied = current_metadata_state
                .apply_committed_wal_record(record)
                .map_err(WalReplayError::MetadataApply)?;
            current_metadata_state = applied.metadata_state;
            push_invariant(&mut checked_invariants, "wal_replay_applies_metadata_rows");
            extend_invariants(&mut checked_invariants, &applied.checked_invariants);
        }
        current_head.visible_wal_tip = Some(wal_segment.pointer());
        extend_wal_replay_invariants(&mut checked_invariants);
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
    validate_decoded_replayed_wal(
        &current_head.namespace_id,
        current_head.seq,
        &wal_object.object_key,
        &envelope,
    )?;

    Ok(envelope)
}

fn validate_pointer_matches_envelope(
    pointer: &WalSegmentPointer,
    object_key: &str,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalChainLoadError> {
    if envelope.pointer(object_key.to_owned()) != *pointer {
        return Err(WalChainLoadError::PointerMismatch {
            object_key: object_key.to_owned(),
        });
    }
    Ok(())
}

fn validate_decoded_replayed_wal(
    expected_namespace: &NamespaceId,
    expected_base_head_seq: ChangeSeq,
    object_key: &str,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalReplayError> {
    validate_wal_segment_id(&envelope.payload.segment_id)
        .map_err(|err| WalReplayError::Codec(err.to_string()))?;
    let expected_object_key = wal_segment(
        envelope.payload.namespace_id.as_str(),
        envelope.payload.start_seq.0,
        envelope.payload.end_seq.0,
        &envelope.payload.segment_id,
    );

    if object_key != expected_object_key {
        return Err(WalReplayError::ObjectKeyMismatch {
            expected: expected_object_key,
            actual: object_key.to_owned(),
        });
    }

    if &envelope.payload.namespace_id != expected_namespace {
        return Err(WalReplayError::NamespaceMismatch {
            expected: expected_namespace.clone(),
            actual: envelope.payload.namespace_id.clone(),
        });
    }

    if envelope.payload.base_head_seq != expected_base_head_seq {
        return Err(WalReplayError::BaseHeadSeqMismatch {
            expected: expected_base_head_seq,
            actual: envelope.payload.base_head_seq,
        });
    }

    let expected_start = expected_base_head_seq
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
        if &record.namespace_id != expected_namespace {
            return Err(WalReplayError::NamespaceMismatch {
                expected: expected_namespace.clone(),
                actual: record.namespace_id.clone(),
            });
        }
    }

    Ok(())
}

fn extend_wal_replay_invariants(checked_invariants: &mut Vec<String>) {
    for invariant in [
        "wal_payload_checksum_matches_payload",
        "wal_key_matches_segment_seq_range",
        "wal_replay_requires_matching_namespace",
        "wal_replay_requires_matching_base_head_seq",
        "wal_tail_seq_is_contiguous",
    ] {
        push_invariant(checked_invariants, invariant);
    }
}

fn replay_next_inode_id(current_next_inode_id: InodeId, deltas: &[WalDelta]) -> InodeId {
    deltas
        .iter()
        .fold(current_next_inode_id, |next_inode_id, delta| match delta {
            WalDelta::CreateInode { inode_id, .. } => {
                InodeId(next_inode_id.0.max(inode_id.0.saturating_add(1)))
            }
            WalDelta::BindDirentry { .. }
            | WalDelta::UnbindDirentry { .. }
            | WalDelta::AppendFileRevision { .. }
            | WalDelta::TombstoneSubtree { .. } => next_inode_id,
        })
}

fn replay_next_inode_id_from_commit_deltas(
    current_next_inode_id: InodeId,
    deltas: &[WalCommitDelta],
) -> InodeId {
    deltas.iter().fold(current_next_inode_id, |next, delta| {
        replay_next_inode_id(next, std::slice::from_ref(&delta.delta))
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
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[test]
    fn build_wal_record_payload_matches_segment_record_payload() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
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
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resolved_source_bindings: vec![None],
            resulting_next_inode_id: InodeId(3),
            name_policy: loon_api::NamePolicy::default(),
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
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
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

    #[test]
    fn validated_wal_chain_loads_visible_segments_in_ascending_order() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let segment = prepare_wal_segment(
            namespace_id.clone(),
            None,
            &[materialized_create_dir(
                &namespace_id,
                "c_wal_chain_a",
                "alpha",
                ChangeSeq(0),
                ChangeSeq(1),
            )],
            "test-writer",
        )
        .expect("prepare wal segment");
        store
            .put_if_absent(&segment.object_key, &segment.encoded_bytes)
            .expect("write wal segment");

        let chain = load_validated_wal_chain(
            &store,
            WalChainLoadRequest {
                namespace_id: &namespace_id,
                chain_base_seq: ChangeSeq(0),
                head_seq: ChangeSeq(1),
                visible_tip: Some(segment.envelope.pointer(segment.object_key.clone())),
            },
        )
        .expect("load valid chain");

        assert_eq!(chain.segments().len(), 1);
        assert_eq!(chain.segments()[0].records()[0].seq, ChangeSeq(1));
    }

    #[test]
    fn validated_wal_chain_rejects_corrupt_visible_segments() {
        assert_wal_chain_corruption_rejected(|object_key, _envelope, pointer| {
            *object_key = wal_segment("demo", 1, 1, "seg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            pointer.object_key = object_key.clone();
        });
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            envelope.payload.namespace_id =
                NamespaceId::parse("other").expect("valid namespace id");
            rewrap_envelope(envelope);
            *object_key = wal_segment(
                envelope.payload.namespace_id.as_str(),
                envelope.payload.start_seq.0,
                envelope.payload.end_seq.0,
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        });
        assert_wal_chain_corruption_rejected(|_object_key, envelope, _pointer| {
            envelope.payload.segment_id = "seg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
            rewrap_envelope(envelope);
        });
        assert_wal_chain_corruption_rejected(|_object_key, _envelope, pointer| {
            pointer.payload_checksum_sha256 = "sha256:not-the-payload".to_owned();
        });
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            envelope.payload.records.clear();
            rewrap_envelope(envelope);
            *pointer = envelope.pointer(object_key.clone());
        });
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            envelope.payload.end_seq = ChangeSeq(2);
            rewrap_envelope(envelope);
            *object_key = wal_segment(
                envelope.payload.namespace_id.as_str(),
                envelope.payload.start_seq.0,
                envelope.payload.end_seq.0,
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        });
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            let mut skipped = envelope.payload.records[0].clone();
            skipped.seq = ChangeSeq(3);
            skipped.apply_after_seq = ChangeSeq(1);
            envelope.payload.records.push(skipped);
            envelope.payload.end_seq = ChangeSeq(3);
            rewrap_envelope(envelope);
            *object_key = wal_segment(
                envelope.payload.namespace_id.as_str(),
                envelope.payload.start_seq.0,
                envelope.payload.end_seq.0,
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        });
        assert_wal_chain_corruption_rejected(|object_key, envelope, pointer| {
            envelope.payload.base_head_seq = ChangeSeq(1);
            envelope.payload.start_seq = ChangeSeq(2);
            envelope.payload.end_seq = ChangeSeq(2);
            envelope.payload.records[0].seq = ChangeSeq(2);
            rewrap_envelope(envelope);
            *object_key = wal_segment(
                envelope.payload.namespace_id.as_str(),
                envelope.payload.start_seq.0,
                envelope.payload.end_seq.0,
                &envelope.payload.segment_id,
            );
            *pointer = envelope.pointer(object_key.clone());
        });
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
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
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
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            apply_after_seq,
            assigned_seq,
            allocated_inode_ids: vec![InodeId(2)],
            resolved_restore_content_refs: vec![None],
            resolved_source_bindings: vec![None],
            resulting_next_inode_id: InodeId(3),
            name_policy: loon_api::NamePolicy::default(),
            metadata_preconditions: Vec::new(),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        materialize_commit(prepared).expect("materialize commit")
    }

    fn assert_wal_chain_corruption_rejected(
        corrupt: impl FnOnce(&mut String, &mut WalSegmentEnvelope, &mut WalSegmentPointer),
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let segment = prepare_wal_segment(
            namespace_id.clone(),
            None,
            &[materialized_create_dir(
                &namespace_id,
                "c_wal_corrupt",
                "docs",
                ChangeSeq(0),
                ChangeSeq(1),
            )],
            "test-writer",
        )
        .expect("prepare wal segment");
        let mut object_key = segment.object_key;
        let mut envelope = segment.envelope;
        let mut pointer = envelope.pointer(object_key.clone());

        corrupt(&mut object_key, &mut envelope, &mut pointer);

        let encoded =
            encode_wal_segment_envelope_zstd(&envelope).expect("encode corrupted envelope");
        store
            .put_if_absent(&object_key, &encoded)
            .expect("write corrupted wal segment");

        load_validated_wal_chain(
            &store,
            WalChainLoadRequest {
                namespace_id: &namespace_id,
                chain_base_seq: ChangeSeq(0),
                head_seq: pointer.end_seq,
                visible_tip: Some(pointer),
            },
        )
        .expect_err("corrupted WAL chain should be rejected");
    }

    fn rewrap_envelope(envelope: &mut WalSegmentEnvelope) {
        *envelope = WalSegmentEnvelope::from_payload(
            envelope.writer_version.clone(),
            envelope.payload.clone(),
        )
        .expect("rewrap wal envelope");
    }
}
