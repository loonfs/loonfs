use crate::commit::{CommitOp, CommitPlan, CommitRequest, Precondition};
use crate::metadata::{MetadataApplyError, MetadataState};
use loon_objectstore::keys::wal_commit;
use loon_types::{
    decode_wal_commit_envelope_zstd, encode_wal_commit_envelope_zstd, ChangeSeq, HeadState,
    InodeId, NamespaceId, WalCommitEnvelope, WalCommitPayload, WalOp, WalPrecondition,
};
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

#[cfg(test)]
fn with_create_ops(
    seq: ChangeSeq,
    base_head_seq: ChangeSeq,
    commit_id: &str,
    writer_fence_token: loon_types::FenceToken,
    ops: Vec<WalOp>,
) -> WalCommitPayload {
    WalCommitPayload {
        namespace_id: NamespaceId::from("ns-1"),
        seq,
        base_head_seq,
        commit_id: commit_id.to_owned(),
        request_id: commit_id.to_owned(),
        writer_id: "writer-a".to_owned(),
        writer_fence_token,
        ops,
        preconditions: vec![WalPrecondition::HeadSeqIs(base_head_seq)],
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

#[cfg(test)]
mod tests {
    use super::{
        prepare_wal_commit, replay_wal_commit, replay_wal_tail, with_create_ops, StoredWalObject,
        WalBuildError, WalReplayError,
    };
    use crate::commit::{
        build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, Precondition,
    };
    use crate::metadata::{InodeRecord, MetadataState, RevisionRecord};
    use loon_objectstore::keys::wal_commit;
    use loon_types::{
        decode_wal_commit_envelope_zstd, encode_wal_commit_envelope_zstd, ChangeSeq, FenceToken,
        HeadState, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo, WalCommitEnvelope,
        WalCommitPayload, WalEnvelopeKind, WalOp, WalPrecondition,
    };

    #[test]
    fn prepare_wal_commit_builds_immutable_object_key_and_payload() {
        let request = sample_request();
        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_metadata_state()),
        )
        .expect("validated commit plan");

        let prepared =
            prepare_wal_commit(&request, &plan, "loon-core-test").expect("prepare wal commit");

        assert_eq!(
            prepared.object_key,
            "namespaces/ns-1/wal/00000000000000000042-req-20260311-0001.cbor.zst"
        );
        assert_eq!(prepared.commit_id, "req-20260311-0001");
        assert!(prepared
            .checked_invariants
            .contains(&"head_publish_requires_durable_wal".to_owned()));
        assert_eq!(prepared.envelope.kind, WalEnvelopeKind::NamespaceWalCommit);
        assert_eq!(prepared.envelope.payload.seq, ChangeSeq(42));
        assert_eq!(prepared.envelope.payload.base_head_seq, ChangeSeq(41));
    }

    #[test]
    fn prepare_wal_commit_round_trips_through_shared_codec() {
        let request = sample_request();
        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_metadata_state()),
        )
        .expect("validated commit plan");

        let prepared =
            prepare_wal_commit(&request, &plan, "loon-core-test").expect("prepare wal commit");
        let decoded = decode_wal_commit_envelope_zstd(&prepared.encoded_bytes)
            .expect("decode shared WAL envelope");

        assert_eq!(decoded, prepared.envelope);
        assert_eq!(
            decoded.payload.ops,
            vec![WalOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }]
        );
        assert_eq!(
            decoded.payload.preconditions,
            vec![
                WalPrecondition::HeadSeqIs(ChangeSeq(41)),
                WalPrecondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(7),
                },
            ]
        );
    }

    #[test]
    fn prepare_wal_commit_embeds_allocated_inode_ids_for_create_ops() {
        let request = CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-create-file".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(902),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:child-note".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(902),
                    name_key: "note.txt".to_owned(),
                },
            ],
        };
        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, create_parent_metadata_state(InodeId(902))),
        )
        .expect("validated commit plan");

        let prepared =
            prepare_wal_commit(&request, &plan, "loon-core-test").expect("prepare wal commit");
        let decoded = decode_wal_commit_envelope_zstd(&prepared.encoded_bytes)
            .expect("decode shared WAL envelope");

        assert_eq!(
            decoded.payload.ops,
            vec![WalOp::CreateFile {
                op_index: 0,
                inode_id: InodeId(501),
                parent_inode: InodeId(902),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:child-note".to_owned(),
            }]
        );
    }

    #[test]
    fn prepare_wal_commit_rejects_namespace_mismatch() {
        let mut request = sample_request();
        let plan = build_commit_plan(
            &request,
            &validation_context_with_metadata(999, replace_metadata_state()),
        )
        .expect("validated commit plan");
        request.namespace_id = NamespaceId::from("ns-2");

        let error =
            prepare_wal_commit(&request, &plan, "loon-core-test").expect_err("should reject");

        assert_eq!(
            error,
            WalBuildError::NamespaceMismatch {
                request: NamespaceId::from("ns-2"),
                plan: NamespaceId::from("ns-1"),
            }
        );
    }

    #[test]
    fn replay_wal_commit_updates_head_for_contiguous_entry() {
        let request = sample_request();
        let context = validation_context_with_metadata(999, replace_metadata_state());
        let prepared = prepare_wal_commit(
            &request,
            &build_commit_plan(&request, &context).expect("validated commit plan"),
            "loon-core-test",
        )
        .expect("prepare wal commit");

        let replayed = replay_wal_commit(&context.head, &StoredWalObject::from(&prepared))
            .expect("replay should succeed");

        assert_eq!(replayed.resulting_head.seq, ChangeSeq(42));
        assert_eq!(replayed.resulting_head.active_fence_token, FenceToken(8));
        assert_eq!(replayed.resulting_head.next_inode_id, InodeId(501));
        assert!(replayed
            .checked_invariants
            .contains(&"wal_tail_seq_is_contiguous".to_owned()));
    }

    #[test]
    fn replay_wal_commit_advances_next_inode_id_for_create_ops() {
        let current_head = replay_basis_head();
        let wal_object = stored_wal_object(with_create_ops(
            ChangeSeq(41),
            ChangeSeq(40),
            "req-20260311-create-0001",
            FenceToken(8),
            vec![
                WalOp::CreateDir {
                    op_index: 0,
                    inode_id: InodeId(501),
                    parent_inode: InodeId(2),
                    display_name: "drafts".to_owned(),
                },
                WalOp::CreateFile {
                    op_index: 0,
                    inode_id: InodeId(502),
                    parent_inode: InodeId(501),
                    display_name: "note.txt".to_owned(),
                    content_manifest_digest: "sha256:child-note".to_owned(),
                },
            ],
        ));

        let replayed = replay_wal_commit(&current_head, &wal_object).expect("replay should work");

        assert_eq!(replayed.resulting_head.seq, ChangeSeq(41));
        assert_eq!(replayed.resulting_head.next_inode_id, InodeId(503));
    }

    #[test]
    fn replay_wal_tail_advances_head_through_multiple_entries() {
        let basis_head = HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let wal_tail = vec![
            stored_wal_object(sample_payload(
                ChangeSeq(41),
                ChangeSeq(40),
                "req-20260311-0001",
                FenceToken(8),
            )),
            stored_wal_object(sample_payload(
                ChangeSeq(42),
                ChangeSeq(41),
                "req-20260311-0002",
                FenceToken(9),
            )),
        ];

        let final_head =
            replay_wal_tail(&basis_head, &wal_tail).expect("tail replay should succeed");

        assert_eq!(final_head.seq, ChangeSeq(42));
        assert_eq!(final_head.active_fence_token, FenceToken(9));
        assert_eq!(final_head.next_inode_id, InodeId(501));
        assert_eq!(final_head.snapshot_hint_seq, Some(ChangeSeq(40)));
    }

    #[test]
    fn replay_wal_commit_rejects_namespace_mismatch() {
        let current_head = replay_basis_head();
        let wal_object = stored_wal_object(WalCommitPayload {
            namespace_id: NamespaceId::from("ns-2"),
            seq: ChangeSeq(41),
            base_head_seq: ChangeSeq(40),
            commit_id: "req-20260311-0001".to_owned(),
            request_id: "req-20260311-0001".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            ops: Vec::new(),
            preconditions: vec![WalPrecondition::HeadSeqIs(ChangeSeq(40))],
        });

        let error = replay_wal_commit(&current_head, &wal_object)
            .expect_err("should reject namespace mismatch");

        assert_eq!(
            error,
            WalReplayError::NamespaceMismatch {
                expected: NamespaceId::from("ns-1"),
                actual: NamespaceId::from("ns-2"),
            }
        );
    }

    #[test]
    fn replay_wal_commit_rejects_base_head_seq_mismatch() {
        let current_head = replay_basis_head();
        let wal_object = stored_wal_object(sample_payload(
            ChangeSeq(41),
            ChangeSeq(39),
            "req-20260311-0001",
            FenceToken(8),
        ));

        let error = replay_wal_commit(&current_head, &wal_object)
            .expect_err("should reject base head mismatch");

        assert_eq!(
            error,
            WalReplayError::BaseHeadSeqMismatch {
                expected: ChangeSeq(40),
                actual: ChangeSeq(39),
            }
        );
    }

    #[test]
    fn replay_wal_commit_rejects_seq_gap() {
        let current_head = replay_basis_head();
        let wal_object = stored_wal_object(sample_payload(
            ChangeSeq(42),
            ChangeSeq(40),
            "req-20260311-0001",
            FenceToken(8),
        ));

        let error =
            replay_wal_commit(&current_head, &wal_object).expect_err("should reject seq gap");

        assert_eq!(
            error,
            WalReplayError::NonContiguousSeq {
                expected: ChangeSeq(41),
                actual: ChangeSeq(42),
            }
        );
    }

    #[test]
    fn replay_wal_commit_rejects_tampered_payload() {
        let current_head = replay_basis_head();
        let payload = sample_payload(
            ChangeSeq(41),
            ChangeSeq(40),
            "req-20260311-0001",
            FenceToken(8),
        );
        let mut envelope = WalCommitEnvelope::from_payload("loon-core-test", payload.clone())
            .expect("build envelope");
        envelope.payload.seq = ChangeSeq(42);

        let wal_object = StoredWalObject {
            object_key: loon_objectstore::keys::wal_commit(
                payload.namespace_id.as_str(),
                payload.seq.0,
                &payload.commit_id,
            ),
            encoded_bytes: encode_wal_commit_envelope_zstd(&envelope)
                .expect("encode tampered wal object"),
        };

        let error =
            replay_wal_commit(&current_head, &wal_object).expect_err("tampering should fail");

        match error {
            WalReplayError::Codec(message) => {
                assert!(message.contains("checksum mismatch"));
            }
            other => panic!("expected codec error, got {other:?}"),
        }
    }

    fn sample_request() -> CommitRequest {
        CommitRequest {
            namespace_id: NamespaceId::from("ns-1"),
            request_id: "req-20260311-0001".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(ChangeSeq(41)),
                Precondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(7),
                },
            ],
        }
    }

    fn sample_payload(
        seq: ChangeSeq,
        base_head_seq: ChangeSeq,
        commit_id: &str,
        writer_fence_token: FenceToken,
    ) -> WalCommitPayload {
        with_create_ops(
            seq,
            base_head_seq,
            commit_id,
            writer_fence_token,
            vec![WalOp::DeleteSubtree {
                op_index: 0,
                root_inode: InodeId(2),
            }],
        )
    }

    fn stored_wal_object(payload: WalCommitPayload) -> StoredWalObject {
        let object_key = wal_commit(
            payload.namespace_id.as_str(),
            payload.seq.0,
            &payload.commit_id,
        );
        let envelope =
            WalCommitEnvelope::from_payload("loon-core-test", payload).expect("build envelope");
        let encoded_bytes =
            encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");

        StoredWalObject {
            object_key,
            encoded_bytes,
        }
    }

    fn replay_basis_head() -> HeadState {
        HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        }
    }

    fn validation_context_with_metadata(
        now_ms: u64,
        metadata_state: MetadataState,
    ) -> CommitValidationContext {
        CommitValidationContext {
            head: HeadState {
                namespace_id: NamespaceId::from("ns-1"),
                seq: ChangeSeq(41),
                active_fence_token: FenceToken(8),
                next_inode_id: InodeId(501),
                snapshot_hint_seq: Some(ChangeSeq(40)),
                retention_floor_seq: ChangeSeq(40),
            },
            lease: LeaseState {
                namespace_id: NamespaceId::from("ns-1"),
                holder_id: "writer-a".to_owned(),
                fence_token: FenceToken(8),
                lease_expires_at_ms: 1_000,
            },
            now_ms,
            metadata_state,
        }
    }

    fn create_parent_metadata_state(parent_inode: InodeId) -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: parent_inode,
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            ..MetadataState::default()
        }
    }

    fn replace_metadata_state() -> MetadataState {
        MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(7),
                committed_seq: ChangeSeq(7),
                revision_op_index: 0,
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            ..MetadataState::default()
        }
    }
}
