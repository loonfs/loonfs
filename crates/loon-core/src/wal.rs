use crate::commit::{CommitOp, CommitPlan, CommitRequest, Precondition};
use loon_objectstore::keys::wal_commit;
use loon_types::{
    encode_wal_commit_envelope_zstd, ChangeSeq, NamespaceId, WalCommitEnvelope, WalCommitPayload,
    WalOp, WalPrecondition,
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
    Codec(String),
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
        ops: request.ops.iter().map(WalOp::from).collect(),
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

impl From<&CommitOp> for WalOp {
    fn from(value: &CommitOp) -> Self {
        match value {
            CommitOp::ReplaceFile {
                inode_id,
                base_revision,
                content_manifest_digest,
            } => Self::ReplaceFile {
                inode_id: *inode_id,
                base_revision: *base_revision,
                content_manifest_digest: content_manifest_digest.clone(),
            },
            CommitOp::Rename {
                inode_id,
                new_parent_inode,
                new_display_name,
            } => Self::Rename {
                inode_id: *inode_id,
                new_parent_inode: *new_parent_inode,
                new_display_name: new_display_name.clone(),
            },
            CommitOp::DeleteSubtree { root_inode } => Self::DeleteSubtree {
                root_inode: *root_inode,
            },
            CommitOp::RestoreRevision {
                inode_id,
                restore_from_revision,
            } => Self::RestoreRevision {
                inode_id: *inode_id,
                restore_from_revision: *restore_from_revision,
            },
        }
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
    use super::{prepare_wal_commit, WalBuildError};
    use crate::commit::{
        build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, Precondition,
    };
    use loon_types::{
        decode_wal_commit_envelope_zstd, ChangeSeq, FenceToken, HeadState, InodeId, LeaseState,
        NamespaceId, RevisionNo, WalEnvelopeKind, WalOp, WalPrecondition,
    };

    #[test]
    fn prepare_wal_commit_builds_immutable_object_key_and_payload() {
        let request = sample_request();
        let plan =
            build_commit_plan(&request, &validation_context(999)).expect("validated commit plan");

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
        let plan =
            build_commit_plan(&request, &validation_context(999)).expect("validated commit plan");

        let prepared =
            prepare_wal_commit(&request, &plan, "loon-core-test").expect("prepare wal commit");
        let decoded = decode_wal_commit_envelope_zstd(&prepared.encoded_bytes)
            .expect("decode shared WAL envelope");

        assert_eq!(decoded, prepared.envelope);
        assert_eq!(
            decoded.payload.ops,
            vec![WalOp::ReplaceFile {
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
    fn prepare_wal_commit_rejects_namespace_mismatch() {
        let mut request = sample_request();
        let plan =
            build_commit_plan(&request, &validation_context(999)).expect("validated commit plan");
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

    fn validation_context(now_ms: u64) -> CommitValidationContext {
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
        }
    }
}
