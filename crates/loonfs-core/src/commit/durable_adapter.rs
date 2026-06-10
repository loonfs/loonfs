use super::{MaterializedCommit, Precondition};
use crate::wal::WalBuildError;
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalPrecondition};

pub(crate) fn wal_payload_from_materialized_commit(
    commit: &MaterializedCommit,
) -> Result<WalCommitPayload, WalBuildError> {
    let prepared = &commit.prepared;
    if prepared.request.namespace_id != prepared.plan.namespace_id {
        return Err(WalBuildError::NamespaceMismatch {
            request: prepared.request.namespace_id.clone(),
            plan: prepared.plan.namespace_id.clone(),
        });
    }

    Ok(WalCommitPayload {
        namespace_id: prepared.plan.namespace_id.clone(),
        seq: prepared.plan.assigned_seq,
        apply_after_seq: prepared.plan.apply_after_seq,
        commit_id: prepared.plan.commit_id.clone(),
        semantic_commit_fingerprint: prepared.semantic_identity.as_str().to_owned(),
        writer_id: prepared.request.writer_id.clone(),
        writer_fence_token: prepared.request.writer_fence_token,
        message: prepared.request.message.clone(),
        annotations: prepared.request.annotations.clone(),
        deltas: commit
            .deltas
            .iter()
            .map(|delta| WalCommitDelta {
                semantic_op_index: delta.semantic_op_index,
                delta: delta.wal_delta.clone(),
            })
            .collect(),
        preconditions: prepared
            .request
            .preconditions
            .iter()
            .map(WalPrecondition::from)
            .collect(),
        results: commit.results.clone(),
    })
}

impl From<&Precondition> for WalPrecondition {
    fn from(value: &Precondition) -> Self {
        match value {
            Precondition::InodeRevisionIs {
                inode_id,
                revision_no,
            } => Self::InodeRevisionIs {
                inode_id: *inode_id,
                revision_no: *revision_no,
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
            Precondition::BindingIs {
                parent_inode,
                name_key,
                child_inode,
                bind_seq,
                bind_delta_index,
            } => Self::BindingIs {
                parent_inode: *parent_inode,
                name_key: name_key.clone(),
                child_inode: *child_inode,
                bind_seq: *bind_seq,
                bind_delta_index: *bind_delta_index,
            },
            Precondition::DirectoryEmpty { inode_id } => Self::DirectoryEmpty {
                inode_id: *inode_id,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        materialize_commit, CommitOp, CommitPlan, CommitRequest, PreparedCommit, ValidatedOp,
    };
    use loonfs_api::{ChangeSeq, CommitId, FenceToken, InodeId, NamespaceId};

    #[test]
    fn durable_adapter_builds_expected_wal_payload() {
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
            validated_ops: vec![ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
                name_key: "docs".to_owned(),
                child_inode: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
            checked_invariants: Vec::new(),
        };
        let prepared = PreparedCommit::new(request, plan).expect("prepare commit");
        let materialized = materialize_commit(prepared);

        let payload =
            wal_payload_from_materialized_commit(&materialized).expect("build wal payload");

        assert_eq!(payload.namespace_id, namespace_id);
        assert_eq!(payload.seq, ChangeSeq(1));
        assert_eq!(payload.deltas.len(), 2);
        assert_eq!(payload.results.len(), 1);
    }
}
