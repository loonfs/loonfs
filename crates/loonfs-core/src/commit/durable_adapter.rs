use super::MaterializedCommit;
use crate::wal::WalBuildError;
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload};

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
        seq: prepared.plan.assigned_seq,
        commit_id: prepared.plan.commit_id.clone(),
        semantic_commit_fingerprint: prepared.semantic_identity.as_str().to_owned(),
        message: prepared.request.message.clone(),
        deltas: commit
            .deltas
            .iter()
            .map(|delta| WalCommitDelta {
                semantic_op_index: delta.semantic_op_index,
                delta: delta.wal_delta.clone(),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        materialize_commit, CommitOp, CommitPlan, CommitRequest, PreparedCommit, ValidatedOp,
    };
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NamespaceId, WriterEpoch};

    #[test]
    fn durable_adapter_builds_expected_wal_payload() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_epoch: WriterEpoch(1),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode: InodeId(1),
                display_name: "docs".to_owned(),
            }],
            preconditions: Vec::new(),
            message: Some("create docs".to_owned()),
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

        assert_eq!(payload.seq, ChangeSeq(1));
        assert_eq!(payload.deltas.len(), 2);
    }
}
