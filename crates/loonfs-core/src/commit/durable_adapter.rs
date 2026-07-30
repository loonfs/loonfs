//! Converts a materialized commit into the durable WAL payload shape.

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
        committed_at_ms: commit.committed_at_ms,
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
        materialize_commit, CommitFingerprint, CommitIr, CommitOp, CommitPlan, PlannedOp,
        PreparedCommit, ValidatedOp,
    };
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey, NamespaceId, WriterEpoch};

    fn test_fingerprint() -> CommitFingerprint {
        CommitFingerprint::new_unchecked("v0:sha256:test".to_owned())
    }

    #[test]
    fn durable_adapter_builds_expected_wal_payload() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let request = CommitIr {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            writer_epoch: WriterEpoch(1),
            ops: vec![PlannedOp::unchecked(CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            })],
            message: Some("create docs".to_owned()),
        };
        let plan = CommitPlan {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            validated_ops: vec![ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
                name_key: NameKey::parse("docs").expect("valid name key"),
                child_inode_id: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
            resulting_next_inode_id: InodeId(3),
        };
        let prepared =
            PreparedCommit::new(request, plan, test_fingerprint()).expect("prepare commit");
        let materialized = materialize_commit(prepared, 4_200);

        let payload =
            wal_payload_from_materialized_commit(&materialized).expect("build wal payload");

        assert_eq!(payload.seq, ChangeSeq(1));
        assert_eq!(payload.committed_at_ms, 4_200);
        assert_eq!(payload.deltas.len(), 2);

        // The stamp is observational: two materializations of one prepared
        // commit under different clocks share a semantic fingerprint, so
        // replay identity is untouched by wall time. Only the stamp differs
        // in the durable payload.
        let restamped = materialize_commit(materialized.prepared.clone(), 9_900);
        assert_eq!(
            restamped.prepared.semantic_identity,
            materialized.prepared.semantic_identity
        );
        let restamped_payload =
            wal_payload_from_materialized_commit(&restamped).expect("build wal payload");
        assert_eq!(restamped_payload.committed_at_ms, 9_900);
        assert_eq!(
            restamped_payload.semantic_commit_fingerprint,
            payload.semantic_commit_fingerprint
        );
        let mut normalized = restamped_payload.clone();
        normalized.committed_at_ms = payload.committed_at_ms;
        assert_eq!(normalized, payload);
    }
}
