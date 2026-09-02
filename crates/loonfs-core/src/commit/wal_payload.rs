//! Converts a materialized commit into the durable WAL payload shape.

use super::MaterializedCommit;
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload};

pub(crate) fn wal_payload_from_materialized_commit(
    commit: &MaterializedCommit,
) -> WalCommitPayload {
    let prepared = &commit.commit;
    WalCommitPayload {
        seq: prepared.assigned_seq,
        commit_id: prepared.commit_id.clone(),
        committed_by: prepared.actor.clone(),
        semantic_commit_fingerprint: prepared.semantic_identity.clone(),
        committed_at_ms: commit.committed_at_ms,
        message: prepared.message.clone(),
        deltas: commit
            .deltas
            .iter()
            .map(|delta| WalCommitDelta {
                semantic_op_index: delta.semantic_op_index,
                delta: delta.wal_delta.clone(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{materialize_commit, CommitFingerprint, CommitPlan, ValidatedOp};
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NameKey, NamespaceId, WriterEpoch};

    fn test_fingerprint() -> CommitFingerprint {
        serde_json::from_str(r#""v1:sha256:test""#).expect("fingerprint")
    }

    #[test]
    fn wal_payload_adapter_builds_expected_payload() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let plan = CommitPlan {
            namespace_id: namespace_id.clone(),
            commit_id: CommitId::parse("c_wal_payload").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            message: Some("create docs".to_owned()),
            semantic_identity: test_fingerprint(),
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
        let materialized = materialize_commit(plan, 4_200);

        let payload = wal_payload_from_materialized_commit(&materialized);

        assert_eq!(payload.seq, ChangeSeq(1));
        assert_eq!(payload.committed_at_ms, 4_200);
        assert_eq!(payload.deltas.len(), 2);

        // The stamp is observational: two materializations of one prepared
        // commit under different clocks share a semantic fingerprint, so
        // replay identity is untouched by wall time. Only the stamp differs
        // in the durable payload.
        let restamped = materialize_commit(materialized.commit.clone(), 9_900);
        assert_eq!(
            restamped.commit.semantic_identity,
            materialized.commit.semantic_identity
        );
        let restamped_payload = wal_payload_from_materialized_commit(&restamped);
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
