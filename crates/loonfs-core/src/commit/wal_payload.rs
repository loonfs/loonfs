//! Adds publication metadata to the validated deltas for the WAL payload.

use super::PreparedCommit;
use loonfs_api::wire::wal::{WalCommitPayload, WalInlineContent};

pub(crate) fn wal_payload_from_prepared_commit(commit: &PreparedCommit) -> WalCommitPayload {
    let prepared = &commit.commit;
    WalCommitPayload {
        committed_seq: prepared.assigned_seq,
        commit_id: prepared.commit_id.clone(),
        committed_by: prepared.actor_id.clone(),
        semantic_commit_fingerprint: prepared.semantic_identity.clone(),
        committed_at_ms: commit.committed_at_ms,
        message: prepared.message.clone(),
        inline_content: commit
            .inline_content
            .iter()
            .map(|value| WalInlineContent {
                content_id: value.content_ref().content_id.clone(),
                bytes: value.bytes().to_vec(),
            })
            .collect(),
        deltas: prepared.deltas.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{CommitFingerprint, CommitPlan};
    use loonfs_api::wire::wal::{WalCommitDelta, WalDelta};
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
            actor_id: loonfs_test_support::test_actor(),
            writer_epoch: WriterEpoch(1),
            message: Some("create docs".to_owned()),
            semantic_identity: test_fingerprint(),
            apply_after_seq: ChangeSeq(0),
            assigned_seq: ChangeSeq(1),
            deltas: vec![
                WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::CreateInode {
                        delta_index: 0,
                        inode_id: InodeId(2),
                        inode_kind: loonfs_api::InodeKind::Directory,
                    },
                },
                WalCommitDelta {
                    semantic_operation_index: 0,
                    delta: WalDelta::BindDirentry {
                        delta_index: 1,
                        parent_inode_id: InodeId(1),
                        name_key: NameKey::parse("docs").expect("valid name key"),
                        display_name: loonfs_api::DisplayName::parse("docs")
                            .expect("valid display name"),
                        child_inode_id: InodeId(2),
                    },
                },
            ],
            resulting_next_inode_id: InodeId(3),
        };
        let prepared = PreparedCommit {
            commit: plan,
            committed_at_ms: 4_200,
            inline_content: Vec::new(),
        };

        let payload = wal_payload_from_prepared_commit(&prepared);

        assert_eq!(payload.committed_seq, ChangeSeq(1));
        assert_eq!(payload.committed_at_ms, 4_200);
        assert_eq!(payload.deltas.len(), 2);

        let restamped = PreparedCommit {
            committed_at_ms: 9_900,
            ..prepared.clone()
        };
        assert_eq!(
            restamped.commit.semantic_identity,
            prepared.commit.semantic_identity
        );
        let restamped_payload = wal_payload_from_prepared_commit(&restamped);
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
