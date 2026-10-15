//! Adds publication metadata to the validated deltas for the WAL payload.

use super::PreparedCommit;
use loonfs_types::format::wal::{WalCommitPayload, WalInlineContent};

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
                offset: 0,
                bytes: value.bytes().to_vec(),
            })
            .chain(prepared.appended.iter().cloned())
            .collect(),
        deltas: prepared.deltas.clone(),
    }
}
