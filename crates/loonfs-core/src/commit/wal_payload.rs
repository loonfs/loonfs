//! Builds the WAL record of a planned commit.

use super::CommitPlan;
use loonfs_types::format::wal::{WalCommitPayload, WalInlineContent};

pub(crate) fn wal_payload_from_commit_plan(plan: &CommitPlan) -> WalCommitPayload {
    WalCommitPayload {
        committed_seq: plan.assigned_seq,
        commit_id: plan.commit_id.clone(),
        committed_by: plan.actor_id.clone(),
        semantic_commit_fingerprint: plan.semantic_identity.clone(),
        committed_at_ms: plan.committed_at_ms,
        message: plan.message.clone(),
        inline_content: plan
            .inline_content
            .iter()
            .map(|value| WalInlineContent {
                content_id: value.content_ref().content_id.clone(),
                offset: 0,
                bytes: value.bytes().to_vec(),
            })
            .chain(plan.appended.iter().cloned())
            .collect(),
        deltas: plan.deltas.clone(),
    }
}
