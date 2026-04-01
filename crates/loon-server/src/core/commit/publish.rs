use super::{push_unique_invariant, CommitHeadPublishError, CommitPlan, PreparedCommitHeadPublish};
use crate::objectstore::error::ObjectStoreError;
use crate::objectstore::keys::namespace_head;
use crate::objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{ChangeSeq, ControlObjectKind, HeadState, HeadStateEnvelope};

pub fn prepare_commit_head_publish(
    current_head: &HeadState,
    plan: &CommitPlan,
    writer_version: &str,
) -> Result<PreparedCommitHeadPublish, CommitHeadPublishError> {
    if writer_version.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyWriterVersion);
    }

    if current_head.namespace_id != plan.namespace_id {
        return Err(CommitHeadPublishError::NamespaceMismatch {
            head: current_head.namespace_id.clone(),
            plan: plan.namespace_id.clone(),
        });
    }

    if current_head.seq != plan.base_head_seq {
        return Err(CommitHeadPublishError::PlanBaseHeadSeqMismatch {
            head: current_head.seq,
            plan: plan.base_head_seq,
        });
    }

    if current_head.seq.0.checked_add(1).map(ChangeSeq) != Some(plan.next_seq) {
        return Err(CommitHeadPublishError::PlanNextSeqMismatch {
            head: current_head.seq,
            plan: plan.next_seq,
        });
    }

    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: plan.next_seq,
        active_fence_token: current_head.active_fence_token,
        next_inode_id: plan.resulting_next_inode_id,
        snapshot_hint_seq: current_head.snapshot_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
    };
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        resulting_head.clone(),
    )
    .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;
    let encoded_bytes = serde_json::to_vec(&envelope)
        .map_err(|err| CommitHeadPublishError::Codec(err.to_string()))?;

    let mut checked_invariants = plan.checked_invariants.clone();
    push_unique_invariant(&mut checked_invariants, "head_publish_requires_durable_wal");

    Ok(PreparedCommitHeadPublish {
        object_key: namespace_head(current_head.namespace_id.as_str()),
        resulting_head,
        envelope,
        encoded_bytes,
        checked_invariants,
    })
}

pub fn publish_commit_head<S: ObjectStore>(
    store: &S,
    expected_head_etag: &str,
    prepared: &PreparedCommitHeadPublish,
) -> Result<ObjectMetadata, CommitHeadPublishError> {
    if expected_head_etag.trim().is_empty() {
        return Err(CommitHeadPublishError::EmptyExpectedHeadEtag);
    }

    store
        .compare_and_swap(
            &prepared.object_key,
            expected_head_etag,
            &prepared.encoded_bytes,
        )
        .map_err(map_object_store_error)
}

fn map_object_store_error(err: ObjectStoreError) -> CommitHeadPublishError {
    CommitHeadPublishError::Store(err.to_string())
}
