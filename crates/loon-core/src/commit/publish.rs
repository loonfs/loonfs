use super::{push_unique_invariant, CommitHeadPublishError, CommitPlan, PreparedCommitHeadPublish};
use loon_api::{ControlObjectKind, HeadState, HeadStateEnvelope, WalSegmentPointer};
use loon_objectstore::keys::namespace_head;
use loon_objectstore::ObjectStoreError;
use loon_objectstore::{ObjectMetadata, ObjectStore};

pub fn prepare_commit_head_publish(
    current_head: &HeadState,
    plan: &CommitPlan,
    visible_wal_tip: WalSegmentPointer,
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

    if plan.apply_after_seq < current_head.seq {
        return Err(CommitHeadPublishError::PlanApplyAfterSeqBeforeHead {
            head: current_head.seq,
            plan: plan.apply_after_seq,
        });
    }

    if plan.assigned_seq <= current_head.seq {
        return Err(CommitHeadPublishError::PlanAssignedSeqNotAfterHead {
            head: current_head.seq,
            plan: plan.assigned_seq,
        });
    }

    let resulting_head = HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: plan.assigned_seq,
        active_fence_token: current_head.active_fence_token,
        next_inode_id: plan.resulting_next_inode_id,
        name_policy: current_head.name_policy,
        snapshot_hint_seq: current_head.snapshot_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: Some(visible_wal_tip),
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

pub fn publish_commit_head<S: ObjectStore + ?Sized>(
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
    match err {
        ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict => {
            CommitHeadPublishError::StaleHead
        }
        other => CommitHeadPublishError::Store(other.to_string()),
    }
}
