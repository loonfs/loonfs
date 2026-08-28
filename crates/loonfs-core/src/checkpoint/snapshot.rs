//! Snapshot-owned checkpoint expiry and release transitions.

use super::record::{
    encode_checkpoint_record, load_checkpoint_record, release_checkpoint_record,
    LoadedCheckpointRecord,
};
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus};
use loonfs_api::{Checkpoint, CheckpointId, NamespaceId, ReleaseSnapshotResponse};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) async fn extend_snapshot_expiry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    requested_expires_at_ms: u64,
    max_lifetime_ms: u64,
    context: &MutationContext,
) -> Result<Checkpoint> {
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    let updated = retry_while_contended(|| async {
        let loaded = classify_live_snapshot(
            load_checkpoint_record(store, namespace_id, checkpoint_id).await?,
            checkpoint_id,
            context.now_ms,
        )?;
        let mut next = loaded.state.clone();
        let lifetime_ceiling = next.created_at_ms.saturating_add(max_lifetime_ms);
        let CheckpointOwner::Snapshot { expires_at_ms, .. } = &mut next.owner else {
            return Err(CoreError::InvalidCheckpointRequest(format!(
                "checkpoint `{checkpoint_id}` is not a snapshot"
            )));
        };
        let new_expires_at_ms = requested_expires_at_ms
            .min(lifetime_ceiling)
            .max(*expires_at_ms);
        if *expires_at_ms == new_expires_at_ms {
            return Ok(CasAttempt::Settled(super::checkpoint_summary(next)));
        }
        *expires_at_ms = new_expires_at_ms;
        let encoded = encode_checkpoint_record(&next)?;
        match store
            .compare_and_swap(&object_key, &loaded.etag, encoded)
            .await
        {
            Ok(_) => Ok(CasAttempt::Settled(super::checkpoint_summary(next))),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended),
            Err(error) => Err(CoreError::store(&object_key, &error)),
        }
    })
    .await?;
    updated.ok_or_else(|| CoreError::contention_exhausted(&object_key))
}

pub(crate) async fn release_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    context: &MutationContext,
) -> Result<ReleaseSnapshotResponse> {
    let Some(loaded) = load_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
        return Ok(ReleaseSnapshotResponse {
            namespace_id: namespace_id.clone(),
            snapshot_id: checkpoint_id.clone(),
        });
    };
    match &loaded.state.owner {
        CheckpointOwner::Snapshot { .. } => {}
        CheckpointOwner::User { .. } => {
            return Err(CoreError::InvalidCheckpointRequest(format!(
                "checkpoint `{checkpoint_id}` is user-owned; release it through the checkpoint \
                 release operation, not the snapshot release operation"
            )))
        }
        CheckpointOwner::Fork {
            target_namespace_id,
            ..
        } => {
            return Err(CoreError::InvalidCheckpointRequest(format!(
                "checkpoint `{checkpoint_id}` is owned by fork target `{target_namespace_id}`; \
                 it is released by deleting that namespace, not by the snapshot release operation"
            )))
        }
    }
    release_checkpoint_record(store, namespace_id, checkpoint_id, context.now_ms).await?;
    Ok(ReleaseSnapshotResponse {
        namespace_id: namespace_id.clone(),
        snapshot_id: checkpoint_id.clone(),
    })
}

fn classify_live_snapshot(
    loaded: Option<LoadedCheckpointRecord>,
    checkpoint_id: &CheckpointId,
    now_ms: u64,
) -> Result<LoadedCheckpointRecord> {
    let Some(loaded) = loaded else {
        return Err(CoreError::SnapshotNotFound {
            snapshot_id: checkpoint_id.clone(),
        });
    };
    match &loaded.state.owner {
        CheckpointOwner::Snapshot { expires_at_ms, .. } => {
            if loaded.state.status != (CheckpointStatus::Active {}) {
                return Err(snapshot_gone(checkpoint_id, "released"));
            }
            if *expires_at_ms <= now_ms {
                return Err(snapshot_gone(checkpoint_id, "expired"));
            }
        }
        CheckpointOwner::User { .. } => {
            return Err(CoreError::InvalidCheckpointRequest(format!(
                "checkpoint `{checkpoint_id}` is user-owned; snapshot extension acts only on \
                 snapshot-owned records"
            )))
        }
        CheckpointOwner::Fork {
            target_namespace_id,
            ..
        } => {
            return Err(CoreError::InvalidCheckpointRequest(format!(
                "checkpoint `{checkpoint_id}` is owned by fork target `{target_namespace_id}`; \
                 snapshot extension acts only on snapshot-owned records"
            )))
        }
    }
    Ok(loaded)
}

fn snapshot_gone(checkpoint_id: &CheckpointId, reason: &str) -> CoreError {
    CoreError::SnapshotGone {
        snapshot_id: checkpoint_id.clone(),
        reason: reason.to_owned(),
    }
}
