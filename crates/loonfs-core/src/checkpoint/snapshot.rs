//! Snapshot-owned checkpoint reads, expiry, and release transitions.

use super::read_basis::{load_checkpoint_read_basis_from_record, CheckpointReadBasis};
use super::record::{encode_checkpoint_record, load_checkpoint_record, LoadedCheckpointRecord};
use super::release::{ensure_owner_is, release_owned_checkpoint, CheckpointOwnerKind};
use super::MetadataSegmentCache;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus, HeadState};
use loonfs_api::{Checkpoint, CheckpointId, NamespaceId, ReleaseSnapshotResponse};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

/// Resolves the read basis a live snapshot lease pins.
pub async fn load_snapshot_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &HeadState,
    snapshot_id: &CheckpointId,
    now_ms: u64,
) -> Result<CheckpointReadBasis> {
    let loaded = classify_live_snapshot(
        load_checkpoint_record(store, &live_head.namespace_id, snapshot_id).await?,
        snapshot_id,
        now_ms,
    )?;
    load_checkpoint_read_basis_from_record(store, segment_cache, live_head, loaded.state).await
}

pub(crate) async fn extend_snapshot_expiry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    requested_expires_at_ms: u64,
    max_lifetime_ms: u64,
    context: &MutationContext,
) -> Result<Checkpoint> {
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    retry_while_contended(
        || async {
            let loaded = classify_live_snapshot(
                load_checkpoint_record(store, namespace_id, checkpoint_id).await?,
                checkpoint_id,
                context.now_ms,
            )?;
            let mut next = loaded.state.clone();
            let lifetime_ceiling = next.created_at_ms.saturating_add(max_lifetime_ms);
            let expires_at_ms = snapshot_expiry_mut(&mut next.owner)
                .expect("a classified snapshot should carry a snapshot owner");
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
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasAttempt::Contended(
                    CoreError::contention_exhausted(&object_key),
                )),
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    Ok(CasAttempt::Ambiguous(error, new_expires_at_ms))
                }
                Err(error) => Err(CoreError::store(&object_key, &error)),
            }
        },
        |_, new_expires_at_ms| {
            let object_key = object_key.clone();
            async move {
                let current = classify_live_snapshot(
                    load_checkpoint_record(store, namespace_id, checkpoint_id).await?,
                    checkpoint_id,
                    context.now_ms,
                )?;
                let expires_at_ms = current
                    .state
                    .owner
                    .expires_at_ms()
                    .expect("a classified snapshot should carry an expiry");
                if expires_at_ms >= new_expires_at_ms {
                    Ok(WriteEvidence::Landed(super::checkpoint_summary(
                        current.state,
                    )))
                } else {
                    Ok(WriteEvidence::Lost(CoreError::contention_exhausted(
                        &object_key,
                    )))
                }
            }
        },
    )
    .await?
}

pub(crate) async fn release_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    context: &MutationContext,
) -> Result<ReleaseSnapshotResponse> {
    release_owned_checkpoint(
        store,
        namespace_id,
        checkpoint_id,
        CheckpointOwnerKind::Snapshot,
        context,
    )
    .await?;
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
    ensure_owner_is(
        checkpoint_id,
        &loaded.state.owner,
        CheckpointOwnerKind::Snapshot,
    )?;
    let expires_at_ms = loaded
        .state
        .owner
        .expires_at_ms()
        .expect("a snapshot owner should carry an expiry");
    if loaded.state.status != (CheckpointStatus::Active {}) {
        return Err(snapshot_gone(checkpoint_id, "released"));
    }
    if expires_at_ms <= now_ms {
        return Err(snapshot_gone(checkpoint_id, "expired"));
    }
    Ok(loaded)
}

fn snapshot_expiry_mut(owner: &mut CheckpointOwner) -> Option<&mut u64> {
    match owner {
        CheckpointOwner::Snapshot { expires_at_ms, .. } => Some(expires_at_ms),
        CheckpointOwner::User { .. } | CheckpointOwner::Fork { .. } => None,
    }
}

fn snapshot_gone(checkpoint_id: &CheckpointId, reason: &str) -> CoreError {
    CoreError::SnapshotGone {
        snapshot_id: checkpoint_id.clone(),
        reason: reason.to_owned(),
    }
}
