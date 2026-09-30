//! Snapshot-owned checkpoint reads, expiry, and pin deletion.

use super::delete::delete_owned_checkpoint;
use super::read_basis::{load_checkpoint_read_basis_from_record, CheckpointReadBasis};
use super::record::{
    encode_checkpoint_record, load_owned_checkpoint_record, CheckpointOwnerKind,
    LoadedCheckpointRecord,
};
use super::MetadataSegmentCache;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use loonfs_api::wire::control::PinOwner;
use loonfs_api::{Checkpoint, DeleteSnapshotResponse, NamespaceId, PinId};
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::sync::Arc;

/// Resolves the read basis a live snapshot lease pins.
pub async fn load_snapshot_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &NamespaceReadState,
    snapshot_id: &PinId,
    now_ms: u64,
) -> Result<CheckpointReadBasis> {
    let loaded = load_owned_checkpoint_record(
        store,
        &live_head.namespace_id,
        snapshot_id,
        CheckpointOwnerKind::Snapshot,
    )
    .await?;
    let loaded = classify_live_snapshot(loaded, now_ms)?;
    load_checkpoint_read_basis_from_record(store, segment_cache, live_head, loaded.state).await
}

pub(crate) async fn extend_snapshot_expiry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
    requested_expires_at_ms: u64,
    max_lifetime_ms: u64,
    context: &MutationContext,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<Checkpoint> {
    let deadline = Deadline::start(timer);
    let object_key = checkpoint_record(namespace_id, checkpoint_id);
    retry_while_contended(|| async {
        let loaded = load_owned_checkpoint_record(
            store,
            namespace_id,
            checkpoint_id,
            CheckpointOwnerKind::Snapshot,
        )
        .await?;
        let loaded = classify_live_snapshot(loaded, context.now_at(&deadline))?;
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
            Err(ObjectStoreError::Transport { .. }) => {
                // This read proves an earlier CAS landed; it does not start a
                // new extension. Expiry after that CAS must not erase success.
                let current = load_owned_checkpoint_record(
                    store,
                    namespace_id,
                    checkpoint_id,
                    CheckpointOwnerKind::Snapshot,
                )
                .await?;
                let current = classify_live_snapshot(current, context.now_ms)?;
                let expires_at_ms = current
                    .state
                    .owner
                    .expires_at_ms()
                    .expect("a classified snapshot should carry an expiry");
                if expires_at_ms >= new_expires_at_ms {
                    Ok(CasAttempt::Settled(super::checkpoint_summary(
                        current.state,
                    )))
                } else {
                    Ok(CasAttempt::Contended(CoreError::contention_exhausted(
                        &object_key,
                    )))
                }
            }
            Err(error) => Err(CoreError::store(&object_key, &error)),
        }
    })
    .await?
}

pub(crate) async fn delete_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> Result<DeleteSnapshotResponse> {
    delete_owned_checkpoint(
        store,
        namespace_id,
        checkpoint_id,
        CheckpointOwnerKind::Snapshot,
    )
    .await?;
    Ok(DeleteSnapshotResponse {
        namespace_id: namespace_id.clone(),
        snapshot_id: checkpoint_id.clone(),
    })
}

pub(crate) fn classify_live_snapshot(
    loaded: LoadedCheckpointRecord,
    now_ms: u64,
) -> Result<LoadedCheckpointRecord> {
    let expires_at_ms = loaded
        .state
        .owner
        .expires_at_ms()
        .expect("a snapshot owner should carry an expiry");
    if expires_at_ms <= now_ms {
        return Err(CoreError::SnapshotGone {
            snapshot_id: loaded.state.pin_id,
        });
    }
    Ok(loaded)
}

fn snapshot_expiry_mut(owner: &mut PinOwner) -> Option<&mut u64> {
    match owner {
        PinOwner::Snapshot { expires_at_ms, .. } => Some(expires_at_ms),
        PinOwner::User { .. } | PinOwner::Fork { .. } => None,
    }
}
