//! Snapshot reads, expiry, extension, and deletion.
//!
//! A snapshot is a user pin. An unexpired snapshot can be read and forked,
//! and an expired one can't, but it stays listed until garbage collection
//! deletes it.

use super::read_basis::{load_snapshot_read_basis_from_record, SnapshotReadBasis};
use super::record::{delete_pin, encode_pin, load_pin, load_user_pin, LoadedPin};
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::error::{CoreError, Result};
use crate::manifest::MetadataSegmentCache;
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::keys::pin;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::PinOwner;
use loonfs_types::{DeleteSnapshotResponse, NamespaceId, PinId, Snapshot};
use std::sync::Arc;

/// Resolves the read basis of an unexpired snapshot.
///
/// A missing snapshot, a fork pin, and a snapshot that expired at or before
/// `now_ms` return `snapshot_not_found`.
pub async fn load_snapshot_read_basis<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    live_head: &NamespaceReadState,
    snapshot_id: &PinId,
    now_ms: u64,
) -> Result<SnapshotReadBasis> {
    let loaded =
        load_unexpired_snapshot(store, &live_head.namespace_id, snapshot_id, now_ms).await?;
    load_snapshot_read_basis_from_record(store, segment_cache, live_head, loaded.state).await
}

/// Loads a snapshot that hasn't expired at `now_ms`. A missing snapshot, a
/// fork pin, and an expired snapshot return `snapshot_not_found`.
pub(crate) async fn load_unexpired_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    now_ms: u64,
) -> Result<LoadedPin> {
    let loaded = load_user_pin(store, namespace_id, snapshot_id).await?;
    ensure_unexpired(loaded, now_ms)
}

/// Returns `loaded` if it hasn't expired at `now_ms`, and
/// `snapshot_not_found` if it has.
pub(crate) fn ensure_unexpired(loaded: LoadedPin, now_ms: u64) -> Result<LoadedPin> {
    match loaded.state.owner.expires_at_ms() {
        Some(expires_at_ms) if expires_at_ms <= now_ms => Err(CoreError::SnapshotNotFound {
            snapshot_id: loaded.state.pin_id,
        }),
        _ => Ok(loaded),
    }
}

/// Loads one snapshot, as the listing returns it, including an expired
/// snapshot that garbage collection hasn't deleted. A missing pin, or a fork
/// pin, returns `snapshot_not_found`.
pub(crate) async fn get_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
) -> Result<Snapshot> {
    crate::namespace::control::load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let loaded = load_user_pin(store, namespace_id, snapshot_id).await?;
    Ok(super::snapshot_summary(loaded.state))
}

/// Moves an unexpired snapshot's expiry to `requested_expires_at_ms`. The
/// expiry never moves earlier, so a snapshot without an expiry is returned
/// unchanged.
pub(crate) async fn extend_snapshot_expiry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
    requested_expires_at_ms: u64,
    context: &MutationContext,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<Snapshot> {
    let deadline = Deadline::start(timer);
    let object_key = pin(namespace_id, snapshot_id);
    retry_while_contended(|| async {
        // The expiry check reads the clock after the load, so the time the
        // load took counts toward the expiry.
        let loaded = load_user_pin(store, namespace_id, snapshot_id).await?;
        let loaded = ensure_unexpired(loaded, context.now_at(&deadline))?;
        let mut next = loaded.state.clone();
        let PinOwner::User {
            expires_at_ms: Some(expires_at_ms),
            ..
        } = &mut next.owner
        else {
            return Ok(CasOutcome::Settled(super::snapshot_summary(next)));
        };
        if *expires_at_ms >= requested_expires_at_ms {
            return Ok(CasOutcome::Settled(super::snapshot_summary(next)));
        }
        *expires_at_ms = requested_expires_at_ms;
        let encoded = encode_pin(&next);
        match store
            .compare_and_swap(&object_key, &loaded.etag, encoded)
            .await
        {
            Ok(_) => Ok(CasOutcome::Settled(super::snapshot_summary(next))),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasOutcome::Contended(
                CoreError::contention_exhausted(&object_key),
            )),
            Err(ObjectStoreError::Transport { .. }) => {
                // This read checks whether an earlier compare-and-swap was
                // written. It doesn't start a new extension, so an expiry
                // after that write doesn't turn the success into a failure.
                let current =
                    load_unexpired_snapshot(store, namespace_id, snapshot_id, context.now_ms)
                        .await?;
                if current
                    .state
                    .owner
                    .expires_at_ms()
                    .is_none_or(|expires_at_ms| expires_at_ms >= requested_expires_at_ms)
                {
                    Ok(CasOutcome::Settled(super::snapshot_summary(current.state)))
                } else {
                    Ok(CasOutcome::Contended(CoreError::contention_exhausted(
                        &object_key,
                    )))
                }
            }
            Err(error) => Err(CoreError::store(&object_key, &error)),
        }
    })
    .await?
}

/// Deletes a snapshot. Deleting a missing snapshot succeeds without a
/// write, so a retried delete returns the same response. A fork pin returns
/// `snapshot_not_found`, because only garbage collection deletes it.
pub(crate) async fn delete_snapshot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &PinId,
) -> Result<DeleteSnapshotResponse> {
    match load_pin(store, namespace_id, snapshot_id).await? {
        None => {}
        Some(loaded) => match loaded.state.owner {
            PinOwner::User { .. } => delete_pin(store, namespace_id, snapshot_id).await?,
            PinOwner::Fork { .. } => {
                return Err(CoreError::SnapshotNotFound {
                    snapshot_id: snapshot_id.clone(),
                })
            }
        },
    }
    Ok(DeleteSnapshotResponse {
        namespace_id: namespace_id.clone(),
        snapshot_id: snapshot_id.clone(),
    })
}
