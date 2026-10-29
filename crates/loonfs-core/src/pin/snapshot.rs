//! Snapshot reads, expiry, extension, and deletion.
//!
//! A snapshot is a user pin. An unexpired snapshot can be read and forked,
//! and an expired one can't, but it stays listed until garbage collection
//! deletes it.

use super::read_basis::load_snapshot_read_basis_from_record;
use super::record::{encode_pin, LoadedPin};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::basis::ReadBasis;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::keys::pin;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::PinOwner;
use loonfs_types::{DeleteSnapshotResponse, PinId, Snapshot};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Resolves the read basis of an unexpired snapshot.
    ///
    /// A missing snapshot, a fork pin, and a snapshot that expired at or before
    /// the wall-clock time of the call return `snapshot_not_found`.
    pub async fn load_snapshot_read_basis(
        &self,
        live_head: &NamespaceReadState,
        snapshot_id: &PinId,
    ) -> Result<ReadBasis> {
        let now_ms = self.wall_clock.now_ms();
        let loaded = ensure_unexpired(self.load_user_pin(snapshot_id).await?, now_ms)?;
        load_snapshot_read_basis_from_record(
            &self.store,
            &self.segment_cache,
            live_head,
            loaded.state,
        )
        .await
    }
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

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Loads one snapshot by id, as the snapshot listing returns it,
    /// including an expired snapshot that garbage collection hasn't deleted.
    /// A missing id, or the id of a fork pin, returns `snapshot_not_found`.
    pub async fn get_snapshot(&self, snapshot_id: &PinId) -> Result<Snapshot> {
        self.namespace.load_current_manifest().await?;
        let loaded = self.namespace.load_user_pin(snapshot_id).await?;
        Ok(super::snapshot_summary(loaded.state))
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Moves an unexpired snapshot's expiry to `requested_expires_at_ms`,
    /// never earlier. A snapshot without an expiry is returned unchanged.
    pub async fn extend_snapshot(
        &self,
        snapshot_id: &PinId,
        requested_expires_at_ms: u64,
    ) -> Result<Snapshot> {
        let clock = self.namespace.start_clock();
        let object_key = pin(&self.namespace.namespace_id, snapshot_id);
        retry_while_contended(|| async {
            // The expiry check reads the clock after the load, so the time the
            // load took counts toward the expiry.
            let loaded = self.namespace.load_user_pin(snapshot_id).await?;
            let loaded = ensure_unexpired(loaded, clock.now_ms())?;
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
            match self
                .namespace
                .store
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
                    let current = ensure_unexpired(
                        self.namespace.load_user_pin(snapshot_id).await?,
                        clock.request_at_ms(),
                    )?;
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
    pub async fn delete_snapshot(&self, snapshot_id: &PinId) -> Result<DeleteSnapshotResponse> {
        match self.namespace.load_pin(snapshot_id).await? {
            None => {}
            Some(loaded) => match loaded.state.owner {
                PinOwner::User { .. } => self.namespace.delete_pin(snapshot_id).await?,
                PinOwner::Fork { .. } => {
                    return Err(CoreError::SnapshotNotFound {
                        snapshot_id: snapshot_id.clone(),
                    })
                }
            },
        }
        Ok(DeleteSnapshotResponse {
            namespace_id: self.namespace.namespace_id.clone(),
            snapshot_id: snapshot_id.clone(),
        })
    }
}
