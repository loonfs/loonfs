//! Snapshot reads, expiry, extension, and deletion.
//!
//! The snapshot API reads and changes user snapshots. An unexpired snapshot can be read and forked,
//! and an expired one can't, but it stays listed until garbage collection
//! deletes it.

use super::read_basis::load_snapshot_read_basis_from_record;
use super::record::{encode_snapshot_record, LoadedSnapshotRecord};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::basis::ReadBasis;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::keys::snapshot_record;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::control::SnapshotOwner;
use loonfs_types::{DeleteSnapshotResponse, Snapshot, SnapshotId};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Resolves the read basis of an unexpired snapshot or of one of this
    /// namespace's fork snapshots, which keeps a manifest just as a snapshot does
    /// ([format section 8.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#83-reads-deletion-and-expiry)).
    ///
    /// A missing snapshot, and a snapshot that expired at or before the wall-clock
    /// time of the call, return `snapshot_not_found`.
    pub async fn load_snapshot_read_basis(
        &self,
        live_head: &NamespaceReadState,
        snapshot_id: &SnapshotId,
    ) -> Result<ReadBasis> {
        let now_ms = self.wall_clock.now_ms();
        let Some(loaded) = self.load_snapshot_record(snapshot_id).await? else {
            return Err(CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            });
        };
        let loaded = ensure_unexpired(loaded, now_ms)?;
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
pub(crate) fn ensure_unexpired(
    loaded: LoadedSnapshotRecord,
    now_ms: u64,
) -> Result<LoadedSnapshotRecord> {
    match loaded.state.owner.expires_at_ms() {
        Some(expires_at_ms) if expires_at_ms <= now_ms => Err(CoreError::SnapshotNotFound {
            snapshot_id: loaded.state.snapshot_id,
        }),
        _ => Ok(loaded),
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Loads one snapshot by id, as the snapshot listing returns it,
    /// including an expired snapshot that garbage collection hasn't deleted.
    /// A missing id, or the id of a fork snapshot, returns `snapshot_not_found`.
    pub async fn get_snapshot(&self, snapshot_id: &SnapshotId) -> Result<Snapshot> {
        self.namespace.load_current_manifest().await?;
        let loaded = self.namespace.load_user_snapshot(snapshot_id).await?;
        Ok(super::snapshot_summary(loaded.state))
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Moves an unexpired snapshot's expiry to `requested_expires_at_ms`,
    /// never earlier. A snapshot without an expiry is returned unchanged.
    pub async fn extend_snapshot(
        &self,
        snapshot_id: &SnapshotId,
        requested_expires_at_ms: u64,
    ) -> Result<Snapshot> {
        let clock = self.namespace.start_clock();
        let object_key = snapshot_record(&self.namespace.namespace_id, snapshot_id);
        retry_while_contended(|| async {
            // The expiry check reads the clock after the load, so the time the
            // load took counts toward the expiry.
            let loaded = self.namespace.load_user_snapshot(snapshot_id).await?;
            let loaded = ensure_unexpired(loaded, clock.now_ms())?;
            let mut next = loaded.state.clone();
            let SnapshotOwner::User {
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
            let encoded = encode_snapshot_record(&next);
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
                        self.namespace.load_user_snapshot(snapshot_id).await?,
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

    /// Deletes a snapshot by moving its expiry to the current time, so the
    /// snapshot is expired from then on and garbage collection deletes its
    /// record after the grace window. A missing or already expired snapshot
    /// succeeds without a write, so a retried delete returns the same response.
    /// A fork snapshot returns `snapshot_not_found`, because only garbage
    /// collection deletes it.
    pub async fn delete_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<DeleteSnapshotResponse> {
        let clock = self.namespace.start_clock();
        let object_key = snapshot_record(&self.namespace.namespace_id, snapshot_id);
        retry_while_contended(|| async {
            let Some(loaded) = self.namespace.load_snapshot_record(snapshot_id).await? else {
                return Ok(CasOutcome::Settled(()));
            };
            let mut next = loaded.state.clone();
            let SnapshotOwner::User { expires_at_ms, .. } = &mut next.owner else {
                return Err(CoreError::SnapshotNotFound {
                    snapshot_id: snapshot_id.clone(),
                });
            };
            // The clock is read after the load, as an extension reads it.
            let now_ms = clock.now_ms();
            if expires_at_ms.is_some_and(|expires_at_ms| expires_at_ms <= now_ms) {
                return Ok(CasOutcome::Settled(()));
            }
            *expires_at_ms = Some(now_ms);
            let encoded = encode_snapshot_record(&next);
            match self
                .namespace
                .store
                .compare_and_swap(&object_key, &loaded.etag, encoded)
                .await
            {
                Ok(_) => Ok(CasOutcome::Settled(())),
                Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasOutcome::Contended(
                    CoreError::contention_exhausted(&object_key),
                )),
                // The next attempt loads the record again. If this write was
                // applied, that attempt finds the expiry at or before its clock
                // and writes nothing.
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    Ok(CasOutcome::Contended(CoreError::store(&object_key, &error)))
                }
                Err(error) => Err(CoreError::store(&object_key, &error)),
            }
        })
        .await??;
        Ok(DeleteSnapshotResponse {
            namespace_id: self.namespace.namespace_id.clone(),
            snapshot_id: snapshot_id.clone(),
        })
    }
}
