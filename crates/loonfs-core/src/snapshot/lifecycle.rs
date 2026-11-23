//! Snapshot reads, expiry, extension, and deletion.
//!
//! The snapshot API reads and changes user snapshots. An unexpired snapshot can be read and forked.
//! An expired or deleted snapshot can't be used or retrieved, and the snapshot
//! listing leaves it out, but its record stays until garbage collection deletes it.

use super::create::CreatedSnapshot;
use super::read_basis::load_snapshot_read_basis_from_record;
use super::record::{encode_snapshot_record, LoadedSnapshotRecord};
use crate::control_update::{retry_while_contended, CasOutcome};
use crate::engine::{Administrator, NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::basis::SnapshotReadBasis;
use crate::namespace::control::ensure_namespace_live;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::keys::snapshot_record;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError, PutMode};
use loonfs_types::format::control::{SnapshotOwner, SnapshotPayload};
use loonfs_types::{DeleteSnapshotResponse, Snapshot, SnapshotId};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Resolves the read basis of an unexpired snapshot or of one of this
    /// namespace's fork snapshots, which keeps a manifest just as a snapshot does
    /// ([format section 8.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#83-reads-deletion-and-expiry)).
    ///
    /// A missing snapshot, a deleted snapshot, and a snapshot that expired at or
    /// before the wall-clock time of the call return `snapshot_not_found`.
    pub async fn load_snapshot_read_basis(
        &self,
        live_head: &NamespaceReadState,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotReadBasis> {
        let now_ms = self.wall_clock.now_ms();
        let Some(loaded) = self.load_snapshot_record(snapshot_id).await? else {
            return Err(CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            });
        };
        let loaded = ensure_live(loaded, now_ms)?;
        self.load_created_snapshot_read_basis(live_head, loaded.state)
            .await
    }

    /// Resolves the read basis of the snapshot `record`, which this process
    /// wrote, from the record in memory instead of a load of the record.
    pub async fn load_created_snapshot_read_basis(
        &self,
        live_head: &NamespaceReadState,
        record: SnapshotPayload,
    ) -> Result<SnapshotReadBasis> {
        load_snapshot_read_basis_from_record(&self.store, &self.segment_cache, live_head, record)
            .await
            .map(SnapshotReadBasis)
    }

    /// Deletes the user snapshot `record`, whose creation failed with
    /// `error`, so that a put of the creation that the store applies later
    /// can't make it an active snapshot (format spec, "Creating and verifying
    /// a snapshot").
    ///
    /// A record at the key is the creation's own, and the call records a
    /// delete time in that record. When the key is empty, the call writes
    /// `record` with a delete time there with put-if-absent. A cleanup that
    /// fails is only logged. It leaves the record undeleted or the key empty,
    /// and collection then deletes an undeleted record after its expiry, or
    /// once its manifest is missing (format spec, "Snapshot cleanup").
    pub(super) async fn delete_failed_snapshot(
        &self,
        record: &SnapshotPayload,
        clock: &OperationClock,
        error: &CoreError,
    ) {
        let deleted = retry_while_contended(|| async {
            let (stored, mode) = match self.load_snapshot_record(&record.snapshot_id).await? {
                Some(loaded) => (
                    loaded.state,
                    PutMode::CompareAndSwap {
                        expected_etag: loaded.etag,
                    },
                ),
                None => (record.clone(), PutMode::CreateIfAbsent),
            };
            self.mark_snapshot_deleted(stored, mode, clock.now_ms())
                .await
        })
        .await;
        if let Err(cleanup_error) = deleted.and_then(|deleted| deleted) {
            tracing::warn!(
                namespace_id = %self.namespace_id,
                snapshot_id = %record.snapshot_id,
                original_error = %error,
                cleanup_error = %cleanup_error,
                "failed to delete a snapshot after its creation failed"
            );
        }
    }

    /// Records `now_ms` as the delete time of the user snapshot `record`, with
    /// a put under `mode`: a compare-and-swap against the version of the
    /// record that the caller read or wrote, or a put-if-absent when the
    /// caller found the key empty. A record with a delete time already is
    /// left as it is.
    async fn mark_snapshot_deleted(
        &self,
        mut record: SnapshotPayload,
        mode: PutMode,
        now_ms: u64,
    ) -> Result<CasOutcome<(), CoreError>> {
        let object_key = snapshot_record(&self.namespace_id, &record.snapshot_id);
        let SnapshotOwner::User { deleted_at_ms, .. } = &mut record.owner else {
            return Err(CoreError::SnapshotNotFound {
                snapshot_id: record.snapshot_id,
            });
        };
        if deleted_at_ms.is_some() {
            return Ok(CasOutcome::Settled(()));
        }
        *deleted_at_ms = Some(now_ms);
        let encoded = encode_snapshot_record(&record);
        match self.store.put(&object_key, encoded, mode).await {
            Ok(_) => Ok(CasOutcome::Settled(())),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CasOutcome::Contended(
                CoreError::contention_exhausted(&object_key),
            )),
            // The next attempt loads the record again. If this write was
            // applied, that attempt finds the delete time and writes nothing.
            Err(error @ ObjectStoreError::Transport { .. }) => {
                Ok(CasOutcome::Contended(CoreError::store(&object_key, &error)))
            }
            Err(error) => Err(CoreError::store(&object_key, &error)),
        }
    }
}

/// Returns `loaded` if it isn't deleted and hasn't expired at `now_ms`, and
/// `snapshot_not_found` otherwise. A deletion is refused whatever `now_ms`
/// reads, because the deleting host's clock can be ahead of this one.
pub(crate) fn ensure_live(
    loaded: LoadedSnapshotRecord,
    now_ms: u64,
) -> Result<LoadedSnapshotRecord> {
    let owner = &loaded.state.owner;
    if owner.deleted_at_ms().is_some()
        || owner
            .expires_at_ms()
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
    {
        return Err(CoreError::SnapshotNotFound {
            snapshot_id: loaded.state.snapshot_id,
        });
    }
    Ok(loaded)
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Loads one snapshot by id, as the snapshot listing returns it. A missing
    /// id, the id of a deleted snapshot, the id of a snapshot that expired at or
    /// before the wall-clock time of the call, or the id of a fork snapshot
    /// returns `snapshot_not_found`.
    ///
    /// `head` is the namespace's head as the caller read it. An initializing
    /// head returns `namespace_not_found`, and a deleted head returns
    /// `namespace_deleted`. `administrator` proves the caller administers
    /// the namespace, because snapshots are only for the administrator.
    pub async fn get_snapshot(
        &self,
        administrator: &Administrator,
        head: &NamespaceReadState,
        snapshot_id: &SnapshotId,
    ) -> Result<Snapshot> {
        administrator.assert_for(&self.namespace.namespace_id);
        ensure_namespace_live(&head.namespace_id, head.status)?;
        let now_ms = self.now_ms();
        let loaded = ensure_live(
            self.namespace.load_user_snapshot(snapshot_id).await?,
            now_ms,
        )?;
        Ok(super::snapshot_summary(loaded.state))
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Moves the expiry of an unexpired snapshot that isn't deleted to
    /// `requested_expires_at_ms`, never earlier. A snapshot without an expiry is returned unchanged.
    /// `administrator` proves the caller administers the namespace.
    ///
    /// Each compare-and-swap checks the call's publication budget first, as a
    /// snapshot record's creation does, so a call that stalled past its
    /// budget doesn't extend the snapshot (format spec, "Extending a
    /// snapshot").
    pub async fn extend_snapshot(
        &self,
        administrator: &Administrator,
        snapshot_id: &SnapshotId,
        requested_expires_at_ms: u64,
    ) -> Result<Snapshot> {
        administrator.assert_for(&self.namespace.namespace_id);
        let clock = self.namespace.start_clock();
        let object_key = snapshot_record(&self.namespace.namespace_id, snapshot_id);
        retry_while_contended(|| async {
            // The expiry check reads the clock after the load, so the time the
            // load took counts toward the expiry.
            let loaded = self.namespace.load_user_snapshot(snapshot_id).await?;
            let loaded = ensure_live(loaded, clock.now_ms())?;
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
            clock.ensure_metadata_publication_budget(&self.namespace.namespace_id)?;
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
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    // This read checks whether an earlier compare-and-swap was
                    // written. It doesn't start a new extension, so an expiry
                    // after that write doesn't turn the success into a failure.
                    let current = ensure_live(
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
                        Ok(CasOutcome::Contended(CoreError::store(&object_key, &error)))
                    }
                }
                Err(error) => Err(CoreError::store(&object_key, &error)),
            }
        })
        .await?
    }

    /// Deletes a snapshot by recording the current time as its delete time, so
    /// the snapshot can't be used or retrieved from then on and garbage
    /// collection deletes its record after the grace window. A missing or
    /// already deleted snapshot succeeds without a write, so a retried delete
    /// returns the same response. A fork snapshot returns `snapshot_not_found`,
    /// because only garbage collection deletes it. `administrator` proves the
    /// caller administers the namespace.
    pub async fn delete_snapshot(
        &self,
        administrator: &Administrator,
        snapshot_id: &SnapshotId,
    ) -> Result<DeleteSnapshotResponse> {
        administrator.assert_for(&self.namespace.namespace_id);
        self.delete_snapshot_record(snapshot_id).await
    }

    /// Deletes a snapshot as [`Self::delete_snapshot`] does, without the
    /// proof.
    async fn delete_snapshot_record(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<DeleteSnapshotResponse> {
        let clock = self.namespace.start_clock();
        retry_while_contended(|| async {
            let Some(loaded) = self.namespace.load_snapshot_record(snapshot_id).await? else {
                return Ok(CasOutcome::Settled(()));
            };
            // The clock is read after the load, as an extension reads it.
            self.namespace
                .mark_snapshot_deleted(
                    loaded.state,
                    PutMode::CompareAndSwap {
                        expected_etag: loaded.etag,
                    },
                    clock.now_ms(),
                )
                .await
        })
        .await??;
        Ok(self.deleted_snapshot(snapshot_id))
    }

    /// Deletes the snapshot that this process wrote as `created`, with one
    /// compare-and-swap against the version that the write stored. A record
    /// that changed after the write, and a write that returned no version,
    /// delete as [`Self::delete_snapshot`] does, which loads the record first.
    ///
    /// A merge calls this function to delete its snapshot of the fork, so the
    /// function makes no administrator check, for the reason that
    /// [`Self::create_verified_snapshot`] describes.
    pub async fn delete_created_snapshot(
        &self,
        created: &CreatedSnapshot,
    ) -> Result<DeleteSnapshotResponse> {
        let snapshot_id = &created.record.snapshot_id;
        if let Some(etag) = &created.etag {
            let now_ms = self.namespace.start_clock().now_ms();
            if let CasOutcome::Settled(()) = self
                .namespace
                .mark_snapshot_deleted(
                    created.record.clone(),
                    PutMode::CompareAndSwap {
                        expected_etag: etag.clone(),
                    },
                    now_ms,
                )
                .await?
            {
                return Ok(self.deleted_snapshot(snapshot_id));
            }
        }
        self.delete_snapshot_record(snapshot_id).await
    }

    fn deleted_snapshot(&self, snapshot_id: &SnapshotId) -> DeleteSnapshotResponse {
        DeleteSnapshotResponse {
            namespace_id: self.namespace.namespace_id.clone(),
            snapshot_id: snapshot_id.clone(),
        }
    }
}
