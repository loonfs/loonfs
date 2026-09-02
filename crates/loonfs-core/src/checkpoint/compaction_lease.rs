//! Lease for a streaming compaction's staged output.
//!
//! A job creates and refreshes an `Active` lease with compare-and-swap.
//! Garbage collection may claim an expired lease by changing it to `Reaping`.
//! A failed heartbeat then prevents the job from publishing.
//!
//! Completed jobs leave the lease in place so a collection pass that started
//! before publication cannot mistake old output for garbage. Malformed leases
//! stop collection because ownership cannot be verified.

use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
};
use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::limits::{
    METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS, METADATA_COMPACTION_LEASE_EXPIRY_MS,
};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, CompactionLeaseStatus, ControlObjectKind, MetadataCompactionLeaseState,
};
use loonfs_api::{MetadataCompactionId, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_lease;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

/// Who owns the objects under one job's prefix, as a collector reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionPrefixOwner {
    /// A job owns it: its lease is `Active` and was refreshed within
    /// [`METADATA_COMPACTION_LEASE_EXPIRY_MS`], or it heartbeated out from
    /// under this pass's claim. Nothing about the objects' age matters.
    LiveJob,
    /// This pass claimed the prefix, or found a claim someone else won. The
    /// job that wrote the objects is fenced, so they are orphans — and the
    /// lease is deleted after them, once nothing unreferenced is left under
    /// the prefix.
    ThisCollector,
    /// There is no lease to own anything. Whatever sits under the prefix is
    /// an ordinary unreferenced orphan and there is no claim to release
    /// afterwards.
    NoOne,
}

/// Returns the current prefix owner, claiming an expired lease when possible.
///
/// A collector parses `metadata_compaction_id` from the key it is deciding.
/// A lease naming a different job or namespace is corrupt because the key
/// and embedded fence disagree.
///
/// An expired lease is not collectable until the compare-and-swap succeeds;
/// the job may still resume and refresh it first.
pub(crate) async fn claim_compaction_prefix<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    metadata_compaction_id: &MetadataCompactionId,
    now_ms: u64,
) -> Result<CompactionPrefixOwner> {
    let object_key = metadata_compaction_lease(namespace_id, metadata_compaction_id);
    let loaded = load_control_object(
        store,
        object_key,
        ControlObjectKind::CompactionLease,
        |state: &MetadataCompactionLeaseState| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_identity_field(
                "compaction job id",
                metadata_compaction_id.as_str(),
                state.job_id.as_str(),
            )
        },
    )
    .await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(CompactionPrefixOwner::NoOne)
        }
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let object_key = loaded.object_key;
    let expected_etag = loaded.etag;
    let state = loaded.state;
    // Terminal: somebody already fenced this job, so the reap goes on from
    // wherever the pass that started it stopped.
    if state.status == (CompactionLeaseStatus::Reaping {}) {
        return Ok(CompactionPrefixOwner::ThisCollector);
    }
    if now_ms
        <= state
            .heartbeat_at_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS)
    {
        return Ok(CompactionPrefixOwner::LiveJob);
    }

    // Expired. Nothing is decided until the claim lands.
    let mut reaping = state;
    reaping.status = CompactionLeaseStatus::Reaping {};
    let encoded = encode_lease(&reaping)?;
    match store
        .compare_and_swap(&object_key, &expected_etag, encoded)
        .await
    {
        Ok(_) => {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                object_key = object_key.as_str(),
                job_id = metadata_compaction_id.as_str(),
                writer_id = reaping.writer_id.as_str(),
                heartbeat_at_ms = reaping.heartbeat_at_ms,
                "a streaming metadata compaction lease expired; garbage collection claimed its \
                 prefix and the job that wrote it can no longer publish"
            );
            Ok(CompactionPrefixOwner::ThisCollector)
        }
        // The job wrote the lease between this pass's read and its claim, so
        // the job is alive and owns its prefix. This pass keeps every object
        // under it.
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CompactionPrefixOwner::LiveJob),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// What one lease write found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LeaseHold {
    /// The write landed: this job still owns its prefix.
    Held,
    /// The lease is gone, or something else wrote it. Ownership is lost for
    /// good — a lease never returns to its previous owner — so the job stops
    /// without publishing.
    Fenced,
}

/// One running job's claim on its own prefix.
///
/// The wall clock is read once, when the job starts; every later heartbeat
/// stamps that instant plus local monotonic elapsed time, so nothing here
/// depends on a clock that can move. That is the same posture every
/// self-enforced budget in the system takes.
pub(super) struct CompactionLease<'a> {
    object_key: String,
    state: MetadataCompactionLeaseState,
    etag: String,
    timer: &'a dyn MonotonicTimer,
    started_monotonic_ms: u64,
    next_heartbeat_monotonic_ms: u64,
}

impl<'a> CompactionLease<'a> {
    /// Writes the lease before the job's first output object.
    pub(super) async fn create<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        job_id: &MetadataCompactionId,
        writer_id: &str,
        started_at_ms: u64,
        timer: &'a dyn MonotonicTimer,
    ) -> Result<Self> {
        let started_monotonic_ms = timer.monotonic_now_ms();
        let object_key = metadata_compaction_lease(namespace_id, job_id);
        let state = initial_lease_state(namespace_id, job_id, writer_id, started_at_ms);
        let encoded = encode_lease(&state)?;
        let metadata =
            create_control_object_under_generated_id(store, &object_key, encoded).await?;
        let etag = required_etag(&object_key, metadata.etag)?;
        Ok(Self {
            object_key,
            state,
            etag,
            timer,
            started_monotonic_ms,
            next_heartbeat_monotonic_ms: started_monotonic_ms,
        })
    }

    /// The clock the job paces itself by. One clock for the job's heartbeat
    /// and its publication budget, because they measure the same span.
    pub(super) fn timer(&self) -> &'a dyn MonotonicTimer {
        self.timer
    }

    /// Refreshes the lease when the interval has passed since the last write.
    ///
    /// Called where cancellation is checked, so the cost is one small
    /// compare-and-swap every [`METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS`]
    /// however many rows the job reads in between.
    pub(super) async fn heartbeat_if_due<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<LeaseHold> {
        if self.timer.monotonic_now_ms() < self.next_heartbeat_monotonic_ms {
            return Ok(LeaseHold::Held);
        }
        self.heartbeat(store).await
    }

    /// Refreshes the lease now, whatever the interval says.
    ///
    /// Finalization uses this: the span from a heartbeat to the root
    /// compare-and-swap that makes the output referenced is what the lease has
    /// to cover, and that span opens at the top of every attempt.
    pub(super) async fn heartbeat<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<LeaseHold> {
        let expected_etag = self.etag.clone();
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_monotonic_ms);
        self.state.heartbeat_at_ms = self.state.started_at_ms.saturating_add(elapsed_ms);
        let encoded = encode_lease(&self.state)?;
        match store
            .compare_and_swap(&self.object_key, &expected_etag, encoded)
            .await
        {
            Ok(metadata) => {
                self.etag = required_etag(&self.object_key, metadata.etag)?;
                self.next_heartbeat_monotonic_ms = self
                    .timer
                    .monotonic_now_ms()
                    .saturating_add(METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS);
                Ok(LeaseHold::Held)
            }
            // Garbage collection claimed the prefix, or the object is gone.
            // Either way this job no longer owns what it wrote.
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::NotFound { .. },
            ) => {
                tracing::warn!(
                    namespace_id = self.state.namespace_id.as_str(),
                    job_id = self.state.job_id.as_str(),
                    object_key = self.object_key.as_str(),
                    "a streaming metadata compaction lost its lease; the job is fenced and \
                     publishes nothing"
                );
                Ok(LeaseHold::Fenced)
            }
            Err(error) => Err(CoreError::store(&self.object_key, &error)),
        }
    }

    /// Opens the claim for a test that drives one phase of a job on its own.
    ///
    /// The driver creates the lease once and carries the same claim through
    /// the rebuild and the finalization. Tests split those two phases, so this
    /// creates the lease for the first of them and adopts the existing one for
    /// the second. A job never creates its lease twice.
    #[cfg(test)]
    pub(super) async fn open_for_test<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        job_id: &MetadataCompactionId,
        writer_id: &str,
        started_at_ms: u64,
        timer: &'a dyn MonotonicTimer,
    ) -> Result<Self> {
        let object_key = metadata_compaction_lease(namespace_id, job_id);
        let loaded = load_control_object(
            store,
            object_key.clone(),
            ControlObjectKind::CompactionLease,
            |state: &MetadataCompactionLeaseState| {
                expect_namespace(namespace_id, &state.namespace_id)?;
                expect_identity_field("compaction job id", job_id.as_str(), state.job_id.as_str())
            },
        )
        .await;
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(ControlObjectLoadError::MissingObject { .. }) => {
                return Self::create(store, namespace_id, job_id, writer_id, started_at_ms, timer)
                    .await
            }
            Err(error) => return Err(CoreError::ControlObjectLoad(error)),
        };
        let started_monotonic_ms = timer.monotonic_now_ms();
        Ok(Self {
            object_key,
            state: initial_lease_state(namespace_id, job_id, writer_id, started_at_ms),
            etag: loaded.etag,
            timer,
            started_monotonic_ms,
            next_heartbeat_monotonic_ms: started_monotonic_ms,
        })
    }
}

fn initial_lease_state(
    namespace_id: &NamespaceId,
    job_id: &MetadataCompactionId,
    writer_id: &str,
    started_at_ms: u64,
) -> MetadataCompactionLeaseState {
    MetadataCompactionLeaseState {
        job_id: job_id.clone(),
        namespace_id: namespace_id.clone(),
        writer_id: writer_id.to_owned(),
        status: CompactionLeaseStatus::Active {},
        started_at_ms,
        heartbeat_at_ms: started_at_ms,
    }
}

fn required_etag(object_key: &str, etag: Option<String>) -> Result<String> {
    etag.ok_or_else(|| {
        CoreError::Internal(format!(
            "the store returned no etag for the compaction lease `{object_key}`, so the job cannot \
             be fenced against garbage collection"
        ))
    })
}

fn encode_lease(state: &MetadataCompactionLeaseState) -> Result<Bytes> {
    let object_key = metadata_compaction_lease(&state.namespace_id, &state.job_id);
    encode_control_state(ControlObjectKind::CompactionLease, state)
        .map(Bytes::from)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}
