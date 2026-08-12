//! The lease one streaming metadata compaction holds over its own output.
//!
//! A job writes every output segment under `metadata/compactions/{job_id}/`
//! and writes this object beside them. It carries lifecycle ownership and
//! nothing else — job, namespace, owner, status, start, last heartbeat —
//! because the one question it answers is who owns the objects under its
//! prefix. There is no cursor here, no output descriptor, no offset, and no
//! progress: a crashed job still loses its work, exactly as the design says
//! it does.
//!
//! The lease is a fence, not a timestamp. Every write after the first is a
//! compare-and-swap on the etag the writer last observed, so the job and the
//! collector cannot both believe they own the prefix. The job creates the
//! lease with `put_if_absent` and refreshes it by compare-and-swap; garbage
//! collection claims an expired lease by compare-and-swapping it from
//! `Active` to `Reaping` and only then treats the prefix as orphaned (format
//! spec, "Garbage collection", rule 12). Exactly one of those two
//! compare-and-swaps can win:
//!
//! - the job's heartbeat wins, so the collector keeps the prefix; or
//! - the collector's claim wins, so the job is fenced, aborts, and publishes
//!   nothing.
//!
//! A published job stops heartbeating and leaves its final lease in place. It
//! is not deleted, because deleting it would open the one hole a lease exists
//! to close: a collection pass that captured its live set before the
//! publication would find output objects far older than any grace window and
//! no lease saying who owns them. The lease expires on its own, and the pass
//! that finds it expired reads a root that already names the segments.
//!
//! A malformed lease is corruption rather than evidence that its prefix is
//! unowned; collection stops before it can race a job whose fence it cannot
//! verify.

use crate::control_object::{
    core_control_load_error, expect_identity_field, expect_namespace, load_control_object,
    ControlObjectLoadError,
};
use crate::error::{CoreError, Result};
use crate::limits::{
    METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS, METADATA_COMPACTION_LEASE_EXPIRY_MS,
};
use crate::timing::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, MetadataCompactionLeaseEnvelope,
    MetadataCompactionLeaseState, MetadataCompactionLeaseStatus,
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
    ALiveJob,
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

/// Reads the lease of one job and says who owns that job's prefix, claiming
/// an expired lease on the way.
///
/// `job_id` is the path component, not a parsed identity: a collector reads
/// it off the key it is deciding. A lease naming a different job or namespace
/// is corrupt because the key and embedded fence disagree.
///
/// The claim is the whole point of reading it. An expired `Active` lease
/// alone proves nothing — the job may be resuming from a long stall right
/// now — so the prefix is orphaned only once this compare-and-swap has landed
/// and the job's next heartbeat is guaranteed to fail.
pub(crate) async fn claim_compaction_prefix<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    job_id: &str,
    now_ms: u64,
) -> Result<CompactionPrefixOwner> {
    let object_key = metadata_compaction_lease(namespace_id.as_str(), job_id);
    let loaded = load_control_object(
        store,
        object_key,
        ControlObjectKind::CompactionLease,
        |state: &MetadataCompactionLeaseState| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_identity_field("compaction job id", job_id, state.job_id.as_str())
        },
    )
    .await;
    let loaded = match loaded {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(CompactionPrefixOwner::NoOne)
        }
        Err(error) => return Err(core_control_load_error(error)),
    };
    let object_key = loaded.object_key;
    let expected_etag = loaded.etag;
    let state = loaded.state;
    // Terminal: somebody already fenced this job, so the reap goes on from
    // wherever the pass that started it stopped.
    if state.status == MetadataCompactionLeaseStatus::Reaping {
        return Ok(CompactionPrefixOwner::ThisCollector);
    }
    if now_ms
        <= state
            .heartbeat_at_ms
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS)
    {
        return Ok(CompactionPrefixOwner::ALiveJob);
    }

    // Expired. Nothing is decided until the claim lands.
    let mut reaping = state;
    reaping.status = MetadataCompactionLeaseStatus::Reaping;
    let encoded = encode_lease(&reaping)?;
    match store
        .compare_and_swap(&object_key, &expected_etag, encoded)
        .await
    {
        Ok(_) => {
            tracing::info!(
                namespace_id = namespace_id.as_str(),
                object_key = object_key.as_str(),
                job_id,
                owner_id = reaping.owner_id.as_str(),
                heartbeat_at_ms = reaping.heartbeat_at_ms,
                "a streaming metadata compaction lease expired; garbage collection claimed its \
                 prefix and the job that wrote it can no longer publish"
            );
            Ok(CompactionPrefixOwner::ThisCollector)
        }
        // The job wrote the lease between this pass's read and its claim, so
        // the job is alive and owns its prefix. This pass keeps every object
        // under it.
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CompactionPrefixOwner::ALiveJob),
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
    /// The etag of the last lease write this job made, which every later
    /// write compare-and-swaps against. `None` before the create, and never
    /// `None` after one: a store that returns no etag on the create fails the
    /// job there rather than letting it run unfenceable.
    etag: Option<String>,
    timer: &'a dyn MonotonicTimer,
    started_monotonic_ms: u64,
    next_heartbeat_monotonic_ms: u64,
}

impl<'a> CompactionLease<'a> {
    pub(super) fn new(
        namespace_id: &NamespaceId,
        job_id: &MetadataCompactionId,
        owner_id: &str,
        started_at_ms: u64,
        timer: &'a dyn MonotonicTimer,
    ) -> Self {
        let started_monotonic_ms = timer.monotonic_now_ms();
        Self {
            object_key: metadata_compaction_lease(namespace_id.as_str(), job_id.as_str()),
            state: MetadataCompactionLeaseState {
                job_id: job_id.clone(),
                namespace_id: namespace_id.clone(),
                owner_id: owner_id.to_owned(),
                status: MetadataCompactionLeaseStatus::Active,
                started_at_ms,
                heartbeat_at_ms: started_at_ms,
            },
            etag: None,
            timer,
            started_monotonic_ms,
            next_heartbeat_monotonic_ms: started_monotonic_ms,
        }
    }

    /// The clock the job paces itself by. One clock for the job's heartbeat
    /// and its publication budget, because they measure the same span.
    pub(super) fn timer(&self) -> &'a dyn MonotonicTimer {
        self.timer
    }

    /// Writes the lease for the first time, before the job's first output
    /// object, so no object under the prefix is ever unclaimed.
    ///
    /// Create-if-absent, like every other object written under a freshly
    /// generated id: an occupied key means two generated job ids collided,
    /// which is a broken generator rather than a lifecycle a caller could
    /// resolve.
    pub(super) async fn create<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let encoded = encode_lease(&self.state)?;
        // A lease is mutable control state, so its first lifecycle state is a conditional create.
        let metadata = match store.put_if_absent(&self.object_key, encoded).await {
            Ok(metadata) => metadata,
            Err(ObjectStoreError::PreconditionFailed { .. }) => {
                return Err(CoreError::Internal(format!(
                    "generated compaction job id collided with the existing lease `{}`",
                    self.object_key
                )))
            }
            Err(error) => return Err(CoreError::store(&self.object_key, &error)),
        };
        self.etag = Some(self.required_etag(metadata.etag)?);
        Ok(())
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
        let expected_etag = self.etag.clone().ok_or_else(|| {
            CoreError::Internal(
                "a compaction lease heartbeat ran before the \
                 lease was created"
                    .to_owned(),
            )
        })?;
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
                self.etag = Some(self.required_etag(metadata.etag)?);
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
    pub(super) async fn open_for_test<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let namespace_id = self.state.namespace_id.clone();
        let job_id = self.state.job_id.clone();
        let loaded = load_control_object(
            store,
            self.object_key.clone(),
            ControlObjectKind::CompactionLease,
            |state: &MetadataCompactionLeaseState| {
                expect_namespace(&namespace_id, &state.namespace_id)?;
                expect_identity_field("compaction job id", job_id.as_str(), state.job_id.as_str())
            },
        )
        .await;
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(ControlObjectLoadError::MissingObject { .. }) => return self.create(store).await,
            Err(error) => return Err(core_control_load_error(error)),
        };
        self.etag = Some(loaded.etag);
        Ok(())
    }

    fn required_etag(&self, etag: Option<String>) -> Result<String> {
        etag.ok_or_else(|| {
            CoreError::Internal(format!(
                "the store returned no etag for the compaction lease `{}`, so the job cannot be \
                 fenced against garbage collection",
                self.object_key
            ))
        })
    }
}

fn encode_lease(state: &MetadataCompactionLeaseState) -> Result<Bytes> {
    let envelope = MetadataCompactionLeaseEnvelope::from_state(
        ControlObjectKind::CompactionLease,
        state.clone(),
    )
    .map_err(|error| CoreError::Internal(format!("failed to build a compaction lease: {error}")))?;
    encode_control_object(&envelope)
        .map(Bytes::from)
        .map_err(|error| {
            CoreError::Internal(format!("failed to encode a compaction lease: {error}"))
        })
}
