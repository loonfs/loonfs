//! The lease one streaming metadata compaction holds over its own output.
//!
//! A job writes every output segment under `metadata/compactions/{job_id}/`
//! and writes this object beside them. It carries lifecycle ownership and
//! nothing else — job, namespace, owner, start, last heartbeat — because the
//! one question it answers is whether the objects under its prefix belong to
//! a job that is still running. There is no cursor here, no output
//! descriptor, no offset, and no progress: a crashed job still loses its
//! work, exactly as the design says it does.
//!
//! Garbage collection is the only reader. It retains a prefix whose lease was
//! refreshed within [`METADATA_COMPACTION_LEASE_EXPIRY_MS`] and lets a prefix
//! with a stale or missing lease age out as ordinary orphans (format spec,
//! "Garbage collection", rule 12). Nothing else loads it, so a lease that
//! does not decode is treated as missing and logged: a corrupt lease must not
//! keep a prefix alive forever, and it must not stop anything else from
//! working either.

use crate::error::{CoreError, Result};
use crate::limits::{
    METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS, METADATA_COMPACTION_LEASE_EXPIRY_MS,
};
use crate::timing::MonotonicTimer;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectKind,
    MetadataCompactionLeaseEnvelope, MetadataCompactionLeaseState,
};
use loonfs_api::{MetadataCompactionId, NamespaceId};
use loonfs_objectstore::keys::metadata_compaction_lease;
use loonfs_objectstore::{ObjectStore, PutMode};

/// What a job's lease says about the objects under that job's prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionLeaseState {
    /// The lease was refreshed recently enough that the job may still be
    /// running, so everything under its prefix belongs to it.
    Held,
    /// There is no lease, or it does not decode, or its last heartbeat is
    /// older than the expiry. Whatever sits under the prefix is an ordinary
    /// unreferenced orphan.
    Gone,
}

/// Reads the lease of one job and says whether that job still owns its
/// prefix.
///
/// `job_id` is the path component, not a parsed identity: a collector reads
/// it off the key it is deciding. A lease naming a different job or a
/// different namespace is not this prefix's lease, so it is read as absent
/// whatever else it is.
pub(crate) async fn read_compaction_lease_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    job_id: &str,
    now_ms: u64,
) -> Result<CompactionLeaseState> {
    let object_key = metadata_compaction_lease(namespace_id.as_str(), job_id);
    let Some(bytes) = store
        .get(&object_key, None)
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?
    else {
        return Ok(CompactionLeaseState::Gone);
    };
    let decoded = decode_control_object::<MetadataCompactionLeaseState>(
        &bytes,
        ControlObjectKind::CompactionLease,
    );
    let state = match decoded {
        Ok(envelope) => envelope.state,
        Err(error) => {
            // Loud, because nothing else will ever say it: no read path
            // touches this object, so a corrupt one would otherwise sit under
            // its prefix in silence. Treated as missing, because the
            // alternative — believing it — would keep a prefix alive forever.
            tracing::warn!(
                namespace_id = namespace_id.as_str(),
                object_key = object_key.as_str(),
                error = %error,
                "a streaming metadata compaction lease does not decode; its prefix is collected \
                 as an orphan"
            );
            return Ok(CompactionLeaseState::Gone);
        }
    };
    if state.namespace_id != *namespace_id || state.job_id.as_str() != job_id {
        tracing::warn!(
            namespace_id = namespace_id.as_str(),
            object_key = object_key.as_str(),
            lease_namespace_id = state.namespace_id.as_str(),
            lease_job_id = state.job_id.as_str(),
            "a streaming metadata compaction lease names another job; its prefix is collected as \
             an orphan"
        );
        return Ok(CompactionLeaseState::Gone);
    }
    Ok(
        if now_ms
            <= state
                .heartbeat_at_ms
                .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS)
        {
            CompactionLeaseState::Held
        } else {
            CompactionLeaseState::Gone
        },
    )
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
                started_at_ms,
                heartbeat_at_ms: started_at_ms,
            },
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

    /// Rewrites the lease when the interval has passed since the last write.
    ///
    /// Called where cancellation is checked, so the cost is one small
    /// overwrite every [`METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS`] however
    /// many rows the job reads in between.
    pub(super) async fn heartbeat_if_due<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<()> {
        if self.timer.monotonic_now_ms() < self.next_heartbeat_monotonic_ms {
            return Ok(());
        }
        self.heartbeat(store).await
    }

    /// Rewrites the lease now, whatever the interval says.
    ///
    /// Finalization uses this: the span from a heartbeat to the root
    /// compare-and-swap that makes the output referenced is what the lease has
    /// to cover, and that span opens at the top of every attempt.
    pub(super) async fn heartbeat<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_monotonic_ms);
        self.state.heartbeat_at_ms = self.state.started_at_ms.saturating_add(elapsed_ms);
        let envelope = MetadataCompactionLeaseEnvelope::from_state(
            ControlObjectKind::CompactionLease,
            self.state.clone(),
        )
        .map_err(|error| {
            CoreError::Internal(format!("failed to build a compaction lease: {error}"))
        })?;
        let bytes = encode_control_object(&envelope).map_err(|error| {
            CoreError::Internal(format!("failed to encode a compaction lease: {error}"))
        })?;
        store
            .put(&self.object_key, bytes.into(), PutMode::Overwrite)
            .await
            .map_err(|error| CoreError::store(&self.object_key, &error))?;
        self.next_heartbeat_monotonic_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_add(METADATA_COMPACTION_HEARTBEAT_INTERVAL_MS);
        Ok(())
    }

    /// Drops the claim after the job's output has been published.
    ///
    /// Best effort: the published segments are named by the manifest and live
    /// wherever they sit, so a lease that survives protects an empty prefix
    /// until it expires and is then collected like any other orphan. Failing
    /// the job over it would throw away work that already landed.
    pub(super) async fn release<S: ObjectStore + ?Sized>(&self, store: &S) {
        if let Err(error) = store.delete(&self.object_key).await {
            tracing::info!(
                namespace_id = self.state.namespace_id.as_str(),
                object_key = self.object_key.as_str(),
                error = %error,
                "a published streaming metadata compaction could not delete its own lease; it \
                 expires on its own"
            );
        }
    }
}
