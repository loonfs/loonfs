//! Lease for a streaming compaction's staged output.
//!
//! A job creates and refreshes an `Active` lease with compare-and-swap.
//! Garbage collection may claim an expired lease by changing it to `Reaping`.
//! A failed refresh then prevents the job from publishing.
//!
//! Before publishing, a job records a protection deadline beside its sealed
//! output. That record remains until every output object is gone, even after
//! the group admits another job. Paused collectors keep their fixed clock.
//! Malformed leases stop collection because ownership cannot be verified.

use super::streaming_compaction::MetadataCompactionSpec;
use crate::control_object::{
    expect_identity_field, expect_namespace, load_control_object, ControlObjectLoadError,
};
use crate::error::{CoreError, Result};
use crate::limits::{
    METADATA_COMPACTION_LEASE_EXPIRY_MS, METADATA_COMPACTION_LEASE_REFRESH_INTERVAL_MS,
};
use crate::time::MonotonicTimer;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_state, CompactionLeaseStatus, CompactionOutputProtectionState,
    ControlObjectKind, MetadataCompactionLeaseState,
};
use loonfs_api::{MetadataCompactionId, MetadataFamilyGroup, NamespaceId, WriterId};
use loonfs_objectstore::keys::{metadata_compaction_lease, metadata_compaction_output_protection};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

/// Who owns the objects under one job's prefix, as a collector reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionPrefixOwner {
    /// A live job or publication deadline protects this output, regardless
    /// of object age. A lost fencing CAS also retains output for this pass.
    Protected,
    /// This pass or another collector fenced the job. Unreferenced output
    /// can be collected; the group slot remains available for replacement.
    Fenced,
    /// There is no lease to own anything. Whatever sits under the prefix is
    /// an ordinary unreferenced orphan.
    Unclaimed,
}

/// Returns the current prefix owner, claiming its expired group lease when possible.
#[cfg(test)]
pub(crate) async fn claim_group_lease<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    metadata_compaction_id: &MetadataCompactionId,
    now_ms: u64,
) -> Result<CompactionPrefixOwner> {
    let Some(loaded) = load_group_lease(store, namespace_id, group).await? else {
        return Ok(CompactionPrefixOwner::Unclaimed);
    };
    claim_loaded_group_lease(store, namespace_id, metadata_compaction_id, loaded, now_ms).await
}

pub(crate) type LoadedCompactionLease =
    crate::control_object::LoadedControl<MetadataCompactionLeaseState>;

pub(crate) async fn load_group_lease<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
) -> Result<Option<LoadedCompactionLease>> {
    let loaded = load_control_object(
        store,
        metadata_compaction_lease(namespace_id, group),
        ControlObjectKind::CompactionLease,
        |state: &MetadataCompactionLeaseState| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_identity_field(
                "metadata family group",
                group.as_str(),
                state.group.as_str(),
            )
        },
    )
    .await;
    match loaded {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

/// Reads the sealed output's publication deadline independently of the group.
pub(crate) async fn load_output_protection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    job_id: &MetadataCompactionId,
) -> Result<Option<crate::control_object::LoadedControl<CompactionOutputProtectionState>>> {
    let loaded = load_control_object(
        store,
        metadata_compaction_output_protection(namespace_id, job_id),
        ControlObjectKind::CompactionOutputProtection,
        |state: &CompactionOutputProtectionState| {
            expect_namespace(namespace_id, &state.namespace_id)?;
            expect_identity_field("compaction job", job_id.as_str(), state.job_id.as_str())
        },
    )
    .await;
    match loaded {
        Ok(loaded) => Ok(Some(loaded)),
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

pub(crate) async fn claim_loaded_group_lease<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    metadata_compaction_id: &MetadataCompactionId,
    loaded: LoadedCompactionLease,
    now_ms: u64,
) -> Result<CompactionPrefixOwner> {
    let object_key = loaded.object_key;
    let expected_etag = loaded.etag;
    let state = loaded.state;
    // A group lease protects only the staged objects written by the job id it names.
    if state.job_id != *metadata_compaction_id {
        return Ok(CompactionPrefixOwner::Unclaimed);
    }
    // Publication has ended. Its separate record protects older collectors.
    if state.status == (CompactionLeaseStatus::Completed {}) {
        return Ok(CompactionPrefixOwner::Unclaimed);
    }
    // Terminal: somebody already fenced this job, so the reap goes on from
    // wherever the pass that started it stopped.
    if state.status == (CompactionLeaseStatus::Reaping {}) {
        return Ok(CompactionPrefixOwner::Fenced);
    }
    if active_lease_is_unexpired(&state, now_ms) {
        return Ok(CompactionPrefixOwner::Protected);
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
                expires_at_ms = reaping.expires_at_ms,
                "a streaming metadata compaction lease expired; garbage collection claimed the \
                 group lease and the job that wrote it can no longer publish"
            );
            Ok(CompactionPrefixOwner::Fenced)
        }
        // The job wrote the lease between this pass's read and its claim, so
        // the job is alive and owns its prefix. This pass keeps every object
        // under it.
        Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(CompactionPrefixOwner::Protected),
        // An ambiguous claim is left to a later collector, which reads the
        // lease afresh and finishes whatever landed.
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// Availability of one family group's deterministic lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupLeaseState {
    /// No unexpired job owns the group.
    Available,
    /// An unexpired job holds the lease.
    Held,
}

/// Reads one family group's lease for planning.
pub(super) async fn group_lease_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    now_ms: u64,
) -> Result<GroupLeaseState> {
    let Some(loaded) = load_group_lease(store, namespace_id, group).await? else {
        return Ok(GroupLeaseState::Available);
    };
    Ok(if active_lease_is_unexpired(&loaded.state, now_ms) {
        GroupLeaseState::Held
    } else {
        GroupLeaseState::Available
    })
}

fn active_lease_is_unexpired(state: &MetadataCompactionLeaseState, now_ms: u64) -> bool {
    state.status == (CompactionLeaseStatus::Active {}) && now_ms <= state.expires_at_ms
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

/// Result of trying to acquire a family group's lease.
pub(super) enum LeaseAcquire<'a> {
    /// This job owns the lease.
    Acquired(CompactionLease<'a>),
    /// Another unexpired job holds the lease.
    Held,
}

/// One running job's claim on its own prefix.
///
/// The wall clock is read once, when the job starts; every later refresh
/// derives a new expiry from that instant plus local monotonic elapsed time,
/// so nothing here depends on a clock that can move. That is the same posture every
/// self-enforced budget in the system takes.
pub(super) struct CompactionLease<'a> {
    object_key: String,
    state: MetadataCompactionLeaseState,
    etag: String,
    timer: &'a dyn MonotonicTimer,
    started_monotonic_ms: u64,
    next_refresh_monotonic_ms: u64,
}

impl<'a> CompactionLease<'a> {
    /// Acquires the group lease before the job's first output object.
    pub(super) async fn acquire<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
        writer_id: &WriterId,
        started_at_ms: u64,
        timer: &'a dyn MonotonicTimer,
    ) -> Result<LeaseAcquire<'a>> {
        let started_monotonic_ms = timer.monotonic_now_ms();
        let object_key = metadata_compaction_lease(namespace_id, spec.group());
        let state = initial_lease_state(namespace_id, spec, writer_id, started_at_ms);
        let encoded = encode_lease(&state)?;
        let loaded = load_group_lease(store, namespace_id, spec.group()).await?;
        let etag = match loaded {
            Some(loaded) => {
                return Self::replace_available(
                    store,
                    loaded,
                    object_key,
                    state,
                    encoded,
                    timer,
                    started_monotonic_ms,
                )
                .await
            }
            None => match store.put_if_absent(&object_key, encoded.clone()).await {
                Ok(metadata) => required_etag(&object_key, metadata.etag)?,
                Err(ObjectStoreError::PreconditionFailed { .. }) => {
                    let Some(loaded) = load_group_lease(store, namespace_id, spec.group()).await?
                    else {
                        return Ok(LeaseAcquire::Held);
                    };
                    return Self::replace_available(
                        store,
                        loaded,
                        object_key,
                        state,
                        encoded,
                        timer,
                        started_monotonic_ms,
                    )
                    .await;
                }
                Err(error @ ObjectStoreError::Transport { .. }) => {
                    let Some(loaded) = load_group_lease(store, namespace_id, spec.group()).await?
                    else {
                        return Err(CoreError::store(&object_key, &error));
                    };
                    if loaded.state == state {
                        loaded.etag
                    } else {
                        return Self::replace_available(
                            store,
                            loaded,
                            object_key,
                            state,
                            encoded,
                            timer,
                            started_monotonic_ms,
                        )
                        .await;
                    }
                }
                Err(error) => return Err(CoreError::store(&object_key, &error)),
            },
        };
        Ok(LeaseAcquire::Acquired(Self {
            object_key,
            state,
            etag,
            timer,
            started_monotonic_ms,
            next_refresh_monotonic_ms: started_monotonic_ms,
        }))
    }

    async fn replace_available<S: ObjectStore + ?Sized>(
        store: &S,
        loaded: LoadedCompactionLease,
        object_key: String,
        state: MetadataCompactionLeaseState,
        encoded: Bytes,
        timer: &'a dyn MonotonicTimer,
        started_monotonic_ms: u64,
    ) -> Result<LeaseAcquire<'a>> {
        if active_lease_is_unexpired(&loaded.state, state.started_at_ms) {
            return Ok(LeaseAcquire::Held);
        }
        // Replacing the complete slot in one CAS fences the previous job's next refresh.
        let etag = match store
            .compare_and_swap(&object_key, &loaded.etag, encoded)
            .await
        {
            Ok(metadata) => required_etag(&object_key, metadata.etag)?,
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::NotFound { .. },
            ) => return Ok(LeaseAcquire::Held),
            Err(error @ ObjectStoreError::Transport { .. }) => {
                let Some(confirmed) =
                    load_group_lease(store, &state.namespace_id, state.group).await?
                else {
                    return Err(CoreError::store(&object_key, &error));
                };
                if confirmed.state != state {
                    return Ok(LeaseAcquire::Held);
                }
                confirmed.etag
            }
            Err(error) => return Err(CoreError::store(&object_key, &error)),
        };
        Ok(LeaseAcquire::Acquired(Self {
            object_key,
            state,
            etag,
            timer,
            started_monotonic_ms,
            next_refresh_monotonic_ms: started_monotonic_ms,
        }))
    }

    /// Confirms output protection before a root publication can make old output live.
    /// No further output writes are permitted after the first call.
    pub(super) async fn protect_output<S: ObjectStore + ?Sized>(&self, store: &S) -> Result<()> {
        let key =
            metadata_compaction_output_protection(&self.state.namespace_id, &self.state.job_id);
        let existing =
            load_output_protection(store, &self.state.namespace_id, &self.state.job_id).await?;
        let state = CompactionOutputProtectionState {
            namespace_id: self.state.namespace_id.clone(),
            job_id: self.state.job_id.clone(),
            expires_at_ms: existing
                .as_ref()
                .map_or(self.state.expires_at_ms, |loaded| {
                    loaded.state.expires_at_ms.max(self.state.expires_at_ms)
                }),
        };
        if existing
            .as_ref()
            .is_some_and(|loaded| loaded.state == state)
        {
            return Ok(());
        }
        let bytes = encode_control_state(ControlObjectKind::CompactionOutputProtection, &state)
            .map(Bytes::from)
            .map_err(|error| CoreError::Codec {
                object_key: key.clone(),
                message: error.to_string(),
            })?;
        let result = match existing {
            Some(loaded) => store.compare_and_swap(&key, &loaded.etag, bytes).await,
            None => store.put_if_absent(&key, bytes).await,
        };
        match result {
            Ok(_) => Ok(()),
            Err(
                error @ (ObjectStoreError::PreconditionFailed { .. }
                | ObjectStoreError::Transport { .. }),
            ) => {
                let confirmed =
                    load_output_protection(store, &state.namespace_id, &state.job_id).await?;
                if confirmed.is_some_and(|loaded| loaded.state == state) {
                    Ok(())
                } else {
                    Err(CoreError::store(&key, &error))
                }
            }
            Err(error) => Err(CoreError::store(&key, &error)),
        }
    }

    /// Releases the group after the last publication attempt. The separate
    /// output protection was confirmed before publication, so a pause here
    /// cannot leave published output dependent on the group slot.
    pub(super) async fn complete<S: ObjectStore + ?Sized>(&self, store: &S) -> Result<()> {
        let mut completed = self.state.clone();
        completed.status = CompactionLeaseStatus::Completed {};
        match store
            .compare_and_swap(&self.object_key, &self.etag, encode_lease(&completed)?)
            .await
        {
            Ok(_)
            | Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::NotFound { .. },
            ) => Ok(()),
            Err(error) => Err(CoreError::store(&self.object_key, &error)),
        }
    }

    /// The clock the job paces itself by. One clock for the job's refresh
    /// and its publication budget, because they measure the same span.
    pub(super) fn timer(&self) -> &'a dyn MonotonicTimer {
        self.timer
    }

    /// Refreshes the lease when the interval has passed since the last write.
    ///
    /// Called where cancellation is checked, so the cost is one small
    /// compare-and-swap every [`METADATA_COMPACTION_LEASE_REFRESH_INTERVAL_MS`]
    /// however many rows the job reads in between.
    pub(super) async fn refresh_if_due<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<LeaseHold> {
        if self.timer.monotonic_now_ms() < self.next_refresh_monotonic_ms {
            return Ok(LeaseHold::Held);
        }
        self.refresh(store).await
    }

    /// Refreshes the lease now, whatever the interval says.
    ///
    /// Finalization uses this: the span from a refresh to the root
    /// compare-and-swap that makes the output referenced is what the lease has
    /// to cover, and that span opens at the top of every attempt.
    pub(super) async fn refresh<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<LeaseHold> {
        let expected_etag = self.etag.clone();
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_monotonic_ms);
        self.state.expires_at_ms = self
            .state
            .started_at_ms
            .saturating_add(elapsed_ms)
            .saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS);
        let encoded = encode_lease(&self.state)?;
        match store
            .compare_and_swap(&self.object_key, &expected_etag, encoded)
            .await
        {
            Ok(metadata) => {
                self.etag = required_etag(&self.object_key, metadata.etag)?;
                self.next_refresh_monotonic_ms = self
                    .timer
                    .monotonic_now_ms()
                    .saturating_add(METADATA_COMPACTION_LEASE_REFRESH_INTERVAL_MS);
                Ok(LeaseHold::Held)
            }
            // Garbage collection claimed the group lease, or the object is gone.
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
            // An unconfirmed refresh has no etag for the next fenced write,
            // so the job ends here.
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
        spec: &MetadataCompactionSpec,
        writer_id: &WriterId,
        started_at_ms: u64,
        timer: &'a dyn MonotonicTimer,
    ) -> Result<Self> {
        match Self::acquire(store, namespace_id, spec, writer_id, started_at_ms, timer).await? {
            LeaseAcquire::Acquired(lease) => return Ok(lease),
            LeaseAcquire::Held => {}
        }
        let object_key = metadata_compaction_lease(namespace_id, spec.group());
        let Some(loaded) = load_group_lease(store, namespace_id, spec.group()).await? else {
            return Err(CoreError::Internal(format!(
                "the compaction lease `{object_key}` disappeared while a test reopened it"
            )));
        };
        if loaded.state.job_id != *spec.job_id()
            || loaded.state.status != (CompactionLeaseStatus::Active {})
        {
            return Err(CoreError::Internal(format!(
                "the compaction lease `{object_key}` is held by another owner"
            )));
        }
        let started_monotonic_ms = timer.monotonic_now_ms();
        Ok(Self {
            object_key,
            state: loaded.state,
            etag: loaded.etag,
            timer,
            started_monotonic_ms,
            next_refresh_monotonic_ms: started_monotonic_ms,
        })
    }
}

fn initial_lease_state(
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    writer_id: &WriterId,
    started_at_ms: u64,
) -> MetadataCompactionLeaseState {
    MetadataCompactionLeaseState {
        job_id: spec.job_id().clone(),
        namespace_id: namespace_id.clone(),
        group: spec.group(),
        writer_id: writer_id.clone(),
        status: CompactionLeaseStatus::Active {},
        started_at_ms,
        expires_at_ms: started_at_ms.saturating_add(METADATA_COMPACTION_LEASE_EXPIRY_MS),
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
    let object_key = metadata_compaction_lease(&state.namespace_id, state.group);
    encode_control_state(ControlObjectKind::CompactionLease, state)
        .map(Bytes::from)
        .map_err(|error| CoreError::Codec {
            object_key,
            message: error.to_string(),
        })
}
