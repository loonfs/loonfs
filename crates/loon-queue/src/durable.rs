use crate::broker::{renew_broker_lease, BrokerLeaseError, BrokerLeaseOutcome};
use crate::repair::{repair_lost_snapshot_enqueue, SnapshotRepairError, SnapshotRepairOutcome};
use crate::types::{QueueShardEnvelope, QueueShardState, WorkClass};
use crate::worker::{
    claim_job, complete_job, heartbeat_job, JobClaimOutcome, JobCompleteOutcome,
    JobHeartbeatOutcome, WorkerMutationError,
};
use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::queue_shard;
use loon_objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{
    payload_checksum_sha256, ControlObjectEnvelope, ControlObjectKind, HeadState, ProgressState,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredQueueShardObject {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedQueueShardObject {
    pub object_key: String,
    pub envelope: QueueShardEnvelope,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueShardLoadError {
    MissingObject {
        object_key: String,
    },
    ObjectKeyMismatch {
        expected: String,
        actual: String,
    },
    KindMismatch {
        expected: ControlObjectKind,
        actual: ControlObjectKind,
    },
    ShardIdMismatch {
        expected: u32,
        actual: u32,
    },
    ChecksumMismatch {
        expected: String,
        actual: String,
    },
    Codec(String),
    Store(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedQueueShardMutation<T> {
    pub metadata: ObjectMetadata,
    pub mutation: T,
    pub shard: LoadedQueueShardObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableQueueShardWriteOutcome<T> {
    Created(PersistedQueueShardMutation<T>),
    Updated(PersistedQueueShardMutation<T>),
}

pub type DurableBrokerLeaseOutcome = DurableQueueShardWriteOutcome<BrokerLeaseOutcome>;
pub type DurableJobClaimOutcome = DurableQueueShardWriteOutcome<JobClaimOutcome>;
pub type DurableJobHeartbeatOutcome = DurableQueueShardWriteOutcome<JobHeartbeatOutcome>;
pub type DurableJobCompleteOutcome = DurableQueueShardWriteOutcome<JobCompleteOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableQueueShardMutationError<E> {
    EmptyWriterVersion,
    MissingObject { object_key: String },
    MissingObjectAfterHead { object_key: String },
    MissingObjectEtag { object_key: String },
    Load(QueueShardLoadError),
    Mutation(E),
    ConcurrentWrite,
    Codec(String),
    Store(String),
}

pub type DurableBrokerLeaseError = DurableQueueShardMutationError<BrokerLeaseError>;
pub type DurableWorkerMutationError = DurableQueueShardMutationError<WorkerMutationError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedQueueShardRepair {
    pub metadata: ObjectMetadata,
    pub repair: SnapshotRepairOutcome,
    pub shard: LoadedQueueShardObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableSnapshotRepairOutcome {
    Created(PersistedQueueShardRepair),
    Updated(PersistedQueueShardRepair),
    NoChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableSnapshotRepairError {
    EmptyWriterVersion,
    MissingObjectAfterHead { object_key: String },
    MissingObjectEtag { object_key: String },
    Load(QueueShardLoadError),
    Repair(SnapshotRepairError),
    ConcurrentWrite,
    Codec(String),
    Store(String),
}

pub fn load_queue_shard(
    shard_index: u32,
    stored: &StoredQueueShardObject,
) -> Result<LoadedQueueShardObject, QueueShardLoadError> {
    let expected_key = queue_shard(shard_index);
    if stored.object_key != expected_key {
        return Err(QueueShardLoadError::ObjectKeyMismatch {
            expected: expected_key,
            actual: stored.object_key.clone(),
        });
    }

    let envelope: QueueShardEnvelope = serde_json::from_slice(&stored.encoded_bytes)
        .map_err(|err| QueueShardLoadError::Codec(err.to_string()))?;
    if envelope.kind != ControlObjectKind::QueueShard {
        return Err(QueueShardLoadError::KindMismatch {
            expected: ControlObjectKind::QueueShard,
            actual: envelope.kind,
        });
    }

    let actual_checksum = payload_checksum_sha256(&envelope.state)
        .map_err(|err| QueueShardLoadError::Codec(err.to_string()))?;
    if envelope.payload_checksum_sha256 != actual_checksum {
        return Err(QueueShardLoadError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual: actual_checksum,
        });
    }

    if envelope.state.shard_id != shard_index {
        return Err(QueueShardLoadError::ShardIdMismatch {
            expected: shard_index,
            actual: envelope.state.shard_id,
        });
    }

    Ok(LoadedQueueShardObject {
        object_key: stored.object_key.clone(),
        envelope,
        checked_invariants: vec![
            "queue_shard_checksum_matches_payload".to_owned(),
            "queue_shard_key_matches_shard_id".to_owned(),
        ],
    })
}

pub fn read_queue_shard<S: ObjectStore>(
    store: &S,
    shard_index: u32,
) -> Result<LoadedQueueShardObject, QueueShardLoadError> {
    let object_key = queue_shard(shard_index);
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_queue_load_store_error)?
        .ok_or_else(|| QueueShardLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;

    load_queue_shard(
        shard_index,
        &StoredQueueShardObject {
            object_key,
            encoded_bytes,
        },
    )
}

pub fn renew_broker_lease_in_store<S: ObjectStore>(
    store: &S,
    shard_index: u32,
    work_class: WorkClass,
    broker_id: &str,
    now_ms: u64,
    lease_duration_ms: u64,
    writer_version: &str,
) -> Result<DurableBrokerLeaseOutcome, DurableBrokerLeaseError> {
    if writer_version.trim().is_empty() {
        return Err(DurableBrokerLeaseError::EmptyWriterVersion);
    }

    let object_key = queue_shard(shard_index);
    match store
        .head(&object_key)
        .map_err(map_queue_mutation_store_error::<BrokerLeaseError>)?
    {
        Some(_) => mutate_existing_queue_shard(store, shard_index, writer_version, |next_state| {
            let outcome = renew_broker_lease(next_state, broker_id, now_ms, lease_duration_ms)?;
            Ok((outcome.clone(), broker_lease_invariants(&outcome)))
        }),
        None => {
            let mut next_state = QueueShardState {
                work_class,
                shard_id: shard_index,
                broker: None,
                jobs: vec![],
            };
            let outcome = renew_broker_lease(&mut next_state, broker_id, now_ms, lease_duration_ms)
                .map_err(DurableBrokerLeaseError::Mutation)?;
            let next = merge_queue_mutation_invariants(
                build_loaded_queue_shard(next_state, writer_version)
                    .map_err(DurableBrokerLeaseError::Codec)?,
                None,
                shard_index,
                &broker_lease_invariants(&outcome),
            );
            let metadata = store
                .put_if_absent(
                    &object_key,
                    &encode_queue_shard_bytes(&next).map_err(DurableBrokerLeaseError::Codec)?,
                )
                .map_err(map_queue_mutation_store_error::<BrokerLeaseError>)?;

            Ok(DurableBrokerLeaseOutcome::Created(
                PersistedQueueShardMutation {
                    metadata,
                    mutation: outcome,
                    shard: next,
                },
            ))
        }
    }
}

pub fn claim_job_in_store<S: ObjectStore>(
    store: &S,
    shard_index: u32,
    broker_id: &str,
    broker_epoch: u64,
    worker_id: &str,
    claim_token: &str,
    job_id: &str,
    now_ms: u64,
    claim_timeout_ms: u64,
    writer_version: &str,
) -> Result<DurableJobClaimOutcome, DurableWorkerMutationError> {
    mutate_existing_queue_shard(store, shard_index, writer_version, |next_state| {
        let outcome = claim_job(
            next_state,
            broker_id,
            broker_epoch,
            worker_id,
            claim_token,
            job_id,
            now_ms,
            claim_timeout_ms,
        )?;
        Ok((outcome.clone(), claim_invariants(&outcome)))
    })
}

pub fn heartbeat_job_in_store<S: ObjectStore>(
    store: &S,
    shard_index: u32,
    broker_id: &str,
    broker_epoch: u64,
    job_id: &str,
    claim_token: &str,
    now_ms: u64,
    claim_timeout_ms: u64,
    writer_version: &str,
) -> Result<DurableJobHeartbeatOutcome, DurableWorkerMutationError> {
    mutate_existing_queue_shard(store, shard_index, writer_version, |next_state| {
        let outcome = heartbeat_job(
            next_state,
            broker_id,
            broker_epoch,
            job_id,
            claim_token,
            now_ms,
            claim_timeout_ms,
        )?;
        Ok((outcome, heartbeat_invariants()))
    })
}

pub fn complete_job_in_store<S: ObjectStore>(
    store: &S,
    shard_index: u32,
    broker_id: &str,
    broker_epoch: u64,
    job_id: &str,
    claim_token: &str,
    now_ms: u64,
    writer_version: &str,
) -> Result<DurableJobCompleteOutcome, DurableWorkerMutationError> {
    mutate_existing_queue_shard(store, shard_index, writer_version, |next_state| {
        let outcome = complete_job(
            next_state,
            broker_id,
            broker_epoch,
            job_id,
            claim_token,
            now_ms,
        )?;
        Ok((outcome, complete_invariants()))
    })
}

pub fn repair_lost_snapshot_enqueue_in_store<S: ObjectStore>(
    store: &S,
    shard_index: u32,
    head: &HeadState,
    progress: Option<&ProgressState>,
    writer_version: &str,
) -> Result<DurableSnapshotRepairOutcome, DurableSnapshotRepairError> {
    if writer_version.trim().is_empty() {
        return Err(DurableSnapshotRepairError::EmptyWriterVersion);
    }

    let object_key = queue_shard(shard_index);
    match store.head(&object_key).map_err(map_repair_store_error)? {
        Some(metadata) => {
            let etag = metadata.etag.clone().ok_or_else(|| {
                DurableSnapshotRepairError::MissingObjectEtag {
                    object_key: object_key.clone(),
                }
            })?;
            let encoded_bytes = store
                .get(&object_key, None)
                .map_err(map_repair_store_error)?
                .ok_or_else(|| DurableSnapshotRepairError::MissingObjectAfterHead {
                    object_key: object_key.clone(),
                })?;
            let current = load_queue_shard(
                shard_index,
                &StoredQueueShardObject {
                    object_key: object_key.clone(),
                    encoded_bytes,
                },
            )
            .map_err(DurableSnapshotRepairError::Load)?;

            let mut next_state = current.envelope.state.clone();
            let repair = repair_lost_snapshot_enqueue(&mut next_state, head, progress)
                .map_err(DurableSnapshotRepairError::Repair)?;
            if matches!(repair, SnapshotRepairOutcome::NoRepairNeeded) {
                return Ok(DurableSnapshotRepairOutcome::NoChange);
            }

            let next = build_loaded_queue_shard(next_state, writer_version)
                .map_err(DurableSnapshotRepairError::Codec)?;
            let metadata = store
                .compare_and_swap(
                    &object_key,
                    &etag,
                    &encode_queue_shard_bytes(&next).map_err(DurableSnapshotRepairError::Codec)?,
                )
                .map_err(map_repair_store_error)?;

            Ok(DurableSnapshotRepairOutcome::Updated(
                PersistedQueueShardRepair {
                    metadata,
                    repair: repair.clone(),
                    shard: merge_repair_invariants(next, Some(&current), shard_index, &repair),
                },
            ))
        }
        None => {
            let mut next_state = QueueShardState {
                work_class: WorkClass::BuildSnapshot,
                shard_id: shard_index,
                broker: None,
                jobs: vec![],
            };
            let repair = repair_lost_snapshot_enqueue(&mut next_state, head, progress)
                .map_err(DurableSnapshotRepairError::Repair)?;
            if matches!(repair, SnapshotRepairOutcome::NoRepairNeeded) {
                return Ok(DurableSnapshotRepairOutcome::NoChange);
            }

            let next = build_loaded_queue_shard(next_state, writer_version)
                .map_err(DurableSnapshotRepairError::Codec)?;
            let metadata = store
                .put_if_absent(
                    &object_key,
                    &encode_queue_shard_bytes(&next).map_err(DurableSnapshotRepairError::Codec)?,
                )
                .map_err(map_repair_store_error)?;

            Ok(DurableSnapshotRepairOutcome::Created(
                PersistedQueueShardRepair {
                    metadata,
                    repair: repair.clone(),
                    shard: merge_repair_invariants(next, None, shard_index, &repair),
                },
            ))
        }
    }
}

fn mutate_existing_queue_shard<S: ObjectStore, E, T, F>(
    store: &S,
    shard_index: u32,
    writer_version: &str,
    mutate: F,
) -> Result<DurableQueueShardWriteOutcome<T>, DurableQueueShardMutationError<E>>
where
    F: FnOnce(&mut QueueShardState) -> Result<(T, Vec<String>), E>,
{
    if writer_version.trim().is_empty() {
        return Err(DurableQueueShardMutationError::EmptyWriterVersion);
    }

    let (etag, current) = read_queue_shard_for_update::<S, E>(store, shard_index)?;
    let object_key = queue_shard(shard_index);
    let mut next_state = current.envelope.state.clone();
    let (mutation, invariants) =
        mutate(&mut next_state).map_err(DurableQueueShardMutationError::Mutation)?;
    let next = merge_queue_mutation_invariants(
        build_loaded_queue_shard(next_state, writer_version)
            .map_err(DurableQueueShardMutationError::Codec)?,
        Some(&current),
        shard_index,
        &invariants,
    );
    let metadata = store
        .compare_and_swap(
            &object_key,
            &etag,
            &encode_queue_shard_bytes(&next).map_err(DurableQueueShardMutationError::Codec)?,
        )
        .map_err(map_queue_mutation_store_error::<E>)?;

    Ok(DurableQueueShardWriteOutcome::Updated(
        PersistedQueueShardMutation {
            metadata,
            mutation,
            shard: next,
        },
    ))
}

fn read_queue_shard_for_update<S: ObjectStore, E>(
    store: &S,
    shard_index: u32,
) -> Result<(String, LoadedQueueShardObject), DurableQueueShardMutationError<E>> {
    let object_key = queue_shard(shard_index);
    let metadata = store
        .head(&object_key)
        .map_err(map_queue_mutation_store_error::<E>)?
        .ok_or_else(|| DurableQueueShardMutationError::MissingObject {
            object_key: object_key.clone(),
        })?;
    let etag =
        metadata
            .etag
            .clone()
            .ok_or_else(|| DurableQueueShardMutationError::MissingObjectEtag {
                object_key: object_key.clone(),
            })?;
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_queue_mutation_store_error::<E>)?
        .ok_or_else(|| DurableQueueShardMutationError::MissingObjectAfterHead {
            object_key: object_key.clone(),
        })?;
    let current = load_queue_shard(
        shard_index,
        &StoredQueueShardObject {
            object_key,
            encoded_bytes,
        },
    )
    .map_err(DurableQueueShardMutationError::Load)?;

    Ok((etag, current))
}

fn build_loaded_queue_shard(
    state: QueueShardState,
    writer_version: &str,
) -> Result<LoadedQueueShardObject, String> {
    let object_key = queue_shard(state.shard_id);
    let envelope =
        ControlObjectEnvelope::from_state(ControlObjectKind::QueueShard, writer_version, state)
            .map_err(|err| err.to_string())?;

    Ok(LoadedQueueShardObject {
        object_key,
        envelope,
        checked_invariants: vec![
            "queue_shard_checksum_matches_payload".to_owned(),
            "queue_shard_key_matches_shard_id".to_owned(),
        ],
    })
}

fn encode_queue_shard_bytes(shard: &LoadedQueueShardObject) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&shard.envelope).map_err(|err| err.to_string())
}

fn broker_lease_invariants(outcome: &BrokerLeaseOutcome) -> Vec<String> {
    let mut invariants = vec![];
    if matches!(outcome, BrokerLeaseOutcome::TakenOver { .. }) {
        invariants.push("broker_lease_takeover_increments_epoch".to_owned());
    }
    invariants
}

fn claim_invariants(outcome: &JobClaimOutcome) -> Vec<String> {
    let mut invariants = vec!["active_broker_lease_required_for_shard_mutation".to_owned()];
    if matches!(outcome, JobClaimOutcome::Stolen { .. }) {
        invariants.push("claim_timeout_allows_steal".to_owned());
    }
    invariants
}

fn heartbeat_invariants() -> Vec<String> {
    vec![
        "active_broker_lease_required_for_shard_mutation".to_owned(),
        "worker_heartbeat_requires_matching_claim_token".to_owned(),
    ]
}

fn complete_invariants() -> Vec<String> {
    vec!["active_broker_lease_required_for_shard_mutation".to_owned()]
}

fn merge_repair_invariants(
    mut shard: LoadedQueueShardObject,
    current: Option<&LoadedQueueShardObject>,
    shard_index: u32,
    repair: &SnapshotRepairOutcome,
) -> LoadedQueueShardObject {
    if let Some(current) = current {
        extend_invariants(&mut shard.checked_invariants, &current.checked_invariants);
    }
    extend_invariants(
        &mut shard.checked_invariants,
        &[
            "queue_shard_checksum_matches_payload".to_owned(),
            "queue_shard_key_matches_shard_id".to_owned(),
            "queue_shard_cas_protects_updates".to_owned(),
        ],
    );
    if let SnapshotRepairOutcome::Enqueued { .. } = repair {
        extend_invariants(
            &mut shard.checked_invariants,
            &[
                "lost_enqueue_repair_enqueues_when_head_outpaces_progress".to_owned(),
                "snapshot_repair_dedupe_key_is_namespace_scoped".to_owned(),
            ],
        );
    }
    if let SnapshotRepairOutcome::AttachedFollowUp { .. } = repair {
        extend_invariants(
            &mut shard.checked_invariants,
            &["snapshot_repair_claimed_job_gets_follow_up".to_owned()],
        );
    }
    if let SnapshotRepairOutcome::RaisedReadyJob { .. } = repair {
        extend_invariants(
            &mut shard.checked_invariants,
            &["snapshot_repair_dedupe_key_is_namespace_scoped".to_owned()],
        );
    }
    if shard.object_key != queue_shard(shard_index) {
        shard.object_key = queue_shard(shard_index);
    }
    shard
}

fn merge_queue_mutation_invariants(
    mut shard: LoadedQueueShardObject,
    current: Option<&LoadedQueueShardObject>,
    shard_index: u32,
    mutation_invariants: &[String],
) -> LoadedQueueShardObject {
    if let Some(current) = current {
        extend_invariants(&mut shard.checked_invariants, &current.checked_invariants);
    }
    extend_invariants(
        &mut shard.checked_invariants,
        &[
            "queue_shard_checksum_matches_payload".to_owned(),
            "queue_shard_key_matches_shard_id".to_owned(),
            "queue_shard_cas_protects_updates".to_owned(),
        ],
    );
    extend_invariants(&mut shard.checked_invariants, mutation_invariants);
    if shard.object_key != queue_shard(shard_index) {
        shard.object_key = queue_shard(shard_index);
    }
    shard
}

fn extend_invariants(checked_invariants: &mut Vec<String>, new_invariants: &[String]) {
    for invariant in new_invariants {
        if !checked_invariants.iter().any(|value| value == invariant) {
            checked_invariants.push(invariant.clone());
        }
    }
}

fn map_queue_load_store_error(err: ObjectStoreError) -> QueueShardLoadError {
    QueueShardLoadError::Store(err.to_string())
}

fn map_queue_mutation_store_error<E>(err: ObjectStoreError) -> DurableQueueShardMutationError<E> {
    match err {
        ObjectStoreError::PreconditionFailed => DurableQueueShardMutationError::ConcurrentWrite,
        other => DurableQueueShardMutationError::Store(other.to_string()),
    }
}

fn map_repair_store_error(err: ObjectStoreError) -> DurableSnapshotRepairError {
    match err {
        ObjectStoreError::PreconditionFailed => DurableSnapshotRepairError::ConcurrentWrite,
        other => DurableSnapshotRepairError::Store(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        claim_job_in_store, complete_job_in_store, heartbeat_job_in_store, load_queue_shard,
        read_queue_shard, renew_broker_lease_in_store, repair_lost_snapshot_enqueue_in_store,
        DurableBrokerLeaseOutcome, DurableJobClaimOutcome, DurableJobCompleteOutcome,
        DurableQueueShardMutationError, DurableSnapshotRepairOutcome, QueueShardLoadError,
        StoredQueueShardObject,
    };
    use crate::types::{
        JobState, QueueClaim, QueueJob, QueueShardEnvelope, QueueShardState, WorkClass,
    };
    use crate::worker::{JobClaimOutcome, JobCompleteOutcome, WorkerMutationError};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::queue_shard;
    use loon_objectstore::ObjectStore;
    use loon_types::{ChangeSeq, ControlObjectKind, FenceToken, HeadState, InodeId, ProgressState};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_queue_shard_rejects_checksum_mismatch() {
        let mut envelope = QueueShardEnvelope::from_state(
            ControlObjectKind::QueueShard,
            "loon-queue-test",
            QueueShardState {
                work_class: WorkClass::BuildSnapshot,
                shard_id: 17,
                broker: None,
                jobs: vec![],
            },
        )
        .expect("build queue shard envelope");
        envelope.state.jobs.push(QueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: JobState::Ready,
            payload: crate::types::SeqScopedPayload {
                namespace_id: "ns-1".into(),
                through_seq: ChangeSeq(42),
            },
            follow_up: None,
            claim: None,
            attempts: 0,
        });

        let error = load_queue_shard(
            17,
            &StoredQueueShardObject {
                object_key: queue_shard(17),
                encoded_bytes: serde_json::to_vec(&envelope).expect("encode tampered shard"),
            },
        )
        .expect_err("tampered queue shard should fail");

        assert!(matches!(
            error,
            QueueShardLoadError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn renew_broker_lease_in_store_creates_missing_shard() {
        let temp_dir = TestDir::new("queue-broker-create");
        let store = LocalFsStore::new(temp_dir.path()).expect("create object store");

        let outcome = renew_broker_lease_in_store(
            &store,
            17,
            WorkClass::BuildSnapshot,
            "broker-a",
            0,
            10_000,
            "loon-queue-test",
        )
        .expect("lease should create missing shard");

        match outcome {
            DurableBrokerLeaseOutcome::Created(persisted) => {
                assert_eq!(
                    persisted
                        .shard
                        .envelope
                        .state
                        .broker
                        .as_ref()
                        .expect("created shard should have broker")
                        .epoch,
                    1
                );
                assert!(persisted
                    .shard
                    .checked_invariants
                    .contains(&"queue_shard_cas_protects_updates".to_owned()));
            }
            other => panic!("unexpected lease outcome: {other:?}"),
        }
    }

    #[test]
    fn heartbeat_in_store_extends_claim_timeout() {
        let temp_dir = TestDir::new("queue-heartbeat");
        let store = LocalFsStore::new(temp_dir.path()).expect("create object store");
        seed_queue_shard(&store, ready_snapshot_shard());
        renew_broker_lease_in_store(
            &store,
            17,
            WorkClass::BuildSnapshot,
            "broker-a",
            0,
            10_000,
            "loon-queue-test",
        )
        .expect("broker lease should succeed");
        claim_job_in_store(
            &store,
            17,
            "broker-a",
            1,
            "worker-a",
            "claim-a",
            "job-1",
            0,
            10_000,
            "loon-queue-test",
        )
        .expect("claim should succeed");

        heartbeat_job_in_store(
            &store,
            17,
            "broker-a",
            1,
            "job-1",
            "claim-a",
            5_000,
            10_000,
            "loon-queue-test",
        )
        .expect("heartbeat should succeed");

        let shard = read_queue_shard(&store, 17).expect("queue shard should load");
        assert_eq!(
            shard.envelope.state.jobs[0]
                .claim
                .as_ref()
                .expect("claim should still exist")
                .timeout_at_ms,
            15_000
        );
    }

    #[test]
    fn claim_timeout_then_steal_rejects_stale_complete_in_store() {
        let temp_dir = TestDir::new("queue-steal");
        let store = LocalFsStore::new(temp_dir.path()).expect("create object store");
        seed_queue_shard(&store, ready_snapshot_shard());

        renew_broker_lease_in_store(
            &store,
            17,
            WorkClass::BuildSnapshot,
            "broker-a",
            0,
            10_000,
            "loon-queue-test",
        )
        .expect("initial broker lease");
        match claim_job_in_store(
            &store,
            17,
            "broker-a",
            1,
            "worker-a",
            "claim-a",
            "job-1",
            0,
            10_000,
            "loon-queue-test",
        )
        .expect("initial claim should succeed")
        {
            DurableJobClaimOutcome::Updated(persisted) => {
                assert_eq!(
                    persisted.mutation,
                    JobClaimOutcome::Claimed {
                        claim_token: "claim-a".to_owned(),
                    }
                );
                assert_eq!(persisted.shard.envelope.state.jobs[0].attempts, 1);
            }
            other => panic!("unexpected claim outcome: {other:?}"),
        }

        renew_broker_lease_in_store(
            &store,
            17,
            WorkClass::BuildSnapshot,
            "broker-b",
            30_000,
            10_000,
            "loon-queue-test",
        )
        .expect("expired lease should allow takeover");

        match claim_job_in_store(
            &store,
            17,
            "broker-b",
            2,
            "worker-b",
            "claim-b",
            "job-1",
            30_000,
            10_000,
            "loon-queue-test",
        )
        .expect("timed-out claim should be stealable")
        {
            DurableJobClaimOutcome::Updated(persisted) => {
                assert_eq!(
                    persisted.mutation,
                    JobClaimOutcome::Stolen {
                        claim_token: "claim-b".to_owned(),
                    }
                );
                assert!(persisted
                    .shard
                    .checked_invariants
                    .contains(&"claim_timeout_allows_steal".to_owned()));
            }
            other => panic!("unexpected steal outcome: {other:?}"),
        }

        let stale_complete = complete_job_in_store(
            &store,
            17,
            "broker-b",
            2,
            "job-1",
            "claim-a",
            30_001,
            "loon-queue-test",
        )
        .expect_err("stale claim token should be rejected");
        assert!(matches!(
            stale_complete,
            DurableQueueShardMutationError::Mutation(
                WorkerMutationError::ClaimTokenMismatch { .. }
            )
        ));

        match complete_job_in_store(
            &store,
            17,
            "broker-b",
            2,
            "job-1",
            "claim-b",
            30_001,
            "loon-queue-test",
        )
        .expect("fresh claim should complete")
        {
            DurableJobCompleteOutcome::Updated(persisted) => {
                assert_eq!(persisted.mutation, JobCompleteOutcome::Removed);
                assert!(persisted.shard.envelope.state.jobs.is_empty());
            }
            other => panic!("unexpected complete outcome: {other:?}"),
        }
    }

    #[test]
    fn repair_in_store_creates_missing_shard_and_enqueues_job() {
        let temp_dir = TestDir::new("queue-shard-create");
        let store = LocalFsStore::new(temp_dir.path()).expect("create object store");

        let outcome = repair_lost_snapshot_enqueue_in_store(
            &store,
            17,
            &sample_head(ChangeSeq(42)),
            Some(&sample_progress(ChangeSeq(40))),
            "loon-queue-test",
        )
        .expect("repair should create missing shard");

        match outcome {
            DurableSnapshotRepairOutcome::Created(persisted) => {
                assert_eq!(persisted.shard.envelope.state.jobs.len(), 1);
                assert!(persisted
                    .shard
                    .checked_invariants
                    .contains(&"queue_shard_cas_protects_updates".to_owned()));
            }
            other => panic!("unexpected repair outcome: {other:?}"),
        }
    }

    #[test]
    fn repair_in_store_updates_existing_claimed_shard() {
        let temp_dir = TestDir::new("queue-shard-update");
        let store = LocalFsStore::new(temp_dir.path()).expect("create object store");
        seed_queue_shard(&store, claimed_snapshot_shard());

        let outcome = repair_lost_snapshot_enqueue_in_store(
            &store,
            17,
            &sample_head(ChangeSeq(42)),
            Some(&sample_progress(ChangeSeq(40))),
            "loon-queue-test",
        )
        .expect("repair should update existing shard");

        match outcome {
            DurableSnapshotRepairOutcome::Updated(persisted) => {
                assert_eq!(
                    persisted.shard.envelope.state.jobs[0]
                        .follow_up
                        .as_ref()
                        .expect("claimed job should have follow-up")
                        .through_seq,
                    ChangeSeq(42)
                );
                assert!(persisted
                    .shard
                    .checked_invariants
                    .contains(&"snapshot_repair_claimed_job_gets_follow_up".to_owned()));
            }
            other => panic!("unexpected repair outcome: {other:?}"),
        }
    }

    fn seed_queue_shard(store: &LocalFsStore, state: QueueShardState) {
        let envelope = QueueShardEnvelope::from_state(
            ControlObjectKind::QueueShard,
            "loon-queue-test",
            state.clone(),
        )
        .expect("build queue shard envelope");

        store
            .put_if_absent(
                &queue_shard(state.shard_id),
                &serde_json::to_vec(&envelope).expect("encode queue shard envelope"),
            )
            .expect("seed queue shard");
    }

    fn ready_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![QueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: JobState::Ready,
                payload: crate::types::SeqScopedPayload {
                    namespace_id: "ns-1".into(),
                    through_seq: ChangeSeq(40),
                },
                follow_up: None,
                claim: None,
                attempts: 0,
            }],
        }
    }

    fn claimed_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![QueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: JobState::Claimed,
                payload: crate::types::SeqScopedPayload {
                    namespace_id: "ns-1".into(),
                    through_seq: ChangeSeq(40),
                },
                follow_up: None,
                claim: Some(QueueClaim {
                    worker_id: "worker-a".to_owned(),
                    claim_token: "claim-a".to_owned(),
                    heartbeat_at_ms: 0,
                    timeout_at_ms: 10_000,
                }),
                attempts: 1,
            }],
        }
    }

    fn sample_head(seq: ChangeSeq) -> HeadState {
        HeadState {
            namespace_id: "ns-1".into(),
            seq,
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        }
    }

    fn sample_progress(through_seq: ChangeSeq) -> ProgressState {
        ProgressState {
            namespace_id: "ns-1".into(),
            work_class: "BuildSnapshot".to_owned(),
            through_seq,
        }
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("loon-queue-{prefix}-{nanos}"));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
