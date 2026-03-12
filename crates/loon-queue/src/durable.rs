use crate::repair::{repair_lost_snapshot_enqueue, SnapshotRepairError, SnapshotRepairOutcome};
use crate::types::{QueueShardEnvelope, QueueShardState, WorkClass};
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
                .compare_and_swap(&object_key, &etag, &encode_queue_shard(&next)?)
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
                .put_if_absent(&object_key, &encode_queue_shard(&next)?)
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

fn encode_queue_shard(
    shard: &LoadedQueueShardObject,
) -> Result<Vec<u8>, DurableSnapshotRepairError> {
    serde_json::to_vec(&shard.envelope)
        .map_err(|err| DurableSnapshotRepairError::Codec(err.to_string()))
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

fn extend_invariants(checked_invariants: &mut Vec<String>, new_invariants: &[String]) {
    for invariant in new_invariants {
        if !checked_invariants.iter().any(|value| value == invariant) {
            checked_invariants.push(invariant.clone());
        }
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
        load_queue_shard, repair_lost_snapshot_enqueue_in_store, DurableSnapshotRepairOutcome,
        QueueShardLoadError, StoredQueueShardObject,
    };
    use crate::types::{QueueShardEnvelope, QueueShardState, WorkClass};
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
        envelope.state.jobs.push(crate::types::QueueJob {
            job_id: "job-1".to_owned(),
            dedupe_key: "BuildSnapshot:ns-1".to_owned(),
            state: crate::types::JobState::Ready,
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

    fn claimed_snapshot_shard() -> QueueShardState {
        QueueShardState {
            work_class: WorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![crate::types::QueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: crate::types::JobState::Claimed,
                payload: crate::types::SeqScopedPayload {
                    namespace_id: "ns-1".into(),
                    through_seq: ChangeSeq(40),
                },
                follow_up: None,
                claim: None,
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
