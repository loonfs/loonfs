use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::derived_progress;
use loon_objectstore::{ObjectMetadata, ObjectStore};
use loon_types::{
    payload_checksum_sha256, ChangeSeq, ControlObjectEnvelope, ControlObjectKind, NamespaceId,
    ProgressState, ProgressStateEnvelope,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredProgressObject {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedProgressObject {
    pub object_key: String,
    pub envelope: ProgressStateEnvelope,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedRetentionAuthorizers {
    pub required_progress: Vec<LoadedProgressObject>,
    pub retention_policy: LoadedProgressObject,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedProgressObject {
    pub metadata: ObjectMetadata,
    pub progress: LoadedProgressObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgressPublishOutcome {
    Created(PublishedProgressObject),
    Advanced(PublishedProgressObject),
    NoChange(LoadedProgressObject),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgressLoadError {
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
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    WorkClassMismatch {
        expected: String,
        actual: String,
    },
    ChecksumMismatch {
        expected: String,
        actual: String,
    },
    Codec(String),
    Store(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProgressPublishError {
    EmptyWriterVersion,
    MissingObjectAfterHead { object_key: String },
    MissingObjectEtag { object_key: String },
    ConcurrentWrite,
    Load(ProgressLoadError),
    Codec(String),
    Store(String),
}

pub fn load_progress_object(
    expected_namespace: &NamespaceId,
    expected_work_class: &str,
    stored: &StoredProgressObject,
) -> Result<LoadedProgressObject, ProgressLoadError> {
    let expected_key = derived_progress(expected_namespace.as_str(), expected_work_class);
    if stored.object_key != expected_key {
        return Err(ProgressLoadError::ObjectKeyMismatch {
            expected: expected_key,
            actual: stored.object_key.clone(),
        });
    }

    let envelope: ProgressStateEnvelope = serde_json::from_slice(&stored.encoded_bytes)
        .map_err(|err| ProgressLoadError::Codec(err.to_string()))?;
    if envelope.kind != ControlObjectKind::NamespaceProgress {
        return Err(ProgressLoadError::KindMismatch {
            expected: ControlObjectKind::NamespaceProgress,
            actual: envelope.kind,
        });
    }

    let actual_checksum = payload_checksum_sha256(&envelope.state)
        .map_err(|err| ProgressLoadError::Codec(err.to_string()))?;
    if envelope.payload_checksum_sha256 != actual_checksum {
        return Err(ProgressLoadError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual: actual_checksum,
        });
    }

    if &envelope.state.namespace_id != expected_namespace {
        return Err(ProgressLoadError::NamespaceMismatch {
            expected: expected_namespace.clone(),
            actual: envelope.state.namespace_id.clone(),
        });
    }

    if envelope.state.work_class != expected_work_class {
        return Err(ProgressLoadError::WorkClassMismatch {
            expected: expected_work_class.to_owned(),
            actual: envelope.state.work_class.clone(),
        });
    }

    Ok(LoadedProgressObject {
        object_key: stored.object_key.clone(),
        envelope,
        checked_invariants: vec![
            "progress_object_checksum_matches_payload".to_owned(),
            "progress_object_key_matches_namespace_and_work_class".to_owned(),
        ],
    })
}

pub fn read_progress_object<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    expected_work_class: &str,
) -> Result<LoadedProgressObject, ProgressLoadError> {
    let object_key = derived_progress(expected_namespace.as_str(), expected_work_class);
    let encoded_bytes = store
        .get(&object_key, None)
        .map_err(map_object_store_error)?
        .ok_or_else(|| ProgressLoadError::MissingObject {
            object_key: object_key.clone(),
        })?;

    load_progress_object(
        expected_namespace,
        expected_work_class,
        &StoredProgressObject {
            object_key,
            encoded_bytes,
        },
    )
}

pub fn load_retention_authorizers<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    required_work_classes: &[String],
    retention_policy_work_class: &str,
) -> Result<LoadedRetentionAuthorizers, ProgressLoadError> {
    let mut required_progress = Vec::new();
    let mut seen_work_classes = BTreeSet::new();

    for work_class in required_work_classes {
        if seen_work_classes.insert(work_class.as_str()) {
            required_progress.push(read_progress_object(store, expected_namespace, work_class)?);
        }
    }
    required_progress.sort_by(|left, right| {
        left.envelope
            .state
            .work_class
            .cmp(&right.envelope.state.work_class)
    });

    let retention_policy =
        read_progress_object(store, expected_namespace, retention_policy_work_class)?;

    let mut checked_invariants = Vec::new();
    for progress in &required_progress {
        extend_invariants(&mut checked_invariants, &progress.checked_invariants);
    }
    extend_invariants(
        &mut checked_invariants,
        &retention_policy.checked_invariants,
    );

    Ok(LoadedRetentionAuthorizers {
        required_progress,
        retention_policy,
        checked_invariants,
    })
}

pub fn publish_progress<S: ObjectStore>(
    store: &S,
    expected_namespace: &NamespaceId,
    expected_work_class: &str,
    requested_through_seq: ChangeSeq,
    writer_version: &str,
) -> Result<ProgressPublishOutcome, ProgressPublishError> {
    if writer_version.trim().is_empty() {
        return Err(ProgressPublishError::EmptyWriterVersion);
    }

    let object_key = derived_progress(expected_namespace.as_str(), expected_work_class);
    match store.head(&object_key).map_err(map_publish_store_error)? {
        Some(metadata) => {
            let etag =
                metadata
                    .etag
                    .clone()
                    .ok_or_else(|| ProgressPublishError::MissingObjectEtag {
                        object_key: object_key.clone(),
                    })?;
            let encoded_bytes = store
                .get(&object_key, None)
                .map_err(map_publish_store_error)?
                .ok_or_else(|| ProgressPublishError::MissingObjectAfterHead {
                    object_key: object_key.clone(),
                })?;
            let current = load_progress_object(
                expected_namespace,
                expected_work_class,
                &StoredProgressObject {
                    object_key: object_key.clone(),
                    encoded_bytes,
                },
            )
            .map_err(ProgressPublishError::Load)?;

            if current.envelope.state.through_seq >= requested_through_seq {
                return Ok(ProgressPublishOutcome::NoChange(current));
            }

            let next = build_loaded_progress_object(
                expected_namespace,
                expected_work_class,
                requested_through_seq,
                writer_version,
            )
            .map_err(ProgressPublishError::Codec)?;
            let written_metadata = store
                .compare_and_swap(&object_key, &etag, &encode_progress_object(&next)?)
                .map_err(map_publish_store_error)?;

            Ok(ProgressPublishOutcome::Advanced(PublishedProgressObject {
                metadata: written_metadata,
                progress: next,
            }))
        }
        None => {
            let next = build_loaded_progress_object(
                expected_namespace,
                expected_work_class,
                requested_through_seq,
                writer_version,
            )
            .map_err(ProgressPublishError::Codec)?;
            let written_metadata = store
                .put_if_absent(&object_key, &encode_progress_object(&next)?)
                .map_err(map_publish_store_error)?;

            Ok(ProgressPublishOutcome::Created(PublishedProgressObject {
                metadata: written_metadata,
                progress: next,
            }))
        }
    }
}

fn build_loaded_progress_object(
    namespace_id: &NamespaceId,
    work_class: &str,
    through_seq: ChangeSeq,
    writer_version: &str,
) -> Result<LoadedProgressObject, String> {
    let object_key = derived_progress(namespace_id.as_str(), work_class);
    let envelope = ControlObjectEnvelope::from_state(
        ControlObjectKind::NamespaceProgress,
        writer_version,
        ProgressState {
            namespace_id: namespace_id.clone(),
            work_class: work_class.to_owned(),
            through_seq,
        },
    )
    .map_err(|err| err.to_string())?;

    Ok(LoadedProgressObject {
        object_key,
        envelope,
        checked_invariants: vec![
            "progress_object_checksum_matches_payload".to_owned(),
            "progress_object_key_matches_namespace_and_work_class".to_owned(),
            "progress_through_seq_advances_monotonically".to_owned(),
        ],
    })
}

fn encode_progress_object(
    progress: &LoadedProgressObject,
) -> Result<Vec<u8>, ProgressPublishError> {
    serde_json::to_vec(&progress.envelope)
        .map_err(|err| ProgressPublishError::Codec(err.to_string()))
}

fn extend_invariants(checked_invariants: &mut Vec<String>, new_invariants: &[String]) {
    for invariant in new_invariants {
        if !checked_invariants.iter().any(|value| value == invariant) {
            checked_invariants.push(invariant.clone());
        }
    }
}

fn map_object_store_error(err: ObjectStoreError) -> ProgressLoadError {
    ProgressLoadError::Store(err.to_string())
}

fn map_publish_store_error(err: ObjectStoreError) -> ProgressPublishError {
    match err {
        ObjectStoreError::PreconditionFailed => ProgressPublishError::ConcurrentWrite,
        other => ProgressPublishError::Store(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        load_progress_object, load_retention_authorizers, publish_progress, ProgressLoadError,
        ProgressPublishOutcome, StoredProgressObject,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::derived_progress;
    use loon_objectstore::ObjectStore;
    use loon_types::{
        payload_checksum_sha256, ChangeSeq, ControlObjectEnvelope, ControlObjectKind, NamespaceId,
        ProgressState,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_progress_object_rejects_checksum_mismatch() {
        let mut envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            "loon-core-test",
            ProgressState {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: "BuildListingIndex".to_owned(),
                through_seq: ChangeSeq(42),
            },
        )
        .expect("build progress envelope");
        envelope.state.through_seq = ChangeSeq(43);

        let error = load_progress_object(
            &NamespaceId::from("ns-1"),
            "BuildListingIndex",
            &StoredProgressObject {
                object_key: derived_progress("ns-1", "BuildListingIndex"),
                encoded_bytes: serde_json::to_vec(&envelope)
                    .expect("encode tampered progress envelope"),
            },
        )
        .expect_err("tampered progress object should fail");

        assert_eq!(
            error,
            ProgressLoadError::ChecksumMismatch {
                expected: envelope.payload_checksum_sha256,
                actual: payload_checksum_sha256(&envelope.state)
                    .expect("recompute progress checksum"),
            }
        );
    }

    #[test]
    fn load_retention_authorizers_reads_required_and_policy_objects_from_store() {
        let temp_dir = TestDir::new("retention-authorizers");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        seed_progress_object(&store, "ns-1", "BuildListingIndex", ChangeSeq(42));
        seed_progress_object(&store, "ns-1", "RetentionPolicy", ChangeSeq(42));

        let loaded = load_retention_authorizers(
            &store,
            &NamespaceId::from("ns-1"),
            &["BuildListingIndex".to_owned()],
            "RetentionPolicy",
        )
        .expect("load retention authorizers");

        assert_eq!(loaded.required_progress.len(), 1);
        assert_eq!(
            loaded.required_progress[0].envelope.state.work_class,
            "BuildListingIndex"
        );
        assert_eq!(
            loaded.retention_policy.envelope.state.work_class,
            "RetentionPolicy"
        );
        assert!(loaded
            .checked_invariants
            .contains(&"progress_object_key_matches_namespace_and_work_class".to_owned()));
    }

    #[test]
    fn publish_progress_creates_missing_object() {
        let temp_dir = TestDir::new("publish-progress-create");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");

        let published = publish_progress(
            &store,
            &NamespaceId::from("ns-1"),
            "BuildSnapshot",
            ChangeSeq(42),
            "loon-core-test",
        )
        .expect("publish missing progress object");

        match published {
            ProgressPublishOutcome::Created(published) => {
                assert_eq!(published.progress.envelope.state.through_seq, ChangeSeq(42));
            }
            other => panic!("unexpected publish outcome: {other:?}"),
        }
    }

    #[test]
    fn publish_progress_advances_existing_object_monotonically() {
        let temp_dir = TestDir::new("publish-progress-advance");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        seed_progress_object(&store, "ns-1", "BuildSnapshot", ChangeSeq(40));

        let published = publish_progress(
            &store,
            &NamespaceId::from("ns-1"),
            "BuildSnapshot",
            ChangeSeq(42),
            "loon-core-test",
        )
        .expect("advance progress object");

        match published {
            ProgressPublishOutcome::Advanced(published) => {
                assert_eq!(published.progress.envelope.state.through_seq, ChangeSeq(42));
                assert!(published
                    .progress
                    .checked_invariants
                    .contains(&"progress_through_seq_advances_monotonically".to_owned()));
            }
            other => panic!("unexpected publish outcome: {other:?}"),
        }
    }

    #[test]
    fn publish_progress_noops_when_requested_seq_is_stale() {
        let temp_dir = TestDir::new("publish-progress-noop");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        seed_progress_object(&store, "ns-1", "BuildSnapshot", ChangeSeq(42));

        let published = publish_progress(
            &store,
            &NamespaceId::from("ns-1"),
            "BuildSnapshot",
            ChangeSeq(41),
            "loon-core-test",
        )
        .expect("stale progress publish should no-op");

        match published {
            ProgressPublishOutcome::NoChange(current) => {
                assert_eq!(current.envelope.state.through_seq, ChangeSeq(42));
            }
            other => panic!("unexpected publish outcome: {other:?}"),
        }
    }

    fn seed_progress_object(
        store: &LocalFsStore,
        namespace_id: &str,
        work_class: &str,
        through_seq: ChangeSeq,
    ) {
        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            "loon-core-test",
            ProgressState {
                namespace_id: NamespaceId::from(namespace_id),
                work_class: work_class.to_owned(),
                through_seq,
            },
        )
        .expect("build progress envelope");

        store
            .put_if_absent(
                &derived_progress(namespace_id, work_class),
                &serde_json::to_vec(&envelope).expect("encode progress envelope"),
            )
            .expect("seed progress object");
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
            let path = std::env::temp_dir().join(format!("loon-core-{prefix}-{nanos}"));
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
