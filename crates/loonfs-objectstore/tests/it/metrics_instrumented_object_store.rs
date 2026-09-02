use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use loonfs_api::ManifestObjectId;
use loonfs_objectstore::keys::{
    checkpoint_record, metadata_manifest_object, metadata_segment, wal_head, wal_segment,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::metrics::{
    InstrumentedObjectStore, JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricsRecorder,
    ObjectStoreOperation, ObjectStoreResultClass, PutModeClass, RangeClass,
    VecObjectStoreMetricsRecorder,
};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, ObjectStoreErrorClass,
    PutMode,
};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

fn bytes(bytes: &'static [u8]) -> Bytes {
    Bytes::from_static(bytes)
}

const SECRET_KEY_SEGMENT: &str = "secret-segment";

fn secret_key() -> String {
    format!("namespaces/ns-1/wal/{SECRET_KEY_SEGMENT}.wal.zst")
}

#[test]
fn metric_result_classes_preserve_every_error_class() {
    let cases = [
        (
            ObjectStoreErrorClass::NotFound,
            ObjectStoreResultClass::NotFound,
        ),
        (
            ObjectStoreErrorClass::InvalidRequest,
            ObjectStoreResultClass::InvalidRequest,
        ),
        (
            ObjectStoreErrorClass::InvalidKey,
            ObjectStoreResultClass::InvalidKey,
        ),
        (
            ObjectStoreErrorClass::PreconditionFailed,
            ObjectStoreResultClass::PreconditionFailed,
        ),
        (
            ObjectStoreErrorClass::PermissionDenied,
            ObjectStoreResultClass::PermissionDenied,
        ),
        (
            ObjectStoreErrorClass::StoredChecksumMissing,
            ObjectStoreResultClass::StoredChecksumMissing,
        ),
        (
            ObjectStoreErrorClass::Unsupported,
            ObjectStoreResultClass::Unsupported,
        ),
        (
            ObjectStoreErrorClass::Configuration,
            ObjectStoreResultClass::Configuration,
        ),
        (
            ObjectStoreErrorClass::RetryableTransport,
            ObjectStoreResultClass::RetryableTransport,
        ),
        (ObjectStoreErrorClass::Other, ObjectStoreResultClass::Other),
    ];

    for (error_class, result_class) in cases {
        assert_eq!(ObjectStoreResultClass::from(error_class), result_class);
    }
}

#[tokio::test]
async fn records_put_success() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

    store
        .put(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            bytes(b"head"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("put object");

    let samples = recorder.samples();
    assert_eq!(samples.len(), 1);
    let sample = &samples[0];
    assert_eq!(sample.operation, ObjectStoreOperation::Put);
    assert_eq!(sample.result, ObjectStoreResultClass::Ok);
    assert_eq!(sample.bytes_in, Some(4));
    assert_eq!(sample.bytes_out, None);
    assert_eq!(sample.put_mode, Some(PutModeClass::CreateIfAbsent));
    assert_eq!(sample.key_class, KeyClass::NamespaceHead);
    assert_eq!(sample.store_kind.as_deref(), Some("local-fs"));
    assert_eq!(sample.attempts, 1);
}

#[tokio::test]
async fn convenience_writes_funnel_through_put_with_distinct_modes() {
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = InstrumentedObjectStore::new(DelegatingWriteStore::default(), recorder.clone());

    store
        .put_overwrite(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            bytes(b"overwrite"),
        )
        .await
        .expect("put overwrite");
    store
        .put_if_absent(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            bytes(b"create"),
        )
        .await
        .expect("put if absent");
    store
        .compare_and_swap(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            "etag-old",
            bytes(b"swap"),
        )
        .await
        .expect("compare and swap");

    let inner = store.into_inner();
    // The sugar methods are trait defaults now: every write funnels through
    // the one instrumented `put`, so the inner store sees `put` three
    // times while the samples still carry the distinct modes below.
    assert_eq!(inner.calls(), vec!["put", "put", "put"]);

    let samples = recorder.samples();
    assert_eq!(samples.len(), 3);
    assert_eq!(samples[0].put_mode, Some(PutModeClass::Overwrite));
    assert_eq!(samples[1].put_mode, Some(PutModeClass::CreateIfAbsent));
    assert_eq!(samples[2].put_mode, Some(PutModeClass::CompareAndSwap));
}

#[tokio::test]
async fn records_get_success_bytes_out() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite(
            "content-stores/cs_abc/objects/ab/cd/con_abcdef0123456789abcdef0123456789",
            bytes(b"abcdef"),
        )
        .await
        .expect("put object");
    let bytes = store
        .get(
            "content-stores/cs_abc/objects/ab/cd/con_abcdef0123456789abcdef0123456789",
            Some(ByteRange {
                start_inclusive: 1,
                end_exclusive: 4,
            }),
        )
        .await
        .expect("get object")
        .expect("object exists");

    assert_eq!(bytes, Bytes::from_static(b"bcd"));
    let samples = recorder.samples();
    let sample = samples.last().expect("get sample");
    assert_eq!(sample.operation, ObjectStoreOperation::Get);
    assert_eq!(sample.result, ObjectStoreResultClass::Ok);
    assert_eq!(sample.bytes_out, Some(3));
    assert_eq!(sample.key_class, KeyClass::Content);
    assert_eq!(sample.range_class, Some(RangeClass::Bounded));
}

#[tokio::test]
async fn records_not_found_without_key_leak() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
    let raw_key = secret_key();

    assert!(store
        .get(&raw_key, None)
        .await
        .expect("get missing")
        .is_none());

    let sample = recorder.samples().pop().expect("get sample");
    assert_eq!(sample.operation, ObjectStoreOperation::Get);
    assert_eq!(sample.result, ObjectStoreResultClass::NotFound);
    let encoded = serde_json::to_string(&sample).expect("serialize sample");
    assert!(!encoded.contains(&raw_key));
    assert!(!encoded.contains(SECRET_KEY_SEGMENT));
}

#[tokio::test]
async fn records_invalid_key_without_error_text() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
    let raw_key = "../escape";

    store
        .put_overwrite(raw_key, bytes(b"bad"))
        .await
        .expect_err("invalid key should fail");

    let sample = recorder.samples().pop().expect("put sample");
    assert_eq!(sample.operation, ObjectStoreOperation::Put);
    assert_eq!(sample.result, ObjectStoreResultClass::InvalidKey);
    let encoded = serde_json::to_string(&sample).expect("serialize sample");
    assert!(!encoded.contains(raw_key));
    assert!(!encoded.contains("escape"));
}

#[tokio::test]
async fn records_list_count() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite("namespaces/ns-1/descriptor.json", bytes(b"descriptor"))
        .await
        .expect("put descriptor");
    store
        .put_overwrite(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            bytes(b"head"),
        )
        .await
        .expect("put head");
    let keys = store
        .list_prefix("namespaces/ns-1/")
        .await
        .expect("list namespace");

    assert_eq!(keys.len(), 2);
    let sample = recorder.samples().pop().expect("list sample");
    assert_eq!(sample.operation, ObjectStoreOperation::ListPrefix);
    assert_eq!(sample.result, ObjectStoreResultClass::Ok);
    assert_eq!(sample.item_count, Some(2));
    assert_eq!(sample.range_class, Some(RangeClass::Prefix));
}

#[tokio::test]
async fn instrumented_store_forwards_start_after_listing() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
    let prefix = "namespaces/ns-1/checkpoints/";
    let first = format!("{prefix}chk_00000000000000000000000000000001.json");
    let second = format!("{prefix}chk_00000000000000000000000000000002.json");
    for key in [&first, &second] {
        store
            .put_overwrite(key, bytes(b"checkpoint"))
            .await
            .expect("put checkpoint");
    }

    let keys = store
        .list_prefix_from_stream(prefix, Some(&first))
        .try_collect::<Vec<_>>()
        .await
        .expect("resume instrumented listing");

    assert_eq!(keys, vec![second]);
    let sample = recorder.samples().pop().expect("streamed list sample");
    assert_eq!(sample.operation, ObjectStoreOperation::ListPrefixStream);
    assert_eq!(sample.item_count, Some(1));
}

#[tokio::test]
async fn classifies_wal_manifest_segment_and_checkpoint_key_families() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite(
            &wal_segment(
                &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
                &loonfs_api::WalSegmentId::parse("wal_00000000000000000001-644e4d336fd4ee33")
                    .expect("valid WAL segment id"),
            ),
            bytes(b"wal"),
        )
        .await
        .expect("put wal");
    store
        .put_overwrite(
            &metadata_manifest_object(
                &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
                &ManifestObjectId::parse("man_00000000000000000002-0123456789abcdef")
                    .expect("valid manifest object id"),
            ),
            bytes(b"manifest"),
        )
        .await
        .expect("put manifest");
    store
        .put_overwrite(
            &metadata_segment(
                &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
                &loonfs_api::MetadataSegmentId::parse("seg_00000000000000000000000000000001")
                    .expect("valid metadata segment id"),
            ),
            bytes(b"segment"),
        )
        .await
        .expect("put segment");
    store
        .put_overwrite(
            &checkpoint_record(
                &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
                &loonfs_api::CheckpointId::parse("chk_00000000000000000000000000000001")
                    .expect("valid checkpoint id"),
            ),
            bytes(b"checkpoint"),
        )
        .await
        .expect("put checkpoint record");

    let samples = recorder.samples();
    assert_eq!(samples[0].key_class, KeyClass::WalSegment);
    assert_eq!(samples[1].key_class, KeyClass::NamespaceManifest);
    assert_eq!(samples[2].key_class, KeyClass::MetadataSegment);
    assert_eq!(samples[3].key_class, KeyClass::GcControl);
}

#[tokio::test]
async fn jsonl_recorder_writes_privacy_safe_samples() {
    let temp_dir = tempdir().expect("tempdir");
    let metrics_path = temp_dir.path().join("metrics/object-store.ndjson");
    let recorder =
        Arc::new(JsonlObjectStoreMetricsRecorder::create(&metrics_path).expect("recorder"));
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
    let raw_key = secret_key();

    store
        .put_overwrite(
            &wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id")),
            bytes(b"head"),
        )
        .await
        .expect("put object");
    assert!(store
        .get(&raw_key, None)
        .await
        .expect("get missing")
        .is_none());
    recorder.flush().expect("flush metrics");

    let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
    let lines = jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    assert!(!jsonl.contains(&raw_key));
    assert!(!jsonl.contains(SECRET_KEY_SEGMENT));

    let first: serde_json::Value = serde_json::from_str(lines[0]).expect("first sample");
    assert_eq!(first["operation"], "put");
    assert_eq!(first["result"], "ok");
    assert_eq!(first["attempts"], 1);
    let second: serde_json::Value = serde_json::from_str(lines[1]).expect("second sample");
    assert_eq!(second["operation"], "get");
    assert_eq!(second["result"], "not_found");
}

fn instrumented_object_store<R>(
    root: &std::path::Path,
    recorder: Arc<R>,
) -> InstrumentedObjectStore<LocalFsStore>
where
    R: ObjectStoreMetricsRecorder,
{
    InstrumentedObjectStore::new(LocalFsStore::new(root).expect("local fs store"), recorder)
        .store_kind("local-fs")
}

#[derive(Debug, Default)]
struct DelegatingWriteStore {
    calls: Mutex<Vec<&'static str>>,
}

impl DelegatingWriteStore {
    fn calls(&self) -> Vec<&'static str> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn record_call(&self, call: &'static str) -> ObjectMetadata {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(call);
        ObjectMetadata {
            etag: Some(format!("{call}-etag")),
            version: None,
            size_bytes: 0,
            last_modified_ms: None,
        }
    }
}

#[async_trait]
impl ObjectStore for DelegatingWriteStore {
    async fn head(&self, _key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(None)
    }

    async fn get(
        &self,
        _key: &str,
        _range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        Ok(None)
    }

    async fn get_with_metadata(&self, _key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(None)
    }

    async fn put(
        &self,
        _key: &str,
        _bytes: Bytes,
        _mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put"))
    }

    async fn put_overwrite(
        &self,
        _key: &str,
        _bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put_overwrite"))
    }

    async fn put_if_absent(
        &self,
        _key: &str,
        _bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put_if_absent"))
    }

    async fn compare_and_swap(
        &self,
        _key: &str,
        _expected_etag: &str,
        _bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("compare_and_swap"))
    }

    async fn delete(&self, _key: &str) -> Result<(), ObjectStoreError> {
        Ok(())
    }

    fn list_prefix_from_stream(
        &self,
        _prefix: &str,
        _start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        Box::pin(stream::empty())
    }
}
