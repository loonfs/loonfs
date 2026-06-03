use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::perf::{
    DefaultKeyClassifier, JsonlPerfRecorder, KeyClass, KeyClassifier, MeasuredStore,
    ObjectStoreOperation, ObjectStoreResultClass, PerfRecorder, PutModeClass, RangeClass,
    VecPerfRecorder,
};
use loon_objectstore::{ByteRange, ObjectStore, PutMode};
use std::sync::Arc;
use tempfile::{tempdir, NamedTempFile};

#[test]
fn records_put_success() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());

    store
        .put(
            "namespaces/ns-1/head.json",
            b"head",
            PutMode::CreateIfAbsent,
        )
        .expect("put object");

    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.operation, ObjectStoreOperation::Put);
    assert_eq!(event.result, ObjectStoreResultClass::Ok);
    assert_eq!(event.bytes_in, Some(4));
    assert_eq!(event.bytes_out, None);
    assert_eq!(event.put_mode, Some(PutModeClass::CreateIfAbsent));
    assert_eq!(event.key_class, KeyClass::NamespaceHead);
    assert_eq!(event.store_kind.as_deref(), Some("local-fs"));
}

#[test]
fn records_get_success_bytes_out() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite("content-stores/cs_abc/blobs/sha256/ab/cd/abcdef", b"abcdef")
        .expect("put object");
    let bytes = store
        .get(
            "content-stores/cs_abc/blobs/sha256/ab/cd/abcdef",
            Some(ByteRange {
                start_inclusive: 1,
                end_exclusive: 4,
            }),
        )
        .expect("get object")
        .expect("object exists");

    assert_eq!(bytes, b"bcd");
    let events = recorder.events();
    let event = events.last().expect("get event");
    assert_eq!(event.operation, ObjectStoreOperation::Get);
    assert_eq!(event.result, ObjectStoreResultClass::Ok);
    assert_eq!(event.bytes_out, Some(3));
    assert_eq!(event.key_class, KeyClass::Content);
    assert_eq!(event.range_class, Some(RangeClass::Bounded));
}

#[test]
fn records_head_and_head_with_checksum_as_distinct_operations() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite("namespaces/ns-1/lease.json", b"lease")
        .expect("put object");
    store.head("namespaces/ns-1/lease.json").expect("head");
    store
        .head_with_checksum("namespaces/ns-1/lease.json")
        .expect("head with checksum");

    let events = recorder.events();
    assert_eq!(events[1].operation, ObjectStoreOperation::Head);
    assert_eq!(events[2].operation, ObjectStoreOperation::HeadWithChecksum);
    assert_eq!(events[1].key_class, KeyClass::Lease);
    assert_eq!(events[2].key_class, KeyClass::Lease);
}

#[test]
fn records_not_found_without_key_leak() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());
    let raw_key = "namespaces/ns-1/wal/secret-segment.cbor.zst";

    assert!(store.get(raw_key, None).expect("get missing").is_none());

    let event = recorder.events().pop().expect("get event");
    assert_eq!(event.operation, ObjectStoreOperation::Get);
    assert_eq!(event.result, ObjectStoreResultClass::NotFound);
    let encoded = serde_json::to_string(&event).expect("serialize event");
    assert!(!encoded.contains(raw_key));
    assert!(!encoded.contains("secret-segment"));
}

#[test]
fn records_invalid_key_without_error_text() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());
    let raw_key = "../escape";

    store
        .put_overwrite(raw_key, b"bad")
        .expect_err("invalid key should fail");

    let event = recorder.events().pop().expect("put event");
    assert_eq!(event.operation, ObjectStoreOperation::Put);
    assert_eq!(event.result, ObjectStoreResultClass::InvalidKey);
    let encoded = serde_json::to_string(&event).expect("serialize event");
    assert!(!encoded.contains(raw_key));
    assert!(!encoded.contains("escape"));
}

#[test]
fn records_list_count() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = measured_store(temp_dir.path(), recorder.clone());

    store
        .put_overwrite("namespaces/ns-1/descriptor.json", b"descriptor")
        .expect("put descriptor");
    store
        .put_overwrite("namespaces/ns-1/head.json", b"head")
        .expect("put head");
    let keys = store
        .list_prefix("namespaces/ns-1/")
        .expect("list namespace");

    assert_eq!(keys.len(), 2);
    let event = recorder.events().pop().expect("list event");
    assert_eq!(event.operation, ObjectStoreOperation::ListPrefix);
    assert_eq!(event.result, ObjectStoreResultClass::Ok);
    assert_eq!(event.item_count, Some(2));
    assert_eq!(event.range_class, Some(RangeClass::Prefix));
}

#[test]
fn default_key_classifier_maps_known_families_conservatively() {
    let classifier = DefaultKeyClassifier;

    assert_eq!(
        classifier.classify_key("content-stores/cs_abc/blobs/sha256/ab/cd/abcdef"),
        KeyClass::Content
    );
    assert_eq!(
        classifier.classify_key("content-stores/cs_abc/descriptor.json"),
        KeyClass::Metadata
    );
    assert_eq!(
        classifier.classify_key("namespaces/ns-1/head.json"),
        KeyClass::NamespaceHead
    );
    assert_eq!(
        classifier.classify_key("namespaces/ns-1/lease.json"),
        KeyClass::Lease
    );
    assert_eq!(
        classifier.classify_key("namespaces/ns-1/derived/checkpoint-builder/progress.json"),
        KeyClass::DerivedProgress
    );
    assert_eq!(
        classifier.classify_key("namespaces/ns-1/checkpoints/00000000000000000001/manifest.json"),
        KeyClass::Metadata
    );
    assert_eq!(
        classifier.classify_key("elsewhere/raw-key"),
        KeyClass::Unknown
    );
}

#[test]
fn jsonl_recorder_writes_valid_events_without_raw_keys() {
    let temp_dir = tempdir().expect("tempdir");
    let jsonl = NamedTempFile::new().expect("jsonl tempfile");
    let recorder = Arc::new(JsonlPerfRecorder::create(jsonl.path()).expect("jsonl recorder"));
    let store = measured_store(temp_dir.path(), recorder);
    let raw_key = "namespaces/ns-1/head.json";

    store.put_overwrite(raw_key, b"head").expect("put object");

    let lines = std::fs::read_to_string(jsonl.path()).expect("read jsonl");
    let mut lines = lines.lines();
    let line = lines.next().expect("one event line");
    assert!(lines.next().is_none());
    let event: serde_json::Value = serde_json::from_str(line).expect("valid event json");
    assert_eq!(event["operation"], "put");
    assert_eq!(event["key_class"], "namespace_head");
    assert!(!line.contains(raw_key));
    assert!(!line.contains("ns-1"));
}

fn measured_store<R>(
    root: &std::path::Path,
    recorder: Arc<R>,
) -> MeasuredStore<LocalFsStore, R, DefaultKeyClassifier>
where
    R: PerfRecorder,
{
    MeasuredStore::new(
        LocalFsStore::new(root).expect("local fs store"),
        recorder,
        Arc::new(DefaultKeyClassifier),
    )
    .with_store_kind("local-fs")
}
