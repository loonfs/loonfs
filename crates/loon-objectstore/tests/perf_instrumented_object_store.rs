use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::perf::{
    InstrumentedObjectStore, KeyClass, ObjectStoreOperation, ObjectStoreResultClass, PerfRecorder,
    PutModeClass, RangeClass, VecPerfRecorder,
};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[test]
fn records_put_success() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

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
fn delegates_convenience_writes_to_inner_store() {
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = InstrumentedObjectStore::new(DelegatingWriteStore::default(), recorder.clone());

    store
        .put_overwrite("namespaces/ns-1/head.json", b"overwrite")
        .expect("put overwrite");
    store
        .put_if_absent("namespaces/ns-1/head.json", b"create")
        .expect("put if absent");
    store
        .compare_and_swap("namespaces/ns-1/head.json", "etag-old", b"swap")
        .expect("compare and swap");

    let inner = store.into_inner();
    assert_eq!(
        inner.calls(),
        vec!["put_overwrite", "put_if_absent", "compare_and_swap"]
    );

    let events = recorder.events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].put_mode, Some(PutModeClass::Overwrite));
    assert_eq!(events[1].put_mode, Some(PutModeClass::CreateIfAbsent));
    assert_eq!(events[2].put_mode, Some(PutModeClass::CompareAndSwap));
}

#[test]
fn records_get_success_bytes_out() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecPerfRecorder::default());
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

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
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

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
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
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
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());
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
    let store = instrumented_object_store(temp_dir.path(), recorder.clone());

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

fn instrumented_object_store<R>(
    root: &std::path::Path,
    recorder: Arc<R>,
) -> InstrumentedObjectStore<LocalFsStore>
where
    R: PerfRecorder,
{
    InstrumentedObjectStore::new(LocalFsStore::new(root).expect("local fs store"), recorder)
        .with_store_kind("local-fs")
}

#[derive(Default)]
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
            size_bytes: 0,
            checksum_sha256: None,
        }
    }
}

impl ObjectStore for DelegatingWriteStore {
    fn head(&self, _key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(None)
    }

    fn get(
        &self,
        _key: &str,
        _range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        Ok(None)
    }

    fn put(
        &self,
        _key: &str,
        _bytes: &[u8],
        _mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put"))
    }

    fn put_overwrite(&self, _key: &str, _bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put_overwrite"))
    }

    fn put_if_absent(&self, _key: &str, _bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("put_if_absent"))
    }

    fn compare_and_swap(
        &self,
        _key: &str,
        _expected_etag: &str,
        _bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(self.record_call("compare_and_swap"))
    }

    fn delete(&self, _key: &str) -> Result<(), ObjectStoreError> {
        Ok(())
    }

    fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        Ok(Vec::new())
    }
}
