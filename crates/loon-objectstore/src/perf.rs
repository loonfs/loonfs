use crate::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const OBJECT_STORE_PERF_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStorePerfEvent {
    pub schema_version: u32,
    pub timestamp_ms: u128,
    pub operation: ObjectStoreOperation,
    pub elapsed_micros: u128,
    pub result: ObjectStoreResultClass,
    pub bytes_in: Option<u64>,
    pub bytes_out: Option<u64>,
    pub item_count: Option<u64>,
    pub key_class: KeyClass,
    pub range_class: Option<RangeClass>,
    pub put_mode: Option<PutModeClass>,
    pub store_kind: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreOperation {
    Head,
    HeadWithChecksum,
    Get,
    Put,
    Delete,
    ListPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreResultClass {
    Ok,
    NotFound,
    InvalidKey,
    InvalidRange,
    PreconditionFailed,
    Conflict,
    Unsupported,
    Transport,
    OtherError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyClass {
    Content,
    Metadata,
    NamespaceHead,
    Lease,
    DerivedProgress,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeClass {
    FullObject,
    Prefix,
    Suffix,
    Bounded,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PutModeClass {
    Overwrite,
    CreateIfAbsent,
    CompareAndSwap,
}

pub trait PerfRecorder: Send + Sync + 'static {
    fn record(&self, event: ObjectStorePerfEvent);
}

pub struct NoopPerfRecorder;

impl PerfRecorder for NoopPerfRecorder {
    fn record(&self, _event: ObjectStorePerfEvent) {}
}

#[derive(Default)]
pub struct VecPerfRecorder {
    events: Mutex<Vec<ObjectStorePerfEvent>>,
}

impl VecPerfRecorder {
    pub fn events(&self) -> Vec<ObjectStorePerfEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl PerfRecorder for VecPerfRecorder {
    fn record(&self, event: ObjectStorePerfEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }
}

pub struct JsonlPerfRecorder {
    writer: Mutex<BufWriter<File>>,
}

impl JsonlPerfRecorder {
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        if let Some(parent) = path
            .as_ref()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

impl PerfRecorder for JsonlPerfRecorder {
    fn record(&self, event: ObjectStorePerfEvent) {
        let Ok(mut writer) = self.writer.lock() else {
            return;
        };
        if serde_json::to_writer(&mut *writer, &event).is_err() {
            return;
        }
        let _ = writer.write_all(b"\n");
    }
}

pub trait KeyClassifier: Send + Sync + 'static {
    fn classify_key(&self, key: &str) -> KeyClass;
}

pub struct DefaultKeyClassifier;

impl KeyClassifier for DefaultKeyClassifier {
    fn classify_key(&self, key: &str) -> KeyClass {
        let segments = key.split('/').collect::<Vec<_>>();
        match segments.as_slice() {
            ["content-stores", _, "blobs", ..] => KeyClass::Content,
            ["content-stores", ..] => KeyClass::Metadata,
            ["namespaces", _, "head.json"] => KeyClass::NamespaceHead,
            ["namespaces", _, "lease.json"] => KeyClass::Lease,
            ["namespaces", _, "derived", .., "progress.json"] => KeyClass::DerivedProgress,
            ["namespaces", ..] | ["queue", ..] => KeyClass::Metadata,
            _ => KeyClass::Unknown,
        }
    }
}

pub struct InstrumentedObjectStore<S, R = NoopPerfRecorder, K = DefaultKeyClassifier> {
    inner: S,
    recorder: Arc<R>,
    key_classifier: Arc<K>,
    store_kind: Option<String>,
}

impl<S> InstrumentedObjectStore<S, NoopPerfRecorder, DefaultKeyClassifier> {
    pub fn noop(inner: S) -> Self {
        Self {
            inner,
            recorder: Arc::new(NoopPerfRecorder),
            key_classifier: Arc::new(DefaultKeyClassifier),
            store_kind: None,
        }
    }
}

impl<S, R, K> InstrumentedObjectStore<S, R, K>
where
    R: PerfRecorder,
    K: KeyClassifier,
{
    pub fn new(inner: S, recorder: Arc<R>, key_classifier: Arc<K>) -> Self {
        Self {
            inner,
            recorder,
            key_classifier,
            store_kind: None,
        }
    }

    pub fn with_store_kind(mut self, store_kind: impl Into<String>) -> Self {
        self.store_kind = Some(store_kind.into());
        self
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, R, K> ObjectStore for InstrumentedObjectStore<S, R, K>
where
    S: ObjectStore,
    R: PerfRecorder,
    K: KeyClassifier,
{
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head(key);
        self.record_head_like(ObjectStoreOperation::Head, key, start.elapsed(), &result);
        result
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head_with_checksum(key);
        self.record_head_like(
            ObjectStoreOperation::HeadWithChecksum,
            key,
            start.elapsed(),
            &result,
        );
        result
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.get(key, range.clone());
        self.record_get(key, range.as_ref(), start.elapsed(), &result);
        result
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put(key, bytes, mode.clone());
        self.record_put(key, bytes.len() as u64, &mode, start.elapsed(), &result);
        result
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put_overwrite(key, bytes);
        self.record_put(
            key,
            bytes.len() as u64,
            &PutMode::Overwrite,
            start.elapsed(),
            &result,
        );
        result
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put_if_absent(key, bytes);
        self.record_put(
            key,
            bytes.len() as u64,
            &PutMode::CreateIfAbsent,
            start.elapsed(),
            &result,
        );
        result
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let mode = PutMode::CompareAndSwap {
            expected_etag: expected_etag.to_owned(),
        };
        let result = self.inner.compare_and_swap(key, expected_etag, bytes);
        self.record_put(key, bytes.len() as u64, &mode, start.elapsed(), &result);
        result
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.delete(key);
        self.record_unit(ObjectStoreOperation::Delete, key, start.elapsed(), &result);
        result
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.list_prefix(prefix);
        self.record_list(prefix, start.elapsed(), &result);
        result
    }
}

impl<S, R, K> InstrumentedObjectStore<S, R, K>
where
    R: PerfRecorder,
    K: KeyClassifier,
{
    fn record_head_like(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        elapsed: Duration,
        result: &Result<Option<ObjectMetadata>, ObjectStoreError>,
    ) {
        self.record(ObjectStorePerfEvent {
            schema_version: OBJECT_STORE_PERF_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            operation,
            elapsed_micros: elapsed.as_micros(),
            result: classify_optional_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: None,
            key_class: self.key_classifier.classify_key(key),
            range_class: None,
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_get(
        &self,
        key: &str,
        range: Option<&ByteRange>,
        elapsed: Duration,
        result: &Result<Option<Vec<u8>>, ObjectStoreError>,
    ) {
        self.record(ObjectStorePerfEvent {
            schema_version: OBJECT_STORE_PERF_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            operation: ObjectStoreOperation::Get,
            elapsed_micros: elapsed.as_micros(),
            result: classify_optional_result(result),
            bytes_in: None,
            bytes_out: result
                .as_ref()
                .ok()
                .and_then(|bytes| bytes.as_ref().map(|bytes| bytes.len() as u64)),
            item_count: None,
            key_class: self.key_classifier.classify_key(key),
            range_class: Some(classify_range(range)),
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_put(
        &self,
        key: &str,
        bytes_in: u64,
        mode: &PutMode,
        elapsed: Duration,
        result: &Result<ObjectMetadata, ObjectStoreError>,
    ) {
        self.record(ObjectStorePerfEvent {
            schema_version: OBJECT_STORE_PERF_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            operation: ObjectStoreOperation::Put,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: Some(bytes_in),
            bytes_out: None,
            item_count: None,
            key_class: self.key_classifier.classify_key(key),
            range_class: None,
            put_mode: Some(classify_put_mode(mode)),
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_unit(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        elapsed: Duration,
        result: &Result<(), ObjectStoreError>,
    ) {
        self.record(ObjectStorePerfEvent {
            schema_version: OBJECT_STORE_PERF_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            operation,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: None,
            key_class: self.key_classifier.classify_key(key),
            range_class: None,
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_list(
        &self,
        prefix: &str,
        elapsed: Duration,
        result: &Result<Vec<String>, ObjectStoreError>,
    ) {
        self.record(ObjectStorePerfEvent {
            schema_version: OBJECT_STORE_PERF_SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            operation: ObjectStoreOperation::ListPrefix,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: result.as_ref().ok().map(|items| items.len() as u64),
            key_class: self.key_classifier.classify_key(prefix),
            range_class: Some(RangeClass::Prefix),
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record(&self, event: ObjectStorePerfEvent) {
        self.recorder.record(event);
    }
}

fn classify_optional_result<T>(
    result: &Result<Option<T>, ObjectStoreError>,
) -> ObjectStoreResultClass {
    match result {
        Ok(Some(_)) => ObjectStoreResultClass::Ok,
        Ok(None) => ObjectStoreResultClass::NotFound,
        Err(error) => classify_error(error),
    }
}

fn classify_result<T>(result: &Result<T, ObjectStoreError>) -> ObjectStoreResultClass {
    match result {
        Ok(_) => ObjectStoreResultClass::Ok,
        Err(error) => classify_error(error),
    }
}

fn classify_error(error: &ObjectStoreError) -> ObjectStoreResultClass {
    match error {
        ObjectStoreError::NotFound => ObjectStoreResultClass::NotFound,
        ObjectStoreError::InvalidKey(_) => ObjectStoreResultClass::InvalidKey,
        ObjectStoreError::InvalidRange => ObjectStoreResultClass::InvalidRange,
        ObjectStoreError::PreconditionFailed => ObjectStoreResultClass::PreconditionFailed,
        ObjectStoreError::Conflict => ObjectStoreResultClass::Conflict,
        ObjectStoreError::Unsupported(_) => ObjectStoreResultClass::Unsupported,
        ObjectStoreError::Transport(_) => ObjectStoreResultClass::Transport,
    }
}

fn classify_range(range: Option<&ByteRange>) -> RangeClass {
    let Some(range) = range else {
        return RangeClass::FullObject;
    };
    if range.start_inclusive == range.end_exclusive {
        RangeClass::Empty
    } else if range.start_inclusive == 0 {
        RangeClass::Prefix
    } else if range.end_exclusive == u64::MAX {
        RangeClass::Suffix
    } else {
        RangeClass::Bounded
    }
}

fn classify_put_mode(mode: &PutMode) -> PutModeClass {
    match mode {
        PutMode::Overwrite => PutModeClass::Overwrite,
        PutMode::CreateIfAbsent => PutModeClass::CreateIfAbsent,
        PutMode::CompareAndSwap { .. } => PutModeClass::CompareAndSwap,
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
