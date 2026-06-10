use crate::fault::{FaultSchedule, ObjectStoreFault, ScheduledFault};
use crate::object_op::{ObjectOp, ObjectOpKind};
use crate::trace::{RunId, SharedSimTrace, SimEventResult, SimTrace, SimTraceEvent};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loon_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
pub struct FaultInjectingObjectStore<S> {
    inner: S,
    schedule: FaultSchedule,
    trace: SharedSimTrace,
    next_step: AtomicU64,
    stale_values: Mutex<HashMap<String, Vec<ObjectBody>>>,
    recent_writes: Mutex<Vec<String>>,
}

impl<S> FaultInjectingObjectStore<S> {
    pub fn new(inner: S, schedule: FaultSchedule) -> Self {
        let trace = SharedSimTrace::new(SimTrace::new(
            RunId(format!("sim-{:016x}", schedule.seed.0)),
            schedule.seed,
        ));
        Self::with_trace(inner, schedule, trace)
    }

    pub fn with_trace(inner: S, schedule: FaultSchedule, trace: SharedSimTrace) -> Self {
        Self {
            inner,
            schedule,
            trace,
            next_step: AtomicU64::new(1),
            stale_values: Mutex::new(HashMap::new()),
            recent_writes: Mutex::new(Vec::new()),
        }
    }

    pub fn trace(&self) -> SharedSimTrace {
        self.trace.clone()
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    fn next_object_op(&self, kind: ObjectOpKind, key: &str) -> ObjectOp {
        ObjectOp {
            step: self.next_step.fetch_add(1, Ordering::SeqCst),
            kind,
            key: key.to_owned(),
        }
    }

    fn push_trace(
        &self,
        op: ObjectOp,
        operation: impl Into<String>,
        fault: Option<ObjectStoreFault>,
        result: SimEventResult,
    ) {
        self.trace.push(SimTraceEvent {
            step: op.step,
            actor: None,
            operation: operation.into(),
            object_op: Some(op),
            injected_fault: fault,
            result,
            model_hash: None,
            core_hash: None,
        });
    }

    fn scheduled_fault(&self, op: &ObjectOp) -> Option<ScheduledFault> {
        self.schedule.fault_for(op).cloned()
    }

    async fn remember_current_value(&self, key: &str)
    where
        S: ObjectStore,
    {
        if let Ok(Some(body)) = self.inner.get_with_metadata(key).await {
            self.stale_values
                .lock()
                .expect("stale value lock poisoned")
                .entry(key.to_owned())
                .or_default()
                .push(body);
        }
    }

    fn remember_successful_write(&self, key: &str) {
        self.recent_writes
            .lock()
            .expect("recent writes lock poisoned")
            .push(key.to_owned());
    }

    fn stale_value(&self, key: &str) -> Option<ObjectBody> {
        self.stale_values
            .lock()
            .expect("stale value lock poisoned")
            .get(key)
            .and_then(|values| values.last().cloned())
    }

    async fn put_with(
        &self,
        key: &str,
        kind: ObjectOpKind,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError>
    where
        S: ObjectStore,
    {
        let op = self.next_object_op(kind, key);
        let scheduled = self.scheduled_fault(&op);
        match scheduled.as_ref().map(|fault| &fault.fault) {
            Some(ObjectStoreFault::PutReturnsTransientError) => {
                self.push_trace(
                    op,
                    "put_transient_error",
                    Some(ObjectStoreFault::PutReturnsTransientError),
                    SimEventResult::Error {
                        class: "transport".to_owned(),
                    },
                );
                Err(ObjectStoreError::Transport(
                    "simulated transient put failure".to_owned(),
                ))
            }
            Some(ObjectStoreFault::PutSucceedsButResponseLost) => {
                self.remember_current_value(key).await;
                let result = self.inner.put(key, bytes, mode).await;
                if result.is_ok() {
                    self.remember_successful_write(key);
                    self.push_trace(
                        op,
                        "put_lost_success",
                        Some(ObjectStoreFault::PutSucceedsButResponseLost),
                        SimEventResult::Error {
                            class: "transport".to_owned(),
                        },
                    );
                    Err(ObjectStoreError::Transport(
                        "simulated lost put response".to_owned(),
                    ))
                } else {
                    self.push_trace(
                        op,
                        "put_lost_success_inner_failed",
                        Some(ObjectStoreFault::PutSucceedsButResponseLost),
                        result_class(&result),
                    );
                    result
                }
            }
            Some(ObjectStoreFault::CompareAndSwapStale) if kind == ObjectOpKind::CompareAndSwap => {
                self.push_trace(
                    op,
                    "compare_and_swap_stale",
                    Some(ObjectStoreFault::CompareAndSwapStale),
                    SimEventResult::Error {
                        class: "precondition_failed".to_owned(),
                    },
                );
                Err(ObjectStoreError::PreconditionFailed)
            }
            Some(incompatible) => {
                let fault = incompatible.clone();
                self.remember_current_value(key).await;
                let result = self.inner.put(key, bytes, mode).await;
                if result.is_ok() {
                    self.remember_successful_write(key);
                }
                self.push_trace(
                    op,
                    "put_fault_skipped",
                    Some(fault),
                    SimEventResult::Skipped {
                        reason: "fault_not_applicable_to_operation".to_owned(),
                    },
                );
                result
            }
            None => {
                self.remember_current_value(key).await;
                let result = self.inner.put(key, bytes, mode).await;
                if result.is_ok() {
                    self.remember_successful_write(key);
                }
                self.push_trace(op, "put", None, result_class(&result));
                result
            }
        }
    }
}

#[async_trait]
impl<S> ObjectStore for FaultInjectingObjectStore<S>
where
    S: ObjectStore,
{
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::Head, key);
        let result = self.inner.head(key).await;
        self.push_trace(op, "head", None, result_class(&result));
        result
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::HeadWithChecksum, key);
        let result = self.inner.head_with_checksum(key).await;
        self.push_trace(op, "head_with_checksum", None, result_class(&result));
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::Get, key);
        let scheduled = self.scheduled_fault(&op);
        match scheduled.as_ref().map(|fault| &fault.fault) {
            Some(ObjectStoreFault::GetReturnsStaleValue) => {
                if let Some(body) = self.stale_value(key) {
                    self.push_trace(
                        op,
                        "get_stale_value",
                        Some(ObjectStoreFault::GetReturnsStaleValue),
                        SimEventResult::Ok,
                    );
                    return Ok(Some(apply_range(body.bytes, range)?));
                }
                let result = self.inner.get(key, range).await;
                self.push_trace(
                    op,
                    "get_stale_value_skipped",
                    Some(ObjectStoreFault::GetReturnsStaleValue),
                    SimEventResult::Skipped {
                        reason: "no_stale_value_recorded".to_owned(),
                    },
                );
                result
            }
            Some(ObjectStoreFault::CorruptObjectBytes) => {
                let mut result = self.inner.get(key, range).await;
                if let Ok(Some(bytes)) = &mut result {
                    let mut corrupted = bytes.to_vec();
                    if let Some(first) = corrupted.first_mut() {
                        *first ^= 0x01;
                    }
                    *bytes = Bytes::from(corrupted);
                }
                self.push_trace(
                    op,
                    "get_corrupt_bytes",
                    Some(ObjectStoreFault::CorruptObjectBytes),
                    result_class(&result),
                );
                result
            }
            Some(incompatible) => {
                let result = self.inner.get(key, range).await;
                self.push_trace(
                    op,
                    "get_fault_skipped",
                    Some(incompatible.clone()),
                    SimEventResult::Skipped {
                        reason: "fault_not_applicable_to_operation".to_owned(),
                    },
                );
                result
            }
            None => {
                let result = self.inner.get(key, range).await;
                self.push_trace(op, "get", None, result_class(&result));
                result
            }
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::Get, key);
        let scheduled = self.scheduled_fault(&op);
        match scheduled.as_ref().map(|fault| &fault.fault) {
            Some(ObjectStoreFault::GetReturnsStaleValue) => {
                if let Some(body) = self.stale_value(key) {
                    self.push_trace(
                        op,
                        "get_with_metadata_stale_value",
                        Some(ObjectStoreFault::GetReturnsStaleValue),
                        SimEventResult::Ok,
                    );
                    return Ok(Some(body));
                }
                let result = self.inner.get_with_metadata(key).await;
                self.push_trace(
                    op,
                    "get_with_metadata_stale_value_skipped",
                    Some(ObjectStoreFault::GetReturnsStaleValue),
                    SimEventResult::Skipped {
                        reason: "no_stale_value_recorded".to_owned(),
                    },
                );
                result
            }
            Some(ObjectStoreFault::CorruptObjectBytes) => {
                let mut result = self.inner.get_with_metadata(key).await;
                if let Ok(Some(body)) = &mut result {
                    if let Some(first) = body.bytes.first_mut() {
                        *first ^= 0x01;
                    }
                }
                self.push_trace(
                    op,
                    "get_with_metadata_corrupt_bytes",
                    Some(ObjectStoreFault::CorruptObjectBytes),
                    result_class(&result),
                );
                result
            }
            Some(incompatible) => {
                let result = self.inner.get_with_metadata(key).await;
                self.push_trace(
                    op,
                    "get_with_metadata_fault_skipped",
                    Some(incompatible.clone()),
                    SimEventResult::Skipped {
                        reason: "fault_not_applicable_to_operation".to_owned(),
                    },
                );
                result
            }
            None => {
                let result = self.inner.get_with_metadata(key).await;
                self.push_trace(op, "get_with_metadata", None, result_class(&result));
                result
            }
        }
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let kind = put_mode_kind(&mode);
        self.put_with(key, kind, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::Delete, key);
        let scheduled = self.scheduled_fault(&op);
        match scheduled.as_ref().map(|fault| &fault.fault) {
            Some(ObjectStoreFault::DeleteReturnsTransientError) => {
                self.push_trace(
                    op,
                    "delete_transient_error",
                    Some(ObjectStoreFault::DeleteReturnsTransientError),
                    SimEventResult::Error {
                        class: "transport".to_owned(),
                    },
                );
                Err(ObjectStoreError::Transport(
                    "simulated transient delete failure".to_owned(),
                ))
            }
            Some(incompatible) => {
                let result = self.inner.delete(key).await;
                self.push_trace(
                    op,
                    "delete_fault_skipped",
                    Some(incompatible.clone()),
                    SimEventResult::Skipped {
                        reason: "fault_not_applicable_to_operation".to_owned(),
                    },
                );
                result
            }
            None => {
                let result = self.inner.delete(key).await;
                self.push_trace(op, "delete", None, result_class(&result));
                result
            }
        }
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let op = self.next_object_op(ObjectOpKind::ListPrefix, prefix);
        let scheduled = self.scheduled_fault(&op);
        match scheduled.as_ref().map(|fault| &fault.fault) {
            Some(ObjectStoreFault::ListOmitsRecentObject) => {
                let mut result = self.inner.list_prefix(prefix).await;
                if let Ok(keys) = &mut result {
                    keys.sort();
                    if let Some(index) = recent_key_to_omit(
                        keys,
                        prefix,
                        &self
                            .recent_writes
                            .lock()
                            .expect("recent writes lock poisoned"),
                    ) {
                        keys.remove(index);
                    }
                }
                self.push_trace(
                    op,
                    "list_omits_recent_object",
                    Some(ObjectStoreFault::ListOmitsRecentObject),
                    result_class(&result),
                );
                result
            }
            Some(incompatible) => {
                let result = self.inner.list_prefix(prefix).await;
                self.push_trace(
                    op,
                    "list_fault_skipped",
                    Some(incompatible.clone()),
                    SimEventResult::Skipped {
                        reason: "fault_not_applicable_to_operation".to_owned(),
                    },
                );
                result
            }
            None => {
                let result = self.inner.list_prefix(prefix).await;
                self.push_trace(op, "list_prefix", None, result_class(&result));
                result
            }
        }
    }

    async fn put_overwrite(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_with(key, ObjectOpKind::Put, bytes, PutMode::Overwrite)
            .await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_with(
            key,
            ObjectOpKind::PutIfAbsent,
            bytes,
            PutMode::CreateIfAbsent,
        )
        .await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_with(
            key,
            ObjectOpKind::CompareAndSwap,
            bytes,
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
        .await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn put_mode_kind(mode: &PutMode) -> ObjectOpKind {
    match mode {
        PutMode::Overwrite => ObjectOpKind::Put,
        PutMode::CreateIfAbsent => ObjectOpKind::PutIfAbsent,
        PutMode::CompareAndSwap { .. } => ObjectOpKind::CompareAndSwap,
    }
}

fn apply_range(bytes: Vec<u8>, range: Option<ByteRange>) -> Result<Bytes, ObjectStoreError> {
    let Some(range) = range else {
        return Ok(Bytes::from(bytes));
    };
    let start =
        usize::try_from(range.start_inclusive).map_err(|_| ObjectStoreError::InvalidRange)?;
    let end = usize::try_from(range.end_exclusive).map_err(|_| ObjectStoreError::InvalidRange)?;
    if start > end || end > bytes.len() {
        return Err(ObjectStoreError::InvalidRange);
    }
    Ok(Bytes::copy_from_slice(&bytes[start..end]))
}

fn recent_key_to_omit(keys: &[String], prefix: &str, recent_writes: &[String]) -> Option<usize> {
    recent_writes
        .iter()
        .rev()
        .find(|key| key.starts_with(prefix))
        .and_then(|recent| keys.iter().position(|key| key == recent))
}

fn result_class<T>(result: &Result<T, ObjectStoreError>) -> SimEventResult {
    match result {
        Ok(_) => SimEventResult::Ok,
        Err(ObjectStoreError::NotFound) => SimEventResult::Error {
            class: "not_found".to_owned(),
        },
        Err(ObjectStoreError::InvalidKey(_)) => SimEventResult::Error {
            class: "invalid_key".to_owned(),
        },
        Err(ObjectStoreError::InvalidRange) => SimEventResult::Error {
            class: "invalid_range".to_owned(),
        },
        Err(ObjectStoreError::PreconditionFailed) => SimEventResult::Error {
            class: "precondition_failed".to_owned(),
        },
        Err(ObjectStoreError::Conflict) => SimEventResult::Error {
            class: "conflict".to_owned(),
        },
        Err(ObjectStoreError::Unsupported(_)) => SimEventResult::Error {
            class: "unsupported".to_owned(),
        },
        Err(ObjectStoreError::Transport(_)) => SimEventResult::Error {
            class: "transport".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::ScheduledFault;
    use crate::rng::SimSeed;
    use loon_objectstore::fs::LocalFsStore;

    fn temp_store() -> (tempfile::TempDir, LocalFsStore) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        (temp_dir, store)
    }

    #[tokio::test]
    async fn fault_store_records_trace() {
        let (_temp_dir, store) = temp_store();
        let schedule = FaultSchedule::empty(SimSeed(1));
        let store = FaultInjectingObjectStore::new(store, schedule);

        store
            .put_overwrite("a", Bytes::from_static(b"abc"))
            .await
            .expect("put");
        store.get("a", None).await.expect("get");

        let trace = store.trace().snapshot();
        assert_eq!(trace.events.len(), 2);
        assert_eq!(
            trace.events[0].object_op.as_ref().expect("first op").step,
            1
        );
        assert_eq!(
            trace.events[1].object_op.as_ref().expect("second op").step,
            2
        );
    }

    #[tokio::test]
    async fn lost_success_fault_performs_inner_write_and_returns_error() {
        let (_temp_dir, store) = temp_store();
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            faults: vec![ScheduledFault {
                step: 1,
                op_kind: Some(ObjectOpKind::Put),
                key_contains: None,
                fault: ObjectStoreFault::PutSucceedsButResponseLost,
            }],
        };
        let store = FaultInjectingObjectStore::new(store, schedule);

        assert!(matches!(
            store.put_overwrite("a", Bytes::from_static(b"abc")).await,
            Err(ObjectStoreError::Transport(_))
        ));
        assert_eq!(
            store.inner().get("a", None).await.expect("inner get"),
            Some(Bytes::from_static(b"abc"))
        );
    }

    #[tokio::test]
    async fn transient_put_fault_does_not_perform_inner_write() {
        let (_temp_dir, store) = temp_store();
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            faults: vec![ScheduledFault {
                step: 1,
                op_kind: Some(ObjectOpKind::Put),
                key_contains: None,
                fault: ObjectStoreFault::PutReturnsTransientError,
            }],
        };
        let store = FaultInjectingObjectStore::new(store, schedule);

        assert!(matches!(
            store.put_overwrite("a", Bytes::from_static(b"abc")).await,
            Err(ObjectStoreError::Transport(_))
        ));
        assert_eq!(store.inner().get("a", None).await.expect("inner get"), None);
    }

    #[tokio::test]
    async fn stale_get_with_metadata_returns_stale_body_and_metadata() {
        let (_temp_dir, store) = temp_store();
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            faults: vec![ScheduledFault {
                step: 3,
                op_kind: Some(ObjectOpKind::Get),
                key_contains: None,
                fault: ObjectStoreFault::GetReturnsStaleValue,
            }],
        };
        let store = FaultInjectingObjectStore::new(store, schedule);

        store
            .put_overwrite("a", Bytes::from_static(b"old"))
            .await
            .expect("put old");
        store
            .put_overwrite("a", Bytes::from_static(b"new-value"))
            .await
            .expect("put new");
        let body = store
            .get_with_metadata("a")
            .await
            .expect("get with metadata")
            .expect("body");

        assert_eq!(body.bytes, b"old");
        assert_eq!(body.metadata.size_bytes, 3);
    }

    #[tokio::test]
    async fn list_omission_fault_is_deterministic() {
        let (_temp_dir, store) = temp_store();
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            faults: vec![ScheduledFault {
                step: 3,
                op_kind: Some(ObjectOpKind::ListPrefix),
                key_contains: Some("prefix/".to_owned()),
                fault: ObjectStoreFault::ListOmitsRecentObject,
            }],
        };
        let store = FaultInjectingObjectStore::new(store, schedule);

        store
            .put_overwrite("prefix/a", Bytes::from_static(b"a"))
            .await
            .expect("put a");
        store
            .put_overwrite("prefix/b", Bytes::from_static(b"b"))
            .await
            .expect("put b");
        let keys = store.list_prefix("prefix/").await.expect("list");

        assert_eq!(keys, vec!["prefix/a".to_owned()]);
    }
}
