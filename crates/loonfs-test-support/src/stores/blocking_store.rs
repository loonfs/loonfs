//! An async gate for selected object-store operations.
//! Tests can wait for both the blocked operation and its eventual completion.

use super::{KeyPredicate, OperationClass, OperationContext, OperationKind};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use loonfs_objectstore::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::fmt;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;

/// Waits until the latch flag is set.
///
/// The helper has no local timeout because tests control both sides of the
/// gate; the test harness reports gates that are never released. The
/// notification future is registered before checking the flag, preventing a
/// notification from being lost between the check and the wait.
async fn wait_for_latch(flag: &AtomicBool, notify: &Notify) {
    loop {
        let mut notified = pin!(notify.notified());
        notified.as_mut().enable();
        if flag.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

#[derive(Debug)]
struct Gate {
    armed: AtomicBool,
    block_next: AtomicUsize,
    blocked: AtomicBool,
    released: AtomicBool,
    completed: AtomicBool,
    blocked_notify: Notify,
    release_notify: Notify,
    completed_notify: Notify,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            block_next: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            released: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            blocked_notify: Notify::new(),
            release_notify: Notify::new(),
            completed_notify: Notify::new(),
        }
    }
}

/// Parks selected operations until a test releases them.
pub struct BlockingStore<S> {
    inner: S,
    predicate: Arc<Predicate>,
    gate: Arc<Gate>,
}

impl<S> BlockingStore<S> {
    /// Selects operations by key predicate and operation class.
    pub fn new(inner: S, keys: KeyPredicate, operation: OperationClass) -> Self {
        Self::matching(inner, move |context| {
            keys.matches(context.key()) && operation.matches(context.kind())
        })
    }

    /// Selects operations with an arbitrary operation predicate.
    pub fn matching(
        inner: S,
        predicate: impl for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            predicate: Arc::new(predicate),
            gate: Arc::new(Gate::default()),
        }
    }

    /// Arms a level-triggered gate. Every matching operation parks until release.
    pub fn arm(&self) {
        self.prepare();
        self.gate.armed.store(true, Ordering::SeqCst);
    }

    /// Arms a one-shot gate for the next matching operation.
    pub fn block_next(&self) {
        self.prepare();
        self.gate.block_next.store(1, Ordering::SeqCst);
    }

    /// Waits until a selected operation has parked.
    pub async fn wait_until_blocked(&self) {
        wait_for_latch(&self.gate.blocked, &self.gate.blocked_notify).await;
    }

    /// Releases all parked operations and disarms a level-triggered gate.
    pub fn release(&self) {
        self.gate.armed.store(false, Ordering::SeqCst);
        self.gate.released.store(true, Ordering::SeqCst);
        self.gate.release_notify.notify_waiters();
    }

    /// Waits until the most recently blocked operation finishes forwarding.
    pub async fn wait_until_completed(&self) {
        wait_for_latch(&self.gate.completed, &self.gate.completed_notify).await;
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn prepare(&self) {
        self.gate.released.store(false, Ordering::SeqCst);
        self.gate.blocked.store(false, Ordering::SeqCst);
        self.gate.completed.store(false, Ordering::SeqCst);
    }

    fn matches(&self, context: &OperationContext<'_>) -> bool {
        (self.predicate)(context)
    }

    async fn block_if_selected(&self, context: &OperationContext<'_>) -> bool {
        if !self.matches(context) {
            return false;
        }
        let level_triggered = self.gate.armed.load(Ordering::SeqCst);
        let one_shot = !level_triggered
            && self
                .gate
                .block_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        if !level_triggered && !one_shot {
            return false;
        }

        self.gate.blocked.store(true, Ordering::SeqCst);
        self.gate.blocked_notify.notify_waiters();
        wait_for_latch(&self.gate.released, &self.gate.release_notify).await;
        self.gate.blocked.store(false, Ordering::SeqCst);
        true
    }

    fn mark_completed(&self, was_blocked: bool) {
        if was_blocked {
            self.gate.completed.store(true, Ordering::SeqCst);
            self.gate.completed_notify.notify_waiters();
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for BlockingStore<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingStore")
            .field("inner", &self.inner)
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for BlockingStore<S> {
    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(key, OperationKind::Head))
            .await;
        let result = self.inner.head_stored_checksum(key).await;
        self.mark_completed(blocked);
        result
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(key, OperationKind::Head))
            .await;
        let result = self.inner.head(key).await;
        self.mark_completed(blocked);
        result
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(key, OperationKind::GetWithMetadata))
            .await;
        let result = self.inner.get_with_metadata(key).await;
        self.mark_completed(blocked);
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(
                key,
                OperationKind::Get {
                    range: range.as_ref(),
                },
            ))
            .await;
        let result = self.inner.get(key, range).await;
        self.mark_completed(blocked);
        result
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(
                key,
                OperationKind::Put {
                    bytes: &bytes,
                    mode: &mode,
                },
            ))
            .await;
        let result = self.inner.put(key, bytes, mode).await;
        self.mark_completed(blocked);
        result
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(
                key,
                OperationKind::PutStreamed { mode: &mode },
            ))
            .await;
        let result = self.inner.put_streamed(key, body, mode).await;
        self.mark_completed(blocked);
        result
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(
                key,
                OperationKind::CompareAndSwap {
                    expected_etag,
                    bytes: &bytes,
                },
            ))
            .await;
        let result = self.inner.compare_and_swap(key, expected_etag, bytes).await;
        self.mark_completed(blocked);
        result
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let blocked = self
            .block_if_selected(&OperationContext::new(key, OperationKind::Delete))
            .await;
        let result = self.inner.delete(key).await;
        self.mark_completed(blocked);
        result
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let selected = self.matches(&OperationContext::new(prefix, OperationKind::List));
        let gate = Arc::clone(&self.gate);
        let inner = self.inner.list_prefix_from_stream(prefix, start_after);
        if !selected {
            return inner;
        }
        Box::pin(
            stream::once(async move {
                let level_triggered = gate.armed.load(Ordering::SeqCst);
                let one_shot = !level_triggered
                    && gate
                        .block_next
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                            remaining.checked_sub(1)
                        })
                        .is_ok();
                if level_triggered || one_shot {
                    gate.blocked.store(true, Ordering::SeqCst);
                    gate.blocked_notify.notify_waiters();
                    wait_for_latch(&gate.released, &gate.release_notify).await;
                    gate.blocked.store(false, Ordering::SeqCst);
                    gate.completed.store(true, Ordering::SeqCst);
                    gate.completed_notify.notify_waiters();
                }
                inner
            })
            .flatten(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::{KeyPredicate, OperationClass};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use std::time::Duration;
    use tempfile::tempdir;

    /// The gate carries no wall-clock budget of its own: a parked operation
    /// waits for the release however long the test takes to reach it. Time is
    /// paused, so the hold below costs nothing to run while being longer than
    /// any deadline this helper could plausibly have carried.
    #[tokio::test(start_paused = true)]
    async fn a_parked_operation_waits_for_the_release_however_long_it_takes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::exact("parked"),
            OperationClass::Put,
        ));

        store.block_next();
        let put = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                store
                    .put("parked", Bytes::from_static(b"body"), PutMode::Overwrite)
                    .await
            }
        });
        store.wait_until_blocked().await;

        tokio::time::advance(Duration::from_secs(3_600)).await;
        assert!(
            !put.is_finished(),
            "the gate must still hold the operation after a long test-side pause"
        );

        store.release();
        store.wait_until_completed().await;
        put.await
            .expect("join the parked put")
            .expect("the released put succeeds");
    }
}
