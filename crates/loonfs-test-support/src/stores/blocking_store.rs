//! A bounded async gate for selected object-store operations.

use super::{KeyPredicate, OperationClass, OperationContext, OperationKind};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;

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
        timeout(Duration::from_secs(10), async {
            while !self.gate.blocked.load(Ordering::SeqCst) {
                let notified = self.gate.blocked_notify.notified();
                if self.gate.blocked.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded wait: `notified` registers on first poll, so a notify
                // between the flag check and the await would otherwise be lost.
                let _ = timeout(Duration::from_millis(50), notified).await;
            }
        })
        .await
        .expect("a selected store operation must reach the block");
    }

    /// Releases all parked operations and disarms a level-triggered gate.
    pub fn release(&self) {
        self.gate.armed.store(false, Ordering::SeqCst);
        self.gate.released.store(true, Ordering::SeqCst);
        self.gate.release_notify.notify_waiters();
    }

    /// Waits until the most recently blocked operation finishes forwarding.
    pub async fn wait_until_completed(&self) {
        timeout(Duration::from_secs(10), async {
            while !self.gate.completed.load(Ordering::SeqCst) {
                let notified = self.gate.completed_notify.notified();
                if self.gate.completed.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded wait: `notified` registers on first poll, so a notify
                // between the flag check and the await would otherwise be lost.
                let _ = timeout(Duration::from_millis(50), notified).await;
            }
        })
        .await
        .expect("a released store operation must finish");
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
        timeout(Duration::from_secs(10), async {
            while !self.gate.released.load(Ordering::SeqCst) {
                let notified = self.gate.release_notify.notified();
                if self.gate.released.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded wait: `notified` registers on first poll, so a notify
                // between the flag check and the await would otherwise be lost.
                let _ = timeout(Duration::from_millis(50), notified).await;
            }
        })
        .await
        .expect("a blocked store operation must be released");
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

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let selected = self.matches(&OperationContext::new(prefix, OperationKind::List));
        let gate = Arc::clone(&self.gate);
        let inner = self.inner.list_prefix_stream(prefix);
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
                    timeout(Duration::from_secs(10), async {
                        while !gate.released.load(Ordering::SeqCst) {
                            let notified = gate.release_notify.notified();
                            if gate.released.load(Ordering::SeqCst) {
                                break;
                            }
                            // Bounded wait: `notified` registers on first poll, so a notify
                            // between the flag check and the await would otherwise be lost.
                            let _ = timeout(Duration::from_millis(50), notified).await;
                        }
                    })
                    .await
                    .expect("a blocked store operation must be released");
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
