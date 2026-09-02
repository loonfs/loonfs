//! An async gate for selected object-store operations.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationClass, OperationContext, Outcome,
};
use async_trait::async_trait;
use std::fmt;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;

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

#[derive(Debug, Default)]
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

impl Gate {
    async fn park(&self) -> bool {
        let level_triggered = self.armed.load(Ordering::SeqCst);
        let one_shot = !level_triggered
            && self
                .block_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        if !level_triggered && !one_shot {
            return false;
        }
        self.blocked.store(true, Ordering::SeqCst);
        self.blocked_notify.notify_waiters();
        wait_for_latch(&self.released, &self.release_notify).await;
        self.blocked.store(false, Ordering::SeqCst);
        true
    }

    fn mark_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
        self.completed_notify.notify_waiters();
    }
}

/// Intercepts selected operations with an async gate.
pub struct BlockingInterceptor {
    predicate: Arc<Predicate>,
    gate: Arc<Gate>,
}

impl fmt::Debug for BlockingInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockingInterceptor")
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

/// Parks selected operations until a test releases them.
pub type BlockingStore<S> = InterceptStore<S, BlockingInterceptor>;

impl<S> InterceptStore<S, BlockingInterceptor> {
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
        Self::with_interceptor(
            inner,
            BlockingInterceptor {
                predicate: Arc::new(predicate),
                gate: Arc::new(Gate::default()),
            },
        )
    }

    /// Arms a level-triggered gate. Every matching operation parks until release.
    pub fn arm(&self) {
        self.prepare();
        self.interceptor().gate.armed.store(true, Ordering::SeqCst);
    }

    /// Arms a one-shot gate for the next matching operation.
    pub fn block_next(&self) {
        self.prepare();
        self.interceptor()
            .gate
            .block_next
            .store(1, Ordering::SeqCst);
    }

    /// Waits until a selected operation has parked.
    pub async fn wait_until_blocked(&self) {
        let gate = &self.interceptor().gate;
        wait_for_latch(&gate.blocked, &gate.blocked_notify).await;
    }

    /// Releases all parked operations and disarms a level-triggered gate.
    pub fn release(&self) {
        let gate = &self.interceptor().gate;
        gate.armed.store(false, Ordering::SeqCst);
        gate.released.store(true, Ordering::SeqCst);
        gate.release_notify.notify_waiters();
    }

    /// Waits until the most recently blocked operation finishes forwarding.
    pub async fn wait_until_completed(&self) {
        let gate = &self.interceptor().gate;
        wait_for_latch(&gate.completed, &gate.completed_notify).await;
    }

    fn prepare(&self) {
        let gate = &self.interceptor().gate;
        gate.released.store(false, Ordering::SeqCst);
        gate.blocked.store(false, Ordering::SeqCst);
        gate.completed.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl Interceptor for BlockingInterceptor {
    async fn before(&self, context: &OperationContext<'_>) -> Intercept {
        if (self.predicate)(context) && self.gate.park().await {
            Intercept::ContinueWithAfter
        } else {
            Intercept::Continue
        }
    }

    fn after(&self, _context: &OperationContext<'_>, _outcome: &Outcome) {
        self.gate.mark_completed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ObjectStore, PutMode};
    use std::time::Duration;
    use tempfile::tempdir;

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
