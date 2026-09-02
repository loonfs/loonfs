//! Configurable failures for selected object-store operations.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationClass, OperationContext,
    OperationKind,
};
use async_trait::async_trait;
use futures::future::BoxFuture;
use loonfs_objectstore::ObjectStoreError;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;
type BeforeOperation = dyn for<'a> Fn(&'a OperationContext<'a>) -> BoxFuture<'a, ()> + Send + Sync;

/// Error returned by a [`FailStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InjectedError {
    /// A provider precondition failure.
    PreconditionFailed,
    /// A provider authorization failure carrying the supplied message.
    PermissionDenied(String),
    /// A transport failure carrying the supplied message.
    Transport(String),
}

impl InjectedError {
    fn for_key(&self, key: &str) -> ObjectStoreError {
        match self {
            Self::PreconditionFailed => ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            },
            Self::PermissionDenied(message) => ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message: message.clone(),
            },
            Self::Transport(message) => ObjectStoreError::transport(key, message),
        }
    }
}

/// Whether a selected write is applied before its injected failure is returned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FailureMode {
    /// Return the failure without forwarding the operation.
    #[default]
    BeforeApply,
    /// Forward successfully, then return the configured failure.
    ApplyThenFail,
}

/// Intercepts selected operations with callbacks and configured failures.
pub struct FailInterceptor {
    predicate: Arc<Predicate>,
    before_operation: Option<Arc<BeforeOperation>>,
    before_operation_pending: AtomicBool,
    error: InjectedError,
    mode: FailureMode,
    remaining: AtomicUsize,
    fail_all: AtomicBool,
    attempts: AtomicUsize,
}

impl fmt::Debug for FailInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailInterceptor")
            .field("error", &self.error)
            .field("mode", &self.mode)
            .field("remaining", &self.remaining)
            .field("fail_all", &self.fail_all)
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

/// Injects a configured error into selected operations.
pub type FailStore<S> = InterceptStore<S, FailInterceptor>;

impl<S> InterceptStore<S, FailInterceptor> {
    /// Selects operations by key predicate and operation class.
    pub fn new(
        inner: S,
        keys: KeyPredicate,
        operation: OperationClass,
        error: InjectedError,
    ) -> Self {
        Self::matching(
            inner,
            move |context| keys.matches(context.key()) && operation.matches(context.kind()),
            error,
        )
    }

    /// Selects operations with an arbitrary operation predicate.
    pub fn matching(
        inner: S,
        predicate: impl for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync + 'static,
        error: InjectedError,
    ) -> Self {
        Self::with_interceptor(
            inner,
            FailInterceptor {
                predicate: Arc::new(predicate),
                before_operation: None,
                before_operation_pending: AtomicBool::new(false),
                error,
                mode: FailureMode::BeforeApply,
                remaining: AtomicUsize::new(0),
                fail_all: AtomicBool::new(false),
                attempts: AtomicUsize::new(0),
            },
        )
    }

    /// Runs `callback` before the first matching operation.
    pub fn before_operation(
        mut self,
        callback: impl for<'a> Fn(&'a OperationContext<'a>) -> BoxFuture<'a, ()> + Send + Sync + 'static,
    ) -> Self {
        let interceptor = Arc::get_mut(&mut self.interceptor)
            .expect("new fail interceptor should have one owner");
        interceptor.before_operation = Some(Arc::new(callback));
        interceptor
            .before_operation_pending
            .store(true, Ordering::SeqCst);
        self
    }

    /// Returns failures after selected operations have been applied.
    pub fn apply_then_fail(mut self) -> Self {
        Arc::get_mut(&mut self.interceptor)
            .expect("new fail interceptor should have one owner")
            .mode = FailureMode::ApplyThenFail;
        self
    }

    /// Fails the next `count` matching operations and resets the attempt count.
    pub fn fail_next(&self, count: usize) {
        let interceptor = self.interceptor();
        interceptor.attempts.store(0, Ordering::SeqCst);
        interceptor.fail_all.store(false, Ordering::SeqCst);
        interceptor.remaining.store(count, Ordering::SeqCst);
    }

    /// Fails every matching operation until [`Self::clear`] is called.
    pub fn fail_all(&self) {
        let interceptor = self.interceptor();
        interceptor.attempts.store(0, Ordering::SeqCst);
        interceptor.remaining.store(0, Ordering::SeqCst);
        interceptor.fail_all.store(true, Ordering::SeqCst);
    }

    /// Stops injecting failures.
    pub fn clear(&self) {
        self.interceptor().fail_all.store(false, Ordering::SeqCst);
        self.interceptor().remaining.store(0, Ordering::SeqCst);
    }

    /// Returns the number of matching attempts since the most recent arming.
    pub fn attempts(&self) -> usize {
        self.interceptor().attempts.load(Ordering::SeqCst)
    }

    /// Returns the number of failures still armed.
    pub fn remaining(&self) -> usize {
        self.interceptor().remaining.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Interceptor for FailInterceptor {
    async fn before(&self, context: &OperationContext<'_>) -> Intercept {
        if !(self.predicate)(context) {
            return Intercept::Continue;
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.before_operation_pending.swap(false, Ordering::SeqCst) {
            if let Some(callback) = &self.before_operation {
                callback(context).await;
            }
        }
        let fail = self.fail_all.load(Ordering::SeqCst)
            || self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
        if !fail {
            return Intercept::Continue;
        }
        let error = self.error.for_key(context.key());
        if self.mode == FailureMode::ApplyThenFail && !matches!(context.kind(), OperationKind::List)
        {
            Intercept::FailAfter(error)
        } else {
            Intercept::FailBefore(error)
        }
    }
}
