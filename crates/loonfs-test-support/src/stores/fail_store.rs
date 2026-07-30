//! Configurable failures for selected object-store operations.

use super::{KeyPredicate, OperationClass, OperationContext, OperationKind};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;

/// Error returned by a [`FailStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InjectedError {
    /// A provider precondition failure.
    PreconditionFailed,
    /// A transport failure carrying the supplied message.
    Transport(String),
}

impl InjectedError {
    fn for_key(&self, key: &str) -> ObjectStoreError {
        match self {
            Self::PreconditionFailed => ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
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

/// Injects a configured error into selected operations.
pub struct FailStore<S> {
    inner: S,
    predicate: Arc<Predicate>,
    error: InjectedError,
    mode: FailureMode,
    remaining: AtomicUsize,
    fail_all: AtomicBool,
    attempts: AtomicUsize,
}

impl<S> FailStore<S> {
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
        Self {
            inner,
            predicate: Arc::new(predicate),
            error,
            mode: FailureMode::BeforeApply,
            remaining: AtomicUsize::new(0),
            fail_all: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
        }
    }

    /// Returns failures after selected operations have been applied.
    pub fn apply_then_fail(mut self) -> Self {
        self.mode = FailureMode::ApplyThenFail;
        self
    }

    /// Fails the next `count` matching operations and resets the attempt count.
    pub fn fail_next(&self, count: usize) {
        self.attempts.store(0, Ordering::SeqCst);
        self.fail_all.store(false, Ordering::SeqCst);
        self.remaining.store(count, Ordering::SeqCst);
    }

    /// Fails every matching operation until [`Self::clear`] is called.
    pub fn fail_all(&self) {
        self.attempts.store(0, Ordering::SeqCst);
        self.remaining.store(0, Ordering::SeqCst);
        self.fail_all.store(true, Ordering::SeqCst);
    }

    /// Stops injecting failures.
    pub fn clear(&self) {
        self.fail_all.store(false, Ordering::SeqCst);
        self.remaining.store(0, Ordering::SeqCst);
    }

    /// Returns the number of matching attempts since the most recent arming.
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Returns the number of failures still armed.
    pub fn remaining(&self) -> usize {
        self.remaining.load(Ordering::SeqCst)
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn should_fail(&self, context: &OperationContext<'_>) -> bool {
        if !(self.predicate)(context) {
            return false;
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.fail_all.load(Ordering::SeqCst)
            || self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
    }
}

impl<S: fmt::Debug> fmt::Debug for FailStore<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailStore")
            .field("inner", &self.inner)
            .field("error", &self.error)
            .field("mode", &self.mode)
            .field("remaining", &self.remaining)
            .field("fail_all", &self.fail_all)
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for FailStore<S> {
    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(key, OperationKind::Head));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.head_stored_checksum(key).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(key, OperationKind::Head));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.head(key).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(key, OperationKind::GetWithMetadata));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.get_with_metadata(key).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(
            key,
            OperationKind::Get {
                range: range.as_ref(),
            },
        ));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.get(key, range).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(
            key,
            OperationKind::Put {
                bytes: &bytes,
                mode: &mode,
            },
        ));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.put(key, bytes, mode).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(
            key,
            OperationKind::CompareAndSwap {
                expected_etag,
                bytes: &bytes,
            },
        ));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.compare_and_swap(key, expected_etag, bytes).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let fail = self.should_fail(&OperationContext::new(key, OperationKind::Delete));
        if fail && self.mode == FailureMode::BeforeApply {
            return Err(self.error.for_key(key));
        }
        let result = self.inner.delete(key).await;
        if fail {
            result?;
            Err(self.error.for_key(key))
        } else {
            result
        }
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        if self.should_fail(&OperationContext::new(prefix, OperationKind::List)) {
            return Box::pin(stream::once({
                let error = self.error.for_key(prefix);
                async move { Err(error) }
            }));
        }
        self.inner.list_prefix_stream(prefix)
    }
}
