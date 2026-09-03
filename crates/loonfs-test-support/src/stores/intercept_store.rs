//! Shared forwarding for object-store wrappers that observe operations.

use super::{OperationContext, OperationKind};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loonfs_api::Checksum;
use loonfs_objectstore::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, PutMode, StoredObjectChecksum,
};
use std::fmt::Debug;
use std::sync::Arc;

/// The result details exposed to an interceptor after an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The operation succeeded without a byte count.
    Success,
    /// A buffered read returned this many bytes.
    Bytes(usize),
    /// A streamed write stored this many bytes.
    StreamedBytes(u64),
    /// The operation failed.
    Failure,
}

impl Outcome {
    pub(crate) fn bytes(self) -> Option<usize> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Success | Self::StreamedBytes(_) | Self::Failure => None,
        }
    }

    pub(crate) fn streamed_bytes(self) -> Option<u64> {
        match self {
            Self::StreamedBytes(bytes) => Some(bytes),
            Self::Success | Self::Bytes(_) | Self::Failure => None,
        }
    }
}

/// A decision made before an intercepted operation is forwarded.
#[derive(Debug)]
pub enum Intercept {
    /// Forwards the operation without an after hook.
    Continue,
    /// Forwards the operation and calls the after hook.
    ContinueWithAfter,
    /// Returns the error without forwarding the operation.
    FailBefore(ObjectStoreError),
    /// Returns the error after a successful forwarded operation.
    FailAfter(ObjectStoreError),
}

impl Intercept {
    fn calls_after(&self) -> bool {
        matches!(self, Self::ContinueWithAfter)
    }
}

/// Observes or changes the disposition of object-store operations.
#[async_trait]
pub trait Interceptor: Send + Sync + Debug {
    /// Runs before the wrapped operation.
    async fn before(&self, context: &OperationContext<'_>) -> Intercept;

    /// Runs after the wrapped operation.
    fn after(&self, _context: &OperationContext<'_>, _outcome: &Outcome) {}
}

/// Applies one interceptor while forwarding every object-store operation.
#[derive(Debug)]
pub struct InterceptStore<S, I> {
    inner: Arc<S>,
    pub(super) interceptor: Arc<I>,
}

impl<S, I: Interceptor> InterceptStore<S, I> {
    /// Wraps `inner` with `interceptor`.
    pub fn with_interceptor(inner: S, interceptor: I) -> Self {
        Self {
            inner: Arc::new(inner),
            interceptor: Arc::new(interceptor),
        }
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub(crate) fn interceptor(&self) -> &I {
        &self.interceptor
    }

    fn finish<T>(
        interceptor: &I,
        context: &OperationContext<'_>,
        intercept: Intercept,
        result: Result<T, ObjectStoreError>,
        outcome: Outcome,
    ) -> Result<T, ObjectStoreError> {
        if intercept.calls_after() {
            interceptor.after(context, &outcome);
        }
        match (intercept, result) {
            (Intercept::FailAfter(error), Ok(_)) => Err(error),
            (_, result) => result,
        }
    }
}

#[async_trait]
impl<S: ObjectStore + 'static, I: Interceptor + 'static> ObjectStore for InterceptStore<S, I> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let context = OperationContext::new(key, OperationKind::Head);
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.head(key).await;
        let outcome = result_outcome(&result);
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        let context = OperationContext::new(key, OperationKind::Head);
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.head_stored_checksum(key).await;
        let outcome = result_outcome(&result);
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String, ObjectStoreError> {
        self.inner.create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion, ObjectStoreError> {
        self.inner
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
    ) -> Result<(), ObjectStoreError> {
        self.inner
            .abort_multipart_upload(key, provider_upload_id)
            .await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let context = OperationContext::new(key, OperationKind::GetWithMetadata);
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.get_with_metadata(key).await;
        let outcome = match &result {
            Ok(Some(body)) => Outcome::Bytes(body.bytes.len()),
            Ok(None) => Outcome::Bytes(0),
            Err(_) => Outcome::Failure,
        };
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn get_range_with_metadata(
        &self,
        key: &str,
        range: ByteRange,
    ) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let context = OperationContext::new(
            key,
            OperationKind::Get {
                range: Some(&range),
            },
        );
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.get_range_with_metadata(key, range.clone()).await;
        let outcome = match &result {
            Ok(Some(body)) => Outcome::Bytes(body.bytes.len()),
            Ok(None) => Outcome::Bytes(0),
            Err(_) => Outcome::Failure,
        };
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let context = OperationContext::new(
            key,
            OperationKind::Get {
                range: range.as_ref(),
            },
        );
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.get(key, range.clone()).await;
        let outcome = match &result {
            Ok(Some(bytes)) => Outcome::Bytes(bytes.len()),
            Ok(None) => Outcome::Bytes(0),
            Err(_) => Outcome::Failure,
        };
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let context = OperationContext::new(
            key,
            OperationKind::Put {
                bytes: &bytes,
                mode: &mode,
            },
        );
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.put(key, bytes.clone(), mode.clone()).await;
        let outcome = result_outcome(&result);
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        let context = OperationContext::new(key, OperationKind::PutStreamed { mode: &mode });
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.put_streamed(key, body, mode.clone()).await;
        let outcome = match &result {
            Ok(bytes) => Outcome::StreamedBytes(*bytes),
            Err(_) => Outcome::Failure,
        };
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let context = OperationContext::new(
            key,
            OperationKind::CompareAndSwap {
                expected_etag,
                bytes: &bytes,
            },
        );
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self
            .inner
            .compare_and_swap(key, expected_etag, bytes.clone())
            .await;
        let outcome = result_outcome(&result);
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let context = OperationContext::new(key, OperationKind::Delete);
        let intercept = match self.interceptor.before(&context).await {
            Intercept::FailBefore(error) => return Err(error),
            intercept => intercept,
        };
        let result = self.inner.delete(key).await;
        let outcome = result_outcome(&result);
        Self::finish(&self.interceptor, &context, intercept, result, outcome)
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        let inner = Arc::clone(&self.inner);
        let interceptor = Arc::clone(&self.interceptor);
        Box::pin(
            stream::once(async move {
                let context = OperationContext::new(&prefix, OperationKind::List);
                let intercept = interceptor.before(&context).await;
                let calls_after = intercept.calls_after();
                match intercept {
                    Intercept::FailBefore(error) | Intercept::FailAfter(error) => Err(error),
                    Intercept::Continue | Intercept::ContinueWithAfter => {
                        let stream = inner.list_prefix_from_stream(&prefix, start_after.as_deref());
                        if calls_after {
                            interceptor.after(&context, &Outcome::Success);
                        }
                        Ok(stream)
                    }
                }
            })
            .map(|result| match result {
                Ok(stream) => stream,
                Err(error) => Box::pin(stream::once(async move { Err(error) }))
                    as BoxStream<'static, Result<String, ObjectStoreError>>,
            })
            .flatten(),
        )
    }
}

fn result_outcome<T>(result: &Result<T, ObjectStoreError>) -> Outcome {
    if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    }
}
