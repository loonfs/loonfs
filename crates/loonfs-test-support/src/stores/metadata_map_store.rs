//! Metadata transforms for selected object-store operations.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::fmt;
use std::sync::Arc;

type Transform = dyn Fn(ObjectMetadata) -> ObjectMetadata + Send + Sync;

/// Maps returned metadata for selected keys.
pub struct MetadataMapStore<S> {
    inner: S,
    keys: KeyPredicate,
    transform: Arc<Transform>,
}

impl<S> MetadataMapStore<S> {
    /// Applies `transform` to returned metadata for matching keys.
    pub fn new(
        inner: S,
        keys: KeyPredicate,
        transform: impl Fn(ObjectMetadata) -> ObjectMetadata + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            keys,
            transform: Arc::new(transform),
        }
    }

    /// Reports matching objects as last modified at unix epoch zero.
    pub fn aged(inner: S, keys: KeyPredicate) -> Self {
        Self::new(inner, keys, |mut metadata| {
            metadata.last_modified_ms = Some(0);
            metadata
        })
    }

    /// Removes last-modified timestamps from matching metadata.
    pub fn without_last_modified(inner: S, keys: KeyPredicate) -> Self {
        Self::new(inner, keys, |mut metadata| {
            metadata.last_modified_ms = None;
            metadata
        })
    }

    /// Removes etags from matching metadata.
    pub fn without_etag(inner: S, keys: KeyPredicate) -> Self {
        Self::new(inner, keys, |mut metadata| {
            metadata.etag = None;
            metadata
        })
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn map(&self, key: &str, metadata: ObjectMetadata) -> ObjectMetadata {
        if self.keys.matches(key) {
            (self.transform)(metadata)
        } else {
            metadata
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for MetadataMapStore<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataMapStore")
            .field("inner", &self.inner)
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for MetadataMapStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self
            .inner
            .head(key)
            .await?
            .map(|metadata| self.map(key, metadata)))
    }

    /// Passed through unchanged: this store rewrites object metadata, and a
    /// stored checksum is the provider's statement about bytes, not metadata
    /// a test may rewrite without making the object lie about itself.
    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        self.inner.head_stored_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self.inner.get_with_metadata(key).await?.map(|mut body| {
            body.metadata = self.map(key, body.metadata);
            body
        }))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let metadata = self.inner.put(key, bytes, mode).await?;
        Ok(self.map(key, metadata))
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        // Nothing to remap: a streamed write reports a length, not identity.
        self.inner.put_streamed(key, body, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let metadata = self
            .inner
            .compare_and_swap(key, expected_etag, bytes)
            .await?;
        Ok(self.map(key, metadata))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}
