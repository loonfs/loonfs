//! Metadata transforms for selected object-store operations.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use loonfs_objectstore::{
    AssemblySource, ListedObject, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError,
    PutMode,
};
use loonfs_types::Checksum;
use std::fmt;
use std::sync::Arc;

type Transform = dyn Fn(ObjectMetadata) -> ObjectMetadata + Send + Sync;

/// Maps returned metadata for selected keys.
pub struct MetadataMapStore<S> {
    inner: S,
    keys: KeyPredicate,
    transform: Arc<Transform>,
    algorithm: Option<loonfs_types::ChecksumAlgorithm>,
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
            algorithm: None,
        }
    }

    /// Declares the algorithm supplied by the metadata transform.
    pub fn algorithm(mut self, algorithm: loonfs_types::ChecksumAlgorithm) -> Self {
        self.algorithm = Some(algorithm);
        self
    }

    /// Reports matching objects as last modified at unix epoch zero.
    pub fn aged(inner: S, keys: KeyPredicate) -> Self {
        Self::new(inner, keys, |mut metadata| {
            metadata.last_modified_at_ms = Some(0);
            metadata
        })
    }

    /// Removes last-modified timestamps from matching metadata.
    pub fn without_last_modified(inner: S, keys: KeyPredicate) -> Self {
        Self::new(inner, keys, |mut metadata| {
            metadata.last_modified_at_ms = None;
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
    // A streamed write reports a length, not metadata, so `put_streamed` has
    // nothing to map.
    crate::delegate_object_store!(self => self.inner;
        identity,
        create_multipart_upload,
        complete_multipart_upload,
        abort_multipart_upload,
        get,
        put_streamed,
        delete,
        list_child_prefixes,
    );

    fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
        self.algorithm
            .unwrap_or_else(|| self.inner.checksum_algorithm())
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self
            .inner
            .head(key)
            .await?
            .map(|metadata| self.map(key, metadata)))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self.inner.get_with_metadata(key).await?.map(|mut body| {
            body.metadata = self.map(key, body.metadata);
            body
        }))
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

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        loonfs_objectstore::buffered::assemble(self, key, sources, tail, expected).await
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject, ObjectStoreError>> {
        let keys = self.keys.clone();
        let transform = Arc::clone(&self.transform);
        self.inner
            .list_entries_from_stream(prefix, start_after)
            .map(move |result| {
                result.map(|mut entry| {
                    if keys.matches(&entry.key) {
                        entry.last_modified_at_ms = transform(ObjectMetadata {
                            last_modified_at_ms: entry.last_modified_at_ms,
                            etag: None,
                            version: None,
                            size_bytes: entry.size_bytes,
                            checksum: None,
                        })
                        .last_modified_at_ms;
                    }
                    entry
                })
            })
            .boxed()
    }
}
