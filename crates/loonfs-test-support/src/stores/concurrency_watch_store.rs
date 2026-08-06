//! Watches how many reads a path keeps in flight at once.
//!
//! A batched fetch's whole promise is that its round trips overlap, and the
//! only place that is visible is the store boundary: request counts and
//! call order both look identical whether a path fetched its objects one at
//! a time or all at once. This wrapper counts matching reads as they enter
//! and leave, and records the high-water mark, so a test can pin the width
//! of a fetch wave rather than only its size.
//!
//! Each watched read yields once before it delegates. That is what makes the
//! mark a measurement rather than a race: a batch polled onto the same task
//! has all of its members inside the wrapper before the first of them can
//! finish, whatever the wrapped store does.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// What one read path was observed to overlap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadConcurrency {
    /// Most matching reads in flight at any instant.
    pub peak_in_flight: usize,
    /// Matching reads issued in total.
    pub total: usize,
}

#[derive(Debug, Default)]
struct InFlight {
    current: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
}

/// Decrements on drop, so a cancelled or failed read leaves no phantom.
struct InFlightGuard<'a>(&'a InFlight);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Wraps a store and records the reads it is asked for concurrently.
#[derive(Debug)]
pub struct ConcurrencyWatchStore<S> {
    inner: S,
    keys: KeyPredicate,
    reads: InFlight,
}

impl<S> ConcurrencyWatchStore<S> {
    /// Watches reads of keys `keys` selects.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self {
            inner,
            keys,
            reads: InFlight::default(),
        }
    }

    /// What has been observed so far.
    pub fn reads(&self) -> ReadConcurrency {
        ReadConcurrency {
            peak_in_flight: self.reads.peak.load(Ordering::SeqCst),
            total: self.reads.total.load(Ordering::SeqCst),
        }
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Counts one read in, and holds the count up until the guard drops.
    async fn enter(&self, key: &str) -> Option<InFlightGuard<'_>> {
        if !self.keys.matches(key) {
            return None;
        }
        self.reads.total.fetch_add(1, Ordering::SeqCst);
        let current = self.reads.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.reads.peak.fetch_max(current, Ordering::SeqCst);
        let guard = InFlightGuard(&self.reads);
        tokio::task::yield_now().await;
        Some(guard)
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for ConcurrencyWatchStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let _in_flight = self.enter(key).await;
        self.inner.head(key).await
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        let _in_flight = self.enter(key).await;
        self.inner.head_stored_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let _in_flight = self.enter(key).await;
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let _in_flight = self.enter(key).await;
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        self.inner.put_streamed(key, body, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}
