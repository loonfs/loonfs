//! Measures payload buffers held by a transfer path.
//!
//! The wrapper observes buffered puts, streamed chunks, and read results. It
//! records the largest buffer and peak live bytes. Each observed buffer owns a
//! drop guard, so live-byte counts fall when the consumer releases the buffer.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use loonfs_objectstore::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// What one write path was observed to hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPeaks {
    /// Largest single payload buffer handed to the store.
    pub largest_buffer_bytes: u64,
    /// Most payload bytes alive at once across every buffer.
    pub peak_live_bytes: u64,
    /// Most payload buffers alive at once.
    pub peak_live_buffers: usize,
    /// Total payload bytes that crossed the boundary.
    pub total_bytes: u64,
}

#[derive(Debug, Default)]
struct Peaks {
    largest_buffer_bytes: AtomicU64,
    live_bytes: AtomicU64,
    peak_live_bytes: AtomicU64,
    live_buffers: AtomicUsize,
    peak_live_buffers: AtomicUsize,
    total_bytes: AtomicU64,
}

impl Peaks {
    fn observe(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.largest_buffer_bytes.fetch_max(bytes, Ordering::SeqCst);
        let live_bytes = self.live_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak_live_bytes.fetch_max(live_bytes, Ordering::SeqCst);
        let live_buffers = self.live_buffers.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_live_buffers
            .fetch_max(live_buffers, Ordering::SeqCst);
    }

    fn release(&self, bytes: u64) {
        self.live_bytes.fetch_sub(bytes, Ordering::SeqCst);
        self.live_buffers.fetch_sub(1, Ordering::SeqCst);
    }

    fn snapshot(&self) -> BufferPeaks {
        BufferPeaks {
            largest_buffer_bytes: self.largest_buffer_bytes.load(Ordering::SeqCst),
            peak_live_bytes: self.peak_live_bytes.load(Ordering::SeqCst),
            peak_live_buffers: self.peak_live_buffers.load(Ordering::SeqCst),
            total_bytes: self.total_bytes.load(Ordering::SeqCst),
        }
    }
}

/// One watched chunk. Owning the bytes lets `Bytes` report the release.
struct WatchedChunk {
    bytes: Bytes,
    peaks: Arc<Peaks>,
}

impl AsRef<[u8]> for WatchedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Drop for WatchedChunk {
    fn drop(&mut self) {
        self.peaks.release(self.bytes.len() as u64);
    }
}

/// Wraps a store and records the payload buffers it is handed.
#[derive(Debug)]
pub struct BufferWatchStore<S> {
    inner: S,
    keys: KeyPredicate,
    peaks: Arc<Peaks>,
}

impl<S> BufferWatchStore<S> {
    /// Watches payload buffers for keys `keys` selects.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self {
            inner,
            keys,
            peaks: Arc::new(Peaks::default()),
        }
    }

    /// Watches payload buffers for content-blob keys, which is where an
    /// upload's payload goes.
    pub fn watching_content(inner: S) -> Self {
        Self::new(inner, KeyPredicate::content_blob())
    }

    /// What has been observed so far.
    pub fn peaks(&self) -> BufferPeaks {
        self.peaks.snapshot()
    }

    fn matches(&self, key: &str) -> bool {
        self.keys.matches(key)
    }

    /// Replaces each chunk with one whose release is observable.
    fn watch(&self, body: ByteStream) -> ByteStream {
        let peaks = Arc::clone(&self.peaks);
        body.map(move |chunk| {
            chunk.map(|bytes| {
                peaks.observe(bytes.len() as u64);
                Bytes::from_owner(WatchedChunk {
                    bytes,
                    peaks: Arc::clone(&peaks),
                })
            })
        })
        .boxed()
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for BufferWatchStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        self.inner.head_stored_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    /// Tracks the returned read buffer until the caller drops it.
    ///
    /// Unranged reads count the whole object; ranged reads count the returned
    /// range. Retained chunks therefore remain included in peak live memory.
    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let bytes = self.inner.get(key, range).await?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if !self.matches(key) {
            return Ok(Some(bytes));
        }
        self.peaks.observe(bytes.len() as u64);
        Ok(Some(Bytes::from_owner(WatchedChunk {
            bytes,
            peaks: Arc::clone(&self.peaks),
        })))
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if !self.matches(key) {
            return self.inner.put(key, bytes, mode).await;
        }
        // A buffered put holds its whole payload for the length of the
        // call, which is exactly what a streaming path must never do.
        let size_bytes = bytes.len() as u64;
        self.peaks.observe(size_bytes);
        let result = self.inner.put(key, bytes, mode).await;
        self.peaks.release(size_bytes);
        result
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        if !self.matches(key) {
            return self.inner.put_streamed(key, body, mode).await;
        }
        self.inner.put_streamed(key, self.watch(body), mode).await
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
