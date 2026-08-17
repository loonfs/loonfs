//! Per-operation request and byte counters for selected keys.

use super::{KeyPredicate, OperationClass};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Snapshot of request counts and transferred bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreCounts {
    /// Metadata-only read calls.
    pub heads: usize,
    /// Byte `get` calls.
    pub gets: usize,
    /// Full-object `get_with_metadata` calls.
    pub gets_with_metadata: usize,
    /// All put calls, including CAS-mode puts.
    pub puts: usize,
    /// Overwrite puts.
    pub overwrite_puts: usize,
    /// Create-if-absent puts.
    pub create_if_absent_puts: usize,
    /// Compare-and-swap calls and CAS-mode puts.
    pub compare_and_swaps: usize,
    /// Delete calls.
    pub deletes: usize,
    /// Prefix-list calls.
    pub lists: usize,
    /// Bytes returned by successful byte reads.
    pub read_bytes: u64,
    /// Bytes supplied to put calls.
    pub written_bytes: u64,
}

impl StoreCounts {
    /// Returns the request count for `operation`.
    pub fn operations(self, operation: OperationClass) -> usize {
        match operation {
            OperationClass::Any => {
                self.heads
                    + self.gets
                    + self.gets_with_metadata
                    + self.puts
                    + self.deletes
                    + self.lists
            }
            OperationClass::Head => self.heads,
            OperationClass::Get => self.gets,
            OperationClass::GetWithMetadata => self.gets_with_metadata,
            OperationClass::Read => self.gets + self.gets_with_metadata,
            OperationClass::Put => self.puts,
            OperationClass::PutOverwrite => self.overwrite_puts,
            OperationClass::PutCreateIfAbsent => self.create_if_absent_puts,
            OperationClass::CompareAndSwap => self.compare_and_swaps,
            OperationClass::Delete => self.deletes,
            OperationClass::List => self.lists,
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    heads: AtomicUsize,
    gets: AtomicUsize,
    gets_with_metadata: AtomicUsize,
    puts: AtomicUsize,
    overwrite_puts: AtomicUsize,
    create_if_absent_puts: AtomicUsize,
    compare_and_swaps: AtomicUsize,
    deletes: AtomicUsize,
    lists: AtomicUsize,
    read_bytes: AtomicU64,
    written_bytes: AtomicU64,
}

/// Counts operations on selected object keys.
#[derive(Debug)]
pub struct CountingStore<S> {
    inner: S,
    keys: KeyPredicate,
    counters: Counters,
}

impl<S> CountingStore<S> {
    /// Counts every operation whose key matches `keys`.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self {
            inner,
            keys,
            counters: Counters::default(),
        }
    }

    /// Counts operations on metadata tables.
    pub fn metadata_tables(inner: S) -> Self {
        Self::new(inner, KeyPredicate::metadata_table())
    }

    /// Returns the current counters.
    pub fn snapshot(&self) -> StoreCounts {
        StoreCounts {
            heads: self.counters.heads.load(Ordering::SeqCst),
            gets: self.counters.gets.load(Ordering::SeqCst),
            gets_with_metadata: self.counters.gets_with_metadata.load(Ordering::SeqCst),
            puts: self.counters.puts.load(Ordering::SeqCst),
            overwrite_puts: self.counters.overwrite_puts.load(Ordering::SeqCst),
            create_if_absent_puts: self.counters.create_if_absent_puts.load(Ordering::SeqCst),
            compare_and_swaps: self.counters.compare_and_swaps.load(Ordering::SeqCst),
            deletes: self.counters.deletes.load(Ordering::SeqCst),
            lists: self.counters.lists.load(Ordering::SeqCst),
            read_bytes: self.counters.read_bytes.load(Ordering::SeqCst),
            written_bytes: self.counters.written_bytes.load(Ordering::SeqCst),
        }
    }

    /// Returns the current request count for `operation`.
    pub fn count(&self, operation: OperationClass) -> usize {
        self.snapshot().operations(operation)
    }

    /// Resets every counter.
    pub fn reset(&self) {
        self.counters.heads.store(0, Ordering::SeqCst);
        self.counters.gets.store(0, Ordering::SeqCst);
        self.counters.gets_with_metadata.store(0, Ordering::SeqCst);
        self.counters.puts.store(0, Ordering::SeqCst);
        self.counters.overwrite_puts.store(0, Ordering::SeqCst);
        self.counters
            .create_if_absent_puts
            .store(0, Ordering::SeqCst);
        self.counters.compare_and_swaps.store(0, Ordering::SeqCst);
        self.counters.deletes.store(0, Ordering::SeqCst);
        self.counters.lists.store(0, Ordering::SeqCst);
        self.counters.read_bytes.store(0, Ordering::SeqCst);
        self.counters.written_bytes.store(0, Ordering::SeqCst);
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn matches(&self, key: &str) -> bool {
        self.keys.matches(key)
    }

    fn record_read_bytes(&self, bytes: usize) {
        self.counters
            .read_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::SeqCst);
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for CountingStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        if self.matches(key) {
            self.counters.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.head(key).await
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        if self.matches(key) {
            self.counters.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.head_stored_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let matches = self.matches(key);
        if matches {
            self.counters
                .gets_with_metadata
                .fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.get_with_metadata(key).await;
        if matches {
            if let Ok(Some(body)) = &result {
                self.record_read_bytes(body.bytes.len());
            }
        }
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let matches = self.matches(key);
        if matches {
            self.counters.gets.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.get(key, range).await;
        if matches {
            if let Ok(Some(bytes)) = &result {
                self.record_read_bytes(bytes.len());
            }
        }
        result
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if self.matches(key) {
            self.counters.puts.fetch_add(1, Ordering::SeqCst);
            self.counters.written_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            match mode {
                PutMode::Overwrite => {
                    self.counters.overwrite_puts.fetch_add(1, Ordering::SeqCst);
                }
                PutMode::CreateIfAbsent => {
                    self.counters
                        .create_if_absent_puts
                        .fetch_add(1, Ordering::SeqCst);
                }
                PutMode::CompareAndSwap { .. } => {
                    self.counters
                        .compare_and_swaps
                        .fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        let result = self.inner.put_streamed(key, body, mode.clone()).await;
        if self.matches(key) {
            self.counters.puts.fetch_add(1, Ordering::SeqCst);
            if let Ok(bytes) = &result {
                self.counters
                    .written_bytes
                    .fetch_add(*bytes, Ordering::SeqCst);
            }
            match mode {
                PutMode::Overwrite => {
                    self.counters.overwrite_puts.fetch_add(1, Ordering::SeqCst);
                }
                PutMode::CreateIfAbsent => {
                    self.counters
                        .create_if_absent_puts
                        .fetch_add(1, Ordering::SeqCst);
                }
                PutMode::CompareAndSwap { .. } => {
                    self.counters
                        .compare_and_swaps
                        .fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        result
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if self.matches(key) {
            self.counters.puts.fetch_add(1, Ordering::SeqCst);
            self.counters
                .compare_and_swaps
                .fetch_add(1, Ordering::SeqCst);
            self.counters.written_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        if self.matches(key) {
            self.counters.deletes.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        if self.matches(prefix) {
            self.counters.lists.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}
