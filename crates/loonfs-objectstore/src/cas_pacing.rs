//! [`CasPacedObjectStore`]: a store wrapper that paces compare-and-swap
//! writes to each object.

use crate::object_store::{Result, SharedObjectStore};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{
    AssemblySource, ByteRange, ByteStream, ListedObject, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, PutMode, StoreIdentity,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_types::{Checksum, ChecksumAlgorithm, EffectiveLimit, Page};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The write interval, the shortest time between the starts of two
/// compare-and-swap writes to one object.
///
/// Some providers limit writes to the same object to one per second (see
/// `docs/specs/object-storage-providers.md`). The namespace hint is the object
/// that LoonFS updates most often by compare-and-swap.
pub const MIN_CAS_WRITE_INTERVAL_MS: u64 = 1_000;

/// A store wrapper that forwards every call to the store it wraps, and starts
/// compare-and-swap writes to one object at least one write interval
/// ([`MIN_CAS_WRITE_INTERVAL_MS`]) apart.
///
/// A write that comes sooner waits until one write interval after the
/// previous write's start, and then proceeds. Create-only puts and every other
/// operation pass through unchanged.
#[derive(Debug)]
pub(crate) struct CasPacedObjectStore {
    inner: SharedObjectStore,
    timer: Arc<dyn MonotonicTimer>,
    /// For each key written by compare-and-swap within the last write
    /// interval, when its latest write started or is scheduled to start. Each
    /// compare-and-swap write first drops the keys whose write interval has
    /// passed, so the map lists only the keys written within the last write
    /// interval.
    written_at_ms: Mutex<HashMap<String, u64>>,
}

impl CasPacedObjectStore {
    pub(crate) fn new(inner: SharedObjectStore) -> Self {
        Self::with_timer(inner, Arc::new(StdMonotonicTimer::default()))
    }

    fn with_timer(inner: SharedObjectStore, timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            inner,
            timer,
            written_at_ms: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u64>> {
        self.written_at_ms
            .lock()
            .expect("the write times lock should not be poisoned")
    }

    /// Reserves the earliest start time that the write interval allows for
    /// `key`, waits until then, and runs `write`.
    async fn paced<T>(&self, key: &str, write: impl std::future::Future<Output = T>) -> T {
        let now_ms = self.timer.monotonic_now_ms();
        let start_at_ms = {
            let mut written = self.lock();
            written.retain(|_, at_ms| now_ms < at_ms.saturating_add(MIN_CAS_WRITE_INTERVAL_MS));
            let start_at_ms = written.get(key).map_or(now_ms, |at_ms| {
                now_ms.max(at_ms.saturating_add(MIN_CAS_WRITE_INTERVAL_MS))
            });
            written.insert(key.to_owned(), start_at_ms);
            start_at_ms
        };
        if start_at_ms > now_ms {
            pause_before_write(Duration::from_millis(start_at_ms - now_ms)).await;
        }
        write.await
    }
}

// The pause changes when a write is sent, not what it writes, so no durable
// state depends on the clock.
#[allow(
    clippy::disallowed_methods,
    reason = "a compare-and-swap write waits out its object's write interval"
)]
async fn pause_before_write(wait: Duration) {
    tokio::time::sleep(wait).await;
}

#[async_trait]
impl ObjectStore for CasPacedObjectStore {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        self.inner.checksum_algorithm()
    }

    fn identity(&self) -> Option<StoreIdentity> {
        self.inner.identity()
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.inner.head(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        self.inner.create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        self.inner
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        self.inner
            .abort_multipart_upload(key, provider_upload_id)
            .await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.inner.get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        match mode {
            PutMode::CreateIfAbsent => self.inner.put(key, bytes, mode).await,
            PutMode::CompareAndSwap { .. } => {
                self.paced(key, self.inner.put(key, bytes, mode)).await
            }
        }
    }

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        match mode {
            PutMode::CreateIfAbsent => self.inner.put_streamed(key, body, mode).await,
            PutMode::CompareAndSwap { .. } => {
                self.paced(key, self.inner.put_streamed(key, body, mode))
                    .await
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        self.inner.list_prefix_stream(prefix)
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject>> {
        self.inner.list_entries_from_stream(prefix, start_after)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: EffectiveLimit,
    ) -> Result<Page<String, String>> {
        self.inner
            .list_child_prefixes(prefix, start_after, limit)
            .await
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.inner.put_if_absent(key, bytes).await
    }

    async fn put_immutable_verified(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.inner.put_immutable_verified(key, bytes).await
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        self.inner.assemble(key, sources, tail, expected).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata> {
        self.paced(key, self.inner.compare_and_swap(key, expected_etag, bytes))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{CasPacedObjectStore, MIN_CAS_WRITE_INTERVAL_MS};
    use crate::local_fs_store::LocalFsStore;
    use crate::test_support::TokioTimer;
    use crate::timing::MonotonicTimer;
    use crate::{ObjectStore, ObjectStoreError};
    use bytes::Bytes;
    use std::assert_matches;
    use std::sync::Arc;

    /// Returns a paced local store, and a timer on the paused test clock
    /// that the store reads too.
    fn paced_store() -> (tempfile::TempDir, CasPacedObjectStore, Arc<TokioTimer>) {
        let directory = tempfile::tempdir().expect("directory");
        let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let timer = Arc::new(TokioTimer::start());
        let store = CasPacedObjectStore::with_timer(inner, timer.clone());
        (directory, store, timer)
    }

    /// Creates `key` and returns its compare token.
    async fn created(store: &CasPacedObjectStore, key: &str) -> String {
        store
            .put_if_absent(key, Bytes::from_static(b"0"))
            .await
            .expect("create")
            .etag
            .expect("etag")
    }

    #[tokio::test(start_paused = true)]
    async fn cas_writes_to_one_key_start_at_least_a_write_interval_apart() {
        let (_directory, store, timer) = paced_store();
        let mut etag = created(&store, "hint.json").await;
        // A local write takes no time on the paused clock, so the time after
        // each write is also when that write started.
        let mut started_at_ms = Vec::new();
        for body in [b"1", b"2", b"3"] {
            etag = store
                .compare_and_swap("hint.json", &etag, Bytes::from_static(body))
                .await
                .expect("swap")
                .etag
                .expect("etag");
            started_at_ms.push(timer.monotonic_now_ms());
        }
        assert_eq!(started_at_ms[0], 0, "the first write doesn't wait");
        for pair in started_at_ms.windows(2) {
            assert!(pair[1] - pair[0] >= MIN_CAS_WRITE_INTERVAL_MS, "{pair:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cas_writes_to_different_keys_and_create_only_puts_do_not_wait() {
        let (_directory, store, timer) = paced_store();
        let first = created(&store, "first.json").await;
        let second = created(&store, "second.json").await;
        for (key, etag) in [("first.json", first), ("second.json", second)] {
            store
                .compare_and_swap(key, &etag, Bytes::from_static(b"1"))
                .await
                .expect("swap");
        }
        let error = store
            .put_if_absent("first.json", Bytes::from_static(b"2"))
            .await
            .expect_err("the key exists");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
        assert_eq!(timer.monotonic_now_ms(), 0, "nothing waited");
    }
}
