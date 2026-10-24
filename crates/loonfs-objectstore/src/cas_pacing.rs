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
/// A write records its start when it begins, not when it plans to. A write
/// that comes sooner waits until one write interval after the latest recorded
/// start, and then checks again, because another write of the same object
/// may have started meanwhile. Create-only puts and every other operation pass
/// through unchanged.
#[derive(Debug)]
pub(crate) struct CasPacedObjectStore {
    inner: SharedObjectStore,
    timer: Arc<dyn MonotonicTimer>,
    /// For each key written by compare-and-swap within the last write
    /// interval, when its latest write began. Each check first drops the keys
    /// whose write interval has passed, so the map lists only the keys written
    /// within the last write interval.
    started_at_ms: Mutex<HashMap<String, u64>>,
}

impl CasPacedObjectStore {
    pub(crate) fn new(inner: SharedObjectStore) -> Self {
        Self::with_timer(inner, Arc::new(StdMonotonicTimer::default()))
    }

    fn with_timer(inner: SharedObjectStore, timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            inner,
            timer,
            started_at_ms: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u64>> {
        self.started_at_ms
            .lock()
            .expect("the write start times lock should not be poisoned")
    }

    /// Records a write of `key` as started now and returns `None`, or returns
    /// how long until the write interval since the key's latest start passes.
    fn try_start(&self, key: &str) -> Option<u64> {
        let now_ms = self.timer.monotonic_now_ms();
        let mut started = self.lock();
        started.retain(|_, at_ms| now_ms < at_ms.saturating_add(MIN_CAS_WRITE_INTERVAL_MS));
        match started.get(key) {
            Some(at_ms) => Some(at_ms.saturating_add(MIN_CAS_WRITE_INTERVAL_MS) - now_ms),
            None => {
                started.insert(key.to_owned(), now_ms);
                None
            }
        }
    }

    /// Runs `write` once the write interval since the latest start of a
    /// write to `key` has passed, and records its start when it begins.
    ///
    /// A waiter that's dropped records nothing. Each pass that can't start
    /// sleeps until the interval since the latest recorded start passes, so a
    /// waiter passes again only when another write of the key started
    /// meanwhile.
    async fn paced<T>(&self, key: &str, write: impl std::future::Future<Output = T>) -> T {
        while let Some(wait_ms) = self.try_start(key) {
            pause_before_write(Duration::from_millis(wait_ms)).await;
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
    use crate::object_store::Result;
    use crate::test_support::TokioTimer;
    use crate::timing::MonotonicTimer;
    use crate::{AssemblySource, ByteRange, ByteStream, ListedObject, ObjectBody, ObjectMetadata};
    use crate::{ObjectStore, ObjectStoreError, PutMode};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::FutureExt;
    use loonfs_types::{Checksum, ChecksumAlgorithm, EffectiveLimit, Page};
    use std::assert_matches;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Forwards to a local store, and records when each compare-and-swap
    /// write that reaches it begins, on the paused test clock.
    #[derive(Debug)]
    struct StartRecordingStore {
        inner: LocalFsStore,
        timer: Arc<TokioTimer>,
        cas_started_at_ms: Mutex<Vec<u64>>,
    }

    impl StartRecordingStore {
        fn cas_started_at_ms(&self) -> Vec<u64> {
            self.cas_started_at_ms.lock().expect("starts").clone()
        }
    }

    #[async_trait]
    impl ObjectStore for StartRecordingStore {
        fn checksum_algorithm(&self) -> ChecksumAlgorithm {
            self.inner.checksum_algorithm()
        }

        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
            self.inner.head(key).await
        }

        async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
            self.inner.get(key, range).await
        }

        async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
            if matches!(mode, PutMode::CompareAndSwap { .. }) {
                let now_ms = self.timer.monotonic_now_ms();
                self.cas_started_at_ms.lock().expect("starts").push(now_ms);
            }
            self.inner.put(key, bytes, mode).await
        }

        async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
            self.inner.put_streamed(key, body, mode).await
        }

        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }

        fn list_entries_from_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> BoxStream<'static, Result<ListedObject>> {
            self.inner.list_entries_from_stream(prefix, start_after)
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

        async fn assemble(
            &self,
            key: &str,
            sources: &[AssemblySource],
            tail: Vec<Bytes>,
            expected: &Checksum,
        ) -> Result<ObjectMetadata> {
            self.inner.assemble(key, sources, tail, expected).await
        }
    }

    /// A paced store over a store that records compare-and-swap starts, and a
    /// timer on the paused test clock that both stores read.
    struct Fixture {
        _directory: tempfile::TempDir,
        store: CasPacedObjectStore,
        recorded: Arc<StartRecordingStore>,
        timer: Arc<TokioTimer>,
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().expect("directory");
        let timer = Arc::new(TokioTimer::start());
        let recorded = Arc::new(StartRecordingStore {
            inner: LocalFsStore::new(directory.path()).expect("store"),
            timer: Arc::clone(&timer),
            cas_started_at_ms: Mutex::new(Vec::new()),
        });
        let store = CasPacedObjectStore::with_timer(recorded.clone(), timer.clone());
        Fixture {
            _directory: directory,
            store,
            recorded,
            timer,
        }
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

    /// Swaps `key` from `etag` and returns the new compare token.
    async fn swapped(store: &CasPacedObjectStore, key: &str, etag: &str) -> String {
        store
            .compare_and_swap(key, etag, Bytes::from_static(b"next"))
            .await
            .expect("swap")
            .etag
            .expect("etag")
    }

    #[tokio::test(start_paused = true)]
    async fn cas_writes_to_one_key_start_at_least_a_write_interval_apart() {
        let fixture = fixture();
        let mut etag = created(&fixture.store, "hint.json").await;
        for _ in 0..3 {
            etag = swapped(&fixture.store, "hint.json", &etag).await;
        }
        assert_eq!(
            fixture.recorded.cas_started_at_ms(),
            [0, MIN_CAS_WRITE_INTERVAL_MS, 2 * MIN_CAS_WRITE_INTERVAL_MS]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cas_writes_to_different_keys_and_create_only_puts_do_not_wait() {
        let fixture = fixture();
        let first = created(&fixture.store, "first.json").await;
        let second = created(&fixture.store, "second.json").await;
        swapped(&fixture.store, "first.json", &first).await;
        swapped(&fixture.store, "second.json", &second).await;
        let error = fixture
            .store
            .put_if_absent("first.json", Bytes::from_static(b"2"))
            .await
            .expect_err("the key exists");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
        assert_eq!(fixture.timer.monotonic_now_ms(), 0, "nothing waited");
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_dropped_while_it_sleeps_adds_no_delay() {
        let fixture = fixture();
        let etag = created(&fixture.store, "hint.json").await;
        let etag = swapped(&fixture.store, "hint.json", &etag).await;
        let waiter =
            fixture
                .store
                .compare_and_swap("hint.json", &etag, Bytes::from_static(b"dropped"));
        assert!(waiter.now_or_never().is_none(), "the waiter sleeps");
        tokio::time::advance(Duration::from_millis(MIN_CAS_WRITE_INTERVAL_MS)).await;
        swapped(&fixture.store, "hint.json", &etag).await;
        assert_eq!(
            fixture.recorded.cas_started_at_ms(),
            [0, MIN_CAS_WRITE_INTERVAL_MS],
            "the dropped waiter sends nothing, and the next write doesn't wait for it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn starts_stay_a_write_interval_apart_when_a_waiter_wakes_late() {
        let fixture = fixture();
        let etag = created(&fixture.store, "hint.json").await;
        let etag = swapped(&fixture.store, "hint.json", &etag).await;
        let mut late = Box::pin(fixture.store.compare_and_swap(
            "hint.json",
            &etag,
            Bytes::from_static(b"late"),
        ));
        assert!(futures::poll!(&mut late).is_pending(), "the waiter sleeps");
        // The waiter isn't polled again until after its wake-up, and another
        // write starts in between.
        tokio::time::advance(Duration::from_millis(MIN_CAS_WRITE_INTERVAL_MS * 3 / 2)).await;
        swapped(&fixture.store, "hint.json", &etag).await;
        // Whether the late write wins its swap doesn't matter here, only when
        // it starts.
        let _ = late.await;
        let started_at_ms = fixture.recorded.cas_started_at_ms();
        assert_eq!(started_at_ms.len(), 3);
        for (index, earlier) in started_at_ms.iter().enumerate() {
            for later in &started_at_ms[index + 1..] {
                assert!(
                    later - earlier >= MIN_CAS_WRITE_INTERVAL_MS,
                    "{started_at_ms:?}"
                );
            }
        }
    }
}
