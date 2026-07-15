//! Commit-window cancellation semantics: a cancelled opener must reject
//! members explicitly while the window is still buffering, must surface an
//! unknown outcome once the flush is in flight (the batch may have durably
//! committed), and a cancelled joiner must not cancel admitted work.

#![allow(clippy::panic, clippy::disallowed_methods)]
// Cancellation tests drive real window timers to real await points, so they
// poll with short sleeps and panic on missed signals for readable failures.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, ErrorCode, FsWriter, NamespaceId, PutFileOptions, RuntimeError,
    SharedObjectStore,
};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{sleep, timeout, Duration};

/// Object store for driving a window flush to a precise point: it can apply
/// a head compare-and-swap durably and then never return — the shape of a
/// request that landed while the future awaiting it gets cancelled — and it
/// counts writes so tests can prove what did or did not reach the store.
#[derive(Debug)]
struct WindowTestStore {
    inner: LocalFsStore,
    head_key: String,
    hang_head_cas: AtomicBool,
    head_cas_hang_reached: AtomicBool,
    head_cas_writes: AtomicUsize,
    other_puts: AtomicUsize,
}

impl WindowTestStore {
    fn new(root: &Path, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            head_key: wal_head(namespace_id.as_str()),
            hang_head_cas: AtomicBool::new(false),
            head_cas_hang_reached: AtomicBool::new(false),
            head_cas_writes: AtomicUsize::new(0),
            other_puts: AtomicUsize::new(0),
        }
    }

    fn reset_counters(&self) {
        self.head_cas_writes.store(0, Ordering::SeqCst);
        self.other_puts.store(0, Ordering::SeqCst);
    }

    fn head_cas_writes(&self) -> usize {
        self.head_cas_writes.load(Ordering::SeqCst)
    }

    fn other_puts(&self) -> usize {
        self.other_puts.load(Ordering::SeqCst)
    }

    fn head_cas_hang_reached(&self) -> bool {
        self.head_cas_hang_reached.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for WindowTestStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
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
        let is_head_cas = key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. });
        if is_head_cas {
            self.head_cas_writes.fetch_add(1, Ordering::SeqCst);
        } else {
            self.other_puts.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.put(key, bytes, mode).await;
        if is_head_cas && self.hang_head_cas.load(Ordering::SeqCst) && result.is_ok() {
            // The CAS landed durably; the response never arrives.
            self.head_cas_hang_reached.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        result
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

async fn writer_with_window(store: &SharedObjectStore, window_ms: u64) -> FsWriter {
    FsWriter::builder_with_store(store.clone())
        .writer_id("window-writer")
        .commit_window_ms(window_ms)
        .build()
        .await
        .expect("build writer")
}

/// Polls a condition every few milliseconds; panics after ten seconds so a
/// missed signal fails the test instead of hanging it.
async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..2_000 {
        if condition() {
            return;
        }
        sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

fn core_code(error: &RuntimeError) -> Option<ErrorCode> {
    match error {
        RuntimeError::Core(core) => Some(core.code()),
        _ => None,
    }
}

/// Opener cancelled while the head compare-and-swap is in flight: the CAS
/// landed durably, so the joiner must be told the outcome is unknown — a
/// definite failure here would deny a commit that is visible to every
/// reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_opener_mid_flush_reports_unknown_outcome_and_the_commit_lands() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("midflush").expect("valid namespace id");
    let test_store = Arc::new(WindowTestStore::new(temp_dir.path(), &namespace_id));
    let store: SharedObjectStore = test_store.clone();
    let writer = Arc::new(writer_with_window(&store, 500).await);
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    test_store.reset_counters();
    test_store.hang_head_cas.store(true, Ordering::SeqCst);

    let opener_writer = Arc::clone(&writer);
    let opener_namespace = namespace_id.clone();
    let opener = tokio::spawn(async move {
        opener_writer
            .put_file_bytes(&opener_namespace, "/a.txt", b"a", PutFileOptions::default())
            .await
    });
    // The opener's content upload is durable before it enters the window;
    // give it a beat to take the opener role, then join the same window.
    wait_until("opener content upload", || test_store.other_puts() >= 1).await;
    sleep(Duration::from_millis(50)).await;
    let joiner_writer = Arc::clone(&writer);
    let joiner_namespace = namespace_id.clone();
    let joiner = tokio::spawn(async move {
        joiner_writer
            .put_file_bytes(&joiner_namespace, "/b.txt", b"b", PutFileOptions::default())
            .await
    });

    wait_until("head CAS applied and hanging", || {
        test_store.head_cas_hang_reached()
    })
    .await;
    opener.abort();
    let _ = opener.await;
    test_store.hang_head_cas.store(false, Ordering::SeqCst);

    let joiner_result = timeout(Duration::from_secs(15), joiner)
        .await
        .expect("joiner must settle once the opener is cancelled")
        .expect("joiner task");
    let error = joiner_result.expect_err("the joiner's outcome was never distributed");
    assert_eq!(
        core_code(&error),
        Some(ErrorCode::CommitOutcomeUnknown),
        "a flush cancelled in flight must surface an unknown outcome, got: {error:?}"
    );

    // Ground truth: the batch committed durably and is visible.
    let reader = writer.reader();
    reader
        .read_file_bytes(&namespace_id, "/a.txt")
        .await
        .expect("opener's mutation committed");
    reader
        .read_file_bytes(&namespace_id, "/b.txt")
        .await
        .expect("joiner's mutation committed");
}

/// Opener cancelled while the window is still buffering: nothing was
/// published, so members get a definite, retryable rejection and the store
/// shows no publication attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_opener_before_flush_rejects_members_and_retry_succeeds() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("preflush").expect("valid namespace id");
    let test_store = Arc::new(WindowTestStore::new(temp_dir.path(), &namespace_id));
    let store: SharedObjectStore = test_store.clone();
    let writer = Arc::new(writer_with_window(&store, 1_500).await);
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    test_store.reset_counters();
    let opener_writer = Arc::clone(&writer);
    let opener_namespace = namespace_id.clone();
    let opener = tokio::spawn(async move {
        opener_writer
            .put_file_bytes(&opener_namespace, "/a.txt", b"a", PutFileOptions::default())
            .await
    });
    wait_until("opener content upload", || test_store.other_puts() >= 1).await;
    sleep(Duration::from_millis(50)).await;
    let joiner_writer = Arc::clone(&writer);
    let joiner_namespace = namespace_id.clone();
    let joiner = tokio::spawn(async move {
        joiner_writer
            .put_file_bytes(&joiner_namespace, "/b.txt", b"b", PutFileOptions::default())
            .await
    });
    wait_until("joiner content upload", || test_store.other_puts() >= 2).await;
    sleep(Duration::from_millis(50)).await;

    // The opener is parked in the window delay; cancelling it closes the
    // window before any flush.
    opener.abort();
    let _ = opener.await;

    let joiner_result = timeout(Duration::from_secs(15), joiner)
        .await
        .expect("joiner must settle once the window closes")
        .expect("joiner task");
    let error = joiner_result.expect_err("the window closed before publishing");
    assert!(
        format!("{error}").contains("closed before its flush was attempted"),
        "expected the explicit pre-flush rejection, got: {error:?}"
    );
    assert_eq!(
        test_store.head_cas_writes(),
        0,
        "no publication may have been attempted"
    );
    let reader = writer.reader();
    reader
        .read_file_bytes(&namespace_id, "/b.txt")
        .await
        .expect_err("nothing was published");

    // A definite rejection is safe to retry, and the retry lands.
    writer
        .put_file_bytes(&namespace_id, "/b.txt", b"b", PutFileOptions::default())
        .await
        .expect("retry after a definite rejection");
    reader
        .read_file_bytes(&namespace_id, "/b.txt")
        .await
        .expect("retried mutation is visible");
}

/// A cancelled joiner abandons only its result: the submission it buffered
/// was admitted to the window and still publishes with the batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_joiner_does_not_cancel_admitted_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("joiner").expect("valid namespace id");
    let test_store = Arc::new(WindowTestStore::new(temp_dir.path(), &namespace_id));
    let store: SharedObjectStore = test_store.clone();
    let writer = Arc::new(writer_with_window(&store, 500).await);
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    test_store.reset_counters();
    let opener_writer = Arc::clone(&writer);
    let opener_namespace = namespace_id.clone();
    let opener = tokio::spawn(async move {
        opener_writer
            .put_file_bytes(&opener_namespace, "/a.txt", b"a", PutFileOptions::default())
            .await
    });
    wait_until("opener content upload", || test_store.other_puts() >= 1).await;
    sleep(Duration::from_millis(50)).await;
    let joiner_writer = Arc::clone(&writer);
    let joiner_namespace = namespace_id.clone();
    let joiner = tokio::spawn(async move {
        joiner_writer
            .put_file_bytes(&joiner_namespace, "/b.txt", b"b", PutFileOptions::default())
            .await
    });
    wait_until("joiner content upload", || test_store.other_puts() >= 2).await;
    sleep(Duration::from_millis(50)).await;

    joiner.abort();
    let _ = joiner.await;

    timeout(Duration::from_secs(15), opener)
        .await
        .expect("opener must settle")
        .expect("opener task")
        .expect("opener's mutation commits");
    let reader = writer.reader();
    reader
        .read_file_bytes(&namespace_id, "/a.txt")
        .await
        .expect("opener's mutation is visible");
    reader
        .read_file_bytes(&namespace_id, "/b.txt")
        .await
        .expect("the cancelled joiner's admitted mutation still published");
}
