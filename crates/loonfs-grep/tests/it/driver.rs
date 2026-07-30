#![allow(clippy::panic)]
//! Per-namespace driver isolation, backoff, and explicit-GC boundaries.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{DeleteNamespaceOptions, FsAdmin, FsReader, FsWriter, SharedObjectStore};
use loonfs_api::{IndexSegmentId, NamespaceId};
use loonfs_grep::keyspace::{root_key, segment_key};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepDriver, GrepDriverParked, GrepDriverState, GrepStepLimiter,
    GrepWorker,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::nonzero_usize;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Notify;

#[tokio::test]
async fn poisoned_driver_backs_off_while_sibling_catches_up_and_gc_stays_explicit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let poisoned = NamespaceId::parse("poisoned").expect("namespace id");
    let healthy = NamespaceId::parse("healthy").expect("namespace id");
    let poisoned_writer = seed(store.clone(), &poisoned).await;
    let healthy_writer = seed(store.clone(), &healthy).await;
    put_file(&poisoned_writer, &poisoned, "poisoned-put").await;
    put_file(&healthy_writer, &healthy, "healthy-put").await;

    let worker = worker(store.clone(), "driver-test-worker").await;
    worker.enable(&poisoned).await.expect("enable poisoned");
    worker.enable(&healthy).await.expect("enable healthy");
    let orphan = segment_key(&healthy, &IndexSegmentId::generate());
    store
        .put_overwrite(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("write orphan");
    store
        .put_overwrite(&root_key(&poisoned), Bytes::from_static(b"poison"))
        .await
        .expect("poison root");

    let runtime = tokio::runtime::Handle::current();
    let step_limiter = GrepStepLimiter::new(nonzero_usize(2));
    let poisoned_task = GrepDriver::new(
        worker.clone(),
        poisoned,
        GramIndexBuildPolicy::default(),
        step_limiter.clone(),
    )
    .spawn_on(&runtime);
    let healthy_task = GrepDriver::new(
        worker,
        healthy,
        GramIndexBuildPolicy::default(),
        step_limiter,
    )
    .spawn_on(&runtime);
    assert_eq!(
        healthy_task.handle().wait_for_quiescence().await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(1)
        })
    );

    let mut poisoned_state = poisoned_task.handle().subscribe_state();
    loop {
        if matches!(
            *poisoned_state.borrow_and_update(),
            GrepDriverState::BackingOff { .. }
        ) {
            break;
        }
        poisoned_state
            .changed()
            .await
            .expect("poisoned driver remains observable");
    }
    assert!(
        store.head(&orphan).await.expect("head orphan").is_some(),
        "driver catch-up must never run garbage collection implicitly"
    );

    poisoned_task
        .shutdown()
        .await
        .expect("stop poisoned driver");
    healthy_task.shutdown().await.expect("stop healthy driver");
}

#[tokio::test]
async fn tombstoned_namespace_driver_parks_as_not_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("deleted").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    let worker = worker(store, "deleted-driver-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    writer
        .delete_namespace(&namespace_id, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");

    let task = GrepDriver::new(
        worker,
        namespace_id,
        GramIndexBuildPolicy::default(),
        GrepStepLimiter::new(NonZeroUsize::MIN),
    )
    .spawn_on(&tokio::runtime::Handle::current());
    assert_eq!(
        task.handle().wait_for_quiescence().await,
        Some(GrepDriverParked::NotEnabled)
    );
    task.shutdown().await.expect("stop deleted driver");
}

/// Namespace drivers keep their singleflight ownership but share one FIFO
/// expensive-step cap. Blocking each namespace's sole backfill content read
/// makes the number of simultaneously entered build steps directly visible.
#[tokio::test]
async fn shared_step_limiter_caps_many_namespaces_and_every_driver_converges() {
    const NAMESPACES: usize = 8;
    const MAX_CONCURRENT_STEPS: usize = 2;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(StepConcurrencyStore::new(temp_dir.path()));
    let worker = worker(store.clone(), "concurrency-driver-worker").await;
    let mut namespace_ids = Vec::new();
    for index in 0..NAMESPACES {
        let namespace_id =
            NamespaceId::parse(format!("concurrency-{index}")).expect("namespace id");
        let writer = seed(store.clone(), &namespace_id).await;
        put_file(&writer, &namespace_id, &format!("concurrency-put-{index}")).await;
        worker.enable(&namespace_id).await.expect("enable grep");
        namespace_ids.push(namespace_id);
    }

    store.pause_content_reads();
    let limiter = GrepStepLimiter::new(nonzero_usize(MAX_CONCURRENT_STEPS));
    let runtime = tokio::runtime::Handle::current();
    let mut tasks = Vec::new();
    for namespace_id in namespace_ids {
        tasks.push(
            GrepDriver::new(
                worker.clone(),
                namespace_id,
                GramIndexBuildPolicy::default(),
                limiter.clone(),
            )
            .spawn_on(&runtime),
        );
    }

    tokio::time::timeout(
        Duration::from_secs(5),
        store.wait_for_started(MAX_CONCURRENT_STEPS),
    )
    .await
    .expect("the permitted drivers must enter their build steps");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), store.wait_for_started(3))
            .await
            .is_err(),
        "a third namespace must remain queued while both permits are held"
    );
    assert_eq!(store.peak_in_flight(), MAX_CONCURRENT_STEPS);

    store.resume_content_reads();
    for task in &tasks {
        assert_eq!(
            task.handle().wait_for_quiescence().await,
            Some(GrepDriverParked::CaughtUp {
                built_through_seq: loonfs_api::ChangeSeq(1),
            })
        );
    }
    assert!(
        store.peak_in_flight() <= MAX_CONCURRENT_STEPS,
        "executing grep steps exceeded the shared cap"
    );
    for task in tasks {
        task.shutdown().await.expect("stop concurrency driver");
    }
}

#[derive(Debug)]
struct StepConcurrencyStore {
    inner: LocalFsStore,
    pause_content_reads: AtomicBool,
    started: AtomicUsize,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    started_notify: Notify,
    resume_notify: Notify,
}

impl StepConcurrencyStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("store"),
            pause_content_reads: AtomicBool::new(false),
            started: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started_notify: Notify::new(),
            resume_notify: Notify::new(),
        }
    }

    fn pause_content_reads(&self) {
        self.pause_content_reads.store(true, Ordering::Release);
    }

    fn resume_content_reads(&self) {
        self.pause_content_reads.store(false, Ordering::Release);
        self.resume_notify.notify_waiters();
    }

    fn peak_in_flight(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }

    async fn wait_for_started(&self, target: usize) {
        loop {
            let notified = self.started_notify.notified();
            if self.started.load(Ordering::Acquire) >= target {
                return;
            }
            notified.await;
        }
    }

    async fn observe_content_read<T>(&self, key: &str, read: impl Future<Output = T>) -> T {
        if !key.contains("/blobs/sha256/") {
            return read.await;
        }
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(in_flight, Ordering::AcqRel);
        self.started.fetch_add(1, Ordering::AcqRel);
        self.started_notify.notify_waiters();
        if self.pause_content_reads.load(Ordering::Acquire) {
            loop {
                let notified = self.resume_notify.notified();
                if !self.pause_content_reads.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        }
        let result = read.await;
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        result
    }
}

#[async_trait]
impl ObjectStore for StepConcurrencyStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.observe_content_read(key, self.inner.get_with_metadata(key))
            .await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.observe_content_read(key, self.inner.get(key, range))
            .await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
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

/// A worker over one store: grep's own keyspace writes go straight to it,
/// and the runtime handles it reads and checkpoints through are opened on
/// the same client, so a fault-injecting store covers both.
async fn worker<S: ObjectStore + 'static>(store: Arc<S>, actor: &str) -> GrepWorker<Arc<S>> {
    let shared: SharedObjectStore = store.clone();
    let reader = FsReader::builder_with_store(shared.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(shared)
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store, reader, admin)
}

async fn seed<S: ObjectStore + 'static>(store: Arc<S>, namespace_id: &NamespaceId) -> FsWriter {
    crate::test_seeding::writer(store, namespace_id, format!("seed-{namespace_id}")).await
}

async fn put_file(writer: &FsWriter, namespace_id: &NamespaceId, commit_id: &str) {
    crate::test_seeding::put_file(
        writer,
        namespace_id,
        b"driver needle\n",
        "/note.txt",
        commit_id,
    )
    .await;
}
