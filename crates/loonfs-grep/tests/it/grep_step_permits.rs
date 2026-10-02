#![allow(clippy::panic)]
// A step that finishes while its content read is held panics with its outcome.

//! How many grep steps hold file content or index segments at once.

use crate::common::GrepHost;
use async_trait::async_trait;
use bytes::Bytes;
use loonfs::{InlineContentPolicy, LoonFs, NamespaceId, SharedObjectStore, Writable};
use loonfs_grep::keyspace::{parse_key, GrepKeyKind};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepReorganizeOutcome, GrepStepBudget,
    GrepStepBudgetStats, GrepWorker,
};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use loonfs_test_support::delegate_object_store;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::timeout;

/// Holds every content read and grep segment read or write for a moment,
/// and records the most namespaces that had one in flight at the same time.
#[derive(Debug)]
struct ContentWatch {
    inner: LocalFsStore,
    holding: Mutex<BTreeMap<String, usize>>,
    peak_namespaces: AtomicUsize,
}

impl ContentWatch {
    fn new(inner: LocalFsStore) -> Self {
        Self {
            inner,
            holding: Mutex::default(),
            peak_namespaces: AtomicUsize::new(0),
        }
    }

    async fn watch<T>(&self, key: &str, request: impl Future<Output = T>) -> T {
        let Some(namespace) = content_namespace(key) else {
            return request.await;
        };
        {
            let mut holding = self.holding.lock().expect("holding lock");
            *holding.entry(namespace.clone()).or_default() += 1;
            self.peak_namespaces
                .fetch_max(holding.len(), Ordering::SeqCst);
        }
        hold_request().await;
        let result = request.await;
        let mut holding = self.holding.lock().expect("holding lock");
        let count = holding.get_mut(&namespace).expect("an entered namespace");
        *count -= 1;
        if *count == 0 {
            holding.remove(&namespace);
        }
        result
    }
}

/// The namespace of a content object or a grep segment.
fn content_namespace(key: &str) -> Option<String> {
    if let Some(parsed) =
        parse_object_key(key).filter(|parsed| parsed.family() == DurableObjectFamily::ContentBlob)
    {
        return Some(parsed.owner_namespace_id().to_owned());
    }
    parse_key(key)
        .filter(|parsed| matches!(parsed.kind, GrepKeyKind::Segment { .. }))
        .map(|parsed| parsed.namespace_id.to_string())
}

#[allow(
    clippy::disallowed_methods,
    reason = "a test-side pause on content requests leaves other steps time to start theirs"
)]
async fn hold_request() {
    tokio::time::sleep(Duration::from_millis(5)).await;
}

#[async_trait]
impl ObjectStore for ContentWatch {
    delegate_object_store!(self => self.inner;
        head,
        head_stored_checksum,
        create_multipart_upload,
        complete_multipart_upload,
        abort_multipart_upload,
        get_with_metadata,
        delete,
        list_prefix_stream,
        list_prefix_from_stream,
        list_prefix,
        list_child_prefixes,
    );

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.watch(key, self.inner.get(key, range)).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.watch(key, self.inner.put(key, bytes, mode)).await
    }
}

/// A writer that stores every file as a content object, so a step's content
/// reads reach the store.
async fn content_object_writer(store: &SharedObjectStore) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone())
        .writer_id("grep-step-permits-writer")
        .min_publish_interval_ms(0)
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: None,
            ..InlineContentPolicy::default()
        })
        .build()
        .await
        .expect("build writer")
}

async fn put_files(writer: &LoonFs<Writable>, namespace_id: &NamespaceId, prefix: &str) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    for index in 0..2 {
        namespace
            .put_file(
                &format!("/{prefix}-{index}.txt"),
                format!("needle {prefix} {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
    }
}

async fn namespace_with_files(writer: &LoonFs<Writable>, name: &str) -> NamespaceId {
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    put_files(writer, &namespace_id, "first").await;
    namespace_id
}

fn worker_with(host: &GrepHost, budget: &GrepStepBudget) -> GrepWorker<SharedObjectStore> {
    GrepWorker::new(
        host.store.clone(),
        host.reader.clone(),
        host.maintenance.clone(),
        budget.clone(),
    )
}

async fn blocking_setup(
    directory: &tempfile::TempDir,
) -> (Arc<BlockingStore<LocalFsStore>>, LoonFs<Writable>, GrepHost) {
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationClass::Get,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = content_object_writer(&store).await;
    let host = GrepHost::new(&store, "grep-step-permits").await;
    (blocking, writer, host)
}

/// Starts a build step and returns it parked at its first content read,
/// holding a step permit.
async fn parked_build_step<'a>(
    blocking: &BlockingStore<LocalFsStore>,
    worker: &'a GrepWorker<SharedObjectStore>,
    namespace_id: &'a NamespaceId,
) -> Pin<Box<impl Future<Output = loonfs_grep::Result<GrepBuildOutcome>> + 'a>> {
    blocking.block_next();
    let mut step = Box::pin(worker.build_step(namespace_id, GramIndexBuildPolicy::default()));
    tokio::select! {
        outcome = &mut step => panic!("the step finished while its read was held: {outcome:?}"),
        () = blocking.wait_until_blocked() => {}
    }
    step
}

/// Drives `step` until `budget` reports `expected`. The step must not finish
/// first.
async fn drive_until_reported(
    step: &mut (impl Future<Output = loonfs_grep::Result<GrepBuildOutcome>> + Unpin),
    budget: &GrepStepBudget,
    expected: GrepStepBudgetStats,
) {
    let reported = async {
        while budget.stats() != expected {
            tokio::task::yield_now().await;
        }
    };
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            outcome = step => panic!("the step finished before the budget reported {expected:?}: {outcome:?}"),
            () = reported => {}
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the budget reported {:?}, not {expected:?}", budget.stats()));
}

#[tokio::test]
async fn no_more_grep_steps_hold_content_than_the_limit() {
    let directory = tempdir().expect("directory");
    let watch = Arc::new(ContentWatch::new(
        LocalFsStore::new(directory.path()).expect("store"),
    ));
    let store: SharedObjectStore = watch.clone();
    let writer = content_object_writer(&store).await;
    let host = GrepHost::new(&store, "grep-step-permits").await;
    let worker = worker_with(&host, &GrepStepBudget::new(NonZeroUsize::MIN));
    let mut namespaces = Vec::new();
    for index in 0..3 {
        let namespace_id = namespace_with_files(&writer, &format!("permits-{index}")).await;
        worker.enable(&namespace_id).await.expect("enable grep");
        namespaces.push(namespace_id);
    }
    // The backfill run and the incremental run make a reorganize step due.
    let policy = GramIndexBuildPolicy {
        max_delta_runs: NonZeroUsize::new(2).expect("two delta runs"),
        ..GramIndexBuildPolicy::default()
    };
    watch.peak_namespaces.store(0, Ordering::SeqCst);

    let backfills = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.build_step(namespace_id, policy)),
    )
    .await
    .expect("backfill steps");
    for namespace_id in &namespaces {
        put_files(&writer, namespace_id, "second").await;
    }
    let increments = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.build_step(namespace_id, policy)),
    )
    .await
    .expect("incremental steps");
    let reorganizations = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.reorganize_step(namespace_id, policy)),
    )
    .await
    .expect("reorganize steps");

    for outcome in backfills.iter().chain(&increments) {
        assert!(
            matches!(outcome, GrepBuildOutcome::Published { .. }),
            "{outcome:?}"
        );
    }
    for outcome in &reorganizations {
        assert!(
            matches!(outcome, GrepReorganizeOutcome::UnitPublished { .. }),
            "{outcome:?}"
        );
    }
    assert_eq!(
        watch.peak_namespaces.load(Ordering::SeqCst),
        1,
        "one step holds content at a time, across every namespace"
    );
}

#[tokio::test]
async fn workers_over_two_stores_share_one_step_permit() {
    let first_directory = tempdir().expect("directory");
    let second_directory = tempdir().expect("directory");
    let (blocking, first_writer, first_host) = blocking_setup(&first_directory).await;
    let (_, second_writer, second_host) = blocking_setup(&second_directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let first_worker = worker_with(&first_host, &budget);
    let second_worker = worker_with(&second_host, &budget);
    let first = namespace_with_files(&first_writer, "first").await;
    let second = namespace_with_files(&second_writer, "second").await;
    first_worker.enable(&first).await.expect("enable grep");
    second_worker.enable(&second).await.expect("enable grep");

    let holding_step = parked_build_step(&blocking, &first_worker, &first).await;
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );
    let mut waiting_step =
        Box::pin(second_worker.build_step(&second, GramIndexBuildPolicy::default()));
    drive_until_reported(
        &mut waiting_step,
        &budget,
        GrepStepBudgetStats {
            running: 1,
            waiting: 1,
        },
    )
    .await;

    blocking.release();
    let outcome = holding_step.await.expect("holding step");
    assert!(
        matches!(outcome, GrepBuildOutcome::Published { .. }),
        "{outcome:?}"
    );
    let outcome = timeout(Duration::from_secs(10), waiting_step)
        .await
        .expect("the waiting step takes the permit the first step returned")
        .expect("waiting step");
    assert!(
        matches!(outcome, GrepBuildOutcome::Published { .. }),
        "{outcome:?}"
    );
    assert_eq!(budget.stats(), GrepStepBudgetStats::default());
}

#[tokio::test]
async fn an_up_to_date_step_on_either_worker_takes_no_permit() {
    let first_directory = tempdir().expect("directory");
    let second_directory = tempdir().expect("directory");
    let (blocking, first_writer, first_host) = blocking_setup(&first_directory).await;
    let (_, second_writer, second_host) = blocking_setup(&second_directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let first_worker = worker_with(&first_host, &budget);
    let second_worker = worker_with(&second_host, &budget);
    let mut current = Vec::new();
    for (writer, host, worker) in [
        (&first_writer, &first_host, &first_worker),
        (&second_writer, &second_host, &second_worker),
    ] {
        let namespace_id = namespace_with_files(writer, "current").await;
        host.enable_grep_index(&namespace_id)
            .await
            .expect("index the current namespace");
        current.push((worker, namespace_id));
    }
    let busy = namespace_with_files(&first_writer, "busy").await;
    first_worker.enable(&busy).await.expect("enable grep");

    let busy_step = parked_build_step(&blocking, &first_worker, &busy).await;
    for (worker, namespace_id) in &current {
        let outcome = timeout(
            Duration::from_secs(10),
            worker.build_step(namespace_id, GramIndexBuildPolicy::default()),
        )
        .await
        .expect("an up-to-date step does not wait for the held permit")
        .expect("build step");
        assert!(
            matches!(outcome, GrepBuildOutcome::UpToDate { .. }),
            "{outcome:?}"
        );
    }
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );

    blocking.release();
    let outcome = busy_step.await.expect("busy step");
    assert!(
        matches!(outcome, GrepBuildOutcome::Published { .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_dropped_step_returns_its_permit() {
    let directory = tempdir().expect("directory");
    let (blocking, writer, host) = blocking_setup(&directory).await;
    let worker = worker_with(&host, &GrepStepBudget::new(NonZeroUsize::MIN));
    let holding = namespace_with_files(&writer, "holding").await;
    let waiting = namespace_with_files(&writer, "waiting").await;
    for namespace_id in [&holding, &waiting] {
        worker.enable(namespace_id).await.expect("enable grep");
    }
    let policy = GramIndexBuildPolicy::default();

    // One step takes the only permit and parks at its first content read.
    blocking.block_next();
    let mut holding_step = Box::pin(worker.build_step(&holding, policy));
    tokio::select! {
        outcome = &mut holding_step => panic!("the holding step finished while its read was held: {outcome:?}"),
        () = blocking.wait_until_blocked() => {}
    }
    // Another step with work waits for that permit, and is dropped waiting.
    assert!(
        timeout(
            Duration::from_millis(200),
            worker.build_step(&waiting, policy)
        )
        .await
        .is_err(),
        "a step with work waits while the only permit is held"
    );
    drop(holding_step);
    blocking.release();

    let outcome = timeout(Duration::from_secs(10), worker.build_step(&waiting, policy))
        .await
        .expect("both dropped steps returned the permit")
        .expect("build step");
    assert!(
        matches!(outcome, GrepBuildOutcome::Published { .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_step_dropped_while_waiting_leaves_the_counts_at_zero() {
    let directory = tempdir().expect("directory");
    let (blocking, writer, host) = blocking_setup(&directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let worker = worker_with(&host, &budget);
    let holding = namespace_with_files(&writer, "holding").await;
    let waiting = namespace_with_files(&writer, "waiting").await;
    for namespace_id in [&holding, &waiting] {
        worker.enable(namespace_id).await.expect("enable grep");
    }

    let holding_step = parked_build_step(&blocking, &worker, &holding).await;
    let mut waiting_step = Box::pin(worker.build_step(&waiting, GramIndexBuildPolicy::default()));
    drive_until_reported(
        &mut waiting_step,
        &budget,
        GrepStepBudgetStats {
            running: 1,
            waiting: 1,
        },
    )
    .await;
    drop(waiting_step);
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );

    blocking.release();
    let outcome = holding_step.await.expect("holding step");
    assert!(
        matches!(outcome, GrepBuildOutcome::Published { .. }),
        "{outcome:?}"
    );
    assert_eq!(budget.stats(), GrepStepBudgetStats::default());
}
