#![allow(clippy::panic)]
//! Standalone poll-loop wakeup coverage, observed through durable state.
//!
//! The loop under test owns no observable state of its own — it steps the
//! grep job and parks — so every assertion here reads the grep root the
//! steps publish, which is what an operator watching this process sees.

use super::{maintain_namespace, NamespaceJob, PollShutdown};
use loonfs::{FsAdmin, FsReader, FsWriter};
use loonfs_api::{ChangeSeq, GrepRequest, NamespaceId};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepIndexSnapshot, GrepMaintenanceJob, GrepService, GrepWorker,
    NamespaceReads,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::task::JoinHandle;

const TEST_POLL_INTERVAL: Duration = Duration::from_millis(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn standalone_starts_before_enable_and_wakes_within_poll() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("poll-enable");
    let writer = seed_namespace(store.clone(), &namespace_id).await;
    put_file(&writer, &namespace_id, "poll-enable-put").await;
    let worker = worker(&store, "poll-enable").await;
    let (poll, shutdown) = spawn_poll(&worker, &namespace_id);
    assert!(
        load_grep_root(&*store, &namespace_id)
            .await
            .expect("load absent root")
            .is_none(),
        "an unenabled namespace must not be enabled by the poll loop"
    );

    worker
        .enable(&namespace_id)
        .await
        .expect("externally enable grep");
    wait_for_steady(&store, &namespace_id, ChangeSeq(1)).await;
    assert_searchable(&store, &namespace_id).await;

    stop_poll(poll, shutdown).await;
}

#[tokio::test]
async fn standalone_wakes_after_disable_then_reenable_and_backfills_fresh() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("poll-reenable");
    let writer = seed_namespace(store.clone(), &namespace_id).await;
    let worker = worker(&store, "poll-reenable").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let (poll, shutdown) = spawn_poll(&worker, &namespace_id);
    wait_for_steady(&store, &namespace_id, ChangeSeq(0)).await;

    worker.disable(&namespace_id).await.expect("disable grep");
    put_file(&writer, &namespace_id, "poll-reenable-put").await;
    worker.enable(&namespace_id).await.expect("re-enable grep");
    let backfill = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load re-enabled root")
        .expect("re-enabled root");
    assert!(
        backfill.state().segments().is_empty(),
        "re-enable must begin a fresh backfill"
    );

    wait_for_steady(&store, &namespace_id, ChangeSeq(1)).await;
    assert_searchable(&store, &namespace_id).await;

    stop_poll(poll, shutdown).await;
}

#[tokio::test]
async fn standalone_enable_without_head_change_wakes_empty_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("poll-empty");
    seed_namespace(store.clone(), &namespace_id).await;
    let admin = admin(&store, "poll-empty").await;
    let worker = worker(&store, "poll-empty").await;
    let (poll, shutdown) = spawn_poll(&worker, &namespace_id);
    let head_before = admin
        .namespace_status(&namespace_id)
        .await
        .expect("status before enable");

    worker
        .enable(&namespace_id)
        .await
        .expect("externally enable empty namespace");
    wait_for_steady(&store, &namespace_id, ChangeSeq(0)).await;

    let head_after = admin
        .namespace_status(&namespace_id)
        .await
        .expect("status after enable");
    assert_eq!(head_after.head_seq, head_before.head_seq);
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load empty root")
        .expect("empty root");
    assert!(root.state().segments().is_empty());

    stop_poll(poll, shutdown).await;
}

async fn reader(store: &SharedObjectStore) -> FsReader {
    FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader")
}

async fn admin(store: &SharedObjectStore, actor: &str) -> FsAdmin {
    FsAdmin::builder_with_store(store.clone())
        .actor_id(format!("{actor}-worker"))
        .build()
        .await
        .expect("build admin")
}

async fn worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    GrepWorker::new(
        store.clone(),
        reader(store).await,
        admin(store, actor).await,
    )
}

async fn seed_namespace(store: SharedObjectStore, namespace_id: &NamespaceId) -> FsWriter {
    super::test_seeding::writer(
        store,
        namespace_id,
        format!("{}-seed", namespace_id.as_str()),
    )
    .await
}

async fn put_file(writer: &FsWriter, namespace_id: &NamespaceId, commit_id: &str) {
    super::test_seeding::put_file(
        writer,
        namespace_id,
        b"standalone wakeup needle\n",
        "/note.txt",
        commit_id,
    )
    .await;
}

fn spawn_poll(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
) -> (JoinHandle<()>, Arc<PollShutdown>) {
    let job: NamespaceJob =
        GrepMaintenanceJob::new(worker.clone(), GramIndexBuildPolicy::default());
    let shutdown = Arc::new(PollShutdown::new());
    let poll = tokio::runtime::Handle::current().spawn(maintain_namespace(
        job,
        namespace_id.clone(),
        TEST_POLL_INTERVAL,
        shutdown.clone(),
    ));
    (poll, shutdown)
}

async fn stop_poll(poll: JoinHandle<()>, shutdown: Arc<PollShutdown>) {
    shutdown.request();
    poll.await.expect("join namespace poll");
}

/// Waits for the poll loop to publish a steady root at `built_through_seq`.
#[allow(clippy::disallowed_methods)]
async fn wait_for_steady(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    built_through_seq: ChangeSeq,
) {
    // Bounded observation of the real per-namespace poll timer under test:
    // the loop publishes durable state and reports nothing in-process.
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(root) = load_grep_root(&**store, namespace_id)
                .await
                .expect("load polled root")
            {
                if matches!(root.state().lifecycle(), GrepLifecycle::Steady)
                    && root.state().index().built_through_seq == built_through_seq
                {
                    return;
                }
            }
            tokio::time::sleep(TEST_POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the poll loop did not reach {built_through_seq:?}"));
}

async fn assert_searchable(store: &SharedObjectStore, namespace_id: &NamespaceId) {
    let reader = reader(store).await;
    let reads = NamespaceReads::new(&reader, namespace_id);
    let service = GrepService::new();
    let snapshot = GrepIndexSnapshot::from_grep_root(&**store, namespace_id, &service).await;
    let response = service
        .query(
            &GrepRequest {
                pattern: "wakeup needle".to_owned(),
                case_insensitive: false,
                path_prefix: None,
                cursor: None,
                limit: None,
                allow_stale: false,
                allow_scan: false,
            },
            &snapshot,
            &reads,
            store,
        )
        .await
        .expect("query caught-up index");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path.as_str(), "/note.txt");
}
