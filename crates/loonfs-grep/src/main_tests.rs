#![allow(clippy::panic)]
//! Standalone poller wakeup coverage with in-process driver observation.

use super::{poll_namespace, PollShutdown};
use loonfs::{FsAdmin, FsReader, FsWriter};
use loonfs_api::{ChangeSeq, GrepRequest, NamespaceId};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepDriver, GrepDriverParked, GrepDriverState, GrepDriverTask,
    GrepIndexSnapshot, GrepService, GrepStepLimiter, GrepWorker, NamespaceReads,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_test_support::ids::namespace_id;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::watch;
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
    let reader = reader(&store).await;
    let worker = worker(&store, "poll-enable").await;
    let driver = spawn_driver(worker.clone(), namespace_id.clone());
    let handle = driver.handle();
    wait_for_parked(&handle, GrepDriverParked::NotEnabled).await;
    let (poll, shutdown) = spawn_poll(
        store.clone(),
        reader.clone(),
        namespace_id.clone(),
        handle.clone(),
    );

    worker
        .enable(&namespace_id)
        .await
        .expect("externally enable grep");
    wait_for_parked(
        &handle,
        GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(1),
        },
    )
    .await;
    assert_searchable(&store, &namespace_id).await;

    stop_poll_and_driver(poll, shutdown, driver).await;
}

#[tokio::test]
async fn standalone_wakes_after_disable_then_reenable_and_backfills_fresh() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("poll-reenable");
    let writer = seed_namespace(store.clone(), &namespace_id).await;
    let reader = reader(&store).await;
    let worker = worker(&store, "poll-reenable").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let driver = spawn_driver(worker.clone(), namespace_id.clone());
    let handle = driver.handle();
    wait_for_parked(
        &handle,
        GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(0),
        },
    )
    .await;
    let (poll, shutdown) = spawn_poll(
        store.clone(),
        reader.clone(),
        namespace_id.clone(),
        handle.clone(),
    );

    worker.disable(&namespace_id).await.expect("disable grep");
    put_file(&writer, &namespace_id, "poll-reenable-put").await;
    wait_for_parked(&handle, GrepDriverParked::NotEnabled).await;
    worker.enable(&namespace_id).await.expect("re-enable grep");
    let backfill = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load re-enabled root")
        .expect("re-enabled root");
    assert!(matches!(
        backfill.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));
    assert!(
        backfill.state().segments().is_empty(),
        "re-enable must begin a fresh backfill"
    );

    wait_for_parked(
        &handle,
        GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(1),
        },
    )
    .await;
    assert_searchable(&store, &namespace_id).await;

    stop_poll_and_driver(poll, shutdown, driver).await;
}

#[tokio::test]
async fn standalone_enable_without_head_change_wakes_empty_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("poll-empty");
    seed_namespace(store.clone(), &namespace_id).await;
    let reader = reader(&store).await;
    let admin = admin(&store, "poll-empty").await;
    let worker = worker(&store, "poll-empty").await;
    let driver = spawn_driver(worker.clone(), namespace_id.clone());
    let handle = driver.handle();
    wait_for_parked(&handle, GrepDriverParked::NotEnabled).await;
    let (poll, shutdown) = spawn_poll(
        store.clone(),
        reader.clone(),
        namespace_id.clone(),
        handle.clone(),
    );
    let head_before = admin
        .namespace_status(&namespace_id)
        .await
        .expect("status before enable");

    worker
        .enable(&namespace_id)
        .await
        .expect("externally enable empty namespace");
    wait_for_parked(
        &handle,
        GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(0),
        },
    )
    .await;

    let head_after = admin
        .namespace_status(&namespace_id)
        .await
        .expect("status after enable");
    assert_eq!(head_after.head_seq, head_before.head_seq);
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load empty root")
        .expect("empty root");
    assert!(matches!(root.state().lifecycle(), GrepLifecycle::Steady));
    assert!(root.state().segments().is_empty());

    stop_poll_and_driver(poll, shutdown, driver).await;
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

fn spawn_driver(
    worker: GrepWorker<SharedObjectStore>,
    namespace_id: NamespaceId,
) -> GrepDriverTask {
    GrepDriver::new(
        worker,
        namespace_id,
        GramIndexBuildPolicy::default(),
        GrepStepLimiter::new(NonZeroUsize::MIN),
    )
    .spawn_on(&tokio::runtime::Handle::current())
}

fn spawn_poll(
    store: SharedObjectStore,
    reader: FsReader,
    namespace_id: NamespaceId,
    driver: loonfs_grep::GrepDriverHandle,
) -> (JoinHandle<()>, Arc<PollShutdown>) {
    let shutdown = Arc::new(PollShutdown::new());
    let poll = tokio::runtime::Handle::current().spawn(poll_namespace(
        store,
        reader,
        namespace_id,
        driver,
        TEST_POLL_INTERVAL,
        shutdown.clone(),
    ));
    (poll, shutdown)
}

async fn wait_for_parked(driver: &loonfs_grep::GrepDriverHandle, expected: GrepDriverParked) {
    let mut state = driver.subscribe_state();
    tokio::time::timeout(TEST_TIMEOUT, wait_for_state(&mut state, expected))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "driver did not reach {expected:?}; state: {:?}",
                driver.state()
            )
        });
}

async fn wait_for_state(state: &mut watch::Receiver<GrepDriverState>, expected: GrepDriverParked) {
    loop {
        if *state.borrow_and_update() == GrepDriverState::Parked(expected) {
            return;
        }
        state
            .changed()
            .await
            .expect("driver state remains observable");
    }
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

async fn stop_poll_and_driver(
    poll: JoinHandle<()>,
    shutdown: Arc<PollShutdown>,
    driver: GrepDriverTask,
) {
    shutdown.request();
    poll.await.expect("join namespace poll");
    driver.shutdown().await.expect("stop grep driver");
}
