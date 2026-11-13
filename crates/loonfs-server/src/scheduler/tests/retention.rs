//! The retention advance that sweeps make under each namespace's
//! retention window.

use super::{
    idle_fold_after_ms, local_store, scheduler_config, seed_writer, SchedulerServer, DAY_MS,
};
use crate::config::ServerConfig;
use crate::scheduler::lock;
use crate::scheduler::retries::MaintenanceCall;
use loonfs::{
    AdvanceRetentionFloorOptions, CommitSeq, CreateNamespaceOptions, NamespaceDiagnostics,
    NamespaceId, RetentionTarget, SharedObjectStore,
};
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_test_support::test_actor;
use std::num::NonZeroU64;
use std::sync::Arc;
use tempfile::tempdir;

/// When the server's clock starts, so every commit a test makes through the
/// server's runtime is stamped at a known time.
pub(super) const START_MS: u64 = 100 * DAY_MS;

/// The 30-day window from the design note.
pub(super) const WINDOW_MS: u64 = 30 * DAY_MS;

/// A namespace's own window, shorter than the server's.
const NAMESPACE_WINDOW_MS: u64 = 15 * DAY_MS;

/// Requests the advance costs in a sweep of an idle namespace whose floor
/// already reached its cutoff, or that has no window: one discovery of the
/// current manifest and the WAL tail after it. The docs quote this number.
const IDLE_ADVANCE_REQUESTS: usize = 6;

/// A config with every default, and `window_ms` as its retention window.
pub(super) fn retention_config(window_ms: Option<u64>) -> ServerConfig {
    let mut config = scheduler_config(None);
    config.retention_window_ms =
        window_ms.map(|window_ms| NonZeroU64::new(window_ms).expect("a positive window"));
    config
}

impl SchedulerServer {
    pub(super) async fn diagnostics(&self, namespace_id: &NamespaceId) -> NamespaceDiagnostics {
        self.maintenance
            .diagnostics(namespace_id)
            .await
            .expect("namespace diagnostics")
    }

    fn retention_failures(&self) -> u64 {
        self.call_failures(MaintenanceCall::Retention)
    }

    /// Creates a namespace with `window_ms` as its own retention window.
    async fn create_with_window(&self, namespace_id: &NamespaceId, window_ms: Option<u64>) {
        self.runtime
            .create_namespace_with_options(
                namespace_id,
                &test_actor(),
                &CreateNamespaceOptions {
                    retention_window_ms: window_ms.and_then(NonZeroU64::new),
                    ..CreateNamespaceOptions::default()
                },
            )
            .await
            .expect("create the namespace");
    }

    async fn retention_floor(&self, namespace_id: &NamespaceId) -> CommitSeq {
        self.diagnostics(namespace_id).await.retention_floor_seq
    }

    /// Writes one file on the server's clock, and returns its commit's
    /// sequence.
    pub(super) async fn commit(&self, namespace_id: &NamespaceId, path: &str) -> CommitSeq {
        self.runtime
            .open_namespace(namespace_id)
            .expect("open the namespace")
            .put_file(path, b"history", &test_actor())
            .await
            .expect("write a file")
            .committed_seq
    }
}

/// Namespaces whose history spans the retention window, beside a deleted
/// namespace that the sweep also lists.
struct History {
    /// A namespace with two commits older than the window and one inside it.
    aged: NamespaceId,
    /// The newest commit of `aged` that is older than the window.
    aged_cutoff_seq: CommitSeq,
    /// A namespace whose every commit is inside the window.
    recent: NamespaceId,
}

/// Commits and folds the history on the server's clock, which then reads
/// 40 days after `START_MS`. With a 30-day window, the cutoff is 10 days
/// after `START_MS`.
async fn seed_history(server: &SchedulerServer) -> History {
    let [aged, recent, deleted] = ["aged", "recent", "deleted"].map(namespace_id);
    for namespace_id in [&aged, &deleted] {
        server
            .runtime
            .create_namespace(namespace_id, &test_actor())
            .await
            .expect("create the namespace");
    }
    server.commit(&aged, "/oldest.txt").await;
    let aged_cutoff_seq = server.commit(&aged, "/old.txt").await;
    server
        .runtime
        .open_namespace(&deleted)
        .expect("open the namespace")
        .delete()
        .await
        .expect("delete the namespace");
    server.clock.advance_ms(20 * DAY_MS);
    server.commit(&aged, "/new.txt").await;
    server
        .runtime
        .create_namespace(&recent, &test_actor())
        .await
        .expect("create the namespace");
    server.commit(&recent, "/recent.txt").await;
    for namespace_id in [&aged, &recent] {
        server
            .maintenance
            .fold_wal(namespace_id)
            .await
            .expect("fold the history");
    }
    server.clock.advance_ms(20 * DAY_MS);
    History {
        aged,
        aged_cutoff_seq,
        recent,
    }
}

#[tokio::test]
async fn a_sweep_advances_each_floor_to_the_last_commit_before_the_window() {
    for window_ms in [None, Some(WINDOW_MS)] {
        let directory = tempdir().expect("tempdir");
        let server = SchedulerServer::start(
            local_store(directory.path()),
            &retention_config(window_ms),
            START_MS,
        )
        .await;
        let history = seed_history(&server).await;
        let aged = server.diagnostics(&history.aged).await;
        let recent = server.diagnostics(&history.recent).await;
        assert!(aged.retention_floor_seq < history.aged_cutoff_seq);
        assert!(history.aged_cutoff_seq < aged.head_seq);

        server.scheduler.run_sweep().await.expect("list namespaces");
        let expected_floor = match window_ms {
            Some(_) => history.aged_cutoff_seq,
            None => aged.retention_floor_seq,
        };
        assert_eq!(
            server.diagnostics(&history.aged).await.retention_floor_seq,
            expected_floor,
            "window {window_ms:?}: the floor moves to the newest commit older than the window, \
             and only with a window"
        );
        let after = server.diagnostics(&history.recent).await;
        assert_eq!(
            (after.retention_floor_seq, after.current_manifest_no),
            (recent.retention_floor_seq, recent.current_manifest_no),
            "window {window_ms:?}: a namespace with no commit older than the window keeps its \
             floor and publishes nothing"
        );
        assert_eq!(
            server.retention_failures(),
            0,
            "window {window_ms:?}: a deleted namespace is skipped, not failed"
        );
    }
}

/// Commits to each namespace at `START_MS` and 20 days later, folds the
/// commits, and returns each namespace's two sequences. The server's clock
/// then reads 40 days after `START_MS`, so a 30-day window passes the first
/// commit, and a 15-day window passes both.
async fn seed_two_commits(
    server: &SchedulerServer,
    namespaces: &[&NamespaceId],
) -> Vec<[CommitSeq; 2]> {
    let mut first = Vec::new();
    for namespace_id in namespaces {
        first.push(server.commit(namespace_id, "/old.txt").await);
    }
    server.clock.advance_ms(20 * DAY_MS);
    let mut seqs = Vec::new();
    for (namespace_id, first) in namespaces.iter().zip(first) {
        let second = server.commit(namespace_id, "/new.txt").await;
        server
            .maintenance
            .fold_wal(namespace_id)
            .await
            .expect("fold the history");
        seqs.push([first, second]);
    }
    server.clock.advance_ms(20 * DAY_MS);
    seqs
}

#[tokio::test]
async fn a_sweep_advances_a_namespace_by_its_own_window_and_a_sibling_by_the_servers_default() {
    for server_window_ms in [None, Some(WINDOW_MS)] {
        let directory = tempdir().expect("tempdir");
        let server = SchedulerServer::start(
            local_store(directory.path()),
            &retention_config(server_window_ms),
            START_MS,
        )
        .await;
        let [windowed, sibling] = ["windowed", "sibling"].map(namespace_id);
        server
            .create_with_window(&windowed, Some(NAMESPACE_WINDOW_MS))
            .await;
        server.create_with_window(&sibling, None).await;
        let seqs = seed_two_commits(&server, &[&windowed, &sibling]).await;

        server.scheduler.run_sweep().await.expect("list namespaces");
        assert_eq!(
            server.retention_floor(&windowed).await,
            seqs[0][1],
            "server window {server_window_ms:?}: the namespace's own window applies, with or \
             without the server's"
        );
        let sibling_floor = match server_window_ms {
            Some(_) => seqs[1][0],
            None => CommitSeq(0),
        };
        assert_eq!(
            server.retention_floor(&sibling).await,
            sibling_floor,
            "server window {server_window_ms:?}: a namespace created without a window has the \
             server's, and keeps its floor when the server has none"
        );
    }
}

#[tokio::test]
async fn each_sweep_uses_the_window_that_the_namespace_has_when_the_sweep_runs() {
    let directory = tempdir().expect("tempdir");
    let server = SchedulerServer::start(
        local_store(directory.path()),
        &retention_config(Some(WINDOW_MS)),
        START_MS,
    )
    .await;
    let namespace_id = namespace_id("changing");
    server.create_with_window(&namespace_id, None).await;
    let [_, second] = seed_two_commits(&server, &[&namespace_id]).await[0];

    // A 50-day window passes neither commit, a cleared window keeps the
    // floor although the server's 30-day default would pass the first, and a
    // 15-day window passes both.
    for (window_ms, expected_floor) in [
        (Some(50 * DAY_MS), CommitSeq(0)),
        (None, CommitSeq(0)),
        (Some(NAMESPACE_WINDOW_MS), second),
    ] {
        server
            .maintenance
            .set_retention_window(&namespace_id, window_ms.and_then(NonZeroU64::new))
            .await
            .expect("set the namespace's window");
        server.scheduler.run_sweep().await.expect("list namespaces");
        assert_eq!(
            server.retention_floor(&namespace_id).await,
            expected_floor,
            "namespace window {window_ms:?}"
        );
    }
}

#[tokio::test]
async fn session_steps_never_advance_the_floor_and_the_next_sweep_does() {
    let directory = tempdir().expect("tempdir");
    let config = retention_config(Some(WINDOW_MS));
    let server = SchedulerServer::start(local_store(directory.path()), &config, START_MS).await;
    let namespace_id = namespace_id("held");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold the session");
    let seq = held
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    let floor = server.diagnostics(&namespace_id).await.retention_floor_seq;

    server.run_steps_after(idle_fold_after_ms(&config)).await;
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "a metadata step folded the tail"
    );
    server.run_steps_after(2 * WINDOW_MS).await;
    for _ in 0..3 {
        server
            .run_steps_after(config.maintenance_interval_ms.get())
            .await;
    }
    let entry = server
        .scheduler
        .inner
        .namespaces
        .entry(&namespace_id)
        .expect("a held entry");
    assert_eq!(
        lock(&entry).host_state.collected_seq,
        Some(seq),
        "the session steps on the collection interval collected the namespace"
    );
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        floor,
        "no session step advances the floor"
    );

    server.scheduler.run_sweep().await.expect("list namespaces");
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the sweep advances it"
    );
    drop(held);
}

#[tokio::test]
async fn a_sweep_that_finds_session_steps_running_leaves_its_advance_to_those_steps() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("busy");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id)),
        OperationSelector::Any,
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SchedulerServer::start(store.clone(), &config, START_MS).await;
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold the session");
    let seq = held
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    let floor = server.diagnostics(&namespace_id).await.retention_floor_seq;
    server.run_steps_after(idle_fold_after_ms(&config)).await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    server.clock.advance_ms(2 * WINDOW_MS);

    // The session's collection parks on its first request, so the session
    // steps have the namespace for the whole sweep.
    store.block_next();
    let steps = tokio::spawn({
        let scheduler = server.scheduler.clone();
        async move { scheduler.run_due_steps().await }
    });
    store.wait_until_blocked().await;
    let sweep_counts = || {
        (
            server.counter("loonfs.maintenance.sweeps", &[("result", "ok")]),
            server.retention_failures(),
        )
    };
    assert_eq!(
        server.scheduler.run_sweep().await.expect("list namespaces"),
        1,
        "the sweep lists the namespace that the session steps have"
    );
    assert_eq!(sweep_counts(), (1, 0));
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        floor,
        "the sweep doesn't wait for the session steps"
    );

    store.release();
    steps.await.expect("finish the session steps");
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the session steps make the advance that the sweep left to them"
    );
    assert_eq!(sweep_counts(), (1, 0), "the sweep's counts don't change");
    drop(held);
}

#[tokio::test]
async fn a_failing_advance_fails_once_per_sweep_until_it_succeeds() {
    let directory = tempdir().expect("tempdir");
    // The advance sends a HEAD request for each segment before it publishes,
    // and no other call in a sweep that doesn't collect sends one.
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Head,
        InjectedError::PermissionDenied("expired credentials".to_owned()),
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SchedulerServer::start(store.clone(), &config, START_MS).await;
    let namespace_id = namespace_id("failing");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let seq = server.commit(&namespace_id, "/old.txt").await;
    server
        .maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the history");
    let floor = server.diagnostics(&namespace_id).await.retention_floor_seq;
    server.clock.advance_ms(2 * WINDOW_MS);
    store.fail_all();

    for sweeps in 1..=3 {
        server.sweep_after(config.sweep_interval_ms.get()).await;
        assert_eq!(
            server.retention_failures(),
            sweeps,
            "each sweep makes the advance once, so it records and logs one failure"
        );
        assert_eq!(
            server.diagnostics(&namespace_id).await.retention_floor_seq,
            floor
        );
    }
    assert_eq!(
        server.call_failures(MaintenanceCall::Metadata),
        0,
        "only the advance failed"
    );

    store.clear();
    server.sweep_after(config.sweep_interval_ms.get()).await;
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the next sweep retries the advance"
    );
    assert_eq!(server.retention_failures(), 3);
}

#[tokio::test]
async fn an_idle_sweep_costs_the_same_requests_whichever_window_applies() {
    let mut sweeps = Vec::new();
    for (server_window_ms, namespace_window_ms) in [
        (None, None),
        (Some(WINDOW_MS), None),
        (None, Some(WINDOW_MS)),
    ] {
        let directory = tempdir().expect("tempdir");
        let base: SharedObjectStore = local_store(directory.path());
        let namespace_id = namespace_id("idle");
        let prefix = loonfs_objectstore::keys::namespace_prefix(&namespace_id);
        let (writer, now_ms) = seed_writer(&base).await;
        writer
            .create_namespace_with_options(
                &namespace_id,
                &test_actor(),
                &CreateNamespaceOptions {
                    retention_window_ms: namespace_window_ms.and_then(NonZeroU64::new),
                    ..CreateNamespaceOptions::default()
                },
            )
            .await
            .expect("create the namespace");
        writer
            .open_namespace(&namespace_id)
            .expect("open the namespace")
            .put_file("/old.txt", b"history", &test_actor())
            .await
            .expect("write a file");
        writer.drain().await.expect("drain");
        writer
            .maintenance(writer_id("before-the-server"))
            .fold_wal(&namespace_id)
            .await
            .expect("fold the history");
        writer.shutdown().await.expect("stop the writer");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("local store"),
            KeyPredicate::any(),
        ));
        let server = SchedulerServer::start(
            store.clone(),
            &retention_config(server_window_ms),
            now_ms + 2 * WINDOW_MS,
        )
        .await;
        // The first sweep moves the floor to the head when a window applies,
        // which creates a floor object and folds it, and it collects
        // what the seed left behind. Each sweep collects the manifests that
        // the previous sweep replaced.
        for _ in 0..3 {
            server.scheduler.run_sweep().await.expect("list namespaces");
        }
        let windows = format!("server {server_window_ms:?}, namespace {namespace_window_ms:?}");

        store.reset();
        server.scheduler.run_sweep().await.expect("list namespaces");
        let sweep: Vec<_> = store
            .take()
            .into_iter()
            .filter(|operation| operation.key().starts_with(&prefix))
            .collect();
        assert!(
            sweep.iter().all(|operation| matches!(
                operation,
                RecordedOperation::Get { .. }
                    | RecordedOperation::GetWithMetadata { .. }
                    | RecordedOperation::Head { .. }
                    | RecordedOperation::List { .. }
            )),
            "{windows}: an idle sweep writes nothing: {sweep:?}"
        );

        // The advance's share of the sweep is one discovery of the current
        // manifest and the WAL tail after it.
        store.reset();
        let target = RetentionTarget::Window;
        server
            .maintenance
            .advance_retention_floor_with_options(
                &namespace_id,
                &AdvanceRetentionFloorOptions { target },
            )
            .await
            .expect("advance the floor");
        let advance = store.take();
        assert!(advance
            .iter()
            .all(|operation| operation.key().starts_with(&prefix)));
        assert_eq!(advance.len(), IDLE_ADVANCE_REQUESTS, "{windows}");
        sweeps.push(sweep.len());
    }
    assert!(
        sweeps.windows(2).all(|pair| pair[0] == pair[1]),
        "every sweep makes the advance, whichever window applies: {sweeps:?}"
    );
}

/// The writes of one namespace in `operations`: the manifests and the WAL
/// objects that they put.
fn manifest_and_wal_puts(
    operations: &[RecordedOperation],
    namespace_id: &NamespaceId,
) -> (usize, usize) {
    let puts = |prefix: String| {
        operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    RecordedOperation::Put { .. } | RecordedOperation::PutStreamed { .. }
                ) && operation.key().starts_with(&prefix)
            })
            .count()
    };
    (
        puts(loonfs_objectstore::keys::metadata_manifest_prefix(
            namespace_id,
        )),
        puts(loonfs_objectstore::keys::wal_prefix(namespace_id)),
    )
}

/// The writer epoch of the namespace's current manifest.
async fn writer_epoch(
    store: &dyn loonfs_objectstore::ObjectStore,
    namespace_id: &NamespaceId,
) -> u64 {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .writer_epoch
        .0
}

/// Checks the three ways a sweep's advance runs. A namespace with a live
/// session gets a floor object from that session's queue, a namespace with
/// no session gets a floor object from the sweep without a session, and each
/// is folded once, with no claim and no new writer epoch. A namespace whose
/// floor would not move gets no write.
#[tokio::test]
async fn a_sweeps_advance_creates_a_floor_object_with_or_without_a_session() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SchedulerServer::start(store.clone(), &config, START_MS).await;
    let [held_id, idle_id, recent_id] = ["held", "idle", "recent"].map(namespace_id);
    for namespace_id in [&held_id, &idle_id, &recent_id] {
        server
            .runtime
            .create_namespace(namespace_id, &test_actor())
            .await
            .expect("create the namespace");
    }
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&held_id)
        .await
        .expect("hold the session");
    let held_seq = held
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    let idle_seq = server.commit(&idle_id, "/old.txt").await;
    for namespace_id in [&held_id, &idle_id] {
        server
            .maintenance
            .fold_wal(namespace_id)
            .await
            .expect("fold the history");
    }
    server.clock.advance_ms(2 * WINDOW_MS);
    server.commit(&recent_id, "/recent.txt").await;
    server
        .maintenance
        .fold_wal(&recent_id)
        .await
        .expect("fold the history");
    let epochs_before = [
        writer_epoch(store.as_ref(), &held_id).await,
        writer_epoch(store.as_ref(), &idle_id).await,
    ];
    let recent_floor = server.retention_floor(&recent_id).await;
    // Only the held namespace has a session.
    server.wait_for_open_sessions(1).await;

    store.reset();
    server.scheduler.run_sweep().await.expect("list namespaces");
    let operations = store.take();

    assert_eq!(server.retention_floor(&held_id).await, held_seq);
    assert_eq!(
        writer_epoch(store.as_ref(), &held_id).await,
        epochs_before[0],
        "the held session keeps its epoch"
    );
    assert_eq!(
        manifest_and_wal_puts(&operations, &held_id),
        (1, 1),
        "one floor object and the fold that records the floor"
    );

    assert_eq!(server.retention_floor(&idle_id).await, idle_seq);
    assert_eq!(
        writer_epoch(store.as_ref(), &idle_id).await,
        epochs_before[1],
        "the advance claims no epoch"
    );
    assert_eq!(
        manifest_and_wal_puts(&operations, &idle_id),
        (1, 1),
        "one floor object and the fold that records the floor"
    );

    assert_eq!(server.retention_floor(&recent_id).await, recent_floor);
    assert_eq!(
        manifest_and_wal_puts(&operations, &recent_id),
        (0, 0),
        "a floor that would not move writes nothing"
    );
    // The advance opened no session for the idle namespace.
    server.wait_for_open_sessions(1).await;
    drop(held);
}
