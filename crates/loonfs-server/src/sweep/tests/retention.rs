//! The retention advance that full passes make under a retention window.

use super::{local_store, seed_writer, sweep_config, SweepServer, DAY_MS};
use crate::config::ServerConfig;
use crate::sweep::{lock, GarbageCollection, SweepCall};
use loonfs::{ChangeSeq, NamespaceDiagnostics, NamespaceId, SharedObjectStore};
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
const START_MS: u64 = 100 * DAY_MS;

/// The 30-day window from the design note.
const WINDOW_MS: u64 = 30 * DAY_MS;

/// Requests the advance adds to the visit of an idle namespace whose floor
/// already reached the head: one load of the current manifest. The docs
/// quote this number.
const IDLE_ADVANCE_REQUESTS: usize = 4;

/// A config with every default, and `window_ms` as its retention window.
fn retention_config(window_ms: Option<u64>) -> ServerConfig {
    let mut config = sweep_config(None);
    config.retention_window_ms =
        window_ms.map(|window_ms| NonZeroU64::new(window_ms).expect("a positive window"));
    config
}

impl SweepServer {
    async fn diagnostics(&self, namespace_id: &NamespaceId) -> NamespaceDiagnostics {
        self.maintenance
            .diagnostics(namespace_id)
            .await
            .expect("namespace diagnostics")
    }

    fn retention_failures(&self) -> u64 {
        self.visit_failures(SweepCall::Retention)
    }

    /// Writes one file on the server's clock, and returns its commit's
    /// sequence.
    async fn commit(&self, namespace_id: &NamespaceId, path: &str) -> ChangeSeq {
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
/// namespace that the pass also lists.
struct History {
    /// A namespace with two commits older than the window and one inside it.
    aged: NamespaceId,
    /// The newest commit of `aged` that is older than the window.
    aged_cutoff_seq: ChangeSeq,
    /// A namespace whose every commit is inside the window.
    recent: NamespaceId,
}

/// Commits and folds the history on the server's clock, which then reads
/// 40 days after `START_MS`. With a 30-day window, the cutoff is 10 days
/// after `START_MS`.
async fn seed_history(server: &SweepServer) -> History {
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
async fn a_full_pass_advances_each_floor_to_the_last_commit_before_the_window() {
    for window_ms in [None, Some(WINDOW_MS)] {
        let directory = tempdir().expect("tempdir");
        let server = SweepServer::start(
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

        server
            .sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("list namespaces");
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

#[tokio::test]
async fn ticks_never_advance_the_floor_and_the_next_full_pass_does() {
    let directory = tempdir().expect("tempdir");
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(local_store(directory.path()), &config, START_MS).await;
    let namespace_id = namespace_id("held");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let held = server
        .sweep
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

    server.tick_after(2 * WINDOW_MS).await;
    for _ in 0..3 {
        server.tick_after(config.tick_interval_ms).await;
    }
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "a tick folded the tail"
    );
    let entry = server
        .sweep
        .inner
        .namespaces
        .entry(&namespace_id)
        .expect("a held entry");
    assert_eq!(
        lock(&entry).collected_seq,
        Some(seq),
        "a tick on the collection interval visited the namespace"
    );
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        floor,
        "no tick advances the floor"
    );

    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("list namespaces");
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the full pass advances it"
    );
    drop(held);
}

#[tokio::test]
async fn a_pass_that_finds_a_tick_visiting_leaves_its_advance_to_that_visit() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("busy");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id)),
        OperationSelector::Any,
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(store.clone(), &config, START_MS).await;
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let held = server
        .sweep
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
    server.clock.advance_ms(2 * WINDOW_MS);

    // The tick's idle fold parks on its first request, so the tick has the
    // namespace for the whole pass.
    store.block_next();
    let tick = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.tick().await }
    });
    store.wait_until_blocked().await;
    let pass_counts = || {
        (
            server.counter("loonfs.maintenance.sweep_passes", &[("result", "ok")]),
            server.retention_failures(),
        )
    };
    assert_eq!(
        server
            .sweep
            .run_pass(GarbageCollection::Skip)
            .await
            .expect("list namespaces"),
        1,
        "the pass lists the namespace that the tick has"
    );
    assert_eq!(pass_counts(), (1, 0));
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        floor,
        "the pass doesn't wait for the tick's visit"
    );

    store.release();
    tick.await.expect("finish the tick");
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the tick's visit makes the advance that the pass left to it"
    );
    assert_eq!(pass_counts(), (1, 0), "the pass's counts don't change");
    drop(held);
}

#[tokio::test]
async fn a_failing_advance_fails_once_per_full_pass_until_it_succeeds() {
    let directory = tempdir().expect("tempdir");
    // The advance sends a HEAD request for each segment before it publishes,
    // and no other call on a pass that doesn't collect sends one.
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Head,
        InjectedError::PermissionDenied("expired credentials".to_owned()),
    ));
    let config = retention_config(Some(WINDOW_MS));
    let server = SweepServer::start(store.clone(), &config, START_MS).await;
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

    for passes in 1..=3 {
        server.pass_after(config.full_sweep_interval_ms).await;
        assert_eq!(
            server.retention_failures(),
            passes,
            "each full pass makes the advance once, so it records and logs one failure"
        );
        assert_eq!(
            server.diagnostics(&namespace_id).await.retention_floor_seq,
            floor
        );
    }
    assert_eq!(
        server.visit_failures(SweepCall::Metadata),
        0,
        "only the advance failed"
    );

    store.clear();
    server.pass_after(config.full_sweep_interval_ms).await;
    assert_eq!(
        server.diagnostics(&namespace_id).await.retention_floor_seq,
        seq,
        "the next full pass retries the advance"
    );
    assert_eq!(server.retention_failures(), 3);
}

#[tokio::test]
async fn an_idle_namespace_whose_floor_reached_the_head_costs_a_fixed_number_of_requests() {
    let mut visits = Vec::new();
    for window_ms in [None, Some(WINDOW_MS)] {
        let mut requests = Vec::new();
        let directory = tempdir().expect("tempdir");
        let base: SharedObjectStore = local_store(directory.path());
        let namespace_id = namespace_id("idle");
        let (writer, now_ms) = seed_writer(&base).await;
        writer
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create the namespace");
        writer
            .open_namespace(&namespace_id)
            .expect("open the namespace")
            .put_file("/old.txt", b"history", &test_actor())
            .await
            .expect("write a file");
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
        let server = SweepServer::start(
            store.clone(),
            &retention_config(window_ms),
            now_ms + 2 * WINDOW_MS,
        )
        .await;
        // The first pass moves the floor to the head and collects what the
        // seed left behind.
        server
            .sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("list namespaces");
        let diagnostics = server.diagnostics(&namespace_id).await;
        if window_ms.is_some() {
            assert_eq!(diagnostics.retention_floor_seq, diagnostics.head_seq);
        }

        for garbage in [GarbageCollection::Skip, GarbageCollection::Collect] {
            store.reset();
            server
                .sweep
                .run_pass(garbage)
                .await
                .expect("list namespaces");
            let prefix = loonfs_objectstore::keys::namespace_prefix(&namespace_id);
            let visit: Vec<_> = store
                .take()
                .into_iter()
                .filter(|operation| operation.key().starts_with(&prefix))
                .collect();
            assert!(
                visit.iter().all(|operation| matches!(
                    operation,
                    RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                        | RecordedOperation::Head { .. }
                        | RecordedOperation::List { .. }
                )),
                "window {window_ms:?}, garbage {garbage:?}: an idle visit writes nothing: \
                 {visit:?}"
            );
            requests.push(visit.len());
        }
        visits.push(requests);
    }
    for (without, with) in visits[0].iter().zip(&visits[1]) {
        assert_eq!(with - without, IDLE_ADVANCE_REQUESTS);
    }
}
