//! What one sweep pass and one index pass do to the namespaces in a store.

#![allow(clippy::panic)]

use super::Sweep;
use crate::config::{GrepConfig, GrepMode, ServerConfig};
use futures::TryStreamExt as _;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};
use loonfs::{LoonFs, Maintenance, NamespaceId, SharedObjectStore, Writable};
use loonfs_grep::manifest::GrepIndexStatus;
use loonfs_grep::{GrepWorker, GrepWorkerConfig};
use loonfs_http::Namespaces;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_test_support::ids::{namespace_id, page_limit, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, OperationKind,
    RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use loonfs_types::EffectiveLimit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

const MINUTE_MS: u64 = 60 * 1_000;
const DAY_MS: u64 = 24 * 60 * MINUTE_MS;

#[derive(Debug)]
struct SettableWallClock(AtomicU64);

impl SettableWallClock {
    fn advance_ms(&self, elapsed_ms: u64) {
        self.0.fetch_add(elapsed_ms, Ordering::SeqCst);
    }
}

impl loonfs::WallClock for SettableWallClock {
    fn now_ms(&self) -> Result<u64, loonfs::CoreError> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

/// A server's runtime and sweep over `store`, reading `clock`.
struct SweepServer {
    clock: Arc<SettableWallClock>,
    recorder: Arc<DefaultMetricsRecorder>,
    runtime: LoonFs<Writable>,
    maintenance: Maintenance,
    grep_worker: Option<GrepWorker<SharedObjectStore>>,
    sweep: Sweep,
}

impl SweepServer {
    async fn start(store: SharedObjectStore, config: &ServerConfig, now_ms: u64) -> Self {
        let clock = Arc::new(SettableWallClock(AtomicU64::new(now_ms)));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let runtime = LoonFs::builder_with_store(store)
            .writer_id("sweep-server")
            .wall_clock(clock.clone())
            .metrics_recorder(recorder.clone() as Arc<dyn MetricsRecorder>)
            .build()
            .await
            .expect("build the server runtime");
        let maintenance = runtime.maintenance(writer_id("sweep-server-maintenance"));
        let grep_worker = config.grep.mode.maintains_index().then(|| {
            GrepWorker::new(
                runtime.object_store(),
                runtime.read_only(),
                maintenance.clone(),
                loonfs_grep::DEFAULT_MAX_CONCURRENT_GREP_STEPS,
            )
        });
        let sweep = Sweep::new(
            config,
            runtime.object_store(),
            maintenance.clone(),
            Arc::new(Namespaces::new(runtime.clone())),
            grep_worker.clone(),
            recorder.as_ref(),
        )
        .expect("build the sweep");
        Self {
            clock,
            recorder,
            runtime,
            maintenance,
            grep_worker,
            sweep,
        }
    }

    fn page_limit(mut self, page_limit: EffectiveLimit) -> Self {
        self.sweep = self.sweep.page_limit(page_limit);
        self
    }

    async fn wal_tail_objects(&self, namespace_id: &NamespaceId) -> u64 {
        self.maintenance
            .diagnostics(namespace_id)
            .await
            .expect("namespace diagnostics")
            .wal_tail_objects
    }

    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let snapshot = self.recorder.snapshot();
        let Some(entry) = snapshot
            .by_name(name)
            .find(|entry| entry.labels.as_slice() == labels)
        else {
            return 0;
        };
        match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("expected `{name}` to be a counter, found {other:?}"),
        }
    }

    async fn grep_status(&self, namespace_id: &NamespaceId) -> GrepIndexStatus {
        self.grep_worker
            .as_ref()
            .expect("a grep-maintaining server")
            .lifecycle(namespace_id)
            .await
            .expect("read the grep lifecycle")
    }
}

/// A config with every default, and `grep` maintained when asked.
fn sweep_config(grep: Option<GrepWorkerConfig>) -> ServerConfig {
    let mut config: ServerConfig = toml::from_str(
        r#"
bind = "127.0.0.1:0"
writer_id = "sweep-server"
content_token_secret = "sweep-test-secret"

[store]
kind = "local-fs"
root = "unused"
"#,
    )
    .expect("parse the sweep test config");
    if let Some(worker) = grep {
        config.grep = GrepConfig {
            mode: GrepMode::ServeAndMaintain,
            worker,
        };
    }
    config
}

/// A writer that ran before the server started, on the real clock.
async fn seed_writer(store: &SharedObjectStore) -> (LoonFs<Writable>, u64) {
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("before-the-server")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build the seed writer");
    let now_ms = writer.now_ms().expect("read the clock");
    (writer, now_ms)
}

/// Creates a namespace whose WAL tail holds one unfolded commit.
async fn seed_unfolded_tail(writer: &LoonFs<Writable>, namespace_id: &NamespaceId) {
    writer
        .create_namespace(namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    writer
        .open_namespace(namespace_id)
        .expect("open the namespace")
        .put_file("/tail.txt", b"unfolded", &test_actor())
        .await
        .expect("write the tail");
}

async fn keys_under(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    store
        .list_prefix_stream(prefix)
        .try_collect()
        .await
        .expect("list keys")
}

fn local_store(root: &std::path::Path) -> Arc<LocalFsStore> {
    Arc::new(LocalFsStore::new(root).expect("local store"))
}

#[tokio::test]
async fn a_restart_then_one_sweep_folds_an_idle_tail_and_collects_garbage() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = local_store(directory.path());
    let namespace_id = namespace_id("idle-tail");
    let (writer, now_ms) = seed_writer(&store).await;
    seed_unfolded_tail(&writer, &namespace_id).await;
    writer
        .maintenance(writer_id("before-the-server"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold the first commit");
    writer
        .open_namespace(&namespace_id)
        .expect("open the namespace")
        .put_file("/second.txt", b"left unfolded", &test_actor())
        .await
        .expect("write a second commit");
    writer.shutdown().await.expect("stop the writer");
    drop(writer);

    let server = SweepServer::start(store.clone(), &sweep_config(None), now_ms).await;
    let tail = server.wal_tail_objects(&namespace_id).await;
    assert!(tail > 0);
    server.sweep.run_pass(true).await.expect("list namespaces");
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        tail,
        "a tail that is not yet idle stays unfolded"
    );
    assert_eq!(
        server.counter("loonfs.gc.reclaimed", &[("category", "deleted_manifests")]),
        0,
        "nothing is past its grace yet"
    );

    server.clock.advance_ms(2 * DAY_MS);
    server.sweep.run_pass(true).await.expect("list namespaces");
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    assert!(
        server.counter("loonfs.gc.reclaimed", &[("category", "deleted_manifests")]) > 0,
        "a superseded manifest past its grace is collected"
    );
    assert_eq!(
        server
            .runtime
            .namespace(&namespace_id)
            .read_file("/second.txt")
            .await
            .expect("read the folded file")
            .bytes,
        b"left unfolded"
    );
}

#[tokio::test]
async fn a_sweep_retires_a_deleted_namespace_after_its_grace() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = local_store(directory.path());
    let namespace_id = namespace_id("deleted");
    let (writer, now_ms) = seed_writer(&store).await;
    seed_unfolded_tail(&writer, &namespace_id).await;
    writer
        .open_namespace(&namespace_id)
        .expect("open the namespace")
        .delete()
        .await
        .expect("delete the namespace");
    writer.shutdown().await.expect("stop the writer");
    let content_prefix = loonfs_objectstore::keys::content_prefix(&namespace_id);
    assert!(!keys_under(store.as_ref(), &content_prefix).await.is_empty());

    let server = SweepServer::start(store.clone(), &sweep_config(None), now_ms).await;
    server.sweep.run_pass(true).await.expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &content_prefix).await.is_empty(),
        "content outlives the deletion until the retirement grace passes"
    );

    server.clock.advance_ms(DAY_MS);
    assert_eq!(
        server.sweep.run_pass(true).await.expect("list namespaces"),
        1,
        "a deleted namespace is still listed"
    );
    assert_eq!(
        keys_under(store.as_ref(), &content_prefix).await,
        Vec::<String>::new()
    );
    assert_eq!(
        server.counter(
            "loonfs.maintenance.sweep_visit_failures",
            &[("call", "metadata")]
        ),
        0,
        "a deleted namespace is not a failed visit"
    );
}

#[tokio::test]
async fn grep_gc_runs_in_a_sweep() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = local_store(directory.path());
    let namespace_id = namespace_id("grep-collected");
    let (writer, now_ms) = seed_writer(&store).await;
    seed_unfolded_tail(&writer, &namespace_id).await;
    writer.shutdown().await.expect("stop the writer");
    let server = SweepServer::start(
        store.clone(),
        &sweep_config(Some(GrepWorkerConfig::default())),
        now_ms,
    )
    .await;
    let worker = server.grep_worker.as_ref().expect("grep worker");
    worker.enable(&namespace_id).await.expect("enable grep");
    server.sweep.run_pass(false).await.expect("list namespaces");
    assert!(matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Active { .. }
    ));
    worker.disable(&namespace_id).await.expect("disable grep");
    let segments_prefix = loonfs_grep::keyspace::segments_prefix(&namespace_id);
    assert!(!keys_under(store.as_ref(), &segments_prefix)
        .await
        .is_empty());

    server.sweep.run_pass(true).await.expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &segments_prefix)
            .await
            .is_empty(),
        "a disabled index keeps its segments until the grace passes"
    );
    server.clock.advance_ms(2 * DAY_MS);
    server.sweep.run_pass(true).await.expect("list namespaces");
    assert_eq!(
        keys_under(store.as_ref(), &segments_prefix).await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_sweep_drives_a_grep_backfill_to_active() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = local_store(directory.path());
    let namespace_id = namespace_id("backfilled");
    let (writer, now_ms) = seed_writer(&store).await;
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let session = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let files = super::MAX_GREP_BUILD_STEPS_PER_VISIT + 4;
    for index in 0..files {
        session
            .put_file(
                &format!("/file-{index}.txt"),
                format!("needle {index}\n").as_bytes(),
                &test_actor(),
            )
            .await
            .expect("write a file");
    }
    writer.shutdown().await.expect("stop the writer");
    let server = SweepServer::start(
        store,
        &sweep_config(Some(GrepWorkerConfig {
            max_files_per_step: 1,
            ..GrepWorkerConfig::default()
        })),
        now_ms,
    )
    .await;
    server
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");

    server.sweep.run_pass(false).await.expect("list namespaces");
    assert!(
        matches!(
            server.grep_status(&namespace_id).await,
            GrepIndexStatus::Backfilling { .. }
        ),
        "one visit runs at most {} build steps",
        super::MAX_GREP_BUILD_STEPS_PER_VISIT
    );
    server.sweep.run_pass(false).await.expect("list namespaces");
    let status = server.grep_status(&namespace_id).await;
    let watermark = status
        .active_watermark()
        .unwrap_or_else(|| panic!("the next pass finishes the backfill, found {status:?}"));
    assert_eq!(
        watermark.built_through_seq(),
        loonfs::ChangeSeq(u64::try_from(files).expect("file count fits"))
    );
}

#[tokio::test]
async fn a_sweep_survives_a_failing_namespace() {
    let directory = tempdir().expect("tempdir");
    let base: SharedObjectStore = local_store(directory.path());
    let names = ["alpha", "beta", "gamma"].map(namespace_id);
    let (writer, now_ms) = seed_writer(&base).await;
    for namespace_id in &names {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("stop the writer");
    let failing_prefix = loonfs_objectstore::keys::namespace_prefix(&names[1]);
    let store = Arc::new(FailStore::matching(
        LocalFsStore::new(directory.path()).expect("local store"),
        move |operation| operation.key().starts_with(&failing_prefix),
        InjectedError::Transport("injected for one namespace".to_owned()),
    ));
    store.fail_all();
    let server =
        SweepServer::start(store.clone(), &sweep_config(None), now_ms + 20 * MINUTE_MS).await;

    assert_eq!(
        server.sweep.run_pass(false).await.expect("list namespaces"),
        3
    );
    store.clear();
    assert_eq!(server.wal_tail_objects(&names[0]).await, 0);
    assert!(server.wal_tail_objects(&names[1]).await > 0);
    assert_eq!(server.wal_tail_objects(&names[2]).await, 0);
    assert_eq!(
        server.counter(
            "loonfs.maintenance.sweep_visit_failures",
            &[("call", "metadata")]
        ),
        1
    );

    server.sweep.run_pass(false).await.expect("list namespaces");
    assert_eq!(
        server.wal_tail_objects(&names[1]).await,
        0,
        "the next pass tries the failed namespace again"
    );
}

#[tokio::test]
async fn a_parked_visit_does_not_hold_up_later_pages() {
    let directory = tempdir().expect("tempdir");
    let base: SharedObjectStore = local_store(directory.path());
    let parked = namespace_id("a-parked");
    let later = ["b-later", "c-later", "d-later", "e-later", "f-later"].map(namespace_id);
    let (writer, now_ms) = seed_writer(&base).await;
    for namespace_id in std::iter::once(&parked).chain(&later) {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("stop the writer");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&parked)),
        OperationClass::Any,
    ));
    let mut config = sweep_config(None);
    config.max_concurrent_maintenance = 2;
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(2));

    store.block_next();
    let pass = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.run_pass(false).await }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        store.wait_until_blocked().await;
        for namespace_id in &later {
            while server.wal_tail_objects(namespace_id).await > 0 {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .expect("every namespace on the later pages is visited while the first visit is parked");
    assert!(!pass.is_finished(), "the pass waits for the parked visit");

    store.release();
    assert_eq!(
        pass.await.expect("join the pass").expect("list namespaces"),
        1 + later.len()
    );
    assert_eq!(server.wal_tail_objects(&parked).await, 0);
}

#[tokio::test]
async fn a_listing_failure_on_a_later_page_lets_started_visits_finish_and_fails_the_pass() {
    let directory = tempdir().expect("tempdir");
    let base: SharedObjectStore = local_store(directory.path());
    let [parked, started, unlisted] = ["a-parked", "b-started", "c-unlisted"].map(namespace_id);
    let (writer, now_ms) = seed_writer(&base).await;
    for namespace_id in [&parked, &started, &unlisted] {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("stop the writer");
    let listings = AtomicUsize::new(0);
    let failing = FailStore::matching(
        LocalFsStore::new(directory.path()).expect("local store"),
        move |operation| {
            matches!(operation.kind(), OperationKind::List)
                && operation.key() == "namespaces/"
                && listings.fetch_add(1, Ordering::SeqCst) == 2
        },
        InjectedError::Transport("injected for the third page".to_owned()),
    );
    failing.fail_all();
    let store = Arc::new(BlockingStore::new(
        failing,
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&parked)),
        OperationClass::Any,
    ));
    let mut config = sweep_config(None);
    config.max_concurrent_maintenance = 2;
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(1));

    store.block_next();
    let pass = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.run_pass(false).await }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        store.wait_until_blocked().await;
        while store.inner().attempts() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the third listing fails while the first visit is parked");
    assert!(
        !pass.is_finished(),
        "the pass waits for the visit it started"
    );

    store.release();
    let error = pass
        .await
        .expect("join the pass")
        .expect_err("a failed listing fails the pass");
    assert!(
        matches!(error, ObjectStoreError::Transport { .. }),
        "expected the injected listing failure, got {error:?}"
    );
    assert_eq!(
        server.wal_tail_objects(&parked).await,
        0,
        "the parked visit finishes before the pass returns"
    );
    assert_eq!(server.wal_tail_objects(&started).await, 0);
    assert!(
        server.wal_tail_objects(&unlisted).await > 0,
        "no visit starts after the failed listing"
    );
    assert_eq!(
        server.counter("loonfs.maintenance.sweep_passes", &[("result", "error")]),
        1
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grep {
    NotMaintained,
    Maintained,
    Enabled,
}

/// Requests one idle namespace costs: a visit that does not collect
/// garbage, then one that does, and how many of the latter are lists. The
/// docs quote these numbers.
const IDLE_VISIT_REQUESTS: [(Grep, usize, usize, usize); 3] = [
    (Grep::NotMaintained, 6, 19, 7),
    (Grep::Maintained, 7, 29, 9),
    (Grep::Enabled, 26, 47, 9),
];

#[tokio::test]
async fn an_idle_pass_costs_one_listing_per_page_and_a_fixed_number_of_requests_per_namespace() {
    for (grep, visit, collecting_visit, collecting_lists) in IDLE_VISIT_REQUESTS {
        let directory = tempdir().expect("tempdir");
        let base: SharedObjectStore = local_store(directory.path());
        let names = ["first", "second", "third"].map(namespace_id);
        let (writer, now_ms) = seed_writer(&base).await;
        let seed_maintenance = writer.maintenance(writer_id("before-the-server"));
        for namespace_id in &names {
            seed_unfolded_tail(&writer, namespace_id).await;
            seed_maintenance
                .fold_wal(namespace_id)
                .await
                .expect("fold the tail");
        }
        writer.shutdown().await.expect("stop the writer");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("local store"),
            KeyPredicate::any(),
        ));
        let config = sweep_config((grep != Grep::NotMaintained).then(GrepWorkerConfig::default));
        let server = SweepServer::start(store.clone(), &config, now_ms + 2 * DAY_MS).await;
        if grep == Grep::Enabled {
            for namespace_id in &names {
                server
                    .grep_worker
                    .as_ref()
                    .expect("grep worker")
                    .enable(namespace_id)
                    .await
                    .expect("enable grep");
            }
        }
        let sweep = server.sweep.page_limit(page_limit(2));
        // The first pass collects what the seed left behind, so the passes
        // below find nothing to collect and nothing inside its grace.
        sweep.run_pass(true).await.expect("list namespaces");

        for (collect_garbage, per_namespace, lists_per_namespace) in [
            (false, visit, 0),
            (true, collecting_visit, collecting_lists),
        ] {
            store.reset();
            sweep
                .run_pass(collect_garbage)
                .await
                .expect("list namespaces");
            let operations = store.take();
            let listings = operations
                .iter()
                .filter(|operation| {
                    matches!(operation, RecordedOperation::List { .. })
                        && operation.key() == "namespaces/"
                })
                .count();
            assert_eq!(listings, 2, "three namespaces at two per page");
            for namespace_id in &names {
                let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
                let visit: Vec<_> = operations
                    .iter()
                    .filter(|operation| operation.key().starts_with(&prefix))
                    .collect();
                let lists = visit
                    .iter()
                    .filter(|operation| matches!(operation, RecordedOperation::List { .. }))
                    .count();
                assert_eq!(
                    (visit.len(), lists),
                    (per_namespace, lists_per_namespace),
                    "requests and lists for `{namespace_id}`, grep {grep:?}, collect_garbage \
                     {collect_garbage}"
                );
            }
            assert_eq!(operations.len(), listings + names.len() * per_namespace);
        }
    }
}

#[tokio::test]
async fn a_stray_child_of_the_namespace_prefix_is_not_a_failed_visit() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    for stray in ["namespaces/Not-An-Id/hint.json", "namespaces/stray/orphan"] {
        store
            .put_overwrite(stray, bytes::Bytes::from_static(b"stray"))
            .await
            .expect("write a stray key");
    }
    let server = SweepServer::start(
        store.clone(),
        &sweep_config(Some(GrepWorkerConfig::default())),
        0,
    )
    .await;
    store.reset();

    assert_eq!(
        server.sweep.run_pass(true).await.expect("list namespaces"),
        1,
        "a child whose name is not a namespace id is skipped"
    );
    let operations = store.take();
    assert!(!operations
        .iter()
        .any(|operation| operation.key().starts_with("namespaces/Not-An-Id/")));
    let stray: Vec<_> = operations
        .iter()
        .filter(|operation| operation.key().starts_with("namespaces/stray/"))
        .collect();
    // One hint read each for the metadata call, the grep build, and
    // collection; grep collection reads the hint and lists its own prefix.
    assert_eq!(stray.len(), 5, "{stray:?}");
    for call in ["metadata", "grep_index", "gc", "grep_gc"] {
        assert_eq!(
            server.counter("loonfs.maintenance.sweep_visit_failures", &[("call", call)]),
            0,
            "a namespace id with no namespace behind it is not a `{call}` failure"
        );
    }
}

#[tokio::test]
async fn the_index_pass_indexes_a_held_session_and_reads_nothing_while_it_is_idle() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.store = crate::StoreConfig::LocalFs {
        root: directory.path().display().to_string(),
        key_prefix: None,
    };
    let (_router, state) = crate::app(
        config,
        crate::AppOptions {
            store: Some(store.clone()),
            direct_transfers: None,
        },
    )
    .await
    .expect("build the app");
    let sweep = state.sweep.as_ref().expect("a maintaining server");
    let namespace_id = namespace_id("held");
    state
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let held = state
        .binding
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold the namespace");
    let worker = state.binding.grep_worker.as_ref().expect("grep worker");
    worker.enable(&namespace_id).await.expect("enable grep");
    sweep.run_pass(false).await.expect("list namespaces");

    let commit = held
        .put_file("/note.txt", b"held needle\n", &test_actor())
        .await
        .expect("write through the held session");
    assert_eq!(held.last_published_seq(), Some(commit.committed_seq));
    sweep.run_index_pass().await;
    let status = worker
        .lifecycle(&namespace_id)
        .await
        .expect("read the grep lifecycle");
    assert_eq!(
        status
            .active_watermark()
            .expect("an active index")
            .built_through_seq(),
        commit.committed_seq,
        "the commit is indexed without a sweep pass"
    );

    store.reset();
    sweep.run_index_pass().await;
    assert_eq!(
        store.take(),
        Vec::new(),
        "an idle held session costs no store request"
    );
    state.runtime.shutdown().await.expect("stop the runtime");
}
