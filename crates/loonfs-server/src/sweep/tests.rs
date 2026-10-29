//! Daily listing, session ticks, and their store request costs.

use super::{GarbageCollection, GrepIndexing, Sweep, SweepCall, SweepRetry, SweepState};
use crate::config::{GrepConfig, JobMode, ServerConfig};
use futures::TryStreamExt as _;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};
use loonfs::{LoonFs, Maintenance, Namespace, NamespaceId, SharedObjectStore, Writable};
use loonfs_grep::manifest::GrepIndexStatus;
use loonfs_grep::{GrepWorker, GrepWorkerConfig};
use loonfs_http::{HeldNamespace, Namespaces, WriterSessions};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, page_limit, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationKind, OperationSelector,
    RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use loonfs_types::EffectiveLimit;
use std::assert_matches;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

const MINUTE_MS: u64 = 60 * 1_000;

/// Lists the ids of the sessions that `namespaces` holds.
fn held_ids(namespaces: &Namespaces<SweepState>) -> Vec<NamespaceId> {
    std::iter::successors(namespaces.next_held_id(None), |after| {
        namespaces.next_held_id(Some(after))
    })
    .collect()
}

/// Reads or updates the held entry of `namespace_id` in `namespaces`.
fn visit_held<T>(
    namespaces: &Namespaces<SweepState>,
    namespace_id: &NamespaceId,
    visit: impl FnOnce(&mut HeldNamespace<SweepState>) -> T,
) -> Option<T> {
    namespaces
        .entry(namespace_id)
        .map(|entry| visit(&mut super::lock(&entry)))
}
const DAY_MS: u64 = 24 * 60 * MINUTE_MS;

/// Returns the idle fold age, in milliseconds, of the runtime that `config`
/// builds, and zero when the runtime doesn't fold idle tails.
fn idle_fold_after_ms(config: &ServerConfig) -> u64 {
    config
        .runtime_config()
        .idle_fold_after_ms
        .map_or(0, std::num::NonZeroU64::get)
}

/// A server's runtime and sweep over `store`, reading `clock`.
struct SweepServer {
    clock: Arc<ManualClock>,
    recorder: Arc<DefaultMetricsRecorder>,
    runtime: LoonFs<Writable>,
    maintenance: Maintenance,
    grep_worker: Option<GrepWorker<SharedObjectStore>>,
    sweep: Sweep,
}

impl SweepServer {
    async fn start(store: SharedObjectStore, config: &ServerConfig, now_ms: u64) -> Self {
        let clock = Arc::new(ManualClock::new(now_ms));
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let runtime = LoonFs::builder_with_store(store)
            .writer_id(loonfs_test_support::ids::writer_id("sweep-server"))
            .config(loonfs::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..config.runtime_config()
            })
            .wall_clock(clock.clone())
            .monotonic_timer(clock.clone())
            .metrics_recorder(recorder.clone() as Arc<dyn MetricsRecorder>)
            .build()
            .await
            .expect("build the server runtime");
        let maintenance = runtime.maintenance(writer_id("sweep-server-maintenance"));
        let grep_worker = config.grep.mode.maintains().then(|| {
            GrepWorker::new(
                runtime.object_store(),
                runtime.read_only(),
                runtime.clone(),
                loonfs_grep::GrepPassBudget::default(),
            )
        });
        let grep = grep_worker.clone().map(|worker| {
            GrepIndexing::new(
                worker,
                config
                    .grep
                    .worker
                    .build_policy()
                    .expect("build the grep policy"),
            )
        });
        let sweep = Sweep::new(
            config.sweep_config(),
            runtime.object_store(),
            maintenance.clone(),
            Arc::new(Namespaces::new(runtime.clone())),
            runtime.monotonic_timer(),
            grep,
            recorder.as_ref(),
        );
        Self {
            clock,
            recorder,
            runtime,
            maintenance,
            grep_worker,
            sweep,
        }
    }

    async fn tick_after(&self, elapsed_ms: u64) {
        self.clock.advance_ms(elapsed_ms);
        self.sweep.tick().await;
    }

    async fn enable_index(&self, namespace_id: &NamespaceId) {
        let _handle = self
            .sweep
            .inner
            .namespaces
            .open(namespace_id)
            .await
            .expect("hold namespace");
        self.grep_worker
            .as_ref()
            .expect("grep worker")
            .enable(namespace_id)
            .await
            .expect("enable grep");
        self.sweep.inner.namespaces.mark_index_dirty(namespace_id);
    }

    async fn disable_index(&self, namespace_id: &NamespaceId) {
        let _handle = self
            .sweep
            .inner
            .namespaces
            .open(namespace_id)
            .await
            .expect("hold namespace");
        self.grep_worker
            .as_ref()
            .expect("grep worker")
            .disable(namespace_id)
            .await
            .expect("disable grep");
        self.sweep.inner.namespaces.mark_index_dirty(namespace_id);
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
kind = "local_fs"
root = "unused"
"#,
    )
    .expect("parse the sweep test config");
    if let Some(worker) = grep {
        config.grep = GrepConfig {
            mode: JobMode::ServeAndMaintain,
            worker,
            ..GrepConfig::default()
        };
    }
    config
}

/// A writer that ran before the server started, on the real clock.
async fn seed_writer(store: &SharedObjectStore) -> (LoonFs<Writable>, u64) {
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("before-the-server"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build the seed writer");
    let now_ms = writer.now_ms();
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

/// Raises the namespace's hint to its current manifest.
///
/// A runtime skips a hint raise less than a second after its last raise of
/// the same hint, so a test that counts discovery's requests raises the hint
/// first.
async fn raise_hint_to_current_manifest(store: &dyn ObjectStore, namespace_id: &NamespaceId) {
    let current = loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .manifest()
        .manifest_no;
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .raise_hint(current, None)
        .await
        .expect("raise the hint");
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
    server
        .sweep
        .run_pass(GarbageCollection::Collect)
        .await
        .expect("list namespaces");
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
    server
        .sweep
        .run_pass(GarbageCollection::Collect)
        .await
        .expect("list namespaces");
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
    server
        .sweep
        .run_pass(GarbageCollection::Collect)
        .await
        .expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &content_prefix).await.is_empty(),
        "content outlives the deletion until the retirement grace passes"
    );

    server.clock.advance_ms(DAY_MS);
    assert_eq!(
        server
            .sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("list namespaces"),
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
    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("list namespaces");
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Active { .. }
    );
    worker.disable(&namespace_id).await.expect("disable grep");
    let segments_prefix = loonfs_grep::keyspace::segments_prefix(&namespace_id);
    assert!(!keys_under(store.as_ref(), &segments_prefix)
        .await
        .is_empty());

    server
        .sweep
        .run_pass(GarbageCollection::Collect)
        .await
        .expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &segments_prefix)
            .await
            .is_empty(),
        "a disabled index keeps its segments until the grace passes"
    );
    server.clock.advance_ms(2 * DAY_MS);
    server
        .sweep
        .run_pass(GarbageCollection::Collect)
        .await
        .expect("list namespaces");
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

    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("list namespaces");
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. },
        "one visit runs at most {} build steps",
        super::MAX_GREP_BUILD_STEPS_PER_VISIT
    );
    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("list namespaces");
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
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS).await;

    assert_eq!(
        server
            .sweep
            .run_pass(GarbageCollection::Skip)
            .await
            .expect("list namespaces"),
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

    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("list namespaces");
    assert_eq!(
        server.wal_tail_objects(&names[1]).await,
        0,
        "a namespace that no host holds keeps no retry state, so the next pass tries it again"
    );
}

/// Forwards every call to a local store, and panics on a read of one
/// namespace's manifest while armed.
#[derive(Debug)]
struct PanicOnManifestRead {
    inner: LocalFsStore,
    manifest_prefix: String,
    armed: AtomicBool,
}

#[async_trait::async_trait]
impl ObjectStore for PanicOnManifestRead {
    loonfs_test_support::delegate_object_store!(self => self.inner; except get);

    async fn get(
        &self,
        key: &str,
        range: Option<loonfs_objectstore::ByteRange>,
    ) -> Result<Option<bytes::Bytes>, ObjectStoreError> {
        if self.armed.load(Ordering::SeqCst) && key.starts_with(&self.manifest_prefix) {
            panic!("injected panic reading `{key}`");
        }
        self.inner.get(key, range).await
    }
}

#[tokio::test]
async fn a_panicking_visit_is_backed_off_and_the_next_namespace_is_still_visited() {
    let directory = tempdir().expect("tempdir");
    let base: SharedObjectStore = local_store(directory.path());
    let names = ["alpha", "beta", "gamma"].map(namespace_id);
    let (writer, now_ms) = seed_writer(&base).await;
    for namespace_id in &names {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("stop the writer");
    let store = Arc::new(PanicOnManifestRead {
        inner: LocalFsStore::new(directory.path()).expect("local store"),
        manifest_prefix: loonfs_objectstore::keys::metadata_manifest_prefix(&names[1]),
        armed: AtomicBool::new(false),
    });
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS).await;
    let _held = server
        .sweep
        .inner
        .namespaces
        .open(&names[1])
        .await
        .expect("hold the namespace that panics");
    store.armed.store(true, Ordering::SeqCst);

    server.pass_after(0).await;
    store.armed.store(false, Ordering::SeqCst);
    assert_eq!(server.wal_tail_objects(&names[0]).await, 0);
    assert!(server.wal_tail_objects(&names[1]).await > 0);
    assert_eq!(
        server.wal_tail_objects(&names[2]).await,
        0,
        "the panic doesn't stop the visits to other namespaces"
    );
    // The acknowledgment of the floor and the retention advance load the
    // same manifest, so they panic too.
    assert_eq!(server.counter("loonfs.maintenance.sweep_panics", &[]), 3);
    for call in [
        SweepCall::Metadata,
        SweepCall::RetentionAcknowledgment,
        SweepCall::Retention,
    ] {
        assert_eq!(server.visit_failures(call), 1, "{call:?}");
    }
    assert_eq!(
        server.retry(&names[1], SweepCall::Metadata),
        SweepRetry {
            retry_at_ms: server.sweep.inner.timer.monotonic_now_ms()
                + config.maintenance_interval_ms,
            attempts: 1,
        },
        "the panic counts as a failure, and ticks wait for its retry time"
    );

    server.pass_after(0).await;
    assert_eq!(
        server.wal_tail_objects(&names[1]).await,
        0,
        "the next pass tries the namespace again"
    );
}

/// A server over a store whose `failing` calls fail once armed, holding a
/// session whose WAL tail has one commit. The returned handle keeps the
/// session from closing while a test holds it.
async fn held_failing_server(
    directory: &std::path::Path,
    config: &ServerConfig,
    failing: OperationSelector,
) -> (
    Arc<FailStore<LocalFsStore>>,
    SweepServer,
    NamespaceId,
    Namespace<Writable>,
) {
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory).expect("local store"),
        KeyPredicate::any(),
        failing,
        InjectedError::PermissionDenied("expired credentials".to_owned()),
    ));
    let server = SweepServer::start(store.clone(), config, 0).await;
    let namespace_id = namespace_id("held-failing");
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
    held.put_file("/tail.txt", b"unfolded", &test_actor())
        .await
        .expect("write the tail");
    (store, server, namespace_id, held)
}

impl SweepServer {
    fn visit_failures(&self, call: SweepCall) -> u64 {
        self.counter(
            "loonfs.maintenance.sweep_visit_failures",
            &[("call", call.as_str())],
        )
    }

    fn retry(&self, namespace_id: &NamespaceId, call: SweepCall) -> SweepRetry {
        visit_held(&self.sweep.inner.namespaces, namespace_id, |held| {
            call.retry(&held.host_state)
        })
        .expect("a held session")
        .expect("a call that keeps retry state")
    }

    async fn pass_after(&self, elapsed_ms: u64) {
        self.clock.advance_ms(elapsed_ms);
        self.sweep
            .run_pass(GarbageCollection::Skip)
            .await
            .expect("list namespaces");
    }

    /// Checks that ticks make a held session's failing `call` again exactly
    /// when each wait in `wait_seconds` ends, from the call's first failure.
    async fn tick_through_retries(
        &self,
        namespace_id: &NamespaceId,
        call: SweepCall,
        wait_seconds: &[u64],
    ) {
        for (attempts, wait_s) in (1..).zip(wait_seconds) {
            assert_eq!(self.retry(namespace_id, call).attempts, attempts);
            let failures = self.visit_failures(call);
            self.tick_after(wait_s * 1_000 - 1).await;
            assert_eq!(
                self.visit_failures(call),
                failures,
                "failure {attempts} waits {wait_s} s"
            );
            self.tick_after(1).await;
            assert_eq!(
                self.visit_failures(call),
                failures + 1,
                "failure {attempts} is retried after {wait_s} s"
            );
        }
    }
}

/// Checks a failing metadata call's schedule with the default 5-second tick
/// and 5-minute metadata interval: the later of that interval and a backoff
/// that starts at one tick and doubles up to an hour.
#[tokio::test]
async fn a_failing_metadata_call_waits_the_later_of_its_interval_and_its_backoff() {
    const WAIT_SECONDS: [u64; 12] = [
        300, 300, 300, 300, 300, 300, 320, 640, 1_280, 2_560, 3_600, 3_600,
    ];
    let directory = tempdir().expect("tempdir");
    let mut config = sweep_config(None);
    assert_eq!(
        (config.tick_interval_ms, config.maintenance_interval_ms),
        (5_000, 5 * MINUTE_MS)
    );
    // Keeps collection from coming due while the clock runs for hours.
    config.gc_interval_ms = 30 * DAY_MS;
    let (store, server, namespace_id, held) =
        held_failing_server(directory.path(), &config, OperationSelector::Any).await;
    store.fail_all();
    server.tick_after(config.tick_interval_ms).await;
    server
        .tick_through_retries(&namespace_id, SweepCall::Metadata, &WAIT_SECONDS)
        .await;

    store.clear();
    server.tick_after(super::MAX_SWEEP_BACKOFF_MS).await;
    assert_eq!(
        server.retry(&namespace_id, SweepCall::Metadata).attempts,
        0,
        "a success clears the backoff"
    );
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "a permission failure is retried, not skipped for good"
    );
    held.put_file("/again.txt", b"unfolded", &test_actor())
        .await
        .expect("write another tail");
    store.fail_all();
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        server.retry(&namespace_id, SweepCall::Metadata).attempts,
        1,
        "a failure after a success starts a new backoff"
    );
}

/// Checks a failing collection's schedule with the default 5-second tick: a
/// backoff that starts at one tick and doubles up to an hour.
#[tokio::test]
async fn a_failing_collection_backs_off_from_one_tick_to_an_hour() {
    const WAIT_SECONDS: [u64; 12] = [5, 10, 20, 40, 80, 160, 320, 640, 1_280, 2_560, 3_600, 3_600];
    let directory = tempdir().expect("tempdir");
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = Some(0);
    let (store, server, namespace_id, held) =
        held_failing_server(directory.path(), &config, OperationSelector::List).await;
    server.tick_after(config.tick_interval_ms).await;
    assert!(held.metadata_caught_up());
    store.fail_all();
    server.tick_after(config.gc_interval_ms).await;
    assert_eq!(server.visit_failures(SweepCall::Gc), 1);
    server
        .tick_through_retries(&namespace_id, SweepCall::Gc, &WAIT_SECONDS)
        .await;
    assert_eq!(server.visit_failures(SweepCall::Metadata), 0);
}

#[tokio::test]
async fn an_idle_session_with_a_failing_call_closes_once_its_backoff_reaches_the_cap() {
    let directory = tempdir().expect("tempdir");
    let mut config = sweep_config(None);
    // Keeps collection from coming due while the clock runs for hours.
    config.gc_interval_ms = 30 * DAY_MS;
    let (store, server, namespace_id, held) =
        held_failing_server(directory.path(), &config, OperationSelector::Any).await;
    let namespaces = &server.sweep.inner.namespaces;
    drop(held);
    store.fail_all();

    server.tick_after(config.tick_interval_ms).await;
    server
        .tick_after(config.idle_session_close_after_ms + 1)
        .await;
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(server.visit_failures(SweepCall::Metadata), 2);
    assert_eq!(
        held_ids(namespaces),
        std::slice::from_ref(&namespace_id),
        "a transient failure keeps the idle session held"
    );

    let attempts = || server.retry(&namespace_id, SweepCall::Metadata).attempts;
    while super::backoff_ms(config.tick_interval_ms, attempts()) < super::MAX_SWEEP_BACKOFF_MS {
        let failures = server.visit_failures(SweepCall::Metadata);
        server.tick_after(super::MAX_SWEEP_BACKOFF_MS).await;
        assert_eq!(
            server.visit_failures(SweepCall::Metadata),
            failures + 1,
            "ticks retry the call on its backoff"
        );
        assert!(!held_ids(namespaces).is_empty());
    }
    server.tick_after(config.tick_interval_ms).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "a call whose backoff reached the cap doesn't keep an idle session open"
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
        OperationSelector::Any,
    ));
    let mut config = sweep_config(None);
    config.max_concurrent_maintenance = const { std::num::NonZeroUsize::new(2).unwrap() };
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(2));

    store.block_next();
    let pass = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.run_pass(GarbageCollection::Skip).await }
    });
    loonfs_test_support::clock::bounded(Duration::from_secs(10), async {
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
        OperationSelector::Any,
    ));
    let mut config = sweep_config(None);
    config.max_concurrent_maintenance = const { std::num::NonZeroUsize::new(2).unwrap() };
    let server = SweepServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(1));

    store.block_next();
    let pass = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.run_pass(GarbageCollection::Skip).await }
    });
    loonfs_test_support::clock::bounded(Duration::from_secs(10), async {
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
    assert_matches!(
        error,
        ObjectStoreError::Transport { .. },
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
    (Grep::NotMaintained, 14, 30, 10),
    (Grep::Maintained, 15, 40, 12),
    (Grep::Enabled, 29, 58, 12),
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
        for namespace_id in &names {
            loonfs_core::test_support::namespace_store(base.as_ref(), namespace_id)
                .raise_hint_to_current_manifest()
                .await;
        }
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
        sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("list namespaces");

        for (garbage, per_namespace, lists_per_namespace) in [
            (GarbageCollection::Skip, visit, 0),
            (
                GarbageCollection::Collect,
                collecting_visit,
                collecting_lists,
            ),
        ] {
            store.reset();
            sweep.run_pass(garbage).await.expect("list namespaces");
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
                    "requests and lists for `{namespace_id}`, grep {grep:?}, garbage \
                     {garbage:?}"
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
            .put_if_absent(stray, bytes::Bytes::from_static(b"stray"))
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
        server
            .sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("list namespaces"),
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
    // The metadata call, the grep build, the acknowledgment of the floor, the
    // retention advance, and collection each read the hint once. Grep
    // collection reads the hint and lists its own prefix.
    assert_eq!(stray.len(), 7, "{stray:?}");
    for call in [
        "metadata",
        "grep_index",
        "retention_acknowledgment",
        "retention",
        "gc",
    ] {
        assert_eq!(
            server.counter("loonfs.maintenance.sweep_visit_failures", &[("call", call)]),
            0,
            "a namespace id with no namespace behind it is not a `{call}` failure"
        );
    }
}

mod floor_acknowledgment;
mod indexing;
mod retention;
mod sessions;

mod concurrency;
