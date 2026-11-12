//! Sweeps, session steps, and their store request costs.

use super::retries::MaintenanceCall;
use super::{GrepIndexing, Scheduler, SchedulerState, StepRetry};
use crate::config::{GrepConfig, JobMode, ServerConfig};
use futures::TryStreamExt as _;
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue, MetricsRecorder};
use loonfs::{LoonFs, Maintenance, Namespace, NamespaceId, SharedObjectStore, Writable};
use loonfs_grep::entry::{grep_extension_name, GrepIndexStatus};
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
use loonfs_types::format::control::{decode_control_object, ControlObjectKind, HintPayload};
use loonfs_types::{EffectiveLimit, ManifestNo};
use std::assert_matches;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

const MINUTE_MS: u64 = 60 * 1_000;

/// Lists the ids of the sessions that `namespaces` holds.
fn held_ids(namespaces: &Namespaces<SchedulerState>) -> Vec<NamespaceId> {
    std::iter::successors(namespaces.next_held_id(None), |after| {
        namespaces.next_held_id(Some(after))
    })
    .collect()
}

/// Counts the list requests in `operations`, which only collection sends.
fn list_count(operations: &[RecordedOperation]) -> usize {
    operations
        .iter()
        .filter(|operation| matches!(operation, RecordedOperation::List { .. }))
        .count()
}

/// Reads or updates the held entry of `namespace_id` in `namespaces`.
fn with_held<T>(
    namespaces: &Namespaces<SchedulerState>,
    namespace_id: &NamespaceId,
    with: impl FnOnce(&mut HeldNamespace<SchedulerState>) -> T,
) -> Option<T> {
    namespaces
        .entry(namespace_id)
        .map(|entry| with(&mut super::lock(&entry)))
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

/// A server's runtime and scheduler over `store`, reading `clock`.
struct SchedulerServer {
    clock: Arc<ManualClock>,
    recorder: Arc<DefaultMetricsRecorder>,
    runtime: LoonFs<Writable>,
    maintenance: Maintenance,
    grep_worker: Option<GrepWorker<SharedObjectStore>>,
    scheduler: Scheduler,
}

impl SchedulerServer {
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
                maintenance.clone(),
                loonfs_grep::GrepStepBudget::default(),
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
        let scheduler = Scheduler::new(
            config.scheduler_config(),
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
            scheduler,
        }
    }

    async fn run_steps_after(&self, elapsed_ms: u64) {
        self.clock.advance_ms(elapsed_ms);
        self.scheduler.run_due_steps().await;
    }

    async fn enable_index(&self, namespace_id: &NamespaceId) {
        let _handle = self
            .scheduler
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
        self.scheduler
            .inner
            .namespaces
            .mark_index_dirty(namespace_id);
    }

    async fn disable_index(&self, namespace_id: &NamespaceId) {
        let _handle = self
            .scheduler
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
        self.scheduler
            .inner
            .namespaces
            .mark_index_dirty(namespace_id);
    }

    fn page_limit(mut self, page_limit: EffectiveLimit) -> Self {
        self.scheduler = self.scheduler.page_limit(page_limit);
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

    /// Waits until the runtime's open writer sessions number `expected`, and
    /// fails the test if they never do. A session ends once its last admitted
    /// work finishes, which can be just after the call that admitted it
    /// returns.
    async fn wait_for_open_sessions(&self, expected: i64) {
        let open = || {
            let snapshot = self.recorder.snapshot();
            let open = snapshot
                .by_name("loonfs.publisher.sessions_open")
                .find_map(|entry| match entry.value {
                    MetricValue::Gauge(value) => Some(value),
                    _ => None,
                })
                .unwrap_or(0);
            open
        };
        for _ in 0..10_000 {
            if open() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(open(), expected, "open writer sessions");
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
fn scheduler_config(grep: Option<GrepWorkerConfig>) -> ServerConfig {
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
    .expect("parse the scheduler test config");
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
///
/// The commit returns before the writer raises the hint to the manifest that
/// claimed the writer epoch, so the seed waits for the writer to finish that
/// raise. A later publication's raise would otherwise sometimes lose its
/// compare-and-swap to the commit's raise and leave the hint one manifest
/// behind.
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
    writer.drain().await.expect("raise the hint");
}

/// Returns the manifest number that the namespace's hint records.
async fn hint_manifest_no(store: &dyn ObjectStore, namespace_id: &NamespaceId) -> ManifestNo {
    let bytes = store
        .get(&loonfs_objectstore::keys::hint(namespace_id), None)
        .await
        .expect("read the hint")
        .expect("the hint exists");
    decode_control_object::<HintPayload>(&bytes, ControlObjectKind::Hint)
        .expect("decode the hint")
        .payload()
        .manifest_no
}

/// Checks that the namespace's hint records the current manifest.
async fn assert_hint_at_current_manifest(store: &dyn ObjectStore, namespace_id: &NamespaceId) {
    let current = loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .manifest()
        .manifest_no;
    assert_eq!(
        hint_manifest_no(store, namespace_id).await,
        current,
        "the runtime raises the hint to the current manifest"
    );
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

    let server = SchedulerServer::start(store.clone(), &scheduler_config(None), now_ms).await;
    let tail = server.wal_tail_objects(&namespace_id).await;
    assert!(tail > 0);
    server.scheduler.run_sweep().await.expect("list namespaces");
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
    server.scheduler.run_sweep().await.expect("list namespaces");
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
    let pool_id = loonfs_core::test_support::load_pool_id(&store, &namespace_id).await;
    let content_prefix = loonfs_objectstore::keys::content_prefix(&pool_id);
    assert!(!keys_under(store.as_ref(), &content_prefix).await.is_empty());
    // A pool that a listing finds without its pool record is skipped, as a
    // pool whose create call deleted the record after the listing is.
    let unrecorded = loonfs_objectstore::keys::content_blob(
        &loonfs_types::PoolId::generate(),
        &loonfs_types::ContentId::generate(),
    );
    store
        .put_if_absent(&unrecorded, bytes::Bytes::from_static(b"content"))
        .await
        .expect("an object of a pool without a record");

    let server = SchedulerServer::start(store.clone(), &scheduler_config(None), now_ms).await;
    server.scheduler.run_sweep().await.expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &content_prefix).await.is_empty(),
        "content outlives the deletion until the retirement grace passes"
    );

    server.clock.advance_ms(DAY_MS);
    assert_eq!(
        server.scheduler.run_sweep().await.expect("list namespaces"),
        1,
        "a deleted namespace is still listed"
    );
    assert_eq!(
        keys_under(store.as_ref(), &content_prefix).await,
        Vec::<String>::new()
    );
    assert!(
        server.counter(
            "loonfs.gc.reclaimed",
            &[("category", "deleted_content_objects")]
        ) > 0,
        "the sweep's collection of the pool deletes the content"
    );
    assert_eq!(
        server.counter("loonfs.maintenance.sweep_failures", &[("call", "pool_gc")]),
        0,
        "a pool without a record is not a failed call"
    );
    assert!(store.head(&unrecorded).await.expect("head").is_some());
    assert_eq!(
        server.counter("loonfs.maintenance.sweep_failures", &[("call", "metadata")]),
        0,
        "a deleted namespace is not a failed call"
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
    let server = SchedulerServer::start(
        store.clone(),
        &scheduler_config(Some(GrepWorkerConfig::default())),
        now_ms,
    )
    .await;
    let worker = server.grep_worker.as_ref().expect("grep worker");
    worker.enable(&namespace_id).await.expect("enable grep");
    server.scheduler.run_sweep().await.expect("list namespaces");
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Active { .. }
    );
    worker.disable(&namespace_id).await.expect("disable grep");
    let segments_prefix =
        loonfs_objectstore::keys::extension_segment_prefix(&namespace_id, &grep_extension_name());
    assert!(!keys_under(store.as_ref(), &segments_prefix)
        .await
        .is_empty());

    server.scheduler.run_sweep().await.expect("list namespaces");
    assert!(
        !keys_under(store.as_ref(), &segments_prefix)
            .await
            .is_empty(),
        "a disabled index keeps its segments until the grace passes"
    );
    server.clock.advance_ms(2 * DAY_MS);
    server.scheduler.run_sweep().await.expect("list namespaces");
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
    let files = super::MAX_GREP_STEPS_PER_INDEX_STEP + 4;
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
    let server = SchedulerServer::start(
        store,
        &scheduler_config(Some(GrepWorkerConfig {
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

    server.scheduler.run_sweep().await.expect("list namespaces");
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. },
        "one index step runs at most {} build steps",
        super::MAX_GREP_STEPS_PER_INDEX_STEP
    );
    server.scheduler.run_sweep().await.expect("list namespaces");
    let status = server.grep_status(&namespace_id).await;
    let watermark = status
        .active_watermark()
        .unwrap_or_else(|| panic!("the next sweep finishes the backfill, found {status:?}"));
    assert_eq!(
        watermark.built_through_seq(),
        loonfs::CommitSeq(u64::try_from(files).expect("file count fits"))
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
    let config = scheduler_config(None);
    let server = SchedulerServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS).await;

    assert_eq!(
        server.scheduler.run_sweep().await.expect("list namespaces"),
        3
    );
    store.clear();
    assert_eq!(server.wal_tail_objects(&names[0]).await, 0);
    assert!(server.wal_tail_objects(&names[1]).await > 0);
    assert_eq!(server.wal_tail_objects(&names[2]).await, 0);
    assert_eq!(
        server.counter("loonfs.maintenance.sweep_failures", &[("call", "metadata")]),
        1
    );

    server.scheduler.run_sweep().await.expect("list namespaces");
    assert_eq!(
        server.wal_tail_objects(&names[1]).await,
        0,
        "a namespace that no host holds keeps no retry state, so the next sweep tries it again"
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
async fn a_panicking_call_is_backed_off_and_the_next_namespace_still_runs_its_steps() {
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
    let config = scheduler_config(None);
    let server = SchedulerServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS).await;
    let _held = server
        .scheduler
        .inner
        .namespaces
        .open(&names[1])
        .await
        .expect("hold the namespace that panics");
    store.armed.store(true, Ordering::SeqCst);

    server.sweep_after(0).await;
    store.armed.store(false, Ordering::SeqCst);
    assert_eq!(server.wal_tail_objects(&names[0]).await, 0);
    assert!(server.wal_tail_objects(&names[1]).await > 0);
    assert_eq!(
        server.wal_tail_objects(&names[2]).await,
        0,
        "the panic doesn't stop the steps of other namespaces"
    );
    // The retention advance and the collections of the namespace and its pool
    // load the same manifest, so they panic too.
    assert_eq!(server.counter("loonfs.maintenance.sweep_panics", &[]), 4);
    for call in [
        MaintenanceCall::Metadata,
        MaintenanceCall::Retention,
        MaintenanceCall::Gc,
        MaintenanceCall::PoolGc,
    ] {
        assert_eq!(server.call_failures(call), 1, "{call:?}");
    }
    assert_eq!(
        server.retry(&names[1], MaintenanceCall::Metadata),
        StepRetry {
            retry_at_ms: server.scheduler.inner.timer.monotonic_now_ms()
                + config.metadata_retry_interval_ms.get(),
            attempts: 1,
        },
        "the panic counts as a failure, and the session steps wait for its retry time"
    );

    server.sweep_after(0).await;
    assert_eq!(
        server.wal_tail_objects(&names[1]).await,
        0,
        "the next sweep tries the namespace again"
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
    SchedulerServer,
    NamespaceId,
    Namespace<Writable>,
) {
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(directory).expect("local store"),
        KeyPredicate::any(),
        failing,
        InjectedError::PermissionDenied("expired credentials".to_owned()),
    ));
    let server = SchedulerServer::start(store.clone(), config, 0).await;
    let namespace_id = namespace_id("held-failing");
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
    held.put_file("/tail.txt", b"unfolded", &test_actor())
        .await
        .expect("write the tail");
    (store, server, namespace_id, held)
}

impl SchedulerServer {
    fn call_failures(&self, call: MaintenanceCall) -> u64 {
        self.counter(
            "loonfs.maintenance.sweep_failures",
            &[("call", call.as_str())],
        )
    }

    fn retry(&self, namespace_id: &NamespaceId, call: MaintenanceCall) -> StepRetry {
        with_held(&self.scheduler.inner.namespaces, namespace_id, |held| {
            call.retry(&held.host_state)
        })
        .expect("a held session")
        .expect("a call that keeps retry state")
    }

    /// When the held session's last metadata step said its tail's idle fold
    /// comes due.
    fn idle_fold_due_at_ms(&self, namespace_id: &NamespaceId) -> Option<u64> {
        with_held(&self.scheduler.inner.namespaces, namespace_id, |held| {
            held.host_state.idle_fold_due_at_ms
        })
        .expect("a held session")
    }

    /// Whether the held session's last metadata step left work due, so the
    /// scheduler waits for the metadata retry time before the next step.
    fn metadata_left_due(&self, namespace_id: &NamespaceId) -> bool {
        self.retry(namespace_id, MaintenanceCall::Metadata)
            .retry_at_ms
            > self.scheduler.inner.timer.monotonic_now_ms()
    }

    /// Makes the metadata call that a session's metadata step makes, and
    /// returns whether it finds the namespace's metadata caught up.
    async fn metadata_caught_up(&self, namespace_id: &NamespaceId) -> bool {
        self.maintenance
            .maintain_metadata_while_due(namespace_id, &loonfs::MaintenanceCancellation::new())
            .await
            .expect("the metadata call")
            .response
            .outcome
            == loonfs::MetadataMaintenanceOutcome::CaughtUp
    }

    async fn sweep_after(&self, elapsed_ms: u64) {
        self.clock.advance_ms(elapsed_ms);
        self.scheduler.run_sweep().await.expect("list namespaces");
    }

    /// Checks that the session steps make a held session's failing `call` again exactly
    /// when each wait in `wait_seconds` ends, from the call's first failure.
    async fn run_steps_through_retries(
        &self,
        namespace_id: &NamespaceId,
        call: MaintenanceCall,
        wait_seconds: &[u64],
    ) {
        for (attempts, wait_s) in (1..).zip(wait_seconds) {
            assert_eq!(self.retry(namespace_id, call).attempts, attempts);
            let failures = self.call_failures(call);
            self.run_steps_after(wait_s * 1_000 - 1).await;
            assert_eq!(
                self.call_failures(call),
                failures,
                "failure {attempts} waits {wait_s} s"
            );
            self.run_steps_after(1).await;
            assert_eq!(
                self.call_failures(call),
                failures + 1,
                "failure {attempts} is retried after {wait_s} s"
            );
        }
    }
}

/// Checks a failing metadata step's schedule with the default 5-second
/// maintenance interval and 5-minute metadata retry interval: the later of
/// the retry interval and a backoff that starts at one maintenance interval,
/// while the session's writer position is current. Once the position passes
/// the revalidation bound, the session steps run no metadata step for the
/// session, and the sweep makes the call instead.
#[tokio::test]
async fn a_failing_metadata_step_waits_its_interval_until_the_position_passes_the_bound() {
    const WAIT_SECONDS: [u64; 3] = [300, 300, 300];
    let directory = tempdir().expect("tempdir");
    let mut config = scheduler_config(None);
    assert_eq!(
        (
            config.maintenance_interval_ms.get(),
            config.metadata_retry_interval_ms.get()
        ),
        (5_000, 5 * MINUTE_MS)
    );
    // Keeps collection from coming due while the clock runs.
    config.gc_interval_ms = const { std::num::NonZeroU64::new(30 * DAY_MS).unwrap() };
    // The tail is due from the first run, so each metadata step folds it
    // and reaches the failing store.
    config.idle_fold_after_ms = Some(1);
    let (store, server, namespace_id, held) =
        held_failing_server(directory.path(), &config, OperationSelector::Any).await;
    store.fail_all();
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    server
        .run_steps_through_retries(&namespace_id, MaintenanceCall::Metadata, &WAIT_SECONDS)
        .await;
    // The next retry time comes after the position's revalidation bound.
    let failures = server.call_failures(MaintenanceCall::Metadata);
    server
        .run_steps_after(config.metadata_retry_interval_ms.get())
        .await;
    assert_eq!(
        server.call_failures(MaintenanceCall::Metadata),
        failures,
        "the session steps run no metadata step for a session without a current position"
    );

    store.clear();
    server.sweep_after(0).await;
    assert_eq!(
        server
            .retry(&namespace_id, MaintenanceCall::Metadata)
            .attempts,
        0,
        "the sweep's success clears the backoff"
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
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    assert_eq!(
        server
            .retry(&namespace_id, MaintenanceCall::Metadata)
            .attempts,
        1,
        "a failure after a success starts a new backoff"
    );
}

/// Checks a failing collection's schedule with the default 5-second
/// maintenance interval: a backoff that starts at one interval and doubles up
/// to an hour.
#[tokio::test]
async fn a_failing_collection_backs_off_from_one_interval_to_an_hour() {
    const WAIT_SECONDS: [u64; 12] = [5, 10, 20, 40, 80, 160, 320, 640, 1_280, 2_560, 3_600, 3_600];
    let directory = tempdir().expect("tempdir");
    let mut config = scheduler_config(None);
    config.idle_fold_after_ms = Some(0);
    let (store, server, namespace_id, _held) =
        held_failing_server(directory.path(), &config, OperationSelector::List).await;
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    assert!(server.metadata_caught_up(&namespace_id).await);
    store.fail_all();
    server.run_steps_after(config.gc_interval_ms.get()).await;
    assert_eq!(server.call_failures(MaintenanceCall::Gc), 1);
    server
        .run_steps_through_retries(&namespace_id, MaintenanceCall::Gc, &WAIT_SECONDS)
        .await;
    assert_eq!(server.call_failures(MaintenanceCall::Metadata), 0);
}

/// Checks that a failing call keeps an idle session open, so the scheduler
/// retry the call on its backoff, until the backoff reaches the cap, and that
/// the session then closes.
#[tokio::test]
async fn an_idle_session_with_a_failing_call_closes_once_its_backoff_reaches_the_cap() {
    let directory = tempdir().expect("tempdir");
    let mut config = scheduler_config(None);
    // The first run folds the tail, so only collection is left due.
    config.idle_fold_after_ms = Some(1);
    let (store, server, namespace_id, held) =
        held_failing_server(directory.path(), &config, OperationSelector::List).await;
    let namespaces = &server.scheduler.inner.namespaces;
    drop(held);
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    store.fail_all();

    server.run_steps_after(config.gc_interval_ms.get()).await;
    assert_eq!(server.call_failures(MaintenanceCall::Gc), 1);
    assert_eq!(
        held_ids(namespaces),
        std::slice::from_ref(&namespace_id),
        "a transient failure keeps the idle session held"
    );

    let attempts = || server.retry(&namespace_id, MaintenanceCall::Gc).attempts;
    while super::retries::backoff_ms(config.maintenance_interval_ms.get(), attempts())
        < super::MAX_STEP_BACKOFF_MS
    {
        let failures = server.call_failures(MaintenanceCall::Gc);
        server.run_steps_after(super::MAX_STEP_BACKOFF_MS).await;
        assert_eq!(
            server.call_failures(MaintenanceCall::Gc),
            failures + 1,
            "the scheduler retries the call on its backoff"
        );
        assert!(!held_ids(namespaces).is_empty());
    }
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    assert!(
        held_ids(namespaces).is_empty(),
        "a call whose backoff reached the cap doesn't keep an idle session open"
    );
}

#[tokio::test]
async fn a_parked_namespace_does_not_hold_up_later_pages() {
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
    let mut config = scheduler_config(None);
    config.max_concurrent_maintenance = const { std::num::NonZeroUsize::new(2).unwrap() };
    let server = SchedulerServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(2));

    store.block_next();
    let sweep = tokio::spawn({
        let scheduler = server.scheduler.clone();
        async move { scheduler.run_sweep().await }
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
    .expect(
        "every namespace on the later pages runs its steps while the first namespace is parked",
    );
    assert!(
        !sweep.is_finished(),
        "the sweep waits for the parked namespace"
    );

    store.release();
    assert_eq!(
        sweep
            .await
            .expect("join the sweep")
            .expect("list namespaces"),
        1 + later.len()
    );
    assert_eq!(server.wal_tail_objects(&parked).await, 0);
}

#[tokio::test]
async fn a_listing_failure_on_a_later_page_lets_started_namespaces_finish_and_fails_the_sweep() {
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
    let mut config = scheduler_config(None);
    config.max_concurrent_maintenance = const { std::num::NonZeroUsize::new(2).unwrap() };
    let server = SchedulerServer::start(store.clone(), &config, now_ms + 20 * MINUTE_MS)
        .await
        .page_limit(page_limit(1));

    store.block_next();
    let sweep = tokio::spawn({
        let scheduler = server.scheduler.clone();
        async move { scheduler.run_sweep().await }
    });
    loonfs_test_support::clock::bounded(Duration::from_secs(10), async {
        store.wait_until_blocked().await;
        while store.inner().attempts() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the third listing fails while the first namespace is parked");
    assert!(
        !sweep.is_finished(),
        "the sweep waits for the namespace it started"
    );

    store.release();
    let error = sweep
        .await
        .expect("join the sweep")
        .expect_err("a failed listing fails the sweep");
    assert_matches!(
        error,
        ObjectStoreError::Transport { .. },
        "expected the injected listing failure, got {error:?}"
    );
    assert_eq!(
        server.wal_tail_objects(&parked).await,
        0,
        "the parked namespace finishes before the sweep returns"
    );
    assert_eq!(server.wal_tail_objects(&started).await, 0);
    assert!(
        server.wal_tail_objects(&unlisted).await > 0,
        "no namespace starts its steps after the failed listing"
    );
    assert_eq!(
        server.counter("loonfs.maintenance.sweeps", &[("result", "error")]),
        1
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grep {
    NotMaintained,
    Maintained,
    Enabled,
}

/// Requests one idle namespace costs in a sweep, and how many of them are
/// lists. The sweep includes the collection of the namespace's pool. The
/// retention advance plans from the view, so its share includes a GET of the
/// WAL number after the manifest and a second manifest HEAD. The docs quote
/// these numbers.
const IDLE_SWEEP_REQUESTS: [(Grep, usize, usize); 3] = [
    (Grep::NotMaintained, 35, 10),
    (Grep::Maintained, 39, 10),
    (Grep::Enabled, 40, 10),
];

#[tokio::test(start_paused = true)]
async fn an_idle_sweep_costs_one_listing_per_page_and_a_fixed_number_of_requests_per_namespace() {
    for (grep, per_namespace, lists_per_namespace) in IDLE_SWEEP_REQUESTS {
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
        let config =
            scheduler_config((grep != Grep::NotMaintained).then(GrepWorkerConfig::default));
        let server = SchedulerServer::start(store.clone(), &config, now_ms + 2 * DAY_MS).await;
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
        let scheduler = server.scheduler.page_limit(page_limit(2));
        // The first sweep collects what the seed left behind, so the sweeps
        // below find nothing to collect and nothing inside its grace.
        scheduler.run_sweep().await.expect("list namespaces");
        // The seed's publications, the publications that enabled each index,
        // and the first sweep's index builds each raised their namespace's
        // hint, so the sweeps below start from hints at the current manifests.
        for namespace_id in &names {
            assert_hint_at_current_manifest(base.as_ref(), namespace_id).await;
        }
        // An index's publication drops the runtime's cached head, so this
        // sweep loads each head again, and the sweep below finds it cached,
        // as an idle namespace's sweeps do.
        scheduler.run_sweep().await.expect("list namespaces");

        let mut pool_prefixes = Vec::new();
        for namespace_id in &names {
            let pool_id =
                loonfs_core::test_support::load_pool_id(base.as_ref(), namespace_id).await;
            pool_prefixes.push(loonfs_objectstore::keys::pool_prefix(&pool_id));
        }
        store.reset();
        scheduler.run_sweep().await.expect("list namespaces");
        let operations = store.take();
        let listings = operations
            .iter()
            .filter(|operation| {
                matches!(operation, RecordedOperation::List { .. })
                    && operation.key() == "namespaces/"
            })
            .count();
        assert_eq!(listings, 2, "three namespaces at two per page");
        let pool_listings = operations
            .iter()
            .filter(|operation| {
                matches!(operation, RecordedOperation::List { .. }) && operation.key() == "pools/"
            })
            .count();
        assert_eq!(pool_listings, 2, "three pools at two per page");
        for (namespace_id, pool) in names.iter().zip(&pool_prefixes) {
            let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
            let namespace_requests: Vec<_> = operations
                .iter()
                .filter(|operation| {
                    operation.key().starts_with(&prefix) || operation.key().starts_with(pool)
                })
                .collect();
            let lists = namespace_requests
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::List { .. }))
                .count();
            assert_eq!(
                (namespace_requests.len(), lists),
                (per_namespace, lists_per_namespace),
                "requests and lists for `{namespace_id}`, grep {grep:?}"
            );
        }
        assert_eq!(
            operations.len(),
            listings + pool_listings + names.len() * per_namespace
        );
    }
}

#[tokio::test]
async fn a_stray_child_of_the_namespace_prefix_is_not_a_failed_call() {
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
    let server = SchedulerServer::start(
        store.clone(),
        &scheduler_config(Some(GrepWorkerConfig::default())),
        0,
    )
    .await;
    store.reset();

    assert_eq!(
        server.scheduler.run_sweep().await.expect("list namespaces"),
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
    // The metadata call, the grep build, the retention advance, and
    // collection each read the hint once.
    assert_eq!(stray.len(), 4, "{stray:?}");
    for call in ["metadata", "grep_index", "retention", "gc"] {
        assert_eq!(
            server.counter("loonfs.maintenance.sweep_failures", &[("call", call)]),
            0,
            "a namespace id with no namespace behind it is not a `{call}` failure"
        );
    }
}

mod indexing;
mod retention;
mod sessions;

mod concurrency;
