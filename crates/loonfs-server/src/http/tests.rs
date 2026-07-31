#![allow(clippy::panic)]
// HTTP smoke helpers panic in unexpected match arms for precise diagnostics.

use super::error::status_for_core_error_code;
use super::{
    app_with_store, app_with_store_and_state, build_handles_with_metrics_jsonl_path, AppState,
    SharedObjectStore,
};
use crate::config::RuntimeCacheConfigOverrides;
use crate::{ServerConfig, StoreConfig};
use async_trait::async_trait;
use axum::body::Bytes;
use futures::stream::{BoxStream, StreamExt};
use loonfs::{
    CreateNamespaceOptions, DeleteOptions, FsAdmin, FsReader, FsWriter, MaintenanceJob,
    MaintenanceJobId, MaintenanceProbe, MaintenanceStepConclusion, MaintenanceStepResult,
    PutFileOptions, TraceMode, TraceStoreKind,
};
use loonfs_api::ErrorCode;
use loonfs_api::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, GrepRequest, NamespaceId,
};
use loonfs_client::{Client, ClientConfig, ClientError, CommitOptions, NamespacePath};
use loonfs_grep::keyspace::{manifest_key as grep_manifest_key, root_key as grep_root_key};
use loonfs_grep::root::{
    encode_grep_root, load_grep_root, GrepManifestId, GrepRootEnvelope, GrepRootPointer,
};
use loonfs_grep::{GrepIndexSnapshot, GrepWorker, NamespaceReads};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;

fn replace_file_options() -> PutFileOptions {
    PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::default()
    }
}

/// The compile-time forcing function for new error codes moved here when
/// `ErrorCode` became `#[non_exhaustive]`: every registered code must
/// appear in the api.md error table, and the status this server serves
/// must be the status the table documents.
#[test]
fn error_status_mapping_matches_the_api_spec_table() {
    let spec = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/api.md"
    ))
    .expect("read docs/specs/api.md");
    let table = spec
        .split("The full registry")
        .nth(1)
        .expect("api.md error registry intro")
        .split("Precondition failures surface")
        .next()
        .expect("api.md error registry end");

    let mut documented = std::collections::BTreeMap::new();
    for line in table.lines() {
        let Some(rest) = line.strip_prefix("| `") else {
            continue;
        };
        let mut cells = rest.split(" | ");
        let code = cells
            .next()
            .expect("code cell")
            .trim_end_matches('`')
            .to_owned();
        let status: u16 = cells
            .next()
            .expect("status cell")
            .trim()
            .parse()
            .expect("numeric status cell");
        documented.insert(code, status);
    }

    for code in ErrorCode::ALL {
        let documented_status = documented.remove(code.as_str()).unwrap_or_else(|| {
            panic!(
                "`{}` is registered in loonfs-api but missing from the api.md error table",
                code.as_str()
            )
        });
        assert_eq!(
            status_for_core_error_code(code).as_u16(),
            documented_status,
            "served status for `{}` disagrees with the api.md error table",
            code.as_str()
        );
    }
    assert!(
        documented.is_empty(),
        "api.md documents codes this build does not register: {documented:?}"
    );
}
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{BlockingStore, BufferWatchStore, KeyPredicate, OperationClass};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

#[derive(Debug)]
struct StaleHeadOnceStore {
    inner: LocalFsStore,
    head_key: String,
    armed: AtomicBool,
}

impl StaleHeadOnceStore {
    fn new(root: impl AsRef<Path>, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("construct local store"),
            head_key: wal_head(namespace),
            armed: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl ObjectStore for StaleHeadOnceStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(mode, PutMode::CompareAndSwap { .. })
            && self.armed.swap(false, Ordering::SeqCst)
        {
            if let Some(existing) = self.inner.get(key, None).await? {
                let _ = self.inner.put_overwrite(key, existing).await?;
            }
        }
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

#[derive(Debug)]
struct FaultGrepRootStore {
    inner: LocalFsStore,
    root_key: String,
    fail_next_root_read: AtomicBool,
    conflict_next_root_publication: AtomicBool,
}

impl FaultGrepRootStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("construct local store"),
            root_key: grep_root_key(namespace_id),
            fail_next_root_read: AtomicBool::new(false),
            conflict_next_root_publication: AtomicBool::new(false),
        }
    }

    fn fail_next_root_read(&self) {
        self.fail_next_root_read.store(true, Ordering::SeqCst);
    }

    fn conflict_next_root_publication(&self) {
        self.conflict_next_root_publication
            .store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for FaultGrepRootStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key == self.root_key && self.fail_next_root_read.swap(false, Ordering::SeqCst) {
            return Err(ObjectStoreError::transport(
                key,
                "injected grep-root outage",
            ));
        }
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.root_key
            && matches!(
                &mode,
                PutMode::CreateIfAbsent | PutMode::CompareAndSwap { .. }
            )
            && self
                .conflict_next_root_publication
                .swap(false, Ordering::SeqCst)
        {
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
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

#[tokio::test]
async fn build_handles_installs_jsonl_object_store_metrics_recorder() {
    let store_dir = tempdir().expect("store tempdir");
    let metrics_dir = tempdir().expect("metrics tempdir");
    let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(store_dir.path(), "server-writer");
    let metrics_path = metrics_dir.path().join("object-store.ndjson");

    {
        let (writer, _reader, _admin) = build_handles_with_metrics_jsonl_path(
            &config,
            store,
            Some(metrics_path.clone().into_os_string()),
        )
        .await
        .expect("build handles");
        writer
            .create_namespace(&namespace_id("metrics"), CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }

    let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
    assert!(!jsonl.is_empty());
    assert!(!jsonl.contains("namespaces/metrics"));
}

#[tokio::test]
async fn app_validates_directly_built_configs() {
    let temp_dir = tempdir().expect("tempdir");
    let mut config = test_config(temp_dir.path(), "app-validate-writer");
    config.max_concurrent_uploads = 0;
    match super::app(config).await {
        Err(crate::config::ServerConfigError::InvalidField { field, .. }) => {
            assert_eq!(field, "max_concurrent_uploads");
        }
        Err(other) => panic!("expected invalid field error, got {other:?}"),
        Ok(_) => panic!("app must reject a zero upload bound"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_drains_requests_and_settles_the_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let config = test_config(temp_dir.path(), "shutdown-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(super::serve_on(listener, config, async move {
        let _ = shutdown_rx.await;
    }));

    // The server accepts work while running.
    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace over http");

    shutdown_tx.send(()).expect("trigger shutdown");
    server
        .await
        .expect("join server task")
        .expect("graceful shutdown settles background work");

    // The listener is closed once serve returns.
    assert!(
        std::net::TcpStream::connect(addr).is_err(),
        "listener should refuse connections after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_shutdown_drains_an_active_grep_step() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-shutdown");
    let blocking_store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::exact(grep_root_key(&namespace_id)),
        OperationClass::GetWithMetadata,
    ));
    let store = blocking_store.clone() as SharedObjectStore;
    let writer = test_runtime(store.clone(), "grep-shutdown-seed").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    grep_worker(&store, "grep-shutdown-enable")
        .await
        .enable(&namespace_id)
        .await
        .expect("enable grep");

    blocking_store.block_next();
    let config = test_config(temp_dir.path(), "grep-shutdown-server");
    let (_router, lifecycle, state) =
        super::app_with_store_and_transfer_issuer(config, store, None)
            .await
            .expect("build app");
    state
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    blocking_store.wait_until_blocked().await;

    let shutdown = tokio::runtime::Handle::current().spawn(lifecycle.shutdown());
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the active bounded grep step"
    );
    blocking_store.release();
    shutdown
        .await
        .expect("join shutdown")
        .expect("drain grep step");
}

/// A job that does nothing but count the steps the runner admitted for it.
///
/// It is registered on the server's own writer, so it queues, waits for a
/// permit, and is shut down through exactly the admission every other job
/// goes through. Counting is the whole point: an ordinary step's work is
/// object-store traffic, and the question this test asks is whether any of
/// it is issued at all once a shutdown has begun.
struct StepCountingJob {
    id: MaintenanceJobId,
    steps: Arc<AtomicUsize>,
}

#[async_trait]
impl MaintenanceJob for StepCountingJob {
    fn id(&self) -> MaintenanceJobId {
        self.id
    }

    async fn step(
        &self,
        _namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> loonfs::Result<MaintenanceStepResult> {
        self.steps.fetch_add(1, Ordering::SeqCst);
        // Idle rather than progressed: a requeueing step would never let
        // the control settle below.
        Ok(MaintenanceStepResult::concluded(
            MaintenanceStepConclusion::Idle,
        ))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> loonfs::Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

/// The server closes maintenance admission before it starts draining
/// publications, mirroring the rule `FsWriter::shutdown_background` holds
/// internally.
///
/// The drain is a wait, and it is the whole window: while it runs, the
/// runner's timer is still promoting deadlines and every publication that
/// lands still fires the observer that nudges the grep index. A nudge that
/// arrives in that window must find the door already shut, or the shutdown
/// spends it starting work it is about to throw away — and then waits for
/// that work to finish.
///
/// The observation is behavioral rather than a flag read, and it is pinned
/// on the shutdown's first poll rather than on wall-clock timing: nudge
/// after that poll, and no step may follow.
#[tokio::test]
async fn shutdown_closes_maintenance_admission_before_draining_publications() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("shutdown-order");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("construct local store"),
        KeyPredicate::wal_head(namespace_id.as_str()),
        OperationClass::CompareAndSwap,
    ));
    let config = test_config(temp_dir.path(), "shutdown-order-server");
    let (_router, lifecycle, state) = super::app_with_store_and_transfer_issuer(
        config,
        blocking.clone() as SharedObjectStore,
        None,
    )
    .await
    .expect("build app");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let steps = Arc::new(AtomicUsize::new(0));
    let job = MaintenanceJobId::new("shutdown-order-probe");
    state
        .writer
        .register_maintenance_job(Arc::new(StepCountingJob {
            id: job,
            steps: Arc::clone(&steps),
        }))
        .expect("register the counting job");

    // The control. Without it, a later count of zero would prove only that
    // this job never ran under any conditions.
    state.writer.maintenance().nudge(job, &namespace_id);
    lifecycle
        .wait_for_maintenance()
        .await
        .expect("settle the admitted step");
    let admitted_while_serving = steps.load(Ordering::SeqCst);
    assert_eq!(
        admitted_while_serving, 1,
        "a nudge on a serving deployment admits one step"
    );

    // Park a publication so the shutdown's publication drain is still
    // pending when its first poll returns — the window the runner would
    // otherwise keep admitting into.
    blocking.block_next();
    let put = tokio::spawn({
        let writer = state.writer.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer
                .put_file_bytes(
                    &namespace_id,
                    "/parked.txt",
                    b"body",
                    PutFileOptions::default(),
                )
                .await
        }
    });
    blocking.wait_until_blocked().await;

    let mut shutdown = Box::pin(lifecycle.shutdown());
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "the parked publication must keep the shutdown pending"
    );
    // Everything after this point is the drain window.
    state.writer.maintenance().nudge(job, &namespace_id);

    blocking.release();
    put.await
        .expect("join the parked put")
        .expect("the released put succeeds");
    // Releasing the put also lets it publish, which fires the publish
    // observer's own nudge — the production path into this same window.
    shutdown
        .await
        .expect("the shutdown settles with its queue discarded");

    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "no maintenance step may be admitted once the shutdown has begun"
    );
    // And the runner stays shut rather than reopening behind the drain.
    state.writer.maintenance().nudge(job, &namespace_id);
    state
        .writer
        .wait_for_background_work()
        .await
        .expect("a shut runner has nothing left to settle");
    assert_eq!(
        steps.load(Ordering::SeqCst),
        admitted_while_serving,
        "a nudge after the shutdown must admit nothing either"
    );
}

#[tokio::test]
async fn the_publish_observer_nudges_the_enabled_namespaces_index() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "grep-observer-server");
    let (_router, lifecycle, state) =
        super::app_with_store_and_transfer_issuer(config, store, None)
            .await
            .expect("build app");
    let namespace_id = namespace_id("grep-observer");
    state
        .writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    state
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    state
        .grep_maintenance
        .as_ref()
        .expect("an index-maintaining app carries a maintenance handle")
        .nudge(&namespace_id);
    lifecycle
        .wait_for_maintenance()
        .await
        .expect("settle the backfill");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(0),
        "an empty namespace's backfill completes at its own head"
    );

    // The publish is the only trigger from here on: nothing below nudges.
    state
        .writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"observer-driven needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("publish file");
    lifecycle
        .wait_for_maintenance()
        .await
        .expect("settle the observer-driven step");
    assert_eq!(
        built_through_seq(&state, &namespace_id).await,
        ChangeSeq(1),
        "the publish observer is what carried the index to the new head"
    );
    let request = GrepRequest {
        pattern: "observer-driven needle".to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    };
    let service = state
        .grep_service
        .as_ref()
        .expect("a query-serving app carries a grep service");
    let store = state.writer.object_store();
    let reads = NamespaceReads::new(&state.reader, &namespace_id);
    let snapshot = GrepIndexSnapshot::from_grep_root(&*store, &namespace_id, service).await;
    let response = service
        .query(&request, &snapshot, &reads, &store)
        .await
        .expect("grep caught-up index");
    assert_eq!(response.matches.len(), 1);
    lifecycle.shutdown().await.expect("drain lifecycle");
}

/// What the index's steps published, read where an operator reads it.
async fn built_through_seq(state: &AppState, namespace_id: &NamespaceId) -> ChangeSeq {
    load_grep_root(&*state.writer.object_store(), namespace_id)
        .await
        .expect("load grep root")
        .expect("an enabled namespace has a grep root")
        .state()
        .lifecycle()
        .steady_watermark()
        .expect("a steady grep root has a watermark")
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_disabled_root_is_not_materialized_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-disabled");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    let worker = grep_error_worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable grep");
    worker.disable(&namespace_id).await.expect("disable grep");
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "disabled-server").await;
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        501,
        ErrorCode::NotSupported,
        "not enabled",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_mid_backfill_is_not_materialized_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-backfill");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    grep_error_worker(&store)
        .await
        .enable(&namespace_id)
        .await
        .expect("leave grep backfilling");
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "backfill-server").await;
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        501,
        ErrorCode::NotSupported,
        "backfill has not completed",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_store_outage_is_provider_failure_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-error-store");
    let fault_store = Arc::new(FaultGrepRootStore::new(temp_dir.path(), &namespace_id));
    let store = fault_store.clone() as SharedObjectStore;
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "store-server").await;
    fault_store.fail_next_root_read();
    let client = &harness.client;
    let binding = grep_error_request();
    let result = client.grep(&namespace_id, &binding);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        500,
        ErrorCode::ServerError,
        "injected grep-root outage",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_corrupt_pointer_is_index_corrupt_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-pointer");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    store
        .put_overwrite(
            &grep_root_key(&namespace_id),
            Bytes::from_static(b"corrupt grep pointer"),
        )
        .await
        .expect("write corrupt grep pointer");
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "pointer-server").await;
    assert_index_corrupt_and_core_read(harness, namespace_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_missing_manifest_is_index_corrupt_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-missing-manifest");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    let manifest_id =
        GrepManifestId::parse("1111111111111111111111111111111111111111111111111111111111111111")
            .expect("manifest id");
    write_grep_pointer(&*store, &namespace_id, namespace_id.clone(), manifest_id).await;
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "missing-manifest-server").await;
    assert_index_corrupt_and_core_read(harness, namespace_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_corrupt_manifest_is_index_corrupt_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-manifest");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    let manifest_id =
        GrepManifestId::parse("2222222222222222222222222222222222222222222222222222222222222222")
            .expect("manifest id");
    store
        .put_overwrite(
            &grep_manifest_key(&namespace_id, &manifest_id),
            Bytes::from_static(b"corrupt grep manifest"),
        )
        .await
        .expect("write corrupt grep manifest");
    write_grep_pointer(&*store, &namespace_id, namespace_id.clone(), manifest_id).await;
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "manifest-server").await;
    assert_index_corrupt_and_core_read(harness, namespace_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_identity_mismatch_is_index_corrupt_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let namespace_id = namespace_id("grep-error-identity");
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    let manifest_id =
        GrepManifestId::parse("3333333333333333333333333333333333333333333333333333333333333333")
            .expect("manifest id");
    write_grep_pointer(
        &*store,
        &namespace_id,
        NamespaceId::parse("different-grep-identity").expect("different namespace id"),
        manifest_id,
    )
    .await;
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_error_server(store, temp_dir.path(), "identity-server").await;
    assert_index_corrupt_and_core_read(harness, namespace_id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grep_error_publication_conflict_is_stale_head_and_core_reads_survive() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("grep-error-conflict");
    let fault_store = Arc::new(FaultGrepRootStore::new(temp_dir.path(), &namespace_id));
    let store = fault_store.clone() as SharedObjectStore;
    let writer = seed_grep_error_namespace(&store, &namespace_id).await;
    writer.shutdown_background().await.expect("shutdown writer");

    let harness = start_grep_admin_error_server(store, temp_dir.path(), "conflict-server").await;
    fault_store.conflict_next_root_publication();
    let client = &harness.client;
    let result = client.enable_grep_index(&namespace_id);
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result.await,
        409,
        ErrorCode::StaleHead,
        "publication conflict",
    )
    .await;
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_created_state_is_readable_through_http() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = test_runtime(store.clone(), "runtime-writer").await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace through runtime");
    fs.put_file_bytes(
        &namespace_id,
        "/notes/hello.txt",
        b"hello from runtime",
        PutFileOptions {
            behavior: DestinationBehavior::NoReplace,
            commit_id: Some(CommitId::parse("runtime-put").expect("valid commit id")),
            message: None,
            expected_revision_no: None,
        },
    )
    .await
    .expect("write file through runtime");

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/hello.txt").expect("target");
    let stat = harness.client.stat_path(&target).await.expect("stat file");
    assert_eq!(stat.absolute_path, "/notes/hello.txt");
    assert_eq!(stat.size_bytes, Some(18));
    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello from runtime");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_created_state_is_readable_through_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = test_runtime(store.clone(), "runtime-reader").await;
    let harness = start_server(store.clone(), temp_dir.path(), "server-writer").await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace through http");
    let target = NamespacePath::parse("demo", "/notes/from-http.txt").expect("target");
    harness
        .client
        .put_file_bytes(&target, b"hello from http", &replace_file_options())
        .await
        .expect("write file through http");

    let file = fs
        .reader()
        .get_file_bytes(
            &NamespaceId::parse("demo").expect("valid namespace id"),
            "/notes/from-http.txt",
        )
        .await
        .expect("read file through runtime");
    assert_eq!(file.bytes, b"hello from http");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_missing_namespace_mutations_return_namespace_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let target = NamespacePath::parse("missing", "/notes/hello.txt").expect("target");
    assert_api_error(
        harness
            .client
            .put_file_bytes(&target, b"hello", &replace_file_options())
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );
    assert_api_error(
        harness
            .client
            .delete_path(&target, &DeleteOptions::default())
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );
    let destination = NamespacePath::parse("missing", "/notes/renamed.txt").expect("target");
    assert_api_error(
        harness
            .client
            .move_path(
                &target,
                &destination,
                DestinationBehavior::NoReplace,
                &CommitOptions::default(),
            )
            .await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_missing_namespace_reads_return_namespace_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let target = NamespacePath::parse("missing", "/").expect("target");
    assert_api_error(
        harness.client.list_path_entries_all(&target).await,
        404,
        "namespace_not_found",
        Some("namespace `missing` does not exist"),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_missing_path_returns_path_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/missing.txt").expect("target");
    assert_api_error(
        harness
            .client
            .delete_path(&target, &DeleteOptions::default())
            .await,
        404,
        "path_not_found",
        None,
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_over_directory_and_move_into_existing_target_return_path_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seeder = bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/readme.txt",
        b"readme",
        "seed-docs",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/tmp/a.txt",
        b"from tmp",
        "seed-tmp",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/a.txt",
        b"in docs",
        "seed-target",
    )
    .await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let dir_target = NamespacePath::parse("demo", "/docs").expect("dir target");
    assert_api_error(
        harness
            .client
            .put_file_bytes(&dir_target, b"not a file", &replace_file_options())
            .await,
        409,
        "path_conflict",
        None,
    );

    let from = NamespacePath::parse("demo", "/tmp/a.txt").expect("from");
    let to = NamespacePath::parse("demo", "/docs/a.txt").expect("to");
    assert_api_error(
        harness
            .client
            .move_path(
                &from,
                &to,
                DestinationBehavior::NoReplace,
                &CommitOptions::default(),
            )
            .await,
        409,
        "path_conflict",
        None,
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_and_move_under_deleted_ancestor_create_fresh_subtrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seeder = bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/docs/old.txt",
        b"old",
        "seed-docs",
    )
    .await;
    write_file_bytes(
        &seeder,
        &namespace_id("demo"),
        "/tmp/source.txt",
        b"source",
        "seed-source",
    )
    .await;
    delete_path_recursive(&seeder, &namespace_id("demo"), "/docs", "delete-docs").await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    // The deleted name is invisible and immediately reusable; the
    // dead subtree's children stay dead.
    let put_target = NamespacePath::parse("demo", "/docs/new.txt").expect("put target");
    harness
        .client
        .put_file_bytes(&put_target, b"new", &replace_file_options())
        .await
        .expect("put recreates the subtree");
    let old_child = NamespacePath::parse("demo", "/docs/old.txt").expect("old child");
    assert_api_error(
        harness.client.stat_path(&old_child).await,
        404,
        "path_not_found",
        None,
    );

    let from = NamespacePath::parse("demo", "/tmp/source.txt").expect("from");
    let to = NamespacePath::parse("demo", "/docs/source.txt").expect("to");
    harness
        .client
        .move_path(
            &from,
            &to,
            DestinationBehavior::NoReplace,
            &CommitOptions::default(),
        )
        .await
        .expect("move lands in the recreated subtree");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_path_mutation_retries_transient_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(StaleHeadOnceStore::new(temp_dir.path(), "demo")) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/race.txt").expect("target");
    let result = harness
        .client
        .put_file_bytes(&target, b"race", &replace_file_options())
        .await
        .expect("path write retries stale head");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_first_write_takes_over_a_namespace_owned_by_another_writer() {
    // With no lease, the server's first semantic write acquires the
    // epoch immediately and fences the previous session.
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "other-writer", &namespace_id("demo")).await;
    let store_for_check = store.clone();

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    let target = NamespacePath::parse("demo", "/notes/taken-over.txt").expect("target");
    let result = harness
        .client
        .put_file_bytes(&target, b"taken over", &replace_file_options())
        .await
        .expect("first write takes over the namespace");
    assert_eq!(result.committed_seq, ChangeSeq(1));

    let head = loonfs::control::load_namespace_head_control(
        store_for_check.as_ref(),
        &namespace_id("demo"),
    )
    .await
    .expect("read head")
    .state;
    assert_eq!(
        head.writer.expect("writer block").writer_id,
        "server-writer"
    );

    harness.server.abort();
}

struct TestHarness {
    client: Client,
    server: tokio::task::JoinHandle<()>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_answers_401_in_envelope_for_missing_and_wrong_tokens() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    for auth_token in [None, Some("wrong-token".to_owned())] {
        let client = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config");
        assert_api_error(
            client.namespace_status(&namespace_id("demo")).await,
            401,
            "unauthorized",
            Some("missing or invalid bearer token"),
        );
    }

    server.abort();
}

/// Malformed query strings, path parameters, and JSON bodies answer inside
/// the JSON error envelope as `invalid_request` — never as a framework
/// plain-text rejection — and authorization is checked first, so the same
/// malformed request without credentials answers 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_malformed_request_pieces_answer_in_envelope_behind_auth() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let expect_enveloped = |error: ureq::Error, status: u16, code: &str| {
        let ureq::Error::Status(actual_status, response) = error else {
            panic!("expected a status error, got {error:?}");
        };
        assert_eq!(actual_status, status);
        assert!(response.header("x-request-id").is_some());
        let body = response.into_string().expect("read error body");
        let body: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|_| panic!("json body, got: {body}"));
        assert_eq!(body["code"], code);
    };

    // A query value that fails its field type: enveloped invalid_request.
    let changes_url = format!("http://{addr}/v0/namespaces/demo/changes?after_seq=abc");
    let error = raw_agent()
        .get(&changes_url)
        .set("authorization", "Bearer test-token")
        .call()
        .expect_err("malformed after_seq should answer 400");
    expect_enveloped(error, 400, "invalid_request");

    // The same malformed query without credentials: 401 wins.
    let error = raw_agent()
        .get(&changes_url)
        .call()
        .expect_err("unauthorized should answer 401");
    expect_enveloped(error, 401, "unauthorized");

    // A missing required query parameter: enveloped invalid_request.
    let error = raw_agent()
        .get(&format!("http://{addr}/v0/namespaces/demo/filesystem/stat"))
        .set("authorization", "Bearer test-token")
        .call()
        .expect_err("missing path parameter should answer 400");
    expect_enveloped(error, 400, "invalid_request");

    // A malformed JSON body: enveloped invalid_request with credentials,
    // 401 without — the body is not read before authorization.
    let create_url = format!("http://{addr}/v0/namespaces");
    let error = raw_agent()
        .post(&create_url)
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_string("{not json")
        .expect_err("malformed body should answer 400");
    expect_enveloped(error, 400, "invalid_request");
    let error = raw_agent()
        .post(&create_url)
        .set("content-type", "application/json")
        .send_string("{not json")
        .expect_err("unauthorized malformed body should answer 401");
    expect_enveloped(error, 401, "unauthorized");

    // Commit operation paths now validate while the authorized JSON body
    // is decoded. The served code stays the same invalid_request
    // classification the former handler-boundary validation used.
    let commits_url = format!("http://{addr}/v0/namespaces/demo/commits");
    let invalid_operation = r#"{
        "commit_id":"invalid-path",
        "operations":[{"kind":"create_directory","path":"relative"}]
    }"#;
    let error = raw_agent()
        .post(&commits_url)
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_string(invalid_operation)
        .expect_err("invalid operation path should answer 400");
    expect_enveloped(error, 400, "invalid_request");
    let error = raw_agent()
        .post(&commits_url)
        .set("content-type", "application/json")
        .send_string(invalid_operation)
        .expect_err("authorization should precede operation path decoding");
    expect_enveloped(error, 401, "unauthorized");

    // Grep scope paths make the same boundary move and retain the same
    // invalid_request code.
    let grep_url = format!("http://{addr}/v0/namespaces/demo/query/grep");
    let invalid_grep = r#"{"pattern":"needle","path_prefix":"relative"}"#;
    let error = raw_agent()
        .post(&grep_url)
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_string(invalid_grep)
        .expect_err("invalid grep path should answer 400");
    expect_enveloped(error, 400, "invalid_request");
    let error = raw_agent()
        .post(&grep_url)
        .set("content-type", "application/json")
        .send_string(invalid_grep)
        .expect_err("authorization should precede grep path decoding");
    expect_enveloped(error, 401, "unauthorized");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_body_over_the_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_upload_bytes = 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    let target = NamespacePath::parse("demo", "/big.bin").expect("target");
    assert_api_error(
        client
            .put_file_bytes(&target, &[0u8; 4096], &replace_file_options())
            .await,
        413,
        "content_too_large",
        None,
    );
    // A body inside the limit still goes through on the same route.
    client
        .put_file_bytes(&target, &[0u8; 512], &replace_file_options())
        .await
        .expect("small upload fits under the limit");

    server.abort();
}

/// A payload with a distinct byte at every offset, so bytes landing in the
/// wrong order or twice cannot go unnoticed.
fn distinct_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// What the write path is allowed to hold at once, and what it is asked to
/// carry: three internal parts' worth, so a path that materializes its
/// payload is caught by more than a rounding error.
const MEMORY_BOUND_PART_BYTES: u64 = loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES;
const MEMORY_BOUND_PAYLOAD_BYTES: usize = 3 * MEMORY_BOUND_PART_BYTES as usize + 4_096;

/// The proxied upload route must not materialize its request body.
///
/// This is measured, not asserted about the process: the store is wrapped in
/// a watcher that records every payload buffer handed across the object-store
/// boundary and, exactly, how many bytes of them are alive at any instant. A
/// route that buffered its body would hand the store one buffer the size of
/// the whole payload; a streaming one hands it a series of chunks and never
/// holds more than a part's worth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proxied_upload_route_never_holds_the_whole_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let watched = Arc::new(BufferWatchStore::watching_content(
        LocalFsStore::new(temp_dir.path()).expect("store"),
    ));
    let store = Arc::clone(&watched) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let payload = distinct_bytes(MEMORY_BOUND_PAYLOAD_BYTES);
    let target = NamespacePath::parse("demo", "/streamed.bin").expect("target");
    harness
        .client
        .put_file_bytes(&target, &payload, &replace_file_options())
        .await
        .expect("a multi-part payload uploads through the proxied route");

    let peaks = watched.peaks();
    assert_eq!(
        peaks.total_bytes, MEMORY_BOUND_PAYLOAD_BYTES as u64,
        "every payload byte crossed the store boundary exactly once"
    );
    assert!(
        peaks.largest_buffer_bytes <= MEMORY_BOUND_PART_BYTES,
        "no single buffer may exceed one part: largest was {}",
        peaks.largest_buffer_bytes
    );
    assert!(
        peaks.peak_live_bytes <= MEMORY_BOUND_PART_BYTES,
        "the write path held {} bytes at once, past its one-part window",
        peaks.peak_live_bytes
    );

    // And the bytes are the bytes.
    let read_back = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read the streamed object back");
    assert_eq!(read_back, payload);

    harness.server.abort();
}

/// The same bound one layer down, on the primitive the route depends on.
/// Driving `put_streamed` directly separates "the route streams" from "the
/// store writes incrementally", so a regression in either is attributable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_streamed_writes_a_multi_part_payload_one_part_at_a_time() {
    let temp_dir = tempdir().expect("tempdir");
    let watched =
        BufferWatchStore::watching_content(LocalFsStore::new(temp_dir.path()).expect("store"));

    let payload = distinct_bytes(MEMORY_BOUND_PAYLOAD_BYTES);
    let key = loonfs_objectstore::keys::content_blob(
        "cs_00000000000000000000000000000001",
        &loonfs_api::ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
    );
    // Chunks the size of an HTTP body's, not the store's: the boundaries a
    // caller hands over carry no meaning, and the store regroups them.
    let chunks: Vec<Bytes> = payload
        .chunks(64 * 1024)
        .map(Bytes::copy_from_slice)
        .collect();
    let stored = watched
        .put_streamed(
            &key,
            futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("stream a multi-part payload into the store");

    assert_eq!(stored, MEMORY_BOUND_PAYLOAD_BYTES as u64);
    let peaks = watched.peaks();
    assert_eq!(peaks.total_bytes, MEMORY_BOUND_PAYLOAD_BYTES as u64);
    assert!(
        peaks.largest_buffer_bytes <= MEMORY_BOUND_PART_BYTES,
        "no single buffer may exceed one part: largest was {}",
        peaks.largest_buffer_bytes
    );
    assert!(
        peaks.peak_live_bytes <= MEMORY_BOUND_PART_BYTES,
        "the store held {} bytes at once, past its one-part window",
        peaks.peak_live_bytes
    );
    assert_eq!(
        watched
            .get(&key, None)
            .await
            .expect("read back")
            .expect("object exists"),
        Bytes::from(payload)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_document_advertises_the_upload_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    let capabilities = harness
        .client
        .capabilities()
        .await
        .expect("fetch capability document");
    assert_eq!(
        capabilities.limits.get("upload.max_content_bytes").copied(),
        Some(256 * 1024 * 1024)
    );
    assert_eq!(
        capabilities
            .limits
            .get("download.max_content_bytes")
            .copied(),
        Some(256 * 1024 * 1024)
    );
    // Every limit a request can trip is discoverable: transfer
    // concurrency and the grep scan budgets.
    assert_eq!(
        capabilities.limits.get("upload.max_concurrent").copied(),
        Some(8)
    );
    assert_eq!(
        capabilities.limits.get("download.max_concurrent").copied(),
        Some(16)
    );
    assert_eq!(
        capabilities
            .limits
            .get("query.grep.scan_budget_files")
            .copied(),
        Some(4096)
    );
    assert_eq!(
        capabilities
            .limits
            .get("query.grep.tail_budget_files")
            .copied(),
        Some(512)
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_unknown_routes_and_methods_answer_in_envelope() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    // Unknown path: in-envelope 404 instead of axum's empty body.
    let error = raw_agent()
        .get(&format!("http://{addr}/v0/nonexistent"))
        .call()
        .expect_err("unknown route should answer 404");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for an unknown route");
    };
    assert_eq!(status, 404);
    assert!(response.header("x-request-id").is_some());
    let body = response.into_string().expect("read 404 body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json 404 body");
    assert_eq!(body["code"], "route_not_found");

    // Served path, unserved method: in-envelope 405.
    let error = raw_agent()
        .delete(&format!("http://{addr}/v0/capabilities"))
        .call()
        .expect_err("wrong method should answer 405");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for a wrong method");
    };
    assert_eq!(status, 405);
    let body = response.into_string().expect("read 405 body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json 405 body");
    assert_eq!(body["code"], "method_not_allowed");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_revisions_cursor_resumes_after_head_drift_and_rejects_the_future() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/file.txt",
        b"one",
        "c-rev1",
    )
    .await;
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/file.txt",
        b"two",
        "c-rev2",
    )
    .await;

    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let cursor = tokio::task::spawn_blocking({
        move || {
            let response = raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/revisions"
                ))
                .set("authorization", "Bearer test-token")
                .query("path", "/notes/file.txt")
                .query("limit", "1")
                .call()
                .expect("first revisions page");
            let body = response.into_string().expect("read revisions body");
            let body: serde_json::Value = serde_json::from_str(&body).expect("json revisions");
            body["next_cursor"]
                .as_str()
                .expect("two revisions produce a next_cursor")
                .to_owned()
        }
    })
    .await
    .expect("join blocking task");

    // A commit landing mid-listing does not retire the cursor: the resume
    // continues after the last returned revision against the new head.
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/other.txt",
        b"x",
        "c-rev3",
    )
    .await;

    let resumed = tokio::task::spawn_blocking({
        let cursor = cursor.clone();
        move || {
            let response = raw_agent()
                .get(&format!(
                    "http://{addr}/v0/namespaces/demo/filesystem/revisions"
                ))
                .set("authorization", "Bearer test-token")
                .query("path", "/notes/file.txt")
                .query("limit", "1")
                .query("cursor", &cursor)
                .call()
                .expect("cursor resumes after head drift");
            let body = response.into_string().expect("read resumed body");
            serde_json::from_str::<serde_json::Value>(&body).expect("json resumed body")
        }
    })
    .await
    .expect("join blocking task");
    assert_eq!(resumed["revisions"][0]["revision_no"], 1);
    assert!(resumed["next_cursor"].is_null());

    // A cursor from the future stays unanswerable.
    let mut future_cursor: loonfs_api::FileRevisionsPageCursor =
        loonfs_api::decode_cursor(&cursor).expect("decode revisions cursor");
    future_cursor.head_seq = loonfs_api::ChangeSeq(future_cursor.head_seq.0 + 1000);
    let future_cursor = loonfs_api::encode_cursor(&future_cursor).expect("encode future cursor");
    let error = raw_agent()
        .get(&format!(
            "http://{addr}/v0/namespaces/demo/filesystem/revisions"
        ))
        .set("authorization", "Bearer test-token")
        .query("path", "/notes/file.txt")
        .query("limit", "1")
        .query("cursor", &future_cursor)
        .call()
        .expect_err("future cursor should answer rebootstrap_required");
    let ureq::Error::Status(status, response) = error else {
        panic!("expected a status error for a future cursor");
    };
    assert_eq!(status, 409);
    let body = response.into_string().expect("read future-cursor body");
    let body: serde_json::Value = serde_json::from_str(&body).expect("json future-cursor body");
    assert_eq!(body["code"], "rebootstrap_required");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_uploads_answer_server_busy_at_the_concurrency_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_uploads = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    // Hold the only buffering slot, standing in for a slow concurrent
    // upload; the next proxied body must be refused before buffering.
    let held = state
        .upload_permits
        .clone()
        .try_acquire_owned()
        .expect("hold the only upload slot");

    let client_config = ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        // These tests assert the raw concurrency-cap answer; the client's
        // transient retry would otherwise sleep through it.
        disable_transient_retry: true,
        ca_cert_path: None,
    };
    let config_for_busy = client_config.clone();
    let client = Client::new(config_for_busy).expect("valid client config");
    let target = NamespacePath::parse("demo", "/one.bin").expect("target");
    assert_api_error(
        client
            .put_file_bytes(&target, &[0u8; 64], &replace_file_options())
            .await,
        503,
        "server_busy",
        Some("the server is at its concurrency limit for proxied uploads; retry shortly"),
    );

    drop(held);
    let client = Client::new(client_config).expect("valid client config");
    let target = NamespacePath::parse("demo", "/one.bin").expect("target");
    client
        .put_file_bytes(&target, &[0u8; 64], &replace_file_options())
        .await
        .expect("a freed slot admits the upload");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_transient_retry_rides_out_a_briefly_full_upload_slot() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_uploads = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let held = state
        .upload_permits
        .clone()
        .try_acquire_owned()
        .expect("hold the only upload slot");
    // Free the slot while the client sleeps between attempts: the first
    // try answers server_busy, a later retry lands. An isolated timer is
    // the point of this test — it exercises the client's real backoff.
    #[allow(clippy::disallowed_methods)]
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        drop(held);
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    let target = NamespacePath::parse("demo", "/retried.bin").expect("target");
    client
        .put_file_bytes(&target, &[0u8; 64], &replace_file_options())
        .await
        .expect("transient retry rides out the briefly full slot");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_content_reads_answer_server_busy_at_the_concurrency_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seed_writer = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/note.txt",
        b"bounded",
        "download-busy-seed-01",
    )
    .await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_concurrent_downloads = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let held = state
        .download_permits
        .clone()
        .try_acquire_owned()
        .expect("hold the only download slot");

    let client_config = ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        // These tests assert the raw concurrency-cap answer; the client's
        // transient retry would otherwise sleep through it.
        disable_transient_retry: true,
        ca_cert_path: None,
    };
    let config_for_busy = client_config.clone();
    let client = Client::new(config_for_busy).expect("valid client config");
    let target = NamespacePath::parse("demo", "/note.txt").expect("target");
    assert_api_error(
        client.get_file_bytes(&target).await,
        503,
        "server_busy",
        Some("the server is at its concurrency limit for proxied content reads; retry shortly"),
    );

    drop(held);
    let client = Client::new(client_config).expect("valid client config");
    let target = NamespacePath::parse("demo", "/note.txt").expect("target");
    let bytes = client
        .get_file_bytes(&target)
        .await
        .expect("a freed slot admits the read");
    assert_eq!(bytes, b"bounded");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_content_read_over_the_download_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let seed_writer = bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/big.bin",
        &[0u8; 64],
        "download-limit-seed-01",
    )
    .await;
    write_file_bytes(
        &seed_writer,
        &namespace_id("demo"),
        "/small.bin",
        &[0u8; 8],
        "download-limit-seed-02",
    )
    .await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_download_bytes = 16;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".to_owned()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("valid client config");
    assert_api_error(
        client
            .get_file_bytes(&NamespacePath::parse("demo", "/big.bin").expect("target"))
            .await,
        413,
        "content_too_large",
        None,
    );
    // Content inside the limit still reads through the same route.
    let bytes = client
        .get_file_bytes(&NamespacePath::parse("demo", "/small.bin").expect("target"))
        .await
        .expect("small content fits under the limit");
    assert_eq!(bytes.len(), 8);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_answers_ready_then_shutting_down_once_admission_closes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let config = test_config(temp_dir.path(), "server-writer");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (router, state) = app_with_store_and_state(config, store)
        .await
        .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    let ready_url = format!("http://{addr}/readiness");
    let url = ready_url.clone();
    let body = raw_agent()
        .get(&url)
        .call()
        .expect("an admitting server is ready")
        .into_string()
        .expect("readiness body");
    assert_eq!(body, "ready");

    state.writer.publisher().close_admission();

    tokio::task::spawn_blocking(move || match raw_agent().get(&ready_url).call() {
        Err(ureq::Error::Status(503, response)) => {
            let body = response.into_string().expect("readiness body");
            assert!(
                body.contains("shutting_down"),
                "readiness names the shutdown: {body}"
            );
        }
        other => panic!("expected 503 from a draining server, got {other:?}"),
    })
    .await
    .expect("join blocking task");

    server.abort();
}

async fn seed_grep_error_namespace(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> FsWriter {
    let writer = test_runtime(store.clone(), "grep-error-seed").await;
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create grep-error namespace");
    writer
        .put_file_bytes(
            namespace_id,
            "/core.txt",
            b"core remains readable",
            PutFileOptions::default(),
        )
        .await
        .expect("write core isolation sentinel");
    writer
}

async fn grep_error_worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    grep_worker(store, "grep-error-worker").await
}

/// A worker composed the way the server composes its own: grep's keyspace
/// on the given store, its filesystem reads and checkpoints on handles over
/// the same store.
async fn grep_worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store.clone(), reader, admin)
}

fn grep_error_request() -> GrepRequest {
    GrepRequest {
        pattern: "needle".to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    }
}

async fn write_grep_pointer(
    store: &dyn ObjectStore,
    stored_namespace_id: &NamespaceId,
    pointer_namespace_id: NamespaceId,
    manifest_id: GrepManifestId,
) {
    let envelope =
        GrepRootEnvelope::from_pointer(GrepRootPointer::new(pointer_namespace_id, manifest_id))
            .expect("build grep pointer");
    store
        .put_overwrite(
            &grep_root_key(stored_namespace_id),
            Bytes::from(encode_grep_root(&envelope).expect("encode grep pointer")),
        )
        .await
        .expect("write grep pointer");
}

/// A deployment that answers searches over an index it does not maintain:
/// exactly the query error surface these tests are about, with no
/// maintenance step racing the fault they injected.
async fn start_grep_error_server(
    store: SharedObjectStore,
    root: &Path,
    writer_id: &str,
) -> TestHarness {
    let mut config = test_config(root, writer_id);
    config.grep.mode = crate::config::GrepMode::ServeOnly;
    start_server_with_config(store, config).await
}

/// Administering a grep root belongs to a deployment that maintains one.
async fn start_grep_admin_error_server(
    store: SharedObjectStore,
    root: &Path,
    writer_id: &str,
) -> TestHarness {
    let mut config = test_config(root, writer_id);
    config.grep.mode = crate::config::GrepMode::ServeAndMaintain;
    start_server_with_config(store, config).await
}

async fn assert_index_corrupt_and_core_read(harness: TestHarness, namespace_id: NamespaceId) {
    let client = &harness.client;
    let result = client.grep(&namespace_id, &grep_error_request()).await;
    assert_grep_api_error_and_core_read(
        client,
        &namespace_id,
        result,
        500,
        ErrorCode::IndexCorrupt,
        "disable and re-enable grep to rebuild it",
    )
    .await;
    harness.server.abort();
}

async fn assert_grep_api_error_and_core_read<T: std::fmt::Debug>(
    client: &Client,
    namespace_id: &NamespaceId,
    result: Result<T, ClientError>,
    status: u16,
    code: ErrorCode,
    message_fragment: &str,
) {
    match result {
        Err(ClientError::Api {
            status: actual_status,
            code: actual_code,
            feature,
            message,
            ..
        }) => {
            assert_eq!(actual_status, status);
            assert_eq!(actual_code, code.as_str());
            assert!(
                message.contains(message_fragment),
                "expected `{message_fragment}` in `{message}`"
            );
            if code == ErrorCode::NotSupported {
                assert_eq!(feature.as_deref(), Some("grep.index"));
            } else {
                assert_eq!(feature, None);
            }
        }
        other => panic!(
            "expected grep api error {status} {}, got {other:?}",
            code.as_str()
        ),
    }

    let target = NamespacePath::parse(namespace_id.as_str(), "/core.txt").expect("core target");
    let bytes = client
        .get_file_bytes(&target)
        .await
        .expect("grep failure must not affect core reads");
    assert_eq!(bytes, b"core remains readable");
}

async fn start_server(store: SharedObjectStore, root: &Path, writer_id: &str) -> TestHarness {
    start_server_with_config(store, test_config(root, writer_id)).await
}

async fn start_server_with_config(store: SharedObjectStore, config: ServerConfig) -> TestHarness {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    TestHarness {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".to_owned()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config"),
        server,
    }
}

async fn test_runtime(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

fn test_config(root: &Path, writer_id: &str) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: writer_id.to_owned(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: crate::config::GrepConfig::default(),
        maintenance: crate::config::MaintenanceMode::Automatic,
        min_publish_interval_ms: 0,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: root.display().to_string(),
            key_prefix: Some("http-tests".to_owned()),
        },
    }
}

/// Bootstraps a namespace through a second embedded runtime — seeding
/// durable state as `writer_id` would from another process — and returns
/// that runtime for follow-up seed writes.
async fn bootstrap_namespace(
    store: &SharedObjectStore,
    writer_id: &str,
    namespace_id: &NamespaceId,
) -> FsWriter {
    let writer = test_runtime(store.clone(), writer_id).await;
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap namespace");
    writer
}

async fn write_file_bytes(
    fs: &FsWriter,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    commit_id: &str,
) {
    fs.put_file_bytes(
        namespace_id,
        absolute_path,
        bytes,
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: Some(CommitId::parse(commit_id).expect("valid test commit id")),
            message: None,
            expected_revision_no: None,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("seed `{absolute_path}`: {error}"));
}

async fn delete_path_recursive(
    fs: &FsWriter,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    commit_id: &str,
) {
    fs.delete_path(
        namespace_id,
        absolute_path,
        DeleteOptions {
            behavior: DeleteDirectoryBehavior::Recursive,
            commit_id: Some(CommitId::parse(commit_id).expect("valid test commit id")),
            message: None,
            expected_inode_id: None,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("delete `{absolute_path}`: {error}"));
}

fn assert_api_error<T: std::fmt::Debug>(
    result: Result<T, ClientError>,
    status: u16,
    code: &str,
    message: Option<&str>,
) {
    match result {
        Err(ClientError::Api {
            status: actual_status,
            code: actual_code,
            message: actual_message,
            ..
        }) => {
            assert_eq!(actual_status, status);
            assert_eq!(actual_code, code);
            if let Some(expected_message) = message {
                assert_eq!(actual_message, expected_message);
            }
        }
        other => panic!("expected api error {status} {code}, got {other:?}"),
    }
}
