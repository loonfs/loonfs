#![allow(clippy::panic)]
// HTTP smoke helpers panic in unexpected match arms for precise diagnostics.

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

use super::error::status_for_core_error_code;
use super::{
    app_with_store, app_with_store_and_state, build_handles_with_metrics_jsonl_path,
    SharedObjectStore,
};
use crate::config::RuntimeCacheConfigOverrides;
use crate::{ServerConfig, StoreConfig};
use async_trait::async_trait;
use axum::body::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, DeleteOptions, FsWriter, PutFileOptions, TraceMode, TraceStoreKind,
};
use loonfs_api::ErrorCode;
use loonfs_api::{
    ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, GrepRequest, NamespaceId,
};
use loonfs_client::{Client, ClientConfig, ClientError, MutationOptions, NamespacePath};
use loonfs_grep::keyspace::root_key as grep_root_key;
use loonfs_grep::{GrepDriverParked, GrepWorker};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Notify;

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
struct BlockGrepRootOnceStore {
    inner: LocalFsStore,
    root_key: String,
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl BlockGrepRootOnceStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("construct local store"),
            root_key: grep_root_key(namespace_id),
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for BlockGrepRootOnceStore {
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
        if key == self.root_key && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.get_with_metadata(key).await
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
    })
    .expect("valid client config");
    tokio::task::spawn_blocking(move || {
        client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace over http");
    })
    .await
    .expect("join blocking task");

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
    let blocking_store = Arc::new(BlockGrepRootOnceStore::new(temp_dir.path(), &namespace_id));
    let store = blocking_store.clone() as SharedObjectStore;
    let writer = test_runtime(store.clone(), "grep-shutdown-seed").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    GrepWorker::new(
        store.clone(),
        "grep-shutdown-enable",
        "grep-shutdown-enable-session",
        "grep-shutdown-enable/0.1",
    )
    .enable(&namespace_id)
    .await
    .expect("enable grep");

    blocking_store.arm();
    let config = test_config(temp_dir.path(), "grep-shutdown-server");
    let (_router, lifecycle, state) =
        super::app_with_store_and_transfer_issuer(config, store, None)
            .await
            .expect("build app");
    state
        .grep_drivers
        .as_ref()
        .expect("embedded drivers")
        .start(&namespace_id);
    blocking_store.entered.notified().await;

    let shutdown = tokio::runtime::Handle::current().spawn(lifecycle.shutdown());
    tokio::task::yield_now().await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the active bounded grep step"
    );
    blocking_store.release.notify_one();
    shutdown
        .await
        .expect("join shutdown")
        .expect("drain grep step");
}

#[tokio::test]
async fn embedded_publish_observer_nudges_only_the_enabled_namespace_driver() {
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
        .grep_drivers
        .as_ref()
        .expect("embedded drivers")
        .start(&namespace_id);
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&namespace_id).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(0)
        })
    );

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
    assert_eq!(
        lifecycle.wait_for_grep_quiescence(&namespace_id).await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: ChangeSeq(1)
        })
    );
    let response = state
        .reader
        .grep(
            &namespace_id,
            &GrepRequest {
                pattern: "observer-driven needle".to_owned(),
                case_insensitive: false,
                path_prefix: None,
                cursor: None,
                limit: None,
                allow_stale: false,
                allow_scan: false,
            },
        )
        .await
        .expect("grep caught-up index");
    assert_eq!(response.matches.len(), 1);
    lifecycle.shutdown().await.expect("drain lifecycle");
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
        },
    )
    .await
    .expect("write file through runtime");

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo", "/notes/hello.txt").expect("target");
        let stat = harness.client.stat_path(&target).expect("stat file");
        assert_eq!(stat.absolute_path, "/notes/hello.txt");
        assert_eq!(stat.size_bytes, Some(18));
        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello from runtime");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_created_state_is_readable_through_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let fs = test_runtime(store.clone(), "runtime-reader").await;
    let harness = start_server(store.clone(), temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace through http");
        let target = NamespacePath::parse("demo", "/notes/from-http.txt").expect("target");
        harness
            .client
            .write_file_bytes(&target, b"hello from http", &MutationOptions::default())
            .expect("write file through http");
    })
    .await
    .expect("join blocking task");

    let file = fs
        .reader()
        .read_file_bytes(
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

    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("missing", "/notes/hello.txt").expect("target");
        assert_api_error(
            harness
                .client
                .write_file_bytes(&target, b"hello", &MutationOptions::default()),
            404,
            "namespace_not_found",
            Some("namespace `missing` does not exist"),
        );
        assert_api_error(
            harness
                .client
                .delete_path(&target, &MutationOptions::default()),
            404,
            "namespace_not_found",
            Some("namespace `missing` does not exist"),
        );
        let destination = NamespacePath::parse("missing", "/notes/renamed.txt").expect("target");
        assert_api_error(
            harness.client.move_path(
                &target,
                &destination,
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            ),
            404,
            "namespace_not_found",
            Some("namespace `missing` does not exist"),
        );
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_missing_namespace_reads_return_namespace_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("missing", "/").expect("target");
        assert_api_error(
            harness.client.list_path_all(&target),
            404,
            "namespace_not_found",
            Some("namespace `missing` does not exist"),
        );
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_missing_path_returns_path_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo", "/missing.txt").expect("target");
        assert_api_error(
            harness
                .client
                .delete_path(&target, &MutationOptions::default()),
            404,
            "path_not_found",
            None,
        );
    })
    .await
    .expect("join blocking task");

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
    tokio::task::spawn_blocking(move || {
        let dir_target = NamespacePath::parse("demo", "/docs").expect("dir target");
        assert_api_error(
            harness.client.write_file_bytes(
                &dir_target,
                b"not a file",
                &MutationOptions::default(),
            ),
            409,
            "path_conflict",
            None,
        );

        let from = NamespacePath::parse("demo", "/tmp/a.txt").expect("from");
        let to = NamespacePath::parse("demo", "/docs/a.txt").expect("to");
        assert_api_error(
            harness.client.move_path(
                &from,
                &to,
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            ),
            409,
            "path_conflict",
            None,
        );
    })
    .await
    .expect("join blocking task");

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
    tokio::task::spawn_blocking(move || {
        // The deleted name is invisible and immediately reusable; the
        // dead subtree's children stay dead.
        let put_target = NamespacePath::parse("demo", "/docs/new.txt").expect("put target");
        harness
            .client
            .write_file_bytes(&put_target, b"new", &MutationOptions::default())
            .expect("put recreates the subtree");
        let old_child = NamespacePath::parse("demo", "/docs/old.txt").expect("old child");
        assert_api_error(
            harness.client.stat_path(&old_child),
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
                &MutationOptions::default(),
            )
            .expect("move lands in the recreated subtree");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_path_mutation_retries_transient_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(StaleHeadOnceStore::new(temp_dir.path(), "demo")) as SharedObjectStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo", "/notes/race.txt").expect("target");
        let result = harness
            .client
            .write_file_bytes(&target, b"race", &MutationOptions::default())
            .expect("path write retries stale head");
        assert_eq!(result.committed_seq, ChangeSeq(1));
    })
    .await
    .expect("join blocking task");

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
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo", "/notes/taken-over.txt").expect("target");
        let result = harness
            .client
            .write_file_bytes(&target, b"taken over", &MutationOptions::default())
            .expect("first write takes over the namespace");
        assert_eq!(result.committed_seq, ChangeSeq(1));
    })
    .await
    .expect("join blocking task");

    let head = loonfs_core::control::load_namespace_head_control(
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

    tokio::task::spawn_blocking(move || {
        for auth_token in [None, Some("wrong-token".to_owned())] {
            let client = Client::new(ClientConfig {
                server_url: format!("http://{addr}"),
                auth_token,
                request_timeout_ms: None,
                disable_transient_retry: false,
            })
            .expect("valid client config");
            assert_api_error(
                client.namespace_status(&namespace_id("demo")),
                401,
                "unauthorized",
                Some("missing or invalid bearer token"),
            );
        }
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
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
        let error = ureq::get(&changes_url)
            .set("authorization", "Bearer test-token")
            .call()
            .expect_err("malformed after_seq should answer 400");
        expect_enveloped(error, 400, "invalid_request");

        // The same malformed query without credentials: 401 wins.
        let error = ureq::get(&changes_url)
            .call()
            .expect_err("unauthorized should answer 401");
        expect_enveloped(error, 401, "unauthorized");

        // A missing required query parameter: enveloped invalid_request.
        let error = ureq::get(&format!("http://{addr}/v0/namespaces/demo/filesystem/stat"))
            .set("authorization", "Bearer test-token")
            .call()
            .expect_err("missing path parameter should answer 400");
        expect_enveloped(error, 400, "invalid_request");

        // A malformed JSON body: enveloped invalid_request with credentials,
        // 401 without — the body is not read before authorization.
        let create_url = format!("http://{addr}/v0/namespaces");
        let error = ureq::post(&create_url)
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/json")
            .send_string("{not json")
            .expect_err("malformed body should answer 400");
        expect_enveloped(error, 400, "invalid_request");
        let error = ureq::post(&create_url)
            .set("content-type", "application/json")
            .send_string("{not json")
            .expect_err("unauthorized malformed body should answer 401");
        expect_enveloped(error, 401, "unauthorized");
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        let client = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: Some("test-token".to_owned()),
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config");
        let target = NamespacePath::parse("demo", "/big.bin").expect("target");
        assert_api_error(
            client.write_file_bytes(&target, &[0u8; 4096], &MutationOptions::default()),
            413,
            "content_too_large",
            None,
        );
        // A body inside the limit still goes through on the same route.
        client
            .write_file_bytes(&target, &[0u8; 512], &MutationOptions::default())
            .expect("small upload fits under the limit");
    })
    .await
    .expect("join blocking task");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_document_advertises_the_upload_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        let capabilities = harness
            .client
            .capabilities()
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
        // concurrency, the commit-body cap, and the grep scan budgets.
        assert_eq!(
            capabilities.limits.get("upload.max_concurrent").copied(),
            Some(8)
        );
        assert_eq!(
            capabilities.limits.get("download.max_concurrent").copied(),
            Some(16)
        );
        assert_eq!(
            capabilities.limits.get("commit.max_body_bytes").copied(),
            Some(8 * 1024 * 1024)
        );
        assert_eq!(
            capabilities.limits.get("commit.max_operations").copied(),
            Some(4096)
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
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        // Unknown path: in-envelope 404 instead of axum's empty body.
        let error = ureq::get(&format!("http://{addr}/v0/nonexistent"))
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
        let error = ureq::delete(&format!("http://{addr}/v0/config"))
            .call()
            .expect_err("wrong method should answer 405");
        let ureq::Error::Status(status, response) = error else {
            panic!("expected a status error for a wrong method");
        };
        assert_eq!(status, 405);
        let body = response.into_string().expect("read 405 body");
        let body: serde_json::Value = serde_json::from_str(&body).expect("json 405 body");
        assert_eq!(body["code"], "method_not_allowed");
    })
    .await
    .expect("join blocking task");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_body_over_the_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    bootstrap_namespace(&store, "runtime-writer", &namespace_id("demo")).await;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.max_commit_body_bytes = 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app_with_store(config, store).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    tokio::task::spawn_blocking(move || {
        // The limit rejects on size before any parsing, so an oversized
        // JSON-shaped body is enough to exercise it.
        let oversized = format!(r#"{{"filler":"{}"}}"#, "d".repeat(4096));
        let error = ureq::post(&format!("http://{addr}/v0/namespaces/demo/commits"))
            .set("content-type", "application/json")
            .send_string(&oversized)
            .expect_err("unauthorized commit should fail before buffering");
        let ureq::Error::Status(status, _) = error else {
            panic!("expected a status error for an unauthorized commit");
        };
        assert_eq!(status, 401);

        let error = ureq::post(&format!("http://{addr}/v0/namespaces/demo/commits"))
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/json")
            .send_string(&oversized)
            .expect_err("over-limit commit body should answer 413");
        let ureq::Error::Status(status, response) = error else {
            panic!("expected a status error for an over-limit commit body");
        };
        assert_eq!(status, 413);
        let body = response.into_string().expect("read 413 body");
        let body: serde_json::Value = serde_json::from_str(&body).expect("json 413 body");
        assert_eq!(body["code"], "content_too_large");
        let message = body["message"].as_str().expect("message string");
        assert!(
            message.contains("commit.max_body_bytes"),
            "413 guidance should name the commit-body limit, got: {message}"
        );
    })
    .await
    .expect("join blocking task");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_stale_revisions_cursor_answers_rebootstrap_required() {
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
            let response = ureq::get(&format!(
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

    // Any commit advances the head and retires outstanding cursors.
    write_file_bytes(
        &fs,
        &namespace_id("demo"),
        "/notes/other.txt",
        b"x",
        "c-rev3",
    )
    .await;

    tokio::task::spawn_blocking(move || {
        let error = ureq::get(&format!(
            "http://{addr}/v0/namespaces/demo/filesystem/revisions"
        ))
        .set("authorization", "Bearer test-token")
        .query("path", "/notes/file.txt")
        .query("limit", "1")
        .query("cursor", &cursor)
        .call()
        .expect_err("stale cursor should answer rebootstrap_required");
        let ureq::Error::Status(status, response) = error else {
            panic!("expected a status error for a stale cursor");
        };
        assert_eq!(status, 409);
        let body = response.into_string().expect("read stale-cursor body");
        let body: serde_json::Value = serde_json::from_str(&body).expect("json stale-cursor body");
        assert_eq!(body["code"], "rebootstrap_required");
    })
    .await
    .expect("join blocking task");

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
    };
    let config_for_busy = client_config.clone();
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config_for_busy).expect("valid client config");
        let target = NamespacePath::parse("demo", "/one.bin").expect("target");
        assert_api_error(
            client.write_file_bytes(&target, &[0u8; 64], &MutationOptions::default()),
            503,
            "server_busy",
            Some("the server is at its concurrency limit for proxied uploads; retry shortly"),
        );
    })
    .await
    .expect("join blocking task");

    drop(held);
    tokio::task::spawn_blocking(move || {
        let client = Client::new(client_config).expect("valid client config");
        let target = NamespacePath::parse("demo", "/one.bin").expect("target");
        client
            .write_file_bytes(&target, &[0u8; 64], &MutationOptions::default())
            .expect("a freed slot admits the upload");
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        let client = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: Some("test-token".to_owned()),
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config");
        let target = NamespacePath::parse("demo", "/retried.bin").expect("target");
        client
            .write_file_bytes(&target, &[0u8; 64], &MutationOptions::default())
            .expect("transient retry rides out the briefly full slot");
    })
    .await
    .expect("join blocking task");

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
    };
    let config_for_busy = client_config.clone();
    tokio::task::spawn_blocking(move || {
        let client = Client::new(config_for_busy).expect("valid client config");
        let target = NamespacePath::parse("demo", "/note.txt").expect("target");
        assert_api_error(
            client.read_file_bytes(&target),
            503,
            "server_busy",
            Some("the server is at its concurrency limit for proxied content reads; retry shortly"),
        );
    })
    .await
    .expect("join blocking task");

    drop(held);
    tokio::task::spawn_blocking(move || {
        let client = Client::new(client_config).expect("valid client config");
        let target = NamespacePath::parse("demo", "/note.txt").expect("target");
        let bytes = client
            .read_file_bytes(&target)
            .expect("a freed slot admits the read");
        assert_eq!(bytes, b"bounded");
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        let client = Client::new(ClientConfig {
            server_url: format!("http://{addr}"),
            auth_token: Some("test-token".to_owned()),
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config");
        assert_api_error(
            client.read_file_bytes(&NamespacePath::parse("demo", "/big.bin").expect("target")),
            413,
            "content_too_large",
            None,
        );
        // Content inside the limit still reads through the same route.
        let bytes = client
            .read_file_bytes(&NamespacePath::parse("demo", "/small.bin").expect("target"))
            .expect("small content fits under the limit");
        assert_eq!(bytes.len(), 8);
    })
    .await
    .expect("join blocking task");

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

    let ready_url = format!("http://{addr}/health/ready");
    let url = ready_url.clone();
    tokio::task::spawn_blocking(move || {
        let body = ureq::get(&url)
            .call()
            .expect("an admitting server is ready")
            .into_string()
            .expect("readiness body");
        assert_eq!(body, "ready");
    })
    .await
    .expect("join blocking task");

    state.publisher.close_admission();

    tokio::task::spawn_blocking(move || match ureq::get(&ready_url).call() {
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

async fn start_server(store: SharedObjectStore, root: &Path, writer_id: &str) -> TestHarness {
    let config = test_config(root, writer_id);
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
        })
        .expect("valid client config"),
        server,
    }
}

async fn test_runtime(store: SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .writer_version(format!("{writer_id}/0.1.0"))
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
        writer_version: format!("{writer_id}/0.1.0"),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: crate::config::GrepConfig::default(),
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_commit_body_bytes: 8 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
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
            expected_inode_id: None,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("delete `{absolute_path}`: {error}"));
}

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
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
