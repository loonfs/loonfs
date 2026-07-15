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
use super::{app_with_store, build_handles_with_metrics_jsonl_path, SharedStore};
use crate::config::RuntimeCacheConfigOverrides;
use crate::{ServerConfig, StoreConfig};
use async_trait::async_trait;
use axum::body::Bytes;
use futures::stream::BoxStream;
use loonfs::ErrorCode;
use loonfs::{
    CreateNamespaceOptions, DeleteOptions, FsAdmin, FsWriter, MaintenanceTickOptions,
    PutFileOptions, TraceMode, TraceStoreKind,
};
use loonfs_api::{ChangeSeq, CommitId, DeleteDirectoryBehavior, NamespaceId, PutBehavior};
use loonfs_client::{Client, ClientConfig, ClientError, MutationOptions, NamespacePath};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
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

#[tokio::test]
async fn build_handles_installs_jsonl_object_store_metrics_recorder() {
    let store_dir = tempdir().expect("store tempdir");
    let metrics_dir = tempdir().expect("metrics tempdir");
    let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedStore;
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
    });
    tokio::task::spawn_blocking(move || {
        client
            .create_namespace("demo")
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
async fn runtime_created_state_is_readable_through_http() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
            behavior: PutBehavior::NoReplace,
            commit_id: Some(CommitId::parse("runtime-put").expect("valid commit id")),
        },
    )
    .await
    .expect("write file through runtime");

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo:/notes/hello.txt").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let fs = test_runtime(store.clone(), "runtime-reader").await;
    let harness = start_server(store.clone(), temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace through http");
        let target = NamespacePath::parse("demo:/notes/from-http.txt").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("missing:/notes/hello.txt").expect("target");
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
        let destination = NamespacePath::parse("missing:/notes/renamed.txt").expect("target");
        assert_api_error(
            harness
                .client
                .move_path(&target, &destination, &MutationOptions::default()),
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let harness = start_server(store, temp_dir.path(), "server-writer").await;

    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("missing:/").expect("target");
        assert_api_error(
            harness.client.list_path(&target),
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo:/missing.txt").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
        let dir_target = NamespacePath::parse("demo:/docs").expect("dir target");
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

        let from = NamespacePath::parse("demo:/tmp/a.txt").expect("from");
        let to = NamespacePath::parse("demo:/docs/a.txt").expect("to");
        assert_api_error(
            harness
                .client
                .move_path(&from, &to, &MutationOptions::default()),
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
        let put_target = NamespacePath::parse("demo:/docs/new.txt").expect("put target");
        harness
            .client
            .write_file_bytes(&put_target, b"new", &MutationOptions::default())
            .expect("put recreates the subtree");
        let old_child = NamespacePath::parse("demo:/docs/old.txt").expect("old child");
        assert_api_error(
            harness.client.stat_path(&old_child),
            404,
            "path_not_found",
            None,
        );

        let from = NamespacePath::parse("demo:/tmp/source.txt").expect("from");
        let to = NamespacePath::parse("demo:/docs/source.txt").expect("to");
        harness
            .client
            .move_path(&from, &to, &MutationOptions::default())
            .expect("move lands in the recreated subtree");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_path_mutation_retries_transient_stale_head_cas() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(StaleHeadOnceStore::new(temp_dir.path(), "demo")) as SharedStore;
    bootstrap_namespace(&store, "server-writer", &namespace_id("demo")).await;

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo:/notes/race.txt").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    bootstrap_namespace(&store, "other-writer", &namespace_id("demo")).await;
    let store_for_check = store.clone();

    let harness = start_server(store, temp_dir.path(), "server-writer").await;
    tokio::task::spawn_blocking(move || {
        let target = NamespacePath::parse("demo:/notes/taken-over.txt").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
            });
            assert_api_error(
                client.namespace_status("demo"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_body_over_the_limit_answers_content_too_large() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
        });
        let target = NamespacePath::parse("demo:/big.bin").expect("target");
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
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
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
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

/// Seeds a namespace with enough durable state that an explicit
/// maintenance tick has metadata to load: files to flush and the gram
/// index feature to build.
async fn seed_namespace_for_maintenance(writer: &FsWriter, admin: &FsAdmin) -> NamespaceId {
    let namespace = namespace_id("cache-config");
    writer
        .create_namespace(&namespace, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for (index, path) in ["/left.txt", "/right.txt"].iter().enumerate() {
        write_file_bytes(
            writer,
            &namespace,
            path,
            b"a needle for maintenance\n",
            &format!("cache-config-seed-{index:02}"),
        )
        .await;
    }
    admin
        .enable_grams_index(&namespace)
        .await
        .expect("enable grams index");
    namespace
}

/// The tick shape `/maintenance/tick` serves with a one-segment
/// threshold override: the WAL flushes, a manifest publishes, and the
/// gram build step loads it through the runtime cache.
fn flush_everything_tick() -> MaintenanceTickOptions {
    MaintenanceTickOptions {
        max_wal_tail_segments: 1,
        ..MaintenanceTickOptions::default()
    }
}

/// Review regression: the admin handle behind `/maintenance/tick` was
/// built without the configured runtime cache, so explicit maintenance
/// ran a default 256 MiB cache no matter what the operator configured.
/// With a zero decoded-byte budget the whole runtime cache is disabled,
/// and maintenance driven through the admin must neither insert into
/// nor hit any cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_maintenance_honors_a_zero_configured_cache_budget() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.background_maintenance = false;
    config.runtime_cache.metadata_table_cache_max_decoded_bytes = Some(0);
    let (writer, _reader, admin) = build_handles_with_metrics_jsonl_path(&config, store, None)
        .await
        .expect("build handles");

    let namespace = seed_namespace_for_maintenance(&writer, &admin).await;
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace, flush_everything_tick())
            .await
            .expect("maintenance tick");
    }

    let stats = admin.runtime_cache_stats();
    assert_eq!(
        stats.metadata_table_cache_inserts, 0,
        "a zero-budget cache must admit nothing during explicit maintenance"
    );
    assert_eq!(
        stats.metadata_table_cache_hits, 0,
        "a zero-budget cache must serve nothing during explicit maintenance"
    );
    writer.shutdown_background().await.expect("writer shutdown");
}

/// The companion premise and the sharing half: with a real budget the
/// identical flow does populate the cache, and the blocks maintenance
/// admits land in the writer's own cache instance — one cache, one
/// budget — rather than in a second admin-only cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_maintenance_populates_the_writer_visible_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let mut config = test_config(temp_dir.path(), "server-writer");
    config.background_maintenance = false;
    let (writer, _reader, admin) = build_handles_with_metrics_jsonl_path(&config, store, None)
        .await
        .expect("build handles");

    let namespace = seed_namespace_for_maintenance(&writer, &admin).await;
    // Everything after this snapshot flows through the admin handle, so
    // any movement in the writer-visible counters is admin work landing
    // in the shared instance.
    let before = writer.runtime_cache_stats().metadata_table_cache_inserts;
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace, flush_everything_tick())
            .await
            .expect("maintenance tick");
    }

    let writer_stats = writer.runtime_cache_stats();
    assert!(
        writer_stats.metadata_table_cache_inserts > before,
        "explicit maintenance must populate the cache the writer observes, \
         inserts before {before}, after {}",
        writer_stats.metadata_table_cache_inserts
    );
    let admin_stats = admin.runtime_cache_stats();
    assert_eq!(
        admin_stats.metadata_table_cache_inserts, writer_stats.metadata_table_cache_inserts,
        "admin and writer must observe one shared cache, not two"
    );
    writer.shutdown_background().await.expect("writer shutdown");
}

async fn start_server(store: SharedStore, root: &Path, writer_id: &str) -> TestHarness {
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
        }),
        server,
    }
}

async fn test_runtime(store: SharedStore, writer_id: &str) -> FsWriter {
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
        gram_index_build: crate::config::GramIndexBuildPolicyOverrides::default(),
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 256 * 1024 * 1024,
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
    store: &SharedStore,
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
            behavior: PutBehavior::Replace,
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
