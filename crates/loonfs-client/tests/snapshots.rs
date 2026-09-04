#![allow(clippy::panic)]

use loonfs_api::{ChangeSeq, DestinationBehavior, NamespaceId};
use loonfs_client::{
    Client, ClientConfig, NamespacePath, PutFileOptions, ReadFileOptions, StatPathOptions,
};
use loonfs_server::{
    app, AppOptions, GrepConfig, MaintenanceMode, RuntimeCacheConfigOverrides, ServerConfig,
    StoreConfig,
};
use tempfile::TempDir;

struct TestServer {
    client: Client,
    server: tokio::task::JoinHandle<()>,
    _temp_dir: TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_server(name: &str) -> TestServer {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config = ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: name.to_owned(),
        max_writer_sessions: 10_000,
        max_concurrent_folds: 2,
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        local_cache: None,
        grep: GrepConfig::default(),
        maintenance: MaintenanceMode::ServeAndMaintain,
        min_publish_interval_ms: 0,
        request_deadline_ms: 60_000,
        shutdown_deadline_ms: 60_000,
        max_upload_bytes: 16 * 1024 * 1024,
        max_download_bytes: 16 * 1024 * 1024,
        snapshot_max_ttl_ms: 60_000,
        snapshot_max_lifetime_ms: 60_000,
        snapshot_max_live_per_namespace: 16,
        max_concurrent_uploads: 4,
        max_concurrent_downloads: 4,
        max_concurrent_maintenance: 2,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: temp_dir.path().join("store").display().to_string(),
            key_prefix: Some(name.to_owned()),
        },
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let (router, _state) = app(config, AppOptions::default())
        .await
        .expect("build server");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let client = Client::new(ClientConfig {
        server_url: format!("http://{address}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("client config");
    TestServer {
        client,
        server,
        _temp_dir: temp_dir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_lifecycle_round_trips_through_the_client() {
    let harness = start_server("client-snapshot-lifecycle").await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let created = harness
        .client
        .create_snapshot(&namespace, "first", 5_000)
        .await
        .expect("create snapshot");
    assert_eq!(created.namespace_id, namespace);
    assert_eq!(created.name, "first");
    assert_eq!(created.head_seq, ChangeSeq(0));

    let mut pager = harness
        .client
        .list_snapshots_pager(&namespace, Some(1), None);
    assert_eq!(
        pager.collect_up_to(10).await.expect("list snapshots"),
        vec![created.clone()]
    );

    let extended = harness
        .client
        .extend_snapshot(&namespace, &created.snapshot_id, 10_000)
        .await
        .expect("extend snapshot");
    assert!(extended.expires_at_ms > created.expires_at_ms);

    harness
        .client
        .release_snapshot(&namespace, &created.snapshot_id)
        .await
        .expect("release snapshot");
    assert!(harness
        .client
        .list_snapshots_page(&namespace, None, None)
        .await
        .expect("list released snapshots")
        .snapshots
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_file_read_returns_the_captured_state() {
    let harness = start_server("client-snapshot-read").await;
    let namespace = NamespaceId::parse("demo").expect("namespace id");
    let path = NamespacePath::parse("demo", "/report.txt").expect("namespace path");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    harness
        .client
        .put_file_bytes(
            &path,
            b"captured",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write captured content");
    let snapshot = harness
        .client
        .create_snapshot(&namespace, "captured", 10_000)
        .await
        .expect("create snapshot");

    let mut replace = PutFileOptions::new(loonfs_test_support::test_actor());
    replace.behavior = DestinationBehavior::Replace;
    harness
        .client
        .put_file_bytes(&path, b"current", &replace)
        .await
        .expect("replace content");

    let entry = harness
        .client
        .get_path_entry(
            &path,
            &StatPathOptions {
                snapshot_id: Some(snapshot.snapshot_id.clone()),
                ..StatPathOptions::default()
            },
        )
        .await
        .expect("snapshot stat");
    assert_eq!(entry.head_seq, snapshot.head_seq);
    let bytes = harness
        .client
        .get_file_bytes(
            &path,
            &ReadFileOptions {
                revision_no: None,
                snapshot_id: Some(snapshot.snapshot_id),
            },
        )
        .await
        .expect("snapshot content");
    assert_eq!(bytes, b"captured");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&path, &Default::default())
            .await
            .expect("current content"),
        b"current"
    );
}
