//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]
#![allow(clippy::panic)]

use loonfs_api::{ListCheckpointsResponse, ListPathEntriesResponse, NamespaceId};
use loonfs_client::{Client, ClientConfig, ListPathEntriesOptions, NamespacePath};
use loonfs_server::{
    app, serve_with_shutdown, GrepConfig, MaintenanceMode, RuntimeCacheConfigOverrides, ServeError,
    ServerConfig, StoreConfig, TlsServerConfig,
};
use loonfs_test_support::http::raw_agent;
use std::collections::BTreeMap;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) async fn collect_path_entries(
    client: &Client,
    spec: &NamespacePath,
    options: &ListPathEntriesOptions,
) -> loonfs_client::Result<ListPathEntriesResponse> {
    let mut pager = client.list_path_entries_pager(spec, None, None, options);
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.head_seq = page.head_seq;
        response.entries.extend(page.entries);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

pub(crate) async fn collect_checkpoints(
    client: &Client,
    namespace_id: &NamespaceId,
) -> loonfs_client::Result<ListCheckpointsResponse> {
    let mut pager = client.list_checkpoints_pager(namespace_id, None, None);
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.checkpoints.extend(page.checkpoints);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

/// The exposition lines of one scrape, keyed by everything left of the
/// value. Comment lines are dropped; a value that does not parse as a number
/// is a rendering bug and fails here.
pub(crate) fn scrape(server_url: &str, token: Option<&str>) -> Result<BTreeMap<String, f64>, u16> {
    let request = raw_agent().get(&format!("{server_url}/metrics"));
    let request = match token {
        Some(token) => request.set("authorization", &format!("Bearer {token}")),
        None => request,
    };
    let response = match request.call() {
        Ok(response) => response,
        Err(ureq::Error::Status(status, _)) => return Err(status),
        Err(error) => panic!("metrics scrape failed: {error}"),
    };
    assert_eq!(
        response.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    let mut body = String::new();
    response
        .into_reader()
        .read_to_string(&mut body)
        .expect("read scrape body");
    Ok(body
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let (series, value) = line
                .rsplit_once(' ')
                .unwrap_or_else(|| panic!("exposition line without a value: {line}"));
            let value: f64 = value
                .parse()
                .unwrap_or_else(|_| panic!("unparsable value in `{line}`"));
            (series.to_owned(), value)
        })
        .collect())
}

pub(crate) fn series(scrape: &BTreeMap<String, f64>, name: &str) -> f64 {
    *scrape
        .get(name)
        .unwrap_or_else(|| panic!("no series `{name}` in the scrape"))
}

pub(crate) struct TestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    #[allow(dead_code)]
    pub(crate) store_root: Option<PathBuf>,
    #[allow(dead_code)]
    pub(crate) store_key_prefix: Option<String>,
    pub(crate) server: tokio::task::JoinHandle<()>,
}

pub(crate) async fn start_server(config: ServerConfig) -> TestServer {
    let (store_root, store_key_prefix) = match &config.store {
        StoreConfig::LocalFs { root, key_prefix } => {
            (Some(PathBuf::from(root)), key_prefix.clone())
        }
        _ => (None, None),
    };
    let auth_token = config
        .auth_token
        .as_ref()
        .map(|token| token.expose().to_owned());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    // The writer is dropped: tests abort the server task instead of shutting
    // it down gracefully.
    let (router, _writer, _local_cache) = app(config).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let server_url = format!("http://{addr}");

    TestServer {
        client: Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token: auth_token.map(Into::into),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config"),
        server_url,
        store_root,
        store_key_prefix,
        server,
    }
}

/// A server terminating TLS on loopback with a certificate generated for
/// this test, plus the CA path a client needs to trust it.
///
/// Unlike [`start_server`] this goes through [`serve_with_shutdown`], the
/// same entry point the binary uses: the identity load, the bind, and the
/// TLS listener are the deployed ones rather than a test-only assembly.
pub(crate) struct TlsTestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    pub(crate) addr: SocketAddr,
    pub(crate) ca_cert_path: String,
    pub(crate) auth_token: Option<String>,
    pub(crate) shutdown: tokio::sync::oneshot::Sender<()>,
    pub(crate) server: tokio::task::JoinHandle<Result<(), ServeError>>,
    /// Holds the generated certificate and key on disk for the server's
    /// lifetime.
    _identity_dir: tempfile::TempDir,
}

pub(crate) async fn start_tls_server(mut config: ServerConfig) -> TlsTestServer {
    let identity_dir = tempfile::tempdir().expect("tls identity dir");
    let cert_path = identity_dir.path().join("server.crt");
    let key_path = identity_dir.path().join("server.key");
    let identity =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("generate self-signed identity");
    std::fs::write(&cert_path, identity.cert.pem()).expect("write certificate");
    std::fs::write(&key_path, identity.signing_key.serialize_pem()).expect("write private key");

    let addr = reserve_loopback_addr().await;
    config.bind = addr.to_string();
    config.tls = Some(TlsServerConfig {
        cert_path: cert_path.display().to_string(),
        key_path: key_path.display().to_string(),
    });
    let auth_token = config
        .auth_token
        .as_ref()
        .map(|token| token.expose().to_owned());

    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_with_shutdown(config, async move {
        let _ = shutdown_signal.await;
    }));
    wait_until_listening(addr).await;

    let server_url = format!("https://{addr}");
    let ca_cert_path = cert_path.display().to_string();
    TlsTestServer {
        client: Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token: auth_token.clone().map(Into::into),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: Some(ca_cert_path.clone()),
        })
        .expect("valid client config"),
        server_url,
        addr,
        ca_cert_path,
        auth_token,
        shutdown,
        server,
        _identity_dir: identity_dir,
    }
}

/// A plaintext server started and stopped through the deployed entry point.
///
/// Unlike [`start_server`], which aborts its task, this shuts down the way a
/// host does: the listener drains, the writer settles, and the local block
/// cache is closed and released. That is what lets a test stop one server
/// and start another on the same directories.
pub(crate) struct GracefulTestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), ServeError>>,
}

pub(crate) async fn start_graceful_server(mut config: ServerConfig) -> GracefulTestServer {
    let addr = reserve_loopback_addr().await;
    config.bind = addr.to_string();
    let auth_token = config
        .auth_token
        .as_ref()
        .map(|token| token.expose().to_owned());

    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_with_shutdown(config, async move {
        let _ = shutdown_signal.await;
    }));
    wait_until_listening(addr).await;

    let server_url = format!("http://{addr}");
    GracefulTestServer {
        client: Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token: auth_token.map(Into::into),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config"),
        server_url,
        shutdown,
        server,
    }
}

impl GracefulTestServer {
    /// Stops the server and reports what settling found. The client goes
    /// first so its idle connections do not hold the listener open.
    pub(crate) async fn shut_down(self) {
        let Self {
            client,
            shutdown,
            server,
            ..
        } = self;
        drop(client);
        shutdown.send(()).expect("signal shutdown");
        server
            .await
            .expect("server task")
            .expect("graceful shutdown settles");
    }
}

/// Picks a free loopback port by binding one and letting it go. The server
/// binds it itself, because `serve_with_shutdown` owns that step and the
/// point of this harness is to exercise it.
async fn reserve_loopback_addr() -> SocketAddr {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    probe.local_addr().expect("probe addr")
}

#[allow(clippy::disallowed_methods, clippy::panic)]
// Test-harness polling for a port the server binds on its own schedule.
async fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..500 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server never started listening on {addr}");
}

pub(crate) fn test_config(
    store: StoreConfig,
    auth_token: &str,
    content_token_secret: &str,
    writer_id: &str,
) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some(auth_token.into()),
        content_token_secret: content_token_secret.into(),
        writer_id: writer_id.to_owned(),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        local_cache: None,
        grep: GrepConfig::default(),
        maintenance: MaintenanceMode::Automatic,
        min_publish_interval_ms: 0,
        request_deadline_ms: 60_000,
        shutdown_deadline_ms: 600_000,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store,
    }
}

pub(crate) mod http_split_support {
    #![allow(dead_code)]

    use loonfs_api::{
        v0::{
            BeginUploadRequest, CompleteUploadRequest, ContentToken, UploadMode,
            UploadSessionStatus,
        },
        CommitRequest, ContentRef, DestinationBehavior, NamespaceId,
    };
    use loonfs_client::{Client, PutFileOptions};

    use loonfs_server::{ServerConfig, StoreConfig};

    use loonfs_test_support::http::raw_agent;

    pub(crate) const TEST_CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";

    pub(crate) fn replace_file_options() -> PutFileOptions {
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        }
    }

    pub(crate) fn test_config(
        store_root: std::path::PathBuf,
        writer_id: &str,
        key_prefix: &str,
    ) -> ServerConfig {
        super::test_config(
            StoreConfig::LocalFs {
                root: store_root.display().to_string(),
                key_prefix: Some(key_prefix.to_owned()),
            },
            "test-token",
            TEST_CONTENT_TOKEN_SECRET,
            writer_id,
        )
    }

    pub(crate) fn send_commit(
        server_url: &str,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> Result<ureq::Response, Box<ureq::Error>> {
        send_commit_json(server_url, namespace_id, request)
    }

    pub(crate) fn send_commit_json(
        server_url: &str,
        namespace_id: &NamespaceId,
        request: &impl serde::Serialize,
    ) -> Result<ureq::Response, Box<ureq::Error>> {
        raw_agent()
            .post(&format!(
                "{server_url}/v0/namespaces/{namespace_id}/commits"
            ))
            .set("authorization", "Bearer test-token")
            .send_json(request)
            .map_err(Box::new)
    }

    pub(crate) struct StagedUpload {
        pub(crate) content_ref: ContentRef,
        pub(crate) content_token: Option<ContentToken>,
    }

    pub(crate) async fn stage_uploaded_content(
        client: &Client,
        namespace_id: &NamespaceId,
        file_bytes: &[u8],
    ) -> StagedUpload {
        let begin = client
            .create_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
            .await
            .expect("begin upload");
        client
            .put_upload_content(namespace_id, begin.upload_id(), file_bytes)
            .await
            .expect("upload content");
        let complete = client
            .complete_upload(
                namespace_id,
                begin.upload_id(),
                &CompleteUploadRequest::ServiceProxied {},
            )
            .await
            .expect("complete upload");
        let repeated = client
            .complete_upload(
                namespace_id,
                begin.upload_id(),
                &CompleteUploadRequest::ServiceProxied {},
            )
            .await
            .expect("repeat complete upload");
        assert_eq!(repeated.namespace_id, complete.namespace_id);
        assert_eq!(repeated.upload_id, complete.upload_id);
        assert_eq!(repeated.mode, UploadMode::ServiceProxied);
        assert_eq!(complete.mode, UploadMode::ServiceProxied);
        let UploadSessionStatus::Completed {
            completed_at_ms,
            content_ref,
            content_token,
        } = complete.status
        else {
            panic!("completion reports a completed session")
        };
        let UploadSessionStatus::Completed {
            completed_at_ms: repeated_at_ms,
            content_ref: repeated_ref,
            content_token: repeated_token,
        } = repeated.status
        else {
            panic!("completion replay reports a completed session")
        };
        assert_eq!(repeated_at_ms, completed_at_ms);
        assert_eq!(repeated_ref, content_ref);
        assert!(content_token.is_some());
        assert!(repeated_token.is_some());
        StagedUpload {
            content_ref,
            content_token,
        }
    }

    pub(crate) fn content_token(completed: &StagedUpload) -> ContentToken {
        completed
            .content_token
            .clone()
            .expect("completed upload carries token")
    }
}
