//! Shared fixtures for the crate's integration tests.

#![allow(
    dead_code,
    reason = "`tests/logging.rs` includes this file and uses only part of it"
)]
#![allow(
    clippy::result_large_err,
    reason = "test helpers preserve the client's structured error type"
)]

use loonfs::SharedObjectStore;
use loonfs_client::{Client, ClientConfig, ClientError, ListOptions, NamespacePath};
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_server::{
    app, serve_with_shutdown, AppConfig, AuthConfig, JobMode, ServerConfig, ServerError,
    StoreConfig, TlsServerConfig, TransportConfig,
};
use loonfs_test_support::http::{raw_agent, reserve_loopback_address};
use loonfs_types::{ErrorCode, ListCheckpointsResponse, ListPathEntriesResponse, NamespaceId};
use std::collections::BTreeMap;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) async fn collect_path_entries(
    client: &Client,
    spec: &NamespacePath,
    options: &ListOptions,
) -> loonfs_client::Result<ListPathEntriesResponse> {
    let mut pager = client.list_with_options(spec, options);
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
    let mut pager = client.list_checkpoints(namespace_id);
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

/// Asserts that `result` failed with an API error of `expected_status` and
/// `code`.
pub(crate) fn assert_api_code<T: std::fmt::Debug>(
    result: Result<T, ClientError>,
    expected_status: u16,
    code: ErrorCode,
) {
    match result.expect_err("request should fail") {
        ClientError::Api {
            status,
            code: actual,
            ..
        } => {
            assert_eq!(status, expected_status);
            assert_eq!(actual, code.as_str());
        }
        other => panic!("expected API error, got {other:?}"),
    }
}

pub(crate) struct TestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    /// The object store the server reads and writes, for a test that opens
    /// its own handles beside the server.
    pub(crate) store: SharedObjectStore,
    pub(crate) store_root: Option<PathBuf>,
    pub(crate) store_key_prefix: Option<String>,
    pub(crate) server: tokio::task::JoinHandle<()>,
}

// The server runs as a task on the test's own runtime, and the tests call it
// through the blocking `ureq` agent as well as the async client. A blocking
// call holds its worker thread until the response arrives, so the tests run
// with `flavor = "multi_thread"` to give the server a second worker.
pub(crate) async fn start_server(config: ServerConfig) -> TestServer {
    let (store_root, store_key_prefix) = match &config.store {
        StoreConfig::LocalFs(LocalFsStoreConfig { root, key_prefix }) => {
            (Some(PathBuf::from(root)), key_prefix.clone())
        }
        _ => (None, None),
    };
    let auth_token = bearer_token(&config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let configured = config.object_store().expect("build the configured store");
    let direct_transfers = configured.direct_transfers();
    let store = configured.into_shared();
    let (router, _) = app(
        config.validate().expect("validate the test config"),
        AppConfig { store: Some(store.clone()), direct_transfers },
    )
    .await
    .expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let server_url = format!("http://{addr}");

    TestServer {
        client: client(&server_url, auth_token, None),
        server_url,
        store,
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
    pub(crate) server: tokio::task::JoinHandle<Result<(), ServerError>>,
    /// Holds the generated certificate and key on disk for the server's
    /// lifetime.
    _identity_dir: tempfile::TempDir,
}

pub(crate) async fn start_tls_server(config: ServerConfig) -> TlsTestServer {
    start_tls_server_with_certificate(config, tls_certificate_params()).await
}

/// Returns the parameters of the harness's self-signed certificate, which
/// names `localhost` and `127.0.0.1` and keeps rcgen's default validity.
pub(crate) fn tls_certificate_params() -> rcgen::CertificateParams {
    rcgen::CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
        .expect("certificate parameters")
}

/// Starts the server of [`start_tls_server`] with a certificate self-signed
/// from `certificate`.
pub(crate) async fn start_tls_server_with_certificate(
    mut config: ServerConfig,
    certificate: rcgen::CertificateParams,
) -> TlsTestServer {
    let identity_dir = tempfile::tempdir().expect("tls identity dir");
    let cert_path = identity_dir.path().join("server.crt");
    let key_path = identity_dir.path().join("server.key");
    let signing_key = rcgen::KeyPair::generate().expect("generate signing key");
    let certificate = certificate
        .self_signed(&signing_key)
        .expect("self-sign certificate");
    std::fs::write(&cert_path, certificate.pem()).expect("write certificate");
    std::fs::write(&key_path, signing_key.serialize_pem()).expect("write private key");

    let addr = reserve_loopback_address();
    config.bind = addr.to_string();
    config.transport = Some(TransportConfig::Tls(TlsServerConfig {
        cert_path: cert_path.display().to_string(),
        key_path: key_path.display().to_string(),
    }));
    let auth_token = bearer_token(&config);

    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(serve_with_shutdown(
        config.validate().expect("validate the test config"),
        async move {
            let _ = shutdown_signal.await;
        },
    ));
    wait_until_listening(addr, &mut server).await;

    let server_url = format!("https://{addr}");
    let ca_cert_path = cert_path.display().to_string();
    TlsTestServer {
        client: client(&server_url, auth_token.clone(), Some(ca_cert_path.clone())),
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
/// host does: the listener drains, the writer settles, and the SSD tier of
/// the metadata segment cache, if the server has one, writes the blocks it
/// queued. That is what lets a test stop one server and start another on
/// the same directories.
pub(crate) struct GracefulTestServer {
    pub(crate) client: Client,
    pub(crate) server_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), ServerError>>,
}

pub(crate) async fn start_graceful_server(mut config: ServerConfig) -> GracefulTestServer {
    let addr = reserve_loopback_address();
    config.bind = addr.to_string();
    let auth_token = bearer_token(&config);

    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(serve_with_shutdown(
        config.validate().expect("validate the test config"),
        async move {
            let _ = shutdown_signal.await;
        },
    ));
    wait_until_listening(addr, &mut server).await;

    let server_url = format!("http://{addr}");
    GracefulTestServer {
        client: client(&server_url, auth_token, None),
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

/// Builds a client of the server at `server_url`.
fn client(server_url: &str, auth_token: Option<String>, ca_cert_path: Option<String>) -> Client {
    Client::new(ClientConfig {
        server_url: server_url.to_owned(),
        auth_token: auth_token.map(Into::into),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path,
    })
    .expect("valid client config")
}

#[allow(
    clippy::disallowed_methods,
    reason = "the harness polls for a port that the server binds on its own schedule"
)]
async fn wait_until_listening(
    addr: SocketAddr,
    server: &mut tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    for _ in 0..500 {
        if server.is_finished() {
            let outcome = server.await;
            panic!("server on {addr} stopped before it listened: {outcome:?}");
        }
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server never started listening on {addr}");
}

/// Returns the bearer token that a client of a server with `config` sends,
/// or `None` for a server without one.
fn bearer_token(config: &ServerConfig) -> Option<String> {
    match &config.auth {
        Some(AuthConfig::BearerToken { token }) => Some(token.expose().to_owned()),
        Some(AuthConfig::Open) | None => None,
    }
}

pub(crate) fn test_config(
    store: StoreConfig,
    auth_token: &str,
    content_token_secret: &str,
    writer_id: &str,
) -> ServerConfig {
    let mut config: ServerConfig = toml::from_str(
        r#"
bind = "127.0.0.1:0"
writer_id = "unused"

[store]
kind = "local_fs"
root = "unused"
"#,
    )
    .expect("parse the test config");
    config.auth = Some(AuthConfig::BearerToken {
        token: auth_token.into(),
    });
    config.content_token_secret = content_token_secret.into();
    config.writer_id = loonfs_test_support::ids::writer_id(writer_id);
    config.grep.mode = JobMode::ServeAndMaintain;
    config.min_publish_interval_ms = 0;
    config.store = store;
    config
}

pub(crate) mod http_split_support {
    use loonfs_client::{Client, PutFileOptions};
    use loonfs_types::{
        api::v0::{
            CompleteUploadBody, ContentToken, CreateUploadBody, UploadMode, UploadSessionStatus,
        },
        CommitRequest, ContentRef, DestinationBehavior, NamespaceId,
    };

    use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
    use loonfs_server::{ServerConfig, StoreConfig};

    use loonfs_test_support::http::raw_agent;

    pub(crate) const TEST_CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";

    pub(crate) fn replace_file_options() -> PutFileOptions {
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..Default::default()
        }
    }

    pub(crate) fn test_config(
        store_root: std::path::PathBuf,
        writer_id: &str,
        key_prefix: &str,
    ) -> ServerConfig {
        super::test_config(
            StoreConfig::LocalFs(LocalFsStoreConfig {
                root: store_root.display().to_string(),
                key_prefix: Some(key_prefix.to_owned()),
            }),
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
            .set("Loonfs-Actor", "test-actor")
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
            .create_upload(namespace_id, &CreateUploadBody::ServiceProxied {})
            .await
            .expect("begin upload");
        client
            .put_upload_content(namespace_id, &begin.upload_id, file_bytes)
            .await
            .expect("upload content");
        let complete = client
            .complete_upload(
                namespace_id,
                &begin.upload_id,
                &CompleteUploadBody::ServiceProxied {},
            )
            .await
            .expect("complete upload");
        let repeated = client
            .complete_upload(
                namespace_id,
                &begin.upload_id,
                &CompleteUploadBody::ServiceProxied {},
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
