//! Starts a local LoonFS server for SDK conformance tests.

use async_trait::async_trait;
use axum::extract::{Path as AxumPath, State};
use axum::http::header::{ETAG, RANGE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use bytes::Bytes;
use loonfs_api::ChecksumAlgorithm;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::{
    DirectGetIssuer, DirectMultipartIssuer, DirectPutIssuer, DirectTransferIssuers,
    PresignedGetRequest, PresignedPartRequest, PresignedPutRequest, PresignedUrl,
};
use loonfs_objectstore::{ObjectStore, ObjectStoreError, SharedObjectStore};
use loonfs_test_support::stores::{FakeMultipartStore, MultipartChecksumEnforcement};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use thiserror::Error;
use tokio::task::JoinHandle;

/// Bearer token accepted by the conformance server.
pub const AUTH_TOKEN: &str = "conformance-test-token";

const DIRECT_PUT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const PROXY_UPLOAD_MAX_BYTES: u64 = 6 * 1024 * 1024;

/// A running conformance API and transfer service.
#[derive(Debug)]
pub struct ConformanceServer {
    /// Base URL for the HTTP API.
    pub base_url: String,
    /// Bearer token accepted by the HTTP API.
    pub token: &'static str,
    server_task: JoinHandle<()>,
    provider_task: JoinHandle<()>,
    _temp_dir: TempDir,
}

impl Drop for ConformanceServer {
    fn drop(&mut self) {
        self.server_task.abort();
        self.provider_task.abort();
    }
}

/// Failure to start a conformance server.
#[derive(Debug, Error)]
pub enum ConformanceServerError {
    /// A listener could not bind or report its address.
    #[error("failed to bind conformance server: {source}")]
    Bind {
        /// Socket error.
        source: io::Error,
    },
    /// The temporary store or server configuration could not be built.
    #[error("failed to configure conformance server: {message}")]
    Config {
        /// Configuration failure.
        message: String,
    },
    /// The API did not answer its health endpoint successfully.
    #[error("conformance server health check failed: {source}")]
    HealthCheck {
        /// HTTP client error.
        source: reqwest::Error,
    },
}

/// Starts a conformance API and its loopback transfer service.
pub async fn start_server() -> Result<ConformanceServer, ConformanceServerError> {
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| ConformanceServerError::Bind { source })?;
    let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| ConformanceServerError::Bind { source })?;
    let api_address = api_listener
        .local_addr()
        .map_err(|source| ConformanceServerError::Bind { source })?;
    let provider_address = provider_listener
        .local_addr()
        .map_err(|source| ConformanceServerError::Bind { source })?;

    let temp_dir = tempfile::tempdir().map_err(config_error)?;
    let store_root = temp_dir.path().join("store");
    let inner = LocalFsStore::new(&store_root).map_err(config_error)?;
    let store = Arc::new(FakeMultipartStore::with_enforcement(
        inner,
        MultipartChecksumEnforcement::Precondition,
    ));
    let shared_store: SharedObjectStore = store.clone();

    let provider_router = transfer_router(store);
    let provider_task = tokio::spawn(async move {
        axum::serve(provider_listener, provider_router)
            .await
            .expect("serve conformance transfers");
    });
    let issuer = Arc::new(LoopbackIssuer {
        base_url: format!("http://{provider_address}"),
    });
    let transfers = DirectTransferIssuers {
        get: issuer.clone(),
        put: Some(issuer.clone()),
        multipart: Some(issuer),
    };

    let config_path = temp_dir.path().join("loonfs-server.toml");
    write_server_config(&config_path, &store_root).map_err(config_error)?;
    let config = loonfs_server::load_server_config(&config_path).map_err(config_error)?;
    let (api_router, _state) = loonfs_server::app(
        config,
        loonfs_server::AppOptions {
            store: Some(shared_store),
            direct_transfers: Some(transfers),
        },
    )
    .await
    .map_err(config_error)?;
    let server_task = tokio::spawn(async move {
        axum::serve(api_listener, api_router)
            .await
            .expect("serve conformance API");
    });

    health_check(api_address)
        .await
        .map_err(|source| ConformanceServerError::HealthCheck { source })?;

    Ok(ConformanceServer {
        base_url: format!("http://{api_address}"),
        token: AUTH_TOKEN,
        server_task,
        provider_task,
        _temp_dir: temp_dir,
    })
}

async fn health_check(address: std::net::SocketAddr) -> Result<(), reqwest::Error> {
    reqwest::get(format!("http://{address}/health"))
        .await?
        .error_for_status()?;
    Ok(())
}

fn config_error(error: impl std::fmt::Display) -> ConformanceServerError {
    ConformanceServerError::Config {
        message: error.to_string(),
    }
}

fn write_server_config(path: &Path, store_root: &Path) -> io::Result<()> {
    let contents = format!(
        r#"bind = "127.0.0.1:0"
auth_token = "{AUTH_TOKEN}"
content_token_secret = "conformance-content-token-secret"
writer_id = "loonfs-conformance"
max_upload_bytes = {PROXY_UPLOAD_MAX_BYTES}

[store]
kind = "local-fs"
root = "{}"
"#,
        store_root.display()
    );
    fs::write(path, contents)
}

#[derive(Debug)]
struct LoopbackIssuer {
    base_url: String,
}

fn presigned_expiry_ms(now: SystemTime) -> u64 {
    let expiry = now + Duration::from_secs(3600);
    expiry
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX >> 1))
        .unwrap_or(0)
}

#[async_trait]
impl DirectGetIssuer for LoopbackIssuer {
    async fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl, ObjectStoreError> {
        Ok(PresignedUrl {
            method: "GET".to_owned(),
            url: format!("{}/objects/{}", self.base_url, request.object_key),
            headers: BTreeMap::new(),
            expires_at_ms: presigned_expiry_ms(now),
        })
    }
}

#[async_trait]
impl DirectPutIssuer for LoopbackIssuer {
    fn stored_checksum_algorithm(&self) -> ChecksumAlgorithm {
        ChecksumAlgorithm::Sha256
    }

    fn max_content_bytes(&self) -> u64 {
        DIRECT_PUT_MAX_BYTES
    }

    async fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl, ObjectStoreError> {
        Ok(PresignedUrl {
            method: "PUT".to_owned(),
            url: format!("{}/objects/{}", self.base_url, request.object_key),
            headers: BTreeMap::new(),
            expires_at_ms: presigned_expiry_ms(now),
        })
    }
}

#[async_trait]
impl DirectMultipartIssuer for LoopbackIssuer {
    async fn presign_multipart_part(
        &self,
        request: PresignedPartRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl, ObjectStoreError> {
        Ok(PresignedUrl {
            method: "PUT".to_owned(),
            url: format!(
                "{}/multipart/{}/{}",
                self.base_url, request.provider_upload_id, request.part_number
            ),
            headers: BTreeMap::new(),
            expires_at_ms: presigned_expiry_ms(now),
        })
    }
}

type ConformanceStore = FakeMultipartStore<LocalFsStore>;

fn transfer_router(store: Arc<ConformanceStore>) -> Router {
    Router::new()
        .route("/objects/{*key}", get(get_object).put(put_object))
        .route("/multipart/{upload_id}/{part_number}", put(put_part))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(store)
}

async fn get_object(
    State(store): State<Arc<ConformanceStore>>,
    AxumPath(key): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    match store.get(&key, None).await {
        Ok(Some(bytes)) => {
            if let Some(start) = range_start(&headers) {
                let Ok(start) = usize::try_from(start) else {
                    return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                };
                if start > bytes.len() {
                    return StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                }
                return (StatusCode::PARTIAL_CONTENT, bytes.slice(start..)).into_response();
            }
            (StatusCode::OK, bytes).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn range_start(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|value| value.strip_suffix('-'))
        .and_then(|value| value.parse().ok())
}

async fn put_object(
    State(store): State<Arc<ConformanceStore>>,
    AxumPath(key): AxumPath<String>,
    body: Bytes,
) -> Response {
    match store.put_if_absent(&key, body).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            StatusCode::PRECONDITION_FAILED.into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn put_part(
    State(store): State<Arc<ConformanceStore>>,
    AxumPath((upload_id, part_number)): AxumPath<(String, u32)>,
    body: Bytes,
) -> Response {
    match store.upload_part(&upload_id, part_number, &body) {
        Ok(etag) => (StatusCode::OK, [(ETAG, etag)]).into_response(),
        Err(ObjectStoreError::NotFound { .. }) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}
