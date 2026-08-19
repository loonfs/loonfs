//! Starts a local LoonFS server for SDK conformance tests.

use async_trait::async_trait;
use axum::extract::{Path as AxumPath, State};
use axum::http::header::{ETAG, RANGE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{Checksum, ChecksumAlgorithm};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::{
    DirectGetIssuer, DirectMultipartIssuer, DirectPutIssuer, DirectTransferIssuers,
    PresignedGetRequest, PresignedPartRequest, PresignedPutRequest, PresignedUrl,
};
use loonfs_objectstore::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, PutMode, SharedObjectStore, StoredObjectChecksum,
};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// Bearer token accepted by the conformance server.
pub const AUTH_TOKEN: &str = "conformance-test-token";

const DIRECT_PUT_MAX_BYTES: u64 = 64 * 1024 * 1024;

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

/// Starts a conformance API and its loopback transfer service.
pub async fn start_server() -> Result<ConformanceServer, Box<dyn std::error::Error + Send + Sync>> {
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let provider_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api_address = api_listener.local_addr()?;
    let provider_address = provider_listener.local_addr()?;

    let temp_dir = tempfile::tempdir()?;
    let store_root = temp_dir.path().join("store");
    let store = Arc::new(ConformanceStore::new(&store_root)?);
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
    let transfers = DirectTransferIssuers::read_only(issuer.clone())
        .with_put(issuer.clone())
        .with_multipart(issuer);

    let config_path = temp_dir.path().join("loonfs-server.toml");
    write_server_config(&config_path, &store_root)?;
    let config = loonfs_server::load_server_config(&config_path)?;
    let api_router =
        loonfs_server::app_with_test_transfers(config, shared_store, transfers).await?;
    let server_task = tokio::spawn(async move {
        axum::serve(api_listener, api_router)
            .await
            .expect("serve conformance API");
    });

    health_check(api_address).await?;

    Ok(ConformanceServer {
        base_url: format!("http://{api_address}"),
        token: AUTH_TOKEN,
        server_task,
        provider_task,
        _temp_dir: temp_dir,
    })
}

async fn health_check(address: std::net::SocketAddr) -> io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    if response.starts_with(b"HTTP/1.1 200 ") {
        return Ok(());
    }
    Err(io::Error::other("conformance server health check failed"))
}

fn write_server_config(path: &Path, store_root: &Path) -> io::Result<()> {
    let contents = format!(
        r#"bind = "127.0.0.1:0"
auth_token = "{AUTH_TOKEN}"
content_token_secret = "conformance-content-token-secret"
writer_id = "loonfs-conformance"

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

impl DirectGetIssuer for LoopbackIssuer {
    fn presign_get(
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

impl DirectPutIssuer for LoopbackIssuer {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        ChecksumAlgorithm::Sha256
    }

    fn max_content_bytes(&self) -> u64 {
        DIRECT_PUT_MAX_BYTES
    }

    fn presign_put(
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

impl DirectMultipartIssuer for LoopbackIssuer {
    fn presign_multipart_part(
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

#[derive(Debug)]
struct ConformanceStore {
    inner: LocalFsStore,
    next_upload_id: AtomicU64,
    multipart: Mutex<HashMap<String, MultipartState>>,
    stored_checksums: Mutex<HashMap<String, Checksum>>,
}

#[derive(Debug)]
struct MultipartState {
    object_key: String,
    parts: HashMap<u32, StoredPart>,
}

#[derive(Debug)]
struct StoredPart {
    bytes: Bytes,
    etag: String,
}

impl ConformanceStore {
    fn new(root: &Path) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            inner: LocalFsStore::new(root)?,
            next_upload_id: AtomicU64::new(1),
            multipart: Mutex::new(HashMap::new()),
            stored_checksums: Mutex::new(HashMap::new()),
        })
    }

    fn put_part(
        &self,
        upload_id: &str,
        part_number: u32,
        bytes: Bytes,
    ) -> Result<String, ObjectStoreError> {
        let mut uploads = self.multipart.lock().expect("multipart lock");
        let upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| ObjectStoreError::NotFound {
                object_key: upload_id.to_owned(),
            })?;
        let etag = format!("\"{upload_id}-{part_number}-{}\"", bytes.len());
        upload.parts.insert(
            part_number,
            StoredPart {
                bytes,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }
}

#[async_trait]
impl ObjectStore for ConformanceStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        let multipart_checksum = self
            .stored_checksums
            .lock()
            .expect("stored checksum lock")
            .get(key)
            .cloned();
        if let Some(checksum) = multipart_checksum {
            let size_bytes = self
                .inner
                .head(key)
                .await?
                .ok_or_else(|| ObjectStoreError::NotFound {
                    object_key: key.to_owned(),
                })?
                .size_bytes;
            return Ok(Some(StoredObjectChecksum {
                size_bytes,
                checksum,
            }));
        }
        self.inner.head_stored_checksum(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String, ObjectStoreError> {
        let number = self.next_upload_id.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("conformance-multipart-{number}");
        self.multipart.lock().expect("multipart lock").insert(
            upload_id.clone(),
            MultipartState {
                object_key: key.to_owned(),
                parts: HashMap::new(),
            },
        );
        Ok(upload_id)
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion, ObjectStoreError> {
        let upload = self
            .multipart
            .lock()
            .expect("multipart lock")
            .remove(provider_upload_id);
        let Some(upload) = upload else {
            return Ok(MultipartCompletion::UnknownUpload);
        };
        if upload.object_key != key {
            return Err(ObjectStoreError::transport(
                key,
                "multipart completion named a different object",
            ));
        }

        let mut assembled = Vec::new();
        for requested in parts {
            let stored = upload.parts.get(&requested.part_number).ok_or_else(|| {
                ObjectStoreError::transport(
                    key,
                    format!("multipart part {} is absent", requested.part_number),
                )
            })?;
            if stored.etag != requested.etag
                || Checksum::compute(requested.checksum.algorithm, &stored.bytes)
                    != requested.checksum
            {
                return Err(ObjectStoreError::transport(
                    key,
                    format!("multipart part {} did not match", requested.part_number),
                ));
            }
            assembled.extend_from_slice(&stored.bytes);
        }
        if Checksum::compute(checksum.algorithm, &assembled) != *checksum {
            return Err(ObjectStoreError::transport(
                key,
                "assembled multipart checksum did not match",
            ));
        }
        self.inner
            .put(key, Bytes::from(assembled), PutMode::CreateIfAbsent)
            .await?;
        self.stored_checksums
            .lock()
            .expect("stored checksum lock")
            .insert(key.to_owned(), checksum.clone());
        Ok(MultipartCompletion::Assembled)
    }

    async fn abort_multipart_upload(
        &self,
        _key: &str,
        provider_upload_id: &str,
    ) -> Result<(), ObjectStoreError> {
        self.multipart
            .lock()
            .expect("multipart lock")
            .remove(provider_upload_id);
        Ok(())
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        self.inner.put_streamed(key, body, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

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
    match store.put_part(&upload_id, part_number, body) {
        Ok(etag) => (StatusCode::OK, [(ETAG, etag)]).into_response(),
        Err(ObjectStoreError::NotFound { .. }) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}
