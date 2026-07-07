//! The v0 HTTP surface of the LoonFS server.
//!
//! This module owns the route table, the server startup path, and the
//! request glue shared by every handler (bearer-token authorization,
//! `{namespace}` path parsing, and the JSON body extractors). The handlers
//! themselves live in sibling modules grouped by API area:
//!
//! - [`handlers_namespace`]: namespace lifecycle, status, capability
//!   discovery, and the admin maintenance endpoints.
//! - [`handlers_filesystem`]: path- and inode-oriented filesystem reads and
//!   mutations, revision listings, semantic commits, and the change feed.
//! - [`handlers_uploads`]: upload session flows plus the presign and
//!   content-token helpers backing them.
//! - [`error`]: the [`ApiResponseError`] envelope and the error-kind → HTTP
//!   status mapping.
//! - [`openapi`]: assembly of the static OpenAPI document.

mod error;
mod handlers_filesystem;
mod handlers_namespace;
mod handlers_uploads;
#[cfg(feature = "openapi")]
mod openapi;
#[cfg(test)]
mod tests;

#[cfg(feature = "openapi")]
pub use self::openapi::{openapi_document, openapi_json_pretty};

use self::error::ApiResponseError;
use self::handlers_filesystem::{
    commit_operations, filesystem_operation, get_content, get_inode_revision_content, list_changes,
    list_entries, list_inode_revisions, list_path_revisions, restore_inode_revision, stat_entry,
};
use self::handlers_namespace::{
    advance_retention, create_checkpoint, create_namespace, delete_namespace, fork_namespace,
    gc_namespace, namespace_status,
};
use self::handlers_uploads::{begin_upload, complete_upload, upload_content};
use crate::config::{ServerConfig, ServerConfigError};
use axum::async_trait;
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequest, FromRequestParts, Path as AxumPath};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use loonfs::publisher::PublisherRegistry;
use loonfs::{
    ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter, JsonlObjectStoreMetricsRecorder,
    ObjectStoreMetricsRecorder, SharedObjectStore, TraceMode, TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_objectstore::presign::ObjectTransferIssuer;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;

type SharedStore = SharedObjectStore;
const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Purpose-specific handles over one shared store client: read endpoints go
/// through `reader`, mutations through `writer` (and its publisher),
/// maintenance endpoints through `admin`.
#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    writer: FsWriter,
    reader: FsReader,
    admin: FsAdmin,
    publisher: PublisherRegistry,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
}

pub async fn app(config: ServerConfig) -> Result<Router, ServerConfigError> {
    Ok(app_parts(config).await?.0)
}

/// Builds the router plus the writer handle `serve` keeps aside, so a
/// graceful shutdown can settle writer-scheduled background maintenance
/// after the listener drains.
async fn app_parts(config: ServerConfig) -> Result<(Router, FsWriter), ServerConfigError> {
    let store = config.object_store()?;
    let transfer_issuer = store.transfer_issuer();
    let store = Arc::new(store) as SharedStore;
    app_with_store_and_transfer_issuer(config, store, transfer_issuer).await
}

#[cfg(test)]
async fn app_with_store(
    config: ServerConfig,
    store: SharedStore,
) -> Result<Router, ServerConfigError> {
    Ok(app_with_store_and_transfer_issuer(config, store, None)
        .await?
        .0)
}

async fn app_with_store_and_transfer_issuer(
    config: ServerConfig,
    store: SharedStore,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
) -> Result<(Router, FsWriter), ServerConfigError> {
    let (writer, reader, admin) = build_handles(&config, store).await?;
    let config = Arc::new(config);
    let publisher = PublisherRegistry::new(writer.clone());
    let state = AppState {
        config,
        writer: writer.clone(),
        reader,
        admin,
        publisher,
        transfer_issuer,
    };
    Ok((router(state), writer))
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        // Module-qualified because handler modules also export a bare
        // `config` name in this scope.
        .route("/v0/config", get(handlers_namespace::config))
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/:namespace",
            get(namespace_status).delete(delete_namespace),
        )
        .route("/v0/namespaces/:namespace/forks", post(fork_namespace))
        .route(
            "/v0/namespaces/:namespace/filesystem/list",
            get(list_entries),
        )
        .route("/v0/namespaces/:namespace/filesystem/stat", get(stat_entry))
        .route(
            "/v0/namespaces/:namespace/filesystem/content",
            get(get_content),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/revisions",
            get(list_path_revisions),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/operations",
            post(filesystem_operation),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions",
            get(list_inode_revisions),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:revision_no/content",
            get(get_inode_revision_content),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:source_revision_no/restore",
            post(restore_inode_revision),
        )
        .route("/v0/namespaces/:namespace/uploads", post(begin_upload))
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/content",
            put(upload_content),
        )
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/complete",
            post(complete_upload),
        )
        .route("/v0/namespaces/:namespace/commits", post(commit_operations))
        .route("/v0/namespaces/:namespace/changes", get(list_changes))
        .route(
            "/v0/admin/namespaces/:namespace/checkpoint",
            post(create_checkpoint),
        )
        .route(
            "/v0/admin/namespaces/:namespace/retention/advance",
            post(advance_retention),
        )
        .route("/v0/admin/namespaces/:namespace/gc", post(gc_namespace))
        .with_state(state)
}

/// Opens the server's runtime handles inside the serving runtime.
///
/// The long-lived server writer opts into background maintenance; the
/// reader shares its caches so read endpoints observe writes immediately;
/// the admin handle drives the explicit maintenance endpoints under its own
/// actor identity. All three deliberately share one provider client inside
/// this one runtime ownership domain.
async fn build_handles(
    config: &ServerConfig,
    store: SharedStore,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    build_handles_with_metrics_jsonl_path(
        config,
        store,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
    )
    .await
}

async fn build_handles_with_metrics_jsonl_path(
    config: &ServerConfig,
    store: SharedStore,
    metrics_jsonl_path: Option<OsString>,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    let metrics_recorder = object_store_metrics_recorder(metrics_jsonl_path)?;
    let trace_store_kind = TraceStoreKind::from(config.store.kind());
    let runtime_error = |error: loonfs::RuntimeError| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    };

    let mut writer_builder = FsWriter::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
        .writer_version(config.writer_version.clone())
        .background_work(FsBackgroundWork::Enabled)
        .runtime_cache(config.runtime_cache_config())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind);
    if let Some(recorder) = metrics_recorder.clone() {
        writer_builder = writer_builder.metrics_recorder(recorder);
    }
    let writer = writer_builder.build().await.map_err(runtime_error)?;
    let reader = writer.reader();

    let mut admin_builder = FsAdmin::builder_with_store(store)
        .actor_id(format!("{}-admin", config.writer_id))
        .actor_version(config.writer_version.clone())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind);
    if let Some(recorder) = metrics_recorder {
        admin_builder = admin_builder.metrics_recorder(recorder);
    }
    let admin = admin_builder.build().await.map_err(runtime_error)?;

    Ok((writer, reader, admin))
}

fn object_store_metrics_recorder(
    metrics_jsonl_path: Option<OsString>,
) -> Result<Option<Arc<dyn ObjectStoreMetricsRecorder>>, ServerConfigError> {
    let Some(path) = metrics_jsonl_path else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }
    let path = std::path::PathBuf::from(path);
    JsonlObjectStoreMetricsRecorder::create(&path)
        .map(|recorder| Some(Arc::new(recorder) as Arc<dyn ObjectStoreMetricsRecorder>))
        .map_err(|error| ServerConfigError::InvalidField {
            field: OBJECT_STORE_METRICS_JSONL_ENV,
            reason: error.to_string(),
        })
}

/// Failure starting or running the HTTP server.
#[derive(Debug, Error)]
pub enum ServeError {
    #[error("invalid bind address `{addr}`: {source}")]
    BindAddr {
        addr: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("invalid server config: {0}")]
    Config(#[from] ServerConfigError),
    #[error("failed to bind `{addr}`: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("server failed while serving requests: {0}")]
    Serve(#[source] std::io::Error),
    #[error("background maintenance did not settle during shutdown: {0}")]
    Shutdown(#[source] loonfs::RuntimeError),
}

/// Serves until ctrl-c or SIGTERM, then shuts down gracefully: the listener
/// stops accepting, in-flight requests drain, and the writer's scheduled
/// background maintenance settles before this returns.
pub async fn serve(config: ServerConfig) -> Result<(), ServeError> {
    serve_with_shutdown(config, shutdown_signal()).await
}

/// [`serve`] with a caller-supplied shutdown trigger instead of process
/// signals, for hosts that manage their own lifecycle.
pub async fn serve_with_shutdown(
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let bind: SocketAddr = config.bind.parse().map_err(|source| ServeError::BindAddr {
        addr: config.bind.clone(),
        source,
    })?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|source| ServeError::Bind { addr: bind, source })?;
    serve_on(listener, config, shutdown).await
}

async fn serve_on(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let (app, writer) = app_parts(config).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)?;
    // The listener has drained; settle writer-owned maintenance so a
    // checkpoint tick in flight is not torn down mid-write-set. Panicked
    // ticks surface here rather than disappearing with the process.
    writer.close().await.map_err(ServeError::Shutdown)
}

/// Resolves on ctrl-c or, on unix, SIGTERM — the stop signal container
/// orchestrators send before a kill.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/health",
        tag = "health",
        summary = "Check health",
        description = "Returns `ok` when the server is running and can accept requests.",
        security(()),
        responses((status = 200, description = "Server health check", body = String))
    )
)]
async fn health() -> &'static str {
    "ok"
}

fn authorize(config: &ServerConfig, headers: &HeaderMap) -> Result<(), ApiResponseError> {
    let Some(expected) = &config.auth_token else {
        return Ok(());
    };
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if actual == format!("Bearer {}", expected.expose()) {
        Ok(())
    } else {
        Err(ApiResponseError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid bearer token",
        ))
    }
}

fn parse_namespace_id(value: String) -> Result<NamespaceId, ApiResponseError> {
    NamespaceId::parse(&value).map_err(ApiResponseError::invalid_namespace_id)
}

/// The decoded `namespace` path segment, deserialized by name so routes with
/// additional path parameters can share the extractor.
#[derive(Debug, serde::Deserialize)]
struct NamespaceSegment {
    namespace: String,
}

/// Extractor for the `:namespace` path segment of namespace-scoped routes.
///
/// The segment is parsed into a [`NamespaceId`] at extraction time, but the
/// outcome is surfaced through [`NamespaceIdPath::into_id`] inside the
/// handler body rather than as an extractor rejection: every handler
/// authorizes before validating the namespace id, and rejecting during
/// extraction would let a malformed id short-circuit `authorize` and turn
/// today's 401 into a 400 for unauthorized requests.
struct NamespaceIdPath(Result<NamespaceId, ApiResponseError>);

impl NamespaceIdPath {
    /// Returns the parsed namespace id, or the same 400
    /// `invalid_namespace_id` response [`parse_namespace_id`] produces.
    fn into_id(self) -> Result<NamespaceId, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for NamespaceIdPath
where
    S: Send + Sync,
{
    type Rejection = PathRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let AxumPath(NamespaceSegment { namespace }) =
            AxumPath::<NamespaceSegment>::from_request_parts(parts, state).await?;
        Ok(Self(parse_namespace_id(namespace)))
    }
}

/// A `Json` extractor whose rejections stay inside the error contract:
/// malformed bodies answer 400 with an `invalid_request` `ApiError` body
/// instead of the raw framework rejection.
struct AppJson<T>(T);

#[async_trait]
impl<S, T> FromRequest<S> for AppJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiResponseError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(AppJson(value)),
            Err(rejection) => Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &rejection.body_text(),
            )),
        }
    }
}

/// Like [`AppJson`], but an absent (empty) body is `None` rather than an
/// error, while a present-but-malformed body still answers 400 in-envelope.
struct OptionalAppJson<T>(Option<T>);

const MAX_OPTIONAL_JSON_BODY_BYTES: usize = 1024 * 1024;

#[async_trait]
impl<S, T> FromRequest<S> for OptionalAppJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let body = axum::body::to_bytes(req.into_body(), MAX_OPTIONAL_JSON_BODY_BYTES)
            .await
            .map_err(|error| {
                ApiResponseError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::InvalidRequest,
                    &format!("request body unreadable: {error}"),
                )
            })?;
        if body.is_empty() {
            return Ok(OptionalAppJson(None));
        }
        let value = serde_json::from_slice(&body).map_err(|error| {
            ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &format!("request body is not valid JSON for this operation: {error}"),
            )
        })?;
        Ok(OptionalAppJson(Some(value)))
    }
}
