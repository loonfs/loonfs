//! The v0 HTTP surface of the LoonFS server.
//!
//! This module owns the route table, the server startup path, and the
//! request glue shared by every handler (bearer-token authorization,
//! `{namespace}` path parsing, and the body extractors). The handlers
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
mod handlers_query;
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
    advance_retention, create_checkpoint, create_namespace, delete_namespace, flush_wal,
    fork_namespace, gc_namespace, maintenance_tick, namespace_status, release_checkpoint,
};
use self::handlers_query::{disable_grams_index, enable_grams_index, grep, grep_not_supported};
use self::handlers_uploads::{begin_upload, complete_upload, upload_content};
use crate::config::{ServerConfig, ServerConfigError};
use axum::async_trait;
use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{
    DefaultBodyLimit, FromRequest, FromRequestParts, Path as AxumPath, Query, Request, State,
};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::publisher::PublisherRegistry;
use loonfs::{
    ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter, SharedObjectStore, TraceMode,
    TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_grep::{GrepWorker, GrepWorkerLoop, GrepWorkerLoopShutdown};
use loonfs_objectstore::presign::ObjectTransferIssuer;
use std::convert::Infallible;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Response header carrying the request's correlation id.
const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    /// Correlation id of the request being served. Scoped around every
    /// handler by [`with_request_id`]; [`error::ApiResponseError`] reads it
    /// when rendering an error body.
    pub(super) static REQUEST_ID: String;
}

/// Assigns each request a correlation id: every response carries it as the
/// `x-request-id` header, and error bodies repeat it as
/// `ApiError.request_id` so a caller's log line and the server's trace can
/// be joined without header plumbing.
async fn with_request_id(request: Request, next: Next) -> Response {
    let request_id = loonfs_api::generated_id("req");
    let mut response = REQUEST_ID
        .scope(request_id.clone(), next.run(request))
        .await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

/// Purpose-specific handles over one shared store client: read endpoints go
/// through `reader`, mutations through `writer` (and its publisher),
/// maintenance endpoints through `admin`. Shutdown-relevant handles also
/// live in the [`ServerLifecycle`] returned beside the router.
#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    writer: FsWriter,
    reader: FsReader,
    admin: FsAdmin,
    publisher: PublisherRegistry,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
    /// Bounds concurrently buffered proxied-upload bodies; with the
    /// per-request body limit this makes worst-case upload memory
    /// `max_concurrent_uploads * max_upload_bytes`. Requests past the cap
    /// answer 503 `server_busy` before any buffering.
    upload_permits: Arc<Semaphore>,
    /// Bounds concurrently materialized proxied content reads the same way:
    /// worst-case download memory is
    /// `max_concurrent_downloads * max_download_bytes`.
    download_permits: Arc<Semaphore>,
}

/// Everything the app spawns that must settle at shutdown: the optional
/// embedded grep loop, publisher publications, and writer maintenance.
///
/// [`serve`] drives this itself. A host embedding the [`Router`] on its own
/// HTTP server must call [`ServerLifecycle::shutdown`] after its listener
/// drains, or publisher tasks and writer maintenance outlive the listener
/// unobserved.
pub struct ServerLifecycle {
    writer: FsWriter,
    publisher: PublisherRegistry,
    grep: Option<GrepBackgroundWork>,
}

impl ServerLifecycle {
    /// Settles the app's spawned work in dependency order: the grep loop
    /// stops between bounded steps, publisher admission closes, admitted
    /// publications finish, then writer-scheduled maintenance settles.
    /// Panicked tasks surface as the returned error.
    pub async fn shutdown(self) -> Result<(), loonfs::RuntimeError> {
        if let Some(grep) = self.grep {
            grep.shutdown().await?;
        }
        self.publisher.close_admission();
        self.publisher.drain().await?;
        self.writer.shutdown_background().await
    }
}

/// One server-owned grep loop and the stop handle that lets shutdown join it.
struct GrepBackgroundWork {
    shutdown: GrepWorkerLoopShutdown,
    task: tokio::task::JoinHandle<()>,
}

impl GrepBackgroundWork {
    fn spawn(config: &ServerConfig, store: SharedObjectStore) -> Result<Self, ServerConfigError> {
        let worker = GrepWorker::new(
            store.clone(),
            format!("{}-grep", config.writer_id),
            loonfs_api::generated_id("wrs"),
            config.writer_version.clone(),
        );
        let worker_loop = GrepWorkerLoop::new(worker, store, config.grep.worker_config());
        let shutdown = worker_loop.shutdown_handle();
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            ServerConfigError::InvalidField {
                field: "runtime",
                reason: format!("server app must be built inside a Tokio runtime: {error}"),
            }
        })?;
        let task = runtime.spawn(worker_loop.run());
        Ok(Self { shutdown, task })
    }

    async fn shutdown(self) -> Result<(), loonfs::RuntimeError> {
        self.shutdown.request_shutdown();
        self.task.await.map_err(|error| {
            loonfs::RuntimeError::RuntimeTask(format!("grep worker task failed: {error}"))
        })
    }
}

/// Builds the HTTP application: the router that serves requests, and the
/// lifecycle handle its host must shut down after draining the listener.
pub async fn app(config: ServerConfig) -> Result<(Router, ServerLifecycle), ServerConfigError> {
    // The one unavoidable validation point: configs that skipped
    // `load_server_config` (direct Rust construction) fail here exactly as
    // file-loaded ones fail at load.
    config.validate()?;
    let store = config.object_store()?;
    let transfer_issuer = store.transfer_issuer();
    let store = Arc::new(store) as SharedObjectStore;
    let (router, lifecycle, _state) =
        app_with_store_and_transfer_issuer(config, store, transfer_issuer).await?;
    Ok((router, lifecycle))
}

#[cfg(test)]
async fn app_with_store(
    config: ServerConfig,
    store: SharedObjectStore,
) -> Result<Router, ServerConfigError> {
    Ok(app_with_store_and_transfer_issuer(config, store, None)
        .await?
        .0)
}

/// Test-only: the router plus its state, so tests can hold admission
/// permits or close publisher admission and observe the served answers.
#[cfg(test)]
async fn app_with_store_and_state(
    config: ServerConfig,
    store: SharedObjectStore,
) -> Result<(Router, AppState), ServerConfigError> {
    let (router, _lifecycle, state) =
        app_with_store_and_transfer_issuer(config, store, None).await?;
    Ok((router, state))
}

async fn app_with_store_and_transfer_issuer(
    config: ServerConfig,
    store: SharedObjectStore,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
) -> Result<(Router, ServerLifecycle, AppState), ServerConfigError> {
    let (writer, reader, admin) = build_handles(&config, store.clone()).await?;
    let grep = if config.grep.mode.runs_worker() {
        Some(GrepBackgroundWork::spawn(&config, store)?)
    } else {
        None
    };
    let config = Arc::new(config);
    let publisher = writer.publisher();
    let lifecycle = ServerLifecycle {
        writer: writer.clone(),
        publisher: publisher.clone(),
        grep,
    };
    let state = AppState {
        upload_permits: Arc::new(Semaphore::new(
            config.max_concurrent_uploads.min(Semaphore::MAX_PERMITS),
        )),
        download_permits: Arc::new(Semaphore::new(
            config.max_concurrent_downloads.min(Semaphore::MAX_PERMITS),
        )),
        config,
        writer,
        reader,
        admin,
        publisher,
        transfer_issuer,
    };
    Ok((router(state.clone()), lifecycle, state))
}

fn router(state: AppState) -> Router {
    // Two routes carry legitimately large bodies and get configured
    // budgets: upload content (raw file bytes) and commits (bulk metadata
    // JSON). Every other route keeps axum's conservative default limit.
    let max_upload_bytes = usize::try_from(state.config.max_upload_bytes).unwrap_or(usize::MAX);
    let max_commit_body_bytes =
        usize::try_from(state.config.max_commit_body_bytes).unwrap_or(usize::MAX);
    let grep_route = if state.config.grep.mode.serves_grep() {
        post(grep)
    } else {
        post(grep_not_supported)
    };
    let enable_grep_route = if state.config.grep.mode.serves_grep() {
        post(enable_grams_index)
    } else {
        post(grep_not_supported)
    };
    let disable_grep_route = if state.config.grep.mode.serves_grep() {
        post(disable_grams_index)
    } else {
        post(grep_not_supported)
    };
    Router::new()
        .route("/health", get(health))
        .route("/health/ready", get(health_ready))
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
        .route("/v0/namespaces/:namespace/query/grep", grep_route)
        .route(
            "/v0/admin/namespaces/:namespace/index/grams/enable",
            enable_grep_route,
        )
        .route(
            "/v0/admin/namespaces/:namespace/index/grams/disable",
            disable_grep_route,
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
            put(upload_content).layer(DefaultBodyLimit::max(max_upload_bytes)),
        )
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/complete",
            post(complete_upload),
        )
        .route(
            "/v0/namespaces/:namespace/commits",
            post(commit_operations).layer(DefaultBodyLimit::max(max_commit_body_bytes)),
        )
        .route("/v0/namespaces/:namespace/changes", get(list_changes))
        .route(
            "/v0/admin/namespaces/:namespace/checkpoints",
            post(create_checkpoint),
        )
        .route(
            "/v0/admin/namespaces/:namespace/checkpoints/:checkpoint_id/release",
            post(release_checkpoint),
        )
        .route("/v0/admin/namespaces/:namespace/wal/flush", post(flush_wal))
        .route(
            "/v0/admin/namespaces/:namespace/retention/advance",
            post(advance_retention),
        )
        .route(
            "/v0/admin/namespaces/:namespace/maintenance/tick",
            post(maintenance_tick),
        )
        .route("/v0/admin/namespaces/:namespace/gc", post(gc_namespace))
        // Unmatched paths and wrong methods answer inside the error
        // contract instead of axum's empty default bodies.
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn(with_request_id))
        .with_state(state)
}

/// 404 for paths outside the served surface. Deliberately unauthenticated:
/// the route set is public in the API spec, and `authorize` runs per
/// matched handler.
async fn route_not_found() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::NOT_FOUND,
        ErrorCode::RouteNotFound,
        "no v0 route matches this path; see the API spec for the served surface",
    )
}

/// 405 for matched paths hit with an unserved method.
async fn method_not_allowed() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::MethodNotAllowed,
        "this path exists but does not serve this HTTP method",
    )
}

/// Opens the server's runtime handles inside the serving runtime.
///
/// The long-lived server writer opts into background maintenance; the
/// reader shares its caches so read endpoints observe writes immediately;
/// the admin handle drives the explicit maintenance endpoints under its own
/// actor identity, sharing the writer's decoded-block cache under the
/// configured budget. All three deliberately share one provider client
/// inside this one runtime ownership domain.
async fn build_handles(
    config: &ServerConfig,
    store: SharedObjectStore,
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
    store: SharedObjectStore,
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
        .background_work(if config.background_maintenance {
            FsBackgroundWork::Enabled
        } else {
            FsBackgroundWork::ManualOnly
        })
        .min_publish_interval_ms(config.min_publish_interval_ms)
        // The reader below shares this core, so the read cap covers every
        // proxied content read the server serves.
        .max_read_content_bytes(config.max_download_bytes)
        .max_concurrent_maintenance(config.max_concurrent_maintenance)
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
        // The admin honors the configured cache sizing and shares the
        // writer's decoded-block cache instance, so explicit maintenance
        // reuses blocks reader traffic already decoded instead of
        // populating a second, default-sized cache.
        .runtime_cache(config.runtime_cache_config())
        .shared_metadata_table_cache(&writer)
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
    #[error("background work did not settle during shutdown: {0}")]
    Shutdown(#[source] loonfs::RuntimeError),
}

/// Serves until ctrl-c or SIGTERM, then shuts down gracefully: the listener
/// stops accepting, in-flight requests drain, the embedded grep loop stops,
/// publisher work finishes, and writer maintenance settles before this
/// returns.
pub async fn serve(config: ServerConfig) -> Result<(), ServeError> {
    serve_with_shutdown(config, shutdown_signal()).await
}

/// [`serve`] with a caller-supplied shutdown trigger instead of process
/// signals, for hosts that manage their own lifecycle.
pub async fn serve_with_shutdown(
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let bind = config.bind_addr()?;
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
    let (router, lifecycle) = app(config).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)?;
    // The listener has drained; stop grep between bounded steps, close
    // publisher admission, finish admitted publications, then settle
    // writer-owned maintenance. Panicked tasks surface here rather than
    // disappearing with the process.
    lifecycle.shutdown().await.map_err(ServeError::Shutdown)
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

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/health/ready",
        tag = "health",
        summary = "Check readiness",
        description = "Returns `ready` while the server admits new work. Once shutdown \
                       begins and publisher admission closes, answers 503 `shutting_down` \
                       so load balancers can drain the instance. `/health` stays the \
                       liveness probe: it only reports that the process is up.",
        security(()),
        responses(
            (status = 200, description = "The server admits new work", body = String),
            (status = 503, description = "Shutdown has begun; admission is closed", body = loonfs_api::ApiError)
        )
    )
)]
async fn health_ready(State(state): State<AppState>) -> Result<&'static str, ApiResponseError> {
    if state.publisher.is_admission_closed() {
        return Err(ApiResponseError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::ShuttingDown,
            "publisher admission is closed; the server is shutting down",
        ));
    }
    Ok("ready")
}

/// One admission cap answered in-envelope: 503 `server_busy`, distinct from
/// `shutting_down` (drain) and `commit_queue_full` (publisher backpressure).
fn server_busy_error(what: &str) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::ServerBusy,
        &format!("the server is at its concurrency limit for {what}; retry shortly"),
    )
    // Transfer slots clear in fractions of a second; one second is the
    // smallest Retry-After HTTP can express.
    .with_retry_after(1)
}

fn authorize(config: &ServerConfig, headers: &HeaderMap) -> Result<(), ApiResponseError> {
    let Some(expected) = &config.auth_token else {
        return Ok(());
    };
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let expected = format!("Bearer {}", expected.expose());
    if constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(ApiResponseError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid bearer token",
        ))
    }
}

/// Compares token bytes without short-circuiting on the first mismatch, so
/// response timing does not narrow down the token byte by byte. Length is
/// still observable; token values are not.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AxumPath::<NamespaceSegment>::from_request_parts(parts, state).await {
            Ok(AxumPath(NamespaceSegment { namespace })) => Ok(Self(parse_namespace_id(namespace))),
            Err(rejection) => Ok(Self(Err(invalid_path_params(&rejection)))),
        }
    }
}

fn invalid_path_params(rejection: &PathRejection) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidRequest,
        &format!("invalid path parameters: {rejection}"),
    )
}

/// Path extractor that never rejects at extraction: the parse outcome is
/// surfaced through [`AppPath::into_params`] inside the handler, after
/// `authorize`, so malformed path parameters answer inside the JSON error
/// envelope and never turn an unauthorized request's 401 into a 400.
struct AppPath<T>(Result<T, ApiResponseError>);

impl<T> AppPath<T> {
    fn into_params(self) -> Result<T, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for AppPath<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned + Send,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AxumPath::<T>::from_request_parts(parts, state).await {
            Ok(AxumPath(value)) => Ok(Self(Ok(value))),
            Err(rejection) => Ok(Self(Err(invalid_path_params(&rejection)))),
        }
    }
}

/// [`AppPath`]'s query-string twin: missing required parameters, values that
/// fail their field types (`after_seq=abc`), and undecodable query strings
/// all surface through [`AppQuery::into_params`] after `authorize`, inside
/// the envelope.
struct AppQuery<T>(Result<T, ApiResponseError>);

impl<T> AppQuery<T> {
    fn into_params(self) -> Result<T, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for AppQuery<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(Self(Ok(value))),
            Err(rejection) => Ok(Self(Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &format!("invalid query parameters: {rejection}"),
            )))),
        }
    }
}

/// A `Json` extractor whose rejections stay inside the error contract:
/// malformed bodies answer 400 with an `invalid_request` `ApiError` body
/// instead of the raw framework rejection, and authorization runs before
/// the body is read so a malformed body never turns an unauthorized
/// request's 401 into a 400.
struct AppJson<T>(T);

async fn extract_json<S, T>(
    req: axum::extract::Request,
    state: &S,
    body_too_large: fn() -> ApiResponseError,
) -> Result<T, ApiResponseError>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    match Json::<T>::from_request(req, state).await {
        Ok(Json(value)) => Ok(value),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(body_too_large())
        }
        Err(rejection) => Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &rejection.body_text(),
        )),
    }
}

#[async_trait]
impl<T> FromRequest<AppState> for AppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        extract_json(req, state, json_body_too_large_error)
            .await
            .map(AppJson)
    }
}

/// Commit JSON is both larger than the framework default and potentially
/// expensive to buffer, so authenticate before reading it and give its 413
/// the route-specific recovery guidance.
struct CommitAppJson<T>(T);

#[async_trait]
impl<T> FromRequest<AppState> for CommitAppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        extract_json(req, state, commit_body_too_large_error)
            .await
            .map(CommitAppJson)
    }
}

/// The proxied-upload body plus the admission permit that bounds how many
/// such bodies the server buffers at once.
///
/// Extraction runs the admission sequence in bounded-cost order:
/// authorization first (an unauthenticated caller must not occupy a
/// buffering slot), then a permit — or 503 `server_busy` — and only then is
/// the body buffered, so the permit covers the buffering itself, not just
/// the handler. The permit rides with the bytes and frees its slot when the
/// handler drops them. Rejections stay inside the error contract: a body
/// over the route's limit answers 413 `content_too_large`, other unreadable
/// bodies 400.
struct UploadBodyBytes {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

#[async_trait]
impl FromRequest<AppState> for UploadBodyBytes {
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        let permit = state
            .upload_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| server_busy_error("proxied uploads"))?;
        match Bytes::from_request(req, state).await {
            Ok(bytes) => Ok(UploadBodyBytes {
                bytes,
                _permit: permit,
            }),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(upload_body_too_large_error())
            }
            Err(rejection) => Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &rejection.body_text(),
            )),
        }
    }
}

/// 413 for over-limit upload bodies: the guidance names the upload byte cap
/// and the `direct_put` path that bypasses proxied buffering entirely.
fn upload_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "request body exceeds this deployment's limit; check the \
         `upload.max_content_bytes` capability limit, and prefer `direct_put` \
         uploads for large content",
    )
}

/// 413 for ordinary JSON routes that retain the framework's default bound.
fn json_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "JSON request body exceeds this route's body limit",
    )
}

/// 413 for over-limit commit JSON. Commit bodies are metadata, so the fix
/// is splitting the batch rather than routing content through `direct_put`.
fn commit_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "commit request body exceeds this deployment's \
         `commit.max_body_bytes` capability limit — split the commit into \
         smaller batches",
    )
}

/// Like [`AppJson`], but an absent (empty) body is `None` rather than an
/// error, while a present-but-malformed body still answers 400 in-envelope.
struct OptionalAppJson<T>(Option<T>);

const MAX_OPTIONAL_JSON_BODY_BYTES: usize = 1024 * 1024;

#[async_trait]
impl<T> FromRequest<AppState> for OptionalAppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
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
