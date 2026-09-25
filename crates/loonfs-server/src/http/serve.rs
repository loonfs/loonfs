//! HTTP application construction, the listener service, and where its
//! graceful shutdown is triggered from.

use super::router;
use super::tls::{self, TlsConfigError, TlsListener};
use crate::config::{MaintenanceMode, ServerConfig, ServerConfigError};
use crate::local_cache::FoyerStoredMetadataBlockCache;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::{
    maintenance_hint_relay, FsMaintenance, FsReader, FsWriter, GarbageCollectionJob,
    MaintenanceHintObserver, MaintenanceRegistry, MaintenanceRunner, MetadataCompactionJob,
    MetadataMaintenanceJob, SharedObjectStore, StoredMetadataBlockCache,
    StoredMetadataBlockCacheCloseError, TraceMode, TraceStoreKind,
};
use loonfs_grep::{
    new_grep_block_cache, GrepGcJob, GrepMaintenanceJob, GrepService, GrepWorker,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_http::{
    AuthPolicy, BindingOptions, BindingState, GrepMaintenance, HttpMetrics, RouterSurface,
};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeReport};
use std::ffi::OsString;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

#[derive(Clone, Default)]
struct RequestDrain {
    inner: Arc<RequestDrainInner>,
}

#[derive(Default)]
struct RequestDrainInner {
    active: AtomicUsize,
    idle: Notify,
}

impl RequestDrain {
    fn start(&self) -> ActiveRequest {
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        ActiveRequest {
            drain: self.clone(),
        }
    }

    async fn settle(&self) {
        loop {
            let idle = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

struct ActiveRequest {
    drain: RequestDrain,
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        if self.drain.inner.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drain.inner.idle.notify_waiters();
        }
    }
}

async fn track_request(
    State(drain): State<RequestDrain>,
    request: Request,
    next: Next,
) -> Response {
    let active = drain.start();
    let response = next.run(request).await;
    response.map(|body| {
        Body::new(DrainedBody {
            body,
            _active: active,
        })
    })
}

/// A response body whose request stays active until the last frame is sent.
///
/// Framing passes through untouched — in particular the exact size hint — so
/// a sized response keeps its `Content-Length` instead of turning chunked.
struct DrainedBody {
    body: Body,
    _active: ActiveRequest,
}

impl http_body::Body for DrainedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

/// Handles and resources whose shutdown belongs to this host.
#[derive(Clone)]
pub struct AppState {
    pub binding: BindingState,
    pub writer: FsWriter,
    pub jobs: MaintenanceRegistry,
    pub runner: Option<MaintenanceRunner>,
    pub local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
}

/// Optional inputs for building the HTTP application.
#[derive(Default)]
pub struct AppOptions {
    /// Existing object store to use instead of building one from the config.
    pub store: Option<SharedObjectStore>,
    /// Direct-transfer issuers to use instead of the store's issuers.
    pub direct_transfers: Option<DirectTransferIssuers>,
}

/// Builds the router and returns its state.
///
/// After an embedded listener drains, the host must shut down
/// [`AppState::writer`] and [`AppState::runner`], then close
/// [`AppState::local_cache`] so in-memory entries are flushed. [`serve`]
/// performs these steps automatically. The runner and cache are optional.
pub async fn app(
    config: ServerConfig,
    options: AppOptions,
) -> Result<(Router, AppState), ServerConfigError> {
    build_app(config, options, RouterSurface::Standalone).await
}

/// Builds the filesystem and query API for composition into another server.
///
/// Uses the same handlers, authentication, limits, and error contract as [`app`],
/// but registers no health, readiness, metrics, or maintenance routes (including
/// the disabled-maintenance wildcard). The host owns those operational surfaces.
/// Capabilities describe only the served API groups. Configured background
/// maintenance is preserved; its HTTP serving flag is ignored.
///
/// The host must drain requests and shut down the returned handles as described
/// by [`app`]. This constructor does not supply tenant admission or isolation;
/// callers remain responsible for selecting the store and trusted request context.
pub async fn filesystem_app(
    mut config: ServerConfig,
    options: AppOptions,
) -> Result<(Router, AppState), ServerConfigError> {
    config.maintenance = if config.maintenance.maintains() {
        MaintenanceMode::MaintainOnly
    } else {
        MaintenanceMode::Disabled
    };
    build_app(config, options, RouterSurface::Filesystem).await
}

async fn build_app(
    config: ServerConfig,
    options: AppOptions,
    surface: RouterSurface,
) -> Result<(Router, AppState), ServerConfigError> {
    // The one unavoidable validation point: configs that skipped
    // `load_server_config` (direct Rust construction) fail here exactly as
    // file-loaded ones fail at load.
    config.validate()?;
    let AppOptions {
        store,
        direct_transfers,
    } = options;
    let (store, direct_transfers) = match store {
        Some(store) => (store, direct_transfers),
        None => {
            let store = config.object_store()?;
            let direct_transfers = direct_transfers.or_else(|| store.direct_transfers());
            (store.into_shared(), direct_transfers)
        }
    };
    let metrics = HttpMetrics::new();
    // Two switches decide scheduled grep indexing and nothing else does:
    // whether this server maintains anything, and whether its
    // grep mode maintains the index.
    let maintains = config.maintenance.maintains();
    let maintains_grep_index = config.grep.mode.maintains_index();
    let (maintenance_hint_observer, maintenance_hint_receiver) = if maintains {
        let (observer, receiver) =
            maintenance_hint_relay(NonZeroUsize::new(4096).expect("relay capacity is nonzero"));
        (Some(observer), Some(receiver))
    } else {
        (None, None)
    };
    // Opened before the handles, because the handles are what it is
    // installed on: a directory that cannot be owned fails startup here
    // rather than after a runtime is already running on it.
    let local_cache = open_local_cache(&config, &metrics).await?;
    // Grep reads and checkpoints through the same handles the HTTP API groups
    // use, so it is composed after them. Nothing has to be wired back into
    // the writer for its publications to reach the index: the job says on
    // the trait that publications concern it, and registering it is what
    // subscribes it.
    let (writer, reader, maintenance) = build_handles(
        &config,
        store,
        &metrics,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
        local_cache.clone(),
        maintenance_hint_observer,
    )
    .await?;
    let probe_store = writer.object_store();
    let grep_block_cache = Arc::new(new_grep_block_cache(
        DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
        metrics.recorder().as_ref(),
    ));
    // A deployment that maintains the index needs a worker whether or not it
    // answers queries with one. It runs on the writer's own instrumented
    // client, so the grep-owned traffic is measured like every other
    // request instead of escaping on a second, raw client.
    let grep_worker =
        (config.grep.mode.serves_grep() || config.grep.mode.maintains_index()).then(|| {
            GrepWorker::with_block_cache(
                writer.object_store(),
                reader.clone(),
                maintenance.clone(),
                Arc::clone(&grep_block_cache),
            )
        });
    let grep_service = config
        .grep
        .mode
        .serves_grep()
        .then(|| Arc::new(GrepService::new(Arc::clone(&grep_block_cache))));
    let jobs = MaintenanceRegistry::new();
    jobs.register(Arc::new(MetadataMaintenanceJob::new(maintenance.clone())))
        .map_err(maintenance_config_error)?;
    jobs.register(Arc::new(MetadataCompactionJob::new(maintenance.clone())))
        .map_err(maintenance_config_error)?;
    jobs.register(Arc::new(GarbageCollectionJob::new(maintenance.clone())))
        .map_err(maintenance_config_error)?;
    let grep_job = if maintains_grep_index {
        let grep_worker = grep_worker
            .as_ref()
            .expect("grep maintenance requires a grep worker");
        let policy = config
            .grep
            .worker_config()
            .build_policy()
            .map_err(grep_config_error)?;
        let job = Arc::new(GrepMaintenanceJob::new(grep_worker.clone(), policy));
        jobs.register(job.clone()).map_err(grep_config_error)?;
        jobs.register(Arc::new(GrepGcJob::new(grep_worker.clone())))
            .map_err(grep_config_error)?;
        Some(job)
    } else {
        None
    };
    let runner = if maintains {
        let runner = MaintenanceRunner::builder(jobs.clone())
            .max_concurrent(
                NonZeroUsize::new(config.max_concurrent_maintenance)
                    .expect("validated maintenance concurrency should be nonzero"),
            )
            .metrics_recorder(metrics.recorder())
            .build()
            .map_err(maintenance_config_error)?;
        runner.attach_hints(
            maintenance_hint_receiver.expect("maintained mode should have a hint relay"),
        );
        Some(runner)
    } else {
        None
    };
    let grep_maintenance = runner
        .as_ref()
        .zip(grep_job)
        .map(|(runner, job)| GrepMaintenance {
            handle: runner.handle(),
            job,
        });
    let options = Arc::new(BindingOptions {
        serves_grep: config.grep.mode.serves_grep(),
        maintains_grep_index,
        serves_maintenance: config.maintenance.serves(),
        snapshot_policy: loonfs::SnapshotPolicy {
            max_ttl_ms: config.snapshot_max_ttl_ms,
            max_lifetime_ms: config.snapshot_max_lifetime_ms,
            max_live_per_namespace: config.snapshot_max_live_per_namespace,
        },
        max_download_bytes: config.max_download_bytes,
        max_upload_bytes: config.max_upload_bytes,
        max_concurrent_uploads: config.max_concurrent_uploads,
        max_concurrent_downloads: config.max_concurrent_downloads,
        inline_content: config.inline_content.resolve(),
        content_token_secret: config.content_token_secret.clone(),
        request_deadline_ms: config.request_deadline_ms,
        store_kind: config.store.kind(),
        auth_policy: config
            .auth_token
            .clone()
            .map_or(AuthPolicy::Unauthenticated, AuthPolicy::BearerToken),
    });
    let binding = BindingState {
        upload_permits: Arc::new(Semaphore::new(
            config.max_concurrent_uploads.min(Semaphore::MAX_PERMITS),
        )),
        download_permits: Arc::new(Semaphore::new(
            config.max_concurrent_downloads.min(Semaphore::MAX_PERMITS),
        )),
        options,
        writer: writer.clone(),
        reader,
        maintenance,
        probe_store,
        direct_transfers,
        grep_worker,
        grep_service,
        grep_maintenance,
        metrics,
    };
    let state = AppState {
        binding,
        writer,
        jobs,
        runner,
        local_cache,
    };
    Ok((router(state.clone(), surface), state))
}

fn maintenance_config_error(error: impl std::fmt::Display) -> ServerConfigError {
    ServerConfigError::InvalidField {
        field: "maintenance",
        reason: error.to_string(),
    }
}

fn grep_config_error(error: impl std::fmt::Display) -> ServerConfigError {
    ServerConfigError::InvalidField {
        field: "grep",
        reason: error.to_string(),
    }
}

async fn open_local_cache(
    config: &ServerConfig,
    metrics: &HttpMetrics,
) -> Result<Option<Arc<FoyerStoredMetadataBlockCache>>, ServerConfigError> {
    match &config.local_cache {
        Some(local_cache) => Ok(Some(Arc::new(
            FoyerStoredMetadataBlockCache::open(local_cache, metrics.recorder().as_ref()).await?,
        ))),
        None => Ok(None),
    }
}

/// Builds the process handles over one store and one metrics recorder.
///
/// An optional JSONL recorder receives the same object-store samples. The
/// local block cache is installed once on the writer's shared runtime core,
/// so the reader and maintenance use the same decoded cache hierarchy.
pub(super) async fn build_handles(
    config: &ServerConfig,
    store: SharedObjectStore,
    metrics: &HttpMetrics,
    metrics_jsonl_path: Option<OsString>,
    local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
    maintenance_hint_observer: Option<MaintenanceHintObserver>,
) -> Result<(FsWriter, FsReader, FsMaintenance), ServerConfigError> {
    let trace_store_kind = TraceStoreKind::from(config.store.kind());
    let samples = object_store_metrics_recorder(metrics_jsonl_path)?;
    let runtime_error = |error: loonfs::RuntimeError| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    };

    let mut writer_builder = FsWriter::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
        .min_publish_interval_ms(config.min_publish_interval_ms)
        .max_writer_sessions(
            std::num::NonZeroUsize::new(config.max_writer_sessions)
                .expect("validated maximum writer sessions should be nonzero"),
        )
        .publication_limits(config.publication.resolve())
        .inline_content(config.inline_content.resolve())
        .max_concurrent_folds(
            std::num::NonZeroUsize::new(config.max_concurrent_folds)
                .expect("validated maximum concurrent folds should be nonzero"),
        )
        // The reader below shares this core, so the read cap covers every
        // proxied content read the server serves.
        .max_read_content_bytes(config.max_download_bytes)
        .runtime_cache(config.runtime_cache_config())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind)
        .metrics_recorder(metrics.recorder());
    if let Some(observer) = maintenance_hint_observer {
        writer_builder = writer_builder.maintenance_hint_observer(move |hint| observer(hint));
    }
    if let Some(samples) = &samples {
        writer_builder = writer_builder.object_store_metrics_recorder(Arc::clone(samples));
    }
    if let Some(local_cache) = local_cache {
        writer_builder = writer_builder.stored_metadata_block_cache(local_cache);
    }
    let writer = writer_builder.build().await.map_err(runtime_error)?;
    let reader = writer.reader();
    let maintenance = writer
        .maintenance_handle(format!("{}-maintenance", config.writer_id))
        .map_err(runtime_error)?;

    Ok((writer, reader, maintenance))
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
    #[error("failed to load the configured TLS identity: {0}")]
    Tls(#[source] TlsConfigError),
    #[error("server failed while serving requests: {0}")]
    Serve(#[source] std::io::Error),
    #[error("writer or maintenance shutdown did not settle: {0}")]
    Shutdown(#[source] loonfs::RuntimeError),
    #[error("the local block cache did not close during shutdown: {0}")]
    LocalCacheClose(#[source] StoredMetadataBlockCacheCloseError),
}

/// Builds the rustls configuration this deployment terminates with, or
/// answers `None` where TLS terminates in front of the process.
///
/// The one place the identity is loaded, so [`check_config`] reads the files
/// a start reads and reports what a start reports.
fn tls_server_config(
    config: &ServerConfig,
) -> Result<Option<rustls::ServerConfig>, TlsConfigError> {
    config.tls.as_ref().map(tls::server_config).transpose()
}

/// Validates configuration and startup resources without starting the server.
///
/// This checks the TLS identity, local cache sizes, and cache directory access.
/// It does not bind the configured address or contact
/// the object store. Use `loonfs-server --probe-store` or
/// `loonfs maintenance store probe` to test storage access. Constructing a local
/// store still creates its root directory. The cache directory is created if
/// missing and checked with a temporary file. No cache device is opened.
pub async fn check_config(config: &ServerConfig) -> Result<(), ServeError> {
    config.validate()?;
    config.object_store()?;
    // Building the identity is the whole check; nothing here serves with it.
    let _identity = tls_server_config(config).map_err(ServeError::Tls)?;
    if let Some(local_cache) = &config.local_cache {
        let path = std::path::Path::new(local_cache.path.trim());
        std::fs::create_dir_all(path)
            .and_then(|()| tempfile::tempfile_in(path))
            .map_err(|error| ServerConfigError::InvalidField {
                field: "local_cache.path",
                reason: format!("cannot write to `{}`: {error}", path.display()),
            })?;
    }
    Ok(())
}

/// Runs the object-store contract probe against the store this config builds.
pub async fn probe_store(config: &ServerConfig) -> Result<StoreProbeReport, ServeError> {
    let store = config.object_store()?.into_shared();
    let run_id = loonfs_api::generated_id("probe");
    Ok(run_store_contract_probe(store.as_ref(), &run_id).await)
}

/// Serves until ctrl-c or SIGTERM, then shuts down gracefully. Admission
/// closes while the listener remains available to reads and probes. After
/// active requests drain, the listener closes and accepted work settles.
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
    // The identity is loaded before the bind, so a deployment with an
    // unreadable certificate fails without ever having held the port.
    let tls = tls_server_config(&config).map_err(ServeError::Tls)?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|source| ServeError::Bind { addr: bind, source })?;
    // The first line a deployment sees. An idle server logs nothing until
    // a request arrives, so without this line a container that started
    // correctly and a container that is stuck look the same. This one says
    // the config loaded and the process holds the port.
    tracing::info!(
        bind = %bind,
        store = config.store.kind().as_str(),
        "loonfs-server is listening"
    );
    match tls {
        Some(tls) => serve_on(TlsListener::new(listener, tls), config, shutdown).await,
        None => serve_on(listener, config, shutdown).await,
    }
}

/// The one serving body, over whichever listener the deployment configured.
/// Plaintext and TLS differ in what `accept` returns and in nothing else.
/// Both close admission, drain requests, close the listener, and settle the
/// writer in the same order.
pub(super) async fn serve_on<L>(
    listener: L,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let shutdown_deadline_ms = config.shutdown_deadline_ms;
    let (router, state) = app(config, AppOptions::default()).await?;
    serve_and_settle(
        listener,
        router,
        state.writer,
        state.runner,
        state.local_cache,
        shutdown_deadline_ms,
        shutdown,
    )
    .await
}

/// Serves until the shutdown trigger fires, then settles what this process
/// owns, in the order it has to be settled in.
///
/// Kept apart from [`serve_on`] so the settling order is a thing a test can
/// drive with handles it holds.
#[allow(clippy::disallowed_methods)]
// Monotonic time is used only to limit graceful shutdown.
pub(super) async fn serve_and_settle<L>(
    listener: L,
    router: Router,
    writer: FsWriter,
    runner: Option<MaintenanceRunner>,
    local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
    shutdown_deadline_ms: u64,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let requests = RequestDrain::default();
    let router = router.layer(axum::middleware::from_fn_with_state(
        requests.clone(),
        track_request,
    ));
    let (listener_close_tx, listener_close_rx) = tokio::sync::oneshot::channel();
    let mut server = Box::pin(
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = listener_close_rx.await;
            })
            .into_future(),
    );
    let mut shutdown = Box::pin(shutdown);
    let served = tokio::select! {
        result = server.as_mut() => result,
        () = shutdown.as_mut() => {
            // This is synchronous so readiness changes before the drain waits.
            writer.close_admission_for_shutdown();
            if let Some(runner) = &runner {
                runner.close_admission();
            }
            // The budget starts when shutdown fires. It does not limit uptime.
            let deadline = tokio::time::Instant::now()
                + Duration::from_millis(shutdown_deadline_ms);
            let mut deadline = Box::pin(tokio::time::sleep_until(deadline));
            drain_with_deadline(
                server.as_mut(),
                &requests,
                listener_close_tx,
                deadline.as_mut(),
                shutdown_deadline_ms,
            )
            .await
        }
    };
    // Dropping the server cancels requests left behind by an expired drain.
    drop(server);
    served.map_err(ServeError::Serve)?;
    let writer_settled = writer.shutdown().await;
    let runner_settled = match runner {
        Some(runner) => runner.shutdown().await,
        None => Ok(()),
    };
    let settled = writer_settled
        .and(runner_settled)
        .map_err(ServeError::Shutdown);
    // Close the cache after writer shutdown, even when writer shutdown fails.
    // Closing flushes retained memory entries to disk. If both steps fail, report
    // the writer failure.
    let closed = match local_cache {
        Some(local_cache) => local_cache
            .close()
            .await
            .map_err(ServeError::LocalCacheClose),
        None => Ok(()),
    };
    settled.and(closed)
}

async fn drain_with_deadline<S>(
    mut server: Pin<&mut S>,
    requests: &RequestDrain,
    listener_close_tx: tokio::sync::oneshot::Sender<()>,
    mut deadline: Pin<&mut tokio::time::Sleep>,
    shutdown_deadline_ms: u64,
) -> std::io::Result<()>
where
    S: Future<Output = std::io::Result<()>> + ?Sized,
{
    let deadline_passed = tokio::select! {
        result = server.as_mut() => return result,
        () = requests.settle() => false,
        () = deadline.as_mut() => true,
    };
    let _ = listener_close_tx.send(());
    let result = if deadline_passed {
        None
    } else {
        tokio::select! {
            result = server.as_mut() => Some(result),
            () = deadline.as_mut() => None,
        }
    };
    match result {
        Some(result) => result,
        None => {
            tracing::warn!(
                shutdown_deadline_ms,
                "graceful drain deadline passed; remaining requests are abandoned"
            );
            Ok(())
        }
    }
}

/// Resolves on ctrl-c or, on unix, SIGTERM — the stop signal container
/// orchestrators send before a kill.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("ctrl-c handler should install");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler should install")
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
