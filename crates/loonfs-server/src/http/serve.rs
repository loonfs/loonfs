//! HTTP application construction, the listener service, and where its
//! graceful shutdown is triggered from.

use super::metrics::ServerMetrics;
use super::router;
use super::tls::{self, TlsConfigError, TlsListener};
use crate::config::{ServerConfig, ServerConfigError};
use crate::local_cache::FoyerStoredMetadataBlockCache;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::{
    FsMaintenance, FsReader, FsWriter, GarbageCollectionJob, MaintenanceHandle,
    MaintenanceHintObserver, MaintenanceHintRelay, MaintenanceJob, MaintenanceProbe,
    MaintenanceRegistry, MaintenanceRunner, MetadataCompactionJob, MetadataMaintenanceJob,
    SharedObjectStore, StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, TraceMode,
    TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_grep::{
    new_grep_block_cache, GrepGcJob, GrepMaintenanceJob, GrepService, GrepWorker,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES, GREP_INDEX_JOB,
};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeReport};
use std::ffi::OsString;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
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

/// Request handles built over one shared store client.
///
/// Read handlers use `reader`, mutations use `writer`, and maintenance uses
/// `maintenance`. After the listener drains, the host shuts down the writer
/// and the optional maintenance runner. `reader` is stored separately because
/// most handlers require only read access.
#[derive(Clone)]
pub struct AppState {
    pub(super) config: Arc<ServerConfig>,
    /// Writer handle that owns mutation and publication work.
    pub writer: FsWriter,
    pub(super) reader: FsReader,
    pub(super) maintenance: FsMaintenance,
    pub jobs: MaintenanceRegistry,
    pub runner: Option<MaintenanceRunner>,
    /// The store itself, for the one endpoint whose subject is the store
    /// rather than a namespace: the contract probe. It is the same
    /// instrumented client the handles were built on, so a probe measures
    /// what production traffic measures.
    pub(super) probe_store: SharedObjectStore,
    /// The direct transfers this deployment can authorize, as its store
    /// settled them at construction. Each feature is read from its own
    /// field; nothing here re-derives what the store already decided.
    pub(super) direct_transfers: Option<DirectTransferIssuers>,
    pub(super) grep_worker: Option<GrepWorker<SharedObjectStore>>,
    /// The grep query service: one process-wide decoded-block cache for
    /// grep's own segments, held here because grep is a composed extension
    /// rather than part of the runtime the handles come from.
    pub(super) grep_service: Option<Arc<GrepService>>,
    /// Present when this deployment maintains the index automatically: how a
    /// request tells the runner a namespace may have indexing to do. Absent
    /// under `maintenance = "manual"`, where the mutating index routes still
    /// work and nothing schedules itself behind them.
    pub(super) grep_maintenance: Option<GrepMaintenance>,
    /// Bounds concurrently streamed proxied-upload bodies; bodies forward to
    /// the store incrementally, so worst-case upload memory is this times one
    /// streamed part. Requests past the cap answer 503 `server_busy` before
    /// any transfer.
    pub(super) upload_permits: Arc<Semaphore>,
    /// Bounds concurrently materialized proxied content reads the same way:
    /// worst-case download memory is
    /// `max_concurrent_downloads * max_download_bytes`.
    pub(super) download_permits: Arc<Semaphore>,
    /// Shared recorder for runtime and request-level metrics. `GET /metrics`
    /// renders its snapshot. It is always installed.
    pub(super) metrics: Arc<ServerMetrics>,
    /// Optional node-local block cache.
    ///
    /// Runtime handles use it through the shared cache interface. The server
    /// retains the concrete type to read foyer statistics and close it during
    /// shutdown.
    pub local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
}

impl AppState {
    pub(super) fn grep_worker(&self) -> &GrepWorker<SharedObjectStore> {
        self.grep_worker
            .as_ref()
            .expect("grep routes should carry a grep worker")
    }

    pub(super) fn grep_service(&self) -> &GrepService {
        self.grep_service
            .as_deref()
            .expect("grep routes should carry a grep service")
    }
}

/// Request-side access to grep maintenance.
///
/// Requests may probe whether indexing is due and submit a non-blocking nudge.
/// The maintenance runner owns admission, concurrency, backoff, and shutdown.
#[derive(Clone)]
pub(super) struct GrepMaintenance {
    handle: MaintenanceHandle,
    job: Arc<GrepMaintenanceJob<SharedObjectStore>>,
}

impl GrepMaintenance {
    /// Asks for one bounded indexing step as soon as a permit frees.
    /// Repeated asks coalesce into one run.
    pub(super) fn nudge(&self, namespace_id: &NamespaceId) {
        self.handle.nudge(GREP_INDEX_JOB, namespace_id);
    }

    /// Nudges a namespace only when the probe reports that indexing is due.
    ///
    /// Probe failures do not schedule work because the indexing step would fail
    /// on the same unreadable state.
    pub(super) async fn nudge_if_behind(&self, namespace_id: &NamespaceId) {
        if matches!(
            self.job.probe(namespace_id).await,
            Ok(MaintenanceProbe::Due)
        ) {
            self.nudge(namespace_id);
        }
    }
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
    let metrics = ServerMetrics::new();
    // Two switches decide automatic grep indexing and nothing else does:
    // whether this server maintains anything automatically, and whether its
    // grep mode maintains the index.
    let automatic = config.maintenance.runs_automatically();
    let maintains_grep_index = config.grep.mode.maintains_index();
    let (maintenance_hint_observer, maintenance_hint_receiver) = if automatic {
        let (observer, receiver) = MaintenanceHintRelay::new(
            std::num::NonZeroUsize::new(4096).expect("relay capacity is nonzero"),
        );
        (Some(observer), Some(receiver))
    } else {
        (None, None)
    };
    // Opened before the handles, because the handles are what it is
    // installed on: a directory that cannot be owned fails startup here
    // rather than after a runtime is already running on it.
    let local_cache = open_local_cache(&config, &metrics).await?;
    // Grep reads and checkpoints through the same handles the HTTP planes
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
    let runner = if automatic {
        let runner = MaintenanceRunner::builder(jobs.clone())
            .max_concurrent(config.max_concurrent_maintenance)
            .metrics_recorder(metrics.recorder())
            .build()
            .map_err(maintenance_config_error)?;
        runner.attach_hints(
            maintenance_hint_receiver.expect("automatic maintenance has a hint relay"),
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
    let config = Arc::new(config);
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
        maintenance,
        jobs,
        runner,
        probe_store,
        direct_transfers,
        grep_worker,
        grep_service,
        grep_maintenance,
        metrics,
        local_cache,
    };
    Ok((router(state.clone()), state))
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

/// Opens the node-local block cache the config asks for, or answers `None`
/// where it asks for none.
///
/// The one place the cache is opened, so [`check_config`] takes the
/// configured directory exactly the way a start takes it: the root is
/// created if it is missing, the lock is claimed, and the disk tier is
/// allocated.
async fn open_local_cache(
    config: &ServerConfig,
    metrics: &ServerMetrics,
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
    metrics: &ServerMetrics,
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
/// This checks the TLS identity and opens the local cache through the same
/// paths used at startup. It does not bind the configured address or contact
/// the object store. Use `loonfs-server --probe-store` or
/// `loonfs maintenance store probe` to test storage access. Constructing a local
/// store still creates its root directory.
pub async fn check_config(config: &ServerConfig) -> Result<(), ServeError> {
    config.validate()?;
    config.object_store()?;
    // Building the identity is the whole check; nothing here serves with it.
    let _identity = tls_server_config(config).map_err(ServeError::Tls)?;
    if let Some(local_cache) = open_local_cache(config, &ServerMetrics::new()).await? {
        // Closing is what releases the directory lock, so a check leaves the
        // directory in the state the start that follows it needs to find.
        local_cache
            .close()
            .await
            .map_err(ServeError::LocalCacheClose)?;
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
