//! HTTP application construction, the listener service, and where its
//! graceful shutdown is triggered from.

use super::metrics::ServerMetrics;
use super::router;
use super::tls::{self, TlsConfigError, TlsListener};
use crate::config::{ServerConfig, ServerConfigError};
use crate::local_cache::FoyerStoredMetadataBlockCache;
use axum::Router;
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::{
    FsAdmin, FsReader, FsWriter, MaintenanceHandle, MaintenanceJob, MaintenanceProbe,
    SharedObjectStore, StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, TraceMode,
    TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_grep::{
    GrepBlockCache, GrepGcJob, GrepMaintenanceJob, GrepService, GrepWorker,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES, GREP_INDEX_JOB,
};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeReport};
use std::ffi::OsString;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Request handles built over one shared store client.
///
/// Read handlers use `reader`, mutations use `writer`, and maintenance uses
/// `admin`. The host also shuts down `writer` after the listener drains.
/// `reader` is stored separately because most handlers require only read
/// access.
#[derive(Clone)]
pub(super) struct AppState {
    pub(super) config: Arc<ServerConfig>,
    pub(super) writer: FsWriter,
    pub(super) reader: FsReader,
    pub(super) admin: FsAdmin,
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
    /// request tells the writer's runner a namespace may have indexing to
    /// do. Absent under `maintenance = "manual"`, where the mutating index
    /// routes still work and nothing schedules itself behind them.
    pub(super) grep_maintenance: Option<GrepMaintenance>,
    /// Bounds concurrently buffered proxied-upload bodies; with the
    /// per-request body limit this makes worst-case upload memory
    /// `max_concurrent_uploads * max_upload_bytes`. Requests past the cap
    /// answer 503 `server_busy` before any buffering.
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
    pub(super) local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
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

/// Builds the router and returns the resources the host must shut down.
///
/// After an embedded listener drains, the host must call
/// [`FsWriter::shutdown`] to settle publication and maintenance tasks, then
/// close the returned cache so in-memory entries are flushed. [`serve`]
/// performs both steps automatically. The cache is present only when
/// `[local_cache]` is configured.
pub async fn app(
    config: ServerConfig,
) -> Result<(Router, FsWriter, Option<Arc<FoyerStoredMetadataBlockCache>>), ServerConfigError> {
    // The one unavoidable validation point: configs that skipped
    // `load_server_config` (direct Rust construction) fail here exactly as
    // file-loaded ones fail at load.
    config.validate()?;
    let store = config.object_store()?;
    // Provider construction already validated direct-transfer signing and endpoint
    // support. Reuse that decision without reinterpreting configuration here.
    let direct_transfers = store.direct_transfers();
    let store = store.into_shared();
    let (router, state) =
        app_with_store_and_direct_transfers(config, store, direct_transfers).await?;
    Ok((router, state.writer, state.local_cache))
}

#[doc(hidden)]
pub async fn app_with_store(
    config: ServerConfig,
    store: SharedObjectStore,
) -> Result<Router, ServerConfigError> {
    Ok(app_with_store_and_direct_transfers(config, store, None)
        .await?
        .0)
}

/// Test-only: the router plus its state, so tests can hold admission
/// permits or close publisher admission and observe the served answers.
#[cfg(test)]
pub(super) async fn app_with_store_and_state(
    config: ServerConfig,
    store: SharedObjectStore,
) -> Result<(Router, AppState), ServerConfigError> {
    app_with_store_and_direct_transfers(config, store, None).await
}

pub(super) async fn app_with_store_and_direct_transfers(
    config: ServerConfig,
    store: SharedObjectStore,
    direct_transfers: Option<DirectTransferIssuers>,
) -> Result<(Router, AppState), ServerConfigError> {
    let metrics = ServerMetrics::new();
    // Two switches decide automatic grep indexing and nothing else does:
    // whether this server maintains anything automatically, and whether its
    // grep mode maintains the index.
    let maintains_grep_index =
        config.maintenance.registers_automatic_jobs() && config.grep.mode.maintains_index();
    // Opened before the handles, because the handles are what it is
    // installed on: a directory that cannot be owned fails startup here
    // rather than after a runtime is already running on it.
    let local_cache = open_local_cache(&config, &metrics).await?;
    // Grep reads and checkpoints through the same handles the HTTP planes
    // use, so it is composed after them. Nothing has to be wired back into
    // the writer for its publications to reach the index: the job says on
    // the trait that publications concern it, and registering it is what
    // subscribes it.
    let (writer, reader, admin) = build_handles(
        &config,
        store,
        &metrics,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
        local_cache.clone(),
    )
    .await?;
    let probe_store = writer.object_store();
    let grep_block_cache = Arc::new(GrepBlockCache::with_metrics(
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
                admin.clone(),
                Arc::clone(&grep_block_cache),
            )
        });
    let grep_service = config
        .grep
        .mode
        .serves_grep()
        .then(|| Arc::new(GrepService::with_block_cache(Arc::clone(&grep_block_cache))));
    let grep_maintenance = if maintains_grep_index {
        let policy = config
            .grep
            .worker_config()
            .build_policy()
            .map_err(|error| ServerConfigError::InvalidField {
                field: "grep",
                reason: error.to_string(),
            })?;
        let job = Arc::new(GrepMaintenanceJob::new(
            grep_worker
                .as_ref()
                .expect("an index-maintaining deployment composes a grep worker")
                .clone(),
            policy,
        ));
        writer
            .register_maintenance_job(job.clone())
            .map_err(|error| ServerConfigError::InvalidField {
                field: "grep",
                reason: error.to_string(),
            })?;
        // Reclaiming what the index leaves behind is upkeep for the same
        // namespaces, gated by the same switch: a deployment that builds
        // grep objects is the one that should collect them.
        writer
            .register_maintenance_job(Arc::new(GrepGcJob::new(
                grep_worker
                    .as_ref()
                    .expect("an index-maintaining deployment composes a grep worker")
                    .clone(),
            )))
            .map_err(|error| ServerConfigError::InvalidField {
                field: "grep",
                reason: error.to_string(),
            })?;
        Some(GrepMaintenance {
            handle: writer.maintenance(),
            job,
        })
    } else {
        None
    };
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
        admin,
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

#[cfg(test)]
pub(super) async fn build_handles_with_metrics_jsonl_path(
    config: &ServerConfig,
    store: SharedObjectStore,
    metrics_jsonl_path: Option<OsString>,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    build_handles(
        config,
        store,
        &ServerMetrics::new(),
        metrics_jsonl_path,
        None,
    )
    .await
}

/// Builds the process handles over one store and one metrics recorder.
///
/// An optional JSONL recorder receives the same object-store samples. The
/// local block cache is installed once on the writer's shared runtime core,
/// so the reader and admin use the same decoded cache hierarchy.
async fn build_handles(
    config: &ServerConfig,
    store: SharedObjectStore,
    metrics: &ServerMetrics,
    metrics_jsonl_path: Option<OsString>,
    local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    let trace_store_kind = TraceStoreKind::from(config.store.kind());
    let samples = object_store_metrics_recorder(metrics_jsonl_path)?;
    let runtime_error = |error: loonfs::RuntimeError| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    };

    let mut writer_builder = FsWriter::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
        .background_work(config.maintenance.background_work())
        .min_publish_interval_ms(config.min_publish_interval_ms)
        // The reader below shares this core, so the read cap covers every
        // proxied content read the server serves.
        .max_read_content_bytes(config.max_download_bytes)
        .max_concurrent_maintenance(config.max_concurrent_maintenance)
        .runtime_cache(config.runtime_cache_config())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind)
        .metrics_recorder(metrics.recorder());
    if let Some(samples) = &samples {
        writer_builder = writer_builder.object_store_metrics_recorder(Arc::clone(samples));
    }
    if let Some(local_cache) = local_cache {
        writer_builder = writer_builder.stored_metadata_block_cache(local_cache);
    }
    let writer = writer_builder.build().await.map_err(runtime_error)?;
    let reader = writer.reader();

    let mut admin_builder = FsAdmin::builder_with_store(store)
        .actor_id(format!("{}-admin", config.writer_id))
        // The admin honors the configured cache sizing and shares the
        // writer's decoded-block cache instance, so explicit maintenance
        // reuses blocks reader traffic already decoded instead of
        // populating a second, default-sized cache.
        .runtime_cache(config.runtime_cache_config())
        .shared_metadata_table_cache(&writer)
        // A maintenance step this server runs on request may plan a
        // background streaming compaction. Sharing the writer's runner is
        // what lets it start one, makes the operator's step and the writer's
        // own steps agree that a namespace runs one job at a time, and puts
        // the job under the shutdown that settles the writer.
        .background_maintenance(&writer)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind)
        .metrics_recorder(metrics.recorder());
    if let Some(samples) = samples {
        admin_builder = admin_builder.object_store_metrics_recorder(samples);
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
    #[error("failed to load the configured TLS identity: {0}")]
    Tls(#[source] TlsConfigError),
    #[error("server failed while serving requests: {0}")]
    Serve(#[source] std::io::Error),
    #[error("background work did not settle during shutdown: {0}")]
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

/// Runs the startup work that reads more than the config file, then releases
/// what it took.
///
/// `loonfs-server --check-config` calls this once the config has loaded, so
/// the flag also fails on the two things that load correctly and start
/// incorrectly: a TLS identity this process cannot use, and a local cache
/// directory it cannot own. Both go through the functions [`app`] and
/// [`serve_with_shutdown`] call, so what this accepts is what a start
/// accepts.
///
/// Two startup failures stay outside it. It does not bind the configured
/// address, because a check that held the port could not run beside the
/// server it is checking, and it performs no object-store operation, because
/// a reachability check belongs to `loonfs-server --probe-store` or
/// `loonfs admin store-probe`. Constructing the store still creates a
/// `local-fs` root, as a start does.
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

/// Serves until ctrl-c or SIGTERM, then shuts down gracefully: the listener
/// stops accepting, in-flight requests drain, publisher work finishes, and
/// writer maintenance — the runtime's steps and grep's alike — settles
/// before this returns.
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
/// Plaintext and TLS differ in what `accept` returns and in nothing else:
/// the same router, the same graceful shutdown, and the same writer settles
/// after the listener drains or reaches its deadline.
pub(super) async fn serve_on<L>(
    listener: L,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let shutdown_deadline_ms = config.shutdown_deadline_ms;
    let (router, writer, local_cache) = app(config).await?;
    serve_and_settle(
        listener,
        router,
        writer,
        local_cache,
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
pub(super) async fn serve_and_settle<L>(
    listener: L,
    router: Router,
    writer: FsWriter,
    local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
    shutdown_deadline_ms: u64,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let (drain_started_tx, mut drain_started_rx) = tokio::sync::oneshot::channel();
    let shutdown = async move {
        shutdown.await;
        let _ = drain_started_tx.send(());
    };
    let mut server = Box::pin(
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .into_future(),
    );
    // The budget starts when shutdown fires. It does not limit server uptime.
    let served = tokio::select! {
        result = server.as_mut() => result,
        Ok(()) = &mut drain_started_rx => {
            match tokio::time::timeout(
                Duration::from_millis(shutdown_deadline_ms),
                server.as_mut(),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        shutdown_deadline_ms,
                        "graceful drain deadline passed; remaining requests are abandoned"
                    );
                    Ok(())
                }
            }
        }
    };
    // Dropping the server cancels requests left behind by an expired drain.
    drop(server);
    served.map_err(ServeError::Serve)?;
    // Shut down the writer after the drain finishes or its deadline passes.
    // Accepted requests keep mutation admission through the drain window.
    // Task panics surface here.
    let settled = writer.shutdown().await.map_err(ServeError::Shutdown);
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
