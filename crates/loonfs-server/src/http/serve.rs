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
use loonfs_grep::{GrepGcJob, GrepMaintenanceJob, GrepService, GrepWorker, GREP_INDEX_JOB};
use loonfs_objectstore::presign::DirectTransferIssuers;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Purpose-specific handles over one shared store client: read endpoints go
/// through `reader`, mutations through `writer` (and the publication service
/// it hands out), maintenance endpoints through `admin`. `writer` is also
/// what a host settles at shutdown, and what [`app`] returns beside the
/// router for that purpose.
///
/// `reader` is a cheap clone derived from `writer` at construction, kept as
/// its own field because most handlers only read.
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
    /// The recorder every handle in this process reports through, and the
    /// request-level instruments only this server can report. `GET /metrics`
    /// renders its snapshot. Always installed: a metrics surface a
    /// deployment has to remember to switch on is a metrics surface nobody
    /// has during the incident.
    pub(super) metrics: Arc<ServerMetrics>,
    /// The node-local block cache this deployment keeps, when `[local_cache]`
    /// asked for one. The handles reach it through the decoded block cache
    /// they were built with; it is held here as its concrete type for the
    /// two things the seam does not offer — reading foyer's own numbers at
    /// scrape time, and closing the cache at shutdown.
    pub(super) local_cache: Option<Arc<FoyerStoredMetadataBlockCache>>,
}

/// Everything a request path tells the writer's maintenance runner about
/// the grep index, and the one question it asks before telling it anything.
///
/// The runner owns admission, the permit pool, backoff, and shutdown: this
/// is a nudge and a probe, both cheap and neither blocking.
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

    /// Nudges only a namespace whose index is actually behind.
    ///
    /// A read has no business admitting work that does not exist, and the
    /// job already knows how to answer that question in at most two small
    /// reads. An unreadable answer nudges nothing: the step would only
    /// rediscover the same failure.
    pub(super) async fn nudge_if_behind(&self, namespace_id: &NamespaceId) {
        if matches!(
            self.job.probe(namespace_id).await,
            Ok(MaintenanceProbe::Due)
        ) {
            self.nudge(namespace_id);
        }
    }
}

/// Builds the HTTP application: the router that serves requests, the writer
/// whose background work its host must settle, and the local block cache
/// that host must close.
///
/// Everything this app spawns belongs to that writer — publications, and
/// the maintenance runner that admits the runtime's steps alongside the
/// grep index's. [`serve`] settles it itself. A host embedding the
/// [`Router`] on its own HTTP server must call [`FsWriter::shutdown`] after
/// its listener drains, or publisher tasks and writer maintenance outlive
/// the listener unobserved, and must then call
/// [`StoredMetadataBlockCache::close`] on the returned cache, or the blocks
/// its memory tier still holds never reach disk. The writer also answers
/// what a deployment's shape is, so a host that needs to know whether the
/// grep index job is registered here asks
/// [`FsWriter::maintenance_job`](loonfs::FsWriter::maintenance_job).
///
/// The cache is `None` unless the config carried a `[local_cache]` table.
pub async fn app(
    config: ServerConfig,
) -> Result<(Router, FsWriter, Option<Arc<FoyerStoredMetadataBlockCache>>), ServerConfigError> {
    // The one unavoidable validation point: configs that skipped
    // `load_server_config` (direct Rust construction) fail here exactly as
    // file-loaded ones fail at load.
    config.validate()?;
    let store = config.object_store()?;
    // The store settled this at construction: a bundle exists only where the
    // provider can sign the preconditions direct transfers rest on and the
    // endpoint is one the live conformance suite has proven. Nothing here
    // asks configuration about it a second time.
    let direct_transfers = store.direct_transfers();
    let store = store.into_shared();
    let (router, state) =
        app_with_store_and_direct_transfers(config, store, direct_transfers).await?;
    Ok((router, state.writer, state.local_cache))
}

#[cfg(test)]
pub(super) async fn app_with_store(
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
    // A deployment that maintains the index needs a worker whether or not it
    // answers queries with one. It runs on the writer's own instrumented
    // client, so the grep-owned traffic is measured like every other
    // request instead of escaping on a second, raw client.
    let grep_worker = (config.grep.mode.serves_grep() || config.grep.mode.maintains_index())
        .then(|| GrepWorker::new(writer.object_store(), reader.clone(), admin.clone()));
    let grep_service = config
        .grep
        .mode
        .serves_grep()
        .then(|| Arc::new(GrepService::new()));
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

/// Opens the process's handles on one store, with the metrics wiring every
/// deployment gets.
///
/// Both handles report through the same recorder, so their instruments are
/// one set of numbers rather than two. The optional JSONL path adds a second
/// sink for the raw object-store samples; the handle fans one store wrapper
/// out to both rather than stacking two.
///
/// The local block cache is installed on the writer, which is what puts it
/// under the decoded block cache the reader and the admin share. One
/// installation covers all three handles.
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
/// a reachability check belongs to `loonfs admin store-probe`. Constructing
/// the store still creates a `local-fs` root, as a start does.
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
/// after the listener has drained.
pub(super) async fn serve_on<L>(
    listener: L,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let (router, writer, local_cache) = app(config).await?;
    serve_and_settle(listener, router, writer, local_cache, shutdown).await
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
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)?;
    // Only once the listener has drained: the writer's shutdown refuses new
    // mutations, so running it while requests are still arriving would fail
    // work this server accepted. What order the shutdown itself runs in is
    // the writer's business, not this function's. Panicked tasks surface
    // here rather than disappearing with the process.
    let settled = writer.shutdown().await.map_err(ServeError::Shutdown);
    // The cache closes after the writer, and it closes whether or not the
    // writer settled: a read may outlive the writer by design, and a lookup
    // after the close is a miss by contract, so nothing is left waiting on
    // bytes this takes away. Closing is what flushes the memory tier to
    // disk, so a shutdown that skipped it would throw away the blocks this
    // process spent the most on. A writer that failed to settle is the
    // worse news of the two and is what a host is told about.
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
