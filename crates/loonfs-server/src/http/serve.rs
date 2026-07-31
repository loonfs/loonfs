//! HTTP application construction, listener service, and shutdown lifecycle.

use super::router;
use crate::config::{ServerConfig, ServerConfigError};
use axum::Router;
use loonfs::metrics::{
    InstrumentedObjectStore, JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder,
};
use loonfs::publisher::PublisherRegistry;
use loonfs::{
    FsAdmin, FsReader, FsWriter, MaintenanceHandle, MaintenanceJob, MaintenanceProbe,
    SharedObjectStore, TraceMode, TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_grep::{GrepMaintenanceJob, GrepService, GrepWorker, GREP_INDEX_JOB};
use loonfs_objectstore::presign::ObjectTransferIssuer;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Purpose-specific handles over one shared store client: read endpoints go
/// through `reader`, mutations through `writer` (and the publication service
/// it hands out), maintenance endpoints through `admin`. Shutdown-relevant
/// handles also live in the [`ServerLifecycle`] returned beside the router.
///
/// `reader` is a cheap clone derived from `writer` at construction, kept as
/// its own field because most handlers only read.
#[derive(Clone)]
pub(super) struct AppState {
    pub(super) config: Arc<ServerConfig>,
    pub(super) writer: FsWriter,
    pub(super) reader: FsReader,
    pub(super) admin: FsAdmin,
    pub(super) transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
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

/// Everything the app spawns that must settle at shutdown: publisher
/// publications, and the writer's maintenance runner — which now admits the
/// grep index's steps alongside the runtime's own.
///
/// [`serve`] drives this itself. A host embedding the [`Router`] on its own
/// HTTP server must call [`ServerLifecycle::shutdown`] after its listener
/// drains, or publisher tasks and writer maintenance outlive the listener
/// unobserved.
pub struct ServerLifecycle {
    writer: FsWriter,
    publisher: PublisherRegistry,
    maintains_grep_index: bool,
}

impl ServerLifecycle {
    /// Settles the app's spawned work in one order: maintenance admission
    /// closes, publisher admission closes, admitted publications finish,
    /// then the writer's runner drains its in-flight steps. Panicked tasks
    /// surface as the returned error.
    ///
    /// Maintenance admission closes first because draining publications is
    /// an await, and until it returns the runner is still live. Its timer
    /// promotes keys whose deadlines have arrived, each publication that
    /// lands nudges the jobs subscribed to publications — the grep index
    /// among them — and a finishing step hands its permit straight to the
    /// next queued key.
    /// Everything admitted in that window is work this shutdown already
    /// decided to drop, and none of it is free: a metadata step advances
    /// the metadata root, a collection pass deletes provider objects, an
    /// index step writes segments. Then the drain below has to wait for
    /// whatever it started. Closing first leaves the window empty.
    ///
    /// Neither order can wedge, and the reason is worth stating because it
    /// is not the obvious one. A maintenance step never submits to the
    /// publication service at all: every job does its work through
    /// `FsAdmin`, which compare-and-swaps against the namespace head
    /// itself. So the publication drain waits only on client work, its
    /// pending set can only shrink, and no step of any kind can be left
    /// waiting behind a publisher that is closing. A step already running
    /// when this lands finishes normally, and its chain then ends rather
    /// than passing its permit on, because a shut admission book releases
    /// the permit instead of handing it to the next key.
    pub async fn shutdown(self) -> Result<(), loonfs::RuntimeError> {
        self.writer.close_maintenance_admission();
        self.publisher.close_admission();
        self.publisher.drain().await?;
        // Closes maintenance admission a second time — idempotent — and
        // then drains the steps that were already running.
        self.writer.shutdown_background().await
    }

    /// Waits for every maintenance step this server's writer has admitted —
    /// metadata, collection, and grep indexing alike — to settle, without
    /// closing the runner.
    ///
    /// This is a drain, not a per-namespace wait: the runner admits work per
    /// `{job, namespace}` key and reports progress durably, so what a caller
    /// waits for is quiet, and what it then reads is durable state.
    pub async fn wait_for_maintenance(&self) -> Result<(), loonfs::RuntimeError> {
        self.writer.wait_for_background_work().await
    }

    /// Whether this deployment registered the grep index job.
    pub fn maintains_grep_index(&self) -> bool {
        self.maintains_grep_index
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
    // The one direct-put gate. A presigned URL is a capability handed to a
    // client, and completion trusts the provider to have enforced the signed
    // checksum and create-only preconditions rather than reading the bytes
    // back — so an issuer exists only when the store can presign *and* the
    // endpoint is one the live conformance suite has proven.
    let transfer_issuer = config
        .store
        .direct_put_is_proven()
        .then(|| store.transfer_issuer())
        .flatten();
    let store = Arc::new(store) as SharedObjectStore;
    let (router, lifecycle, _state) =
        app_with_store_and_transfer_issuer(config, store, transfer_issuer).await?;
    Ok((router, lifecycle))
}

#[cfg(test)]
pub(super) async fn app_with_store(
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
pub(super) async fn app_with_store_and_state(
    config: ServerConfig,
    store: SharedObjectStore,
) -> Result<(Router, AppState), ServerConfigError> {
    let (router, _lifecycle, state) =
        app_with_store_and_transfer_issuer(config, store, None).await?;
    Ok((router, state))
}

pub(super) async fn app_with_store_and_transfer_issuer(
    config: ServerConfig,
    store: SharedObjectStore,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
) -> Result<(Router, ServerLifecycle, AppState), ServerConfigError> {
    // Instrumentation is installed once, here, so every LoonFS-owned request
    // the process makes is measured — the handles' own traffic and the
    // grep-owned traffic that used to run on a second, raw client.
    let store = instrumented_store(
        &config,
        store,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
    )?;
    // Two switches decide automatic grep indexing and nothing else does:
    // whether this server maintains anything automatically, and whether its
    // grep mode maintains the index.
    let maintains_grep_index =
        config.maintenance.registers_automatic_jobs() && config.grep.mode.maintains_index();
    // Grep reads and checkpoints through the same handles the HTTP planes
    // use, so it is composed after them. Nothing has to be wired back into
    // the writer for its publications to reach the index: the job says on
    // the trait that publications concern it, and registering it is what
    // subscribes it.
    let (writer, reader, admin) = build_handles(&config, store.clone()).await?;
    // A deployment that maintains the index needs a worker whether or not it
    // answers queries with one.
    let grep_worker = (config.grep.mode.serves_grep() || config.grep.mode.maintains_index())
        .then(|| GrepWorker::new(store.clone(), reader.clone(), admin.clone()));
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
        Some(GrepMaintenance {
            handle: writer.maintenance(),
            job,
        })
    } else {
        None
    };
    let config = Arc::new(config);
    let lifecycle = ServerLifecycle {
        writer: writer.clone(),
        publisher: writer.publisher(),
        maintains_grep_index: grep_maintenance.is_some(),
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
        transfer_issuer,
        grep_worker,
        grep_service,
        grep_maintenance,
    };
    Ok((router(state.clone()), lifecycle, state))
}

/// Wraps the store for object-store metrics when a recorder is configured.
///
/// One wrapper serves the whole process: the handles are built on it, and so
/// is the grep worker, so no LoonFS-owned request escapes measurement.
fn instrumented_store(
    config: &ServerConfig,
    store: SharedObjectStore,
    metrics_jsonl_path: Option<OsString>,
) -> Result<SharedObjectStore, ServerConfigError> {
    let Some(recorder) = object_store_metrics_recorder(metrics_jsonl_path)? else {
        return Ok(store);
    };
    Ok(Arc::new(
        InstrumentedObjectStore::new(store, recorder)
            .store_kind(TraceStoreKind::from(config.store.kind()).as_str()),
    ) as SharedObjectStore)
}

#[cfg(test)]
pub(super) async fn build_handles_with_metrics_jsonl_path(
    config: &ServerConfig,
    store: SharedObjectStore,
    metrics_jsonl_path: Option<OsString>,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    let store = instrumented_store(config, store, metrics_jsonl_path)?;
    build_handles(config, store).await
}

async fn build_handles(
    config: &ServerConfig,
    store: SharedObjectStore,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    let trace_store_kind = TraceStoreKind::from(config.store.kind());
    let runtime_error = |error: loonfs::RuntimeError| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    };

    let writer_builder = FsWriter::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
        .background_work(config.maintenance.background_work())
        .min_publish_interval_ms(config.min_publish_interval_ms)
        // The reader below shares this core, so the read cap covers every
        // proxied content read the server serves.
        .max_read_content_bytes(config.max_download_bytes)
        .max_concurrent_maintenance(config.max_concurrent_maintenance)
        .runtime_cache(config.runtime_cache_config())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind);
    let writer = writer_builder.build().await.map_err(runtime_error)?;
    let reader = writer.reader();

    let admin_builder = FsAdmin::builder_with_store(store)
        .actor_id(format!("{}-admin", config.writer_id))
        // The admin honors the configured cache sizing and shares the
        // writer's decoded-block cache instance, so explicit maintenance
        // reuses blocks reader traffic already decoded instead of
        // populating a second, default-sized cache.
        .runtime_cache(config.runtime_cache_config())
        .shared_metadata_table_cache(&writer)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind);
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
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|source| ServeError::Bind { addr: bind, source })?;
    serve_on(listener, config, shutdown).await
}

pub(super) async fn serve_on(
    listener: tokio::net::TcpListener,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let (router, lifecycle) = app(config).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)?;
    // The listener has drained; close maintenance admission, then publisher
    // admission, finish admitted publications, then settle the maintenance
    // steps already running. Panicked tasks surface here rather than
    // disappearing with the process.
    lifecycle.shutdown().await.map_err(ServeError::Shutdown)
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
