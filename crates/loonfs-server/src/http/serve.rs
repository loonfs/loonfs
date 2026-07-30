//! HTTP application construction, listener service, and shutdown lifecycle.

use super::router;
use crate::config::{ServerConfig, ServerConfigError};
use crate::grep_drivers::GrepDrivers;
use axum::Router;
use loonfs::metrics::{
    InstrumentedObjectStore, JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder,
};
use loonfs::publisher::PublisherRegistry;
use loonfs::{
    FsAdmin, FsBackgroundWork, FsReader, FsWriter, SharedObjectStore, TraceMode, TraceStoreKind,
};
use loonfs_api::NamespaceId;
use loonfs_grep::{GrepDriverParked, GrepService, GrepWorker};
use loonfs_objectstore::presign::ObjectTransferIssuer;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
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
    pub(super) grep_drivers: Option<GrepDrivers>,
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

/// Everything the app spawns that must settle at shutdown: optional
/// per-namespace grep drivers, publisher publications, and writer maintenance.
///
/// [`serve`] drives this itself. A host embedding the [`Router`] on its own
/// HTTP server must call [`ServerLifecycle::shutdown`] after its listener
/// drains, or publisher tasks and writer maintenance outlive the listener
/// unobserved.
pub struct ServerLifecycle {
    writer: FsWriter,
    publisher: PublisherRegistry,
    grep_drivers: Option<GrepDrivers>,
}

impl ServerLifecycle {
    /// Settles the app's spawned work in dependency order: publisher
    /// admission closes, admitted publications finish and send their last
    /// nudges, grep drivers stop between bounded steps, then writer-scheduled
    /// maintenance settles.
    /// Panicked tasks surface as the returned error.
    pub async fn shutdown(self) -> Result<(), loonfs::RuntimeError> {
        self.publisher.close_admission();
        self.publisher.drain().await?;
        if let Some(drivers) = self.grep_drivers {
            drivers.shutdown().await?;
        }
        self.writer.shutdown_background().await
    }

    /// Waits without polling for an embedded namespace driver to catch up or
    /// discover that grep is not enabled. Returns `None` when no driver runs.
    pub async fn wait_for_grep_quiescence(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<GrepDriverParked> {
        match &self.grep_drivers {
            Some(drivers) => drivers.wait_for_quiescence(namespace_id).await,
            None => None,
        }
    }

    /// Whether embedded mode currently owns a driver for `namespace_id`.
    pub fn grep_driver_running(&self, namespace_id: &NamespaceId) -> bool {
        self.grep_drivers
            .as_ref()
            .is_some_and(|drivers| drivers.is_running(namespace_id))
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
    // Grep reads and checkpoints through the same handles the HTTP planes
    // use, so it has to be composed after them. The drivers close the loop:
    // the writer nudges them on every publish, and they own a worker built
    // from that writer's own handles. The observer therefore resolves its
    // target through a slot filled before the router is served — nothing
    // publishes until then.
    let driver_slot: Arc<OnceLock<GrepDrivers>> = Arc::new(OnceLock::new());
    let (writer, reader, admin) = build_handles(
        &config,
        store.clone(),
        config.grep.mode.runs_worker().then(|| driver_slot.clone()),
    )
    .await?;
    let grep_worker = config
        .grep
        .mode
        .serves_grep()
        .then(|| GrepWorker::new(store.clone(), reader.clone(), admin.clone()));
    let grep_service = config
        .grep
        .mode
        .serves_grep()
        .then(|| Arc::new(GrepService::new()));
    let grep_drivers = if config.grep.mode.runs_worker() {
        let worker_config = config.grep.worker_config();
        let grep_config_error =
            |error: loonfs_grep::GrepWorkerConfigError| ServerConfigError::InvalidField {
                field: "grep",
                reason: error.to_string(),
            };
        let drivers = GrepDrivers::new(
            grep_worker
                .as_ref()
                .expect("driver-running grep mode should serve grep")
                .clone(),
            worker_config.build_policy().map_err(grep_config_error)?,
            worker_config
                .concurrent_step_limit()
                .map_err(grep_config_error)?,
        )?;
        let _ = driver_slot.set(drivers.clone());
        Some(drivers)
    } else {
        None
    };
    let config = Arc::new(config);
    let lifecycle = ServerLifecycle {
        writer: writer.clone(),
        publisher: writer.publisher(),
        grep_drivers: grep_drivers.clone(),
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
        grep_drivers,
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
    build_handles(config, store, None).await
}

async fn build_handles(
    config: &ServerConfig,
    store: SharedObjectStore,
    grep_drivers: Option<Arc<OnceLock<GrepDrivers>>>,
) -> Result<(FsWriter, FsReader, FsAdmin), ServerConfigError> {
    let trace_store_kind = TraceStoreKind::from(config.store.kind());
    let runtime_error = |error: loonfs::RuntimeError| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    };

    let mut writer_builder = FsWriter::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
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
    if let Some(drivers) = grep_drivers {
        writer_builder = writer_builder.publish_observer(move |namespace_id, _committed_seq| {
            if let Some(drivers) = drivers.get() {
                drivers.nudge_existing(namespace_id);
            }
        });
    }
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
/// stops accepting, in-flight requests drain, embedded grep drivers stop,
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
    // The listener has drained; close publisher admission, finish admitted
    // publications and their final grep nudges, stop grep drivers between
    // bounded steps, then settle writer-owned maintenance. Panicked tasks
    // surface here rather than disappearing with the process.
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
