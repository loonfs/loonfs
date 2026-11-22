//! HTTP application construction, the listener service, and where its
//! graceful shutdown is triggered from.

use super::host_routes::router;
use super::tls::{self, TlsListener};
use crate::config::{MemorySizing, ServerConfig, TransportConfig, ValidatedServerConfig};
use crate::scheduler::{GrepIndexing, Scheduler, SchedulerState};
use crate::{Result, ServerError};
use axum::serve::ListenerExt as _;
use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::{
    ConfiguredObjectStoreKind, LoonFs, Maintenance, SharedObjectStore, Writable, WriterId,
};
use loonfs_grep::{GrepBlockCache, GrepService, GrepWorker};
use loonfs_http::{HttpConfig, HttpHandles, HttpMetrics, HttpState, Namespaces, RequestLimit};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeReport};
use loonfs_types::config::ConfigFieldError;
use std::ffi::OsString;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinSet};
use tower::ServiceExt as _;

const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";

/// Handles and resources whose shutdown belongs to this host.
#[derive(Clone)]
pub struct AppState {
    pub http: HttpState,
    /// The maintenance scheduler, which runs only the close step when the
    /// config doesn't maintain. [`app`] does not start it.
    pub scheduler: Scheduler,
}

impl AppState {
    /// Runs the due steps of each held session once (see
    /// [`Scheduler::run_due_steps`]).
    pub async fn run_due_steps(&self) {
        self.scheduler.run_due_steps().await;
    }
}

/// Optional inputs for building the HTTP application.
#[derive(Default)]
pub struct AppConfig {
    /// Existing object store to use instead of building one from the config.
    pub store: Option<SharedObjectStore>,
    /// Direct-transfer issuers to use instead of the store's issuers.
    pub direct_transfers: Option<DirectTransferIssuers>,
}

/// Builds the router and returns its state.
///
/// The state contains the maintenance scheduler, which this does not start.
/// [`serve_with_shutdown`] starts it; embedded hosts can drive one pass at a
/// time with [`AppState::run_due_steps`]. After
/// an embedded listener drains, the host must shut down the HTTP API's runtime,
/// which [`serve_with_shutdown`] does automatically.
pub async fn app(config: ValidatedServerConfig, options: AppConfig) -> Result<(Router, AppState)> {
    let ValidatedServerConfig {
        config,
        auth_policy,
        memory,
        grep_policy,
        maintenance_writer_id,
        ..
    } = config;
    memory.log();
    let AppConfig {
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
    // Grep reads and creates snapshots through the same runtime the HTTP API
    // groups use, so it is composed after it.
    let RuntimeHandles {
        runtime,
        maintenance,
    } = build_handles(
        &config,
        &memory,
        maintenance_writer_id,
        store,
        &metrics,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
    )
    .await?;
    // The worker runs on the runtime's own instrumented client, so the
    // grep-owned traffic is measured like every other request instead of
    // escaping on a second, raw client.
    let grep_worker = || {
        GrepWorker::new(
            runtime.object_store(),
            runtime.read_only(),
            maintenance.clone(),
            loonfs_grep::GrepStepBudget::new(
                std::num::NonZeroUsize::new(config.grep.worker.max_concurrent_steps)
                    .expect("validated grep step limit should be nonzero"),
            ),
        )
    };
    let grep_service = || {
        Arc::new(GrepService::new(Arc::new(GrepBlockCache::new(
            loonfs::DecodedBlockCacheConfig::new(memory.grep.bytes),
        ))))
    };
    let grep_service = config.grep.mode.serves().then(grep_service);
    let grep_worker = config.grep.mode.maintains().then(grep_worker);
    let namespaces = Arc::new(Namespaces::<SchedulerState>::new(runtime.clone()));
    let scheduler = Scheduler::new(
        config.scheduler_config(),
        runtime.object_store(),
        config.maintenance.maintains().then(|| maintenance.clone()),
        Arc::clone(&namespaces),
        runtime.monotonic_timer(),
        grep_worker
            .as_ref()
            .map(|worker| GrepIndexing::new(worker.clone(), grep_policy)),
        metrics.recorder().as_ref(),
    );
    let http = HttpState::new(
        HttpConfig {
            serves_maintenance: config.maintenance.serves(),
            max_download_bytes: Some(config.runtime_config().max_read_content_bytes),
            max_upload_bytes: Some(config.max_upload_bytes.get()),
            max_concurrent_uploads: config.max_concurrent_uploads.get(),
            max_concurrent_downloads: config.max_concurrent_downloads.get(),
            max_in_flight_requests: Some(config.max_in_flight_requests),
            content_token_secret: config.content_token_secret.clone(),
            request_deadline_ms: config.request_deadline_ms.get(),
            auth_policy,
        },
        HttpHandles {
            runtime,
            namespaces,
            maintenance,
            direct_transfers,
            grep_service,
            grep_worker,
            metrics,
            usage_recorder: None,
        },
    );
    let state = AppState { http, scheduler };
    Ok((router(state.clone()), state))
}

/// The process runtime and the maintenance handle of its host.
pub(super) struct RuntimeHandles {
    pub(super) runtime: LoonFs<Writable>,
    pub(super) maintenance: Maintenance,
}

/// Builds the process runtime and its maintenance over one store and one
/// metrics recorder.
///
/// The runtime reads through one metadata cache built from the
/// `[metadata_cache]` table, with an SSD tier if the table has an `ssd`
/// table, and runs its publications, folds, and compactions
/// under one execution budget built from the fold and compaction limits, the
/// compaction input limit, and the `[publication]` totals. The execution budget reports
/// to the recorder, and a scrape of `/metrics` reads the cache's counters
/// from its stats. An optional JSONL recorder receives the same object-store
/// samples.
pub(super) async fn build_handles(
    config: &ServerConfig,
    memory: &MemorySizing,
    maintenance_writer_id: WriterId,
    store: SharedObjectStore,
    metrics: &HttpMetrics,
    metrics_jsonl_path: Option<OsString>,
) -> Result<RuntimeHandles> {
    let store_kind = config.store.kind();
    let samples = object_store_metrics_recorder(metrics_jsonl_path)?;
    let ssd = match &config.metadata_cache.ssd {
        Some(ssd) => Some(ssd.open().await?),
        None => None,
    };
    let invalid_runtime = |error: loonfs::Error| {
        ServerError::Config(ConfigFieldError::Invalid {
            field: "runtime",
            reason: error.to_string(),
        })
    };

    let mut builder = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(config.writer_id.clone())
        .config(config.runtime_config())
        .execution_budget(config.execution_budget(memory, metrics.recorder()))
        .metadata_cache(memory.metadata_cache(ssd))
        .store_kind(store_kind)
        .metrics_recorder(metrics.recorder());
    if let Some(samples) = &samples {
        builder = builder.object_store_metrics_recorder(Arc::clone(samples));
    }
    let runtime = builder.build().await.map_err(invalid_runtime)?;
    let maintenance = runtime.maintenance(maintenance_writer_id);

    Ok(RuntimeHandles {
        runtime,
        maintenance,
    })
}

fn object_store_metrics_recorder(
    metrics_jsonl_path: Option<OsString>,
) -> Result<Option<Arc<dyn ObjectStoreMetricsRecorder>>> {
    let Some(path) = metrics_jsonl_path else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }
    let path = std::path::PathBuf::from(path);
    JsonlObjectStoreMetricsRecorder::create(&path)
        .map(|recorder| Some(Arc::new(recorder) as Arc<dyn ObjectStoreMetricsRecorder>))
        .map_err(|error| {
            ServerError::Config(ConfigFieldError::Invalid {
                field: OBJECT_STORE_METRICS_JSONL_ENV,
                reason: error.to_string(),
            })
        })
}

/// Builds the rustls configuration this deployment terminates with, or
/// returns `None` where TLS terminates in front of the process.
///
/// The one place the identity is loaded, so [`check_config`] reads the files
/// a start reads and reports what a start reports.
fn tls_server_config(config: &ServerConfig) -> Result<Option<rustls::ServerConfig>> {
    match &config.transport {
        Some(TransportConfig::Tls(identity)) => tls::server_config(identity).map(Some),
        Some(TransportConfig::PlaintextBehindProxy) | None => Ok(None),
    }
}

/// Checks the startup resources of a validated config without starting the
/// server.
///
/// This builds the object store client and the TLS identity, and creates
/// the SSD tier's directory if it's missing and checks that the server can
/// write a file in it. It does not bind the configured address or contact
/// the object store. Use `loonfs-server --probe-store` or
/// `loonfs maintenance store probe` to test storage access. Constructing a
/// local store still creates its root directory.
pub async fn check_config(config: &ValidatedServerConfig) -> Result<()> {
    let config = &config.config;
    config.object_store()?;
    if let Some(ssd) = &config.metadata_cache.ssd {
        ssd.check_directory()?;
    }
    // Building the identity is the whole check; nothing here serves with it.
    let _identity = tls_server_config(config)?;
    Ok(())
}

/// Runs the object-store contract probe against the store this config builds.
pub async fn probe_store(config: &ValidatedServerConfig) -> Result<StoreProbeReport> {
    let store = config.config.object_store()?.into_shared();
    let probe_id = loonfs_types::ProbeId::generate();
    Ok(run_store_contract_probe(store.as_ref(), &probe_id).await)
}

/// Serves on the configured address until ctrl-c or SIGTERM, then shuts down
/// gracefully.
///
/// The app is built before the port is bound, so a connection that arrives
/// while the store client and the SSD cache are being built is refused and
/// retried instead of waiting in the backlog, and the listening line means
/// the server answers. Admission closes while the listener remains available
/// to probes, and a request that arrives during the drain is answered
/// `shutting_down`. After admitted requests drain, the listener closes and
/// accepted work settles.
pub async fn serve(config: ValidatedServerConfig) -> Result<()> {
    let shutdown = shutdown_signal().map_err(ServerError::Signal)?;
    let bind = config.bind;
    let server = BuiltServer::build(config, AppConfig::default()).await?;
    let listener = TcpListener::bind(bind)
        .await
        .map_err(|source| ServerError::Bind { addr: bind, source })?;
    server.serve(listener, shutdown).await
}

/// [`serve`] on a listener the caller bound, with the store and transfer
/// overrides of `options` and a caller-supplied shutdown trigger instead of
/// process signals, for hosts that manage their own lifecycle.
///
/// The server serves on the listener's address, whatever the config's `bind`
/// address says.
pub async fn serve_with_shutdown(
    config: ValidatedServerConfig,
    options: AppConfig,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    BuiltServer::build(config, options)
        .await?
        .serve(listener, shutdown)
        .await
}

/// A server whose app is built and which has not yet bound or accepted a
/// connection.
struct BuiltServer {
    /// The configured bind address, which a failed read of the listener's
    /// address reports.
    bind: SocketAddr,
    store_kind: ConfiguredObjectStoreKind,
    tls: Option<rustls::ServerConfig>,
    router: Router,
    state: AppState,
    max_connections: usize,
    connection_idle_timeout_ms: u64,
    shutdown_deadline_ms: u64,
}

impl BuiltServer {
    /// Builds the TLS identity and the app that a server for `config` serves.
    async fn build(config: ValidatedServerConfig, options: AppConfig) -> Result<Self> {
        let bind = config.bind;
        let store_kind = config.config.store.kind();
        // The first line a deployment sees. An idle server logs nothing until
        // a request arrives, so without this line a container stuck building
        // its store client or scanning its SSD cache and a container that
        // never started look the same. This one says the config loaded.
        tracing::info!(
            bind = %bind,
            store = store_kind.as_str(),
            "loonfs-server is starting"
        );
        let tls = tls_server_config(&config.config)?;
        let max_connections = config.config.max_connections.get();
        let connection_idle_timeout_ms = config.config.connection_idle_timeout_ms.get();
        let shutdown_deadline_ms = config.config.shutdown_deadline_ms.get();
        let (router, state) = app(config, options).await?;
        Ok(Self {
            bind,
            store_kind,
            tls,
            router,
            state,
            max_connections,
            connection_idle_timeout_ms,
            shutdown_deadline_ms,
        })
    }

    /// Serves on `listener` until `shutdown` resolves, then settles what the
    /// server owns.
    async fn serve(
        mut self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let bind = listener.local_addr().map_err(|source| ServerError::Bind {
            addr: self.bind,
            source,
        })?;
        tracing::info!(
            bind = %bind,
            store = self.store_kind.as_str(),
            "loonfs-server is listening"
        );
        match self.tls.take() {
            Some(tls) => {
                self.serve_on(TlsListener::new(listener, tls), shutdown)
                    .await
            }
            None => self.serve_on(listener.tap_io(set_nodelay), shutdown).await,
        }
    }

    /// The one serving body, over whichever listener the deployment
    /// configured.
    ///
    /// Plaintext and TLS differ in what `accept` returns and in nothing else.
    /// Both start the maintenance scheduler, close admission, drain requests,
    /// close the listener, stop the scheduler, and settle the runtime in the
    /// same order.
    async fn serve_on<L>(
        self,
        listener: L,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()>
    where
        L: axum::serve::Listener<Addr = SocketAddr>,
    {
        let Self {
            router,
            state,
            max_connections,
            connection_idle_timeout_ms,
            shutdown_deadline_ms,
            ..
        } = self;
        let listener = super::connection_limit::ConnectionLimit::new(
            listener,
            max_connections,
            state.http.metrics.recorder().as_ref(),
        );
        let settlement = Settlement {
            requests: state.http.request_limit().cloned(),
            runtime: state.http.runtime,
            scheduler: state.scheduler,
            deadline_ms: shutdown_deadline_ms,
        };
        serve_and_settle(
            listener,
            router,
            connection_idle_timeout_ms,
            settlement,
            shutdown,
        )
        .await
    }
}

/// Turns off Nagle's algorithm on an accepted connection.
///
/// hyper writes a streamed response's head before the body's first chunk is
/// ready, so a read whose content comes from the store leaves in more than
/// one segment. With Nagle's algorithm on, the kernel holds each later small
/// segment until the client acknowledges the one before, and a client that
/// has nothing to send delays that acknowledgement, by 40 ms on Linux. A
/// connection that refuses the option still serves, only slower, so the
/// failure is logged and the connection kept.
pub(super) fn set_nodelay(stream: &mut TcpStream) {
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(error = %error, "failed to set `TCP_NODELAY` on an accepted connection");
    }
}

/// What a server settles after it stops serving, and how long the drain and
/// the scheduler's stop may take, counted from the shutdown signal.
pub(super) struct Settlement {
    /// The admitted requests the drain waits for. An HTTP API without a request
    /// limit drains nothing before the listener closes.
    pub(super) requests: Option<RequestLimit>,
    pub(super) runtime: LoonFs<Writable>,
    pub(super) scheduler: Scheduler,
    pub(super) deadline_ms: u64,
}

/// Starts `scheduler`, serves until the shutdown trigger fires, then settles
/// what this process owns, in the order it has to be settled in.
///
/// Kept apart from [`BuiltServer::serve_on`] so the settling order is a thing
/// a test can drive with handles it keeps.
#[expect(
    clippy::disallowed_methods,
    reason = "monotonic time is used only to limit graceful shutdown"
)]
pub(super) async fn serve_and_settle<L>(
    listener: L,
    router: Router,
    connection_idle_timeout_ms: u64,
    settlement: Settlement,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let Settlement {
        requests,
        runtime,
        scheduler,
        deadline_ms,
    } = settlement;
    let scheduler = scheduler.start();
    let (listener_close_tx, listener_close_rx) = tokio::sync::oneshot::channel();
    let mut server = Box::pin(serve_connections(
        listener,
        router,
        connection_idle_timeout_ms,
        async move {
            let _ = listener_close_rx.await;
        },
    ));
    let mut shutdown = Box::pin(shutdown);
    // The budget starts when shutdown fires. It does not limit uptime.
    let deadline_from_now = || {
        Box::pin(tokio::time::sleep_until(
            tokio::time::Instant::now() + Duration::from_millis(deadline_ms),
        ))
    };
    let mut deadline = tokio::select! {
        () = server.as_mut() => deadline_from_now(),
        () = shutdown.as_mut() => {
            // This is synchronous so readiness changes before the drain waits.
            // The scheduler stops first, so none of its runs finds the sessions
            // closed by the admission close and tries to close them itself.
            scheduler.cancel();
            runtime.close_admission();
            let mut deadline = deadline_from_now();
            drain_with_deadline(
                server.as_mut(),
                requests.as_ref(),
                listener_close_tx,
                deadline.as_mut(),
                deadline_ms,
            )
            .await;
            deadline
        }
    };
    // Dropping the server aborts the connection tasks that a passed deadline
    // left behind, and with them their requests.
    drop(server);
    // The scheduler waits under the same deadline, and is aborted when the
    // deadline ends.
    let stopped = scheduler.stop(deadline.as_mut(), deadline_ms).await;
    let runtime_settled = runtime.shutdown().await;
    // Nothing reads through the cache any more, so the SSD tier can write
    // the blocks it queued and stop.
    runtime.metadata_cache().close().await;
    runtime_settled.and(stopped).map_err(ServerError::Shutdown)
}

async fn drain_with_deadline<S>(
    mut server: Pin<&mut S>,
    requests: Option<&RequestLimit>,
    listener_close_tx: tokio::sync::oneshot::Sender<()>,
    mut deadline: Pin<&mut tokio::time::Sleep>,
    shutdown_deadline_ms: u64,
) where
    S: Future<Output = ()> + ?Sized,
{
    let deadline_passed = tokio::select! {
        () = server.as_mut() => return,
        () = async {
            if let Some(requests) = requests {
                requests.settle().await;
            }
        } => false,
        () = deadline.as_mut() => true,
    };
    let _ = listener_close_tx.send(());
    if !deadline_passed {
        tokio::select! {
            () = server.as_mut() => return,
            () = deadline.as_mut() => {}
        }
    }
    tracing::warn!(
        shutdown_deadline_ms,
        "graceful drain deadline passed; remaining requests are abandoned"
    );
}

/// Serves each connection that `listener` accepts in its own task until
/// `close_listener` resolves, then closes the listener and waits for every
/// accepted connection to close.
///
/// When the listener closes, a connection without a request in flight
/// closes at once, and a connection with one closes after the response.
/// Dropping the returned future aborts every connection task.
async fn serve_connections<L>(
    mut listener: L,
    router: Router,
    connection_idle_timeout_ms: u64,
    close_listener: impl Future<Output = ()>,
) where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let idle_timeout = Duration::from_millis(connection_idle_timeout_ms);
    // The header read timeout covers the wait for each request head on an
    // HTTP/1 connection, including the wait between keep-alive requests.
    // HTTP/2 has no such timeout. Its pings close a connection whose peer
    // stopped answering, but not an idle peer that still answers them.
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(idle_timeout);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(idle_timeout)
        .keep_alive_timeout(idle_timeout);
    let graceful = GracefulShutdown::new();
    let mut tasks = JoinSet::new();
    let mut close_listener = pin!(close_listener);
    loop {
        let (io, _address) = tokio::select! {
            accepted = listener.accept() => accepted,
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                log_connection_panic(joined);
                continue;
            }
            () = close_listener.as_mut() => break,
        };
        let first_request = Arc::new(Notify::new());
        let service = TowerToHyperService::new(router.clone().map_request({
            let first_request = Arc::clone(&first_request);
            move |request| {
                first_request.notify_one();
                request
            }
        }));
        let builder = builder.clone();
        let watcher = graceful.watcher();
        tasks.spawn(async move {
            let connection =
                watcher.watch(builder.serve_connection_with_upgrades(TokioIo::new(io), service));
            serve_connection(
                connection,
                first_request.notified(),
                connection_idle_timeout_ms,
            )
            .await;
        });
    }
    drop(listener);
    graceful.shutdown().await;
    while let Some(joined) = tasks.join_next().await {
        log_connection_panic(joined);
    }
}

fn log_connection_panic(joined: std::result::Result<(), JoinError>) {
    if let Err(error) = joined {
        tracing::error!(%error, "connection task panicked");
    }
}

/// Drives one connection to its end, and closes it if no request arrives
/// within `connection_idle_timeout_ms`.
///
/// The header read timeout starts only once hyper knows the connection
/// speaks HTTP/1, so it doesn't cover a TLS handshake, the bytes hyper
/// reads to tell HTTP/1 from HTTP/2, or an HTTP/2 connection's first
/// request. This bound covers all three.
#[expect(
    clippy::disallowed_methods,
    reason = "monotonic time is used only to limit how long a connection waits for its first request, and no protocol time depends on it"
)]
async fn serve_connection<E: std::fmt::Display>(
    connection: impl Future<Output = std::result::Result<(), E>>,
    first_request: impl Future<Output = ()>,
    connection_idle_timeout_ms: u64,
) {
    let mut connection = pin!(connection);
    let first_request = tokio::time::timeout(
        Duration::from_millis(connection_idle_timeout_ms),
        first_request,
    );
    let served = tokio::select! {
        served = connection.as_mut() => served,
        arrived = first_request => match arrived {
            Ok(()) => connection.await,
            Err(_elapsed) => {
                tracing::debug!(
                    connection_idle_timeout_ms,
                    "closed a connection that sent no request within `connection_idle_timeout_ms`"
                );
                return;
            }
        },
    };
    if let Err(error) = served {
        tracing::debug!(error = %error, "connection closed with an error");
    }
}

/// Installs the handlers for ctrl-c and, on unix, SIGTERM (the stop signal
/// container orchestrators send before a kill), and returns a future that
/// resolves when either signal arrives.
fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        })
    }
    #[cfg(not(unix))]
    {
        let mut ctrl_c = tokio::signal::windows::ctrl_c()?;
        Ok(async move {
            ctrl_c.recv().await;
        })
    }
}
