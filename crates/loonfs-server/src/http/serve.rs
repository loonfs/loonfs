//! HTTP application construction, the listener service, and where its
//! graceful shutdown is triggered from.

use super::host_routes::router;
use super::tls::{self, TlsConfigError, TlsListener};
use crate::config::{JobMode, MemorySizing, ServerConfig, ServerConfigError};
use crate::sweep::{GrepIndexing, Sweep};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum::Router;
use loonfs::metrics::{JsonlObjectStoreMetricsRecorder, ObjectStoreMetricsRecorder};
use loonfs::{LoonFs, Maintenance, SharedObjectStore, TraceMode, Writable, WriterId};
use loonfs_grep::{GrepBlockCache, GrepService, GrepWorker};
use loonfs_http::{AuthPolicy, BindingConfig, BindingState, GrepBinding, HttpMetrics, Namespaces};
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeReport};
use loonfs_types::config::ConfigFieldError;
use std::ffi::OsString;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Notify;

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
    pub runtime: LoonFs<Writable>,
    /// The maintenance sweep of a config that maintains. [`app`] does not
    /// start it.
    pub sweep: Option<Sweep>,
    /// The grep block cache of a config that serves grep, whose stats a
    /// scrape of `/metrics` reads.
    pub grep_block_cache: Option<Arc<GrepBlockCache>>,
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
/// The state holds the maintenance sweep when the config maintains, and
/// this does not start it. [`serve`] starts it. After an embedded listener
/// drains, the host must shut down [`AppState::runtime`], which [`serve`]
/// does automatically.
pub async fn app(
    config: ServerConfig,
    options: AppConfig,
) -> Result<(Router, AppState), ServerConfigError> {
    // The one unavoidable validation point: configs that skipped
    // `load_server_config` (direct Rust construction) fail here exactly as
    // file-loaded ones fail at load.
    let memory = config.validate()?;
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
    // Grep reads and checkpoints through the same runtime the HTTP API
    // groups use, so it is composed after it.
    let RuntimeHandles {
        runtime,
        maintenance,
    } = build_handles(
        &config,
        &memory,
        store,
        &metrics,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
    )
    .await?;
    let probe_store = runtime.object_store();
    // The worker runs on the runtime's own instrumented client, so the
    // grep-owned traffic is measured like every other request instead of
    // escaping on a second, raw client.
    let grep_worker = || {
        GrepWorker::new(
            runtime.object_store(),
            runtime.read_only(),
            runtime.clone(),
            loonfs_grep::GrepPassBudget::new(
                std::num::NonZeroUsize::new(config.grep.worker.max_concurrent_steps)
                    .expect("validated grep step limit should be nonzero"),
            ),
        )
    };
    let grep_block_cache = config.grep.mode.serves().then(|| {
        Arc::new(GrepBlockCache::new(
            loonfs::engine::DecodedBlockCacheConfig::new(memory.grep.bytes),
        ))
    });
    let grep_service = || {
        let block_cache = grep_block_cache
            .as_ref()
            .expect("a config that serves grep should build a grep block cache");
        Arc::new(GrepService::new(
            Arc::clone(block_cache),
            runtime.monotonic_timer(),
        ))
    };
    let grep = match config.grep.mode {
        JobMode::Disabled => GrepBinding::Disabled,
        JobMode::ServeOnly => GrepBinding::ServeOnly {
            service: grep_service(),
        },
        JobMode::MaintainOnly => GrepBinding::MaintainOnly {
            worker: grep_worker(),
        },
        JobMode::ServeAndMaintain => GrepBinding::ServeAndMaintain {
            service: grep_service(),
            worker: grep_worker(),
        },
    };
    let namespaces = Arc::new(Namespaces::new(runtime.clone()));
    let sweep = config
        .maintenance
        .maintains()
        .then(|| {
            let grep = grep
                .worker()
                .map(|worker| GrepIndexing::new(worker.clone(), config.grep.worker_config()))
                .transpose()?;
            Ok::<_, ServerConfigError>(Sweep::new(
                config.sweep_config(),
                runtime.object_store(),
                maintenance.clone(),
                Arc::clone(&namespaces),
                grep,
                metrics.recorder().as_ref(),
            ))
        })
        .transpose()?;
    let options = Arc::new(BindingConfig {
        serves_maintenance: config.maintenance.serves(),
        max_download_bytes: Some(config.max_download_bytes),
        max_upload_bytes: Some(config.max_upload_bytes),
        max_concurrent_uploads: config.max_concurrent_uploads.get(),
        max_concurrent_downloads: config.max_concurrent_downloads.get(),
        content_token_secret: config.content_token_secret.clone(),
        request_deadline_ms: config.request_deadline_ms,
        store_kind: config.store.kind(),
        auth_policy: config
            .auth_token
            .clone()
            .map_or(AuthPolicy::Unauthenticated, AuthPolicy::BearerToken),
    });
    let binding = BindingState {
        request_limit: Some(loonfs_http::RequestLimit::new(
            config.max_in_flight_requests,
        )),
        upload_permits: loonfs_http::transfer_permits(config.max_concurrent_uploads.get()),
        download_permits: loonfs_http::transfer_permits(config.max_concurrent_downloads.get()),
        options,
        runtime: runtime.clone(),
        namespaces,
        maintenance,
        probe_store,
        direct_transfers,
        grep,
        metrics,
        usage_recorder: None,
    };
    let state = AppState {
        binding,
        runtime,
        sweep,
        grep_block_cache,
    };
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
/// table, and runs its publications, folds, and merges
/// under one execution budget built from the fold, compaction, and merge
/// input limits and the `[publication]` totals. The execution budget reports
/// to the recorder, and a scrape of `/metrics` reads the cache's counters
/// from its stats. An optional JSONL recorder receives the same object-store
/// samples.
pub(super) async fn build_handles(
    config: &ServerConfig,
    memory: &MemorySizing,
    store: SharedObjectStore,
    metrics: &HttpMetrics,
    metrics_jsonl_path: Option<OsString>,
) -> Result<RuntimeHandles, ServerConfigError> {
    let store_kind = config.store.kind();
    let samples = object_store_metrics_recorder(metrics_jsonl_path)?;
    let ssd = match &config.metadata_cache.ssd {
        Some(ssd) => Some(ssd.open().await?),
        None => None,
    };
    let invalid_runtime = |error: loonfs::Error| {
        ServerConfigError::Field(ConfigFieldError::Invalid {
            field: "runtime",
            reason: error.to_string(),
        })
    };

    let mut builder = LoonFs::builder_with_store(store.clone())
        .writer_id(config.writer_id.clone())
        .min_publish_interval_ms(config.min_publish_interval_ms)
        .publication_policy(config.publication_policy())
        .inline_content(config.inline_content.effective_policy())
        .idle_fold_after_ms(std::num::NonZeroU64::new(config.idle_fold_after_ms))
        .execution_budget(config.execution_budget(memory, metrics.recorder()))
        // Every read the server serves goes through this runtime, so the
        // read cap covers every proxied content read.
        .max_read_content_bytes(config.max_download_bytes)
        .metadata_cache(memory.metadata_cache(ssd))
        .trace_mode(TraceMode::Remote)
        .store_kind(store_kind)
        .metrics_recorder(metrics.recorder());
    if let Some(interval_ms) = config.manifest_revalidation_interval_ms {
        builder = builder.manifest_revalidation_interval_ms(interval_ms);
    }
    if let Some(interval_ms) = config.max_staleness_ms {
        builder = builder.max_staleness_ms(interval_ms);
    }
    builder = builder.consistency(config.consistency);
    if let Some(samples) = &samples {
        builder = builder.object_store_metrics_recorder(Arc::clone(samples));
    }
    let runtime = builder.build().await.map_err(invalid_runtime)?;
    let maintenance_writer_id = WriterId::parse(format!("{}-maintenance", config.writer_id))
        .map_err(|error| {
            ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "writer_id",
                reason: error.to_string(),
            })
        })?;
    let maintenance = runtime.maintenance(maintenance_writer_id);

    Ok(RuntimeHandles {
        runtime,
        maintenance,
    })
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
        .map_err(|error| {
            ServerConfigError::Field(ConfigFieldError::Invalid {
                field: OBJECT_STORE_METRICS_JSONL_ENV,
                reason: error.to_string(),
            })
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
    #[error("failed to install the shutdown signal handlers: {0}")]
    Signal(#[source] std::io::Error),
    #[error("server failed while serving requests: {0}")]
    Serve(#[source] std::io::Error),
    #[error("runtime or maintenance shutdown did not settle: {0}")]
    Shutdown(#[source] loonfs::Error),
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
/// This checks the config fields and the TLS identity, and creates the SSD
/// tier's directory if it's missing and checks that the server can write a
/// file in it. It does not bind the configured address or contact the
/// object store. Use `loonfs-server --probe-store` or
/// `loonfs maintenance store probe` to test storage access. Constructing a
/// local store still creates its root directory.
pub async fn check_config(config: &ServerConfig) -> Result<(), ServeError> {
    config.validate()?;
    config.object_store()?;
    if let Some(ssd) = &config.metadata_cache.ssd {
        ssd.check_directory()?;
    }
    // Building the identity is the whole check; nothing here serves with it.
    let _identity = tls_server_config(config).map_err(ServeError::Tls)?;
    Ok(())
}

/// Runs the object-store contract probe against the store this config builds.
pub async fn probe_store(config: &ServerConfig) -> Result<StoreProbeReport, ServeError> {
    let store = config.object_store()?.into_shared();
    let run_id = loonfs_types::ProbeRunId::generate();
    Ok(run_store_contract_probe(store.as_ref(), &run_id).await)
}

/// Serves until ctrl-c or SIGTERM, then shuts down gracefully.
///
/// Admission closes while the listener remains available to reads and probes.
/// After active requests drain, the listener closes and accepted work settles.
pub async fn serve(config: ServerConfig) -> Result<(), ServeError> {
    let shutdown = shutdown_signal().map_err(ServeError::Signal)?;
    serve_with_shutdown(config, shutdown).await
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
/// Both start the maintenance sweep when the config maintains, close
/// admission, drain requests, close the listener, stop the sweep, and settle
/// the runtime in the same order.
pub(super) async fn serve_on<L>(
    listener: L,
    config: ServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let shutdown_deadline_ms = config.shutdown_deadline_ms;
    let max_connections = config.max_connections.get();
    let (router, state) = app(config, AppConfig::default()).await?;
    let listener = super::connection_limit::ConnectionLimit::new(
        listener,
        max_connections,
        state.binding.metrics.recorder().as_ref(),
    );
    serve_and_settle(
        listener,
        router,
        state.runtime,
        state.sweep,
        shutdown_deadline_ms,
        shutdown,
    )
    .await
}

/// Starts `sweep`, serves until the shutdown trigger fires, then settles
/// what this process owns, in the order it has to be settled in.
///
/// Kept apart from [`serve_on`] so the settling order is a thing a test can
/// drive with handles it holds.
#[expect(
    clippy::disallowed_methods,
    reason = "monotonic time is used only to limit graceful shutdown"
)]
pub(super) async fn serve_and_settle<L>(
    listener: L,
    router: Router,
    runtime: LoonFs<Writable>,
    sweep: Option<Sweep>,
    shutdown_deadline_ms: u64,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError>
where
    L: axum::serve::Listener<Addr = SocketAddr>,
{
    let sweep = sweep.map(|sweep| sweep.start());
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
    // The budget starts when shutdown fires. It does not limit uptime.
    let deadline_from_now = || {
        Box::pin(tokio::time::sleep_until(
            tokio::time::Instant::now() + Duration::from_millis(shutdown_deadline_ms),
        ))
    };
    let (served, mut deadline) = tokio::select! {
        result = server.as_mut() => (result, deadline_from_now()),
        () = shutdown.as_mut() => {
            // This is synchronous so readiness changes before the drain waits.
            runtime.close_admission();
            if let Some(sweep) = &sweep {
                sweep.cancel();
            }
            let mut deadline = deadline_from_now();
            let drained = drain_with_deadline(
                server.as_mut(),
                &requests,
                listener_close_tx,
                deadline.as_mut(),
                shutdown_deadline_ms,
            )
            .await;
            (drained, deadline)
        }
    };
    // Dropping the server cancels requests left behind by an expired drain.
    drop(server);
    // The sweep waits under the same deadline, and is aborted when it passes.
    let swept = match sweep {
        Some(sweep) => sweep.stop(deadline.as_mut(), shutdown_deadline_ms).await,
        None => Ok(()),
    };
    served.map_err(ServeError::Serve)?;
    let runtime_settled = runtime.shutdown().await;
    // Nothing reads through the cache any more, so the SSD tier can write
    // the blocks it queued and stop.
    runtime.metadata_cache().close().await;
    runtime_settled.and(swept).map_err(ServeError::Shutdown)
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
