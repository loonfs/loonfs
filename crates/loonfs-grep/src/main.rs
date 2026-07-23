//! The explicitly assigned `loonfs-grep` per-namespace maintenance binary.

use clap::Parser;
use loonfs_api::NamespaceId;
use loonfs_core::control::load_namespace_head_control;
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::{
    GrepDriver, GrepDriverParked, GrepDriverState, GrepDriverTask, GrepStepLimiter, GrepWorker,
    GrepWorkerConfig,
};
use loonfs_objectstore::{SharedObjectStore, StoreConfig};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Notify;
use tracing_subscriber::EnvFilter;

const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Parser)]
#[command(about = "Drive grep maintenance for explicitly assigned LoonFS namespaces")]
struct Args {
    /// TOML file containing `[store]`, optional `[grep]`, and `poll_interval_ms`.
    #[arg(long)]
    config: PathBuf,
    /// Namespace to maintain. Repeat the flag to assign more namespaces.
    #[arg(long, required = true)]
    namespace: Vec<NamespaceId>,
    /// Run every assigned namespace to caught-up and exit.
    #[arg(long)]
    once: bool,
    /// Run explicit grep garbage collection for every assigned namespace.
    #[arg(long)]
    gc: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StandaloneConfig {
    store: StoreConfig,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default)]
    grep: GrepWorkerConfig,
}

#[derive(Debug, Error)]
enum StandaloneError {
    #[error("failed to read config `{path}`: {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode config `{path}`: {source}")]
    DecodeConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid grep config: {0}")]
    GrepConfig(#[from] loonfs_grep::GrepWorkerConfigError),
    #[error("invalid `poll_interval_ms`: must be greater than zero")]
    PollInterval,
    #[error("invalid store config: {0}")]
    StoreConfig(#[from] loonfs_objectstore::StoreConfigError),
    #[error("failed to open configured store: {0}")]
    OpenStore(#[from] loonfs_objectstore::ObjectStoreError),
    #[error("grep maintenance failed: {0}")]
    Maintenance(#[from] loonfs_core::Error),
    #[error("grep operation failed: {0}")]
    Grep(#[from] loonfs_grep::GrepError),
    #[error("grep driver for namespace `{namespace_id}` stopped before parking")]
    DriverStopped { namespace_id: NamespaceId },
    #[error("grep driver task failed: {0}")]
    DriverTask(#[from] tokio::task::JoinError),
    #[error("failed to initialize tracing: {0}")]
    Tracing(String),
}

#[derive(Debug)]
struct PollShutdown {
    requested: AtomicBool,
    notify: Notify,
}

impl PollShutdown {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        if self.requested.load(Ordering::Acquire) {
            return;
        }
        self.notify.notified().await;
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "loonfs-grep failed");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), StandaloneError> {
    init_tracing()?;
    let args = Args::parse();
    let config = load_config(&args.config)?;
    let step_limit = config.grep.concurrent_step_limit()?;
    let build_policy = config.grep.build_policy()?;
    let store = Arc::new(config.store.configured_object_store()?) as SharedObjectStore;
    let worker = GrepWorker::new(
        store.clone(),
        "loonfs-grep",
        loonfs_api::generated_id("wrs"),
        format!("loonfs-grep/{}", env!("CARGO_PKG_VERSION")),
    );
    let namespace_ids: BTreeSet<_> = args.namespace.into_iter().collect();

    if args.gc {
        collect_assigned(&worker, &namespace_ids).await?;
    }

    let runtime = tokio::runtime::Handle::current();
    let step_limiter = GrepStepLimiter::new(step_limit);
    let mut drivers = Vec::with_capacity(namespace_ids.len());
    for namespace_id in &namespace_ids {
        let task = GrepDriver::new(
            worker.clone(),
            namespace_id.clone(),
            build_policy,
            step_limiter.clone(),
        )
        .spawn_on(&runtime);
        drivers.push((namespace_id.clone(), task));
    }

    if args.once {
        wait_for_assigned(&drivers).await?;
        shutdown_drivers(drivers).await?;
        return Ok(());
    }

    let shutdown = Arc::new(PollShutdown::new());
    let mut poll_tasks = Vec::with_capacity(drivers.len());
    for (namespace_id, driver) in &drivers {
        poll_tasks.push(runtime.spawn(poll_namespace(
            store.clone(),
            namespace_id.clone(),
            driver.handle(),
            Duration::from_millis(config.poll_interval_ms),
            shutdown.clone(),
        )));
    }

    shutdown_signal().await;
    shutdown.request();
    for task in poll_tasks {
        task.await?;
    }
    shutdown_drivers(drivers).await
}

async fn wait_for_assigned(
    drivers: &[(NamespaceId, GrepDriverTask)],
) -> Result<(), StandaloneError> {
    for (namespace_id, driver) in drivers {
        let parked = driver.handle().wait_for_quiescence().await.ok_or_else(|| {
            StandaloneError::DriverStopped {
                namespace_id: namespace_id.clone(),
            }
        })?;
        tracing::info!(
            namespace_id = %namespace_id,
            state = ?parked,
            "grep namespace caught up"
        );
    }
    Ok(())
}

async fn collect_assigned(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_ids: &BTreeSet<NamespaceId>,
) -> Result<(), StandaloneError> {
    let now_ms = current_time_ms()?;
    for namespace_id in namespace_ids {
        let report = worker
            .garbage_collect_namespace(namespace_id, now_ms)
            .await?;
        tracing::info!(
            namespace_id = %namespace_id,
            deleted_segments = report.deleted_segments,
            deleted_other_objects = report.deleted_other_objects,
            namespace_reaped = report.namespace_reaped,
            "grep namespace garbage collection completed"
        );
    }
    Ok(())
}

async fn shutdown_drivers(
    drivers: Vec<(NamespaceId, GrepDriverTask)>,
) -> Result<(), StandaloneError> {
    for (_, driver) in &drivers {
        driver.handle().request_stop();
    }
    for (_, driver) in drivers {
        driver.shutdown().await?;
    }
    Ok(())
}

async fn poll_namespace(
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    driver: loonfs_grep::GrepDriverHandle,
    poll_interval: Duration,
    shutdown: Arc<PollShutdown>,
) {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                match load_namespace_head_control(&*store, &namespace_id).await {
                    Ok(head) => {
                        let should_nudge = match driver.state() {
                            GrepDriverState::Parked(GrepDriverParked::CaughtUp {
                                built_through_seq,
                            }) => built_through_seq < head.state.seq,
                            GrepDriverState::Parked(GrepDriverParked::NotEnabled) => {
                                // Assigned namespaces are an explicit, small operator-chosen set,
                                // so one small conditional read per existing poll interval is
                                // cheaper than introducing a second cadence concept.
                                match load_grep_root(&*store, &namespace_id).await {
                                    Ok(Some(root)) => matches!(
                                        root.state().lifecycle(),
                                        GrepLifecycle::Backfilling { .. } | GrepLifecycle::Steady
                                    ),
                                    Ok(None) => false,
                                    Err(error) => {
                                        tracing::warn!(
                                            namespace_id = %namespace_id,
                                            phase = "grep_root_poll",
                                            result = "error",
                                            error = %error,
                                            "grep namespace root poll failed"
                                        );
                                        false
                                    }
                                }
                            }
                            GrepDriverState::Active
                            | GrepDriverState::BackingOff { .. }
                            | GrepDriverState::Stopped => false,
                        };
                        if should_nudge {
                            driver.nudge();
                        }
                    }
                    Err(error) => tracing::warn!(
                        namespace_id = %namespace_id,
                        phase = "grep_namespace_poll",
                        result = "error",
                        error = %error,
                        "grep namespace head poll failed"
                    ),
                }
            }
            () = shutdown.wait() => return,
        }
    }
}

fn load_config(path: &Path) -> Result<StandaloneConfig, StandaloneError> {
    let source = std::fs::read_to_string(path).map_err(|source| StandaloneError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    let config: StandaloneConfig =
        toml::from_str(&source).map_err(|source| StandaloneError::DecodeConfig {
            path: path.to_path_buf(),
            source,
        })?;
    config.grep.validate()?;
    if config.poll_interval_ms == 0 {
        return Err(StandaloneError::PollInterval);
    }
    config.store.validate()?;
    Ok(config)
}

const fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

fn init_tracing() -> Result<(), StandaloneError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("loonfs_grep=info,loonfs_core=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| StandaloneError::Tracing(error.to_string()))
}

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

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> Result<u64, loonfs_core::Error> {
    // Standalone explicit GC enters wall time at the command boundary; durable replay stays deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|error| {
            loonfs_core::Error::Internal(format!("system clock before unix epoch: {error}"))
        })
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
