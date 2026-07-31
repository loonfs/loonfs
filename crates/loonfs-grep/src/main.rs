//! The explicitly assigned `loonfs-grep` per-namespace maintenance binary.
//!
//! This process serves nothing and hosts no runtime writer, so it runs the
//! grep job's bounded steps directly on a per-namespace timer instead of
//! through the maintenance runner a server's writer owns. It performs the
//! identical durable protocol either way — the job is the same executor —
//! and the loop here is only the poll a detached deployment needs.

use clap::Parser;
use loonfs::{FsAdmin, FsReader, MaintenanceJob, MaintenanceProbe, MaintenanceStepConclusion};
use loonfs_api::NamespaceId;
use loonfs_grep::{GrepMaintenanceJob, GrepWorker, GrepWorkerConfig};
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

/// The executor this binary runs, over the shared provider client.
type NamespaceJob = GrepMaintenanceJob<SharedObjectStore>;

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
    #[error("failed to open the LoonFS runtime: {0}")]
    Runtime(#[from] loonfs::RuntimeError),
    #[error("system clock before unix epoch: {0}")]
    Clock(String),
    #[error("grep operation failed: {0}")]
    Grep(#[from] loonfs_grep::GrepError),
    #[error("namespace maintenance task failed: {0}")]
    MaintenanceTask(#[from] tokio::task::JoinError),
    #[error("failed to initialize tracing: {0}")]
    Tracing(String),
}

/// Cooperative stop for the per-namespace loops.
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

    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    /// Waits one poll interval. Answers `false` once a stop is requested,
    /// which is the loops' only exit.
    #[allow(clippy::disallowed_methods)]
    async fn rest(&self, poll_interval: Duration) -> bool {
        // The one timer a detached deployment needs, at this process
        // scheduling boundary, so durable replay stays deterministic.
        if self.requested() {
            return false;
        }
        tokio::select! {
            () = tokio::time::sleep(poll_interval) => !self.requested(),
            () = self.notify.notified() => false,
        }
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
    let build_policy = config.grep.build_policy()?;
    let store = Arc::new(config.store.configured_object_store()?) as SharedObjectStore;
    // One process, one provider client: the runtime handles grep reads and
    // checkpoints through share the client grep writes its own keyspace with.
    let reader = FsReader::builder_with_store(store.clone()).build().await?;
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("loonfs-grep")
        .build()
        .await?;
    let worker = GrepWorker::new(store, reader, admin);
    let namespace_ids: BTreeSet<_> = args.namespace.into_iter().collect();

    if args.gc {
        collect_assigned(&worker, &namespace_ids).await?;
    }

    let job = GrepMaintenanceJob::new(worker, build_policy);
    if args.once {
        return catch_up_assigned(&job, &namespace_ids).await;
    }

    let runtime = tokio::runtime::Handle::current();
    let shutdown = Arc::new(PollShutdown::new());
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let mut tasks = Vec::with_capacity(namespace_ids.len());
    for namespace_id in namespace_ids {
        tasks.push(runtime.spawn(maintain_namespace(
            job.clone(),
            namespace_id,
            poll_interval,
            shutdown.clone(),
        )));
    }

    shutdown_signal().await;
    shutdown.request();
    for task in tasks {
        task.await?;
    }
    Ok(())
}

/// Runs every assigned namespace to caught-up once, surfacing the first
/// failure instead of retrying it.
async fn catch_up_assigned(
    job: &NamespaceJob,
    namespace_ids: &BTreeSet<NamespaceId>,
) -> Result<(), StandaloneError> {
    // A one-shot run answers to no stop signal, so every namespace here
    // reaches a conclusion.
    let never_stops = PollShutdown::new();
    for namespace_id in namespace_ids {
        if let Some(conclusion) = catch_up(job, namespace_id, &never_stops).await? {
            tracing::info!(
                namespace_id = %namespace_id,
                conclusion = ?conclusion,
                "grep namespace caught up"
            );
        }
    }
    Ok(())
}

/// Keeps one assigned namespace's index caught up until the process stops.
///
/// A namespace with nothing to do costs one probe per interval: the grep
/// root, and one page of one change when the root is steady. Work only
/// starts once that probe says there is some.
async fn maintain_namespace(
    job: NamespaceJob,
    namespace_id: NamespaceId,
    poll_interval: Duration,
    shutdown: Arc<PollShutdown>,
) {
    loop {
        match catch_up(&job, &namespace_id, &shutdown).await {
            Ok(None) => return,
            Ok(Some(conclusion)) => tracing::debug!(
                namespace_id = %namespace_id,
                conclusion = ?conclusion,
                "grep namespace settled"
            ),
            // The next interval retries: the step re-reads durable state, so
            // nothing carries over from the attempt that failed.
            Err(error) => tracing::warn!(
                namespace_id = %namespace_id,
                phase = "grep_step",
                result = "error",
                error = %error,
                "grep namespace step failed"
            ),
        }
        loop {
            if !shutdown.rest(poll_interval).await {
                return;
            }
            match job.probe(&namespace_id).await {
                Ok(MaintenanceProbe::Due) => break,
                Ok(MaintenanceProbe::Idle) => {}
                Err(error) => tracing::warn!(
                    namespace_id = %namespace_id,
                    phase = "grep_probe",
                    result = "error",
                    error = %error,
                    "grep namespace probe failed"
                ),
            }
        }
    }
}

/// Runs bounded steps until one reports the index has nothing left to do.
///
/// Answers `None` when a stop landed between steps: every step is bounded
/// and every conclusion is durable, so stopping between two of them costs
/// nothing but the work not yet started.
async fn catch_up(
    job: &NamespaceJob,
    namespace_id: &NamespaceId,
    shutdown: &PollShutdown,
) -> Result<Option<MaintenanceStepConclusion>, StandaloneError> {
    while !shutdown.requested() {
        match job.step(namespace_id, None).await?.conclusion {
            // Both mean the same thing here: durable state moved, by this
            // step or by whoever won the race it lost.
            MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded => {}
            settled => return Ok(Some(settled)),
        }
    }
    Ok(None)
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
        .unwrap_or_else(|_| EnvFilter::new("loonfs_grep=info,loonfs=info"));
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

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> Result<u64, StandaloneError> {
    // Standalone explicit GC enters wall time at the command boundary; durable replay stays deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|error| StandaloneError::Clock(error.to_string()))
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;

#[cfg(test)]
mod test_seeding;
