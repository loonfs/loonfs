//! The `loonfs-grep` standalone worker binary.

use clap::Parser;
use loonfs_grep::{GrepWorker, GrepWorkerConfig, GrepWorkerLoop};
use loonfs_objectstore::{SharedObjectStore, StoreConfig};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use thiserror::Error;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Drive LoonFS grep indexing and garbage collection")]
struct Args {
    /// TOML file containing `[store]` and optional `[grep]` tables.
    #[arg(long)]
    config: PathBuf,
    /// Rediscover grep roots, run one build/fold/GC sweep, and exit.
    #[arg(long)]
    once: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StandaloneConfig {
    store: StoreConfig,
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
    #[error("invalid store config: {0}")]
    StoreConfig(#[from] loonfs_objectstore::StoreConfigError),
    #[error("failed to open configured store: {0}")]
    OpenStore(#[from] loonfs_objectstore::ObjectStoreError),
    #[error(transparent)]
    RunOnce(#[from] loonfs_grep::GrepWorkerRunOnceError),
    #[error("failed to initialize tracing: {0}")]
    Tracing(String),
    #[error("standalone signal task failed: {0}")]
    SignalTask(String),
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
    let store = Arc::new(config.store.configured_object_store()?) as SharedObjectStore;
    let worker = GrepWorker::new(
        store.clone(),
        "loonfs-grep",
        loonfs_api::generated_id("wrs"),
        format!("loonfs-grep/{}", env!("CARGO_PKG_VERSION")),
    );
    let mut worker_loop = GrepWorkerLoop::new(worker, store, config.grep);

    if args.once {
        let report = worker_loop.run_once().await?;
        tracing::info!(
            namespaces_seen = report.namespaces_seen,
            namespaces_completed = report.namespaces_completed,
            "grep worker one-shot sweep completed"
        );
        return Ok(());
    }

    let shutdown = worker_loop.shutdown_handle();
    let signal_task = tokio::runtime::Handle::current().spawn(async move {
        shutdown_signal().await;
        shutdown.request_shutdown();
    });
    worker_loop.run().await;
    signal_task
        .await
        .map_err(|error| StandaloneError::SignalTask(error.to_string()))?;
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
    config.store.validate()?;
    Ok(config)
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
