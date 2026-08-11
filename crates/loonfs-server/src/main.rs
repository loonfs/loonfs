//! The reference server binary: load config, open the runtime, serve HTTP
//! until shutdown.

use clap::Parser;
use loonfs_objectstore::{run_store_contract_probe, StoreProbeOutcome};
use std::io::Write as _;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "loonfs-server", version)]
struct Args {
    /// Config file to load.
    #[arg(long)]
    config: String,
    /// Validate the startup config and exit without serving.
    #[arg(long)]
    check_config: bool,
    /// Validate startup, write and delete scratch objects while probing the
    /// configured store, and exit without serving.
    #[arg(long)]
    probe_store: bool,
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    // Parse the arguments first. `--version` and `--help` then answer even
    // when `LOONFS_TRACE` holds a value this build rejects.
    let args = Args::parse();
    loonfs_server::init_tracing_from_env()?;
    let config = loonfs_server::load_server_config(&args.config)?;
    if args.check_config || args.probe_store {
        // Run this once when the flags are combined: the probe adds to the
        // startup checks instead of opening the cache a second time.
        loonfs_server::check_config(&config).await?;
        let mut stdout = std::io::stdout().lock();
        if args.probe_store {
            let store = config.object_store()?.into_shared();
            let run_id = loonfs_api::generated_id("probe");
            let report = run_store_contract_probe(store.as_ref(), &run_id).await;
            for check in &report.checks {
                match &check.outcome {
                    StoreProbeOutcome::Passed => writeln!(stdout, "{}: passed", check.name)?,
                    StoreProbeOutcome::Unsupported => {
                        writeln!(stdout, "{}: unsupported", check.name)?
                    }
                    StoreProbeOutcome::Failed { message } => {
                        writeln!(stdout, "{}: failed: {message}", check.name)?
                    }
                }
            }
            stdout.flush()?;
            return Ok(if report.all_passed() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            });
        }
        writeln!(stdout, "{}", config.check_summary())?;
        stdout.flush()?;
        return Ok(ExitCode::SUCCESS);
    }
    loonfs_server::serve(config).await?;
    Ok(ExitCode::SUCCESS)
}
