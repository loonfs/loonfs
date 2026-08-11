//! The reference server binary: load config, open the runtime, serve HTTP
//! until shutdown.

use clap::Parser;
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
    let mut exit_code = ExitCode::SUCCESS;
    if args.check_config || args.probe_store {
        // Either flag includes the startup checks, and combined flags run
        // them once before printing both requested reports.
        loonfs_server::check_config(&config).await?;
        let mut stdout = std::io::stdout().lock();
        if args.check_config {
            writeln!(stdout, "{}", config.check_summary())?;
        }
        if args.probe_store {
            let report = loonfs_server::probe_store(&config).await?;
            for check in &report.checks {
                writeln!(stdout, "{}", check.check_line())?;
            }
            if !report.all_passed() {
                exit_code = ExitCode::FAILURE;
            }
        }
        stdout.flush()?;
    } else {
        loonfs_server::serve(config).await?;
    }
    Ok(exit_code)
}
