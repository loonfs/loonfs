//! The reference server binary: load config, open the runtime, serve HTTP
//! until shutdown.

use clap::{ArgGroup, Parser};
use std::io::Write as _;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(
    name = "loonfs-server",
    version,
    group(
        ArgGroup::new("config_source")
            .required(true)
            .multiple(false)
            .args(["config", "config_toml"])
    )
)]
struct Args {
    /// Config file to load.
    #[arg(long)]
    config: Option<String>,
    /// Inline config TOML. Prefer LOONFS_SERVER_CONFIG_TOML so the value does
    /// not appear in process arguments.
    #[arg(long, env = "LOONFS_SERVER_CONFIG_TOML", hide_env_values = true)]
    config_toml: Option<String>,
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
    let config = match args.config {
        Some(path) => loonfs_server::load_server_config(path)?,
        None => loonfs_server::parse_server_config(
            args.config_toml
                .as_deref()
                .expect("clap should require exactly one config source"),
        )?,
    };
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
