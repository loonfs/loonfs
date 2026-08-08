//! The reference server binary: load config, open the runtime, serve HTTP
//! until shutdown.

use clap::Parser;
use std::io::Write as _;

#[derive(Debug, Parser)]
#[command(name = "loonfs-server", version)]
struct Args {
    /// Config file to load.
    #[arg(long)]
    config: String,
    /// Run the startup checks, report the result, and exit without serving.
    /// The check reads the config, loads the TLS identity, and opens the
    /// local block cache. It does not bind the configured address and it
    /// performs no object-store operation, though constructing a `local-fs`
    /// store creates its root directory as a start does.
    #[arg(long)]
    check_config: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the arguments first. `--version` and `--help` then answer even
    // when `LOONFS_TRACE` holds a value this build rejects.
    let args = Args::parse();
    loonfs_server::init_tracing_from_env()?;
    let config = loonfs_server::load_server_config(&args.config)?;
    if args.check_config {
        // The summary is printed only once the checks a start runs have
        // passed, so the printed line means the same thing the flag claims.
        loonfs_server::check_config(&config).await?;
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{}", config.check_summary())?;
        stdout.flush()?;
        return Ok(());
    }
    loonfs_server::serve(config).await?;
    Ok(())
}
