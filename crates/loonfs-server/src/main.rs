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
    /// Load and validate the config, report the result, and exit without
    /// serving. A `local-fs` store creates its root directory during the
    /// check; the cloud providers touch no storage.
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
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{}", config.check_summary())?;
        stdout.flush()?;
        return Ok(());
    }
    loonfs_server::serve(config).await?;
    Ok(())
}
