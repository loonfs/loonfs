//! The reference server binary: load config, open the runtime, serve HTTP
//! until shutdown.

use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    loonfs_server::init_tracing_from_env()?;
    let args = Args::parse();
    let config = loonfs_server::load_server_config(&args.config)?;
    loonfs_server::serve(config).await?;
    Ok(())
}
