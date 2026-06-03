use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    loon_server::init_tracing_from_env()?;
    let args = Args::parse();
    let config = loon_server::load_server_config(&args.config)?;
    loon_server::serve(config).await?;
    Ok(())
}
