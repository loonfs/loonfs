use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = loon_server::load_server_config(&args.config)?;
    loon_server::serve(config).await?;
    Ok(())
}
