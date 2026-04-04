use anyhow::Result;
use clap::{Parser, Subcommand};
use loon_client::{Client, ClientConfig, NamespacePath};

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Smoke {
        #[arg(long)]
        config: String,
        #[arg(long)]
        namespace: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Smoke { config, namespace } => {
            let config = ClientConfig::load(&config)?;
            let client = Client::new(config);
            let _ = client.create_namespace(&namespace);
            let root = NamespacePath {
                namespace: namespace.clone(),
                absolute_path: "/".to_owned(),
            };
            let _ = client.list_path(&root)?;
            println!("smoke ok: {}", namespace);
        }
    }
    Ok(())
}
