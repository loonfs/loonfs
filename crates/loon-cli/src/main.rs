use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use loon_client::{Client, ClientConfig, NamespacePath};

#[derive(Debug, Parser)]
#[command(name = "loon")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    File {
        #[command(subcommand)]
        command: FileCommand,
    },
}

#[derive(Debug, Subcommand)]
enum NamespaceCommand {
    Create { name: String },
    List(JsonArgs),
}

#[derive(Debug, Subcommand)]
enum FileCommand {
    Ls {
        target: String,
        #[command(flatten)]
        json: JsonArgs,
    },
    Stat {
        target: String,
        #[command(flatten)]
        json: JsonArgs,
    },
    Cat {
        target: String,
    },
    Get {
        target: String,
        destination: String,
    },
    Put {
        source: String,
        target: String,
    },
    Rm {
        target: String,
    },
    Mv {
        from: String,
        to: String,
    },
}

#[derive(Debug, Args)]
struct JsonArgs {
    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.clone().context("missing --config")?;
    let config = ClientConfig::load(&config_path)
        .with_context(|| format!("load client config {}", config_path))?;
    let client = Client::new(config);

    match cli.command {
        Command::Namespace {
            command: NamespaceCommand::Create { name },
        } => {
            let namespace = client.create_namespace(&name)?;
            println!("{}", namespace.name);
        }
        Command::Namespace {
            command: NamespaceCommand::List(args),
        } => {
            let namespaces = client.list_namespaces()?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&namespaces)?);
            } else {
                for namespace in namespaces {
                    println!("{}", namespace.name);
                }
            }
        }
        Command::File {
            command: FileCommand::Ls { target, json },
        } => {
            let target = NamespacePath::parse(&target)?;
            let entries = client.list_path(&target)?;
            if json.json {
                println!("{}", serde_json::to_string_pretty(&entries)?);
            } else {
                for entry in entries {
                    let size = entry
                        .size_bytes
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_owned());
                    println!("{:?}\t{}\t{}", entry.inode_kind, size, entry.absolute_path);
                }
            }
        }
        Command::File {
            command: FileCommand::Stat { target, json },
        } => {
            let target = NamespacePath::parse(&target)?;
            let entry = client.stat_path(&target)?;
            if json.json {
                println!("{}", serde_json::to_string_pretty(&entry)?);
            } else {
                println!("path: {}", entry.absolute_path);
                println!("inode: {}", entry.inode_id);
                println!("kind: {:?}", entry.inode_kind);
                println!("seq: {}", entry.authoritative_head_seq.0);
                if let Some(size) = entry.size_bytes {
                    println!("size: {}", size);
                }
                if let Some(revision) = entry.revision_no {
                    println!("revision: {}", revision.0);
                }
            }
        }
        Command::File {
            command: FileCommand::Cat { target },
        } => {
            let target = NamespacePath::parse(&target)?;
            let bytes = client.read_file_bytes(&target)?;
            print!("{}", String::from_utf8_lossy(&bytes));
        }
        Command::File {
            command:
                FileCommand::Get {
                    target,
                    destination,
                },
        } => {
            let target = NamespacePath::parse(&target)?;
            client.get_to_path(&target, &destination)?;
        }
        Command::File {
            command: FileCommand::Put { source, target },
        } => {
            let target = NamespacePath::parse(&target)?;
            client.put_from_path(&source, &target)?;
        }
        Command::File {
            command: FileCommand::Rm { target },
        } => {
            let target = NamespacePath::parse(&target)?;
            let result = client.delete_path(&target)?;
            println!("{}", result.committed_seq.0);
        }
        Command::File {
            command: FileCommand::Mv { from, to },
        } => {
            let from = NamespacePath::parse(&from)?;
            let to = NamespacePath::parse(&to)?;
            let result = client.move_path(&from, &to)?;
            println!("{}", result.committed_seq.0);
        }
    }

    Ok(())
}
