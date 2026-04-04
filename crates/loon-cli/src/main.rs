use clap::{Args, Parser, Subcommand};
use loon_api::ApiError;
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use serde_json::json;
use std::process::ExitCode;

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
    Create {
        name: String,
        #[command(flatten)]
        json: JsonArgs,
    },
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
        #[command(flatten)]
        json: JsonArgs,
    },
    Put {
        source: String,
        target: String,
        #[command(flatten)]
        json: JsonArgs,
    },
    Rm {
        target: String,
        #[command(flatten)]
        json: JsonArgs,
    },
    Mv {
        from: String,
        to: String,
        #[command(flatten)]
        json: JsonArgs,
    },
}

#[derive(Debug, Args)]
struct JsonArgs {
    #[arg(long)]
    json: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let wants_json = cli.command.wants_json();

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            render_error(&error, wants_json);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), CliError> {
    let config_path = cli.config.ok_or(CliError::missing_config())?;
    let config = ClientConfig::load(&config_path).map_err(CliError::from)?;
    let client = Client::new(config);

    match cli.command {
        Command::Namespace {
            command: NamespaceCommand::Create { name, json },
        } => {
            let namespace = client.create_namespace(&name)?;
            if json.json {
                print_json(&namespace)?;
            } else {
                println!("{}", namespace.name);
            }
        }
        Command::Namespace {
            command: NamespaceCommand::List(args),
        } => {
            let namespaces = client.list_namespaces()?;
            if args.json {
                print_json(&namespaces)?;
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
                print_json(&entries)?;
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
                print_json(&entry)?;
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
                    json,
                },
        } => {
            let target_path = NamespacePath::parse(&target)?;
            let result = client.get_to_path(&target_path, &destination)?;
            if json.json {
                print_json(&json!({
                    "target": render_target(&target_path),
                    "destination": result.destination.display().to_string(),
                    "bytes_written": result.bytes_written,
                }))?;
            }
        }
        Command::File {
            command:
                FileCommand::Put {
                    source,
                    target,
                    json,
                },
        } => {
            let target_path = NamespacePath::parse(&target)?;
            let result = client.put_from_path(&source, &target_path)?;
            if json.json {
                print_json(&json!({
                    "target": render_target(&target_path),
                    "committed_seq": result.committed_seq.0,
                }))?;
            }
        }
        Command::File {
            command: FileCommand::Rm { target, json },
        } => {
            let target_path = NamespacePath::parse(&target)?;
            let result = client.delete_path(&target_path)?;
            if json.json {
                print_json(&json!({
                    "target": render_target(&target_path),
                    "committed_seq": result.committed_seq.0,
                }))?;
            } else {
                println!("{}", result.committed_seq.0);
            }
        }
        Command::File {
            command: FileCommand::Mv { from, to, json },
        } => {
            let from_path = NamespacePath::parse(&from)?;
            let to_path = NamespacePath::parse(&to)?;
            let result = client.move_path(&from_path, &to_path)?;
            if json.json {
                print_json(&json!({
                    "from": render_target(&from_path),
                    "to": render_target(&to_path),
                    "committed_seq": result.committed_seq.0,
                }))?;
            } else {
                println!("{}", result.committed_seq.0);
            }
        }
    }

    Ok(())
}

fn print_json<T>(value: &T) -> Result<(), CliError>
where
    T: serde::Serialize,
{
    let body = serde_json::to_string_pretty(value).map_err(CliError::json)?;
    println!("{body}");
    Ok(())
}

fn render_target(path: &NamespacePath) -> String {
    format!("{}:{}", path.namespace, path.absolute_path)
}

fn render_error(error: &CliError, json_mode: bool) {
    if json_mode {
        let api_error = error.as_api_error();
        let body = serde_json::to_string(&api_error).unwrap_or_else(|_| {
            "{\"code\":\"client_error\",\"message\":\"failed to render cli error\"}".to_owned()
        });
        eprintln!("{body}");
    } else {
        eprintln!("{}", error.message());
    }
}

#[derive(Debug)]
struct CliError {
    code: String,
    message: String,
}

impl CliError {
    fn missing_config() -> Self {
        Self {
            code: "invalid_config".to_owned(),
            message: "missing `--config`".to_owned(),
        }
    }

    fn json(error: serde_json::Error) -> Self {
        Self {
            code: "client_error".to_owned(),
            message: format!("json error: {error}"),
        }
    }

    fn message(&self) -> &str {
        &self.message
    }

    fn as_api_error(&self) -> ApiError {
        ApiError {
            code: self.code.clone(),
            message: self.message.clone(),
        }
    }
}

impl From<ClientError> for CliError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::ConfigIo(message) => Self {
                code: "invalid_config".to_owned(),
                message: format!("failed to read config: {message}"),
            },
            ClientError::ConfigDecode(message) => Self {
                code: "invalid_config".to_owned(),
                message: format!("failed to decode config: {message}"),
            },
            ClientError::MissingConfigField { field } => Self {
                code: "invalid_config".to_owned(),
                message: format!("missing `{field}`"),
            },
            ClientError::ConfigValidation { field, reason } => Self {
                code: "invalid_config".to_owned(),
                message: format!("invalid `{field}`: {reason}"),
            },
            ClientError::InvalidNamespacePath(path) => Self {
                code: "invalid_target".to_owned(),
                message: format!("invalid namespace path `{path}`"),
            },
            ClientError::Io(message) => Self {
                code: "io_error".to_owned(),
                message: format!("i/o error: {message}"),
            },
            ClientError::Api { code, message, .. } => Self { code, message },
            ClientError::Http(message) => Self {
                code: "client_error".to_owned(),
                message: format!("http error: {message}"),
            },
            ClientError::Json(message) => Self {
                code: "client_error".to_owned(),
                message: format!("json error: {message}"),
            },
        }
    }
}

impl Command {
    fn wants_json(&self) -> bool {
        match self {
            Command::Namespace { command } => match command {
                NamespaceCommand::Create { json, .. } => json.json,
                NamespaceCommand::List(args) => args.json,
            },
            Command::File { command } => match command {
                FileCommand::Ls { json, .. }
                | FileCommand::Stat { json, .. }
                | FileCommand::Get { json, .. }
                | FileCommand::Put { json, .. }
                | FileCommand::Rm { json, .. }
                | FileCommand::Mv { json, .. } => json.json,
                FileCommand::Cat { .. } => false,
            },
        }
    }
}
