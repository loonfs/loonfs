use crate::cmd::config::resolve_config_path;
use clap::{Args, Subcommand};
use loon_client::ops::FileCommand;
use std::path::PathBuf;

#[derive(Debug, Args)]
#[command(
    after_help = "Examples:\n  loon file ls demo:/\n  loon file stat demo:/docs/report.txt\n  loon file get demo:/hello.txt ./downloads\n  loon file cat demo:/hello.txt"
)]
pub struct FileArgs {
    #[command(subcommand)]
    command: FileSubcommand,
}

#[derive(Debug, Subcommand)]
enum FileSubcommand {
    /// List visible children from authoritative namespace state.
    Ls(FileSelectorArgs),
    /// Show authoritative metadata for one visible file or directory.
    Stat(FileSelectorArgs),
    /// Download one authoritative file to a local filesystem path.
    Get(FileGetArgs),
    /// Print one authoritative file's raw bytes to stdout.
    Cat(FileSelectorArgs),
}

#[derive(Debug, Clone, Args)]
struct FileSelectorArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Optional path to the ops TOML config file. If omitted, LOON_CONFIG, ./loondb-demo.local.toml, and ./loondb-demo.toml are checked in that order."
    )]
    config: Option<PathBuf>,
    #[arg(
        value_name = "SELECTOR",
        help = "Authoritative selector in the form <namespace>:/absolute/path."
    )]
    selector: String,
}

#[derive(Debug, Clone, Args)]
struct FileGetArgs {
    #[command(flatten)]
    selector: FileSelectorArgs,
    #[arg(
        value_name = "LOCAL_PATH",
        help = "Destination file path or existing directory."
    )]
    local_path: PathBuf,
}

impl FileArgs {
    pub(crate) fn into_command(self) -> anyhow::Result<FileCommand> {
        match self.command {
            FileSubcommand::Ls(args) => Ok(FileCommand::Ls {
                config_path: resolve_config_path(args.config)?.path,
                selector: args.selector,
            }),
            FileSubcommand::Stat(args) => Ok(FileCommand::Stat {
                config_path: resolve_config_path(args.config)?.path,
                selector: args.selector,
            }),
            FileSubcommand::Get(args) => Ok(FileCommand::Get {
                config_path: resolve_config_path(args.selector.config)?.path,
                selector: args.selector.selector,
                local_path: args.local_path,
            }),
            FileSubcommand::Cat(args) => Ok(FileCommand::Cat {
                config_path: resolve_config_path(args.config)?.path,
                selector: args.selector,
            }),
        }
    }
}
