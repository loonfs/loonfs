use clap::{Args, Parser, Subcommand};
use std::io::IsTerminal;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "loon", version)]
pub struct Cli {
    #[arg(long, global = true)]
    pub profile: Option<String>,
    #[arg(long, global = true)]
    pub json: bool,
    #[arg(long, global = true)]
    pub no_input: bool,
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    Local {
        #[command(subcommand)]
        command: LocalCommand,
    },
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    Filesystem {
        #[command(subcommand)]
        command: FilesystemCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Version,
}

#[derive(Debug, Subcommand)]
pub enum ProfileCommand {
    Add {
        #[command(subcommand)]
        command: ProfileAddCommand,
    },
    List,
    Use {
        name: String,
    },
    Show {
        name: Option<String>,
    },
    Remove {
        name: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ProfileAddCommand {
    Local(ProfileAddLocalArgs),
    Remote(ProfileAddRemoteArgs),
}

#[derive(Debug, Args)]
pub struct ProfileAddLocalArgs {
    pub name: String,
    #[arg(long)]
    pub server_config: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ProfileAddRemoteArgs {
    pub name: String,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum LocalCommand {
    Up,
    Status,
    Down,
}

#[derive(Debug, Subcommand)]
pub enum NamespaceCommand {
    Create { name: String },
    List,
}

#[derive(Debug, Subcommand)]
pub enum FilesystemCommand {
    Ls {
        namespace: String,
        path: Option<String>,
    },
    Stat {
        namespace: String,
        path: String,
    },
    Cat {
        namespace: String,
        path: String,
    },
    Get {
        namespace: String,
        remote_path: String,
        local_destination: Option<String>,
    },
    Put {
        namespace: String,
        local_path: String,
        remote_path: Option<String>,
        #[arg(long)]
        force: bool,
    },
    Rm {
        namespace: String,
        remote_path: String,
    },
    Mv {
        namespace: String,
        source_path: String,
        dest_path: String,
    },
    Cp {
        namespace: String,
        source_path: String,
        dest_path: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Path,
    Show,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeBehavior {
    pub json: bool,
    pub no_input: bool,
    pub interactive: bool,
}

impl RuntimeBehavior {
    pub fn detect(cli: &Cli) -> Self {
        let interactive = !cli.json
            && !cli.no_input
            && std::io::stdin().is_terminal()
            && std::io::stderr().is_terminal();
        Self {
            json: cli.json,
            no_input: cli.no_input,
            interactive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    ProfileAdd,
    ProfileList,
    ProfileUse,
    ProfileShow,
    ProfileRemove,
    LocalUp,
    LocalStatus,
    LocalDown,
    NamespaceCreate,
    NamespaceList,
    FilesystemLs,
    FilesystemStat,
    FilesystemCat,
    FilesystemGet,
    FilesystemPut,
    FilesystemRm,
    FilesystemMv,
    FilesystemCp,
    ConfigPath,
    ConfigShow,
    Version,
}

impl CommandKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CommandKind::ProfileAdd => "profile_add",
            CommandKind::ProfileList => "profile_list",
            CommandKind::ProfileUse => "profile_use",
            CommandKind::ProfileShow => "profile_show",
            CommandKind::ProfileRemove => "profile_remove",
            CommandKind::LocalUp => "local_up",
            CommandKind::LocalStatus => "local_status",
            CommandKind::LocalDown => "local_down",
            CommandKind::NamespaceCreate => "namespace_create",
            CommandKind::NamespaceList => "namespace_list",
            CommandKind::FilesystemLs => "filesystem_ls",
            CommandKind::FilesystemStat => "filesystem_stat",
            CommandKind::FilesystemCat => "filesystem_cat",
            CommandKind::FilesystemGet => "filesystem_get",
            CommandKind::FilesystemPut => "filesystem_put",
            CommandKind::FilesystemRm => "filesystem_rm",
            CommandKind::FilesystemMv => "filesystem_mv",
            CommandKind::FilesystemCp => "filesystem_cp",
            CommandKind::ConfigPath => "config_path",
            CommandKind::ConfigShow => "config_show",
            CommandKind::Version => "version",
        }
    }

    pub fn supports_json(self) -> bool {
        !matches!(self, CommandKind::FilesystemCat)
    }
}

impl Cli {
    pub fn kind(&self) -> CommandKind {
        match &self.command {
            Command::Profile { command } => match command {
                ProfileCommand::Add { .. } => CommandKind::ProfileAdd,
                ProfileCommand::List => CommandKind::ProfileList,
                ProfileCommand::Use { .. } => CommandKind::ProfileUse,
                ProfileCommand::Show { .. } => CommandKind::ProfileShow,
                ProfileCommand::Remove { .. } => CommandKind::ProfileRemove,
            },
            Command::Local { command } => match command {
                LocalCommand::Up => CommandKind::LocalUp,
                LocalCommand::Status => CommandKind::LocalStatus,
                LocalCommand::Down => CommandKind::LocalDown,
            },
            Command::Namespace { command } => match command {
                NamespaceCommand::Create { .. } => CommandKind::NamespaceCreate,
                NamespaceCommand::List => CommandKind::NamespaceList,
            },
            Command::Filesystem { command } => match command {
                FilesystemCommand::Ls { .. } => CommandKind::FilesystemLs,
                FilesystemCommand::Stat { .. } => CommandKind::FilesystemStat,
                FilesystemCommand::Cat { .. } => CommandKind::FilesystemCat,
                FilesystemCommand::Get { .. } => CommandKind::FilesystemGet,
                FilesystemCommand::Put { .. } => CommandKind::FilesystemPut,
                FilesystemCommand::Rm { .. } => CommandKind::FilesystemRm,
                FilesystemCommand::Mv { .. } => CommandKind::FilesystemMv,
                FilesystemCommand::Cp { .. } => CommandKind::FilesystemCp,
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}
