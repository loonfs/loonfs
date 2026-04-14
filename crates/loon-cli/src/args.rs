use clap::{Args, Parser, Subcommand};
use std::io::IsTerminal;

#[derive(Debug, Parser)]
#[command(name = "loon")]
pub struct Cli {
    #[arg(long, global = true)]
    pub json: bool,
    #[arg(long, global = true)]
    pub no_input: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Init(InitArgs),
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    Use(NamespaceUseArgs),
    Current(CurrentArgs),
    Ls(FilesystemLsArgs),
    Stat(FilesystemPathArgs),
    Versions(FilesystemVersionsArgs),
    Cat(FilesystemCatArgs),
    Get(FilesystemGetArgs),
    Put(FilesystemPutArgs),
    Rm(FilesystemPathArgs),
    Mv(FilesystemMoveArgs),
    Cp(FilesystemMoveArgs),
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Version,
}

#[derive(Debug, Args)]
pub struct InitArgs {
    pub name: Option<String>,
    #[arg(long)]
    pub mode: Option<String>,
    #[arg(long)]
    pub store_kind: Option<String>,
    #[arg(long)]
    pub root: Option<String>,
    #[arg(long)]
    pub bucket: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long)]
    pub access_key_id: Option<String>,
    #[arg(long)]
    pub secret_access_key: Option<String>,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long)]
    pub session_token: Option<String>,
    #[arg(long)]
    pub account_id: Option<String>,
    #[arg(long)]
    pub key_prefix: Option<String>,
    #[arg(long)]
    pub force_path_style: bool,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ProfileCommand {
    Create(ProfileCreateArgs),
    List,
    Show { name: Option<String> },
    Update(ProfileUpdateArgs),
    Remove { name: String },
    Use { name: String },
}

#[derive(Debug, Args)]
pub struct ProfileCreateArgs {
    pub name: String,
    #[arg(long)]
    pub mode: Option<String>,
    #[arg(long)]
    pub store_kind: Option<String>,
    #[arg(long)]
    pub root: Option<String>,
    #[arg(long)]
    pub key_prefix: Option<String>,
    #[arg(long)]
    pub bucket: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long)]
    pub access_key_id: Option<String>,
    #[arg(long)]
    pub secret_access_key: Option<String>,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long)]
    pub session_token: Option<String>,
    #[arg(long)]
    pub force_path_style: bool,
    #[arg(long)]
    pub account_id: Option<String>,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Args)]
pub struct ProfileUpdateArgs {
    pub name: String,
    #[arg(long)]
    pub root: Option<String>,
    #[arg(long)]
    pub key_prefix: Option<String>,
    #[arg(long)]
    pub bucket: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long)]
    pub access_key_id: Option<String>,
    #[arg(long)]
    pub secret_access_key: Option<String>,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long)]
    pub session_token: Option<String>,
    #[arg(long)]
    pub account_id: Option<String>,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct ProfileSelectorArgs {
    #[arg(long)]
    pub profile: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct TargetSelectorArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[arg(long)]
    pub namespace: Option<String>,
}

#[derive(Debug, Args)]
pub struct NamespaceUseArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub namespace: String,
}

#[derive(Debug, Args)]
pub struct CurrentArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
}

#[derive(Debug, Subcommand)]
pub enum NamespaceCommand {
    Create(NamespaceCreateArgs),
    List(NamespaceListArgs),
}

#[derive(Debug, Args)]
pub struct NamespaceCreateArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub name: String,
}

#[derive(Debug, Args)]
pub struct NamespaceListArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
}

#[derive(Debug, Args)]
pub struct FilesystemLsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: Option<String>,
}

#[derive(Debug, Args)]
pub struct FilesystemPathArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
}

#[derive(Debug, Args)]
pub struct FilesystemVersionsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: Option<String>,
    #[arg(long)]
    pub inode: Option<u64>,
    #[arg(long = "before-revision")]
    pub before_revision: Option<u64>,
    #[arg(long)]
    pub limit: Option<u32>,
}

#[derive(Debug, Args)]
pub struct FilesystemCatArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: Option<String>,
    #[arg(long)]
    pub inode: Option<u64>,
    #[arg(long)]
    pub revision: Option<u64>,
}

#[derive(Debug, Args)]
pub struct FilesystemGetArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub remote_path: Option<String>,
    #[arg(long)]
    pub inode: Option<u64>,
    #[arg(long)]
    pub revision: Option<u64>,
    pub local_destination: Option<String>,
}

#[derive(Debug, Args)]
pub struct FilesystemPutArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub local_path: String,
    pub remote_path: Option<String>,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct FilesystemMoveArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub source_path: String,
    pub dest_path: String,
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
    Init,
    ProfileCreate,
    ProfileList,
    ProfileShow,
    ProfileUpdate,
    ProfileRemove,
    ProfileUse,
    NamespaceCreate,
    NamespaceList,
    NamespaceUse,
    Current,
    FilesystemLs,
    FilesystemStat,
    FilesystemVersions,
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
            CommandKind::Init => "init",
            CommandKind::ProfileCreate => "profile_create",
            CommandKind::ProfileList => "profile_list",
            CommandKind::ProfileShow => "profile_show",
            CommandKind::ProfileUpdate => "profile_update",
            CommandKind::ProfileRemove => "profile_remove",
            CommandKind::ProfileUse => "profile_use",
            CommandKind::NamespaceCreate => "namespace_create",
            CommandKind::NamespaceList => "namespace_list",
            CommandKind::NamespaceUse => "namespace_use",
            CommandKind::Current => "current",
            CommandKind::FilesystemLs => "filesystem_ls",
            CommandKind::FilesystemStat => "filesystem_stat",
            CommandKind::FilesystemVersions => "filesystem_versions",
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
            Command::Init(_) => CommandKind::Init,
            Command::Profile { command } => match command {
                ProfileCommand::Create(_) => CommandKind::ProfileCreate,
                ProfileCommand::List => CommandKind::ProfileList,
                ProfileCommand::Show { .. } => CommandKind::ProfileShow,
                ProfileCommand::Update(_) => CommandKind::ProfileUpdate,
                ProfileCommand::Remove { .. } => CommandKind::ProfileRemove,
                ProfileCommand::Use { .. } => CommandKind::ProfileUse,
            },
            Command::Namespace { command } => match command {
                NamespaceCommand::Create(_) => CommandKind::NamespaceCreate,
                NamespaceCommand::List(_) => CommandKind::NamespaceList,
            },
            Command::Use(_) => CommandKind::NamespaceUse,
            Command::Current(_) => CommandKind::Current,
            Command::Ls(_) => CommandKind::FilesystemLs,
            Command::Stat(_) => CommandKind::FilesystemStat,
            Command::Versions(_) => CommandKind::FilesystemVersions,
            Command::Cat(_) => CommandKind::FilesystemCat,
            Command::Get(_) => CommandKind::FilesystemGet,
            Command::Put(_) => CommandKind::FilesystemPut,
            Command::Rm(_) => CommandKind::FilesystemRm,
            Command::Mv(_) => CommandKind::FilesystemMv,
            Command::Cp(_) => CommandKind::FilesystemCp,
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}
