use clap::{Args, Parser, Subcommand};
use std::io::IsTerminal;

#[derive(Debug, Parser)]
#[command(name = "loon")]
pub(crate) struct Cli {
    #[arg(long, global = true)]
    pub json: bool,
    #[arg(long, global = true)]
    pub no_input: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
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
    Cat(FilesystemCatArgs),
    Get(FilesystemGetArgs),
    Put(FilesystemPutArgs),
    Revisions(FilesystemRevisionsArgs),
    Restore(FilesystemRestoreArgs),
    Mkdir(FilesystemPathArgs),
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
pub(crate) struct InitArgs {
    pub name: Option<String>,
    #[arg(long, value_name = "embedded|remote")]
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
    pub account_name: Option<String>,
    #[arg(long)]
    pub container_name: Option<String>,
    #[arg(long)]
    pub access_key: Option<String>,
    #[arg(long)]
    pub service_account_key_path: Option<String>,
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
pub(crate) enum ProfileCommand {
    Create(ProfileCreateArgs),
    List,
    Show { name: Option<String> },
    Update(ProfileUpdateArgs),
    Remove { name: String },
    Use { name: String },
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateArgs {
    pub name: String,
    #[arg(long, value_name = "embedded|remote")]
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
    pub account_name: Option<String>,
    #[arg(long)]
    pub container_name: Option<String>,
    #[arg(long)]
    pub access_key: Option<String>,
    #[arg(long)]
    pub service_account_key_path: Option<String>,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileUpdateArgs {
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
    pub account_name: Option<String>,
    #[arg(long)]
    pub container_name: Option<String>,
    #[arg(long)]
    pub access_key: Option<String>,
    #[arg(long)]
    pub service_account_key_path: Option<String>,
    #[arg(long)]
    pub server_url: Option<String>,
    #[arg(long)]
    pub auth_token: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct ProfileSelectorArgs {
    #[arg(long)]
    pub profile: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct TargetSelectorArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[arg(long)]
    pub namespace: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceUseArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub namespace: String,
}

#[derive(Debug, Args)]
pub(crate) struct CurrentArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespaceCommand {
    Create(NamespaceCreateArgs),
    Delete(NamespaceDeleteArgs),
    Fork(NamespaceForkArgs),
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceCreateArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceDeleteArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub namespace_id: String,
    /// Delete only if the namespace head is still at this sequence.
    #[arg(long)]
    pub expected_head_seq: Option<u64>,
    /// Skip the interactive confirmation.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceForkArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    pub source: String,
    pub new_namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemLsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemPathArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRevisionsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemCatArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    #[arg(long)]
    pub revision: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemGetArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub remote_path: String,
    pub local_destination: Option<String>,
    #[arg(long)]
    pub revision: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemPutArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub local_path: String,
    pub remote_path: Option<String>,
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemMoveArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub source_path: String,
    pub dest_path: String,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRestoreArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    #[arg(long)]
    pub revision: u64,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    Path,
    Show,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeBehavior {
    pub json: bool,
    pub no_input: bool,
    pub interactive: bool,
}

impl RuntimeBehavior {
    pub(crate) fn detect(cli: &Cli) -> Self {
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
pub(crate) enum CommandKind {
    Init,
    ProfileCreate,
    ProfileList,
    ProfileShow,
    ProfileUpdate,
    ProfileRemove,
    ProfileUse,
    NamespaceCreate,
    NamespaceDelete,
    NamespaceFork,
    NamespaceUse,
    Current,
    FilesystemLs,
    FilesystemStat,
    FilesystemCat,
    FilesystemGet,
    FilesystemPut,
    FilesystemRevisions,
    FilesystemRestore,
    FilesystemMkdir,
    FilesystemRm,
    FilesystemMv,
    FilesystemCp,
    ConfigPath,
    ConfigShow,
    Version,
}

impl CommandKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CommandKind::Init => "init",
            CommandKind::ProfileCreate => "profile_create",
            CommandKind::ProfileList => "profile_list",
            CommandKind::ProfileShow => "profile_show",
            CommandKind::ProfileUpdate => "profile_update",
            CommandKind::ProfileRemove => "profile_remove",
            CommandKind::ProfileUse => "profile_use",
            CommandKind::NamespaceCreate => "namespace_create",
            CommandKind::NamespaceDelete => "namespace_delete",
            CommandKind::NamespaceFork => "namespace_fork",
            CommandKind::NamespaceUse => "namespace_use",
            CommandKind::Current => "current",
            CommandKind::FilesystemLs => "filesystem_ls",
            CommandKind::FilesystemStat => "filesystem_stat",
            CommandKind::FilesystemCat => "filesystem_cat",
            CommandKind::FilesystemGet => "filesystem_get",
            CommandKind::FilesystemPut => "filesystem_put",
            CommandKind::FilesystemRevisions => "filesystem_revisions",
            CommandKind::FilesystemRestore => "filesystem_restore",
            CommandKind::FilesystemMkdir => "filesystem_mkdir",
            CommandKind::FilesystemRm => "filesystem_rm",
            CommandKind::FilesystemMv => "filesystem_mv",
            CommandKind::FilesystemCp => "filesystem_cp",
            CommandKind::ConfigPath => "config_path",
            CommandKind::ConfigShow => "config_show",
            CommandKind::Version => "version",
        }
    }

    pub(crate) fn supports_json(self) -> bool {
        !matches!(self, CommandKind::FilesystemCat)
    }
}

impl Cli {
    pub(crate) fn kind(&self) -> CommandKind {
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
                NamespaceCommand::Delete(_) => CommandKind::NamespaceDelete,
                NamespaceCommand::Fork(_) => CommandKind::NamespaceFork,
            },
            Command::Use(_) => CommandKind::NamespaceUse,
            Command::Current(_) => CommandKind::Current,
            Command::Ls(_) => CommandKind::FilesystemLs,
            Command::Stat(_) => CommandKind::FilesystemStat,
            Command::Cat(_) => CommandKind::FilesystemCat,
            Command::Get(_) => CommandKind::FilesystemGet,
            Command::Put(_) => CommandKind::FilesystemPut,
            Command::Revisions(_) => CommandKind::FilesystemRevisions,
            Command::Restore(_) => CommandKind::FilesystemRestore,
            Command::Mkdir(_) => CommandKind::FilesystemMkdir,
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
