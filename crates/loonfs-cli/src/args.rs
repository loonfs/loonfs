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
    Mv(FilesystemTransferArgs),
    Cp(FilesystemTransferArgs),
    Changes(ChangesArgs),
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
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
pub(crate) struct FilesystemTransferArgs {
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

#[derive(Debug, Args)]
pub(crate) struct ChangesArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Return committed changes after this sequence (defaults to 0, the
    /// start of retained history).
    #[arg(long)]
    pub after: Option<u64>,
    /// Maximum number of changes to return.
    #[arg(long)]
    pub limit: Option<u32>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminCommand {
    Checkpoint(AdminCheckpointArgs),
    CheckpointRelease(AdminCheckpointReleaseArgs),
    Flush(AdminNamespaceArgs),
    RetentionAdvance(AdminNamespaceArgs),
    Tick(AdminTickArgs),
    Gc(AdminGcArgs),
}

#[derive(Debug, Args)]
pub(crate) struct AdminNamespaceArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct AdminCheckpointArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Label recorded on the checkpoint record (a label, not a key).
    #[arg(long)]
    pub name: String,
    /// Optional lifetime; the record expires this many milliseconds from
    /// now. Omitted means the pin holds until explicitly released.
    #[arg(long)]
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct AdminCheckpointReleaseArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Checkpoint id to release.
    pub checkpoint_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct AdminTickArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Flush the visible WAL tail into metadata tables when it reaches this many
    /// segments (server default when omitted).
    #[arg(long)]
    pub max_wal_tail_segments: Option<u64>,
    /// Run a garbage-collection pass after the tick's flush work.
    #[arg(long)]
    pub gc: bool,
}

#[derive(Debug, Args)]
pub(crate) struct AdminGcArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Objects younger than this are never deleted (server default when
    /// omitted).
    #[arg(long)]
    pub grace_window_ms: Option<u64>,
    /// Abandoned bootstrap trees older than this may be reaped (server
    /// default when omitted).
    #[arg(long)]
    pub reap_window_ms: Option<u64>,
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
    Changes,
    AdminCheckpoint,
    AdminCheckpointRelease,
    AdminFlush,
    AdminRetentionAdvance,
    AdminTick,
    AdminGc,
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
            CommandKind::Changes => "changes",
            CommandKind::AdminCheckpoint => "admin_checkpoint",
            CommandKind::AdminCheckpointRelease => "admin_checkpoint_release",
            CommandKind::AdminFlush => "admin_flush",
            CommandKind::AdminRetentionAdvance => "admin_retention_advance",
            CommandKind::AdminTick => "admin_tick",
            CommandKind::AdminGc => "admin_gc",
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
            Command::Changes(_) => CommandKind::Changes,
            Command::Admin { command } => match command {
                AdminCommand::Checkpoint(_) => CommandKind::AdminCheckpoint,
                AdminCommand::CheckpointRelease(_) => CommandKind::AdminCheckpointRelease,
                AdminCommand::Flush(_) => CommandKind::AdminFlush,
                AdminCommand::RetentionAdvance(_) => CommandKind::AdminRetentionAdvance,
                AdminCommand::Tick(_) => CommandKind::AdminTick,
                AdminCommand::Gc(_) => CommandKind::AdminGc,
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}
