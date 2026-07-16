//! The clap argument grammar for every `loon` command.

use clap::{Args, Parser, Subcommand};
use std::io::IsTerminal;

/// `loon x.y.z (commit date)`: the string served by both `--version` and
/// the `version` subcommand, built from the metadata `build.rs` embeds.
pub(crate) const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("LOON_GIT_COMMIT"),
    " ",
    env!("LOON_GIT_COMMIT_DATE"),
    ")"
);

#[derive(Debug, Parser)]
#[command(name = "loon", version = LONG_VERSION)]
pub(crate) struct Cli {
    /// Emit machine-readable JSON instead of human output.
    #[arg(long, global = true)]
    pub json: bool,
    /// Never prompt; fail instead when input would be required.
    #[arg(long, global = true)]
    pub no_input: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Create the config file and a first profile.
    Init(InitArgs),
    /// Manage connection profiles (embedded stores and remote servers).
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    /// Create, fork, or delete namespaces.
    Namespace {
        #[command(subcommand)]
        command: NamespaceCommand,
    },
    /// Set the default namespace for a profile.
    Use(NamespaceUseArgs),
    /// Show the active profile and its default namespace.
    Current(CurrentArgs),
    /// List a directory.
    Ls(FilesystemLsArgs),
    /// Describe one path (kind, size, revision, content digest).
    Stat(FilesystemPathArgs),
    /// Print a file's content to stdout.
    Cat(FilesystemCatArgs),
    /// Search file content through the gram index.
    Grep(FilesystemGrepArgs),
    /// Download a file to a local path (or `-` for stdout).
    Get(FilesystemGetArgs),
    /// Upload a local file to a namespace path.
    Put(FilesystemPutArgs),
    /// List a file's revision history, newest first.
    Revisions(FilesystemRevisionsArgs),
    /// Write a prior revision's content as the file's next revision.
    Restore(FilesystemRestoreArgs),
    /// Create a directory.
    Mkdir(FilesystemMkdirArgs),
    /// Delete a file or empty directory.
    Rm(FilesystemPathMutationArgs),
    /// Move or rename a path.
    Mv(FilesystemTransferArgs),
    /// Copy a file to another path.
    Cp(FilesystemTransferArgs),
    /// List committed changes after a sequence number.
    Changes(ChangesArgs),
    /// Maintenance operations: checkpoints, ticks, retention, GC, indexes.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
    /// Inspect the CLI config file.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print version and build metadata.
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
    // Provider secrets fall back to the standard environment variables so
    // quickstarts never need them on the command line (argv is visible to
    // `ps` and lands in shell history).
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key_id: Option<String>,
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_access_key: Option<String>,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long, env = "AWS_SESSION_TOKEN")]
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
    #[arg(long, env = "LOONFS_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// Add a profile to the config file.
    Create(ProfileCreateArgs),
    /// List configured profiles.
    List,
    /// Show one profile (secrets redacted).
    Show { name: Option<String> },
    /// Update fields of an existing profile.
    Update(ProfileUpdateArgs),
    /// Remove a profile from the config file.
    Remove { name: String },
    /// Make a profile the default.
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
    // Same environment fallbacks as `InitArgs`, and for the same reason.
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key_id: Option<String>,
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_access_key: Option<String>,
    #[arg(long)]
    pub endpoint_url: Option<String>,
    #[arg(long, env = "AWS_SESSION_TOKEN")]
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
    #[arg(long, env = "LOONFS_AUTH_TOKEN")]
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
    /// Profile to run against (defaults to the configured default profile).
    #[arg(long)]
    pub profile: Option<String>,
    /// Disable the bounded automatic retry of transient server errors
    /// (`server_busy`, `commit_queue_full`) in remote mode.
    #[arg(long)]
    pub no_retry: bool,
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
    /// Create a new empty namespace.
    Create(NamespaceCreateArgs),
    /// Permanently delete a namespace and retire its id.
    Delete(NamespaceDeleteArgs),
    /// Fork a namespace into a new one; O(1), no bytes copied.
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
pub(crate) struct FilesystemPathMutationArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemMkdirArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    /// Create missing parent directories as well.
    #[arg(short = 'p', long)]
    pub parents: bool,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long)]
    pub commit_id: Option<String>,
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
pub(crate) struct FilesystemGrepArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Pattern in the Rust regex dialect; `^`/`$` anchor lines.
    pub pattern: String,
    /// Restrict matches to files under this absolute path prefix.
    #[arg(long)]
    pub path_prefix: Option<String>,
    #[arg(short = 'i', long)]
    pub ignore_case: bool,
    /// Matches per page; the command follows cursors to completion.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Permit a capped exhaustive scan for patterns with no literal bytes.
    #[arg(long)]
    pub allow_scan: bool,
    /// Accept indexed-only results when the unindexed tail exceeds the
    /// scan budget.
    #[arg(long)]
    pub allow_stale: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemGetArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub remote_path: String,
    /// Local destination (defaults to the remote basename; `-` streams to
    /// stdout).
    pub local_destination: Option<String>,
    /// Download this revision instead of the current content.
    #[arg(long)]
    pub revision: Option<u64>,
    /// Overwrite the local destination if it already exists.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemPutArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub local_path: String,
    pub remote_path: Option<String>,
    #[arg(long)]
    pub force: bool,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemTransferArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub source_path: String,
    pub dest_path: String,
    #[arg(long)]
    pub force: bool,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRestoreArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    pub path: String,
    #[arg(long)]
    pub revision: u64,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long)]
    pub commit_id: Option<String>,
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
    /// Pin the namespace's current state under a named checkpoint.
    Checkpoint(AdminCheckpointArgs),
    /// Release a checkpoint pin.
    CheckpointRelease(AdminCheckpointReleaseArgs),
    /// Flush the WAL tail into a durable segment.
    Flush(AdminNamespaceArgs),
    /// Advance the retention floor to reclaim old history.
    RetentionAdvance(AdminNamespaceArgs),
    /// Run one maintenance tick (checkpoint, folds, index catch-up).
    Tick(AdminTickArgs),
    /// Run a mark-and-sweep garbage-collection pass.
    Gc(AdminGcArgs),
    /// Enable the gram content index and start its backfill.
    IndexEnable(AdminNamespaceArgs),
    /// Disable the gram content index.
    IndexDisable(AdminNamespaceArgs),
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
    /// Print the config file path.
    Path,
    /// Print the config file (secrets redacted).
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
    FilesystemGrep,
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
    AdminIndexEnable,
    AdminIndexDisable,
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
            CommandKind::FilesystemGrep => "filesystem_grep",
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
            CommandKind::AdminIndexEnable => "admin_index_enable",
            CommandKind::AdminIndexDisable => "admin_index_disable",
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
            Command::Grep(_) => CommandKind::FilesystemGrep,
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
                AdminCommand::IndexEnable(_) => CommandKind::AdminIndexEnable,
                AdminCommand::IndexDisable(_) => CommandKind::AdminIndexDisable,
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}
