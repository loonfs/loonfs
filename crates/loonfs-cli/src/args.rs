//! The clap argument grammar for every `loonfs` command.

use crate::progress::ProgressMode;
use clap::{ArgGroup, Args, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
use loonfs_api::InodeId;
use std::io::IsTerminal;
use std::path::PathBuf;

fn parse_public_inode_id(value: &str) -> Result<InodeId, String> {
    loonfs_api::public_inode_id::decode(value).map_err(|error| error.to_string())
}

const TOP_LEVEL_HELP_TEMPLATE: &str = "\
{before-help}{about-with-newline}
{usage-heading} {usage}

Filesystem:
  ls          List a directory
  cat         Print a file's content to stdout
  grep        Search file content through the grep index
  get         Download a file or directory tree
  put         Upload a file or directory tree
  restore     Restore a prior file revision
  undelete    Recover a deleted file or directory
  mkdir       Create a directory
  rm          Delete a file or directory
  mv          Move or rename a path
  cp          Copy a file or directory tree
  annotate    Write and remove attributes

Context and configuration:
  init        Interactively create a config and first profile
  profile     Manage connection profiles
  namespace   Manage namespaces
  use         Set a profile's default namespace
  current     Show the selected profile and namespace
  config      Inspect the CLI config file

Inspection:
  stat        Describe one visible path or inode
  revisions   List a file's revision history
  trash       List recoverable deletions
  changes     List committed changes
  capabilities  Show the selected deployment's protocol capabilities
  doctor      Check the selected deployment without writing to it
  completion  Print a shell completion script
  version     Print version and build metadata

Administration:
  admin       Run administrative operations

Options:
{options}{after-help}\
";

/// Defines the `loonfs` command-line interface.
#[derive(Debug, Parser)]
#[command(name = "loonfs", version, help_template = TOP_LEVEL_HELP_TEMPLATE)]
pub(crate) struct Cli {
    /// Config file to use, ahead of LOONFS_CONFIG and the default location.
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        value_hint = ValueHint::FilePath
    )]
    pub(crate) config: Option<PathBuf>,
    /// Profile to run against. Precedence is `--profile`, `LOONFS_PROFILE`,
    /// then the configured default profile.
    #[arg(long, global = true, value_hint = ValueHint::Other)]
    pub(crate) profile: Option<String>,
    /// Namespace to run against. Precedence is `--namespace`,
    /// `LOONFS_NAMESPACE`, then the profile default.
    #[arg(long, global = true, value_hint = ValueHint::Other)]
    pub(crate) namespace: Option<String>,
    /// Emit machine-readable JSON instead of human output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
    /// Disable bounded retry of `server_busy`, `commit_queue_full`,
    /// `shutting_down`, and transport errors.
    #[arg(long, global = true)]
    pub(crate) no_retry: bool,
    /// Never prompt; fail instead when input would be required.
    #[arg(long, global = true)]
    pub(crate) no_input: bool,
    /// Say nothing about a transfer while it runs.
    #[arg(long, global = true)]
    pub(crate) no_progress: bool,
    #[command(subcommand)]
    pub(crate) command: Command,
}

pub(crate) fn validate_cli(cli: &Cli) -> Result<(), clap::Error> {
    if cli.profile.is_some() && !cli.command.accepts_profile_selector() {
        return Err(selector_not_accepted("--profile"));
    }
    if cli.namespace.is_some() && !cli.command.accepts_namespace_selector() {
        return Err(selector_not_accepted("--namespace"));
    }
    let Some(pagination) = cli.command.pagination() else {
        return Ok(());
    };
    if cli.json && pagination.jsonl {
        let mut error = Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "the argument '--jsonl' cannot be used with '--json'",
        );
        error.insert(
            clap::error::ContextKind::InvalidArg,
            clap::error::ContextValue::String("--jsonl".to_owned()),
        );
        return Err(error);
    }
    if cli.json && pagination.all {
        let mut error = Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "--all cannot be used with --json; use --json --limit <n> for a bounded document or --jsonl to stream all results",
        );
        error.insert(
            clap::error::ContextKind::InvalidArg,
            clap::error::ContextValue::String("--all".to_owned()),
        );
        return Err(error);
    }
    Ok(())
}

fn selector_not_accepted(selector: &str) -> clap::Error {
    let mut error = Cli::command().error(
        clap::error::ErrorKind::ArgumentConflict,
        format!("the argument '{selector}' cannot be used with this command"),
    );
    error.insert(
        clap::error::ContextKind::InvalidArg,
        clap::error::ContextValue::String(selector.to_owned()),
    );
    error
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Interactively create the config file and a first profile.
    Init,
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
    /// This sets the interactive default; concurrent automation should pass
    /// `--namespace` or set `LOONFS_NAMESPACE`.
    Use(NamespaceUseArgs),
    /// Show the selected profile and namespace.
    Current(CurrentArgs),
    /// List a directory.
    Ls(FilesystemLsArgs),
    /// Describe one visible path or inode (kind, size, revision, content digest, attributes).
    Stat(FilesystemStatArgs),
    /// Write and remove attributes on a file or directory.
    Annotate(FilesystemAnnotateArgs),
    /// Print a file's content to stdout.
    Cat(FilesystemCatArgs),
    /// Search file content through the grep index.
    Grep(FilesystemGrepArgs),
    /// Download a file (or directory tree with -r) to a local path.
    Get(FilesystemGetArgs),
    /// Upload a local file (or directory tree with -r) to a namespace path.
    Put(FilesystemPutArgs),
    /// List a file's revision history, newest first.
    Revisions(FilesystemRevisionsArgs),
    /// Write a prior revision's content as the file's next revision.
    Restore(FilesystemRestoreArgs),
    /// Recover a deleted file or directory at a destination path.
    Undelete(FilesystemUndeleteArgs),
    /// Create a directory.
    Mkdir(FilesystemMkdirArgs),
    /// Delete a file or directory.
    Rm(FilesystemRmArgs),
    /// Move or rename a path.
    Mv(FilesystemTransferArgs),
    /// Copy a file (or directory tree with -r) to another path.
    Cp(FilesystemTransferArgs),
    /// List recoverable deletions: what was deleted, when, and the exact
    /// handle `undelete` needs.
    Trash(TrashArgs),
    /// List committed changes after a sequence number.
    Changes(ChangesArgs),
    /// Show the selected deployment's protocol capabilities.
    Capabilities(CapabilitiesArgs),
    /// Check the selected deployment without writing to it.
    Doctor(DoctorArgs),
    /// Maintenance operations: checkpoints, steps, retention, GC, indexes.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
    /// Inspect the CLI config file.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print a shell completion script to stdout.
    Completion(CompletionArgs),
    /// Print version and build metadata.
    Version,
}

impl Command {
    fn accepts_profile_selector(&self) -> bool {
        !matches!(
            self,
            Self::Init
                | Self::Profile { .. }
                | Self::Config { .. }
                | Self::Completion(_)
                | Self::Version
        )
    }

    fn accepts_namespace_selector(&self) -> bool {
        matches!(
            self,
            Self::Namespace {
                command: NamespaceCommand::Show(_),
            } | Self::Ls(_)
                | Self::Stat(_)
                | Self::Annotate(_)
                | Self::Cat(_)
                | Self::Grep(_)
                | Self::Get(_)
                | Self::Put(_)
                | Self::Revisions(_)
                | Self::Restore(_)
                | Self::Undelete(_)
                | Self::Mkdir(_)
                | Self::Rm(_)
                | Self::Mv(_)
                | Self::Cp(_)
                | Self::Trash(_)
                | Self::Changes(_)
                | Self::Doctor(_)
                | Self::Admin {
                    command: AdminCommand::Checkpoint { .. }
                        | AdminCommand::Index { .. }
                        | AdminCommand::Maintenance {
                            command: AdminMaintenanceCommand::Step(_)
                                | AdminMaintenanceCommand::Flush(_),
                        }
                        | AdminCommand::Retention { .. }
                        | AdminCommand::Gc(_),
                }
        )
    }

    fn pagination(&self) -> Option<&PaginationArgs> {
        match self {
            Self::Ls(args) => Some(&args.pagination),
            Self::Grep(args) => Some(&args.pagination),
            Self::Revisions(args) => Some(&args.pagination),
            Self::Trash(args) => Some(&args.pagination),
            Self::Changes(args) => Some(&args.pagination),
            Self::Admin {
                command:
                    AdminCommand::Checkpoint {
                        command: AdminCheckpointCommand::List(args),
                    },
            } => Some(&args.pagination),
            _ => None,
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct CompletionArgs {
    /// Shell to generate completions for.
    #[arg(long, value_enum, value_name = "bash|zsh|fish|powershell|elvish")]
    pub(crate) shell: Option<clap_complete::Shell>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// Add a profile to the config file.
    Create {
        #[command(subcommand)]
        provider: Box<ProfileCreateCommand>,
    },
    /// List configured profiles.
    List,
    /// Show one profile (secrets redacted).
    Show {
        #[arg(value_hint = ValueHint::Other)]
        name: Option<String>,
    },
    /// Update fields of an existing profile.
    Update(Box<ProfileUpdateArgs>),
    /// Delete a profile from the config file.
    Delete {
        #[arg(value_hint = ValueHint::Other)]
        name: String,
    },
    /// Make a profile the default.
    Use {
        #[arg(value_hint = ValueHint::Other)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCreateCommand {
    /// Create an AWS S3-backed embedded profile.
    S3(ProfileCreateS3Args),
    /// Create a Cloudflare R2-backed embedded profile.
    R2(ProfileCreateR2Args),
    /// Create a Google Cloud Storage-backed embedded profile.
    Gcs(ProfileCreateGcsArgs),
    /// Create an Azure Blob Storage-backed embedded profile.
    Azure(ProfileCreateAzureArgs),
    /// Create a local-filesystem-backed embedded profile.
    Local(ProfileCreateLocalArgs),
    /// Create a remote-server profile.
    Remote(ProfileCreateRemoteArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateActorArgs {
    /// Actor kind to save in the profile. Must be used with --actor-id.
    /// Defaults to service/loonfs-cli when no actor is configured.
    #[arg(long, value_enum)]
    pub actor_kind: Option<ActorKindArg>,
    /// Actor ID to save in the profile. Must be used with --actor-kind.
    #[arg(long, value_hint = ValueHint::Other)]
    pub actor_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateS3Args {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// S3 bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// AWS region.
    #[arg(long, value_hint = ValueHint::Other)]
    pub region: Option<String>,
    /// AWS credential source.
    #[arg(long, value_name = "ambient|static", value_hint = ValueHint::Other)]
    pub credential_source: Option<String>,
    /// Static access key id.
    #[arg(long, value_hint = ValueHint::Other)]
    pub access_key_id: Option<String>,
    /// Static secret access key.
    #[arg(long, value_hint = ValueHint::Other)]
    pub secret_access_key: Option<String>,
    /// Custom S3 endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional static session token.
    #[arg(long, value_hint = ValueHint::Other)]
    pub session_token: Option<String>,
    /// Use path-style S3 addressing.
    #[arg(long)]
    pub force_path_style: bool,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateR2Args {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// R2 bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// Cloudflare account id.
    #[arg(long, value_hint = ValueHint::Other)]
    pub account_id: Option<String>,
    /// R2 endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// R2 credential source.
    #[arg(long, value_name = "ambient|static", value_hint = ValueHint::Other)]
    pub credential_source: Option<String>,
    /// Static access key id.
    #[arg(long, value_hint = ValueHint::Other)]
    pub access_key_id: Option<String>,
    /// Static secret access key.
    #[arg(long, value_hint = ValueHint::Other)]
    pub secret_access_key: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateGcsArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// GCS bucket name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub bucket: Option<String>,
    /// Path to a GCP service-account key.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub service_account_key_path: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateAzureArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// Azure storage account name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub account_name: Option<String>,
    /// Azure blob container name.
    #[arg(long, value_hint = ValueHint::Other)]
    pub container_name: Option<String>,
    /// Azure storage access key.
    #[arg(long, value_hint = ValueHint::Other)]
    pub access_key: Option<String>,
    /// Custom Azure endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateLocalArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// Local filesystem store root.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub root: Option<String>,
    /// Optional object-key prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub key_prefix: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateRemoteArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// Remote LoonFS server URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub server_url: Option<String>,
    /// Remote bearer token to store; omitted profiles use LOONFS_AUTH_TOKEN at request time.
    #[arg(long, value_hint = ValueHint::Other)]
    pub auth_token: Option<String>,
    /// PEM bundle of extra certificate authorities to trust.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub ca_cert_path: Option<String>,
    #[command(flatten)]
    pub actor: ProfileCreateActorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct ProfileUpdateArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// Local filesystem store root.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub root: Option<String>,
    /// Optional object-key prefix within the provider.
    #[arg(long)]
    pub key_prefix: Option<String>,
    /// S3, R2, or GCS bucket name.
    #[arg(long)]
    pub bucket: Option<String>,
    /// AWS region.
    #[arg(long)]
    pub region: Option<String>,
    /// AWS or R2 credential source.
    #[arg(long, value_name = "ambient|static")]
    pub credential_source: Option<String>,
    /// AWS or R2 static access key id.
    #[arg(long)]
    pub access_key_id: Option<String>,
    /// AWS or R2 static secret access key.
    #[arg(long)]
    pub secret_access_key: Option<String>,
    /// Custom provider endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional static AWS session token.
    #[arg(long)]
    pub session_token: Option<String>,
    /// Cloudflare R2 account id.
    #[arg(long)]
    pub account_id: Option<String>,
    /// Azure storage account name.
    #[arg(long)]
    pub account_name: Option<String>,
    /// Azure blob container name.
    #[arg(long)]
    pub container_name: Option<String>,
    /// Azure storage access key.
    #[arg(long)]
    pub access_key: Option<String>,
    /// Path to a GCP service-account key.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub service_account_key_path: Option<String>,
    /// Remote LoonFS server URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub server_url: Option<String>,
    /// Remote bearer token to store; an empty value clears the stored token.
    #[arg(long)]
    pub auth_token: Option<String>,
    /// PEM bundle of extra certificate authorities to trust for an
    /// https server URL, when a private CA issued the certificate.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub ca_cert_path: Option<String>,
    /// Sets the profile's actor kind. Must be used with --actor-id.
    #[arg(long, value_enum)]
    pub actor_kind: Option<ActorKindArg>,
    /// Sets the profile's actor ID. Must be used with --actor-kind.
    #[arg(long)]
    pub actor_id: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct ProfileSelectorArgs {
    #[arg(from_global)]
    pub profile: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct RequestBehaviorArgs {
    #[arg(from_global)]
    pub no_retry: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct TargetSelectorArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[arg(from_global)]
    pub namespace: Option<String>,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct ActorSelectorArgs {
    /// Actor kind for this mutation. Must be used with `--actor-id`.
    /// Overrides actor values from the environment or profile.
    #[arg(long, value_enum)]
    pub actor_kind: Option<ActorKindArg>,
    /// Actor ID for this mutation. Must be used with `--actor-kind`.
    /// If no actor is configured, the CLI uses service/loonfs-cli.
    #[arg(long)]
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ActorKindArg {
    User,
    Service,
    System,
}

impl From<ActorKindArg> for loonfs_api::ActorKind {
    fn from(value: ActorKindArg) -> Self {
        match value {
            ActorKindArg::User => Self::User,
            ActorKindArg::Service => Self::Service,
            ActorKindArg::System => Self::System,
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceUseArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct CurrentArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct CapabilitiesArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
}

#[derive(Debug, Args)]
pub(crate) struct DoctorArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Also test the object store by writing and deleting temporary objects.
    #[arg(long)]
    pub write_check: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespaceCommand {
    /// Create a new empty namespace.
    Create(NamespaceCreateArgs),
    /// Show a namespace's current status.
    Show(NamespaceShowArgs),
    /// Permanently delete a namespace and retire its id.
    Delete(NamespaceDeleteArgs),
    /// Fork a namespace into a new one; O(1), no bytes copied.
    Fork(NamespaceForkArgs),
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceShowArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Namespace to show; selection defaults apply when omitted.
    #[arg(value_hint = ValueHint::Other, conflicts_with = "namespace")]
    pub namespace_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceCreateArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceDeleteArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
    #[arg(value_hint = ValueHint::Other)]
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
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub source: String,
    #[arg(value_hint = ValueHint::Other)]
    pub new_namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct PaginationArgs {
    /// Return at most this many items.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Request this many items per page.
    #[arg(long)]
    pub page_size: Option<u32>,
    /// Continue until the limit is reached or no pages remain.
    #[arg(long)]
    pub all: bool,
    /// Write one JSON item per line until the limit is reached or no pages remain.
    #[arg(long)]
    pub jsonl: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemLsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Resume from a cursor returned by a previous listing.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
#[command(
    override_usage = "loonfs stat <PATH>\n       loonfs stat --inode <INODE_ID>",
    group(
        ArgGroup::new("stat_target")
            .required(true)
            .multiple(false)
            .args(["path", "inode"])
    )
)]
pub(crate) struct FilesystemStatArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Absolute path to describe.
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    /// Visible inode to describe instead of a path.
    #[arg(long, value_name = "INODE_ID", value_parser = parse_public_inode_id)]
    pub inode: Option<InodeId>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemAnnotateArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Attribute to write, as `key=value`. The key ends at the first `=`, so
    /// the value may contain more of them. Repeat the flag to write more.
    #[arg(long = "set", conflicts_with = "attributes_json")]
    pub sets: Vec<String>,
    /// Attribute key to remove. Repeat the flag to remove more.
    #[arg(long = "remove", conflicts_with = "attributes_json")]
    pub removes: Vec<String>,
    /// The whole update as one JSON object, `{"set": {...}, "remove": [...]}`,
    /// with values as strings, for example `{"set": {"owner": "ada"}}`.
    /// This is how a script passes an update it built. Cannot be combined with
    /// --set or --remove. Named for what it carries because --json is already
    /// the global output-format flag.
    #[arg(long)]
    pub attributes_json: Option<String>,
    /// Update only while the path still resolves to this inode; a raced
    /// rebinding fails instead of annotating a different inode.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub expected_inode_id: Option<InodeId>,
    /// Update only while the inode's attribute revision is still this one.
    #[arg(long)]
    pub expected_attributes_revision: Option<u64>,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRmArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Delete a directory and everything under it, as one commit. The whole
    /// subtree stays recoverable through the printed undelete handle.
    #[arg(short, long)]
    pub recursive: bool,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemMkdirArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Create missing parent directories as well.
    #[arg(short = 'p', long)]
    pub parents: bool,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRevisionsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Resume cursor from a previous revisions page.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemCatArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    /// Print this revision instead of the current content.
    #[arg(long)]
    pub revision: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemGrepArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Pattern in the Rust regex dialect; `^`/`$` anchor lines.
    #[arg(value_hint = ValueHint::Other)]
    pub pattern: String,
    /// Restrict matches to files under this absolute path prefix.
    #[arg(long, value_hint = ValueHint::Other)]
    pub path_prefix: Option<String>,
    /// Match ASCII letters without regard to case.
    #[arg(short = 'i', long)]
    pub ignore_case: bool,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Resume from a cursor returned by a previous search.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
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
    #[arg(value_hint = ValueHint::Other)]
    pub remote_path: String,
    /// Local destination (defaults to the remote basename). For one file,
    /// `-` streams to stdout. A large file is written as it arrives and never
    /// held whole, so what a get costs in memory does not follow what it
    /// downloads. A file destination is written beside itself and renamed
    /// into place only once the download is complete and its content verified,
    /// so a failed download leaves nothing there. Streaming to stdout hands
    /// bytes on as they arrive, so content that fails verification at the end
    /// exits nonzero after part of it has already been written — the exit
    /// status, not the output, is what says the content was verified.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_destination: Option<String>,
    /// Download the directory tree into the exact local destination root.
    /// `loonfs get -r /reports ./download` writes the contents directly under
    /// `./download`, never `./download/reports`. With the destination omitted,
    /// `loonfs get -r /reports` derives `./reports`. Reruns are no-clobber by
    /// default; `--force` rewrites existing files.
    #[arg(short, long)]
    pub recursive: bool,
    /// Download this revision instead of the current content.
    #[arg(long)]
    pub revision: Option<u64>,
    /// Overwrite existing local files. Recursive reruns are no-clobber by
    /// default.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemPutArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Local file to upload, or `-` to read standard input. A large file
    /// and a pipe are both read once and never held whole, so what a put
    /// costs in memory does not follow what it uploads. Reading `-` needs
    /// an explicit remote path.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_path: String,
    #[arg(value_hint = ValueHint::Other)]
    pub remote_path: Option<String>,
    /// Upload the directory tree rooted at `local_path`. Every file lands
    /// as its own commit with bounded concurrency, so progress is per file
    /// and a partial failure reruns per file.
    #[arg(short, long)]
    pub recursive: bool,
    /// Replace the remote destination if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Replace only while the file's current revision is still this one
    /// (implies --force); a raced write fails instead of stacking on it.
    #[arg(long)]
    pub expected_revision: Option<u64>,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemTransferArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub source_path: String,
    #[arg(value_hint = ValueHint::Other)]
    pub destination_path: String,
    /// Copy the directory tree rooted at `source_path` (cp only; mv moves
    /// a directory in one commit without -r).
    #[arg(short, long)]
    pub recursive: bool,
    /// Replace the destination if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemRestoreArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: String,
    #[arg(long)]
    pub revision: u64,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemUndeleteArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// Destination path for the recovered file or directory. Omit to
    /// restore in place: the entry re-binds under the parent and name its
    /// deletion recorded, which lands correctly even when the enclosing
    /// directories were renamed since. A deletion that recorded no binding
    /// needs the explicit path.
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    /// Inode ID of the deleted item, as reported by `rm` and the change
    /// feed.
    #[arg(long, value_parser = parse_public_inode_id)]
    pub inode: InodeId,
    /// Committed sequence of the delete being recovered, as reported by
    /// `rm` and the change feed. Scopes recovery to that exact deletion,
    /// so a stale command cannot cancel a later delete.
    #[arg(long)]
    pub deletion_seq: u64,
    /// Annotation recorded on the commit and shown by `loonfs changes`. Part
    /// of the commit's identity: resubmitting the same --commit-id with a
    /// different message is a commit id conflict.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Idempotency key for the commit; resubmit with the same id to retry
    /// safely. Generated when absent and returned in the output.
    #[arg(long, value_hint = ValueHint::Other)]
    pub commit_id: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct TrashArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Resume cursor from a previous page.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ChangesArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Return committed changes after this sequence (defaults to 0, the
    /// start of retained history).
    #[arg(long)]
    pub after: Option<u64>,
    #[command(flatten)]
    pub pagination: PaginationArgs,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminCommand {
    /// Create, list, or release checkpoint pins.
    Checkpoint {
        #[command(subcommand)]
        command: AdminCheckpointCommand,
    },
    /// Inspect and manage the gram content index.
    Index {
        #[command(subcommand)]
        command: AdminIndexCommand,
    },
    /// Run or directly trigger maintenance work.
    Maintenance {
        #[command(subcommand)]
        command: AdminMaintenanceCommand,
    },
    /// Manage retained replay history.
    Retention {
        #[command(subcommand)]
        command: AdminRetentionCommand,
    },
    /// Run a mark-and-sweep garbage-collection pass.
    Gc(AdminGcArgs),
    /// Inspect and manage the profile's object store.
    Store {
        #[command(subcommand)]
        command: AdminStoreCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminCheckpointCommand {
    /// Pin the namespace's current state under a named checkpoint.
    Create(AdminCheckpointArgs),
    /// List active checkpoint pins in checkpoint-id order.
    List(AdminCheckpointListArgs),
    /// Release a checkpoint pin.
    Release(AdminCheckpointReleaseArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminIndexCommand {
    /// Enable the gram content index and wait for its backfill to reach the
    /// sequence the namespace was at when this command started.
    Enable(AdminIndexEnableArgs),
    /// Disable the gram content index.
    Disable(AdminNamespaceArgs),
    /// Show whether the gram content index is disabled, backfilling, or active.
    Status(AdminNamespaceArgs),
    /// Collect the namespace's unreferenced gram-index objects.
    Gc(AdminIndexGcArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminMaintenanceCommand {
    /// Run maintenance for selected namespaces. Requires an embedded profile.
    Run(AdminRunArgs),
    /// Run one metadata maintenance step.
    Step(AdminStepArgs),
    /// Flush the WAL tail into a durable segment.
    Flush(AdminNamespaceArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminRetentionCommand {
    /// Advance the retention floor and discard older replay history.
    /// This does not remove file revisions.
    Advance(AdminNamespaceArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminStoreCommand {
    /// Test the object-store operations LoonFS requires.
    Probe(AdminStoreProbeArgs),
}

/// Minimum `--poll-interval-ms`. Each poll reads durable state for every
/// assigned key, so shorter intervals add provider requests without improving
/// scheduling precision.
const MIN_POLL_INTERVAL_MS: u64 = 100;

#[derive(Debug, Args)]
pub(crate) struct AdminRunArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
    /// Namespace to maintain. Repeat the flag to select more than one.
    #[arg(long = "namespaces", required = true, value_hint = ValueHint::Other)]
    pub namespaces: Vec<String>,
    /// Maintenance job to run. Repeat the flag to select more than one.
    /// Omitting it selects all four jobs. `core-gc` selects the job reported
    /// internally as `gc`.
    #[arg(long = "job")]
    pub jobs: Vec<MaintenanceJobArg>,
    /// Interval between checks for assigned namespaces, in milliseconds.
    /// Defaults to 60000. Drains ignore this setting.
    #[arg(long, value_parser = clap::value_parser!(u64).range(MIN_POLL_INTERVAL_MS..))]
    pub poll_interval_ms: Option<u64>,
    /// Complete the current assignments and exit.
    #[arg(long)]
    pub drain: bool,
    /// Stop the drain after this many total steps. Requires `--drain`.
    #[arg(long, requires = "drain")]
    pub max_steps: Option<u64>,
    /// Stop the drain after this many milliseconds. Requires `--drain`.
    #[arg(long, requires = "drain")]
    pub deadline_ms: Option<u64>,
}

/// Jobs accepted by `admin maintenance run --job`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum MaintenanceJobArg {
    /// Flush the WAL tail past its threshold and fold one reorganization
    /// unit per step.
    Metadata,
    /// Run one bounded mark-and-sweep collection pass per step.
    CoreGc,
    /// Build and fold the gram content index.
    GrepIndex,
    /// Reclaim one namespace's unreferenced grep objects per step.
    GrepGc,
}

#[derive(Debug, Args)]
pub(crate) struct AdminIndexEnableArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Return as soon as the index is enabled, without waiting for the
    /// backfill.
    #[arg(long)]
    pub no_wait: bool,
    /// Give up after this many steps: one bounded index step where the
    /// profile is embedded, one status check where it is remote. Exits
    /// nonzero and reports how far the index got.
    #[arg(long)]
    pub max_steps: Option<u64>,
    /// Give up after this many milliseconds. Exits nonzero and reports how
    /// far the index got.
    #[arg(long)]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Args)]
pub(crate) struct AdminIndexGcArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Spend at most this many reads and return after one bounded pass.
    /// Omit to loop bounded passes through completion.
    #[arg(long)]
    pub max_objects: Option<u64>,
    /// Resume from `next_cursor` returned by a previous pass.
    #[arg(long, value_name = "TOKEN", value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
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
pub(crate) struct AdminCheckpointListArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[command(flatten)]
    pub pagination: PaginationArgs,
    /// Resume from a cursor returned by a previous listing.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct AdminCheckpointReleaseArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Checkpoint id to release.
    #[arg(value_hint = ValueHint::Other)]
    pub checkpoint_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct AdminStepArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Flush the visible WAL tail into metadata tables when it reaches this many
    /// segments (server default when omitted).
    #[arg(long)]
    pub max_wal_tail_segments: Option<u64>,
    /// Advance the retention floor after the step's flush work. This discards
    /// replay history below the flushed manifest head.
    #[arg(long)]
    pub retention: bool,
    /// Run a garbage-collection pass after the step's flush work.
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
    /// Examine at most this many candidates and return after one bounded
    /// pass. Omit to loop bounded passes through completion.
    #[arg(long)]
    pub max_objects: Option<u64>,
    /// Resume token from a previous pass's next_cursor; only valid for the
    /// same namespace.
    #[arg(
        long,
        value_name = "TOKEN",
        value_hint = ValueHint::Other
    )]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct AdminStoreProbeArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub request: RequestBehaviorArgs,
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
    /// How a transfer that takes human time says where it has got to.
    pub progress: ProgressMode,
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
            // Progress asks standard error alone, not the terminal pair
            // `interactive` needs: a `put` fed by a pipe still has a
            // terminal to draw on, and `--no-input` says nothing about
            // whether anyone is watching.
            progress: ProgressMode::detect(cli.no_progress, cli.json),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandKind {
    Completion,
    Init,
    ProfileCreate,
    ProfileList,
    ProfileShow,
    ProfileUpdate,
    ProfileDelete,
    ProfileUse,
    NamespaceCreate,
    NamespaceShow,
    NamespaceDelete,
    NamespaceFork,
    NamespaceUse,
    Current,
    FilesystemLs,
    FilesystemStat,
    FilesystemAnnotate,
    FilesystemCat,
    FilesystemGrep,
    FilesystemGet,
    FilesystemPut,
    FilesystemRevisions,
    FilesystemTrash,
    FilesystemRestore,
    FilesystemUndelete,
    FilesystemMkdir,
    FilesystemRm,
    FilesystemMv,
    FilesystemCp,
    Changes,
    Capabilities,
    Doctor,
    AdminCheckpointCreate,
    AdminCheckpointList,
    AdminCheckpointRelease,
    AdminMaintenanceFlush,
    AdminRetentionAdvance,
    AdminMaintenanceRun,
    AdminMaintenanceStep,
    AdminGc,
    AdminStoreProbe,
    AdminIndexEnable,
    AdminIndexDisable,
    AdminIndexStatus,
    AdminIndexGc,
    ConfigPath,
    ConfigShow,
    Version,
}

impl CommandKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            CommandKind::Completion => "completion",
            CommandKind::Init => "init",
            CommandKind::ProfileCreate => "profile_create",
            CommandKind::ProfileList => "profile_list",
            CommandKind::ProfileShow => "profile_show",
            CommandKind::ProfileUpdate => "profile_update",
            CommandKind::ProfileDelete => "profile_delete",
            CommandKind::ProfileUse => "profile_use",
            CommandKind::NamespaceCreate => "namespace_create",
            CommandKind::NamespaceShow => "namespace_show",
            CommandKind::NamespaceDelete => "namespace_delete",
            CommandKind::NamespaceFork => "namespace_fork",
            CommandKind::NamespaceUse => "namespace_use",
            CommandKind::Current => "current",
            CommandKind::FilesystemLs => "filesystem_ls",
            CommandKind::FilesystemStat => "filesystem_stat",
            CommandKind::FilesystemAnnotate => "filesystem_annotate",
            CommandKind::FilesystemCat => "filesystem_cat",
            CommandKind::FilesystemGrep => "filesystem_grep",
            CommandKind::FilesystemGet => "filesystem_get",
            CommandKind::FilesystemPut => "filesystem_put",
            CommandKind::FilesystemRevisions => "filesystem_revisions",
            CommandKind::FilesystemTrash => "filesystem_trash",
            CommandKind::FilesystemRestore => "filesystem_restore",
            CommandKind::FilesystemUndelete => "filesystem_undelete",
            CommandKind::FilesystemMkdir => "filesystem_mkdir",
            CommandKind::FilesystemRm => "filesystem_rm",
            CommandKind::FilesystemMv => "filesystem_mv",
            CommandKind::FilesystemCp => "filesystem_cp",
            CommandKind::Changes => "changes",
            CommandKind::Capabilities => "capabilities",
            CommandKind::Doctor => "doctor",
            CommandKind::AdminCheckpointCreate => "admin_checkpoint_create",
            CommandKind::AdminCheckpointList => "admin_checkpoint_list",
            CommandKind::AdminCheckpointRelease => "admin_checkpoint_release",
            CommandKind::AdminMaintenanceFlush => "admin_maintenance_flush",
            CommandKind::AdminRetentionAdvance => "admin_retention_advance",
            CommandKind::AdminMaintenanceRun => "admin_maintenance_run",
            CommandKind::AdminMaintenanceStep => "admin_maintenance_step",
            CommandKind::AdminGc => "admin_gc",
            CommandKind::AdminStoreProbe => "admin_store_probe",
            CommandKind::AdminIndexEnable => "admin_index_enable",
            CommandKind::AdminIndexDisable => "admin_index_disable",
            CommandKind::AdminIndexStatus => "admin_index_status",
            CommandKind::AdminIndexGc => "admin_index_gc",
            CommandKind::ConfigPath => "config_path",
            CommandKind::ConfigShow => "config_show",
            CommandKind::Version => "version",
        }
    }

    pub(crate) fn supports_json(self) -> bool {
        !matches!(self, CommandKind::Completion | CommandKind::FilesystemCat)
    }
}

impl Cli {
    pub(crate) fn kind(&self) -> CommandKind {
        match &self.command {
            Command::Completion(_) => CommandKind::Completion,
            Command::Init => CommandKind::Init,
            Command::Profile { command } => match command {
                ProfileCommand::Create { .. } => CommandKind::ProfileCreate,
                ProfileCommand::List => CommandKind::ProfileList,
                ProfileCommand::Show { .. } => CommandKind::ProfileShow,
                ProfileCommand::Update(_) => CommandKind::ProfileUpdate,
                ProfileCommand::Delete { .. } => CommandKind::ProfileDelete,
                ProfileCommand::Use { .. } => CommandKind::ProfileUse,
            },
            Command::Namespace { command } => match command {
                NamespaceCommand::Create(_) => CommandKind::NamespaceCreate,
                NamespaceCommand::Show(_) => CommandKind::NamespaceShow,
                NamespaceCommand::Delete(_) => CommandKind::NamespaceDelete,
                NamespaceCommand::Fork(_) => CommandKind::NamespaceFork,
            },
            Command::Use(_) => CommandKind::NamespaceUse,
            Command::Current(_) => CommandKind::Current,
            Command::Ls(_) => CommandKind::FilesystemLs,
            Command::Stat(_) => CommandKind::FilesystemStat,
            Command::Annotate(_) => CommandKind::FilesystemAnnotate,
            Command::Cat(_) => CommandKind::FilesystemCat,
            Command::Grep(_) => CommandKind::FilesystemGrep,
            Command::Get(_) => CommandKind::FilesystemGet,
            Command::Put(_) => CommandKind::FilesystemPut,
            Command::Revisions(_) => CommandKind::FilesystemRevisions,
            Command::Trash(_) => CommandKind::FilesystemTrash,
            Command::Restore(_) => CommandKind::FilesystemRestore,
            Command::Undelete(_) => CommandKind::FilesystemUndelete,
            Command::Mkdir(_) => CommandKind::FilesystemMkdir,
            Command::Rm(_) => CommandKind::FilesystemRm,
            Command::Mv(_) => CommandKind::FilesystemMv,
            Command::Cp(_) => CommandKind::FilesystemCp,
            Command::Changes(_) => CommandKind::Changes,
            Command::Capabilities(_) => CommandKind::Capabilities,
            Command::Doctor(_) => CommandKind::Doctor,
            Command::Admin { command } => match command {
                AdminCommand::Checkpoint { command } => match command {
                    AdminCheckpointCommand::Create(_) => CommandKind::AdminCheckpointCreate,
                    AdminCheckpointCommand::List(_) => CommandKind::AdminCheckpointList,
                    AdminCheckpointCommand::Release(_) => CommandKind::AdminCheckpointRelease,
                },
                AdminCommand::Index { command } => match command {
                    AdminIndexCommand::Enable(_) => CommandKind::AdminIndexEnable,
                    AdminIndexCommand::Disable(_) => CommandKind::AdminIndexDisable,
                    AdminIndexCommand::Status(_) => CommandKind::AdminIndexStatus,
                    AdminIndexCommand::Gc(_) => CommandKind::AdminIndexGc,
                },
                AdminCommand::Maintenance { command } => match command {
                    AdminMaintenanceCommand::Run(_) => CommandKind::AdminMaintenanceRun,
                    AdminMaintenanceCommand::Step(_) => CommandKind::AdminMaintenanceStep,
                    AdminMaintenanceCommand::Flush(_) => CommandKind::AdminMaintenanceFlush,
                },
                AdminCommand::Retention { command } => match command {
                    AdminRetentionCommand::Advance(_) => CommandKind::AdminRetentionAdvance,
                },
                AdminCommand::Gc(_) => CommandKind::AdminGc,
                AdminCommand::Store { command } => match command {
                    AdminStoreCommand::Probe(_) => CommandKind::AdminStoreProbe,
                },
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_selectors_reach_leaf_arguments_from_every_position() {
        let before_command = Cli::try_parse_from([
            "loonfs",
            "--profile",
            "prod",
            "--namespace",
            "demo",
            "ls",
            "/",
        ])
        .expect("selectors before command");
        assert_selected_target(&before_command, "prod", "demo");

        let between_subcommands = Cli::try_parse_from([
            "loonfs",
            "admin",
            "--profile",
            "prod",
            "checkpoint",
            "--namespace",
            "demo",
            "list",
        ])
        .expect("selectors between subcommands");
        assert_selected_target(&between_subcommands, "prod", "demo");

        let after_leaf_arguments = Cli::try_parse_from([
            "loonfs",
            "ls",
            "/",
            "--profile",
            "prod",
            "--namespace",
            "demo",
        ])
        .expect("selectors after leaf arguments");
        assert_selected_target(&after_leaf_arguments, "prod", "demo");
    }

    #[test]
    fn unused_global_selectors_are_rejected() {
        let cases: &[(&[&str], &str)] = &[
            (&["loonfs", "--namespace", "demo", "version"], "--namespace"),
            (
                &["loonfs", "--profile", "prod", "config", "path"],
                "--profile",
            ),
            (
                &["loonfs", "--namespace", "demo", "namespace", "create", "x"],
                "--namespace",
            ),
            (&["loonfs", "use", "x", "--namespace", "y"], "--namespace"),
        ];

        for (arguments, selector) in cases {
            let cli = Cli::try_parse_from(*arguments).expect("global selector parses once");
            let error = validate_cli(&cli).expect_err("unused selector must fail");
            assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
            assert!(error.to_string().contains(selector), "{error}");
        }
    }

    #[test]
    fn stat_help_and_parser_show_both_exclusive_target_forms() {
        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("stat")
            .expect("stat command")
            .render_long_help()
            .to_string();
        assert!(help.contains("loonfs stat <PATH>"), "{help}");
        assert!(help.contains("loonfs stat --inode <INODE_ID>"), "{help}");

        let missing = Cli::try_parse_from(["loonfs", "stat"]).expect_err("target is required");
        assert_eq!(
            missing.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );

        let both = Cli::try_parse_from(["loonfs", "stat", "/doc", "--inode", "ino_2"])
            .expect_err("targets conflict");
        assert_eq!(both.kind(), clap::error::ErrorKind::ArgumentConflict);

        Cli::try_parse_from(["loonfs", "stat", "/doc"]).expect("path target");
        Cli::try_parse_from(["loonfs", "stat", "--inode", "ino_2"]).expect("inode target");
    }

    #[test]
    fn recursive_get_help_names_the_exact_destination_root() {
        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("get")
            .expect("get command")
            .render_long_help()
            .to_string()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        assert!(help.contains("loonfs get -r /reports ./download"), "{help}");
        assert!(help.contains("never `./download/reports`"), "{help}");
        assert!(
            help.contains("loonfs get -r /reports` derives `./reports"),
            "{help}"
        );
        assert!(help.contains("Reruns are no-clobber by default"), "{help}");
        assert!(help.contains("--force` rewrites existing files"), "{help}");
    }

    fn assert_selected_target(cli: &Cli, profile: &str, namespace: &str) {
        assert_eq!(cli.profile.as_deref(), Some(profile));
        assert_eq!(cli.namespace.as_deref(), Some(namespace));
        let target = match &cli.command {
            Command::Ls(args) => &args.target,
            Command::Admin {
                command:
                    AdminCommand::Checkpoint {
                        command: AdminCheckpointCommand::List(args),
                    },
            } => &args.target,
            command => panic!("expected command with selected target, got {command:?}"),
        };
        assert_eq!(target.profile.profile.as_deref(), Some(profile));
        assert_eq!(target.namespace.as_deref(), Some(namespace));
    }

    #[test]
    fn every_paginated_listing_accepts_the_shared_flags() {
        let cases: &[&[&str]] = &[
            &[
                "loonfs",
                "ls",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--cursor",
                "c",
            ],
            &[
                "loonfs",
                "revisions",
                "/f",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--cursor",
                "c",
            ],
            &[
                "loonfs",
                "trash",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--cursor",
                "c",
            ],
            &[
                "loonfs",
                "changes",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--after",
                "1",
            ],
            &[
                "loonfs",
                "grep",
                "x",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--cursor",
                "c",
            ],
            &[
                "loonfs",
                "admin",
                "checkpoint",
                "list",
                "--limit",
                "5",
                "--page-size",
                "2",
                "--all",
                "--jsonl",
                "--cursor",
                "c",
            ],
        ];
        for arguments in cases {
            let cli = Cli::try_parse_from(*arguments).expect("pagination arguments parse");
            let pagination = cli.command.pagination().expect("paginated command");
            assert_eq!(pagination.limit, Some(5));
            assert_eq!(pagination.page_size, Some(2));
            assert!(pagination.all);
            assert!(pagination.jsonl);
        }
    }

    #[test]
    fn all_json_is_rejected_for_every_paginated_listing() {
        let cases: &[&[&str]] = &[
            &["loonfs", "--json", "ls", "--all", "--limit", "5"],
            &[
                "loonfs",
                "--json",
                "revisions",
                "/f",
                "--all",
                "--limit",
                "5",
            ],
            &["loonfs", "--json", "trash", "--all", "--limit", "5"],
            &["loonfs", "--json", "changes", "--all", "--limit", "5"],
            &["loonfs", "--json", "grep", "x", "--all", "--limit", "5"],
            &[
                "loonfs",
                "--json",
                "admin",
                "checkpoint",
                "list",
                "--all",
                "--limit",
                "5",
            ],
        ];
        for arguments in cases {
            let cli = Cli::try_parse_from(*arguments).expect("arguments parse");
            let error = validate_cli(&cli).expect_err("--json --all must fail");
            assert!(error.to_string().contains(
                "--all cannot be used with --json; use --json --limit <n> for a bounded document or --jsonl to stream all results"
            ));
        }
    }

    #[test]
    fn max_matches_is_removed_and_changes_uses_after_to_resume() {
        assert!(Cli::try_parse_from(["loonfs", "grep", "x", "--max-matches", "1"]).is_err());
        assert!(Cli::try_parse_from(["loonfs", "changes", "--cursor", "opaque"]).is_err());
    }

    #[test]
    fn index_gc_accepts_the_same_cursor_flag_as_core_gc() {
        let cli = Cli::try_parse_from([
            "loonfs",
            "admin",
            "index",
            "gc",
            "--max-objects",
            "7",
            "--cursor",
            "resume",
        ])
        .expect("index gc arguments");
        assert!(matches!(
            &cli.command,
            Command::Admin {
                command: AdminCommand::Index {
                    command: AdminIndexCommand::Gc(_),
                },
            }
        ));
        if let Command::Admin {
            command:
                AdminCommand::Index {
                    command: AdminIndexCommand::Gc(args),
                },
        } = &cli.command
        {
            assert_eq!(args.max_objects, Some(7));
            assert_eq!(args.cursor.as_deref(), Some("resume"));
        }
    }

    #[test]
    fn command_kind_envelope_values_are_pinned() {
        let cases = [
            (CommandKind::Completion, "completion"),
            (CommandKind::Init, "init"),
            (CommandKind::ProfileCreate, "profile_create"),
            (CommandKind::ProfileList, "profile_list"),
            (CommandKind::ProfileShow, "profile_show"),
            (CommandKind::ProfileUpdate, "profile_update"),
            (CommandKind::ProfileDelete, "profile_delete"),
            (CommandKind::ProfileUse, "profile_use"),
            (CommandKind::NamespaceCreate, "namespace_create"),
            (CommandKind::NamespaceShow, "namespace_show"),
            (CommandKind::NamespaceDelete, "namespace_delete"),
            (CommandKind::NamespaceFork, "namespace_fork"),
            (CommandKind::NamespaceUse, "namespace_use"),
            (CommandKind::Current, "current"),
            (CommandKind::FilesystemLs, "filesystem_ls"),
            (CommandKind::FilesystemStat, "filesystem_stat"),
            (CommandKind::FilesystemAnnotate, "filesystem_annotate"),
            (CommandKind::FilesystemCat, "filesystem_cat"),
            (CommandKind::FilesystemGrep, "filesystem_grep"),
            (CommandKind::FilesystemGet, "filesystem_get"),
            (CommandKind::FilesystemPut, "filesystem_put"),
            (CommandKind::FilesystemRevisions, "filesystem_revisions"),
            (CommandKind::FilesystemTrash, "filesystem_trash"),
            (CommandKind::FilesystemRestore, "filesystem_restore"),
            (CommandKind::FilesystemUndelete, "filesystem_undelete"),
            (CommandKind::FilesystemMkdir, "filesystem_mkdir"),
            (CommandKind::FilesystemRm, "filesystem_rm"),
            (CommandKind::FilesystemMv, "filesystem_mv"),
            (CommandKind::FilesystemCp, "filesystem_cp"),
            (CommandKind::Changes, "changes"),
            (CommandKind::Capabilities, "capabilities"),
            (CommandKind::Doctor, "doctor"),
            (
                CommandKind::AdminCheckpointCreate,
                "admin_checkpoint_create",
            ),
            (CommandKind::AdminCheckpointList, "admin_checkpoint_list"),
            (
                CommandKind::AdminCheckpointRelease,
                "admin_checkpoint_release",
            ),
            (
                CommandKind::AdminMaintenanceFlush,
                "admin_maintenance_flush",
            ),
            (
                CommandKind::AdminRetentionAdvance,
                "admin_retention_advance",
            ),
            (CommandKind::AdminMaintenanceRun, "admin_maintenance_run"),
            (CommandKind::AdminMaintenanceStep, "admin_maintenance_step"),
            (CommandKind::AdminGc, "admin_gc"),
            (CommandKind::AdminStoreProbe, "admin_store_probe"),
            (CommandKind::AdminIndexEnable, "admin_index_enable"),
            (CommandKind::AdminIndexDisable, "admin_index_disable"),
            (CommandKind::AdminIndexStatus, "admin_index_status"),
            (CommandKind::AdminIndexGc, "admin_index_gc"),
            (CommandKind::ConfigPath, "config_path"),
            (CommandKind::ConfigShow, "config_show"),
            (CommandKind::Version, "version"),
        ];

        for (kind, expected) in cases {
            assert_eq!(kind.as_str(), expected);
        }
    }

    #[test]
    fn management_command_model_uses_noun_families() {
        let command = Cli::command();
        let profile_create = subcommand(subcommand(&command, "profile"), "create");
        assert_subcommands(
            profile_create,
            &["s3", "r2", "gcs", "azure", "local", "remote"],
        );

        let namespace = subcommand(&command, "namespace");
        assert_subcommands(namespace, &["create", "show", "delete", "fork"]);

        let admin = subcommand(&command, "admin");
        assert_subcommands(
            admin,
            &[
                "checkpoint",
                "index",
                "maintenance",
                "retention",
                "gc",
                "store",
            ],
        );
        assert_subcommands(
            subcommand(admin, "checkpoint"),
            &["create", "list", "release"],
        );
        assert_subcommands(
            subcommand(admin, "index"),
            &["enable", "disable", "status", "gc"],
        );
        assert_subcommands(subcommand(admin, "maintenance"), &["run", "step", "flush"]);
        assert_subcommands(subcommand(admin, "retention"), &["advance"]);
        assert_subcommands(subcommand(admin, "store"), &["probe"]);
    }

    #[test]
    fn provider_create_commands_only_expose_applicable_flags() {
        let command = Cli::command();
        let create = subcommand(subcommand(&command, "profile"), "create");
        let s3 = subcommand(create, "s3");
        assert!(has_argument(s3, "credential_source"));
        assert!(has_argument(s3, "session_token"));
        assert!(!has_argument(s3, "account_id"));

        let r2 = subcommand(create, "r2");
        assert!(has_argument(r2, "credential_source"));
        assert!(has_argument(r2, "account_id"));
        assert!(!has_argument(r2, "region"));

        let local = subcommand(create, "local");
        assert!(has_argument(local, "root"));
        assert!(!has_argument(local, "bucket"));

        let remote = subcommand(create, "remote");
        assert!(has_argument(remote, "server_url"));
        assert!(!has_argument(remote, "key_prefix"));

        let init = subcommand(&command, "init");
        assert!(!has_argument(init, "mode"));
        assert!(!has_argument(init, "store_kind"));
    }

    #[test]
    fn local_and_remote_paths_have_completion_hints() {
        let command = Cli::command();

        assert_hint(&command, "config", ValueHint::FilePath);
        assert_hint(&command, "profile", ValueHint::Other);
        assert_hint(&command, "namespace", ValueHint::Other);

        let put = subcommand(&command, "put");
        assert_hint(put, "local_path", ValueHint::AnyPath);
        assert_hint(put, "remote_path", ValueHint::Other);

        let get = subcommand(&command, "get");
        assert_hint(get, "local_destination", ValueHint::AnyPath);
        assert_hint(get, "remote_path", ValueHint::Other);

        let cat = subcommand(&command, "cat");
        assert_hint(cat, "path", ValueHint::Other);

        let mv = subcommand(&command, "mv");
        assert_hint(mv, "source_path", ValueHint::Other);
        assert_hint(mv, "destination_path", ValueHint::Other);

        let profile_create = subcommand(subcommand(&command, "profile"), "create");
        let local = subcommand(profile_create, "local");
        assert_hint(local, "name", ValueHint::Other);
        assert_hint(local, "root", ValueHint::DirPath);
        let gcs = subcommand(profile_create, "gcs");
        assert_hint(gcs, "service_account_key_path", ValueHint::FilePath);
        let namespace_show = subcommand(subcommand(&command, "namespace"), "show");
        assert_hint(namespace_show, "namespace_id", ValueHint::Other);
    }

    fn subcommand<'a>(command: &'a clap::Command, name: &str) -> &'a clap::Command {
        command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == name)
            .expect("subcommand exists")
    }

    fn assert_hint(command: &clap::Command, id: &str, expected: ValueHint) {
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_id() == id)
            .expect("argument exists");
        assert_eq!(argument.get_value_hint(), expected);
    }

    fn has_argument(command: &clap::Command, id: &str) -> bool {
        command
            .get_arguments()
            .any(|argument| argument.get_id() == id)
    }

    fn assert_subcommands(command: &clap::Command, expected: &[&str]) {
        let actual = command
            .get_subcommands()
            .filter(|subcommand| subcommand.get_name() != "help")
            .map(clap::Command::get_name)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
