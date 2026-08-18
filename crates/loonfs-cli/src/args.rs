//! The clap argument grammar for every `loonfs` command.

use crate::progress::ProgressMode;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
use loonfs_api::InodeId;
use std::io::IsTerminal;
use std::path::PathBuf;

fn parse_public_inode_id(value: &str) -> Result<InodeId, String> {
    loonfs_api::public_inode_id::decode(value).map_err(|error| error.to_string())
}

/// Parsed `loonfs` command line and its Clap command model.
#[derive(Debug, Parser)]
#[command(name = "loonfs", version)]
pub struct Cli {
    /// Config file to use, ahead of LOONFS_CONFIG and the default location.
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        value_hint = ValueHint::FilePath
    )]
    pub(crate) config: Option<PathBuf>,
    /// Emit machine-readable JSON instead of human output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
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
    if cli.json && matches!(&cli.command, Command::Ls(args) if args.jsonl) {
        return Err(Cli::command().error(
            clap::error::ErrorKind::ArgumentConflict,
            "the argument '--jsonl' cannot be used with '--json'",
        ));
    }
    Ok(())
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
    /// This sets the interactive default; concurrent automation should pass
    /// `--namespace` or set `LOONFS_NAMESPACE`.
    Use(NamespaceUseArgs),
    /// Show the active profile and its default namespace.
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
    Completions(CompletionsArgs),
    /// Print version and build metadata.
    Version,
}

#[derive(Debug, Args)]
pub(crate) struct CompletionsArgs {
    /// Shell to generate for.
    pub shell: clap_complete::Shell,
}

#[derive(Debug, Args)]
pub(crate) struct InitArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: Option<String>,
    /// Profile mode to configure.
    #[arg(long, value_name = "embedded|remote")]
    pub mode: Option<String>,
    /// Embedded object-store provider.
    #[arg(long)]
    pub store_kind: Option<String>,
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
    // Provider secrets fall back to the standard environment variables, but
    // not through clap's `env`: a value clap filled from the environment is
    // indistinguishable from one the caller typed, which turns an ambient
    // AWS key into a flag "passed" to a GCS profile. The fallback is applied
    // where the value is consumed instead — see `AmbientCredentials`.
    /// AWS or R2 access key id (env AWS_ACCESS_KEY_ID).
    #[arg(long)]
    pub access_key_id: Option<String>,
    /// AWS or R2 secret access key (env AWS_SECRET_ACCESS_KEY).
    #[arg(long)]
    pub secret_access_key: Option<String>,
    /// Custom provider endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional AWS session token (env AWS_SESSION_TOKEN).
    #[arg(long)]
    pub session_token: Option<String>,
    /// Use path-style S3 addressing.
    #[arg(long)]
    pub force_path_style: bool,
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
    /// Remote LoonFS bearer token (env LOONFS_AUTH_TOKEN).
    #[arg(long)]
    pub auth_token: Option<String>,
    /// PEM bundle of extra certificate authorities to trust for an
    /// https server URL, when a private CA issued the certificate.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub ca_cert_path: Option<String>,
    /// Actor kind to save in the profile. Must be used with --actor-id.
    /// Defaults to service/loonfs-cli when no actor is configured.
    #[arg(long, value_enum)]
    pub actor_kind: Option<ActorKindArg>,
    /// Actor ID to save in the profile. Must be used with --actor-kind.
    #[arg(long)]
    pub actor_id: Option<String>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProfileCommand {
    /// Add a profile to the config file.
    Create(ProfileCreateArgs),
    /// List configured profiles.
    List,
    /// Show one profile (secrets redacted).
    Show {
        #[arg(value_hint = ValueHint::Other)]
        name: Option<String>,
    },
    /// Update fields of an existing profile.
    Update(ProfileUpdateArgs),
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

#[derive(Debug, Args)]
pub(crate) struct ProfileCreateArgs {
    #[arg(value_hint = ValueHint::Other)]
    pub name: String,
    /// Profile mode to configure.
    #[arg(long, value_name = "embedded|remote")]
    pub mode: Option<String>,
    /// Embedded object-store provider.
    #[arg(long)]
    pub store_kind: Option<String>,
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
    // Same environment fallbacks as `InitArgs`, and for the same reason.
    /// AWS or R2 access key id (env AWS_ACCESS_KEY_ID).
    #[arg(long)]
    pub access_key_id: Option<String>,
    /// AWS or R2 secret access key (env AWS_SECRET_ACCESS_KEY).
    #[arg(long)]
    pub secret_access_key: Option<String>,
    /// Custom provider endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional AWS session token (env AWS_SESSION_TOKEN).
    #[arg(long)]
    pub session_token: Option<String>,
    /// Use path-style S3 addressing.
    #[arg(long)]
    pub force_path_style: bool,
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
    /// Remote LoonFS bearer token (env LOONFS_AUTH_TOKEN).
    #[arg(long)]
    pub auth_token: Option<String>,
    /// PEM bundle of extra certificate authorities to trust for an
    /// https server URL, when a private CA issued the certificate.
    #[arg(long, value_hint = ValueHint::FilePath)]
    pub ca_cert_path: Option<String>,
    /// Actor kind to save in the profile. Must be used with --actor-id.
    /// Defaults to service/loonfs-cli when no actor is configured.
    #[arg(long, value_enum)]
    pub actor_kind: Option<ActorKindArg>,
    /// Actor ID to save in the profile. Must be used with --actor-kind.
    #[arg(long)]
    pub actor_id: Option<String>,
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
    /// AWS or R2 access key id.
    #[arg(long)]
    pub access_key_id: Option<String>,
    /// AWS or R2 secret access key.
    #[arg(long)]
    pub secret_access_key: Option<String>,
    /// Custom provider endpoint URL.
    #[arg(long, value_hint = ValueHint::Url)]
    pub endpoint_url: Option<String>,
    /// Optional AWS session token.
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
    /// Remote LoonFS bearer token.
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
    /// Profile to run against (defaults to the configured default profile).
    #[arg(long, value_hint = ValueHint::Other)]
    pub profile: Option<String>,
    /// Disable bounded retry of `server_busy`, `commit_queue_full`,
    /// `shutting_down`, and transport errors.
    #[arg(long)]
    pub no_retry: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct TargetSelectorArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    /// Namespace to run against. Precedence is `--namespace`,
    /// `LOONFS_NAMESPACE`, then the profile default.
    #[arg(long, value_hint = ValueHint::Other)]
    pub namespace: Option<String>,
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
    #[arg(value_hint = ValueHint::Other)]
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
    #[arg(value_hint = ValueHint::Other)]
    pub namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct NamespaceDeleteArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
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
    #[arg(value_hint = ValueHint::Other)]
    pub source: String,
    #[arg(value_hint = ValueHint::Other)]
    pub new_namespace_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemLsArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    #[arg(value_hint = ValueHint::Other)]
    pub path: Option<String>,
    /// Stop after this many entries in total.
    #[arg(long, conflicts_with = "all")]
    pub limit: Option<u32>,
    /// Resume cursor from a previous bounded listing.
    #[arg(long, value_hint = ValueHint::Other)]
    pub cursor: Option<String>,
    /// Follow every page and print entries as pages arrive.
    #[arg(long)]
    pub all: bool,
    /// Print one JSON entry per line as pages arrive.
    #[arg(long)]
    pub jsonl: bool,
}

#[derive(Debug, Args)]
pub(crate) struct FilesystemStatArgs {
    #[command(flatten)]
    pub target: TargetSelectorArgs,
    /// Absolute path to describe.
    #[arg(
        required_unless_present = "inode",
        conflicts_with = "inode",
        value_hint = ValueHint::Other
    )]
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
    /// Maximum revisions to return in this page.
    #[arg(long)]
    pub limit: Option<u32>,
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
    /// Matches fetched per page, bounded by the deployment's
    /// `query.grep.max_limit`. To bound the total, use --max-matches.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Stop after this many matches in total. Without it the command
    /// follows cursors to completion and prints every match.
    #[arg(long)]
    pub max_matches: Option<u32>,
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
    /// Local destination (defaults to the remote basename; `-` streams to
    /// stdout). A large file is written as it arrives and never held whole,
    /// so what a get costs in memory does not follow what it downloads. A
    /// file destination is written beside itself and renamed into place only
    /// once the download is complete and its content verified, so a failed
    /// download leaves nothing there. Streaming to stdout hands bytes on as
    /// they arrive, so content that fails verification at the end exits
    /// nonzero after part of it has already been written — the exit status,
    /// not the output, is what says the content was verified.
    #[arg(value_hint = ValueHint::AnyPath)]
    pub local_destination: Option<String>,
    /// Download the directory tree rooted at `remote_path`, with bounded
    /// concurrency and per-file outcomes. The local destination is created
    /// if it does not exist.
    #[arg(short, long)]
    pub recursive: bool,
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
    /// Maximum number of entries to return.
    #[arg(long)]
    pub limit: Option<u32>,
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
    /// Maximum number of changes to return.
    #[arg(long)]
    pub limit: Option<u32>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AdminCommand {
    /// Pin the namespace's current state under a named checkpoint.
    Checkpoint(AdminCheckpointArgs),
    /// List active checkpoint pins in checkpoint-id order.
    CheckpointList(AdminCheckpointListArgs),
    /// Release a checkpoint pin.
    CheckpointRelease(AdminCheckpointReleaseArgs),
    /// Flush the WAL tail into a durable segment.
    Flush(AdminNamespaceArgs),
    /// Advance the retention floor, surrendering replay history below the
    /// flushed manifest head. File revision history is never affected.
    RetentionAdvance(AdminNamespaceArgs),
    /// Host maintenance in-process for assigned namespaces; this command is
    /// embedded-only.
    Run(AdminRunArgs),
    /// Run one core maintenance step (WAL flush and metadata folds).
    Step(AdminStepArgs),
    /// Run a mark-and-sweep garbage-collection pass.
    Gc(AdminGcArgs),
    /// Prove the profile's object store honours the contract LoonFS
    /// depends on, and report what it found check by check.
    StoreProbe(AdminStoreProbeArgs),
    /// Enable the gram content index and wait for its backfill to reach the
    /// sequence the namespace was at when this command started.
    IndexEnable(AdminIndexEnableArgs),
    /// Disable the gram content index.
    IndexDisable(AdminNamespaceArgs),
    /// Report where the gram content index is: disabled, backfilling, or
    /// active at a watermark.
    IndexStatus(AdminNamespaceArgs),
    /// Collect the namespace's unreferenced gram-index objects.
    IndexGc(AdminIndexGcArgs),
}

/// Floor on `--poll-interval-ms`. A re-assertion is a nudge per assigned
/// key and the runner answers each one by reading durable state, so a
/// cadence below this buys nothing and only spends provider requests.
const MIN_POLL_INTERVAL_MS: u64 = 100;

#[derive(Debug, Args)]
pub(crate) struct AdminRunArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    /// Namespace this host maintains. Repeat the flag to assign more; at
    /// least one is required, because this command never discovers
    /// namespaces.
    #[arg(
        long = "namespace",
        required = true,
        value_hint = ValueHint::Other
    )]
    pub namespaces: Vec<String>,
    /// Maintenance job to host. Repeat the flag to select more; all four
    /// when omitted. `core-gc` selects the runtime's collection job, which
    /// logs and settles under its own name, `gc`.
    #[arg(long = "job")]
    pub jobs: Vec<MaintenanceJobArg>,
    /// How often every assigned namespace is re-checked for work, in
    /// milliseconds. Defaults to 60000, and a drain ignores it because it
    /// never rests between keys.
    #[arg(long, value_parser = clap::value_parser!(u64).range(MIN_POLL_INTERVAL_MS..))]
    pub poll_interval_ms: Option<u64>,
    /// Catch every assigned namespace up and exit, instead of hosting until
    /// a signal.
    #[arg(long)]
    pub drain: bool,
    /// Give up after this many steps across the whole drain. Exits nonzero
    /// and reports where every key got to. Requires --drain.
    #[arg(long, requires = "drain")]
    pub max_steps: Option<u64>,
    /// Give up after this many milliseconds across the whole drain. Exits
    /// nonzero and reports where every key got to. Requires --drain.
    #[arg(long, requires = "drain")]
    pub deadline_ms: Option<u64>,
}

/// The maintenance jobs `admin run` can host, as an operator names them.
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
    /// Maximum checkpoints to return. Supplying this requests one page.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Resume cursor from a previous page. Supplying this requests one page.
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
    /// Advance the retention floor after the step's flush work. Replay
    /// history below the flushed manifest head is surrendered.
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
    Init,
    ProfileCreate,
    ProfileList,
    ProfileShow,
    ProfileUpdate,
    ProfileDelete,
    ProfileUse,
    NamespaceCreate,
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
    AdminCheckpoint,
    AdminCheckpointList,
    AdminCheckpointRelease,
    AdminFlush,
    AdminRetentionAdvance,
    AdminRun,
    AdminStep,
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
            CommandKind::Init => "init",
            CommandKind::ProfileCreate => "profile_create",
            CommandKind::ProfileList => "profile_list",
            CommandKind::ProfileShow => "profile_show",
            CommandKind::ProfileUpdate => "profile_update",
            CommandKind::ProfileDelete => "profile_delete",
            CommandKind::ProfileUse => "profile_use",
            CommandKind::NamespaceCreate => "namespace_create",
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
            CommandKind::AdminCheckpoint => "admin_checkpoint",
            CommandKind::AdminCheckpointList => "admin_checkpoint_list",
            CommandKind::AdminCheckpointRelease => "admin_checkpoint_release",
            CommandKind::AdminFlush => "admin_flush",
            CommandKind::AdminRetentionAdvance => "admin_retention_advance",
            CommandKind::AdminRun => "admin_run",
            CommandKind::AdminStep => "admin_step",
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
        !matches!(self, CommandKind::FilesystemCat)
    }
}

impl Cli {
    pub(crate) fn kind(&self) -> Option<CommandKind> {
        Some(match &self.command {
            Command::Completions(_) => return None,
            Command::Init(_) => CommandKind::Init,
            Command::Profile { command } => match command {
                ProfileCommand::Create(_) => CommandKind::ProfileCreate,
                ProfileCommand::List => CommandKind::ProfileList,
                ProfileCommand::Show { .. } => CommandKind::ProfileShow,
                ProfileCommand::Update(_) => CommandKind::ProfileUpdate,
                ProfileCommand::Delete { .. } => CommandKind::ProfileDelete,
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
            Command::Admin { command } => match command {
                AdminCommand::Checkpoint(_) => CommandKind::AdminCheckpoint,
                AdminCommand::CheckpointList(_) => CommandKind::AdminCheckpointList,
                AdminCommand::CheckpointRelease(_) => CommandKind::AdminCheckpointRelease,
                AdminCommand::Flush(_) => CommandKind::AdminFlush,
                AdminCommand::RetentionAdvance(_) => CommandKind::AdminRetentionAdvance,
                AdminCommand::Run(_) => CommandKind::AdminRun,
                AdminCommand::Step(_) => CommandKind::AdminStep,
                AdminCommand::Gc(_) => CommandKind::AdminGc,
                AdminCommand::StoreProbe(_) => CommandKind::AdminStoreProbe,
                AdminCommand::IndexEnable(_) => CommandKind::AdminIndexEnable,
                AdminCommand::IndexDisable(_) => CommandKind::AdminIndexDisable,
                AdminCommand::IndexStatus(_) => CommandKind::AdminIndexStatus,
                AdminCommand::IndexGc(_) => CommandKind::AdminIndexGc,
            },
            Command::Config { command } => match command {
                ConfigCommand::Path => CommandKind::ConfigPath,
                ConfigCommand::Show => CommandKind::ConfigShow,
            },
            Command::Version => CommandKind::Version,
        })
    }
}
