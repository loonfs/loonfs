//! The top-level grammar: the global flags, the command tree, and the
//! runtime behavior and selectors read from them.

use super::filesystem::{
    AccessCommand, ChangesArgs, FilesystemAnnotateArgs, FilesystemAppendArgs, FilesystemCatArgs,
    FilesystemGetArgs, FilesystemGrepArgs, FilesystemLsArgs, FilesystemMkdirArgs,
    FilesystemPutArgs, FilesystemRestoreArgs, FilesystemRevisionsArgs, FilesystemRmArgs,
    FilesystemStatArgs, FilesystemTransferArgs, FilesystemUndeleteArgs, TrashArgs,
};
use super::flags::PageLimitArgs;
use super::maintenance::{MaintenanceCheckpointCommand, MaintenanceCommand};
use super::namespace::{NamespaceCommand, NamespaceUseArgs, SnapshotCommand};
use super::profile::ProfileCommand;
use crate::progress::ProgressMode;
use crate::render::OutputFormat;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueHint};
use loonfs_types::{NamespaceId, SubjectId};
use std::io::IsTerminal;
use std::path::PathBuf;

const TOP_LEVEL_HELP_TEMPLATE: &str = "\
{before-help}{about-with-newline}
{usage-heading} {usage}

Filesystem:
  ls            List a directory
  cat           Print a file's content to stdout
  grep          Search file content through the grep index
  get           Download a file or directory tree
  put           Upload a file or directory tree
  restore       Restore a prior file revision
  undelete      Recover a deleted file or directory
  mkdir         Create a directory
  rm            Delete a file or directory
  mv            Move or rename a path
  cp            Copy a file or directory tree
  annotate      Write and remove attributes
  access        Replace an item's access row

Context and configuration:
  init          Interactively create a config and first profile
  profile       Manage connection profiles
  namespace     Manage namespaces
  snapshot      Manage point-in-time snapshots
  use           Set a profile's default namespace
  current       Show the selected profile and namespace
  config        Inspect the CLI config file

Inspection:
  stat          Describe one visible path or inode
  revisions     List a file's revision history
  trash         List recoverable deletions
  changes       List committed changes
  maintenance   Run maintenance operations
  capabilities  Show the selected deployment's protocol capabilities
  doctor        Check the selected deployment without writing to it
  completion    Print a shell completion script
  version       Print version and build metadata

Options:
{options}{after-help}\
";

#[derive(Debug, Parser)]
#[command(name = "loonfs", version, about = "A durable filesystem built on object storage.", help_template = TOP_LEVEL_HELP_TEMPLATE)]
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
    pub(crate) namespace: Option<NamespaceId>,
    /// Subject ID to act as. Precedence is `--subject-id`,
    /// `LOONFS_SUBJECT_ID`, the profile, then the actor ID.
    #[arg(long, global = true, value_hint = ValueHint::Other)]
    pub(crate) subject_id: Option<SubjectId>,
    /// Principal scope for an ACL namespace or a subject's principal IDs.
    /// Subject precedence is the flag, `LOONFS_PRINCIPAL_SCOPE`, then the profile.
    #[arg(long, global = true, value_hint = ValueHint::Other)]
    pub(crate) principal_scope: Option<String>,
    /// Comma-separated principal IDs whose grants apply. Precedence is
    /// `--principals`, `LOONFS_PRINCIPALS`, then the profile. Without
    /// principals the CLI acts as the token holder.
    #[arg(long, global = true, value_hint = ValueHint::Other)]
    pub(crate) principals: Option<String>,
    /// Emit machine-readable JSON instead of human output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
    /// Disable bounded retry of `server_busy`, `commit_queue_full`,
    /// `shutting_down`, and transport errors.
    #[arg(long, global = true)]
    pub(crate) no_retry: bool,
    /// Never prompt. Fail instead when input would be required.
    #[arg(long, global = true)]
    pub(crate) no_input: bool,
    /// Don't show progress while a transfer runs.
    #[arg(long, global = true)]
    pub(crate) no_progress: bool,
    #[command(subcommand)]
    pub(crate) command: TopLevelCommand,
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
            "`--all` cannot be used with `--json`; use `--json --limit <n>` for a bounded document or `--jsonl` to stream all results",
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

/// A top-level command, split by whether it runs before the config file's
/// location is resolved.
#[derive(Debug, Subcommand)]
pub(crate) enum TopLevelCommand {
    #[command(flatten)]
    Configured(Command),
    /// Check the selected deployment without writing to it.
    Doctor(DoctorArgs),
    /// Print a shell completion script to stdout.
    Completion(CompletionArgs),
}

impl TopLevelCommand {
    fn accepts_profile_selector(&self) -> bool {
        match self {
            Self::Configured(command) => command.accepts_profile_selector(),
            Self::Doctor(_) => true,
            Self::Completion(_) => false,
        }
    }

    fn accepts_namespace_selector(&self) -> bool {
        match self {
            Self::Configured(command) => command.accepts_namespace_selector(),
            Self::Doctor(_) => true,
            Self::Completion(_) => false,
        }
    }

    pub(crate) fn pagination(&self) -> Option<&PageLimitArgs> {
        match self {
            Self::Configured(command) => command.pagination(),
            Self::Doctor(_) | Self::Completion(_) => None,
        }
    }
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
    /// Create, list, extend, or delete point-in-time snapshots.
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    /// Set the default namespace for a profile.
    /// This sets the interactive default. Automation that runs concurrently
    /// should pass `--namespace` or set `LOONFS_NAMESPACE` instead.
    Use(NamespaceUseArgs),
    /// Show the selected profile and namespace.
    Current,
    /// List a directory.
    Ls(FilesystemLsArgs),
    /// Describe one visible path or inode (kind, size, revision, content digest, attributes).
    Stat(FilesystemStatArgs),
    /// Write and remove attributes on a file or directory.
    Annotate(FilesystemAnnotateArgs),
    /// Replace an item's access row.
    Access {
        #[command(subcommand)]
        command: AccessCommand,
    },
    /// Print a file's content to stdout.
    Cat(FilesystemCatArgs),
    /// Search file content through the grep index.
    Grep(FilesystemGrepArgs),
    /// Download a file (or directory tree with -r) to a local path.
    Get(FilesystemGetArgs),
    /// Upload a local file (or directory tree with -r) to a namespace path.
    Put(FilesystemPutArgs),
    /// Add a local file's bytes, or standard input's, to the end of a file.
    Append(FilesystemAppendArgs),
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
    Capabilities,
    /// Maintenance operations: metadata, the checkpoint inventory, retention, GC, indexes, and the store.
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommand,
    },
    /// Inspect the CLI config file.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print version and build metadata.
    Version,
}

impl Command {
    fn accepts_profile_selector(&self) -> bool {
        !matches!(
            self,
            Self::Init | Self::Profile { .. } | Self::Config { .. } | Self::Version
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
                | Self::Access { .. }
                | Self::Cat(_)
                | Self::Grep(_)
                | Self::Get(_)
                | Self::Put(_)
                | Self::Append(_)
                | Self::Revisions(_)
                | Self::Restore(_)
                | Self::Undelete(_)
                | Self::Mkdir(_)
                | Self::Rm(_)
                | Self::Mv(_)
                | Self::Cp(_)
                | Self::Trash(_)
                | Self::Changes(_)
                | Self::Maintenance {
                    command: MaintenanceCommand::Checkpoint { .. }
                        | MaintenanceCommand::Index { .. }
                        | MaintenanceCommand::Metadata(_)
                        | MaintenanceCommand::Fold
                        | MaintenanceCommand::Retention { .. }
                        | MaintenanceCommand::Gc(_)
                        | MaintenanceCommand::RecoverAdministrator(_),
                }
        )
    }

    fn pagination(&self) -> Option<&PageLimitArgs> {
        match self {
            Self::Ls(args) => Some(&args.pagination.page_limits),
            Self::Grep(args) => Some(&args.pagination.page_limits),
            Self::Revisions(args) => Some(&args.pagination.page_limits),
            Self::Trash(args) => Some(&args.pagination.page_limits),
            Self::Changes(args) => Some(&args.pagination.page_limits),
            Self::Snapshot {
                command: SnapshotCommand::List(args),
            } => Some(&args.pagination.page_limits),
            Self::Maintenance {
                command:
                    MaintenanceCommand::Checkpoint {
                        command: MaintenanceCheckpointCommand::List(args),
                    },
            } => Some(&args.pagination.page_limits),
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

#[derive(Debug, Args)]
pub(crate) struct DoctorArgs {
    /// Also test the object store by writing and deleting temporary objects.
    #[arg(long)]
    pub write_check: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommand {
    /// Print the config file path.
    Path,
    /// Print the config file (secrets redacted).
    Show,
}

/// Whether the client retries a transient failure, which `--no-retry`
/// turns off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransientRetry {
    /// Retry the transient failures of a request that is safe to resend.
    Retry,
    /// Send every request once.
    NoRetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeBehavior {
    pub output: OutputFormat,
    pub interactive: bool,
    /// How a long-running transfer reports its progress.
    pub progress: ProgressMode,
    pub transient_retry: TransientRetry,
}

/// The selector flags of one invocation, which every command reads from the
/// top-level command line.
#[derive(Debug, Clone, Default)]
pub(crate) struct Selectors {
    pub profile: Option<String>,
    pub namespace: Option<NamespaceId>,
    pub subject: SubjectSelectors,
}

/// The flags that select the subject a command acts as.
#[derive(Debug, Clone, Default)]
pub(crate) struct SubjectSelectors {
    pub subject_id: Option<SubjectId>,
    pub principal_scope: Option<String>,
    pub principals: Option<String>,
}

impl Cli {
    pub(crate) fn selectors(&self) -> Selectors {
        Selectors {
            profile: self.profile.clone(),
            namespace: self.namespace.clone(),
            subject: SubjectSelectors {
                subject_id: self.subject_id.clone(),
                principal_scope: self.principal_scope.clone(),
                principals: self.principals.clone(),
            },
        }
    }
}

impl RuntimeBehavior {
    pub(crate) fn detect(cli: &Cli) -> Self {
        let interactive = !cli.json
            && !cli.no_input
            && std::io::stdin().is_terminal()
            && std::io::stderr().is_terminal();
        Self {
            output: if cli.json {
                OutputFormat::Json
            } else {
                OutputFormat::Human
            },
            interactive,
            // Progress asks standard error alone, not the terminal pair
            // `interactive` needs: a `put` fed by a pipe still has a
            // terminal to draw on, and `--no-input` says nothing about
            // whether anyone is watching.
            progress: ProgressMode::detect(cli.no_progress, cli.json),
            transient_retry: if cli.no_retry {
                TransientRetry::NoRetry
            } else {
                TransientRetry::Retry
            },
        }
    }
}
