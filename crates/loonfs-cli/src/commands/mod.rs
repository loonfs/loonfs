//! Command implementations, one submodule per command group.

mod access;
mod config;
mod context;
pub(crate) use context::shell_quote;
pub(crate) mod download;
mod fs;
mod inspection;
mod maintenance;
mod namespace;
mod output;
mod pagination;
mod partial;
mod profile;
mod profile_config;
mod recursive;
mod snapshot;
mod tree_failures;
pub(crate) use tree_failures::TreeTransferFailures;

pub(crate) use self::output::{
    CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus, ListingHeadDrift,
    MaintenanceRan, TrashListing,
};

use crate::args::{AccessCommand, Cli, Command, CompletionArgs, RuntimeBehavior, TopLevelCommand};
use crate::config::resolve_config_location;
use crate::error::CliError;
use crate::render::OutputFormat;
use clap::CommandFactory;
use clap_complete::Shell;
use context::{fail, Invocation};
use std::path::Path;

const COMPLETION_SHELL_REQUIRED: &str =
    "pass `--shell` with one of: bash, zsh, fish, powershell, elvish (`$SHELL` is unset or unsupported)";

pub(crate) async fn run(
    cli: Cli,
    behavior: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let kind = cli.kind();
    if behavior.output == OutputFormat::Json && !kind.supports_json() {
        return Err(fail(kind, None, None, CliError::json_not_supported()));
    }
    let selectors = cli.selectors();

    let command = match cli.command {
        TopLevelCommand::Completion(args) => return run_completion(kind, &args),
        TopLevelCommand::Doctor(args) => {
            return inspection::run_doctor(
                kind,
                cli.config.as_deref(),
                &selectors,
                behavior.transient_retry,
                &args,
            )
            .await
        }
        TopLevelCommand::Configured(command) => command,
    };
    let config = resolve_config_location(cli.config.as_deref())
        .map_err(|error| fail(kind, None, None, error))?;
    let invocation = Invocation {
        kind,
        config,
        behavior,
        selectors,
    };
    let invocation = &invocation;
    match command {
        Command::Version => Ok(invocation.output(CommandData::Version {
            version: env!("CARGO_PKG_VERSION").to_owned(),
        })),
        Command::Init => config::run_config_init(invocation),
        Command::Config { command } => config::run_config_command(invocation, command),
        Command::Profile { command } => profile::run_profile_command(invocation, command),
        Command::Namespace { command } => {
            namespace::run_namespace_command(invocation, command).await
        }
        Command::Snapshot { command } => snapshot::run_snapshot_command(invocation, command).await,
        Command::Use(args) => namespace::run_namespace_use(invocation, args).await,
        Command::Current => namespace::run_namespace_current(invocation),
        Command::Ls(args) => fs::run_filesystem_ls(invocation, args).await,
        Command::Stat(args) => fs::run_filesystem_stat(invocation, args).await,
        Command::Annotate(args) => fs::run_filesystem_annotate(invocation, args).await,
        Command::Access { command } => match command {
            AccessCommand::Set(args) => access::run_access_set(invocation, args).await,
        },
        Command::Cat(args) => fs::run_filesystem_cat(invocation, args).await,
        Command::Grep(args) => fs::run_filesystem_grep(invocation, args).await,
        Command::Get(args) => fs::run_filesystem_get(invocation, args).await,
        Command::Put(args) => fs::run_filesystem_put(invocation, args).await,
        Command::Append(args) => fs::run_filesystem_append(invocation, args).await,
        Command::Revisions(args) => fs::run_filesystem_revisions(invocation, args).await,
        Command::Restore(args) => fs::run_filesystem_restore(invocation, args).await,
        Command::Undelete(args) => fs::run_filesystem_undelete(invocation, args).await,
        Command::Mkdir(args) => fs::run_filesystem_mkdir(invocation, args).await,
        Command::Rm(args) => fs::run_filesystem_rm(invocation, args).await,
        Command::Mv(args) => fs::run_filesystem_mv(invocation, args).await,
        Command::Cp(args) => fs::run_filesystem_cp(invocation, args).await,
        Command::Trash(args) => fs::run_filesystem_trash(invocation, args).await,
        Command::Changes(args) => maintenance::run_changes(invocation, args).await,
        Command::Capabilities => inspection::run_capabilities(invocation).await,
        Command::Maintenance { command } => {
            maintenance::run_maintenance_command(invocation, command).await
        }
    }
}

fn run_completion(
    kind: crate::args::CommandKind,
    args: &CompletionArgs,
) -> Result<CommandOutput, CommandFailure> {
    let shell = args
        .shell
        .or_else(completion_shell_from_environment)
        .ok_or_else(|| {
            fail(
                kind,
                None,
                None,
                CliError::invalid_request(COMPLETION_SHELL_REQUIRED).with_param("--shell"),
            )
        })?;
    let mut script = Vec::new();
    clap_complete::generate(shell, &mut Cli::command(), "loonfs", &mut script);
    Ok(CommandOutput {
        kind,
        profile: None,
        mode: None,
        data: CommandData::CompletionScript(script),
    })
}

fn completion_shell_from_environment() -> Option<Shell> {
    let shell = std::env::var_os("SHELL")?;
    match Path::new(&shell).file_name()?.to_str()? {
        "bash" => Some(Shell::Bash),
        "zsh" => Some(Shell::Zsh),
        "fish" => Some(Shell::Fish),
        "pwsh" | "powershell" => Some(Shell::PowerShell),
        "elvish" => Some(Shell::Elvish),
        _ => None,
    }
}
