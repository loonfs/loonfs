//! Command implementations, one submodule per command group.

mod admin;
mod config;
mod context;
mod fs;
mod namespace;
mod output;
mod partial;
mod profile;
mod profile_config;
mod recursive;

pub(crate) use self::output::{
    CommandData, CommandFailure, CommandOutput, ListingHeadDrift, MaintenanceKeyReport,
};

use crate::args::{Cli, Command, CommandKind, CompletionArgs, RuntimeBehavior};
use crate::config::resolve_config_location;
use crate::error::CliError;
use clap::CommandFactory;
use clap_complete::Shell;
use std::path::Path;

const COMPLETION_SHELL_REQUIRED: &str =
    "pass `--shell` with one of: bash, zsh, fish, powershell, elvish (`$SHELL` is unset or unsupported)";

pub(crate) async fn run(
    cli: Cli,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let kind = cli.kind();
    if runtime.json && !kind.supports_json() {
        return Err(CommandFailure {
            kind,
            profile: None,
            mode: None,
            error: Box::new(CliError::json_not_supported()),
        });
    }

    if let Command::Completion(args) = &cli.command {
        return run_completion(kind, args);
    }

    // One resolution for the whole invocation: every command below reads
    // and writes the same file, whichever way it was named.
    let location = resolve_config_location(cli.config.as_deref())
        .map_err(|error| context::fail(kind, None, None, error))?;
    let config_path = location.path.as_path();

    match cli.command {
        // Completion returns before config resolution above. This twin keeps
        // dispatch exhaustive and deliberately uses the same output path.
        Command::Completion(args) => run_completion(kind, &args),
        Command::Version => Ok(CommandOutput {
            kind,
            profile: None,
            mode: None,
            data: CommandData::Version {
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
        }),
        Command::Init(args) => config::run_config_init(kind, config_path, args, runtime),
        Command::Config { command } => config::run_config_command(kind, &location, command),
        Command::Profile { command } => {
            profile::run_profile_command(kind, config_path, command, runtime)
        }
        Command::Namespace { command } => {
            namespace::run_namespace_command(kind, config_path, command, runtime).await
        }
        Command::Use(args) => namespace::run_namespace_use(kind, config_path, args).await,
        Command::Current(args) => namespace::run_namespace_current(kind, config_path, args).await,
        Command::Ls(args) => fs::run_filesystem_ls(kind, config_path, args, runtime).await,
        Command::Stat(args) => fs::run_filesystem_stat(kind, config_path, args).await,
        Command::Annotate(args) => fs::run_filesystem_annotate(kind, config_path, args).await,
        Command::Cat(args) => fs::run_filesystem_cat(kind, config_path, args).await,
        Command::Grep(args) => fs::run_filesystem_grep(kind, config_path, args).await,
        Command::Get(args) => fs::run_filesystem_get(kind, config_path, args, runtime).await,
        Command::Put(args) => fs::run_filesystem_put(kind, config_path, args, runtime).await,
        Command::Revisions(args) => fs::run_filesystem_revisions(kind, config_path, args).await,
        Command::Restore(args) => fs::run_filesystem_restore(kind, config_path, args).await,
        Command::Undelete(args) => fs::run_filesystem_undelete(kind, config_path, args).await,
        Command::Mkdir(args) => fs::run_filesystem_mkdir(kind, config_path, args).await,
        Command::Rm(args) => fs::run_filesystem_rm(kind, &location, args).await,
        Command::Mv(args) => fs::run_filesystem_mv(kind, config_path, args, runtime).await,
        Command::Cp(args) => fs::run_filesystem_cp(kind, config_path, args, runtime).await,
        Command::Trash(args) => fs::run_filesystem_trash(kind, &location, args).await,
        Command::Changes(args) => admin::run_admin_changes(kind, config_path, args).await,
        Command::Admin { command } => {
            admin::run_admin_command(kind, config_path, command, runtime).await
        }
    }
}

fn run_completion(
    kind: CommandKind,
    args: &CompletionArgs,
) -> Result<CommandOutput, CommandFailure> {
    let shell = args
        .shell
        .or_else(completion_shell_from_environment)
        .ok_or_else(|| {
            context::fail(
                kind,
                None,
                None,
                CliError::invalid_input(COMPLETION_SHELL_REQUIRED),
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
