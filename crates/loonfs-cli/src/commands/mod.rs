//! Command implementations, one submodule per command group.

mod admin;
mod config;
mod context;
mod fs;
mod namespace;
mod output;
mod profile;
mod profile_config;

pub(crate) use self::output::{CommandData, CommandFailure, CommandOutput};

use crate::args::{Cli, Command, RuntimeBehavior};

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
            error: Box::new(crate::error::CliError::json_not_supported_for_streaming()),
        });
    }

    match cli.command {
        Command::Version => Ok(CommandOutput {
            kind,
            profile: None,
            mode: None,
            data: CommandData::Version {
                version: env!("CARGO_PKG_VERSION").to_owned(),
                commit: env!("LOON_GIT_COMMIT").to_owned(),
                commit_date: env!("LOON_GIT_COMMIT_DATE").to_owned(),
            },
        }),
        Command::Init(args) => config::run_config_init(kind, args, runtime),
        Command::Config { command } => config::run_config_command(kind, command),
        Command::Profile { command } => profile::run_profile_command(kind, command, runtime),
        Command::Namespace { command } => {
            namespace::run_namespace_command(kind, command, runtime).await
        }
        Command::Use(args) => namespace::run_namespace_use(kind, args).await,
        Command::Current(args) => namespace::run_namespace_current(kind, args).await,
        Command::Ls(args) => fs::run_filesystem_ls(kind, args).await,
        Command::Stat(args) => fs::run_filesystem_stat(kind, args).await,
        Command::Cat(args) => fs::run_filesystem_cat(kind, args).await,
        Command::Grep(args) => fs::run_filesystem_grep(kind, args).await,
        Command::Get(args) => fs::run_filesystem_get(kind, args, runtime).await,
        Command::Put(args) => fs::run_filesystem_put(kind, args).await,
        Command::Revisions(args) => fs::run_filesystem_revisions(kind, args).await,
        Command::Restore(args) => fs::run_filesystem_restore(kind, args).await,
        Command::Undelete(args) => fs::run_filesystem_undelete(kind, args).await,
        Command::Mkdir(args) => fs::run_filesystem_mkdir(kind, args).await,
        Command::Rm(args) => fs::run_filesystem_rm(kind, args).await,
        Command::Mv(args) => fs::run_filesystem_mv(kind, args).await,
        Command::Cp(args) => fs::run_filesystem_cp(kind, args).await,
        Command::Changes(args) => admin::run_admin_changes(kind, args).await,
        Command::Admin { command } => admin::run_admin_command(kind, command).await,
    }
}
