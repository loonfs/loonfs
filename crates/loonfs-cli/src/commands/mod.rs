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

pub(crate) fn run(cli: Cli, runtime: RuntimeBehavior) -> Result<CommandOutput, CommandFailure> {
    let kind = cli.kind();
    if runtime.json && !kind.supports_json() {
        return Err(CommandFailure {
            kind,
            profile: None,
            mode: None,
            error: crate::error::CliError::json_not_supported_for_streaming(),
        });
    }

    match cli.command {
        Command::Version => Ok(CommandOutput {
            kind,
            profile: None,
            mode: None,
            data: CommandData::Version {
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
        }),
        Command::Init(args) => config::run_init(kind, args, runtime),
        Command::Config { command } => config::run_config_command(kind, command),
        Command::Profile { command } => profile::run_profile_command(kind, command, runtime),
        Command::Namespace { command } => namespace::run_namespace_command(kind, command),
        Command::Use(args) => namespace::run_namespace_use(kind, args),
        Command::Current(args) => namespace::run_current(kind, args),
        Command::Ls(args) => fs::run_filesystem_ls(kind, args),
        Command::Stat(args) => fs::run_filesystem_stat(kind, args),
        Command::Cat(args) => fs::run_filesystem_cat(kind, args),
        Command::Get(args) => fs::run_filesystem_get(kind, args, runtime),
        Command::Put(args) => fs::run_filesystem_put(kind, args),
        Command::Revisions(args) => fs::run_filesystem_revisions(kind, args),
        Command::Restore(args) => fs::run_filesystem_restore(kind, args),
        Command::Mkdir(args) => fs::run_filesystem_mkdir(kind, args),
        Command::Rm(args) => fs::run_filesystem_rm(kind, args),
        Command::Mv(args) => fs::run_filesystem_mv(kind, args),
        Command::Cp(args) => fs::run_filesystem_cp(kind, args),
        Command::Changes(args) => admin::run_changes(kind, args),
        Command::Admin { command } => admin::run_admin_command(kind, command),
    }
}
