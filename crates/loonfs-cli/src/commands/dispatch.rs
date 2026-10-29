//! Dispatches a parsed command line to the command group that runs it.

use super::context::{fail, Invocation};
use super::{
    access, completion, config, fs, inspection, maintenance, namespace, profile, snapshot,
    CommandData, CommandFailure, CommandOutput,
};
use crate::args::{AccessCommand, Cli, Command, RuntimeBehavior, TopLevelCommand};
use crate::config_file::resolve_config_location;
use crate::error::CliError;
use crate::render::OutputFormat;

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
        TopLevelCommand::Completion(args) => return completion::run_completion(kind, &args),
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
