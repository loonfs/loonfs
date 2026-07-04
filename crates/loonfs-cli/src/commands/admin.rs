use super::context::{fail, resolve_command_context};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{AdminCommand, AdminNamespaceArgs, ChangesArgs, CommandKind};
use loonfs_api::ChangeSeq;

// --- maintenance/admin plane ---

pub(crate) fn run_admin_command(
    kind: CommandKind,
    command: AdminCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        AdminCommand::Checkpoint(args) => run_admin_checkpoint(kind, args),
        AdminCommand::RetentionAdvance(args) => run_admin_retention_advance(kind, args),
    }
}

fn run_admin_checkpoint(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let response = context
        .target
        .backend()
        .create_checkpoint(&context.namespace)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::CheckpointCreated(response),
    })
}

fn run_admin_retention_advance(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let response = context
        .target
        .backend()
        .advance_retention(&context.namespace)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::RetentionAdvanced(response),
    })
}

pub(crate) fn run_changes(
    kind: CommandKind,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let after_seq = ChangeSeq(args.after.unwrap_or(0));
    let response = context
        .target
        .backend()
        .list_changes(&context.namespace, after_seq, args.limit)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::Changes(response),
    })
}
