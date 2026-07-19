//! `loon admin` commands: status, checkpoints, retention, gc, indexes,
//! and the change feed.

use super::context::resolve_command_context;
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    AdminCheckpointArgs, AdminCheckpointReleaseArgs, AdminCommand, AdminGcArgs, AdminNamespaceArgs,
    AdminTickArgs, ChangesArgs, CommandKind,
};
use loonfs_api::{ChangeSeq, CreateCheckpointRequest, GcRequest, MaintenanceTickRequest};

// --- maintenance/admin plane ---

pub(crate) async fn run_admin_command(
    kind: CommandKind,
    command: AdminCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        AdminCommand::Checkpoint(args) => run_admin_checkpoint(kind, args).await,
        AdminCommand::CheckpointRelease(args) => run_admin_checkpoint_release(kind, args).await,
        AdminCommand::Flush(args) => run_admin_flush(kind, args).await,
        AdminCommand::RetentionAdvance(args) => run_admin_retention_advance(kind, args).await,
        AdminCommand::Tick(args) => run_admin_tick(kind, args).await,
        AdminCommand::Gc(args) => run_admin_gc(kind, args).await,
        AdminCommand::IndexEnable(args) => run_admin_index_enable(kind, args).await,
        AdminCommand::IndexDisable(args) => run_admin_index_disable(kind, args).await,
    }
}

async fn run_admin_tick(
    kind: CommandKind,
    args: AdminTickArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let request = MaintenanceTickRequest {
        max_wal_tail_segments: args.max_wal_tail_segments,
        gc: args.gc.then(GcRequest::default),
    };
    let response = context
        .target
        .backend()
        .maintenance_tick(&context.namespace, request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::MaintenanceTicked(response),
    })
}

async fn run_admin_gc(
    kind: CommandKind,
    args: AdminGcArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let request = GcRequest {
        grace_window_ms: args.grace_window_ms,
        reap_window_ms: args.reap_window_ms,
    };
    let response = context
        .target
        .backend()
        .gc_namespace(&context.namespace, request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GarbageCollected(response),
    })
}

async fn run_admin_checkpoint(
    kind: CommandKind,
    args: AdminCheckpointArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let request = CreateCheckpointRequest {
        name: args.name,
        ttl_ms: args.ttl_ms,
    };
    let response = context
        .target
        .backend()
        .create_checkpoint(&context.namespace, request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::CheckpointCreated(response),
    })
}

async fn run_admin_checkpoint_release(
    kind: CommandKind,
    args: AdminCheckpointReleaseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .release_checkpoint(&context.namespace, &args.checkpoint_id)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::CheckpointReleased(response),
    })
}

async fn run_admin_flush(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .flush_wal(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::WalFlushed(response),
    })
}

async fn run_admin_retention_advance(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .advance_retention(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::RetentionAdvanced(response),
    })
}

pub(crate) async fn run_admin_changes(
    kind: CommandKind,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let after_seq = ChangeSeq(args.after.unwrap_or(0));
    let response = context
        .target
        .backend()
        .list_changes_page(&context.namespace, after_seq, args.limit)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::Changes(response),
    })
}

async fn run_admin_index_enable(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .enable_grams_index(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    // An enabled index is only queryable once maintenance builds it; kick a
    // tick here instead of leaving search unavailable until someone
    // discovers `loon admin tick`. Run it even when the feature was already
    // enabled so rerunning after a prior tick failure actually retries the
    // failed half of this command.
    let backfill_tick = Some(
        context
            .target
            .backend()
            .maintenance_tick(&context.namespace, MaintenanceTickRequest::default())
            .await
            .map_err(|error| context.fail(kind, error))?,
    );
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GramsIndexEnabled {
            enabled: response,
            backfill_tick,
        },
    })
}

async fn run_admin_index_disable(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .disable_grams_index(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GramsIndexDisabled(response),
    })
}
