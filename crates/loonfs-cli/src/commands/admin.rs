//! `loon admin` commands: checkpoints, retention, GC, namespace repair,
//! indexes, and the change feed.

use super::context::resolve_command_context;
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    AdminCheckpointArgs, AdminCheckpointReleaseArgs, AdminCommand, AdminGcArgs, AdminNamespaceArgs,
    AdminStepArgs, ChangesArgs, CommandKind,
};
use loonfs_api::{
    ChangeSeq, CheckpointId, CreateCheckpointRequest, ErrorCode, GcRequest, MaintenanceStepRequest,
};

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
        AdminCommand::Step(args) => run_admin_step(kind, args).await,
        AdminCommand::Gc(args) => run_admin_gc(kind, args).await,
        AdminCommand::Repair(args) => run_admin_repair(kind, args).await,
        AdminCommand::IndexEnable(args) => run_admin_index_enable(kind, args).await,
        AdminCommand::IndexDisable(args) => run_admin_index_disable(kind, args).await,
    }
}

async fn run_admin_repair(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .backend()
        .repair_namespace(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::NamespaceRepaired(response),
    })
}

async fn run_admin_step(
    kind: CommandKind,
    args: AdminStepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let request = MaintenanceStepRequest {
        max_wal_tail_segments: args.max_wal_tail_segments,
        gc: args.gc.then(GcRequest::default),
    };
    let response = context
        .target
        .backend()
        .maintenance_step(&context.namespace, request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::MaintenanceStepped(response),
    })
}

async fn run_admin_gc(
    kind: CommandKind,
    args: AdminGcArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let single_pass = args.max_objects.is_some();
    let mut request = GcRequest {
        grace_window_ms: args.grace_window_ms,
        reap_window_ms: args.reap_window_ms,
        max_objects: Some(args.max_objects.unwrap_or(loonfs::DEFAULT_GC_MAX_OBJECTS)),
        cursor: None,
    };
    let mut response = None;
    loop {
        let pass = context
            .target
            .backend()
            .gc_namespace(&context.namespace, request.clone())
            .await
            .map_err(|error| context.fail(kind, error))?;
        let next_cursor = pass.next_cursor.clone();
        match &mut response {
            Some(total) => accumulate_gc_response(total, pass),
            None => response = Some(pass),
        }
        if single_pass || next_cursor.is_none() {
            break;
        }
        request.cursor = next_cursor;
    }
    let response = response.expect("GC loop should run at least once");

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GarbageCollected(response),
    })
}

fn accumulate_gc_response(total: &mut loonfs_api::GcResponse, pass: loonfs_api::GcResponse) {
    total.deleted_wal_segments += pass.deleted_wal_segments;
    total.deleted_metadata_tables += pass.deleted_metadata_tables;
    total.deleted_manifests += pass.deleted_manifests;
    total.deleted_checkpoint_records += pass.deleted_checkpoint_records;
    total.released_fork_checkpoints += pass.released_fork_checkpoints;
    total.deleted_upload_sessions += pass.deleted_upload_sessions;
    total.released_missing_basis_checkpoints += pass.released_missing_basis_checkpoints;
    total.retained_candidates += pass.retained_candidates;
    total.degraded_retention |= pass.degraded_retention;
    total.incomplete_namespace_ignored |= pass.incomplete_namespace_ignored;
    total.next_cursor = pass.next_cursor;
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
    let checkpoint_id = CheckpointId::parse(&args.checkpoint_id).map_err(|error| {
        context.fail(
            kind,
            crate::error::CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()),
        )
    })?;
    let response = context
        .target
        .backend()
        .release_checkpoint(&context.namespace, &checkpoint_id)
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
        .advance_retention_floor(&context.namespace)
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
        .list_changes(&context.namespace, after_seq, args.limit)
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
        .enable_grep_index(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexEnabled(response),
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
        .disable_grep_index(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexDisabled(response),
    })
}
