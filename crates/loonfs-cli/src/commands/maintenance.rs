//! `loonfs maintenance` commands: checkpoints, retention, GC, indexes, and the
//! change feed.

use super::context::{
    parse_public_ordinal_arg, parse_snapshot_id_arg, resolve_command_context,
    resolve_mutation_context, resolve_profile_context,
};
use super::output::{CommandData, CommandFailure, CommandOutput, MaintenanceRan};
use super::pagination::{collect_or_stream_pages, page_request, PagePlan, PagedListing};
use crate::args::{
    ChangesArgs, CommandKind, MaintenanceCheckpointArgs, MaintenanceCheckpointCommand,
    MaintenanceCheckpointDeleteArgs, MaintenanceCheckpointListArgs, MaintenanceCommand,
    MaintenanceGcArgs, MaintenanceIndexCommand, MaintenanceIndexEnableArgs,
    MaintenanceMetadataArgs, MaintenanceNamespaceArgs, MaintenanceRecoverAdministratorArgs,
    MaintenanceRetentionAdvanceArgs, MaintenanceRetentionCommand, MaintenanceStoreCommand,
    MaintenanceStoreProbeArgs,
};
use crate::backend::StepBudget;
use crate::error::CliError;
use loonfs_types::api::v0::GrepIndexLifecycle;
use loonfs_types::{
    AdvanceRetentionRequest, ChangeSeq, CreateCheckpointRequest, ErrorCode, GcRequest,
    MetadataCompactionRequest, MetadataMaintenanceRequest, PinId, PrincipalId,
    RecoverAdministratorRequest, RunMaintenanceRequest,
};
use std::path::Path;

// --- maintenance API group ---

pub(crate) async fn run_maintenance_command(
    kind: CommandKind,
    config_path: &Path,
    command: MaintenanceCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        MaintenanceCommand::RecoverAdministrator(args) => {
            run_maintenance_recover_administrator(kind, config_path, args).await
        }
        MaintenanceCommand::Metadata(args) => {
            run_maintenance_metadata(kind, config_path, args).await
        }
        MaintenanceCommand::Fold(args) => run_maintenance_fold(kind, config_path, args).await,
        MaintenanceCommand::Compact(args) => run_maintenance_compact(kind, config_path, args).await,
        MaintenanceCommand::Checkpoint { command } => match command {
            MaintenanceCheckpointCommand::Create(args) => {
                run_maintenance_checkpoint(kind, config_path, args).await
            }
            MaintenanceCheckpointCommand::List(args) => {
                run_maintenance_checkpoint_list(kind, config_path, args).await
            }
            MaintenanceCheckpointCommand::Delete(args) => {
                run_maintenance_checkpoint_delete(kind, config_path, args).await
            }
        },
        MaintenanceCommand::Index { command } => match command {
            MaintenanceIndexCommand::Enable(args) => {
                run_maintenance_index_enable(kind, config_path, args).await
            }
            MaintenanceIndexCommand::Disable(args) => {
                run_maintenance_index_disable(kind, config_path, args).await
            }
            MaintenanceIndexCommand::Status(args) => {
                run_maintenance_index_status(kind, config_path, args).await
            }
        },
        MaintenanceCommand::Retention { command } => match command {
            MaintenanceRetentionCommand::Advance(args) => {
                run_maintenance_retention_advance(kind, config_path, args).await
            }
        },
        MaintenanceCommand::Gc(args) => run_maintenance_gc(kind, config_path, args).await,
        MaintenanceCommand::GrepGc(args) => run_maintenance_grep_gc(kind, config_path, args).await,
        MaintenanceCommand::Store { command } => match command {
            MaintenanceStoreCommand::Probe(args) => {
                run_maintenance_store_probe(kind, config_path, args).await
            }
        },
    }
}

async fn run_maintenance_recover_administrator(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceRecoverAdministratorArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_mutation_context(kind, config_path, &args.target, &args.actor).await?;
    let principal_id = PrincipalId::parse(&args.principal_id).map_err(|error| {
        context.fail(
            kind,
            CliError::invalid_request(error.to_string()).with_param("principal_id"),
        )
    })?;
    let request =
        RunMaintenanceRequest::RecoverAdministrator(RecoverAdministratorRequest { principal_id });
    let response = context
        .target
        .client
        .run_maintenance(context.namespace(), &request, context.actor_id.as_ref())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_metadata(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceMetadataArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let request = RunMaintenanceRequest::Metadata(MetadataMaintenanceRequest {
        max_wal_tail_objects: args.max_wal_tail_objects,
    });
    let response = context
        .target
        .client
        .run_maintenance(context.namespace(), &request, context.actor_id.as_ref())
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_gc(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceGcArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .run_maintenance(
            context.namespace(),
            &RunMaintenanceRequest::Gc(GcRequest {
                grace_window_ms: args.grace_window_ms,
            }),
            context.actor_id.as_ref(),
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_checkpoint(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceCheckpointArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let request = CreateCheckpointRequest {
        name: args.name,
        ttl_ms: args.ttl_ms,
    };
    let response = context
        .target
        .client
        .create_checkpoint(context.namespace(), &request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::CheckpointCreated(response)))
}

async fn run_maintenance_checkpoint_list(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceCheckpointListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let listing = collect_or_stream_pages(
        PagePlan::new(&args.pagination.page_limits),
        args.pagination.cursor.clone(),
        args.pagination.page_limits.jsonl,
        async |cursor, limit| {
            Ok(context
                .target
                .client
                .list_checkpoints(context.namespace())
                .page(page_request(cursor, limit)?)
                .await?)
        },
        |_: &loonfs_types::ListCheckpointsResponse| {},
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(kind, CommandData::StreamedToStdout));
    };

    Ok(context.output(kind, CommandData::CheckpointsListed(response)))
}

async fn run_maintenance_checkpoint_delete(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceCheckpointDeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let checkpoint_id = PinId::parse(&args.checkpoint_id).map_err(|error| {
        context.fail(
            kind,
            crate::error::CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                .with_param("checkpoint_id"),
        )
    })?;
    let response = context
        .target
        .client
        .delete_checkpoint(context.namespace(), &checkpoint_id)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::CheckpointDeleted(response)))
}

/// One metadata-upkeep pass at a threshold of one WAL object.
///
/// The fold an operator asks for explicitly runs whatever the tail length.
/// The bounded compaction step runs in the same pass, and the output reports
/// both.
async fn run_maintenance_fold(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .run_maintenance(
            context.namespace(),
            &RunMaintenanceRequest::Metadata(MetadataMaintenanceRequest {
                max_wal_tail_objects: Some(1),
            }),
            context.actor_id.as_ref(),
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_compact(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .run_maintenance(
            context.namespace(),
            &RunMaintenanceRequest::MetadataCompaction(MetadataCompactionRequest {}),
            context.actor_id.as_ref(),
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_retention_advance(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceRetentionAdvanceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let retention_floor_before = context
        .target
        .client
        .get_namespace(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?
        .retention_floor_seq;
    let response = context
        .target
        .client
        .run_maintenance(
            context.namespace(),
            &RunMaintenanceRequest::Retention(AdvanceRetentionRequest {
                to_seq: args.to_seq.map(ChangeSeq),
                cutoff_at_ms: args.before,
            }),
            context.actor_id.as_ref(),
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::after_retention(
            response,
            retention_floor_before,
        )),
    ))
}

/// Checks that the profile's object store supports the operations LoonFS
/// requires. This command checks the store, not a namespace.
async fn run_maintenance_store_probe(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceStoreProbeArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let context = resolve_profile_context(
        kind,
        config_path,
        explicit_profile,
        args.request.no_retry,
        None,
    )
    .await?;
    let response = context
        .target
        .client
        .probe_store(&loonfs_types::api::v0::StoreProbeRequest {})
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::StoreProbed(response)))
}

pub(crate) async fn run_changes(
    kind: CommandKind,
    config_path: &Path,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let after_seq = parse_public_ordinal_arg("--after", args.after.unwrap_or(0), ChangeSeq::parse)
        .map_err(|error| context.fail(kind, error))?;
    let snapshot_id = args
        .snapshot_id
        .as_deref()
        .map(|value| parse_snapshot_id_arg("--snapshot-id", value))
        .transpose()
        .map_err(|error| context.fail(kind, error))?;
    let listing = collect_or_stream_pages(
        PagePlan::new(&args.pagination.page_limits),
        Some(after_seq),
        args.pagination.page_limits.jsonl,
        async |cursor, limit| {
            Ok(context
                .target
                .list_changes_at_snapshot(context.namespace(), after_seq, snapshot_id.as_ref())
                .page(page_request(cursor, limit)?)
                .await?)
        },
        |_: &loonfs_types::api::v0::ListChangesResponse| {},
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(kind, CommandData::StreamedToStdout));
    };

    Ok(context.output(kind, CommandData::Changes(response)))
}

/// Enables the index and, by default, waits for it to catch up to one fixed
/// sequence.
///
/// The sequence is captured before any waiting starts and never re-read:
/// writes that land afterwards are not waited for, so a namespace that is
/// being written to cannot keep this command running.
async fn run_maintenance_index_enable(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceIndexEnableArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .enable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    let target_seq = match (args.no_wait, &response.lifecycle) {
        // Nothing to wait for: the caller opted out, or the index is
        // disabled, which enable would have changed if it could.
        (true, _) | (_, GrepIndexLifecycle::Disabled) => None,
        // A backfill already names the namespace sequence its checkpoint
        // captured, and reaching it is what completes the backfill.
        (_, GrepIndexLifecycle::Backfilling { captured_seq, .. }) => Some(*captured_seq),
        // An active index is asked to catch up to where the namespace is
        // now: one read, before any stepping, so an index that is already
        // there returns without doing anything.
        (_, GrepIndexLifecycle::Active { .. }) => Some(
            context
                .target
                .client
                .get_namespace(context.namespace())
                .await
                .map_err(|error| context.fail(kind, error))?
                .head_seq,
        ),
    };
    let waited = match target_seq {
        Some(target_seq) => Some(
            context
                .target
                .wait_for_grep_index(
                    context.namespace(),
                    target_seq,
                    StepBudget {
                        max_steps: args.max_steps,
                        deadline_ms: args.deadline_ms,
                    },
                )
                .await
                .map_err(|error| context.fail(kind, error))?,
        ),
        None => None,
    };
    let response = if waited.is_some() {
        context
            .target
            .client
            .get_grep_index(context.namespace())
            .await
            .map_err(|error| context.fail(kind, error))?
    } else {
        response
    };

    Ok(context.output(
        kind,
        CommandData::GrepIndexEnabled {
            response,
            waited_for_seq: target_seq,
            steps: waited.as_ref().map_or(0, |waited| waited.steps),
            budget_exhausted: waited.is_some_and(|waited| !waited.reached),
        },
    ))
}

async fn run_maintenance_index_status(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .get_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::GrepIndexStatus(response)))
}

async fn run_maintenance_grep_gc(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .run_maintenance(
            context.namespace(),
            &RunMaintenanceRequest::GrepGc {},
            context.actor_id.as_ref(),
        )
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(
        kind,
        CommandData::MaintenanceRan(MaintenanceRan::new(response)),
    ))
}

async fn run_maintenance_index_disable(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .client
        .disable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::GrepIndexDisabled(response)))
}
