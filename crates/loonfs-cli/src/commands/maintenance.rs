//! `loonfs maintenance` commands: the checkpoint inventory, retention, GC,
//! indexes, and the change feed.

use super::context::{Invocation, NamespaceContext, ProfileContext};
use super::output::{CommandData, CommandFailure, CommandOutput, MaintenanceRan};
use super::pagination::{collect_or_stream_pages, PagedListing};
use crate::args::{
    ChangesArgs, MaintenanceCheckpointCommand, MaintenanceCheckpointListArgs, MaintenanceCommand,
    MaintenanceGcArgs, MaintenanceIndexCommand, MaintenanceIndexEnableArgs,
    MaintenanceMetadataArgs, MaintenanceRecoverAdministratorArgs, MaintenanceRetentionAdvanceArgs,
    MaintenanceRetentionCommand, MaintenanceRetentionWindowArgs, MaintenanceStoreCommand,
};
use crate::backend::StepBudget;
use crate::error::Result;
use loonfs_client::{GcOptions, ListChangesOptions, ReadAt};
use loonfs_types::api::v0::GrepIndexStatus;
use loonfs_types::{
    AdvanceRetentionRequest, ChangeSeq, MetadataMaintenanceRequest, RunMaintenanceResponse,
};

pub(crate) async fn run_maintenance_command(
    invocation: &Invocation,
    command: MaintenanceCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        MaintenanceCommand::RecoverAdministrator(args) => {
            run_maintenance_recover_administrator(invocation, args).await
        }
        MaintenanceCommand::Metadata(args) => run_maintenance_metadata(invocation, args).await,
        MaintenanceCommand::Fold => run_maintenance_fold(invocation).await,
        MaintenanceCommand::Checkpoint { command } => match command {
            MaintenanceCheckpointCommand::List(args) => {
                run_maintenance_checkpoint_list(invocation, args).await
            }
        },
        MaintenanceCommand::Index { command } => match command {
            MaintenanceIndexCommand::Enable(args) => {
                run_maintenance_index_enable(invocation, args).await
            }
            MaintenanceIndexCommand::Disable => run_maintenance_index_disable(invocation).await,
            MaintenanceIndexCommand::Status => run_maintenance_index_status(invocation).await,
        },
        MaintenanceCommand::Retention { command } => match command {
            MaintenanceRetentionCommand::Advance(args) => {
                run_maintenance_retention_advance(invocation, args).await
            }
            MaintenanceRetentionCommand::Window(args) => {
                run_maintenance_retention_window(invocation, args).await
            }
        },
        MaintenanceCommand::Gc(args) => run_maintenance_gc(invocation, args).await,
        MaintenanceCommand::Store { command } => match command {
            MaintenanceStoreCommand::Probe => run_maintenance_store_probe(invocation).await,
        },
    }
}

fn maintenance_ran(context: &NamespaceContext, response: RunMaintenanceResponse) -> CommandOutput {
    context.output(CommandData::MaintenanceRan(MaintenanceRan::new(response)))
}

async fn run_maintenance_recover_administrator(
    invocation: &Invocation,
    args: MaintenanceRecoverAdministratorArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, args.actor.actor_id.as_ref()).await?;
    let response = context
        .profile
        .target
        .client
        .recover_administrator(context.namespace(), &args.principal_id, context.actor())
        .await
        .map_err(|error| context.fail(error))?;
    Ok(maintenance_ran(
        &context,
        RunMaintenanceResponse::RecoverAdministrator(response),
    ))
}

async fn run_maintenance_metadata(
    invocation: &Invocation,
    args: MaintenanceMetadataArgs,
) -> Result<CommandOutput, CommandFailure> {
    maintain_metadata(
        invocation,
        &MetadataMaintenanceRequest {
            max_wal_tail_objects: args.max_wal_tail_objects,
        },
    )
    .await
}

/// One metadata-upkeep pass at a threshold of one WAL object.
///
/// The fold an operator asks for explicitly runs whatever the tail length.
/// The same job then compacts metadata until nothing is due, and the output
/// reports the outcome and the compaction units.
async fn run_maintenance_fold(invocation: &Invocation) -> Result<CommandOutput, CommandFailure> {
    maintain_metadata(
        invocation,
        &MetadataMaintenanceRequest {
            max_wal_tail_objects: Some(std::num::NonZeroU64::MIN),
        },
    )
    .await
}

async fn maintain_metadata(
    invocation: &Invocation,
    options: &MetadataMaintenanceRequest,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .maintain_metadata_while_due_with_options(context.namespace(), options)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(maintenance_ran(
        &context,
        RunMaintenanceResponse::Metadata(response),
    ))
}

async fn run_maintenance_gc(
    invocation: &Invocation,
    args: MaintenanceGcArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let client = &context.profile.target.client;
    let response = match args.grace_window_ms {
        Some(grace_window_ms) => {
            client
                .gc_with_options(
                    context.namespace(),
                    &GcOptions {
                        grace_window_ms,
                        ..GcOptions::default()
                    },
                )
                .await
        }
        None => client.gc(context.namespace()).await,
    }
    .map_err(|error| context.fail(error))?;
    Ok(maintenance_ran(
        &context,
        RunMaintenanceResponse::Gc(response),
    ))
}

async fn run_maintenance_checkpoint_list(
    invocation: &Invocation,
    args: MaintenanceCheckpointListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .list_checkpoints(context.namespace())
            .cursor(args.pagination.cursor.clone()),
        &args.pagination.page_limits,
        |_: &loonfs_types::ListCheckpointsResponse| {},
    )
    .await
    .map_err(|error| context.fail(error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(CommandData::StreamedToStdout));
    };

    Ok(context.output(CommandData::CheckpointsListed(response)))
}

async fn run_maintenance_retention_advance(
    invocation: &Invocation,
    args: MaintenanceRetentionAdvanceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let client = &context.profile.target.client;
    let retention_floor_before = client
        .get_namespace(context.namespace())
        .await
        .map_err(|error| context.fail(error))?
        .retention_floor_seq;
    let response = client
        .advance_retention_floor_with_options(
            context.namespace(),
            &AdvanceRetentionRequest {
                to_seq: args.to_seq,
                cutoff_at_ms: args.before,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::MaintenanceRan(
        MaintenanceRan::after_retention(
            RunMaintenanceResponse::Retention(response),
            retention_floor_before,
        ),
    )))
}

async fn run_maintenance_retention_window(
    invocation: &Invocation,
    args: MaintenanceRetentionWindowArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .set_retention_window(context.namespace(), args.retention_window_ms)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(maintenance_ran(
        &context,
        RunMaintenanceResponse::RetentionWindow(response),
    ))
}

/// Checks that the profile's object store supports the operations LoonFS
/// requires. This command checks the store, not a namespace.
async fn run_maintenance_store_probe(
    invocation: &Invocation,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve(invocation).await?;
    let response = context
        .target
        .client
        .probe_store(&loonfs_types::api::v0::StoreProbeRequest {})
        .await
        .map_err(|error| context.fail(error))?;

    Ok(context.output(CommandData::StoreProbed(response)))
}

pub(crate) async fn run_changes(
    invocation: &Invocation,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let after_seq = args.after.unwrap_or(ChangeSeq(0));
    let listing = collect_or_stream_pages(
        context.profile.target.client.list_changes_with_options(
            context.namespace(),
            after_seq,
            &ListChangesOptions {
                at: ReadAt::from(args.snapshot_id),
            },
        ),
        &args.pagination.page_limits,
        |_: &loonfs_types::api::v0::ListChangesResponse| {},
    )
    .await
    .map_err(|error| context.fail(error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(CommandData::StreamedToStdout));
    };

    Ok(context.output(CommandData::Changes(response)))
}

/// Enables the index and, by default, waits for it to catch up to one fixed
/// sequence.
///
/// The sequence is captured before any waiting starts and never re-read:
/// writes that land afterwards are not waited for, so a namespace that is
/// being written to cannot keep this command running.
async fn run_maintenance_index_enable(
    invocation: &Invocation,
    args: MaintenanceIndexEnableArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let target = &context.profile.target;
    let response = target
        .client
        .enable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(error))?;
    let target_seq = match (args.no_wait, &response.status) {
        // Nothing to wait for: the caller opted out, or the index is
        // disabled, which enable would have changed if it could.
        (true, _) | (_, GrepIndexStatus::Disabled | GrepIndexStatus::Unknown) => None,
        // A backfill already names the namespace sequence its checkpoint
        // captured, and reaching it is what completes the backfill.
        (_, GrepIndexStatus::Backfilling { captured_seq, .. }) => Some(*captured_seq),
        // An active index is asked to catch up to where the namespace is
        // now: one read, before any stepping, so an index that is already
        // there returns without doing anything.
        (_, GrepIndexStatus::Active { .. }) => Some(
            target
                .client
                .get_namespace(context.namespace())
                .await
                .map_err(|error| context.fail(error))?
                .head_seq,
        ),
    };
    let waited = match target_seq {
        Some(target_seq) => Some(
            target
                .wait_for_grep_index(
                    context.namespace(),
                    target_seq,
                    StepBudget {
                        max_steps: args.max_steps,
                        deadline_ms: args.deadline_ms,
                    },
                )
                .await
                .map_err(|error| context.fail(error))?,
        ),
        None => None,
    };
    let response = if waited.is_some() {
        target
            .client
            .get_grep_index(context.namespace())
            .await
            .map_err(|error| context.fail(error))?
    } else {
        response
    };

    Ok(context.output(CommandData::GrepIndexEnabled {
        response,
        waited_for_seq: target_seq,
        steps: waited.as_ref().map_or(0, |waited| waited.steps),
        budget_exhausted: waited.is_some_and(|waited| !waited.reached),
    }))
}

async fn run_maintenance_index_status(
    invocation: &Invocation,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .get_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::GrepIndexStatus(response)))
}

async fn run_maintenance_index_disable(
    invocation: &Invocation,
) -> Result<CommandOutput, CommandFailure> {
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .disable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::GrepIndexDisabled(response)))
}
