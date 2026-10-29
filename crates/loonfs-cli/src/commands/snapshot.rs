//! `loonfs snapshot` commands: create, list, extend, and delete.

use super::context::{Invocation, NamespaceContext, ProfileContext};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::pagination::{collect_or_stream_pages, PagedListing};
use crate::args::{
    SnapshotCommand, SnapshotCreateArgs, SnapshotDeleteArgs, SnapshotExtendArgs, SnapshotListArgs,
    SnapshotTargetArgs,
};
use loonfs_client::CreateSnapshotOptions;

/// Resolves the context of a snapshot command, which acts as the selected
/// subject on the namespace that the command names.
async fn resolve_snapshot_context(
    invocation: &Invocation,
    target: &SnapshotTargetArgs,
) -> Result<NamespaceContext, CommandFailure> {
    let context = ProfileContext::resolve_as_subject(invocation).await?;
    let actor_id = context.actor(None)?;
    Ok(context.for_namespace(target.namespace_id.clone(), actor_id))
}

pub(crate) async fn run_snapshot_command(
    invocation: &Invocation,
    command: SnapshotCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        SnapshotCommand::Create(args) => run_snapshot_create(invocation, args).await,
        SnapshotCommand::List(args) => run_snapshot_list(invocation, args).await,
        SnapshotCommand::Extend(args) => run_snapshot_extend(invocation, args).await,
        SnapshotCommand::Delete(args) => run_snapshot_delete(invocation, args).await,
    }
}

async fn run_snapshot_create(
    invocation: &Invocation,
    args: SnapshotCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(invocation, &args.target).await?;
    let response = context
        .profile
        .target
        .client
        .create_snapshot_with_options(
            context.namespace(),
            &args.name,
            &CreateSnapshotOptions {
                ttl_ms: args.ttl_ms,
            },
        )
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::SnapshotCreated(response)))
}

async fn run_snapshot_list(
    invocation: &Invocation,
    args: SnapshotListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(invocation, &args.target).await?;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .list_snapshots(context.namespace())
            .cursor(args.pagination.cursor),
        &args.pagination.page_limits,
        |_: &loonfs_types::api::v0::ListSnapshotsResponse| {},
    )
    .await
    .map_err(|error| context.fail(error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(CommandData::StreamedToStdout));
    };
    Ok(context.output(CommandData::SnapshotsListed(response)))
}

async fn run_snapshot_extend(
    invocation: &Invocation,
    args: SnapshotExtendArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(invocation, &args.target).await?;
    let response = context
        .profile
        .target
        .client
        .extend_snapshot(context.namespace(), &args.snapshot_id, args.ttl_ms)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::SnapshotExtended(response)))
}

async fn run_snapshot_delete(
    invocation: &Invocation,
    args: SnapshotDeleteArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(invocation, &args.target).await?;
    let response = context
        .profile
        .target
        .client
        .delete_snapshot(context.namespace(), &args.snapshot_id)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::SnapshotDeleted(response)))
}
