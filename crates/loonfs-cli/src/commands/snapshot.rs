//! `loonfs snapshot` commands: create, list, extend, and delete.

use super::context::{Invocation, NamespaceContext};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::pagination::{collect_or_stream_pages, PagedListing};
use crate::args::{
    SnapshotCommand, SnapshotCreateArgs, SnapshotDeleteArgs, SnapshotExtendArgs, SnapshotListArgs,
};
use loonfs_client::CreateSnapshotOptions;

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
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .create_snapshot_with_options(
            context.namespace(),
            &CreateSnapshotOptions {
                name: args.name,
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
    let context = NamespaceContext::resolve(invocation, None).await?;
    let listing = collect_or_stream_pages(
        context
            .profile
            .target
            .client
            .list_snapshots(context.namespace())
            .cursor(args.pagination.cursor),
        &args.pagination.page_limits,
        |_: &loonfs_types::ListSnapshotsResponse| {},
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
    let context = NamespaceContext::resolve(invocation, None).await?;
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
    let context = NamespaceContext::resolve(invocation, None).await?;
    let response = context
        .profile
        .target
        .client
        .delete_snapshot(context.namespace(), &args.snapshot_id)
        .await
        .map_err(|error| context.fail(error))?;
    Ok(context.output(CommandData::SnapshotDeleted(response)))
}
