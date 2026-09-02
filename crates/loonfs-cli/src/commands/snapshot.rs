use super::context::{resolve_profile_context, CommandContext};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::pagination::{collect_or_stream_pages, PagePlan, PagedListing};
use crate::args::{
    CommandKind, SnapshotCommand, SnapshotCreateArgs, SnapshotExtendArgs, SnapshotListArgs,
    SnapshotReleaseArgs, SnapshotTargetArgs,
};
use crate::error::CliError;
use crate::resolve::parse_namespace_id;
use loonfs_api::{CheckpointId, ErrorCode};
use std::path::Path;

async fn resolve_snapshot_context(
    kind: CommandKind,
    config_path: &Path,
    target: &SnapshotTargetArgs,
) -> Result<CommandContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let mut context =
        resolve_profile_context(kind, config_path, explicit_profile, target.request.no_retry)
            .await?;
    let namespace_id = parse_namespace_id(&target.namespace_id)
        .map_err(|error| error.with_param("namespace_id"))
        .map_err(|error| context.fail(kind, error))?;
    context.namespace = Some(namespace_id);
    Ok(context)
}

fn parse_snapshot_id(value: &str) -> Result<CheckpointId, CliError> {
    CheckpointId::parse(value).map_err(|error| {
        CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
            .with_param("snapshot_id")
    })
}

pub(crate) async fn run_snapshot_command(
    kind: CommandKind,
    config_path: &Path,
    command: SnapshotCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        SnapshotCommand::Create(args) => run_snapshot_create(kind, config_path, args).await,
        SnapshotCommand::List(args) => run_snapshot_list(kind, config_path, args).await,
        SnapshotCommand::Extend(args) => run_snapshot_extend(kind, config_path, args).await,
        SnapshotCommand::Release(args) => run_snapshot_release(kind, config_path, args).await,
    }
}

async fn run_snapshot_create(
    kind: CommandKind,
    config_path: &Path,
    args: SnapshotCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .create_snapshot(context.namespace(), &args.name, args.ttl_ms)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::SnapshotCreated(response)))
}

async fn run_snapshot_list(
    kind: CommandKind,
    config_path: &Path,
    args: SnapshotListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(kind, config_path, &args.target).await?;
    let listing = collect_or_stream_pages(
        PagePlan::new(&args.pagination),
        args.pagination.cursor,
        args.pagination.jsonl,
        async |cursor, limit| {
            context
                .target
                .list_snapshots_page(context.namespace(), limit, cursor.as_deref())
                .await
        },
        |_: &loonfs_api::v0::ListSnapshotsResponse| {},
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(kind, CommandData::StreamedToStdout));
    };
    Ok(context.output(kind, CommandData::SnapshotsListed(response)))
}

async fn run_snapshot_extend(
    kind: CommandKind,
    config_path: &Path,
    args: SnapshotExtendArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(kind, config_path, &args.target).await?;
    let snapshot_id =
        parse_snapshot_id(&args.snapshot_id).map_err(|error| context.fail(kind, error))?;
    let response = context
        .target
        .extend_snapshot(context.namespace(), &snapshot_id, args.ttl_ms)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::SnapshotExtended(response)))
}

async fn run_snapshot_release(
    kind: CommandKind,
    config_path: &Path,
    args: SnapshotReleaseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_snapshot_context(kind, config_path, &args.target).await?;
    let snapshot_id =
        parse_snapshot_id(&args.snapshot_id).map_err(|error| context.fail(kind, error))?;
    let response = context
        .target
        .release_snapshot(context.namespace(), &snapshot_id)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::SnapshotReleased(response)))
}
