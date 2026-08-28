use super::context::{fail, fail_for};
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::pagination::{write_jsonl_page, PagePlan};
use crate::args::{
    CommandKind, SnapshotCommand, SnapshotCreateArgs, SnapshotExtendArgs, SnapshotListArgs,
    SnapshotReleaseArgs, SnapshotTargetArgs,
};
use crate::backend_error::BackendError;
use crate::resolve::{parse_namespace_id, resolve_target_profile, ResolvedTarget};
use loonfs_api::{CheckpointId, ErrorCode, NamespaceId};
use std::io::{self, BufWriter};
use std::path::Path;

struct SnapshotContext {
    profile_name: String,
    mode: String,
    namespace_id: NamespaceId,
    target: ResolvedTarget,
}

impl SnapshotContext {
    fn fail(&self, kind: CommandKind, error: impl Into<crate::error::CliError>) -> CommandFailure {
        fail_for(kind, &self.profile_name, &self.mode, error)
    }

    fn output(&self, kind: CommandKind, data: CommandData) -> CommandOutput {
        CommandOutput {
            kind,
            profile: Some(self.profile_name.clone()),
            mode: Some(self.mode.clone()),
            data,
        }
    }
}

async fn resolve_snapshot_context(
    kind: CommandKind,
    config_path: &Path,
    target: &SnapshotTargetArgs,
) -> Result<SnapshotContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, target.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace_id = parse_namespace_id(&target.namespace_id)
        .map_err(|error| error.with_param("namespace_id"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    Ok(SnapshotContext {
        profile_name: resolved.profile_name,
        mode,
        namespace_id,
        target: resolved.target,
    })
}

fn parse_snapshot_id(value: &str) -> Result<CheckpointId, BackendError> {
    CheckpointId::parse(value).map_err(|error| {
        BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
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
        .create_snapshot(&context.namespace_id, &args.name, args.ttl_ms)
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
    let mut plan = PagePlan::new(&args.pagination);
    let mut cursor = args.cursor;
    let mut response: Option<loonfs_api::v0::ListSnapshotsResponse> = None;
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    loop {
        let page = context
            .target
            .list_snapshots_page(
                &context.namespace_id,
                plan.request_size(),
                cursor.as_deref(),
            )
            .await
            .map_err(|error| context.fail(kind, error))?;
        plan.record(page.snapshots.len());
        cursor = page.next_cursor.clone();
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &page.snapshots)
                .map_err(crate::error::CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else if let Some(response) = response.as_mut() {
            response.snapshots.extend(page.snapshots);
            response.next_cursor = page.next_cursor;
        } else {
            response = Some(page);
        }
        if !plan.should_continue(cursor.is_some()) {
            break;
        }
    }
    if args.pagination.jsonl {
        return Ok(context.output(kind, CommandData::StreamedToStdout));
    }
    Ok(context.output(
        kind,
        CommandData::SnapshotsListed(
            response.expect("snapshot loop should fetch at least one page"),
        ),
    ))
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
        .extend_snapshot(&context.namespace_id, &snapshot_id, args.ttl_ms)
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
        .release_snapshot(&context.namespace_id, &snapshot_id)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::SnapshotReleased(response)))
}
