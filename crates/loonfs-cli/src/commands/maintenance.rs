//! `loonfs maintenance` commands: checkpoints, retention, GC, indexes, and the
//! change feed.

use super::context::{parse_public_ordinal_arg, resolve_command_context, resolve_profile_context};
use super::output::{
    CommandData, CommandFailure, CommandOutput, MaintenanceKeyReport, MaintenanceRan,
};
use super::pagination::{collect_or_stream_pages, PagePlan, PagedListing};
use crate::args::{
    ChangesArgs, CommandKind, MaintenanceCheckpointArgs, MaintenanceCheckpointCommand,
    MaintenanceCheckpointListArgs, MaintenanceCheckpointReleaseArgs, MaintenanceCommand,
    MaintenanceGcArgs, MaintenanceIndexCommand, MaintenanceIndexEnableArgs, MaintenanceIndexGcArgs,
    MaintenanceJobArg, MaintenanceLoopArgs, MaintenanceMetadataArgs, MaintenanceNamespaceArgs,
    MaintenanceRetentionCommand, MaintenanceStoreCommand, MaintenanceStoreProbeArgs,
    RuntimeBehavior,
};
use crate::backend::{MaintenanceKeyProgress, StepBudget};
use crate::resolve::parse_namespace_id;
use clap::ValueEnum;
use loonfs::{MaintenanceJobId, NamespaceId};
use loonfs_api::v0::{GrepGcRequest, GrepIndexLifecycle};
use loonfs_api::{
    AdvanceRetentionRequest, ChangeSeq, CheckpointId, CreateCheckpointRequest, ErrorCode,
    GcRequest, MetadataCompactionRequest, MetadataMaintenanceRequest, RunMaintenanceRequest,
};
use loonfs_grep::{GREP_GC_JOB, GREP_INDEX_JOB};
use std::collections::BTreeSet;
use std::path::Path;

// --- maintenance API group ---

pub(crate) async fn run_maintenance_command(
    kind: CommandKind,
    config_path: &Path,
    command: MaintenanceCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        MaintenanceCommand::Loop(args) => run_maintenance_loop(kind, config_path, args).await,
        MaintenanceCommand::Metadata(args) => {
            run_maintenance_metadata(kind, config_path, args).await
        }
        MaintenanceCommand::Flush(args) => run_maintenance_flush(kind, config_path, args).await,
        MaintenanceCommand::Compact(args) => run_maintenance_compact(kind, config_path, args).await,
        MaintenanceCommand::Checkpoint { command } => match command {
            MaintenanceCheckpointCommand::Create(args) => {
                run_maintenance_checkpoint(kind, config_path, args).await
            }
            MaintenanceCheckpointCommand::List(args) => {
                run_maintenance_checkpoint_list(kind, config_path, args).await
            }
            MaintenanceCheckpointCommand::Release(args) => {
                run_maintenance_checkpoint_release(kind, config_path, args).await
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
            MaintenanceIndexCommand::Gc(args) => {
                run_maintenance_index_gc(kind, config_path, args, runtime).await
            }
        },
        MaintenanceCommand::Retention { command } => match command {
            MaintenanceRetentionCommand::Advance(args) => {
                run_maintenance_retention_advance(kind, config_path, args).await
            }
        },
        MaintenanceCommand::Gc(args) => run_maintenance_gc(kind, config_path, args).await,
        MaintenanceCommand::Store { command } => match command {
            MaintenanceStoreCommand::Probe(args) => {
                run_maintenance_store_probe(kind, config_path, args).await
            }
        },
    }
}

async fn run_maintenance_metadata(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceMetadataArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let request = RunMaintenanceRequest::Metadata(MetadataMaintenanceRequest {
        max_wal_tail_segments: args.max_wal_tail_segments,
    });
    let response = context
        .target
        .run_maintenance(context.namespace(), request)
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
        .run_maintenance(
            context.namespace(),
            RunMaintenanceRequest::Gc(GcRequest {
                grace_window_ms: args.grace_window_ms,
            }),
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
        .create_checkpoint(context.namespace(), request)
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
        PagePlan::new(&args.pagination),
        args.pagination.cursor.clone(),
        args.pagination.jsonl,
        async |cursor, limit| {
            context
                .target
                .list_checkpoints_page(context.namespace(), limit, cursor.as_deref())
                .await
        },
        |_: &loonfs_api::ListCheckpointsResponse| {},
    )
    .await
    .map_err(|error| context.fail(kind, error))?;
    let PagedListing::Collected(response) = listing else {
        return Ok(context.output(kind, CommandData::StreamedToStdout));
    };

    Ok(context.output(kind, CommandData::CheckpointsListed(response)))
}

async fn run_maintenance_checkpoint_release(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceCheckpointReleaseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let checkpoint_id = CheckpointId::parse(&args.checkpoint_id).map_err(|error| {
        context.fail(
            kind,
            crate::error::CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                .with_param("checkpoint_id"),
        )
    })?;
    let response = context
        .target
        .release_checkpoint(context.namespace(), &checkpoint_id)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::CheckpointReleased(response)))
}

/// One metadata-upkeep pass at a threshold of one segment.
///
/// The fold an operator asks for explicitly runs whatever the tail length,
/// and the reorganization unit rides along: upkeep is one action, and the
/// output reports both halves.
async fn run_maintenance_flush(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .run_maintenance(
            context.namespace(),
            RunMaintenanceRequest::Metadata(MetadataMaintenanceRequest {
                max_wal_tail_segments: Some(1),
            }),
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
        .run_maintenance(
            context.namespace(),
            RunMaintenanceRequest::MetadataCompaction(MetadataCompactionRequest {}),
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
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let retention_floor_before = context
        .target
        .get_namespace(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?
        .retention_floor_seq;
    let response = context
        .target
        .run_maintenance(
            context.namespace(),
            RunMaintenanceRequest::Retention(AdvanceRetentionRequest {}),
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

/// Runs maintenance for explicitly assigned namespaces. The command runs
/// until stopped, or completes the current assignments once with `--drain`.
async fn run_maintenance_loop(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceLoopArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let context =
        resolve_profile_context(kind, config_path, explicit_profile, args.request.no_retry).await?;
    let namespaces = args
        .namespaces
        .iter()
        .map(|namespace| {
            parse_namespace_id(namespace).map_err(|error| error.with_param("--namespaces"))
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| context.fail(kind, error))?;
    // Sort and deduplicate assignments for stable execution and reporting.
    let namespaces: Vec<NamespaceId> = namespaces.into_iter().collect();
    let jobs = selected_jobs(&args.jobs);
    let fail_here = |error| context.fail(kind, error);

    let job_names: Vec<String> = jobs.iter().map(|job| job.as_str().to_owned()).collect();
    let data = if args.drain {
        let budget = StepBudget {
            max_steps: args.max_steps,
            deadline_ms: args.deadline_ms,
        };
        let progress = context
            .target
            .drain_maintenance(&namespaces, &jobs, budget)
            .await
            .map_err(fail_here)?;
        CommandData::MaintenanceDrained {
            namespaces,
            jobs: job_names,
            keys: progress.keys.iter().map(key_report).collect(),
            steps: progress.steps,
            budget_exhausted: progress.budget_exhausted(),
        }
    } else {
        context
            .target
            .host_maintenance(&namespaces, &jobs, args.poll_interval_ms, shutdown_signal())
            .await
            .map_err(fail_here)?;
        CommandData::MaintenanceHosted {
            namespaces,
            jobs: job_names,
        }
    };

    Ok(context.output(kind, data))
}

/// Checks that the profile's object store supports the operations LoonFS
/// requires. This command checks the store, not a namespace.
async fn run_maintenance_store_probe(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceStoreProbeArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let context =
        resolve_profile_context(kind, config_path, explicit_profile, args.request.no_retry).await?;
    let response = context
        .target
        .probe_store()
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::StoreProbed(response)))
}

/// Returns the selected jobs in a stable order without duplicates.
/// An empty selection enables every available job.
fn selected_jobs(requested: &[MaintenanceJobArg]) -> Vec<MaintenanceJobId> {
    MaintenanceJobArg::value_variants()
        .iter()
        .filter(|job| requested.is_empty() || requested.contains(job))
        .map(|job| job_id(*job))
        .collect()
}

fn job_id(job: MaintenanceJobArg) -> MaintenanceJobId {
    match job {
        MaintenanceJobArg::Metadata => MaintenanceJobId::METADATA,
        MaintenanceJobArg::MetadataCompaction => MaintenanceJobId::METADATA_COMPACTION,
        MaintenanceJobArg::Gc => MaintenanceJobId::GC,
        MaintenanceJobArg::GrepIndex => GREP_INDEX_JOB,
        MaintenanceJobArg::GrepGc => GREP_GC_JOB,
    }
}

fn key_report(key: &MaintenanceKeyProgress) -> MaintenanceKeyReport {
    MaintenanceKeyReport {
        namespace_id: key.namespace_id.clone(),
        job: key.job.as_str().to_owned(),
        steps: key.steps,
        conclusion: key
            .conclusion
            .map(|conclusion| conclusion.as_str().to_owned()),
        settled: key.settled(),
    }
}

/// Resolves on ctrl-c or, on unix, SIGTERM — the stop an orchestrator sends
/// before a kill. The clean shutdown behind it is the writer's own.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("ctrl-c handler should install");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler should install")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        _ = terminate => {}
    }
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
        .map(CheckpointId::parse)
        .transpose()
        .map_err(|error| {
            context.fail(
                kind,
                crate::error::CliError::new(
                    ErrorCode::InvalidRequest.as_str(),
                    format!("invalid --snapshot-id: {error}"),
                )
                .with_param("--snapshot-id"),
            )
        })?;
    let listing = collect_or_stream_pages(
        PagePlan::for_sequence(&args.pagination),
        Some(after_seq),
        args.pagination.jsonl,
        async |cursor, limit| {
            context
                .target
                .list_changes(
                    context.namespace(),
                    cursor.expect("change page collection should carry a sequence"),
                    limit,
                    snapshot_id.as_ref(),
                )
                .await
        },
        |_: &loonfs_api::v0::ListChangesResponse| {},
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
        .enable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    let target_seq = match (args.no_wait, &response.lifecycle) {
        // Nothing to wait for: the caller opted out, or the index is
        // disabled, which enable would have changed if it could.
        (true, _) | (_, GrepIndexLifecycle::Disabled) => None,
        // A backfill already names the namespace sequence its checkpoint
        // captured, and reaching it is what completes the backfill.
        (_, GrepIndexLifecycle::Backfilling { target_seq, .. }) => Some(*target_seq),
        // An active index is asked to catch up to where the namespace is
        // now: one read, before any stepping, so an index that is already
        // there returns without doing anything.
        (_, GrepIndexLifecycle::Active { .. }) => Some(
            context
                .target
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
        .get_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::GrepIndexStatus(response)))
}

/// Collects the namespace's grep keyspace, looping the cursor exactly like
/// Grep collection runs bounded passes through completion, unless `--max-objects`
/// asks for one pass and its resume token.
async fn run_maintenance_index_gc(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceIndexGcArgs,
    _runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .gc_grep_index(context.namespace(), &GrepGcRequest {})
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::GrepIndexCollected(response)))
}

async fn run_maintenance_index_disable(
    kind: CommandKind,
    config_path: &Path,
    args: MaintenanceNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .disable_grep_index(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(context.output(kind, CommandData::GrepIndexDisabled(response)))
}
