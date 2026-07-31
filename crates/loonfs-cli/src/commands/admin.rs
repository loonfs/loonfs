//! `loonfs admin` commands: checkpoints, retention, GC, indexes, and the
//! change feed.

use super::context::{fail, fail_for, resolve_command_context};
use super::output::{CommandData, CommandFailure, CommandOutput, MaintenanceKeyReport};
use crate::args::{
    AdminCheckpointArgs, AdminCheckpointReleaseArgs, AdminCommand, AdminGcArgs,
    AdminIndexEnableArgs, AdminIndexGcArgs, AdminNamespaceArgs, AdminProbeStoreArgs, AdminRunArgs,
    AdminStepArgs, ChangesArgs, CommandKind, MaintenanceJobArg,
};
use crate::backend::{MaintenanceKeyProgress, StepBudget};
use crate::resolve::{parse_namespace_id, resolve_target_profile};
use clap::ValueEnum;
use loonfs::{MaintenanceJobId, NamespaceId};
use loonfs_api::v0::{GrepGcRequest, GrepIndexLifecycle};
use loonfs_api::{
    ChangeSeq, CheckpointId, CreateCheckpointRequest, ErrorCode, GcRequest, MaintenanceStepKind,
    MaintenanceStepRequest,
};
use loonfs_grep::GREP_INDEX_JOB;
use std::collections::BTreeSet;

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
        AdminCommand::Run(args) => run_admin_run(kind, args).await,
        AdminCommand::Step(args) => run_admin_step(kind, args).await,
        AdminCommand::Gc(args) => run_admin_gc(kind, args).await,
        AdminCommand::ProbeStore(args) => run_admin_probe_store(kind, args).await,
        AdminCommand::IndexEnable(args) => run_admin_index_enable(kind, args).await,
        AdminCommand::IndexDisable(args) => run_admin_index_disable(kind, args).await,
        AdminCommand::IndexStatus(args) => run_admin_index_status(kind, args).await,
        AdminCommand::IndexGc(args) => run_admin_index_gc(kind, args).await,
    }
}

async fn run_admin_step(
    kind: CommandKind,
    args: AdminStepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let request = MaintenanceStepRequest {
        max_wal_tail_segments: args.max_wal_tail_segments,
        retention: args.retention.then_some(true),
        gc: args.gc.then(GcRequest::default),
        only: None,
    };
    let response = context
        .target
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
        max_objects: Some(args.max_objects.unwrap_or(loonfs::DEFAULT_GC_MAX_OBJECTS)),
        cursor: None,
    };
    let mut response = None;
    loop {
        let pass = context
            .target
            .maintenance_step(
                &context.namespace,
                MaintenanceStepRequest {
                    max_wal_tail_segments: None,
                    retention: None,
                    gc: Some(request.clone()),
                    only: Some(MaintenanceStepKind::Gc),
                },
            )
            .await
            .map_err(|error| context.fail(kind, error))?
            .gc
            .expect("gc report present when the step opted in");
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
    total.deleted_content_objects += pass.deleted_content_objects;
    total.released_missing_basis_checkpoints += pass.released_missing_basis_checkpoints;
    total.retained_candidates += pass.retained_candidates;
    total.degraded_retention |= pass.degraded_retention;
    total.content_reclamation_deferred |= pass.content_reclamation_deferred;
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
        .maintenance_step(
            &context.namespace,
            MaintenanceStepRequest {
                // The WAL flush an operator asks for explicitly runs whatever
                // the tail length, so the threshold drops to one segment.
                max_wal_tail_segments: Some(1),
                retention: None,
                gc: None,
                only: Some(MaintenanceStepKind::WalFlush),
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::MaintenanceStepped(response),
    })
}

async fn run_admin_retention_advance(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .maintenance_step(
            &context.namespace,
            MaintenanceStepRequest {
                max_wal_tail_segments: None,
                retention: None,
                gc: None,
                only: Some(MaintenanceStepKind::Retention),
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::MaintenanceStepped(response),
    })
}

/// Hosts maintenance for the namespaces named on the command line.
///
/// Nothing here discovers a namespace, because LoonFS has no operation that
/// enumerates them. The flags are the assignment, and the assignment is what
/// brings a namespace no process is writing to under automatic maintenance:
/// continuously until a signal, or as one bounded catch-up with `--drain`.
async fn run_admin_run(
    kind: CommandKind,
    args: AdminRunArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile, args.profile.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespaces = args
        .namespaces
        .iter()
        .map(|namespace| parse_namespace_id(namespace))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    // Sorted and deduplicated, so the keys are driven in the order the
    // runner itself keys them and two spellings of one assignment produce
    // one report.
    let namespaces: Vec<NamespaceId> = namespaces.into_iter().collect();
    let jobs = selected_jobs(&args.jobs);
    let fail_here = |error| fail_for(kind, &resolved.profile_name, &mode, error);

    let (keys, steps, budget_exhausted) = if args.drain {
        let budget = StepBudget {
            max_steps: args.max_steps,
            deadline_ms: args.deadline_ms,
        };
        let progress = resolved
            .target
            .drain_maintenance(&namespaces, &jobs, budget)
            .await
            .map_err(fail_here)?;
        (
            progress.keys.iter().map(key_report).collect(),
            progress.steps,
            progress.budget_exhausted(),
        )
    } else {
        resolved
            .target
            .host_maintenance(&namespaces, &jobs, args.poll_interval_ms, shutdown_signal())
            .await
            .map_err(fail_here)?;
        // A hosted run reports no per-key outcome: the runner ran the steps,
        // and where each namespace got to is durable state to read, not a
        // tally this process kept.
        (Vec::new(), 0, false)
    };

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::MaintenanceHosted {
            namespaces,
            jobs: jobs.iter().map(|job| job.as_str().to_owned()).collect(),
            drained: args.drain,
            keys,
            steps,
            budget_exhausted,
        },
    })
}

/// Proves the profile's object store honours the contract LoonFS depends
/// on. Store-scoped like `admin run`: it names no namespace, because the
/// store is the subject.
async fn run_admin_probe_store(
    kind: CommandKind,
    args: AdminProbeStoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile, args.profile.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let response = resolved
        .target
        .probe_store()
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::StoreProbed(response),
    })
}

/// The jobs to host, in the order the runner keys them, with no repeats.
/// An empty selection is every job this host knows how to run.
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
        MaintenanceJobArg::CoreGc => MaintenanceJobId::GC,
        MaintenanceJobArg::GrepIndex => GREP_INDEX_JOB,
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

pub(crate) async fn run_admin_changes(
    kind: CommandKind,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let after_seq = ChangeSeq(args.after.unwrap_or(0));
    let response = context
        .target
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

/// Enables the index and, by default, waits for it to catch up to one fixed
/// sequence.
///
/// The sequence is captured before any waiting starts and never re-read:
/// writes that land afterwards are not waited for, so a namespace that is
/// being written to cannot keep this command running.
async fn run_admin_index_enable(
    kind: CommandKind,
    args: AdminIndexEnableArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .enable_grep_index(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    let target_seq = match (args.no_wait, &response.state) {
        // Nothing to wait for: the caller opted out, or the index is
        // disabled, which enable would have changed if it could.
        (true, _) | (_, GrepIndexLifecycle::Disabled) => None,
        // A backfill already names the namespace sequence its checkpoint
        // captured, and reaching it is what completes the backfill.
        (_, GrepIndexLifecycle::Backfilling { target_seq, .. }) => Some(*target_seq),
        // A steady index is asked to catch up to where the namespace is
        // now: one read, before any stepping, so an index that is already
        // there returns without doing anything.
        (_, GrepIndexLifecycle::Steady { .. }) => Some(
            context
                .target
                .namespace_status(&context.namespace)
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
                    &context.namespace,
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

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexEnabled {
            namespace_id: response.namespace_id,
            already_enabled: response.already_enabled,
            state: waited
                .as_ref()
                .map_or(response.state, |waited| waited.state.clone()),
            waited_for_seq: target_seq,
            steps: waited.as_ref().map_or(0, |waited| waited.steps),
            budget_exhausted: waited.is_some_and(|waited| !waited.reached),
        },
    })
}

async fn run_admin_index_status(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
        .grep_index_status(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexStatus(response),
    })
}

/// Collects the namespace's grep keyspace, looping the cursor exactly like
/// `admin gc`: bounded passes through completion, unless `--max-objects`
/// asks for one pass and its resume token.
async fn run_admin_index_gc(
    kind: CommandKind,
    args: AdminIndexGcArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let single_pass = args.max_objects.is_some();
    let mut request = GrepGcRequest {
        max_objects: Some(args.max_objects.unwrap_or(loonfs::DEFAULT_GC_MAX_OBJECTS)),
        cursor: None,
    };
    let mut response: Option<loonfs_api::v0::GrepGcResponse> = None;
    loop {
        let pass = context
            .target
            .gc_grep_index(&context.namespace, &request)
            .await
            .map_err(|error| context.fail(kind, error))?;
        let next_cursor = pass.next_cursor.clone();
        match &mut response {
            Some(total) => accumulate_grep_gc_response(total, pass),
            None => response = Some(pass),
        }
        if single_pass || next_cursor.is_none() {
            break;
        }
        request.cursor = next_cursor;
    }
    let response = response.expect("grep GC loop should run at least once");

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexCollected(response),
    })
}

fn accumulate_grep_gc_response(
    total: &mut loonfs_api::v0::GrepGcResponse,
    pass: loonfs_api::v0::GrepGcResponse,
) {
    total.deleted_segments += pass.deleted_segments;
    total.deleted_other_objects += pass.deleted_other_objects;
    total.retained_candidates += pass.retained_candidates;
    total.namespace_reaped |= pass.namespace_reaped;
    total.namespace_degraded |= pass.namespace_degraded;
    total.next_cursor = pass.next_cursor;
}

async fn run_admin_index_disable(
    kind: CommandKind,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target).await?;
    let response = context
        .target
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
