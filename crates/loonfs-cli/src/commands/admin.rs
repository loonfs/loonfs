//! `loonfs admin` commands: checkpoints, retention, GC, indexes, and the
//! change feed.

use super::context::{fail, fail_for, parse_public_ordinal_arg, resolve_command_context};
use super::output::{CommandData, CommandFailure, CommandOutput, MaintenanceKeyReport};
use super::pagination::{write_jsonl_page, PagePlan};
use crate::args::{
    AdminCheckpointArgs, AdminCheckpointCommand, AdminCheckpointListArgs,
    AdminCheckpointReleaseArgs, AdminCommand, AdminGcArgs, AdminIndexCommand, AdminIndexEnableArgs,
    AdminIndexGcArgs, AdminMaintenanceCommand, AdminNamespaceArgs, AdminRetentionCommand,
    AdminRunArgs, AdminStepArgs, AdminStoreCommand, AdminStoreProbeArgs, ChangesArgs, CommandKind,
    MaintenanceJobArg, RuntimeBehavior,
};
use crate::backend::{MaintenanceKeyProgress, StepBudget};
use crate::render::{gc_pass_line, write_stderr_progress};
use crate::resolve::{parse_namespace_id, resolve_target_profile};
use clap::ValueEnum;
use loonfs::{MaintenanceJobId, NamespaceId};
use loonfs_api::v0::{GrepGcRequest, GrepIndexLifecycle};
use loonfs_api::{
    AdvanceRetentionRequest, ChangeSeq, CheckpointId, CreateCheckpointRequest, ErrorCode,
    GcRequest, MaintenanceStepRequest, MetadataMaintenanceRequest,
};
use loonfs_grep::{GREP_GC_JOB, GREP_INDEX_JOB};
use std::collections::BTreeSet;
use std::io::{self, BufWriter};
use std::path::Path;

// --- maintenance/admin plane ---

pub(crate) async fn run_admin_command(
    kind: CommandKind,
    config_path: &Path,
    command: AdminCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        AdminCommand::Checkpoint { command } => match command {
            AdminCheckpointCommand::Create(args) => {
                run_admin_checkpoint(kind, config_path, args).await
            }
            AdminCheckpointCommand::List(args) => {
                run_admin_checkpoint_list(kind, config_path, args).await
            }
            AdminCheckpointCommand::Release(args) => {
                run_admin_checkpoint_release(kind, config_path, args).await
            }
        },
        AdminCommand::Index { command } => match command {
            AdminIndexCommand::Enable(args) => {
                run_admin_index_enable(kind, config_path, args).await
            }
            AdminIndexCommand::Disable(args) => {
                run_admin_index_disable(kind, config_path, args).await
            }
            AdminIndexCommand::Status(args) => {
                run_admin_index_status(kind, config_path, args).await
            }
            AdminIndexCommand::Gc(args) => {
                run_admin_index_gc(kind, config_path, args, runtime).await
            }
        },
        AdminCommand::Maintenance { command } => match command {
            AdminMaintenanceCommand::Run(args) => run_admin_run(kind, config_path, args).await,
            AdminMaintenanceCommand::Step(args) => run_admin_step(kind, config_path, args).await,
            AdminMaintenanceCommand::Flush(args) => run_admin_flush(kind, config_path, args).await,
        },
        AdminCommand::Retention { command } => match command {
            AdminRetentionCommand::Advance(args) => {
                run_admin_retention_advance(kind, config_path, args).await
            }
        },
        AdminCommand::Gc(args) => run_admin_gc(kind, config_path, args, runtime).await,
        AdminCommand::Store { command } => match command {
            AdminStoreCommand::Probe(args) => run_admin_store_probe(kind, config_path, args).await,
        },
    }
}

async fn run_admin_step(
    kind: CommandKind,
    config_path: &Path,
    args: AdminStepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    // A step always runs metadata maintenance. The flags add retention or
    // garbage collection when requested.
    let request = MaintenanceStepRequest {
        metadata_maintenance: Some(MetadataMaintenanceRequest {
            max_wal_tail_segments: args.max_wal_tail_segments,
        }),
        retention: args.retention.then(AdvanceRetentionRequest::default),
        gc: args.gc.then(GcRequest::default),
    };
    let response = context
        .target
        .run_maintenance(&context.namespace, request)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::MaintenanceStepped(response),
    })
}

/// Runs garbage collection until it finishes unless `--max-objects` requests
/// one bounded pass. Multi-pass human output writes progress to stderr and a
/// combined summary to stdout. JSON and single-pass output contain no progress
/// lines.
async fn run_admin_gc(
    kind: CommandKind,
    config_path: &Path,
    args: AdminGcArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let single_pass = args.max_objects.is_some();
    let mut request = GcRequest {
        grace_window_ms: args.grace_window_ms,
        max_objects: Some(args.max_objects.unwrap_or(loonfs::DEFAULT_GC_MAX_OBJECTS)),
        cursor: args.cursor,
    };
    let mut progress = PassProgress::new(runtime);
    let mut response = None;
    loop {
        let pass = context
            .target
            .run_maintenance(
                &context.namespace,
                MaintenanceStepRequest {
                    gc: Some(request.clone()),
                    ..MaintenanceStepRequest::default()
                },
            )
            .await
            .map_err(|error| context.fail(kind, error))?
            .gc
            .expect("a step selecting collection reports its pass");
        let next_cursor = pass.next_cursor.clone();
        progress.pass_completed(gc_pass_line(&pass));
        match &mut response {
            Some(total) => accumulate_gc_response(total, pass),
            None => response = Some(pass),
        }
        // A pass that hands back the cursor it was given ran out of budget
        // before deciding anything, and would again; the summary's
        // budget-exhausted line says why the drain stopped short.
        if single_pass || next_cursor.is_none() || next_cursor == request.cursor {
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

/// Holds the passes of a cursor loop until there are at least two of them.
///
/// The first pass's line is written only once a second pass proves the run
/// is a multi-pass one, so a single-pass run stays as quiet as it always
/// was. Nothing is written when progress is off or uses structured events;
/// these pass summaries are human-readable prose.
struct PassProgress {
    enabled: bool,
    held_first_line: Option<String>,
    passes: u64,
}

impl PassProgress {
    fn new(runtime: RuntimeBehavior) -> Self {
        Self {
            enabled: runtime.progress.human_lines_enabled(),
            held_first_line: None,
            passes: 0,
        }
    }

    fn pass_completed(&mut self, line: String) {
        for line in self.lines_for_completed_pass(line) {
            write_stderr_progress(line);
        }
    }

    /// The lines this completed pass adds, in the order they are written.
    /// Separate from the writing so the sequencing itself is testable.
    fn lines_for_completed_pass(&mut self, line: String) -> Vec<String> {
        self.passes += 1;
        if !self.enabled {
            return Vec::new();
        }
        if self.passes == 1 {
            self.held_first_line = Some(line);
            return Vec::new();
        }
        let mut lines = Vec::new();
        if let Some(first) = self.held_first_line.take() {
            lines.push(format!("pass 1: {first}"));
        }
        lines.push(format!("pass {}: {line}", self.passes));
        lines
    }
}

fn accumulate_gc_response(total: &mut loonfs_api::GcResponse, pass: loonfs_api::GcResponse) {
    total.deleted.add(&pass.deleted);
    total.released_checkpoints.add(&pass.released_checkpoints);
    total.retained_candidates += pass.retained_candidates;
    total.retained.add(&pass.retained);
    total.retention_degraded |= pass.retention_degraded;
    total.content_reclamation_deferred |= pass.content_reclamation_deferred;
    total.budget_exhausted |= pass.budget_exhausted;
    // The summary keeps the soonest obligation any pass reported — the same
    // soonest-wake rule the maintenance runner applies. A later pass with
    // nothing deferred does not erase an earlier pass's pending horizon.
    total.next_reclamation_at_ms = match (total.next_reclamation_at_ms, pass.next_reclamation_at_ms)
    {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    total.next_cursor = pass.next_cursor;
}

async fn run_admin_checkpoint(
    kind: CommandKind,
    config_path: &Path,
    args: AdminCheckpointArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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

async fn run_admin_checkpoint_list(
    kind: CommandKind,
    config_path: &Path,
    args: AdminCheckpointListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let mut plan = PagePlan::new(&args.pagination);
    let mut cursor = args.cursor.clone();
    let mut response: Option<loonfs_api::ListCheckpointsResponse> = None;
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    loop {
        let page = context
            .target
            .list_checkpoints_page(&context.namespace, plan.request_size(), cursor.as_deref())
            .await
            .map_err(|error| context.fail(kind, error))?;
        plan.record(page.checkpoints.len());
        cursor = page.next_cursor.clone();
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &page.checkpoints)
                .map_err(crate::error::CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else if let Some(response) = response.as_mut() {
            response.checkpoints.extend(page.checkpoints);
            response.next_cursor = page.next_cursor;
        } else {
            response = Some(page);
        }
        if !plan.should_continue(cursor.is_some()) {
            break;
        }
    }
    if args.pagination.jsonl {
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }
    let response = response.expect("checkpoint loop should fetch at least one page");

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::CheckpointsListed(response),
    })
}

async fn run_admin_checkpoint_release(
    kind: CommandKind,
    config_path: &Path,
    args: AdminCheckpointReleaseArgs,
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

/// One metadata-upkeep pass at a threshold of one segment.
///
/// The fold an operator asks for explicitly runs whatever the tail length,
/// and the reorganization unit rides along: upkeep is one action, and the
/// output reports both halves.
async fn run_admin_flush(
    kind: CommandKind,
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .run_maintenance(
            &context.namespace,
            MaintenanceStepRequest {
                metadata_maintenance: Some(MetadataMaintenanceRequest {
                    max_wal_tail_segments: Some(1),
                }),
                ..MaintenanceStepRequest::default()
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
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .run_maintenance(
            &context.namespace,
            MaintenanceStepRequest {
                retention: Some(AdvanceRetentionRequest::default()),
                ..MaintenanceStepRequest::default()
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

/// Runs maintenance for explicitly assigned namespaces. The command runs
/// until stopped, or completes the current assignments once with `--drain`.
async fn run_admin_run(
    kind: CommandKind,
    config_path: &Path,
    args: AdminRunArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespaces = args
        .namespaces
        .iter()
        .map(|namespace| {
            parse_namespace_id(namespace).map_err(|error| error.with_param("--namespaces"))
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    // Sort and deduplicate assignments for stable execution and reporting.
    let namespaces: Vec<NamespaceId> = namespaces.into_iter().collect();
    let jobs = selected_jobs(&args.jobs);
    let fail_here = |error| fail_for(kind, &resolved.profile_name, &mode, error);

    let job_names: Vec<String> = jobs.iter().map(|job| job.as_str().to_owned()).collect();
    let data = if args.drain {
        let budget = StepBudget {
            max_steps: args.max_steps,
            deadline_ms: args.deadline_ms,
        };
        let progress = resolved
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
        resolved
            .target
            .host_maintenance(&namespaces, &jobs, args.poll_interval_ms, shutdown_signal())
            .await
            .map_err(fail_here)?;
        CommandData::MaintenanceHosted {
            namespaces,
            jobs: job_names,
        }
    };

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data,
    })
}

/// Checks that the profile's object store supports the operations LoonFS
/// requires. This command checks the store, not a namespace.
async fn run_admin_store_probe(
    kind: CommandKind,
    config_path: &Path,
    args: AdminStoreProbeArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
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
        MaintenanceJobArg::CoreGc => MaintenanceJobId::GC,
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

pub(crate) async fn run_admin_changes(
    kind: CommandKind,
    config_path: &Path,
    args: ChangesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let after_seq = parse_public_ordinal_arg("--after", args.after.unwrap_or(0), ChangeSeq::parse)
        .map_err(|error| context.fail(kind, error))?;
    let mut plan = PagePlan::new(&args.pagination);
    let mut cursor = after_seq;
    let mut response: Option<loonfs_api::v0::ListChangesResponse> = None;
    let stdout = io::stdout();
    let mut stdout = BufWriter::with_capacity(64 * 1024, stdout.lock());
    loop {
        let page = context
            .target
            .list_changes(&context.namespace, cursor, plan.request_size())
            .await
            .map_err(|error| context.fail(kind, error))?;
        plan.record(page.changes.len());
        let next_after_seq = page.next_after_seq;
        if args.pagination.jsonl {
            write_jsonl_page(&mut stdout, &page.changes)
                .map_err(crate::error::CliError::io)
                .map_err(|error| context.fail(kind, error))?;
        } else if let Some(response) = response.as_mut() {
            response.through_seq = page.through_seq;
            response.next_after_seq = page.next_after_seq;
            response.changes.extend(page.changes);
        } else {
            response = Some(page);
        }
        if !plan.should_continue(next_after_seq.is_some()) {
            break;
        }
        cursor = next_after_seq.expect("continuation was checked above");
    }
    if args.pagination.jsonl {
        return Ok(CommandOutput {
            kind,
            profile: Some(context.profile_name),
            mode: Some(context.mode),
            data: CommandData::StreamedToStdout,
        });
    }
    let response = response.expect("changes loop should fetch at least one page");

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
    config_path: &Path,
    args: AdminIndexEnableArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .enable_grep_index(&context.namespace)
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
                .get_namespace(&context.namespace)
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
    let response = if waited.is_some() {
        context
            .target
            .get_grep_index(&context.namespace)
            .await
            .map_err(|error| context.fail(kind, error))?
    } else {
        response
    };

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::GrepIndexEnabled {
            response,
            waited_for_seq: target_seq,
            steps: waited.as_ref().map_or(0, |waited| waited.steps),
            budget_exhausted: waited.is_some_and(|waited| !waited.reached),
        },
    })
}

async fn run_admin_index_status(
    kind: CommandKind,
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .get_grep_index(&context.namespace)
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
    config_path: &Path,
    args: AdminIndexGcArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let single_pass = args.max_objects.is_some();
    // An omitted budget is left omitted: grep resolves it to the same
    // per-pass default the runtime uses, and one authority for that number
    // is what keeps a remote pass and an embedded one the same size.
    let mut request = GrepGcRequest {
        max_objects: args.max_objects,
        cursor: args.cursor,
    };
    let mut progress = PassProgress::new(runtime);
    let mut response: Option<loonfs_api::v0::GrepGcResponse> = None;
    loop {
        let pass = context
            .target
            .gc_grep_index(&context.namespace, &request)
            .await
            .map_err(|error| context.fail(kind, error))?;
        let next_cursor = pass.next_cursor.clone();
        progress.pass_completed(format!(
            "{} deleted, {} retained",
            pass.deleted_segments + pass.deleted_other_objects,
            pass.retained_candidates
        ));
        match &mut response {
            Some(total) => accumulate_grep_gc_response(total, pass),
            None => response = Some(pass),
        }
        if single_pass || next_cursor.is_none() || next_cursor == request.cursor {
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
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::ProgressMode;
    use loonfs_api::{GcResponse, NamespaceId, RetainedReason};

    #[test]
    fn the_summary_folds_expired_releases_and_keeps_the_soonest_horizon() {
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        let mut total = GcResponse::empty(namespace.clone());

        let mut first = GcResponse::empty(namespace.clone());
        first.released_checkpoints.expired = 2;
        first.next_reclamation_at_ms = Some(9_000);
        accumulate_gc_response(&mut total, first);

        let mut second = GcResponse::empty(namespace.clone());
        second.released_checkpoints.expired = 1;
        accumulate_gc_response(&mut total, second);

        let mut third = GcResponse::empty(namespace);
        third.next_reclamation_at_ms = Some(12_000);
        accumulate_gc_response(&mut total, third);

        assert_eq!(total.released_checkpoints.expired, 3);
        assert_eq!(total.next_reclamation_at_ms, Some(9_000));
    }

    fn runtime(json: bool) -> RuntimeBehavior {
        RuntimeBehavior {
            json,
            no_input: true,
            interactive: false,
            progress: if json {
                ProgressMode::Events
            } else {
                ProgressMode::Human
            },
        }
    }

    #[test]
    fn a_single_pass_run_reports_no_progress() {
        let mut progress = PassProgress::new(runtime(false));
        assert!(progress
            .lines_for_completed_pass("first".to_owned())
            .is_empty());
    }

    #[test]
    fn a_multi_pass_run_reports_every_pass_in_order() {
        let mut progress = PassProgress::new(runtime(false));
        assert!(progress
            .lines_for_completed_pass("first".to_owned())
            .is_empty());
        assert_eq!(
            progress.lines_for_completed_pass("second".to_owned()),
            vec!["pass 1: first".to_owned(), "pass 2: second".to_owned()]
        );
        assert_eq!(
            progress.lines_for_completed_pass("third".to_owned()),
            vec!["pass 3: third".to_owned()]
        );
    }

    #[test]
    fn json_output_stays_silent_across_passes() {
        let mut progress = PassProgress::new(runtime(true));
        for pass in ["first", "second", "third"] {
            assert!(progress
                .lines_for_completed_pass(pass.to_owned())
                .is_empty());
        }
    }

    #[test]
    fn a_pass_line_names_what_stayed_and_mostly_why() {
        let mut pass = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
        pass.deleted.wal_segments = 2;
        pass.deleted.upload_sessions = 3;
        pass.deleted.content_objects = 1;
        for _ in 0..4 {
            pass.retain(RetainedReason::WithinGraceWindow);
        }
        pass.retain(RetainedReason::UploadSessionWindow);
        pass.next_reclamation_at_ms = Some(1_700_000_000_000);

        let line = gc_pass_line(&pass);
        assert!(
            line.contains("6 deleted, 5 retained; mostly within_grace_window: 4"),
            "every deleted family counts, upload sessions included: {line}"
        );
        assert!(
            line.contains("next reclaimable at 2023-11-14 22:13:20Z"),
            "{line}"
        );

        let quiet = gc_pass_line(&GcResponse::empty(
            NamespaceId::parse("demo").expect("namespace id"),
        ));
        assert!(quiet.contains("0 deleted, 0 retained"), "{quiet}");
        assert!(!quiet.contains("mostly"), "{quiet}");
        assert!(!quiet.contains("next reclaimable"), "{quiet}");
    }
}
