//! `loonfs admin` commands: checkpoints, retention, GC, indexes, and the
//! change feed.

use super::context::{fail, fail_for, resolve_command_context};
use super::output::{CommandData, CommandFailure, CommandOutput, MaintenanceKeyReport};
use crate::args::{
    AdminCheckpointArgs, AdminCheckpointReleaseArgs, AdminCommand, AdminGcArgs,
    AdminIndexEnableArgs, AdminIndexGcArgs, AdminNamespaceArgs, AdminProbeStoreArgs, AdminRunArgs,
    AdminStepArgs, ChangesArgs, CommandKind, MaintenanceJobArg, RuntimeBehavior,
};
use crate::backend::{MaintenanceKeyProgress, StepBudget};
use crate::render::{format_utc_ms, write_stderr_progress};
use crate::resolve::{parse_namespace_id, resolve_target_profile};
use clap::ValueEnum;
use loonfs::{MaintenanceJobId, NamespaceId};
use loonfs_api::v0::{GrepGcRequest, GrepIndexLifecycle};
use loonfs_api::{
    ChangeSeq, CheckpointId, CreateCheckpointRequest, ErrorCode, GcRequest, MaintenanceStepKind,
    MaintenanceStepRequest,
};
use loonfs_grep::{GREP_GC_JOB, GREP_INDEX_JOB};
use std::collections::BTreeSet;
use std::path::Path;

// --- maintenance/admin plane ---

pub(crate) async fn run_admin_command(
    kind: CommandKind,
    config_path: &Path,
    command: AdminCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        AdminCommand::Checkpoint(args) => run_admin_checkpoint(kind, config_path, args).await,
        AdminCommand::CheckpointList(args) => {
            run_admin_checkpoint_list(kind, config_path, args).await
        }
        AdminCommand::CheckpointRelease(args) => {
            run_admin_checkpoint_release(kind, config_path, args).await
        }
        AdminCommand::Flush(args) => run_admin_flush(kind, config_path, args).await,
        AdminCommand::RetentionAdvance(args) => {
            run_admin_retention_advance(kind, config_path, args).await
        }
        AdminCommand::Run(args) => run_admin_run(kind, config_path, args).await,
        AdminCommand::Step(args) => run_admin_step(kind, config_path, args).await,
        AdminCommand::Gc(args) => run_admin_gc(kind, config_path, args, runtime).await,
        AdminCommand::ProbeStore(args) => run_admin_probe_store(kind, config_path, args).await,
        AdminCommand::IndexEnable(args) => run_admin_index_enable(kind, config_path, args).await,
        AdminCommand::IndexDisable(args) => run_admin_index_disable(kind, config_path, args).await,
        AdminCommand::IndexStatus(args) => run_admin_index_status(kind, config_path, args).await,
        AdminCommand::IndexGc(args) => run_admin_index_gc(kind, config_path, args, runtime).await,
    }
}

async fn run_admin_step(
    kind: CommandKind,
    config_path: &Path,
    args: AdminStepArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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

/// Sweeps the namespace, looping the cursor through completion unless
/// `--max-objects` asks for one pass.
///
/// A run that takes several passes says where it has got to as each pass
/// lands, on standard error: the summary on standard output is still one
/// accumulated report per invocation, whether the run took one pass or
/// twenty, and `--json` is untouched by the progress. A single-pass run
/// prints no progress at all — its summary is the whole story.
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
        cursor: None,
    };
    let mut progress = PassProgress::new(runtime);
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
        progress.pass_completed(gc_pass_line(&pass));
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

/// Holds the passes of a cursor loop until there are at least two of them.
///
/// The first pass's line is written only once a second pass proves the run
/// is a multi-pass one, so a single-pass run stays as quiet as it always
/// was. Nothing is written at all under `--json`, where standard output is
/// the whole answer.
struct PassProgress {
    enabled: bool,
    held_first_line: Option<String>,
    passes: u64,
}

impl PassProgress {
    fn new(runtime: RuntimeBehavior) -> Self {
        Self {
            enabled: !runtime.json,
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

/// What one collection pass did, in the terms an operator watching a long
/// run needs: what went, what stayed and mostly why, and when the next thing
/// this pass kept becomes reclaimable.
fn gc_pass_line(pass: &loonfs_api::GcResponse) -> String {
    let deleted = pass.deleted_wal_segments
        + pass.deleted_metadata_tables
        + pass.deleted_manifests
        + pass.deleted_checkpoint_records
        + pass.deleted_upload_sessions
        + pass.deleted_content_objects;
    let mut line = format!("{deleted} deleted, {} retained", pass.retained_candidates);
    if let Some((reason, count)) = pass.retained.top_reason() {
        line.push_str(&format!(" (mostly {reason}: {count})"));
    }
    if let Some(at_ms) = pass.next_reclamation_at_ms {
        line.push_str(&format!("; next reclaimable at {}", format_utc_ms(at_ms)));
    }
    line
}

fn accumulate_gc_response(total: &mut loonfs_api::GcResponse, pass: loonfs_api::GcResponse) {
    total.deleted_wal_segments += pass.deleted_wal_segments;
    total.deleted_metadata_tables += pass.deleted_metadata_tables;
    total.deleted_manifests += pass.deleted_manifests;
    total.deleted_checkpoint_records += pass.deleted_checkpoint_records;
    total.released_fork_checkpoints += pass.released_fork_checkpoints;
    total.released_expired_checkpoints += pass.released_expired_checkpoints;
    total.deleted_upload_sessions += pass.deleted_upload_sessions;
    total.deleted_content_objects += pass.deleted_content_objects;
    total.released_missing_basis_checkpoints += pass.released_missing_basis_checkpoints;
    total.retained_candidates += pass.retained_candidates;
    total.retained.add(&pass.retained);
    total.degraded_retention |= pass.degraded_retention;
    total.content_reclamation_deferred |= pass.content_reclamation_deferred;
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
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
    let response = context
        .target
        .list_checkpoints(&context.namespace)
        .await
        .map_err(|error| context.fail(kind, error))?;

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
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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
    config_path: &Path,
    args: AdminRunArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.profile.no_retry)
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
    config_path: &Path,
    args: AdminProbeStoreArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.profile.no_retry)
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
    config_path: &Path,
    args: AdminIndexEnableArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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
    config_path: &Path,
    args: AdminNamespaceArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, config_path, &args.target).await?;
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
        cursor: None,
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

    /// The multi-pass summary folds every counter and keeps the soonest
    /// reclamation obligation any pass reported: a later pass with nothing
    /// deferred does not erase an earlier pass's pending horizon.
    #[test]
    fn the_summary_folds_expired_releases_and_keeps_the_soonest_horizon() {
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        let mut total = GcResponse::empty(namespace.clone());

        let mut first = GcResponse::empty(namespace.clone());
        first.released_expired_checkpoints = 2;
        first.next_reclamation_at_ms = Some(9_000);
        accumulate_gc_response(&mut total, first);

        let mut second = GcResponse::empty(namespace.clone());
        second.released_expired_checkpoints = 1;
        accumulate_gc_response(&mut total, second);

        let mut third = GcResponse::empty(namespace);
        third.next_reclamation_at_ms = Some(12_000);
        accumulate_gc_response(&mut total, third);

        assert_eq!(total.released_expired_checkpoints, 3);
        assert_eq!(total.next_reclamation_at_ms, Some(9_000));
    }

    fn runtime(json: bool) -> RuntimeBehavior {
        RuntimeBehavior {
            json,
            no_input: true,
            interactive: false,
            progress: ProgressMode::Off,
        }
    }

    /// A run that finishes in one pass has nothing to report progress about:
    /// its summary is the whole story, and a stray "pass 1" line before it
    /// would be noise on every quiet invocation.
    #[test]
    fn a_single_pass_run_reports_no_progress() {
        let mut progress = PassProgress::new(runtime(false));
        assert!(progress
            .lines_for_completed_pass("first".to_owned())
            .is_empty());
    }

    /// Once a second pass proves the run is a long one, the first pass's line
    /// is written too — so a multi-pass run accounts for every pass, in
    /// order, rather than starting the story at pass two.
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

    /// `--json` promises one machine-readable envelope and nothing else, so
    /// however many passes a run takes, it says nothing on the way.
    #[test]
    fn json_output_stays_silent_across_passes() {
        let mut progress = PassProgress::new(runtime(true));
        for pass in ["first", "second", "third"] {
            assert!(progress
                .lines_for_completed_pass(pass.to_owned())
                .is_empty());
        }
    }

    /// A progress line answers what an operator watching a sweep asks: what
    /// went, what stayed and mostly why, and when to expect the rest.
    #[test]
    fn a_pass_line_names_what_stayed_and_mostly_why() {
        let mut pass = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
        pass.deleted_wal_segments = 2;
        pass.deleted_content_objects = 1;
        for _ in 0..4 {
            pass.retain(RetainedReason::GraceWindow);
        }
        pass.retain(RetainedReason::UploadSessionWindow);
        pass.next_reclamation_at_ms = Some(1_700_000_000_000);

        assert_eq!(
            gc_pass_line(&pass),
            "3 deleted, 5 retained (mostly grace_window: 4); \
             next reclaimable at 2023-11-14 22:13:20Z"
        );
    }

    /// A pass that kept nothing says so plainly rather than inventing a
    /// reason for zero candidates.
    #[test]
    fn a_pass_line_that_kept_nothing_names_no_reason() {
        let pass = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
        assert_eq!(gc_pass_line(&pass), "0 deleted, 0 retained");
    }
}
