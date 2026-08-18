//! Grep index-building and garbage-collection jobs for the runtime
//! maintenance runner.
//!
//! Each job performs one bounded operation against durable state and reports
//! a scheduling conclusion. The shared runner provides admission, permits,
//! backoff, and shutdown; grep does not create another scheduler.

use crate::root::{load_grep_root, GrepLifecycle};
use crate::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepGcOptions, GrepGcReport,
    GrepReorganizeOutcome, GrepWorker,
};
use loonfs::{
    current_time_ms, MaintenanceJob, MaintenanceJobId, MaintenanceProbe, MaintenanceStepConclusion,
    MaintenanceStepReport, NamespaceId, Result, RuntimeError,
};
use loonfs_api::ErrorCode;
use loonfs_objectstore::ObjectStore;

/// Identity of the grep-index job wherever it is registered.
pub const GREP_INDEX_JOB: MaintenanceJobId = MaintenanceJobId::new("grep-index");
/// Identity of the grep-collection job wherever it is registered.
pub const GREP_GC_JOB: MaintenanceJobId = MaintenanceJobId::new("grep-gc");

/// One change is all a probe needs to see to know there is work.
const PROBE_CHANGE_LIMIT: usize = 1;

/// Keeps one namespace's grep index moving, one bounded step at a time.
///
/// The job owns the step policy — how many revisions a build examines, how
/// many rows a reorganization merges — because those bound the unit of work
/// it performs. It owns nothing about when that unit runs.
#[derive(Debug, Clone)]
pub struct GrepMaintenanceJob<S> {
    worker: GrepWorker<S>,
    policy: GramIndexBuildPolicy,
}

impl<S: ObjectStore + Clone> GrepMaintenanceJob<S> {
    /// Creates the executor a host registers, over the worker that owns
    /// grep's durable keyspace.
    pub fn new(worker: GrepWorker<S>, policy: GramIndexBuildPolicy) -> Self {
        Self { worker, policy }
    }
}

#[async_trait::async_trait]
impl<S: ObjectStore + Clone + Send + Sync + 'static> MaintenanceJob for GrepMaintenanceJob<S> {
    fn id(&self) -> MaintenanceJobId {
        GREP_INDEX_JOB
    }

    /// The index is a projection of the namespace's own history, so a
    /// publication is its one real trigger and the cheapest possible hint.
    /// Subscribing here is all it takes: no host wires anything to the
    /// write path on grep's behalf.
    fn nudged_by_publications(&self) -> bool {
        true
    }

    /// Runs one bounded build step, then one reorganization step only when the
    /// index is caught up.
    ///
    /// Catch-up takes priority because it affects query completeness;
    /// reorganization only improves read cost. Progress is scheduled again so a
    /// backlog is drained through repeated bounded steps.
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepReport> {
        let build = match self.worker.build_step(namespace_id, self.policy).await {
            Ok(report) => report.outcome,
            Err(error) if has_nothing_to_index(&error) => return Ok(not_enabled_step()),
            Err(error) => return Err(step_failure(namespace_id, "grep_build", error)),
        };
        let GrepBuildOutcome::UpToDate { .. } = build else {
            return Ok(MaintenanceStepReport::concluded(build_conclusion(&build)));
        };
        let reorganize = match self.worker.reorganize_step(namespace_id, self.policy).await {
            Ok(report) => report.outcome,
            Err(error) if has_nothing_to_index(&error) => return Ok(not_enabled_step()),
            Err(error) => return Err(step_failure(namespace_id, "grep_reorganize", error)),
        };
        Ok(MaintenanceStepReport::concluded(reorganize_conclusion(
            &reorganize,
        )))
    }

    /// Reports whether the index is behind its namespace. This reads the
    /// grep root and, for an active index at a commit boundary, at most one
    /// page of the change feed.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        let Some(root) = load_grep_root(self.worker.store(), namespace_id)
            .await
            .map_err(|error| probe_failure(namespace_id, GrepError::from(error)))?
        else {
            return Ok(MaintenanceProbe::Idle);
        };
        match root.manifest_state().lifecycle() {
            // Nothing to maintain: the runner forgets this namespace until
            // an enable nudges it back.
            GrepLifecycle::Disabled => Ok(MaintenanceProbe::Idle),
            // A backfill always has its next page to walk.
            GrepLifecycle::Backfilling { .. } => Ok(MaintenanceProbe::Due),
            // A watermark inside a commit has the rest of that commit left,
            // which no question about later commits would reveal.
            GrepLifecycle::Active {
                next_event_index, ..
            } if *next_event_index != 0 => Ok(MaintenanceProbe::Due),
            GrepLifecycle::Active {
                built_through_seq, ..
            } => {
                let built_through_seq = *built_through_seq;
                match self
                    .worker
                    .reads(namespace_id)
                    .list_changes_after(built_through_seq, PROBE_CHANGE_LIMIT)
                    .await
                {
                    Ok(changes) if changes.changes.is_empty() => Ok(MaintenanceProbe::Idle),
                    Ok(_) => Ok(MaintenanceProbe::Due),
                    // The watermark fell below the retention floor: the next
                    // step rebuilds from a fresh checkpoint, which is work.
                    Err(error) if error.code() == ErrorCode::RebootstrapRequired => {
                        Ok(MaintenanceProbe::Due)
                    }
                    Err(error) if has_nothing_to_index(&error) => Ok(MaintenanceProbe::Idle),
                    Err(error) => Err(probe_failure(namespace_id, error)),
                }
            }
        }
    }
}

fn not_enabled_step() -> MaintenanceStepReport {
    MaintenanceStepReport::concluded(MaintenanceStepConclusion::NotEnabled)
}

/// Runs one bounded grep garbage-collection pass.
///
/// The continuation stores the enumeration cursor. Each pass reloads live
/// references from durable state, so a missing cursor can only repeat work.
#[derive(Debug, Clone)]
pub struct GrepGcJob<S> {
    worker: GrepWorker<S>,
}

impl<S: ObjectStore + Clone> GrepGcJob<S> {
    /// Creates a grep garbage-collection maintenance job.
    pub fn new(worker: GrepWorker<S>) -> Self {
        Self { worker }
    }
}

#[async_trait::async_trait]
impl<S: ObjectStore + Clone + Send + Sync + 'static> MaintenanceJob for GrepGcJob<S> {
    fn id(&self) -> MaintenanceJobId {
        GREP_GC_JOB
    }

    async fn step(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceStepReport> {
        let request = GrepGcOptions {
            // Use the default per-step object limit.
            max_objects: None,
            cursor: continuation.map(str::to_owned),
        };
        let now_ms = current_time_ms()?;
        let report = match self
            .worker
            .garbage_collect_namespace(namespace_id, now_ms, &request)
            .await
        {
            Ok(report) => report,
            Err(error) if continuation.is_some() && error.code() == ErrorCode::InvalidRequest => {
                // With every other option fixed, the one thing this pass can
                // be asked to reject is the cursor it was resumed with.
                // Concluding without one hands the runner an empty
                // continuation and takes the key again, which restarts
                // enumeration — always sound, because every pass rebuilds
                // its own safety proof from durable state.
                tracing::info!(
                    namespace_id = %namespace_id,
                    error = %error,
                    "grep collection rejected its resume position; restarting the pass"
                );
                return Ok(MaintenanceStepReport::concluded(
                    MaintenanceStepConclusion::Superseded,
                ));
            }
            Err(error) => return Err(step_failure(namespace_id, "grep_gc", error)),
        };
        Ok(grep_gc_step_result(report, continuation))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        // Collection has no cheap question: whether anything is reclaimable
        // is what a pass finds out, and a pass is not a probe. Grep's
        // reclamation is explicit and per namespace, so what brings this job
        // back is somebody asking for it.
        Ok(MaintenanceProbe::Idle)
    }
}

/// Converts a grep GC report into a scheduling conclusion.
///
/// A changed cursor means enumeration advanced and should continue. An
/// unchanged cursor means the pass made no progress and should park. A
/// completed pass runs again only after deleting objects. Incomplete
/// liveness information is reported as blocked rather than idle.
fn grep_gc_step_result(
    report: GrepGcReport,
    submitted_cursor: Option<&str>,
) -> MaintenanceStepReport {
    let conclusion = match report.next_cursor.as_deref() {
        Some(next_cursor) if Some(next_cursor) == submitted_cursor => {
            MaintenanceStepConclusion::Blocked
        }
        Some(_) => MaintenanceStepConclusion::Progressed,
        None if report.deleted_segments > 0 || report.deleted_other_objects > 0 => {
            MaintenanceStepConclusion::Progressed
        }
        None if report.namespace_degraded => MaintenanceStepConclusion::Blocked,
        None => MaintenanceStepConclusion::Idle,
    };
    MaintenanceStepReport {
        conclusion,
        continuation: report.next_cursor,
        // Grep objects age against one fixed grace window rather than
        // against leases a write path plants, so a pass observes no deadline
        // to hand back. What brings the job round again is a nudge.
        not_before_ms: None,
    }
}

/// Namespace states with no index to maintain: one that was never created,
/// and one whose tombstone leaves nothing to index.
fn has_nothing_to_index(error: &GrepError) -> bool {
    matches!(
        error.code(),
        ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
    )
}

/// What one bounded build accomplished.
fn build_conclusion(outcome: &GrepBuildOutcome) -> MaintenanceStepConclusion {
    match outcome {
        GrepBuildOutcome::NotEnabled => MaintenanceStepConclusion::NotEnabled,
        GrepBuildOutcome::UpToDate { .. } => MaintenanceStepConclusion::Idle,
        GrepBuildOutcome::Published { .. } | GrepBuildOutcome::BackfillRestarted { .. } => {
            MaintenanceStepConclusion::Progressed
        }
        GrepBuildOutcome::Superseded => MaintenanceStepConclusion::Superseded,
    }
}

/// Maps one bounded reorganization outcome to a scheduling result.
///
/// Reorganization publishes whatever fits within its budget, so it has no
/// zero-progress `Blocked` result.
fn reorganize_conclusion(outcome: &GrepReorganizeOutcome) -> MaintenanceStepConclusion {
    match outcome {
        GrepReorganizeOutcome::NotEnabled => MaintenanceStepConclusion::NotEnabled,
        GrepReorganizeOutcome::NotNeeded { .. } => MaintenanceStepConclusion::Idle,
        GrepReorganizeOutcome::UnitPublished { .. } => MaintenanceStepConclusion::Progressed,
        GrepReorganizeOutcome::Superseded => MaintenanceStepConclusion::Superseded,
    }
}

/// Carries a grep failure to the runner, which logs it and backs the key
/// off. The runtime's error vocabulary has no grep variants, so the phase
/// and the namespace ride in the message rather than being dropped.
fn step_failure(namespace_id: &NamespaceId, phase: &'static str, error: GrepError) -> RuntimeError {
    match error {
        GrepError::Runtime(error) => error,
        error => RuntimeError::RuntimeTask(format!(
            "{phase} step failed for namespace `{namespace_id}`: {error}"
        )),
    }
}

fn probe_failure(namespace_id: &NamespaceId, error: GrepError) -> RuntimeError {
    step_failure(namespace_id, "grep_probe", error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::ChangeSeq;

    #[test]
    fn a_caught_up_index_is_idle_and_a_disabled_one_is_not_enabled() {
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::UpToDate {
                built_through_seq: ChangeSeq(7)
            }),
            MaintenanceStepConclusion::Idle
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::NotNeeded {
                l0_runs: 1,
                mid_runs: 0
            }),
            MaintenanceStepConclusion::Idle
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::NotEnabled),
            MaintenanceStepConclusion::NotEnabled
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::NotEnabled),
            MaintenanceStepConclusion::NotEnabled
        );
    }

    #[test]
    fn every_publication_progresses_and_a_lost_race_is_superseded() {
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::Published {
                built_through_seq: ChangeSeq(3),
                indexed_revisions: 2,
                skipped_revisions: 0,
                segments_written: 1,
                materialized: false,
            }),
            MaintenanceStepConclusion::Progressed
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::BackfillRestarted {
                target_seq: ChangeSeq(9)
            }),
            MaintenanceStepConclusion::Progressed,
            "a restarted backfill discarded a dead projection and published a fresh basis"
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::UnitPublished {
                merged_rows: 128,
                segments_written: 1,
                completed: false,
            }),
            MaintenanceStepConclusion::Progressed
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::Superseded),
            MaintenanceStepConclusion::Superseded
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::Superseded),
            MaintenanceStepConclusion::Superseded
        );
    }

    /// Where the enumeration got to is the whole conclusion, so a pass that
    /// hands back the position it was given has nothing to gain from being
    /// run again at once.
    #[test]
    fn a_collection_pass_concludes_on_where_its_enumeration_reached() {
        let stopped = grep_gc_step_result(
            GrepGcReport {
                next_cursor: Some("second-page".to_owned()),
                ..GrepGcReport::default()
            },
            Some("first-page"),
        );
        assert_eq!(stopped.conclusion, MaintenanceStepConclusion::Progressed);
        assert_eq!(stopped.continuation.as_deref(), Some("second-page"));

        assert_eq!(
            grep_gc_step_result(
                GrepGcReport {
                    next_cursor: Some("first-page".to_owned()),
                    ..GrepGcReport::default()
                },
                Some("first-page"),
            )
            .conclusion,
            MaintenanceStepConclusion::Blocked
        );
    }

    /// A finished walk reports what it freed, and a namespace it could not
    /// read parks rather than passing for idle.
    #[test]
    fn a_finished_pass_separates_reclamation_from_an_unreadable_namespace() {
        assert_eq!(
            grep_gc_step_result(GrepGcReport::default(), None).conclusion,
            MaintenanceStepConclusion::Idle
        );
        assert_eq!(
            grep_gc_step_result(
                GrepGcReport {
                    deleted_segments: 2,
                    ..GrepGcReport::default()
                },
                None,
            )
            .conclusion,
            MaintenanceStepConclusion::Progressed
        );
        assert_eq!(
            grep_gc_step_result(
                GrepGcReport {
                    namespace_degraded: true,
                    retained_candidates: 3,
                    ..GrepGcReport::default()
                },
                None,
            )
            .conclusion,
            MaintenanceStepConclusion::Blocked
        );
    }

    #[test]
    fn a_grep_failure_keeps_its_message_and_a_runtime_one_stays_itself() {
        let namespace_id = loonfs_test_support::ids::namespace_id("demo");
        let corrupt = step_failure(
            &namespace_id,
            "grep_build",
            GrepError::CorruptIndex {
                message: "segment header did not decode".to_owned(),
            },
        );
        assert_eq!(corrupt.code(), ErrorCode::ServerError);
        assert!(
            corrupt
                .to_string()
                .contains("segment header did not decode")
                && corrupt.to_string().contains("grep_build")
                && corrupt.to_string().contains("demo"),
            "the runner's log line is the only place this failure is described: {corrupt}"
        );

        let runtime = step_failure(
            &namespace_id,
            "grep_build",
            GrepError::Runtime(RuntimeError::Config("bad".to_owned())),
        );
        assert_eq!(
            runtime.code(),
            ErrorCode::InvalidRequest,
            "a failure the runtime raised keeps the runtime's own code"
        );
    }
}
