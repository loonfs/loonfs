//! Grep index-building and garbage-collection jobs for the runtime
//! maintenance runner.
//!
//! Each job performs one bounded operation against durable state and reports
//! a scheduling conclusion. The shared runner provides admission, permits,
//! backoff, and shutdown; grep does not create another scheduler.

use crate::root::{load_current_grep_manifest, GrepIndexStatus};
use crate::{GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepWorker};
use loonfs::{
    current_time_ms, MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob,
    MaintenanceJobId, MaintenanceProbe, MaintenanceRunReport, NamespaceId, NamespacePublication,
    Result, RuntimeError,
};
use loonfs_api::ErrorCode;
use loonfs_objectstore::ObjectStore;

/// Identity of the grep index job wherever it is registered.
pub const GREP_INDEX_JOB: MaintenanceJobId = MaintenanceJobId::new("grep_index");
/// Identity of the grep-collection job wherever it is registered.
pub const GREP_GC_JOB: MaintenanceJobId = MaintenanceJobId::new("grep_gc");

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

    fn should_run_after_publication(&self, publication: &NamespacePublication) -> bool {
        publication.committed_through_seq.is_some()
    }

    /// Runs one bounded build step, then one reorganization step only when the
    /// index is caught up.
    ///
    /// Catch-up takes priority because it affects query completeness;
    /// reorganization only improves read cost. Progress is scheduled again so a
    /// backlog is drained through repeated bounded steps.
    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        let build = match self.worker.build_step(namespace_id, self.policy).await {
            Ok(outcome) => outcome,
            Err(error) if has_nothing_to_index(&error) => return Ok(not_enabled_step()),
            Err(error) => return Err(step_failure(namespace_id, "grep_build", error)),
        };
        let GrepBuildOutcome::UpToDate { .. } = build else {
            return Ok(MaintenanceRunReport::concluded(build_conclusion(&build)));
        };
        let reorganize = match self.worker.reorganize_step(namespace_id, self.policy).await {
            Ok(outcome) => outcome,
            Err(error) if has_nothing_to_index(&error) => return Ok(not_enabled_step()),
            Err(error) => return Err(step_failure(namespace_id, "grep_reorganize", error)),
        };
        Ok(MaintenanceRunReport::concluded(reorganize_conclusion(
            &reorganize,
        )))
    }

    /// Reports whether the index is behind its namespace. This reads the
    /// grep root and, for an active index at a commit boundary, at most one
    /// page of the change feed.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        let Some(root) = load_current_grep_manifest(self.worker.store(), namespace_id)
            .await
            .map_err(|error| probe_failure(namespace_id, GrepError::from(error)))?
        else {
            return Ok(MaintenanceProbe::Idle);
        };
        match root.manifest_state().status() {
            // Nothing to maintain: the runner forgets this namespace until
            // an enable nudges it back.
            GrepIndexStatus::Disabled {} => Ok(MaintenanceProbe::Idle),
            // A backfill always has its next page to walk.
            GrepIndexStatus::Backfilling { .. } => Ok(MaintenanceProbe::Due),
            // A watermark inside a commit has the rest of that commit left,
            // which no question about later commits would reveal.
            GrepIndexStatus::Active {
                next_event_index, ..
            } if *next_event_index != 0 => Ok(MaintenanceProbe::Due),
            GrepIndexStatus::Active {
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

fn not_enabled_step() -> MaintenanceRunReport {
    MaintenanceRunReport::concluded(MaintenanceConclusion::NotEnabled)
}

#[derive(Debug, Clone)]
pub struct GrepGcJob<S> {
    worker: GrepWorker<S>,
}

impl<S: ObjectStore + Clone> GrepGcJob<S> {
    pub fn new(worker: GrepWorker<S>) -> Self {
        Self { worker }
    }
}

#[async_trait::async_trait]
impl<S: ObjectStore + Clone + Send + Sync + 'static> MaintenanceJob for GrepGcJob<S> {
    fn id(&self) -> MaintenanceJobId {
        GREP_GC_JOB
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        self.worker
            .garbage_collect_namespace(namespace_id, current_time_ms()?)
            .await
            .map_err(|error| step_failure(namespace_id, "grep_gc", error))?;
        Ok(MaintenanceRunReport::concluded(MaintenanceConclusion::Idle))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
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
fn build_conclusion(outcome: &GrepBuildOutcome) -> MaintenanceConclusion {
    match outcome {
        GrepBuildOutcome::NotEnabled => MaintenanceConclusion::NotEnabled,
        GrepBuildOutcome::UpToDate { .. } => MaintenanceConclusion::Idle,
        GrepBuildOutcome::Published { .. } | GrepBuildOutcome::BackfillRestarted { .. } => {
            MaintenanceConclusion::Progressed
        }
        GrepBuildOutcome::Superseded => MaintenanceConclusion::Superseded,
    }
}

/// Maps one bounded reorganization outcome to a scheduling result.
///
/// Reorganization publishes whatever fits within its budget, so it has no
/// zero-progress `Blocked` result.
fn reorganize_conclusion(outcome: &GrepReorganizeOutcome) -> MaintenanceConclusion {
    match outcome {
        GrepReorganizeOutcome::NotEnabled => MaintenanceConclusion::NotEnabled,
        GrepReorganizeOutcome::NotNeeded { .. } => MaintenanceConclusion::Idle,
        GrepReorganizeOutcome::UnitPublished { .. } => MaintenanceConclusion::Progressed,
        GrepReorganizeOutcome::Superseded => MaintenanceConclusion::Superseded,
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
            MaintenanceConclusion::Idle
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::NotNeeded {
                delta_runs: 1,
                mid_runs: 0
            }),
            MaintenanceConclusion::Idle
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::NotEnabled),
            MaintenanceConclusion::NotEnabled
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::NotEnabled),
            MaintenanceConclusion::NotEnabled
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
            }),
            MaintenanceConclusion::Progressed
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::BackfillRestarted {
                target_seq: ChangeSeq(9)
            }),
            MaintenanceConclusion::Progressed,
            "a restarted backfill discarded a dead projection and published a fresh basis"
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::UnitPublished {
                merged_rows: 128,
                segments_written: 1,
                completed: false,
            }),
            MaintenanceConclusion::Progressed
        );
        assert_eq!(
            build_conclusion(&GrepBuildOutcome::Superseded),
            MaintenanceConclusion::Superseded
        );
        assert_eq!(
            reorganize_conclusion(&GrepReorganizeOutcome::Superseded),
            MaintenanceConclusion::Superseded
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
