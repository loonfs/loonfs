//! Grep's executor for the runtime's maintenance runner.
//!
//! The index is upkeep like any other: one bounded unit of work against
//! durable state, published through a compare-and-swap, reported as a
//! conclusion. Registering this job is all a host does — the runner it
//! registers with owns admission, the permit pool every maintenance family
//! shares, backoff, and shutdown, so grep brings no scheduler of its own.

use crate::root::{load_grep_root, GrepLifecycle};
use crate::{GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepWorker};
use loonfs::{
    MaintenanceJob, MaintenanceJobId, MaintenanceProbe, MaintenanceStepConclusion,
    MaintenanceStepResult, NamespaceId, Result, RuntimeError,
};
use loonfs_api::ErrorCode;
use loonfs_objectstore::ObjectStore;

/// Identity of the grep-index job wherever it is registered.
pub const GREP_INDEX_JOB: MaintenanceJobId = MaintenanceJobId::new("grep-index");

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

    /// Builds one bounded unit, and folds one only when there is nothing
    /// left to build.
    ///
    /// The order is the index's own priority: catching up with the namespace
    /// keeps queries answerable, while reorganization only makes an already
    /// complete answer cheaper. A step that built something says so and is
    /// eligible again at once, so a backlog is drained by repeated steps
    /// rather than by one long one.
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult> {
        let build = match self.worker.build_step(namespace_id, self.policy).await {
            Ok(report) => report.outcome,
            Err(error) if has_nothing_to_index(&error) => {
                return Ok(MaintenanceStepResult::concluded(
                    MaintenanceStepConclusion::NotEnabled,
                ))
            }
            Err(error) => return Err(step_failure(namespace_id, "grep_build", error)),
        };
        let GrepBuildOutcome::UpToDate { .. } = build else {
            return Ok(MaintenanceStepResult::concluded(build_conclusion(&build)));
        };
        let reorganize = match self.worker.reorganize_step(namespace_id, self.policy).await {
            Ok(report) => report.outcome,
            Err(error) if has_nothing_to_index(&error) => {
                return Ok(MaintenanceStepResult::concluded(
                    MaintenanceStepConclusion::NotEnabled,
                ))
            }
            Err(error) => return Err(step_failure(namespace_id, "grep_reorganize", error)),
        };
        Ok(MaintenanceStepResult::concluded(reorganize_conclusion(
            &reorganize,
        )))
    }

    /// Answers whether the index trails its namespace, in at most two small
    /// reads: the grep root, and — only for a steady root at a commit
    /// boundary — one page of one change after its watermark.
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
            GrepLifecycle::Steady {
                next_event_index, ..
            } if *next_event_index != 0 => Ok(MaintenanceProbe::Due),
            GrepLifecycle::Steady {
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

/// What one bounded fold accomplished.
///
/// Nothing here reports a budget it could not fit: a reorganization step
/// merges what its policy allows and publishes that, so there is no
/// zero-progress outcome for the runner to park as `Blocked`.
fn reorganize_conclusion(outcome: &GrepReorganizeOutcome) -> MaintenanceStepConclusion {
    match outcome {
        GrepReorganizeOutcome::NotEnabled => MaintenanceStepConclusion::NotEnabled,
        GrepReorganizeOutcome::NotNeeded { .. } => MaintenanceStepConclusion::Idle,
        GrepReorganizeOutcome::StepPublished { .. } => MaintenanceStepConclusion::Progressed,
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
            reorganize_conclusion(&GrepReorganizeOutcome::StepPublished {
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
