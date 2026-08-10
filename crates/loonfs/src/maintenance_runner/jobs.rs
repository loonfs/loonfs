//! The two executors the runtime registers on every write-capable handle:
//! metadata upkeep, and garbage collection.
//!
//! Both run the same operations an operator runs, through the same
//! [`FsAdmin`] surface, rather than a private copy of them — and over the
//! writer's own runtime, so their cache invalidations reach the writer's
//! caches and publisher engines.
//!
//! Retention is deliberately not here. Advancing the floor surrenders
//! replay history, which is a decision rather than upkeep, so it stays an
//! explicit call with no scheduler behind it.

use super::{
    BackgroundCompactions, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceStepConclusion, MaintenanceStepResult,
};
use crate::fs::{ReadCore, WriterBits};
use crate::publisher::PublisherRegistry;
use crate::{
    ErrorCode, FsAdmin, GcConfig, GcResponse, MaintenancePlan, MetadataMaintenanceOptions,
    MetadataMaintenanceResponse, NamespaceId, ReorganizeStepOutcome, Result, RuntimeError,
    WalFlushStepOutcome,
};
use loonfs_core::limits::{
    CONTENT_RECLAMATION_GRACE_MS, GC_SAFETY_MARGIN_MS, UPLOAD_SESSION_LEASE_MS,
};
use std::sync::{Arc, Weak};

/// Registers the jobs the runtime owns. Called once, after the writer's
/// publication service exists.
pub(crate) fn register_core_jobs(
    runner: &super::MaintenanceRunner,
    core: &ReadCore,
    bits: &Arc<WriterBits>,
    publisher: &PublisherRegistry,
) -> Result<()> {
    let context = || StepContext {
        core: core.clone(),
        bits: Arc::downgrade(bits),
        publisher: publisher.clone(),
        compactions: runner.compactions(),
    };
    runner.register(Arc::new(MetadataJob { context: context() }))?;
    runner.register(Arc::new(GcJob { context: context() }))?;
    Ok(())
}

/// Earliest instant a garbage-collection pass could reclaim an upload
/// session opened now.
///
/// Derived from what the collector actually waits for: a session opens with
/// [`UPLOAD_SESSION_LEASE_MS`] of lease, an expired session is only aborted
/// once the pass's grace window has passed on top of that, and
/// [`GC_SAFETY_MARGIN_MS`] covers the skew between the clock reading here
/// and the one the pass will read. Taken after the session is durable, so
/// the schedule can only land after the collector's own predicate, never
/// before it — a pass that arrives early finds the session retained and
/// parks with nothing left to bring it back.
pub(crate) fn upload_session_reclaim_at_ms(session_durable_at_ms: u64) -> u64 {
    session_durable_at_ms
        .saturating_add(UPLOAD_SESSION_LEASE_MS)
        .saturating_add(GcConfig::default().grace_window_ms)
        .saturating_add(GC_SAFETY_MARGIN_MS)
}

/// Earliest instant a garbage-collection pass could reclaim the content a
/// session completed now, plus the same clock-skew margin.
pub(crate) fn completed_upload_reclaim_at_ms(completion_observed_at_ms: u64) -> u64 {
    completion_observed_at_ms
        .saturating_add(CONTENT_RECLAMATION_GRACE_MS)
        .saturating_add(GC_SAFETY_MARGIN_MS)
}

/// What an executor needs from the writer that owns it.
///
/// The writer's state is held weakly and upgraded for the length of one
/// step: a scheduled step is the writer's own work and publishes under its
/// identity, so it holds that identity while it runs and schedules nothing
/// once the writer is gone. Holding it strongly instead would make the
/// runner and the writer keep each other alive forever.
struct StepContext {
    core: ReadCore,
    bits: Weak<WriterBits>,
    publisher: PublisherRegistry,
    /// Held strongly: it is a map and a weak reference to the runner, so it
    /// keeps nothing alive that a dropped writer should have taken with it.
    compactions: BackgroundCompactions,
}

impl StepContext {
    fn admin(&self) -> Option<(Arc<WriterBits>, FsAdmin)> {
        let bits = self.bits.upgrade()?;
        let admin = FsAdmin::from_writer_parts(
            self.core.clone(),
            bits.identity.clone(),
            self.publisher.clone(),
            self.compactions.clone(),
        );
        Some((bits, admin))
    }
}

/// Flushes the WAL tail past its threshold and folds one bounded metadata
/// reorganization unit.
struct MetadataJob {
    context: StepContext,
}

#[async_trait::async_trait]
impl MaintenanceJob for MetadataJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::METADATA
    }

    /// Carries no continuation: what is left to flush or fold is what the
    /// next step's own read of durable state reports, so there is no
    /// position for the runner to hold on this job's behalf.
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepResult::concluded(
                MaintenanceStepConclusion::NotEnabled,
            ));
        };
        // Upkeep alone: no retention, no garbage collection. Both are other
        // decisions, and one of them is another job.
        match admin
            .maintenance_step_namespace(namespace_id, MaintenancePlan::metadata())
            .await
        {
            Ok(step) => {
                let metadata = step
                    .metadata
                    .expect("a plan selecting metadata upkeep reports it");
                let conclusion = metadata_conclusion(&metadata);
                // Quiet sub-outcomes emit nothing at default levels, so this
                // is the only record of what a step actually did. The runner
                // logs what the conclusion means for scheduling; this logs
                // what produced it.
                tracing::debug!(
                    namespace_id = %step.namespace_id,
                    wal_tail_segments_before = step.status_before.wal_tail_segments,
                    wal_flush = ?metadata.wal_flush,
                    reorganize = ?metadata.reorganize,
                    conclusion = conclusion.as_str(),
                    "metadata maintenance step concluded"
                );
                Ok(MaintenanceStepResult::concluded(conclusion))
            }
            Err(error) if metadata_has_nothing_to_maintain(&error) => Ok(
                MaintenanceStepResult::concluded(MaintenanceStepConclusion::NotEnabled),
            ),
            Err(error) => Err(error),
        }
    }

    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceProbe::Idle);
        };
        // One head summary, the same read the step opens with. Reorganize
        // debt has no comparably cheap question, and it needs none: a
        // publishing unit concludes `Progressed`, so a backlog is folded by
        // the run that found it rather than left for a sweep.
        match admin.namespace_status(namespace_id).await {
            Ok(status) => {
                let threshold = MetadataMaintenanceOptions::default().max_wal_tail_segments;
                Ok(if status.wal_tail_segments >= threshold.get() {
                    MaintenanceProbe::Due
                } else {
                    MaintenanceProbe::Idle
                })
            }
            Err(error) if metadata_has_nothing_to_maintain(&error) => Ok(MaintenanceProbe::Idle),
            Err(error) => Err(error),
        }
    }
}

/// Runs one bounded mark-and-sweep pass, resuming the enumeration the last
/// pass stopped at.
///
/// The enumeration cursor is the runner's, not this job's: it arrives as
/// the step's `continuation` and leaves as the result's. That keeps one
/// place holding what a key is waiting for, and it costs nothing here
/// because the cursor was never authority — a resumed pass rebuilds the
/// live set exactly as a fresh one does, so a cursor lost with the process
/// costs re-enumeration and can never authorize a deletion.
struct GcJob {
    context: StepContext,
}

#[async_trait::async_trait]
impl MaintenanceJob for GcJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::GC
    }

    async fn step(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepResult::concluded(
                MaintenanceStepConclusion::NotEnabled,
            ));
        };
        let plan = MaintenancePlan {
            gc: Some(GcConfig {
                cursor: continuation.map(str::to_owned),
                // The step resolves the absent candidate budget to the
                // per-step default, which is what bounds this pass.
                ..GcConfig::default()
            }),
            ..MaintenancePlan::default()
        };
        let step = match admin.maintenance_step_namespace(namespace_id, plan).await {
            Ok(step) => step,
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                // A deleted namespace still owns reclaimable state, so only
                // one that was never created is nothing to collect.
                return Ok(MaintenanceStepResult::concluded(
                    MaintenanceStepConclusion::NotEnabled,
                ));
            }
            Err(error) if continuation.is_some() && error.code() == ErrorCode::InvalidRequest => {
                // This plan selects collection and nothing else, and fixes
                // every field of it but the cursor, so the cursor it was
                // resumed with is the one thing the step can be asked to
                // reject. Concluding without one hands the runner an empty
                // continuation and takes the key again, which restarts
                // enumeration — always sound, because every pass rebuilds
                // its own safety proof from durable state.
                tracing::info!(
                    namespace_id = %namespace_id,
                    error = %error,
                    "collection rejected its resume position; restarting the pass"
                );
                return Ok(MaintenanceStepResult::concluded(
                    MaintenanceStepConclusion::Superseded,
                ));
            }
            Err(error) => return Err(error),
        };
        // Selection is presence, so a plan that named collection is answered
        // with a collection report: there is no step that ran and said
        // nothing.
        let gc = step
            .gc
            .expect("a plan selecting collection reports its pass");
        // The counts a pass produced survive here or nowhere: the runner
        // reads a pass as one conclusion, and everything it reclaimed is
        // dropped with the response a line below.
        self.context.core.instruments().gc_pass(&gc);
        Ok(gc_step_result(gc, continuation))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        // Collection has no cheap question: whether anything is reclaimable
        // is what a pass finds out, and a pass is not a probe. What brings
        // this job back is the not-before time each upload plants for the
        // deadline it creates, so a sweep has nothing to add.
        Ok(MaintenanceProbe::Idle)
    }
}

/// Namespace states with no metadata left to maintain: one that was never
/// created, and one whose tombstone has nothing to flush or reorganize.
fn metadata_has_nothing_to_maintain(error: &RuntimeError) -> bool {
    matches!(
        error.code(),
        ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
    )
}

/// One upkeep pass's two outcomes, read as one conclusion.
///
/// Precedence runs progress, then races, then blocks: a step that flushed
/// and then failed to fit a fold unit did move durable state and should run
/// again; a step that only lost a race should take that race again; a step
/// that only failed to fit has nothing to gain from an immediate retry.
///
/// Starting a streaming compaction is progress of the useful kind: the step
/// that started it did no folding, and the steps behind it now have the
/// group's peers to fold while the job runs. A job queued behind the process
/// compaction limit reads the same way — the group is claimed, so the steps
/// behind it have the same peers to fold, and nothing is needed to make the
/// job run. A group waiting on a job already running, and a group needing one
/// this handle cannot run at all, are blocks: there is work and this step
/// cannot make it, so the key parks and comes back on the next nudge or
/// sweep. The job's own ending nudges the key back when it lands.
fn metadata_conclusion(step: &MetadataMaintenanceResponse) -> MaintenanceStepConclusion {
    let flush = match step.wal_flush {
        WalFlushStepOutcome::Flushed { .. } => Some(MaintenanceStepConclusion::Progressed),
        WalFlushStepOutcome::Superseded { .. } | WalFlushStepOutcome::RaceLost { .. } => {
            Some(MaintenanceStepConclusion::Superseded)
        }
        WalFlushStepOutcome::NotNeeded => None,
    };
    let reorganize = match step.reorganize {
        ReorganizeStepOutcome::UnitPublished
        | ReorganizeStepOutcome::CompactionStarted
        | ReorganizeStepOutcome::CompactionAtCapacity => {
            Some(MaintenanceStepConclusion::Progressed)
        }
        ReorganizeStepOutcome::Superseded => Some(MaintenanceStepConclusion::Superseded),
        ReorganizeStepOutcome::CompactionRunning | ReorganizeStepOutcome::CompactionRequired => {
            Some(MaintenanceStepConclusion::Blocked)
        }
        ReorganizeStepOutcome::NotNeeded => None,
    };
    [flush, reorganize]
        .into_iter()
        .flatten()
        .max_by_key(|conclusion| conclusion_precedence(*conclusion))
        .unwrap_or(MaintenanceStepConclusion::Idle)
}

fn conclusion_precedence(conclusion: MaintenanceStepConclusion) -> u8 {
    match conclusion {
        MaintenanceStepConclusion::Progressed => 3,
        MaintenanceStepConclusion::Superseded => 2,
        MaintenanceStepConclusion::Blocked => 1,
        MaintenanceStepConclusion::Idle | MaintenanceStepConclusion::NotEnabled => 0,
    }
}

/// One collection pass, read as everything the runner needs: what to do
/// with the key, and where the next pass picks up.
///
/// `submitted_cursor` is what the runner handed this step; the pass's own
/// `next_cursor` is what it hands back. Comparing the two is how a pass
/// that walked keyspace is told apart from one that decided nothing, and
/// handing the second one back is what lets a retry resume rather than
/// restart.
fn gc_step_result(gc: GcResponse, submitted_cursor: Option<&str>) -> MaintenanceStepResult {
    MaintenanceStepResult {
        conclusion: gc_conclusion(&gc, submitted_cursor),
        continuation: gc.next_cursor,
        // The pass itself is the source of truth for when it should be run
        // again: it compared every retained candidate against its own
        // clock, so it knows the soonest of those waits without being told.
        // Handing it back here is what makes a pass self-scheduling —
        // reclamation an upload path never planted a deadline for, and the
        // deadlines a restart forgot, both come back through the next pass
        // over the namespace rather than through a side channel.
        not_before_ms: gc.next_reclamation_at_ms,
    }
}

/// One collection pass, read as one conclusion.
///
/// Progress is where the enumeration got to, never what the counters say. A
/// pass that hands back a cursor past the one it was given walked keyspace
/// and has more to walk, so it runs again. A pass that hands back the very
/// cursor it started from decided nothing at all — its budget died in the
/// marking or the content-reference scan, both of which every pass redoes
/// before it may delete anything — and repeating it immediately would
/// repeat that exact result forever, so it parks like any other
/// zero-progress step and waits for something to change.
///
/// A pass that walked the whole keyspace is finished: idle when it freed
/// nothing, eligible again when it did, because reclamation cascades — a
/// deleted manifest can leave tables unreferenced. Ambiguous roots that
/// suppressed deletion are the one clean-pass case with work provably left
/// undone, so they park distinctly rather than reading as idle.
///
/// A pass with no cursor at all can still have run out: a namespace whose
/// roots cost more than `max_objects` never gets as far as the keyspace,
/// and says so outright. That parks too, and pointedly not as idle — idle
/// clears the stored continuation, and this pass freed nothing and walked
/// nowhere.
fn gc_conclusion(gc: &GcResponse, submitted_cursor: Option<&str>) -> MaintenanceStepConclusion {
    match gc.next_cursor.as_deref() {
        Some(next_cursor) if Some(next_cursor) == submitted_cursor => {
            MaintenanceStepConclusion::Blocked
        }
        Some(_) => MaintenanceStepConclusion::Progressed,
        None if reclaimed_anything(gc) => MaintenanceStepConclusion::Progressed,
        None if gc.budget_exhausted => MaintenanceStepConclusion::Blocked,
        None if gc.degraded_retention => MaintenanceStepConclusion::Blocked,
        None => MaintenanceStepConclusion::Idle,
    }
}

fn reclaimed_anything(gc: &GcResponse) -> bool {
    gc.deleted_wal_segments > 0
        || gc.deleted_metadata_tables > 0
        || gc.deleted_manifests > 0
        || gc.deleted_checkpoint_records > 0
        || gc.released_fork_checkpoints > 0
        || gc.released_expired_checkpoints > 0
        || gc.deleted_upload_sessions > 0
        || gc.released_missing_basis_checkpoints > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ManifestId;
    use loonfs_api::ChangeSeq;
    use loonfs_test_support::ids::namespace_id;

    fn step_response(
        wal_flush: WalFlushStepOutcome,
        reorganize: ReorganizeStepOutcome,
    ) -> MetadataMaintenanceResponse {
        MetadataMaintenanceResponse {
            wal_flush,
            reorganize,
        }
    }

    #[test]
    fn a_quiet_metadata_step_is_idle() {
        let step = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(metadata_conclusion(&step), MaintenanceStepConclusion::Idle);
    }

    #[test]
    fn published_metadata_work_progresses() {
        let flushed = step_response(
            WalFlushStepOutcome::Flushed {
                manifest_head_seq: ChangeSeq(7),
            },
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(
            metadata_conclusion(&flushed),
            MaintenanceStepConclusion::Progressed
        );
        let folded = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::UnitPublished,
        );
        assert_eq!(
            metadata_conclusion(&folded),
            MaintenanceStepConclusion::Progressed
        );
    }

    #[test]
    fn a_lost_race_is_superseded_and_an_unfittable_unit_is_blocked() {
        let raced = step_response(
            WalFlushStepOutcome::RaceLost {
                observed_head_seq: ChangeSeq(3),
            },
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(
            metadata_conclusion(&raced),
            MaintenanceStepConclusion::Superseded
        );
        let superseded = step_response(
            WalFlushStepOutcome::Superseded {
                attempted_seq: ChangeSeq(3),
                current_manifest_id: ManifestId(9),
            },
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(
            metadata_conclusion(&superseded),
            MaintenanceStepConclusion::Superseded
        );
        let blocked = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::CompactionRunning,
        );
        assert_eq!(
            metadata_conclusion(&blocked),
            MaintenanceStepConclusion::Blocked
        );
    }

    /// Starting a background rebuild is progress: the step that started it
    /// folded nothing, and the steps behind it have the group's peers to fold
    /// while the job runs. A job queued behind the process limit reads the
    /// same way, because the group is claimed either way.
    #[test]
    fn a_started_or_queued_compaction_progresses_and_the_other_two_block() {
        let started = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::CompactionStarted,
        );
        assert_eq!(
            metadata_conclusion(&started),
            MaintenanceStepConclusion::Progressed
        );
        let queued = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::CompactionAtCapacity,
        );
        assert_eq!(
            metadata_conclusion(&queued),
            MaintenanceStepConclusion::Progressed,
            "a queued job owns its group already, so the step behind it folds the peers"
        );
        let waiting = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::CompactionRunning,
        );
        assert_eq!(
            metadata_conclusion(&waiting),
            MaintenanceStepConclusion::Blocked,
            "a group waiting on a job already running has nothing to gain from an immediate retry"
        );
        let unschedulable = step_response(
            WalFlushStepOutcome::NotNeeded,
            ReorganizeStepOutcome::CompactionRequired,
        );
        assert_eq!(
            metadata_conclusion(&unschedulable),
            MaintenanceStepConclusion::Blocked,
            "a handle that cannot run the job gains nothing from being asked again at once"
        );
    }

    #[test]
    fn progress_outranks_a_block_in_the_same_step() {
        let step = step_response(
            WalFlushStepOutcome::Flushed {
                manifest_head_seq: ChangeSeq(7),
            },
            ReorganizeStepOutcome::CompactionRunning,
        );
        assert_eq!(
            metadata_conclusion(&step),
            MaintenanceStepConclusion::Progressed,
            "a step that moved durable state runs again regardless of what it could not fit"
        );
    }

    #[test]
    fn a_clean_gc_pass_is_idle_and_a_deletion_progresses() {
        let namespace = namespace_id("demo");
        let clean = GcResponse::empty(namespace.clone());
        assert_eq!(gc_conclusion(&clean, None), MaintenanceStepConclusion::Idle);

        let mut reclaimed = GcResponse::empty(namespace.clone());
        reclaimed.deleted_upload_sessions = 1;
        assert_eq!(
            gc_conclusion(&reclaimed, None),
            MaintenanceStepConclusion::Progressed
        );

        let mut retained_only = GcResponse::empty(namespace.clone());
        retained_only.retained_candidates = 12;
        assert_eq!(
            gc_conclusion(&retained_only, None),
            MaintenanceStepConclusion::Idle,
            "candidates inside their grace window are not progress and not a reason to spin"
        );

        let mut degraded = GcResponse::empty(namespace);
        degraded.degraded_retention = true;
        assert_eq!(
            gc_conclusion(&degraded, None),
            MaintenanceStepConclusion::Blocked
        );
    }

    #[test]
    fn a_gc_cursor_that_moved_progresses_and_one_that_did_not_blocks() {
        let namespace = namespace_id("demo");
        let mut walked = GcResponse::empty(namespace.clone());
        walked.next_cursor = Some("second".to_owned());
        assert_eq!(
            gc_conclusion(&walked, None),
            MaintenanceStepConclusion::Progressed,
            "a first pass that stopped mid-keyspace has more to walk"
        );
        assert_eq!(
            gc_conclusion(&walked, Some("first")),
            MaintenanceStepConclusion::Progressed
        );

        let mut parked = GcResponse::empty(namespace);
        parked.next_cursor = Some("first".to_owned());
        assert_eq!(
            gc_conclusion(&parked, Some("first")),
            MaintenanceStepConclusion::Blocked,
            "a pass whose budget died before it decided anything must not requeue hot"
        );
    }

    /// A budget that ran out before the pass finished is a park, not an
    /// idle: idle clears the stored continuation, and a pass that never got
    /// to the keyspace has nothing to show for itself but that.
    #[test]
    fn a_pass_that_ran_out_of_budget_parks_unless_it_got_somewhere_first() {
        let namespace = namespace_id("demo");
        let mut marked_nothing = GcResponse::empty(namespace.clone());
        marked_nothing.budget_exhausted = true;
        assert_eq!(
            gc_conclusion(&marked_nothing, None),
            MaintenanceStepConclusion::Blocked
        );

        // The same pass on a resumed step returns the token it was given,
        // byte for byte, because it made no progress. That parks it too.
        let mut echoed = GcResponse::empty(namespace.clone());
        echoed.budget_exhausted = true;
        echoed.next_cursor = Some("page-2".to_owned());
        assert_eq!(
            gc_conclusion(&echoed, Some("page-2")),
            MaintenanceStepConclusion::Blocked
        );

        let mut swept_then_stopped = GcResponse::empty(namespace.clone());
        swept_then_stopped.budget_exhausted = true;
        swept_then_stopped.next_cursor = Some("page-3".to_owned());
        assert_eq!(
            gc_conclusion(&swept_then_stopped, Some("page-2")),
            MaintenanceStepConclusion::Progressed,
            "a pass that walked keyspace before running out has more to walk"
        );

        let mut reclaimed_then_stopped = GcResponse::empty(namespace);
        reclaimed_then_stopped.budget_exhausted = true;
        reclaimed_then_stopped.deleted_wal_segments = 2;
        assert_eq!(
            gc_conclusion(&reclaimed_then_stopped, None),
            MaintenanceStepConclusion::Progressed,
            "reclamation cascades whether or not the budget lasted"
        );
    }

    /// The cursor is the runner's now: it arrives as the step's
    /// continuation and leaves as the result's, with no map on this job's
    /// side of the seam.
    #[test]
    fn a_collection_pass_hands_its_cursor_back_to_the_runner() {
        let mut walked = GcResponse::empty(namespace_id("demo"));
        walked.next_cursor = Some("page-2".to_owned());

        let first = gc_step_result(walked.clone(), None);
        assert_eq!(first.conclusion, MaintenanceStepConclusion::Progressed);
        assert_eq!(
            first.continuation,
            Some("page-2".to_owned()),
            "the runner stores where the pass stopped"
        );
        assert_eq!(
            first.not_before_ms, None,
            "a pass that retained nothing on a clock has no deadline to report"
        );

        // The resumed step is handed that cursor back. One that cannot get
        // past it decided nothing, and parks still holding it, so a retry
        // with room to work resumes instead of walking the same ground.
        let parked = gc_step_result(walked, Some("page-2"));
        assert_eq!(parked.conclusion, MaintenanceStepConclusion::Blocked);
        assert_eq!(parked.continuation, Some("page-2".to_owned()));

        let mut finished = GcResponse::empty(namespace_id("demo"));
        finished.deleted_wal_segments = 3;
        let cleared = gc_step_result(finished, Some("page-2"));
        assert_eq!(cleared.conclusion, MaintenanceStepConclusion::Progressed);
        assert_eq!(
            cleared.continuation, None,
            "a pass that reached the end of the keyspace carries nothing forward"
        );
    }

    /// The deadline a pass reports for what it retained becomes the key's
    /// own, which is what makes a pass schedule its own successor.
    #[test]
    fn a_collection_pass_hands_its_soonest_retained_deadline_to_the_runner() {
        let mut retained = GcResponse::empty(namespace_id("demo"));
        retained.retained_candidates = 2;
        retained.next_reclamation_at_ms = Some(1_700_000_600_000);

        let result = gc_step_result(retained, None);
        assert_eq!(
            result.conclusion,
            MaintenanceStepConclusion::Idle,
            "a candidate inside its grace window is still not work to redo now"
        );
        assert_eq!(
            result.not_before_ms,
            Some(1_700_000_600_000),
            "the runner parks the key on the pass's own soonest retention"
        );
    }

    #[test]
    fn reclamation_times_are_derived_from_what_the_collector_waits_for() {
        let now_ms = 1_700_000_000_000;
        assert_eq!(
            upload_session_reclaim_at_ms(now_ms),
            now_ms
                + UPLOAD_SESSION_LEASE_MS
                + GcConfig::default().grace_window_ms
                + GC_SAFETY_MARGIN_MS
        );
        assert_eq!(
            completed_upload_reclaim_at_ms(now_ms),
            now_ms + CONTENT_RECLAMATION_GRACE_MS + GC_SAFETY_MARGIN_MS
        );
        assert!(
            upload_session_reclaim_at_ms(now_ms) > now_ms + UPLOAD_SESSION_LEASE_MS,
            "a session's own lease has to pass before the pass may abort it"
        );
        assert!(
            completed_upload_reclaim_at_ms(now_ms) > now_ms + CONTENT_RECLAMATION_GRACE_MS,
            "the schedule lands after the collector's predicate, never before it"
        );
    }
}
