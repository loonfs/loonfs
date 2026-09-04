//! Runtime maintenance jobs for metadata upkeep and garbage collection.
//!
//! Both jobs use the public [`FsAdmin`] operations on the writer's runtime,
//! so they share the same behavior and cache invalidation paths as manual
//! maintenance. Retention-floor advancement remains an explicit admin action
//! because it discards replay history.

use super::{
    BackgroundCompactions, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceStepConclusion, MaintenanceStepReport, NamespacePublication,
};
use crate::fs::{ReadCore, WriterBits};
use crate::publisher::PublisherRegistry;
use crate::{
    ErrorCode, FsAdmin, GcConfig, GcResponse, MaintenanceRunRequest, MaintenanceRunResponse,
    MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId, ReorganizeStepOutcome,
    Result, RuntimeError, WalFlushStepOutcome,
};
use loonfs_api::GcRequest;
use loonfs_core::cache::load_namespace_diagnostics;
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
    runner.register(Arc::new(MetadataJob {
        context: context(),
        options: MetadataMaintenanceOptions::default(),
    }))?;
    runner.register(Arc::new(GcJob { context: context() }))?;
    Ok(())
}

/// Returns the earliest safe time to run garbage collection for a newly
/// durable upload session.
///
/// The deadline includes the upload lease, the collector's grace window, and
/// a clock-skew safety margin.
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

/// Writer-owned state needed by one maintenance step.
///
/// The writer is held weakly to avoid a reference cycle. Each step upgrades
/// it for the duration of the operation and stops scheduling when the writer
/// has been dropped.
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
    options: MetadataMaintenanceOptions,
}

#[async_trait::async_trait]
impl MaintenanceJob for MetadataJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::METADATA
    }

    fn should_nudge_after_publication(&self, publication: &NamespacePublication) -> bool {
        publication.folded || self.options.flush_is_due(publication.wal_tail_segments)
    }

    /// Carries no continuation: what is left to flush or fold is what the
    /// next step's own read of durable state reports, so there is no
    /// position for the runner to hold on this job's behalf.
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepReport> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepReport::concluded(
                MaintenanceStepConclusion::NotEnabled,
            ));
        };
        match admin.maintain_metadata(namespace_id, self.options).await {
            Ok(metadata) => {
                let conclusion = metadata_conclusion(&metadata);
                Ok(MaintenanceStepReport::concluded(conclusion))
            }
            Err(error) if metadata_has_nothing_to_maintain(&error) => Ok(
                MaintenanceStepReport::concluded(MaintenanceStepConclusion::NotEnabled),
            ),
            Err(error) => Err(error),
        }
    }

    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        let Some(_writer) = self.context.bits.upgrade() else {
            return Ok(MaintenanceProbe::Idle);
        };
        // The same durable read the step opens with: control objects plus
        // the WAL tail count, without listing checkpoints. Reorganize debt
        // has no comparably cheap question, and it needs none: a publishing
        // unit concludes `Progressed`, so a backlog is folded by the run that
        // found it rather than left for a sweep.
        match load_namespace_diagnostics(self.context.core.store(), namespace_id).await {
            Ok(diagnostics) => Ok(
                if self.options.flush_is_due(diagnostics.wal_tail_segments) {
                    MaintenanceProbe::Due
                } else {
                    MaintenanceProbe::Idle
                },
            ),
            Err(error)
                if matches!(
                    error.code(),
                    ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
                ) =>
            {
                Ok(MaintenanceProbe::Idle)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Runs one bounded garbage-collection pass.
///
/// The runner stores the enumeration cursor and passes it back as the next
/// continuation. A resumed pass rebuilds its live set from durable state, so
/// losing the cursor only restarts enumeration and cannot authorize deletion.
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
    ) -> Result<MaintenanceStepReport> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepReport::concluded(
                MaintenanceStepConclusion::NotEnabled,
            ));
        };
        let response = match admin
            .run_maintenance(
                namespace_id,
                MaintenanceRunRequest::Gc(GcRequest {
                    cursor: continuation.map(str::to_owned),
                    ..GcRequest::default()
                }),
            )
            .await
        {
            Ok(response) => response,
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                // A deleted namespace still owns reclaimable state, so only
                // one that was never created is nothing to collect.
                return Ok(MaintenanceStepReport::concluded(
                    MaintenanceStepConclusion::NotEnabled,
                ));
            }
            Err(error) if continuation.is_some() && error.code() == ErrorCode::InvalidRequest => {
                // The cursor is the only caller-supplied field in this GC plan. If it is
                // rejected, clear the continuation and restart enumeration. Each pass
                // rebuilds its safety checks from durable state.
                tracing::info!(
                    namespace_id = %namespace_id,
                    error = %error.public_message(),
                    "collection rejected its resume position; restarting the pass"
                );
                return Ok(MaintenanceStepReport::concluded(
                    MaintenanceStepConclusion::Superseded,
                ));
            }
            Err(error) => return Err(error),
        };
        let MaintenanceRunResponse::Gc(gc) = response else {
            return Err(RuntimeError::Core(loonfs_core::Error::Internal(
                "maintenance GC returned a non-GC response".to_owned(),
            )));
        };
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

/// Combines WAL flush and reorganization results for scheduling.
///
/// Progress takes precedence over a lost race, and a lost race takes
/// precedence over a budget block. A step that changed durable state should
/// run again even when its second operation was blocked.
///
/// Starting or queueing a streaming compaction counts as progress because the
/// group is claimed and other groups may still be merged. Waiting for an
/// existing compaction, or requiring one that this handle cannot run, is
/// blocked.
fn metadata_conclusion(step: &MetadataMaintenanceResponse) -> MaintenanceStepConclusion {
    let flush = match step.wal_flush {
        WalFlushStepOutcome::Flushed { .. } => Some(MaintenanceStepConclusion::Progressed),
        WalFlushStepOutcome::AlreadyPublished { .. }
        | WalFlushStepOutcome::RetriesExhausted { .. } => {
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
        ReorganizeStepOutcome::RootAdvanced => Some(MaintenanceStepConclusion::Superseded),
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
fn gc_step_result(gc: GcResponse, submitted_cursor: Option<&str>) -> MaintenanceStepReport {
    MaintenanceStepReport {
        conclusion: gc_conclusion(&gc, submitted_cursor),
        continuation: gc.next_cursor,
        // The pass reports the earliest future reclamation time it observed.
        // Returning that deadline lets GC reschedule itself even when the original
        // upload deadline was missed or lost during a restart.
        not_before_ms: gc.next_reclamation_at_ms,
    }
}

/// Converts one garbage-collection response into a scheduling conclusion.
///
/// A changed cursor means enumeration advanced and should continue. An
/// unchanged cursor means the pass exhausted its budget before making
/// progress and should park. A completed pass runs again only when it
/// reclaimed something, because one deletion can expose more garbage.
/// Budget exhaustion or degraded retention without progress is reported as
/// blocked rather than idle.
fn gc_conclusion(gc: &GcResponse, submitted_cursor: Option<&str>) -> MaintenanceStepConclusion {
    match gc.next_cursor.as_deref() {
        Some(next_cursor) if Some(next_cursor) == submitted_cursor => {
            MaintenanceStepConclusion::Blocked
        }
        Some(_) => MaintenanceStepConclusion::Progressed,
        None if reclaimed_anything(gc) => MaintenanceStepConclusion::Progressed,
        None if gc.budget_exhausted || gc.retention_degraded => MaintenanceStepConclusion::Blocked,
        None => MaintenanceStepConclusion::Idle,
    }
}

fn reclaimed_anything(gc: &GcResponse) -> bool {
    // A content object is only ever deleted together with the upload session
    // that staged it, so the session count already covers it.
    gc.deleted.wal_segments > 0
        || gc.deleted.metadata_segments > 0
        || gc.deleted.manifests > 0
        || gc.deleted.checkpoint_records > 0
        || gc.deleted.upload_sessions > 0
        || gc.released_checkpoints.fork > 0
        || gc.released_checkpoints.expired > 0
        || gc.released_checkpoints.missing_basis > 0
        || gc.released_checkpoints.snapshot > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ManifestNo;
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
    fn exhausted_retries_are_superseded_and_an_unfittable_unit_is_blocked() {
        let exhausted = step_response(
            WalFlushStepOutcome::RetriesExhausted {
                observed_head_seq: ChangeSeq(3),
            },
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(
            metadata_conclusion(&exhausted),
            MaintenanceStepConclusion::Superseded
        );
        let already_published = step_response(
            WalFlushStepOutcome::AlreadyPublished {
                attempted_seq: ChangeSeq(3),
                current_manifest_no: ManifestNo(9),
            },
            ReorganizeStepOutcome::NotNeeded,
        );
        assert_eq!(
            metadata_conclusion(&already_published),
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
        reclaimed.deleted.upload_sessions = 1;
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
        degraded.retention_degraded = true;
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
        reclaimed_then_stopped.deleted.wal_segments = 2;
        assert_eq!(
            gc_conclusion(&reclaimed_then_stopped, None),
            MaintenanceStepConclusion::Progressed,
            "reclamation cascades whether or not the budget lasted"
        );
    }

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
        finished.deleted.wal_segments = 3;
        let cleared = gc_step_result(finished, Some("page-2"));
        assert_eq!(cleared.conclusion, MaintenanceStepConclusion::Progressed);
        assert_eq!(
            cleared.continuation, None,
            "a pass that reached the end of the keyspace carries nothing forward"
        );
    }

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
