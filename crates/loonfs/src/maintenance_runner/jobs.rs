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

use super::{MaintenanceJob, MaintenanceJobId, MaintenanceProbe, MaintenanceStepConclusion};
use crate::fs::{ReadCore, WriterBits};
use crate::publisher::PublisherRegistry;
use crate::{
    ErrorCode, FsAdmin, GcConfig, GcResponse, MaintenanceStepKind, MaintenanceStepOptions,
    MaintenanceStepResponse, NamespaceId, ReorganizeStepOutcome, Result, RuntimeError,
    WalFlushStepOutcome,
};
use loonfs_core::limits::{
    CONTENT_RECLAMATION_GRACE_MS, GC_SAFETY_MARGIN_MS, UPLOAD_SESSION_LEASE_MS,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

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
    };
    runner.register(Arc::new(MetadataJob { context: context() }))?;
    runner.register(Arc::new(GcJob {
        context: context(),
        cursors: Mutex::new(BTreeMap::new()),
    }))?;
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
}

impl StepContext {
    fn admin(&self) -> Option<(Arc<WriterBits>, FsAdmin)> {
        let bits = self.bits.upgrade()?;
        let admin = FsAdmin::from_writer_parts(
            self.core.clone(),
            bits.identity.clone(),
            self.publisher.clone(),
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

    async fn step(&self, namespace_id: &NamespaceId) -> Result<MaintenanceStepConclusion> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepConclusion::NotEnabled);
        };
        // The default step: no retention, no garbage collection. Both are
        // other decisions, and one of them is another job.
        let options = MaintenanceStepOptions::default();
        match admin
            .maintenance_step_namespace(namespace_id, options)
            .await
        {
            Ok(step) => {
                let conclusion = metadata_conclusion(&step);
                // Quiet sub-outcomes emit nothing at default levels, so this
                // is the only record of what a step actually did. The runner
                // logs what the conclusion means for scheduling; this logs
                // what produced it.
                tracing::debug!(
                    namespace_id = %step.namespace_id,
                    wal_tail_segments_before = step.status_before.wal_tail_segments,
                    wal_flush = ?step.wal_flush,
                    reorganize = ?step.reorganize,
                    conclusion = conclusion.as_str(),
                    "metadata maintenance step concluded"
                );
                Ok(conclusion)
            }
            Err(error) if metadata_has_nothing_to_maintain(&error) => {
                Ok(MaintenanceStepConclusion::NotEnabled)
            }
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
                let threshold = MaintenanceStepOptions::default().max_wal_tail_segments;
                Ok(if status.wal_tail_segments >= threshold {
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
struct GcJob {
    context: StepContext,
    /// Where each namespace's bounded pass stopped. In-process scheduling
    /// state and nothing more: resuming rebuilds every safety proof from
    /// scratch, so a cursor lost with the process costs re-enumeration and
    /// never authorizes a deletion.
    cursors: Mutex<BTreeMap<NamespaceId, String>>,
}

impl GcJob {
    fn lock_cursors(&self) -> std::sync::MutexGuard<'_, BTreeMap<NamespaceId, String>> {
        self.cursors.lock().expect("gc cursor lock poisoned")
    }

    fn cursor(&self, namespace_id: &NamespaceId) -> Option<String> {
        self.lock_cursors().get(namespace_id).cloned()
    }

    fn remember(&self, namespace_id: &NamespaceId, cursor: Option<String>) {
        let mut cursors = self.lock_cursors();
        match cursor {
            Some(cursor) => {
                cursors.insert(namespace_id.clone(), cursor);
            }
            None => {
                cursors.remove(namespace_id);
            }
        }
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for GcJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::GC
    }

    async fn step(&self, namespace_id: &NamespaceId) -> Result<MaintenanceStepConclusion> {
        let Some((_writer, admin)) = self.context.admin() else {
            return Ok(MaintenanceStepConclusion::NotEnabled);
        };
        let submitted_cursor = self.cursor(namespace_id);
        let options = MaintenanceStepOptions {
            only: Some(MaintenanceStepKind::Gc),
            gc: Some(GcConfig {
                cursor: submitted_cursor.clone(),
                // The step resolves the absent candidate budget to the
                // per-step default, which is what bounds this pass.
                ..GcConfig::default()
            }),
            ..MaintenanceStepOptions::default()
        };
        let step = match admin
            .maintenance_step_namespace(namespace_id, options)
            .await
        {
            Ok(step) => step,
            Err(error) if error.code() == ErrorCode::NamespaceNotFound => {
                // A deleted namespace still owns reclaimable state, so only
                // one that was never created is nothing to collect.
                self.remember(namespace_id, None);
                return Ok(MaintenanceStepConclusion::NotEnabled);
            }
            Err(error) => {
                // With every other option fixed, the one thing this step
                // can be asked to reject is the cursor it was resumed with.
                // Dropping it restarts enumeration, which is always sound.
                if error.code() == ErrorCode::InvalidRequest {
                    self.remember(namespace_id, None);
                }
                return Err(error);
            }
        };
        let Some(gc) = step.gc else {
            // A gc-only step always reports its pass; nothing to conclude
            // from otherwise.
            return Ok(MaintenanceStepConclusion::Idle);
        };
        self.remember(namespace_id, gc.next_cursor.clone());
        Ok(gc_conclusion(&gc, submitted_cursor.as_deref()))
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

/// The step's two sub-outcomes, read as one conclusion.
///
/// Precedence runs progress, then races, then blocks: a step that flushed
/// and then failed to fit a fold unit did move durable state and should run
/// again; a step that only lost a race should take that race again; a step
/// that only failed to fit has nothing to gain from an immediate retry.
fn metadata_conclusion(step: &MaintenanceStepResponse) -> MaintenanceStepConclusion {
    let flush = match step.wal_flush {
        WalFlushStepOutcome::Flushed { .. } => Some(MaintenanceStepConclusion::Progressed),
        WalFlushStepOutcome::Superseded { .. } | WalFlushStepOutcome::RaceLost { .. } => {
            Some(MaintenanceStepConclusion::Superseded)
        }
        WalFlushStepOutcome::NotNeeded => None,
    };
    let reorganize = match step.reorganize {
        ReorganizeStepOutcome::UnitPublished => Some(MaintenanceStepConclusion::Progressed),
        ReorganizeStepOutcome::Superseded => Some(MaintenanceStepConclusion::Superseded),
        ReorganizeStepOutcome::BudgetExhausted => Some(MaintenanceStepConclusion::Blocked),
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

/// One collection pass, read as one conclusion.
///
/// Progress is where the enumeration got to, never what the counters say. A
/// pass that hands back a cursor past the one it was given walked keyspace
/// and has more to walk, so it runs again. A pass that hands back the very
/// cursor it started from decided nothing at all — its budget died inside
/// the content-reference scan, which every pass has to redo before it may
/// delete anything — and repeating it immediately would repeat that exact
/// result forever, so it parks like any other zero-progress step and waits
/// for something to change.
///
/// A pass that walked the whole keyspace is finished: idle when it freed
/// nothing, eligible again when it did, because reclamation cascades — a
/// deleted manifest can leave tables unreferenced. Ambiguous roots that
/// suppressed deletion are the one clean-pass case with work provably left
/// undone, so they park distinctly rather than reading as idle.
fn gc_conclusion(gc: &GcResponse, submitted_cursor: Option<&str>) -> MaintenanceStepConclusion {
    match gc.next_cursor.as_deref() {
        Some(next_cursor) if Some(next_cursor) == submitted_cursor => {
            MaintenanceStepConclusion::Blocked
        }
        Some(_) => MaintenanceStepConclusion::Progressed,
        None if reclaimed_anything(gc) => MaintenanceStepConclusion::Progressed,
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
    use crate::{ManifestId, NamespaceStatusResponse};
    use loonfs_api::ChangeSeq;
    use loonfs_test_support::ids::namespace_id;

    fn step_response(
        wal_flush: WalFlushStepOutcome,
        reorganize: ReorganizeStepOutcome,
    ) -> MaintenanceStepResponse {
        let namespace = namespace_id("demo");
        MaintenanceStepResponse {
            namespace_id: namespace.clone(),
            status_before: NamespaceStatusResponse {
                namespace_id: namespace,
                head_seq: ChangeSeq(1),
                current_manifest_id: None,
                wal_tail_segments: 0,
                retention_floor_seq: ChangeSeq(0),
            },
            wal_flush,
            reorganize,
            retention_floor_seq: ChangeSeq(0),
            gc: None,
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
            ReorganizeStepOutcome::BudgetExhausted,
        );
        assert_eq!(
            metadata_conclusion(&blocked),
            MaintenanceStepConclusion::Blocked
        );
    }

    #[test]
    fn progress_outranks_a_block_in_the_same_step() {
        let step = step_response(
            WalFlushStepOutcome::Flushed {
                manifest_head_seq: ChangeSeq(7),
            },
            ReorganizeStepOutcome::BudgetExhausted,
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
