//! The grep worker: its lifecycle, its backfill pins, and the bounded steps
//! that build and reorganize one namespace's grep index.

mod build;
mod reorganize;

use crate::manifest::{
    load_current_grep_manifest, publish_grep_manifest, GrepIndexState, GrepIndexStatus,
    GrepManifestState, LoadedGrepManifest,
};
use crate::reads::NamespaceReads;
use crate::{GrepError, GrepPassBudget, Result};
use loonfs::engine::{next_run_no_after, Observation, OperationClock};
use loonfs::{CoreError, CreateSnapshotOptions, LoonFs, ReadOnly, Writable};
use loonfs_objectstore::ObjectStore;
use loonfs_types::api::v0::{self, GrepIndex};
use loonfs_types::format::segment_blocks::{
    DEFAULT_MAX_COMPACTION_INPUT_ROWS, DEFAULT_MAX_DELTA_RUNS, DEFAULT_MAX_ROWS_PER_SEGMENT,
};
use loonfs_types::MonotonicTimer;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId, PinId, RunNo};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

/// Lifetime of the snapshot used by one grep backfill attempt.
///
/// If the snapshot expires before backfill finishes, reads through it
/// return `snapshot_not_found` and the worker restarts from a fresh
/// snapshot. The worker does not extend an old pin.
pub(crate) const GREP_BACKFILL_CHECKPOINT_TTL_MS: NonZeroU64 =
    NonZeroU64::new(24 * 60 * 60 * 1000).expect("a day is a nonzero lifetime");

const GREP_BACKFILL_CHECKPOINT_NAME: &str = "loonfs-grep-backfill";
/// Content bytes one backfill step ingests before it checkpoints its cursor.
/// This budgets ingestion, not LSM merging — it happens to share a value
/// with the metadata reorganization input limit, and must stay independent
/// so a compaction policy change cannot silently change backfill behavior.
const GREP_BACKFILL_MAX_CONTENT_BYTES_PER_STEP: u64 = 64 * 1024 * 1024;
/// Content reads and index segment writes the worker runs at once. Eight
/// reads of files at the 8 MiB eligibility cap hold at most 64 MiB, the same
/// as one backfill step's content budget.
pub(crate) const GREP_WORKER_REQUESTS_IN_FLIGHT: usize = 8;
// Keep grep's fold threshold independent of metadata reorganization limits.
const GREP_MAX_MID_RUNS: usize = 8;

/// Writer-side budgets for one grep build or reorganize step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GramIndexBuildPolicy {
    /// Revisions examined per step, at most `MAX_PAGE_LIMIT`, because a
    /// step reads them as one page of the change feed.
    pub max_files_per_step: NonZeroUsize,
    /// Content bytes read per step.
    pub max_content_bytes_per_step: NonZeroU64,
    /// Rows per written grep segment.
    pub max_rows_per_segment: NonZeroUsize,
    /// Delta-level runs that trigger a reorganization into a fresh mid run.
    pub max_delta_runs: NonZeroUsize,
    /// Mid-level runs that trigger a reorganization into a fresh base run.
    pub max_mid_runs: NonZeroUsize,
    /// Rows one reorganize step merges before publishing and yielding.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
}

impl Default for GramIndexBuildPolicy {
    fn default() -> Self {
        Self {
            max_files_per_step: const { NonZeroUsize::new(256).unwrap() },
            max_content_bytes_per_step: const {
                NonZeroU64::new(GREP_BACKFILL_MAX_CONTENT_BYTES_PER_STEP).unwrap()
            },
            max_rows_per_segment: const { NonZeroUsize::new(DEFAULT_MAX_ROWS_PER_SEGMENT).unwrap() },
            max_delta_runs: const { NonZeroUsize::new(DEFAULT_MAX_DELTA_RUNS).unwrap() },
            max_mid_runs: const { NonZeroUsize::new(GREP_MAX_MID_RUNS).unwrap() },
            max_decoded_input_rows_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_COMPACTION_INPUT_ROWS).unwrap()
            },
        }
    }
}

/// Result of enabling grep for one namespace.
///
/// Both settled outcomes answer with the durable lifecycle itself rather
/// than a sequence number, because the two phases do not have the same
/// number to give: a fresh enable publishes a backfill with a target, and an
/// already-enabled namespace may be backfilling or active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepEnableOutcome {
    Enabled { state: GrepIndexStatus },
    AlreadyEnabled { state: GrepIndexStatus },
    Superseded,
}

/// Result of disabling grep for one namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrepDisableOutcome {
    Disabled,
    NotEnabled,
    Superseded,
}

/// Outcome of one bounded build or backfill step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepBuildOutcome {
    NotEnabled,
    UpToDate {
        built_through_seq: ChangeSeq,
    },
    Published {
        built_through_seq: ChangeSeq,
        indexed_revisions: u64,
        skipped_revisions: u64,
        segments_written: u64,
    },
    BackfillRestarted {
        captured_seq: ChangeSeq,
    },
    Superseded,
}

/// Outcome of one bounded reorganize step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepReorganizeOutcome {
    NotEnabled,
    NotNeeded {
        delta_runs: usize,
        mid_runs: usize,
    },
    UnitPublished {
        merged_rows: u64,
        segments_written: u64,
        completed: bool,
    },
    Superseded,
}

/// Bounded writer for grep-owned durable state.
///
/// The worker writes only grep keys. It reads namespace state through a
/// read-only `LoonFs` and creates or deletes backfill snapshots through a
/// writable `LoonFs`, whose writer id the snapshot folds record. Scheduling is
/// external to this type. A build or reorganize step that finds work waits
/// for a permit of the worker's [`GrepPassBudget`] before it reads file
/// content or index segments.
#[derive(Clone)]
pub struct GrepWorker<S> {
    store: S,
    reader: LoonFs<ReadOnly>,
    writer: LoonFs<Writable>,
    step_budget: GrepPassBudget,
    timer: Arc<dyn MonotonicTimer>,
}

/// The runtime handles have no debug representation (they are clones of a
/// shared runtime, not state), so a worker prints what identifies its work.
impl<S: std::fmt::Debug> std::fmt::Debug for GrepWorker<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrepWorker")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    /// Creates a worker over one grep-keyspace store handle and the runtime
    /// handles it reads and creates snapshots through. Its steps share
    /// `step_budget` with every other worker given a clone of it, and its
    /// deadlines run on `reader`'s monotonic timer.
    pub fn new(
        store: S,
        reader: LoonFs<ReadOnly>,
        writer: LoonFs<Writable>,
        step_budget: GrepPassBudget,
    ) -> Self {
        let timer = reader.monotonic_timer();
        Self {
            store,
            reader,
            writer,
            step_budget,
            timer,
        }
    }

    /// The monotonic timer this worker's deadlines and observations run on.
    pub(crate) fn timer(&self) -> Arc<dyn MonotonicTimer> {
        Arc::clone(&self.timer)
    }

    /// Reads the wall clock of the runtime this worker writes through.
    pub(crate) fn now_ms(&self) -> u64 {
        self.writer.now_ms()
    }

    /// The budget this worker's build and reorganize steps draw from.
    pub fn step_budget(&self) -> &GrepPassBudget {
        &self.step_budget
    }

    /// This worker's filesystem reads for one namespace.
    pub(crate) fn reads(&self, namespace_id: &NamespaceId) -> NamespaceReads {
        NamespaceReads::new(self.reader.namespace(namespace_id))
    }

    /// The grep-owned keyspace this worker publishes through.
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Enables grep by pinning a checkpoint and publishing a numbered
    /// backfilling manifest. Enabling an active manifest is idempotent.
    pub async fn enable(&self, namespace_id: &NamespaceId) -> Result<GrepEnableOutcome> {
        self.reads(namespace_id).head().await?;
        let deadline = OperationClock::start(self.now_ms(), self.timer());
        let current =
            load_current_grep_manifest(&self.store, namespace_id, deadline.observe()).await?;
        if let Some(current) = &current {
            if !matches!(
                current.manifest_state().status(),
                GrepIndexStatus::Disabled {}
            ) {
                return Ok(GrepEnableOutcome::AlreadyEnabled {
                    state: current.manifest_state().status().clone(),
                });
            }
        }
        let Some(checkpoint) = self
            .publish_backfill(namespace_id, current.as_ref(), &deadline)
            .await?
        else {
            return Ok(GrepEnableOutcome::Superseded);
        };
        Ok(GrepEnableOutcome::Enabled {
            state: backfill_status(&checkpoint),
        })
    }

    /// Disables grep by publishing the next manifest. Existing segments become grep-GC
    /// candidates and are never deleted synchronously.
    pub async fn disable(&self, namespace_id: &NamespaceId) -> Result<GrepDisableOutcome> {
        self.reads(namespace_id).head().await?;
        let deadline = OperationClock::start(self.now_ms(), self.timer());
        let Some(current) = self.load_enabled(namespace_id, &deadline).await? else {
            return Ok(GrepDisableOutcome::NotEnabled);
        };
        let next = GrepManifestState::new(
            namespace_id.clone(),
            next_manifest_no(Some(&current))?,
            GrepIndexStatus::Disabled {},
            GrepIndexState {
                reorganize: None,
                next_run_no: current.manifest_state().index().next_run_no,
            },
            Vec::new(),
        )
        .expect("a successor of a grep manifest that decoding validated should be valid");
        match publish_grep_manifest(&self.store, Some(&current), &next, &deadline).await {
            Ok(_) => {
                if let Some(pin_id) = current.manifest_state().status().backfill_pin_id() {
                    self.delete_pin_if_present(namespace_id, pin_id).await;
                }
                Ok(GrepDisableOutcome::Disabled)
            }
            Err(GrepError::PublicationConflict { .. }) => Ok(GrepDisableOutcome::Superseded),
            Err(error) => Err(error),
        }
    }

    /// Reads this namespace's durable grep lifecycle, or `Disabled` where no
    /// manifest has ever been published.
    pub async fn lifecycle(&self, namespace_id: &NamespaceId) -> Result<GrepIndexStatus> {
        Ok(self
            .manifest_state(namespace_id)
            .await?
            .map_or(GrepIndexStatus::Disabled {}, |state| state.status().clone()))
    }

    /// Loads current index bookkeeping, or `None` when grep is not enabled.
    pub async fn manifest_state(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<GrepManifestState>> {
        Ok(
            load_current_grep_manifest(&self.store, namespace_id, Observation::now(self.timer()))
                .await?
                .map(|manifest| manifest.manifest_state().clone()),
        )
    }

    /// Returns the grep index's state and maintenance progress.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        self.reads(namespace_id).head().await?;
        let manifest = self.manifest_state(namespace_id).await?;
        let (status, next_run_no, reorganize_pending) = match &manifest {
            Some(manifest) => (
                v0::GrepIndexStatus::from(manifest.status()),
                manifest.index().next_run_no,
                manifest.index().reorganize.is_some(),
            ),
            None => (v0::GrepIndexStatus::Disabled, RunNo(0), false),
        };
        Ok(GrepIndex {
            namespace_id: namespace_id.clone(),
            status,
            next_run_no,
            reorganize_pending,
        })
    }

    async fn create_backfill_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_types::Snapshot> {
        Ok(self
            .writer
            .open_namespace(namespace_id)?
            .create_snapshot_with_options(
                GREP_BACKFILL_CHECKPOINT_NAME,
                &CreateSnapshotOptions {
                    ttl_ms: Some(GREP_BACKFILL_CHECKPOINT_TTL_MS),
                },
            )
            .await?)
    }

    /// Deletes a backfill pin. Deleting a missing pin succeeds. A failed
    /// delete is only logged, and collection removes the pin after it
    /// expires.
    async fn delete_pin_if_present(&self, namespace_id: &NamespaceId, pin_id: &PinId) {
        let deleted = match self.writer.open_namespace(namespace_id) {
            Ok(namespace) => namespace.delete_snapshot(pin_id).await.map(drop),
            Err(error) => Err(error),
        };
        match deleted {
            Ok(()) => {}
            Err(error) => tracing::warn!(
                %namespace_id,
                %pin_id,
                %error,
                "failed to delete a grep backfill pin"
            ),
        }
    }

    async fn restart_backfill(
        &self,
        namespace_id: &NamespaceId,
        current: &LoadedGrepManifest,
        deadline: &OperationClock,
    ) -> Result<GrepBuildOutcome> {
        let Some(checkpoint) = self
            .publish_backfill(namespace_id, Some(current), deadline)
            .await?
        else {
            return Ok(GrepBuildOutcome::Superseded);
        };
        if let Some(previous_pin_id) = current.manifest_state().status().backfill_pin_id() {
            self.delete_pin_if_present(namespace_id, previous_pin_id)
                .await;
        }
        Ok(GrepBuildOutcome::BackfillRestarted {
            captured_seq: checkpoint.captured_seq,
        })
    }

    /// Loads the current grep manifest, or `None` when grep was never enabled
    /// or is disabled.
    async fn load_enabled(
        &self,
        namespace_id: &NamespaceId,
        deadline: &OperationClock,
    ) -> Result<Option<LoadedGrepManifest>> {
        let current =
            load_current_grep_manifest(&self.store, namespace_id, deadline.observe()).await?;
        Ok(current.filter(|current| {
            !matches!(
                current.manifest_state().status(),
                GrepIndexStatus::Disabled {}
            )
        }))
    }

    /// Creates a backfill snapshot and publishes the backfilling successor of
    /// `current`. Returns the snapshot, or `None` when another publisher won.
    async fn publish_backfill(
        &self,
        namespace_id: &NamespaceId,
        current: Option<&LoadedGrepManifest>,
        deadline: &OperationClock,
    ) -> Result<Option<loonfs_types::Snapshot>> {
        let manifest_no = next_manifest_no(current)?;
        let checkpoint = self.create_backfill_checkpoint(namespace_id).await?;
        let next = GrepManifestState::new(
            namespace_id.clone(),
            manifest_no,
            backfill_status(&checkpoint),
            GrepIndexState {
                reorganize: None,
                next_run_no: current.map_or(RunNo(0), |manifest| {
                    manifest.manifest_state().index().next_run_no
                }),
            },
            Vec::new(),
        )
        .expect("a backfilling grep manifest state should be valid");
        match publish_grep_manifest(&self.store, current, &next, deadline).await {
            Ok(_) => Ok(Some(checkpoint)),
            Err(GrepError::PublicationConflict { .. }) => {
                self.delete_pin_if_present(namespace_id, &checkpoint.snapshot_id)
                    .await;
                Ok(None)
            }
            // A non-conflict failure may be an acknowledgement failure after
            // the manifest landed. Keep the checkpoint because it
            // may already reference it.
            Err(error) => Err(error),
        }
    }
}

/// The status of a backfill that starts at the beginning of `checkpoint`.
fn backfill_status(checkpoint: &loonfs_types::Snapshot) -> GrepIndexStatus {
    GrepIndexStatus::Backfilling {
        captured_seq: checkpoint.captured_seq,
        cursor_inode_id: None,
        pin_id: checkpoint.snapshot_id.clone(),
    }
}

/// The errors that mean "the state this projection was built from is
/// gone": the change feed's cursor fell below the retention floor, the
/// backfill snapshot was deleted or expired, or it no longer pins its
/// manifest. Every other failure is a failure.
fn rebootstrap_required(error: &GrepError) -> bool {
    matches!(
        error,
        GrepError::Runtime(loonfs::Error::Core(
            CoreError::RebootstrapRequired { .. }
                | CoreError::SnapshotNotFound { .. }
                | CoreError::CheckpointUnavailable(_)
        ))
    )
}

fn next_manifest_no(current: Option<&LoadedGrepManifest>) -> Result<ManifestNo> {
    let Some(current) = current else {
        return Ok(ManifestNo(1));
    };
    current
        .manifest_no()
        .successor()
        .map_err(|_| GrepError::IndexCorrupt {
            message: format!(
                "the current grep manifest is number {}, the largest manifest number, so no \
                 manifest can follow it",
                current.manifest_no()
            ),
        })
}

/// The run number after the grep manifest's `current` one.
fn next_index_run_no(current: RunNo) -> Result<RunNo> {
    next_run_no_after(current).map_err(|_| GrepError::IndexCorrupt {
        message: format!(
            "the grep manifest's next run number is {current}, the largest run number, so no \
             run can follow it"
        ),
    })
}
