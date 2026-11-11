//! The grep worker: its lifecycle, its backfill pins, and the bounded steps
//! that build and reorganize one namespace's grep index.

mod build;
mod index_rows;
mod pruning;
mod reorganize;
mod steps;
mod tables;

pub use steps::{
    GrepProgress, GrepPublicationStats, GrepPublishOutcome, GrepSteps,
    DEFAULT_GREP_PUBLISH_INTERVAL_MS,
};

use crate::cache::GrepBlockCache;
use crate::entry::{GrepEntry, GrepIndexStatus};
use crate::reads::NamespaceReads;
use crate::{GrepError, GrepStepBudget, Result};
use loonfs::engine::{next_run_no_after, DecodedBlockCacheConfig, DecodedBlockCacheStats};
use loonfs::{CoreError, CreateSnapshotOptions, LoonFs, Maintenance, ReadOnly, Writable};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::segment_blocks::{
    DEFAULT_MAX_DELTA_RUNS, DEFAULT_MAX_ROWS_PER_SEGMENT, DEFAULT_TARGET_BLOCK_BYTES,
};
use loonfs_types::GrepIndex;
use loonfs_types::{CommitSeq, NamespaceId, PinId, RunNo};
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
/// This budgets ingestion, not index reorganization — it happens to share a value
/// with the metadata compaction input limit, and must stay independent
/// so a compaction policy change cannot silently change backfill behavior.
const GREP_BACKFILL_MAX_CONTENT_BYTES_PER_STEP: NonZeroU64 =
    NonZeroU64::new(64 * 1024 * 1024).unwrap();
/// Content reads and index segment writes the worker runs at once. Eight
/// reads of files at the 8 MiB eligibility cap keep at most 64 MiB, the same
/// as one backfill step's content budget.
pub(crate) const GREP_WORKER_REQUESTS_IN_FLIGHT: usize = 8;
/// Decoded bytes of the block cache that a worker's build and reorganize
/// steps read index segments through. It fits 256 data blocks, enough for
/// the blocks that a build step's lookups of 256 values read, and it keeps
/// the blocks of a small segment that a lookup loads whole until the lookup
/// reads them.
const GREP_WORKER_BLOCK_CACHE_DECODED_BYTES: usize = 256 * DEFAULT_TARGET_BLOCK_BYTES.get();
// Keep grep's fold threshold independent of metadata reorganization limits.
const GREP_MAX_MID_RUNS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// Rows one reorganize step rewrites before it publishes and yields.
const GREP_REORGANIZE_MAX_INPUT_ROWS_PER_STEP: NonZeroUsize = NonZeroUsize::new(131_072).unwrap();

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
    /// Rows one reorganize step rewrites before publishing and yielding.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
}

impl Default for GramIndexBuildPolicy {
    fn default() -> Self {
        Self {
            max_files_per_step: const { NonZeroUsize::new(256).unwrap() },
            max_content_bytes_per_step: GREP_BACKFILL_MAX_CONTENT_BYTES_PER_STEP,
            max_rows_per_segment: DEFAULT_MAX_ROWS_PER_SEGMENT,
            max_delta_runs: DEFAULT_MAX_DELTA_RUNS,
            max_mid_runs: GREP_MAX_MID_RUNS,
            max_decoded_input_rows_per_step: GREP_REORGANIZE_MAX_INPUT_ROWS_PER_STEP,
        }
    }
}

/// Result of enabling grep for one namespace.
///
/// Both settled outcomes return the durable lifecycle itself rather
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
        built_through_seq: CommitSeq,
    },
    /// The step indexed a range of the namespace's files or commits, and its
    /// change to grep's entry is pending or published.
    Indexed {
        built_through_seq: CommitSeq,
        /// Revisions whose content value the step read and tokenized.
        indexed_revisions: u64,
        /// Revisions whose content value the step didn't tokenize, because
        /// the value is ineligible or the index has already examined it.
        skipped_revisions: u64,
        segments_written: u64,
    },
    BackfillRestarted {
        captured_seq: CommitSeq,
    },
    /// Another process changed grep's entry before the step's publication,
    /// so the step's change and every change pending before it weren't
    /// published.
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
    /// The step wrote one bounded unit of the reorganization's output, and
    /// its change to grep's entry is pending or published.
    UnitWritten {
        combined_rows: u64,
        segments_written: u64,
        completed: bool,
    },
    /// The step dropped a reorganization into the base level, either
    /// because a commit since its filters' sequence brought back a value that
    /// it drops, or because the retention floor passed that sequence. The
    /// next step starts a new reorganization.
    Abandoned,
    /// Another process changed grep's entry before the step's publication,
    /// so the step's change and every change pending before it weren't
    /// published.
    Superseded,
}

/// Bounded writer of grep's index.
///
/// The worker writes grep segments into one store, and it publishes grep's
/// entry in the namespace's manifests through a [`Maintenance`]. It reads
/// namespace state through a read-only `LoonFs`, and creates or deletes
/// backfill snapshots through a writable `LoonFs`. Scheduling is external to
/// this type. A build or reorganize step that finds work waits for a permit
/// of the worker's [`GrepStepBudget`] before it reads file content or index
/// segments.
#[derive(Clone)]
pub struct GrepWorker<S> {
    store: S,
    reader: LoonFs<ReadOnly>,
    writer: LoonFs<Writable>,
    maintenance: Maintenance,
    step_budget: GrepStepBudget,
    /// The index blocks that build and reorganize steps read, which keeps
    /// the `texts` blocks that later build steps look up again, and the value
    /// table blocks that the steps of one reorganization read again, within
    /// a bounded size. Clones of a worker share it.
    blocks: Arc<GrepBlockCache>,
    /// How the worker's publications ended. Clones of a worker share it.
    publications: Arc<steps::PublicationCounts>,
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
    /// Creates a worker that writes grep segments into `store`, reads through
    /// `reader`, creates snapshots through `writer`, and publishes grep's
    /// entry through `maintenance`. Its steps share `step_budget` with every
    /// other worker given a clone of it.
    pub fn new(
        store: S,
        reader: LoonFs<ReadOnly>,
        writer: LoonFs<Writable>,
        maintenance: Maintenance,
        step_budget: GrepStepBudget,
    ) -> Self {
        Self {
            store,
            reader,
            writer,
            maintenance,
            step_budget,
            blocks: Arc::new(GrepBlockCache::new(DecodedBlockCacheConfig::new(
                GREP_WORKER_BLOCK_CACHE_DECODED_BYTES,
            ))),
            publications: Arc::default(),
        }
    }

    /// The budget this worker's build and reorganize steps draw from.
    pub fn step_budget(&self) -> &GrepStepBudget {
        &self.step_budget
    }

    /// The counters and retained bytes of the block cache that this worker's
    /// steps and its clones' steps read index segments through.
    pub fn block_cache_stats(&self) -> DecodedBlockCacheStats {
        self.blocks.stats()
    }

    /// Reports how the publications of grep's entry by this worker and its
    /// clones ended.
    pub fn publication_stats(&self) -> GrepPublicationStats {
        self.publications.stats()
    }

    /// This worker's filesystem reads for one namespace.
    pub(crate) fn reads(&self, namespace_id: &NamespaceId) -> NamespaceReads {
        NamespaceReads::new(self.reader.clone(), namespace_id)
    }

    /// Starts a run of steps over `namespace_id` whose changes are published
    /// once `publish_interval_ms` has passed since the change began, and when
    /// the caller calls [`GrepSteps::publish`].
    pub fn steps(&self, namespace_id: &NamespaceId, publish_interval_ms: u64) -> GrepSteps<'_, S> {
        GrepSteps::new(self, namespace_id, publish_interval_ms)
    }

    /// Enables grep by pinning a snapshot and publishing a backfilling entry.
    /// Enabling an enabled index changes nothing.
    ///
    /// If grep's entry is invalid, enabling returns
    /// [`GrepError::IndexCorrupt`], whose message says to disable grep first
    /// (see [`Self::disable`]). If the entry has another version, enabling
    /// returns [`GrepError::UnsupportedVersion`].
    pub async fn enable(&self, namespace_id: &NamespaceId) -> Result<GrepEnableOutcome> {
        let mut steps = self.steps(namespace_id, 0);
        let pending = steps.pending().await?;
        if let Some(entry) = pending.enabled_entry() {
            return Ok(GrepEnableOutcome::AlreadyEnabled {
                state: entry.status().clone(),
            });
        }
        let expected = pending.entry.clone();
        let Some(checkpoint) = steps.start_backfill(expected.as_ref()).await? else {
            // Another process changed the entry while the snapshot was
            // created.
            return Ok(match steps.pending().await?.enabled_entry() {
                Some(entry) => GrepEnableOutcome::AlreadyEnabled {
                    state: entry.status().clone(),
                },
                None => GrepEnableOutcome::Superseded,
            });
        };
        Ok(match steps.publish().await? {
            GrepPublishOutcome::Superseded => GrepEnableOutcome::Superseded,
            GrepPublishOutcome::Published | GrepPublishOutcome::NothingPending => {
                GrepEnableOutcome::Enabled {
                    state: backfill_status(&checkpoint),
                }
            }
        })
    }

    /// Disables grep by publishing an entry that lists no segments. The
    /// segments become garbage-collection candidates and are never deleted
    /// synchronously.
    ///
    /// An invalid entry of this build's version (one that doesn't decode or
    /// fails validation) is replaced the same way, so grep can be enabled
    /// again. Disabling can't read which backfill pin such an entry refers
    /// to, so it doesn't delete the pin, and garbage collection removes the
    /// pin after the pin expires. If the entry has another version, disabling
    /// returns [`GrepError::UnsupportedVersion`] and leaves the entry in
    /// place, so a build never discards an index that it can't read.
    pub async fn disable(&self, namespace_id: &NamespaceId) -> Result<GrepDisableOutcome> {
        let mut steps = self.steps(namespace_id, 0);
        let pending = steps.pending_for_disable().await?;
        if let Some(entry) = pending.enabled_entry() {
            if let Some(pin_id) = entry.status().backfill_pin_id().cloned() {
                pending.record_released_pin(pin_id);
            }
            pending.set_entry(GrepEntry::disabled());
        }
        // Nothing is pending when grep is disabled or was never enabled.
        Ok(match steps.publish().await? {
            GrepPublishOutcome::Superseded => GrepDisableOutcome::Superseded,
            GrepPublishOutcome::Published => GrepDisableOutcome::Disabled,
            GrepPublishOutcome::NothingPending => GrepDisableOutcome::NotEnabled,
        })
    }

    /// Reads this namespace's grep lifecycle from its current manifest, or
    /// `Disabled` when grep was never enabled.
    pub async fn lifecycle(&self, namespace_id: &NamespaceId) -> Result<GrepIndexStatus> {
        Ok(self
            .entry(namespace_id)
            .await?
            .map_or(GrepIndexStatus::Disabled {}, |entry| entry.status().clone()))
    }

    /// Reads grep's entry from the namespace's current manifest, or `None`
    /// when grep was never enabled.
    pub async fn entry(&self, namespace_id: &NamespaceId) -> Result<Option<GrepEntry>> {
        let mut steps = self.steps(namespace_id, 0);
        Ok(steps.pending().await?.published().cloned())
    }

    /// Returns the grep index's state and maintenance progress.
    pub async fn get_grep_index(&self, namespace_id: &NamespaceId) -> Result<GrepIndex> {
        let entry = self.entry(namespace_id).await?;
        Ok(GrepIndex {
            namespace_id: namespace_id.clone(),
            status: entry
                .as_ref()
                .map_or(loonfs_types::GrepIndexStatus::Disabled, |entry| {
                    loonfs_types::GrepIndexStatus::from(entry.status())
                }),
            reorganize_pending: entry.is_some_and(|entry| entry.reorganize().is_some()),
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
}

impl<S: ObjectStore + Clone> GrepSteps<'_, S> {
    /// Pins a snapshot and, if the pending entry is still `expected`, makes
    /// the pending entry a backfill from the snapshot's beginning.
    ///
    /// Returns the snapshot, or `None` after deleting it when the entry
    /// changed.
    ///
    /// The backfill keeps the entry's segments and reorganization, because
    /// the index's postings refer to content values, which stay valid, and
    /// the backfill skips each value that the segments' `texts` rows record.
    /// So a rebootstrap, and a fork's restart of a backfill that it
    /// inherited, cost a `texts` lookup for each value that the index has.
    ///
    /// Creating a snapshot of a head with an unfolded WAL tail publishes the
    /// manifest that folds the tail, and a change pending over the manifest
    /// before it would then lose its put. So the run first publishes a change
    /// that's pending, and returns `None` if that publication is superseded.
    /// Once the snapshot exists, the change starts again from the current
    /// manifest. The entry there can differ from the one the step planned
    /// from, such as when another process disabled the index, and the
    /// backfill doesn't replace it.
    async fn start_backfill(
        &mut self,
        expected: Option<&GrepEntry>,
    ) -> Result<Option<loonfs_types::Snapshot>> {
        if self.is_changed() && self.publish().await? == GrepPublishOutcome::Superseded {
            return Ok(None);
        }
        let checkpoint = self
            .worker
            .create_backfill_checkpoint(&self.namespace_id)
            .await?;
        self.reload_unless_changed();
        let pending = self.pending().await?;
        if pending.entry.as_ref() != expected {
            self.worker
                .delete_pin_if_present(&self.namespace_id, &checkpoint.snapshot_id)
                .await;
            return Ok(None);
        }
        pending.record_created_pin(checkpoint.snapshot_id.clone());
        let previous_pin_id = pending
            .entry
            .as_ref()
            .and_then(|entry| entry.status().backfill_pin_id().cloned());
        if let Some(pin_id) = previous_pin_id {
            pending.record_released_pin(pin_id);
        }
        let (reorganize, runs) = pending.entry.as_ref().map_or_else(
            || (None, Vec::new()),
            |entry| (entry.reorganize().cloned(), entry.runs().to_vec()),
        );
        pending.set_entry(
            GrepEntry::new(backfill_status(&checkpoint), reorganize, runs)
                .expect("a backfill should keep a valid entry's segments"),
        );
        Ok(Some(checkpoint))
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

/// Returns the run number after `current`, which a change took.
fn next_index_run_no(current: RunNo) -> Result<RunNo> {
    next_run_no_after(current).map_err(|_| GrepError::IndexCorrupt {
        message: format!(
            "the manifest's next run number is {current}, the largest run number, so no run can \
             follow it"
        ),
    })
}
