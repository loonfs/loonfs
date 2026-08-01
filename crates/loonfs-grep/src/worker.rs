//! Explicit grep building, checkpointed backfill, reorganization, and
//! garbage collection.
//!
//! Reorganization is the same idea as the runtime's own metadata
//! reorganization and uses the same word: a bounded step merges older runs
//! into a newer one and publishes the result. The grep index carries one
//! more level than the metadata store — delta, mid, base, against the
//! metadata store's delta and base — because a gram posting list fans out
//! far wider than a metadata row: every indexed file contributes to many
//! gram keys, so merging deltas straight into the base would rewrite most
//! of the base on every step. The mid level absorbs that churn.

use crate::cache::{GrepBlockCache, MAX_CACHED_GREP_BLOCKS};
use crate::codec::{
    extract_grams, lookup::GRAM_ROW_PREFIX, Gram, GramPosting, IndexRow, INDEX_GRAMS_MAX_FILE_BYTES,
};
use crate::index_read::{load_data_block, load_index_block};
use crate::keyspace::{
    manifest_key, namespace_prefix, parse_key, root_key, segment_key, GrepKeyKind,
};
use crate::reads::{published_revision, NamespaceReads};
use crate::root::{
    advance_grep_root, load_grep_root, seed_grep_root, ChangeFeedResume, GrepIndexState,
    GrepLifecycle, GrepManifestState, GrepReorganizeState, GrepRootError, GrepSegmentRef,
    LoadedGrepRoot,
};
use crate::service::is_indexable_text_content;
use crate::{GrepError, Result};
use futures::future::try_join_all;
use futures::StreamExt as _;
use loonfs::{
    delete_if_aged, CheckpointFilesPageCursor, CoreError, CreateCheckpointOptions, FsAdmin,
    FsReader, PassBudget, RuntimeError, StoreFailureClass, DEFAULT_GC_MAX_OBJECTS,
    GC_MIN_GRACE_WINDOW_MS, METADATA_PUBLICATION_BUDGET_MS,
};
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::wire::sst_blocks::{
    index_blocks_for_key_range, DecodedDataBlock, SegmentBlocksBuilder, SegmentIndexEntry,
};
use loonfs_api::{
    decode_namespace_cursor, encode_cursor, sha256_digest, ChangeSeq, CheckpointId, ContentRef,
    ErrorCode, IndexSegmentId, InodeId, NamespaceCursor, NamespaceCursorError, NamespaceId,
    PageCursor, RevisionNo,
};
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

/// User-checkpoint lifetime used by one backfill attempt.
///
/// A backfill that outlives it needs no rescue and no extension: the pin
/// keeps serving until a garbage-collection pass releases it, and the
/// release is what the enumeration reports, at which point the worker
/// rebootstraps onto a fresh checkpoint and a reset cursor. Nothing here
/// refreshes a pin — an attempt this long has lost its race with the
/// retention floor anyway, and starting over is the cheaper answer.
pub const GREP_BACKFILL_CHECKPOINT_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Unreferenced grep objects receive one hour of unconditional protection
/// before grep-owned garbage collection may delete them.
///
/// The window has to outlast the longest publication that could still be
/// holding an object it has not yet made reachable. Grep's publications are
/// bounded by the same [`METADATA_PUBLICATION_BUDGET_MS`] the runtime's own
/// are, so the runtime's derived floor is the bound grep must clear too.
pub const GREP_GC_GRACE_WINDOW_MS: u64 = 60 * 60 * 1000;

// The floor is derived from publication budgets and provider bounds rather
// than chosen, so a window that stopped clearing it is a compile error here
// instead of an unreproducible delete of an object a publication was about
// to reference.
const _: () = assert!(
    GREP_GC_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS,
    "grep's grace window must outlast the longest publication that could still \
     reference an object it has written"
);

const GREP_BACKFILL_CHECKPOINT_NAME: &str = "loonfs-grep-backfill";
const GRAM_POSTING_BATCH_TARGET: usize = 256;
const INLINE_INDEX_FILTER_MAX_BYTES: u32 = 1024;
const MAX_GREP_WORKER_IO: usize = 8;
const INDEX_GRAMS_DELTA_LEVEL: u32 = 0;
const INDEX_GRAMS_MID_LEVEL: u32 = 1;
const INDEX_GRAMS_BASE_LEVEL: u32 = 2;

/// Writer-side budgets for one grep build or reorganize step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GramIndexBuildPolicy {
    /// Revisions examined per step.
    pub max_files_per_step: NonZeroUsize,
    /// Content bytes read per step.
    pub max_content_bytes_per_step: NonZeroU64,
    /// Rows per written grep segment.
    pub max_rows_per_segment: NonZeroUsize,
    /// Delta-level runs that trigger a reorganization into a fresh mid run.
    pub max_l0_runs: NonZeroUsize,
    /// Mid-level runs that trigger a reorganization into a fresh base run.
    pub max_mid_runs: NonZeroUsize,
    /// Rows one reorganize step merges before publishing and yielding.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
}

impl Default for GramIndexBuildPolicy {
    fn default() -> Self {
        Self {
            max_files_per_step: NonZeroUsize::new(256)
                .expect("default file budget should be nonzero"),
            max_content_bytes_per_step: NonZeroU64::new(64 * 1024 * 1024)
                .expect("default content budget should be nonzero"),
            max_rows_per_segment: NonZeroUsize::new(65_536)
                .expect("default segment row budget should be nonzero"),
            max_l0_runs: NonZeroUsize::new(8).expect("default delta run limit should be nonzero"),
            max_mid_runs: NonZeroUsize::new(8).expect("default mid run limit should be nonzero"),
            max_decoded_input_rows_per_step: NonZeroUsize::new(131_072)
                .expect("default reorganize row budget should be nonzero"),
        }
    }
}

/// Result of enabling grep for one namespace.
///
/// Both settled outcomes answer with the durable lifecycle itself rather
/// than a sequence number, because the two phases do not have the same
/// number to give: a fresh enable publishes a backfill with a target, and an
/// already-enabled namespace may be mid-backfill or steady.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepEnableOutcome {
    Enabled { state: GrepLifecycle },
    AlreadyEnabled { state: GrepLifecycle },
    Superseded,
}

/// Result of disabling grep for one namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrepDisableOutcome {
    Disabled,
    NotEnabled,
    Superseded,
}

/// Report from one bounded build or backfill step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepBuildReport {
    pub namespace_id: NamespaceId,
    pub outcome: GrepBuildOutcome,
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
        materialized: bool,
    },
    BackfillRestarted {
        target_seq: ChangeSeq,
    },
    Superseded,
}

/// Report from one bounded reorganize step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepReorganizeReport {
    pub namespace_id: NamespaceId,
    pub outcome: GrepReorganizeOutcome,
}

/// Outcome of one bounded reorganize step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepReorganizeOutcome {
    NotEnabled,
    NotNeeded {
        l0_runs: usize,
        mid_runs: usize,
    },
    StepPublished {
        merged_rows: u64,
        segments_written: u64,
        completed: bool,
    },
    Superseded,
}

/// Budgets and resume position for one grep garbage-collection pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrepGcRequest {
    /// Reads this pass may spend before returning a resume cursor. Absent
    /// resolves to the runtime's own per-pass default, so no caller can ask
    /// for an unbounded walk by leaving the field out.
    pub max_objects: Option<u64>,
    /// Opaque token an earlier pass over the same namespace returned.
    pub cursor: Option<String>,
}

/// Counts from one namespace's grep garbage-collection pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrepGcReport {
    pub deleted_segments: u64,
    pub deleted_other_objects: u64,
    pub namespace_reaped: bool,
    pub retained_candidates: u64,
    pub namespace_degraded: bool,
    /// Present when the budget stopped the pass with keys left to examine.
    pub next_cursor: Option<String>,
}

/// Namespace-independent writer for the grep-owned durable keyspace.
///
/// Calls are explicit and bounded. Whatever schedules them decides when to
/// call them without changing the durable protocol.
///
/// The worker writes only grep's own keyspace, through `store`. Everything
/// it needs from the filesystem it takes from the two runtime handles it
/// borrows: reads through the reader, and the backfill checkpoint's
/// creation and release through the admin handle — so the checkpoints grep
/// pins are recorded under that handle's actor identity, like any other
/// operator-created pin.
#[derive(Clone)]
pub struct GrepWorker<S> {
    store: S,
    reader: FsReader,
    admin: FsAdmin,
    block_cache: Arc<GrepBlockCache>,
}

/// The runtime handles carry no debug representation — they are clones of a
/// shared runtime, not state — so a worker prints what identifies its work.
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
    /// handles it reads and checkpoints through.
    pub fn new(store: S, reader: FsReader, admin: FsAdmin) -> Self {
        Self {
            store,
            reader,
            admin,
            block_cache: Arc::new(GrepBlockCache::new(MAX_CACHED_GREP_BLOCKS)),
        }
    }

    /// This worker's filesystem reads for one namespace.
    pub(crate) fn reads<'a>(&'a self, namespace_id: &'a NamespaceId) -> NamespaceReads<'a> {
        NamespaceReads::new(&self.reader, namespace_id)
    }

    /// The grep-owned keyspace this worker publishes through.
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Enables grep by pinning a checkpoint and CAS-publishing a fresh
    /// backfilling root. Enabling an active root is idempotent.
    pub async fn enable(&self, namespace_id: &NamespaceId) -> Result<GrepEnableOutcome> {
        if let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        {
            if !matches!(
                current.manifest_state().lifecycle(),
                GrepLifecycle::Disabled
            ) {
                return Ok(GrepEnableOutcome::AlreadyEnabled {
                    state: current.manifest_state().lifecycle().clone(),
                });
            }
        }

        let checkpoint = self.create_backfill_checkpoint(namespace_id).await?;
        let current = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?;
        if let Some(current) = &current {
            if !matches!(
                current.manifest_state().lifecycle(),
                GrepLifecycle::Disabled
            ) {
                self.release_checkpoint_if_unreferenced(
                    namespace_id,
                    &checkpoint.checkpoint_id,
                    current.manifest_state(),
                )
                .await?;
                return Ok(GrepEnableOutcome::AlreadyEnabled {
                    state: current.manifest_state().lifecycle().clone(),
                });
            }
        }
        let next_run_ordinal = current
            .as_ref()
            .map_or(0, |root| root.manifest_state().index().next_run_ordinal);
        let next = backfilling_root(
            namespace_id,
            checkpoint.checkpoint_seq,
            checkpoint.checkpoint_id.clone(),
            next_run_ordinal,
        )?;
        let published = match current {
            Some(current) => self.advance_root(&current, &next).await,
            None => self.seed_root(&next).await,
        };
        match published {
            Ok(published) => Ok(GrepEnableOutcome::Enabled {
                state: published.manifest_state().lifecycle().clone(),
            }),
            Err(GrepRootError::Conflict { .. }) => {
                self.release_superseded_checkpoint_if_unreferenced(
                    namespace_id,
                    &checkpoint.checkpoint_id,
                )
                .await?;
                Ok(GrepEnableOutcome::Superseded)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Disables grep with one root CAS. Existing segments become grep-GC
    /// candidates and are never deleted synchronously.
    pub async fn disable(&self, namespace_id: &NamespaceId) -> Result<GrepDisableOutcome> {
        ensure_live_namespace(&self.reads(namespace_id)).await?;
        let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        else {
            return Ok(GrepDisableOutcome::NotEnabled);
        };
        if matches!(
            current.manifest_state().lifecycle(),
            GrepLifecycle::Disabled
        ) {
            return Ok(GrepDisableOutcome::NotEnabled);
        }
        let checkpoint_id = match current.manifest_state().lifecycle() {
            GrepLifecycle::Backfilling { checkpoint_id, .. } => Some(checkpoint_id.clone()),
            GrepLifecycle::Steady { .. } | GrepLifecycle::Disabled => None,
        };
        let next = GrepManifestState::new(
            namespace_id.clone(),
            GrepLifecycle::Disabled,
            GrepIndexState::new(None, current.manifest_state().index().next_run_ordinal),
            Vec::new(),
        )
        .map_err(core_state_error)?;
        match self.advance_root(&current, &next).await {
            Ok(_) => {
                if let Some(checkpoint_id) = checkpoint_id {
                    self.release_checkpoint(namespace_id, &checkpoint_id)
                        .await?;
                }
                Ok(GrepDisableOutcome::Disabled)
            }
            Err(GrepRootError::Conflict { .. }) => Ok(GrepDisableOutcome::Superseded),
            Err(error) => Err(error.into()),
        }
    }

    /// Runs one bounded checkpoint-backfill or incremental build unit.
    pub async fn build_step(
        &self,
        namespace_id: &NamespaceId,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepBuildReport> {
        let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        else {
            return Ok(build_report(namespace_id, GrepBuildOutcome::NotEnabled));
        };
        if matches!(
            current.manifest_state().lifecycle(),
            GrepLifecycle::Disabled
        ) {
            return Ok(build_report(namespace_id, GrepBuildOutcome::NotEnabled));
        }
        let reads = self.reads(namespace_id);

        let collected = match current.manifest_state().lifecycle() {
            GrepLifecycle::Backfilling {
                target_seq,
                cursor,
                checkpoint_id,
            } => collect_backfill_unit(&reads, checkpoint_id, *target_seq, *cursor, policy).await,
            GrepLifecycle::Steady {
                built_through_seq,
                next_event_index,
            } => {
                collect_incremental_unit(&reads, *built_through_seq, *next_event_index, policy)
                    .await
            }
            GrepLifecycle::Disabled => unreachable!("disabled returned above"),
        };

        let unit = match collected {
            Ok(Some(unit)) => unit,
            // Only a steady root can be up to date: a backfill always has
            // its next page, and answering `None` there would be a claim
            // about a watermark it does not have.
            Ok(None) => {
                let built_through_seq = current
                    .manifest_state()
                    .lifecycle()
                    .steady_watermark()
                    .map(|(built_through_seq, _)| built_through_seq)
                    .ok_or_else(|| GrepError::CorruptIndex {
                        message: "a backfilling grep root reported nothing left to index"
                            .to_owned(),
                    })?;
                return Ok(build_report(
                    namespace_id,
                    GrepBuildOutcome::UpToDate { built_through_seq },
                ));
            }
            // The projection's basis is gone. Either the change feed no
            // longer retains history back to the watermark, or the pinned
            // checkpoint stopped pinning its manifest. The old checkpoint
            // reader answered the second case with `Ok(None)` and restarted
            // silently; both are explicit now, and both mean the same thing:
            // discard the incomplete projection and start a fresh backfill
            // from a new checkpoint.
            Err(error) if rebootstrap_required(&error) => {
                return self.restart_backfill(namespace_id, &current).await;
            }
            Err(error) => return Err(error),
        };

        self.publish_build_unit(namespace_id, current, unit, policy)
            .await
    }

    /// Reads this namespace's durable grep lifecycle, or `Disabled` where no
    /// root has ever been published.
    ///
    /// One object read and no side effects: this is what a status surface
    /// and a caller waiting on a captured target both ask.
    pub async fn lifecycle(&self, namespace_id: &NamespaceId) -> Result<GrepLifecycle> {
        Ok(load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
            .map_or(GrepLifecycle::Disabled, |root| {
                root.manifest_state().lifecycle().clone()
            }))
    }

    /// Reads the whole durable root, for a caller that wants the index
    /// bookkeeping beside the lifecycle. `None` where no root exists.
    pub async fn root_state(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<GrepManifestState>> {
        Ok(load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
            .map(|root| root.manifest_state().clone()))
    }

    async fn seed_root(
        &self,
        state: &GrepManifestState,
    ) -> std::result::Result<LoadedGrepRoot, GrepRootError> {
        seed_grep_root(&self.store, state).await
    }

    async fn advance_root(
        &self,
        current: &LoadedGrepRoot,
        next: &GrepManifestState,
    ) -> std::result::Result<LoadedGrepRoot, GrepRootError> {
        advance_grep_root(&self.store, current, next).await
    }

    async fn create_backfill_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_api::CreateCheckpointResponse> {
        Ok(self
            .admin
            .create_checkpoint(
                namespace_id,
                CreateCheckpointOptions {
                    name: GREP_BACKFILL_CHECKPOINT_NAME.to_owned(),
                    ttl_ms: Some(GREP_BACKFILL_CHECKPOINT_TTL_MS),
                },
            )
            .await?)
    }

    async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<()> {
        self.admin
            .release_checkpoint(namespace_id, checkpoint_id)
            .await?;
        Ok(())
    }

    async fn release_checkpoint_if_unreferenced(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
        root: &GrepManifestState,
    ) -> Result<()> {
        if !root_names_checkpoint(root, checkpoint_id) {
            self.release_checkpoint(namespace_id, checkpoint_id).await?;
        }
        Ok(())
    }

    async fn release_superseded_checkpoint_if_unreferenced(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<()> {
        let winner = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?;
        if let Some(winner) = winner {
            self.release_checkpoint_if_unreferenced(
                namespace_id,
                checkpoint_id,
                winner.manifest_state(),
            )
            .await
        } else {
            self.release_checkpoint(namespace_id, checkpoint_id).await
        }
    }

    async fn restart_backfill(
        &self,
        namespace_id: &NamespaceId,
        current: &LoadedGrepRoot,
    ) -> Result<GrepBuildReport> {
        let previous_checkpoint_id = match current.manifest_state().lifecycle() {
            GrepLifecycle::Backfilling { checkpoint_id, .. } => Some(checkpoint_id.clone()),
            GrepLifecycle::Steady { .. } | GrepLifecycle::Disabled => None,
        };
        let checkpoint = self.create_backfill_checkpoint(namespace_id).await?;
        let next = backfilling_root(
            namespace_id,
            checkpoint.checkpoint_seq,
            checkpoint.checkpoint_id.clone(),
            current.manifest_state().index().next_run_ordinal,
        )?;
        match self.advance_root(current, &next).await {
            Ok(_) => {
                if let Some(previous_checkpoint_id) = previous_checkpoint_id {
                    if previous_checkpoint_id != checkpoint.checkpoint_id {
                        self.release_checkpoint(namespace_id, &previous_checkpoint_id)
                            .await?;
                    }
                }
                Ok(build_report(
                    namespace_id,
                    GrepBuildOutcome::BackfillRestarted {
                        target_seq: checkpoint.checkpoint_seq,
                    },
                ))
            }
            Err(GrepRootError::Conflict { .. }) => {
                self.release_superseded_checkpoint_if_unreferenced(
                    namespace_id,
                    &checkpoint.checkpoint_id,
                )
                .await?;
                Ok(build_report(namespace_id, GrepBuildOutcome::Superseded))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn publish_build_unit(
        &self,
        namespace_id: &NamespaceId,
        current: LoadedGrepRoot,
        unit: CollectedIndexUnit,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepBuildReport> {
        let rows = gram_postings_rows(unit.postings)?;
        let timer = StdMonotonicTimer::default();
        let publication_started_ms = timer.monotonic_now_ms();
        let new_segments = write_index_segments(
            &self.store,
            namespace_id,
            unit.run_seq,
            current.manifest_state().index().next_run_ordinal,
            rows,
            policy.max_rows_per_segment,
            INDEX_GRAMS_DELTA_LEVEL,
        )
        .await?;
        let segments_written = new_segments.len() as u64;
        let mut segments = current.manifest_state().segments().to_vec();
        segments.extend(new_segments);
        let next_run_ordinal =
            current.manifest_state().index().next_run_ordinal + u64::from(segments_written > 0);
        // A finished backfill turns steady at exactly the sequence its
        // checkpoint captured: the walk answered that state and no other, so
        // the watermark it hands the change feed is the target it reached.
        let (lifecycle, completed_checkpoint_id) = match current.manifest_state().lifecycle() {
            GrepLifecycle::Backfilling {
                target_seq,
                checkpoint_id,
                ..
            } => match unit.backfill_cursor {
                Some(after_inode_id) => (
                    GrepLifecycle::Backfilling {
                        target_seq: *target_seq,
                        cursor: Some(after_inode_id),
                        checkpoint_id: checkpoint_id.clone(),
                    },
                    None,
                ),
                None => (
                    GrepLifecycle::Steady {
                        built_through_seq: unit.built_through_seq,
                        next_event_index: unit.next_event_index,
                    },
                    Some(checkpoint_id.clone()),
                ),
            },
            GrepLifecycle::Steady { .. } => (
                GrepLifecycle::Steady {
                    built_through_seq: unit.built_through_seq,
                    next_event_index: unit.next_event_index,
                },
                None,
            ),
            GrepLifecycle::Disabled => unreachable!("disabled returned before collection"),
        };
        let materialized = matches!(lifecycle, GrepLifecycle::Steady { .. });
        let next = GrepManifestState::new(
            namespace_id.clone(),
            lifecycle,
            GrepIndexState::new(
                current.manifest_state().index().reorganize.clone(),
                next_run_ordinal,
            ),
            segments,
        )
        .map_err(core_state_error)?;
        ensure_publication_budget(&timer, publication_started_ms)?;
        match self.advance_root(&current, &next).await {
            Ok(_) => {
                if let Some(checkpoint_id) = completed_checkpoint_id {
                    self.release_checkpoint(namespace_id, &checkpoint_id)
                        .await?;
                }
                Ok(build_report(
                    namespace_id,
                    GrepBuildOutcome::Published {
                        built_through_seq: unit.built_through_seq,
                        indexed_revisions: unit.indexed_revisions,
                        skipped_revisions: unit.skipped_revisions,
                        segments_written,
                        materialized,
                    },
                ))
            }
            Err(GrepRootError::Conflict { .. }) => {
                Ok(build_report(namespace_id, GrepBuildOutcome::Superseded))
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn build_report(namespace_id: &NamespaceId, outcome: GrepBuildOutcome) -> GrepBuildReport {
    GrepBuildReport {
        namespace_id: namespace_id.clone(),
        outcome,
    }
}

fn backfilling_root(
    namespace_id: &NamespaceId,
    target_seq: ChangeSeq,
    checkpoint_id: CheckpointId,
    next_run_ordinal: u64,
) -> Result<GrepManifestState> {
    GrepManifestState::new(
        namespace_id.clone(),
        GrepLifecycle::Backfilling {
            target_seq,
            cursor: None,
            checkpoint_id,
        },
        GrepIndexState::new(None, next_run_ordinal),
        Vec::new(),
    )
    .map_err(core_state_error)
}

/// The two answers that mean "the state this projection was built from is
/// gone": the change feed's cursor fell below the retention floor, and the
/// checkpoint enumeration's record no longer pins its manifest. Every other
/// failure is a failure.
fn rebootstrap_required(error: &GrepError) -> bool {
    matches!(
        error,
        GrepError::Runtime(RuntimeError::Core(
            CoreError::RebootstrapRequired { .. } | CoreError::CheckpointUnavailable(_)
        ))
    )
}

fn root_names_checkpoint(root: &GrepManifestState, checkpoint_id: &CheckpointId) -> bool {
    matches!(
        root.lifecycle(),
        GrepLifecycle::Backfilling {
            checkpoint_id: root_checkpoint_id,
            ..
        } if root_checkpoint_id == checkpoint_id
    )
}

struct CollectedIndexUnit {
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    indexed_revisions: u64,
    skipped_revisions: u64,
    run_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    next_event_index: u32,
    backfill_cursor: Option<InodeId>,
}

/// Collects one backfill step from the files the checkpoint pins.
///
/// The enumeration answers the checkpointed state directly — one current
/// revision per visible file, in ascending inode order — so a step reads
/// pages until one of its budgets is met and remembers the last inode it
/// consumed. The budgets are the ones the step always had: at most
/// `max_files_per_step` files examined, and content planning stops once
/// `max_content_bytes_per_step` is reached (the file that crosses it is
/// still included, exactly as the row walk did).
async fn collect_backfill_unit(
    reads: &NamespaceReads<'_>,
    checkpoint_id: &CheckpointId,
    target_seq: ChangeSeq,
    cursor: Option<InodeId>,
    policy: GramIndexBuildPolicy,
) -> Result<Option<CollectedIndexUnit>> {
    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: target_seq,
        built_through_seq: target_seq,
        next_event_index: 0,
        backfill_cursor: cursor,
    };
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut files_remaining = policy.max_files_per_step.get();
    let mut cursor = cursor.map(|after_inode_id| CheckpointFilesPageCursor { after_inode_id });
    loop {
        let page = reads
            .list_checkpoint_files_page(checkpoint_id, cursor, files_remaining)
            .await?;
        if page.checkpoint_seq != target_seq {
            // The root and its checkpoint disagree about which state is
            // being walked; the walk cannot be resumed against either.
            return Err(CoreError::CheckpointUnavailable(format!(
                "checkpoint `{checkpoint_id}` pins sequence `{}` but the grep root is \
                 backfilling sequence `{target_seq}`",
                page.checkpoint_seq
            ))
            .into());
        }
        let page_exhausted_family = page.next_cursor.is_none();
        let mut budget_reached = false;
        for file in page.files {
            files_remaining = files_remaining.saturating_sub(1);
            if file.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
                unit.skipped_revisions += 1;
            } else {
                planned_content_bytes += file.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id: file.inode_id,
                    revision_no: file.revision_no,
                    content_ref: file.content_ref,
                });
            }
            unit.backfill_cursor = Some(file.inode_id);
            if planned_content_bytes >= policy.max_content_bytes_per_step.get() {
                budget_reached = true;
                break;
            }
        }
        if budget_reached {
            break;
        }
        if page_exhausted_family {
            // Every file the checkpoint pins is indexed; the next published
            // root leaves backfill for the change feed.
            unit.backfill_cursor = None;
            break;
        }
        cursor = page.next_cursor;
        if files_remaining == 0 {
            break;
        }
    }
    load_and_fold_revision_contents(reads, &pending, &mut unit).await?;
    Ok(Some(unit))
}

/// Collects one incremental step from the semantic change feed.
///
/// `Ok(None)` means the index is already at the namespace head.
async fn collect_incremental_unit(
    reads: &NamespaceReads<'_>,
    built_through_seq: ChangeSeq,
    next_event_index: u32,
    policy: GramIndexBuildPolicy,
) -> Result<Option<CollectedIndexUnit>> {
    let resume = ChangeFeedResume::new(built_through_seq, next_event_index);
    let feed = reads
        .list_changes_after(resume.after_seq(), policy.max_files_per_step.get())
        .await?;
    if feed.changes.is_empty() {
        return Ok(None);
    }
    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: built_through_seq,
        built_through_seq,
        next_event_index,
        backfill_cursor: None,
    };
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut examined_files = 0usize;
    'changes: for change in feed.changes {
        let start_event_index = resume.start_event_index(change.seq).map_err(|_| {
            CoreError::Internal("grep event cursor does not fit in memory".to_owned())
        })?;
        if start_event_index > change.events.len() {
            return Err(GrepError::CorruptIndex {
                message: format!(
                    "grep event cursor `{}` exceeds commit `{}` length `{}`",
                    resume.next_event_index(),
                    change.seq,
                    change.events.len()
                ),
            });
        }
        for (event_index, event) in change.events.iter().enumerate().skip(start_event_index) {
            let Some(revision) = published_revision(event) else {
                continue;
            };
            let would_exceed_content_budget = planned_content_bytes > 0
                && planned_content_bytes.saturating_add(revision.content_ref.size_bytes)
                    > policy.max_content_bytes_per_step.get();
            if examined_files >= policy.max_files_per_step.get() || would_exceed_content_budget {
                if event_index > 0 {
                    unit.built_through_seq = change.seq;
                    unit.run_seq = change.seq;
                    unit.next_event_index = u32::try_from(event_index).map_err(|_| {
                        CoreError::Internal("grep event cursor overflow".to_owned())
                    })?;
                }
                break 'changes;
            }
            examined_files += 1;
            if revision.content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
                unit.skipped_revisions += 1;
            } else {
                planned_content_bytes += revision.content_ref.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    content_ref: revision.content_ref.clone(),
                });
            }
            if examined_files >= policy.max_files_per_step.get()
                || planned_content_bytes >= policy.max_content_bytes_per_step.get()
            {
                unit.built_through_seq = change.seq;
                unit.run_seq = change.seq;
                let next_event_index = event_index
                    .checked_add(1)
                    .ok_or_else(|| CoreError::Internal("grep event cursor overflow".to_owned()))?;
                if next_event_index < change.events.len() {
                    unit.next_event_index = u32::try_from(next_event_index).map_err(|_| {
                        CoreError::Internal("grep event cursor overflow".to_owned())
                    })?;
                } else {
                    unit.next_event_index = 0;
                }
                break 'changes;
            }
        }
        unit.built_through_seq = change.seq;
        unit.run_seq = change.seq;
        unit.next_event_index = 0;
    }
    if unit.built_through_seq == built_through_seq && unit.next_event_index == next_event_index {
        return Ok(None);
    }
    load_and_fold_revision_contents(reads, &pending, &mut unit).await?;
    Ok(Some(unit))
}

struct PendingRevisionContent {
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
}

async fn load_and_fold_revision_contents(
    reads: &NamespaceReads<'_>,
    pending: &[PendingRevisionContent],
    unit: &mut CollectedIndexUnit,
) -> Result<()> {
    for chunk in pending.chunks(MAX_GREP_WORKER_IO) {
        let contents = try_join_all(chunk.iter().map(|revision| {
            // Index eligibility is the worker's own read budget: content
            // past the cap was skipped before it was ever planned.
            reads.read_content_ref(&revision.content_ref, INDEX_GRAMS_MAX_FILE_BYTES)
        }))
        .await?;
        for (revision, content) in chunk.iter().zip(contents) {
            if !is_indexable_text_content(&content) {
                unit.skipped_revisions += 1;
                continue;
            }
            let posting = GramPosting {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
            };
            for gram in extract_grams(&content) {
                unit.postings.entry(gram).or_default().push(posting);
            }
            unit.indexed_revisions += 1;
        }
    }
    Ok(())
}

fn gram_postings_rows(postings: BTreeMap<Gram, Vec<GramPosting>>) -> Result<Vec<IndexRow>> {
    let mut rows = Vec::new();
    for (gram, mut gram_postings) in postings {
        gram_postings.sort_unstable();
        gram_postings.dedup();
        for batch in gram_postings.chunks(GRAM_POSTING_BATCH_TARGET) {
            rows.push(IndexRow::gram_postings(gram, batch).map_err(|error| {
                CoreError::Internal(format!("failed to build gram postings row: {error}"))
            })?);
        }
    }
    Ok(rows)
}

async fn write_index_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    run_ordinal: u64,
    rows: Vec<IndexRow>,
    max_rows_per_segment: NonZeroUsize,
    level: u32,
) -> Result<Vec<GrepSegmentRef>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut requests = Vec::new();
    for (segment_index, segment_rows) in rows.chunks(max_rows_per_segment.get()).enumerate() {
        let segment_index = u32::try_from(segment_index)
            .map_err(|_| CoreError::Internal("index segment index overflow".to_owned()))?;
        requests.push((segment_index, segment_rows.to_vec()));
    }
    let mut descriptors = Vec::with_capacity(requests.len());
    let mut pending = requests.into_iter();
    loop {
        let chunk = pending
            .by_ref()
            .take(MAX_GREP_WORKER_IO)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        descriptors.extend(
            try_join_all(chunk.into_iter().map(|(segment_index, segment_rows)| {
                write_index_segment(
                    store,
                    namespace_id,
                    run_seq,
                    run_ordinal,
                    segment_index,
                    segment_rows,
                    level,
                )
            }))
            .await?,
        );
    }
    Ok(descriptors)
}

async fn write_index_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    run_ordinal: u64,
    segment_index: u32,
    rows: Vec<IndexRow>,
    level: u32,
) -> Result<GrepSegmentRef> {
    let segment_id = IndexSegmentId::generate();
    let object_key = segment_key(namespace_id, &segment_id);
    let mut builder = SegmentBlocksBuilder::default();
    for row in &rows {
        builder
            .push(&row.row_key(), &row.filter_key(), row)
            .map_err(|error| {
                CoreError::Internal(format!(
                    "failed to build index segment `{object_key}`: {error}"
                ))
            })?;
    }
    let built = builder.finish().map_err(|error| {
        CoreError::Internal(format!(
            "failed to build index segment `{object_key}`: {error}"
        ))
    })?;
    store
        .put_immutable_verified(&object_key, bytes::Bytes::from(built.bytes.clone()))
        .await
        .map_err(grep_immutable_write_error)?;
    let filter_inline = (built.filter.stored_len <= INLINE_INDEX_FILTER_MAX_BYTES).then(|| {
        let start = built.filter.offset as usize;
        hex_encode_bytes(&built.bytes[start..start + built.filter.stored_len as usize])
    });
    Ok(GrepSegmentRef {
        segment_id,
        run_seq,
        run_ordinal,
        level,
        segment_index,
        min_row_key: built.min_key,
        max_row_key: built.max_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
        payload_checksum: sha256_digest(&built.bytes),
    })
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    /// Runs one partitioned delta-to-mid or mid-plus-base reorganize step.
    pub async fn reorganize_step(
        &self,
        namespace_id: &NamespaceId,
        policy: GramIndexBuildPolicy,
    ) -> Result<GrepReorganizeReport> {
        let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        else {
            return Ok(reorganize_report(
                namespace_id,
                GrepReorganizeOutcome::NotEnabled,
            ));
        };
        if matches!(
            current.manifest_state().lifecycle(),
            GrepLifecycle::Disabled
        ) {
            return Ok(reorganize_report(
                namespace_id,
                GrepReorganizeOutcome::NotEnabled,
            ));
        }
        let (reorganize, next_run_ordinal) =
            match current.manifest_state().index().reorganize.clone() {
                Some(reorganize) => (
                    reorganize,
                    current.manifest_state().index().next_run_ordinal,
                ),
                None => {
                    let l0_runs = distinct_run_ordinals_at_level(
                        current.manifest_state().segments(),
                        INDEX_GRAMS_DELTA_LEVEL,
                    );
                    let mid_runs = distinct_run_ordinals_at_level(
                        current.manifest_state().segments(),
                        INDEX_GRAMS_MID_LEVEL,
                    );
                    let (snapshot_segment_ids, output_level) =
                        if l0_runs >= policy.max_l0_runs.get() {
                            (
                                current
                                    .manifest_state()
                                    .segments()
                                    .iter()
                                    .filter(|segment| segment.level == INDEX_GRAMS_DELTA_LEVEL)
                                    .map(|segment| segment.segment_id.clone())
                                    .collect(),
                                INDEX_GRAMS_MID_LEVEL,
                            )
                        } else if mid_runs >= policy.max_mid_runs.get() {
                            (
                                current
                                    .manifest_state()
                                    .segments()
                                    .iter()
                                    .filter(|segment| segment.level != INDEX_GRAMS_DELTA_LEVEL)
                                    .map(|segment| segment.segment_id.clone())
                                    .collect(),
                                INDEX_GRAMS_BASE_LEVEL,
                            )
                        } else {
                            return Ok(reorganize_report(
                                namespace_id,
                                GrepReorganizeOutcome::NotNeeded { l0_runs, mid_runs },
                            ));
                        };
                    (
                        GrepReorganizeState {
                            snapshot_segment_ids,
                            output_segment_ids: Vec::new(),
                            row_key_cursor: String::new(),
                            output_level,
                            run_ordinal: current.manifest_state().index().next_run_ordinal,
                        },
                        current.manifest_state().index().next_run_ordinal + 1,
                    )
                }
            };
        let snapshot = reorganize
            .snapshot_segment_ids
            .iter()
            .map(|segment_id| {
                current
                    .manifest_state()
                    .segments()
                    .iter()
                    .find(|segment| &segment.segment_id == segment_id)
                    .ok_or_else(|| GrepError::CorruptIndex {
                        message: format!(
                            "grep reorganization snapshot segment `{segment_id}` is missing from the root"
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let merged = merge_snapshot_range(
            &self.store,
            &self.block_cache,
            namespace_id,
            &snapshot,
            &reorganize.row_key_cursor,
            policy.max_decoded_input_rows_per_step.get(),
        )
        .await?;
        let rows = gram_postings_rows(merged.postings)?;
        // Only an empty snapshot falls through, and an empty one merges no
        // rows and writes no segment for this to stamp. The lifecycle's
        // watermark is not a substitute: a backfilling root has none.
        let run_seq = snapshot
            .iter()
            .map(|segment| segment.run_seq)
            .max()
            .unwrap_or(ChangeSeq(0));
        let timer = StdMonotonicTimer::default();
        let publication_started_ms = timer.monotonic_now_ms();
        let new_segments = write_index_segments(
            &self.store,
            namespace_id,
            run_seq,
            reorganize.run_ordinal,
            rows,
            policy.max_rows_per_segment,
            reorganize.output_level,
        )
        .await?;
        let segments_written = new_segments.len() as u64;
        let mut segments = current.manifest_state().segments().to_vec();
        let mut next_reorganize = reorganize.clone();
        next_reorganize.output_segment_ids.extend(
            new_segments
                .iter()
                .map(|segment| segment.segment_id.clone()),
        );
        segments.extend(new_segments);
        let completed = merged.exhausted;
        let reorganize = if completed {
            let snapshot_ids: BTreeSet<&IndexSegmentId> =
                next_reorganize.snapshot_segment_ids.iter().collect();
            segments.retain(|segment| !snapshot_ids.contains(&segment.segment_id));
            None
        } else {
            next_reorganize.row_key_cursor = merged.next_cursor;
            Some(next_reorganize)
        };
        let next = GrepManifestState::new(
            namespace_id.clone(),
            current.manifest_state().lifecycle().clone(),
            GrepIndexState::new(reorganize, next_run_ordinal),
            segments,
        )
        .map_err(core_state_error)?;
        ensure_publication_budget(&timer, publication_started_ms)?;
        match self.advance_root(&current, &next).await {
            Ok(_) => Ok(reorganize_report(
                namespace_id,
                GrepReorganizeOutcome::StepPublished {
                    merged_rows: merged.rows,
                    segments_written,
                    completed,
                },
            )),
            Err(GrepRootError::Conflict { .. }) => Ok(reorganize_report(
                namespace_id,
                GrepReorganizeOutcome::Superseded,
            )),
            Err(error) => Err(error.into()),
        }
    }

    /// Collects one namespace's grep keyspace under a read budget. A live
    /// namespace retains its verified root and every segment it names; a
    /// deleted or absent namespace has its entire grep prefix reaped after
    /// the grace window.
    ///
    /// The pass enumerates the prefix as a stream and stops when
    /// `max_objects` reads are spent, answering the position it stopped at
    /// so the next call resumes there. Like core garbage collection, that
    /// cursor is an enumeration shortcut and nothing else: every resumed
    /// pass re-reads liveness and the grep root before it deletes anything,
    /// so losing the cursor costs a repeated walk and never a wrong delete.
    ///
    /// A pass always examines at least one key, whatever the budget. The
    /// budget bounds how much work one call does; it may not stop a caller
    /// looping the cursor from ever finishing.
    pub async fn garbage_collect_namespace(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
        request: &GrepGcRequest,
    ) -> Result<GrepGcReport> {
        let resume = match request.cursor.as_deref() {
            Some(token) => GrepGcCursor::decode(token, namespace_id)?,
            None => GrepGcCursor::initial(namespace_id),
        };
        let mut report = GrepGcReport::default();
        // An absent budget is the per-pass default, resolved here — the
        // entry point an operator calls — because that is where the runtime
        // resolves its own, and one authority is what keeps the two the
        // same number.
        let mut budget =
            PassBudget::new(Some(request.max_objects.unwrap_or(DEFAULT_GC_MAX_OBJECTS)));
        let reads = self.reads(namespace_id);
        // Liveness is decided once per pass — and re-decided per candidate
        // below, which is what authorizes a delete. This first read is
        // charged like any other.
        budget.charge();
        let liveness = namespace_liveness(&reads).await;
        if liveness == NamespaceLiveness::Unknown {
            report.namespace_degraded = true;
        }

        let prefix = namespace_prefix(namespace_id);
        let mut keys = self.store.list_prefix_stream(&prefix);
        let mut position = resume.clone();
        let mut deleted_any = false;
        let mut examined = 0_u64;
        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| core_store_error(&prefix, &error))?;
            if resume.covers(&key) {
                continue;
            }
            // The first key of a pass is examined whatever the budget: the
            // cursor must advance, or a caller looping it never finishes.
            if examined > 0 && budget.exhausted() {
                // Proof that work remains, at the cost of one listed key and
                // no reads. The key is reconsidered from the exclusive
                // last-examined position on resume.
                report.next_cursor = Some(position.encode()?);
                break;
            }
            budget.charge();
            let deleted = self
                .collect_one_key(
                    namespace_id,
                    &reads,
                    liveness,
                    &key,
                    now_ms,
                    &mut budget,
                    &mut report,
                )
                .await?;
            deleted_any |= deleted;
            examined += 1;
            position = GrepGcCursor::after(namespace_id, key);
        }
        if deleted_any && liveness == NamespaceLiveness::Gone {
            report.namespace_reaped = true;
        }
        Ok(report)
    }

    /// Decides one candidate key against freshly re-read state.
    ///
    /// Answers whether the key was deleted. Selection may be stale — the
    /// listing is a snapshot — but every decision here re-reads what
    /// authorizes it, which is what makes resuming from a cursor safe.
    #[allow(clippy::too_many_arguments)]
    async fn collect_one_key(
        &self,
        namespace_id: &NamespaceId,
        reads: &NamespaceReads<'_>,
        liveness: NamespaceLiveness,
        key: &str,
        now_ms: u64,
        budget: &mut PassBudget,
        report: &mut GrepGcReport,
    ) -> Result<bool> {
        match liveness {
            // A verified deleted namespace head is already the absorbing
            // gate for this pointer: `enable` refuses that tombstone, so no
            // legal writer can re-reference grep state after this liveness
            // check. Pointer deletion needs no second state.
            NamespaceLiveness::Gone => {
                budget.charge();
                if namespace_liveness(reads).await != NamespaceLiveness::Gone {
                    report.retained_candidates += 1;
                    return Ok(false);
                }
                budget.charge();
                Ok(self.delete_if_unreferenced(key, now_ms, report).await?)
            }
            // Grep segments and manifests are immutable and are written
            // under freshly minted ids, so an unreferenced one is the
            // leftover of a publication that already ended — no live
            // publication can be about to point at this key. The grace
            // window covers the one that has not ended yet, which is why
            // aged unreachable objects need no condemned state before
            // deletion.
            NamespaceLiveness::Live => {
                budget.charge();
                let root = match load_grep_root(&self.store, namespace_id).await {
                    Ok(root) => root,
                    Err(_) => {
                        report.namespace_degraded = true;
                        report.retained_candidates += 1;
                        return Ok(false);
                    }
                };
                let live = root
                    .as_ref()
                    .map(live_grep_keys)
                    .unwrap_or_else(|| BTreeSet::from([root_key(namespace_id)]));
                if live.contains(key) {
                    return Ok(false);
                }
                budget.charge();
                Ok(self.delete_if_unreferenced(key, now_ms, report).await?)
            }
            NamespaceLiveness::Unknown => {
                report.retained_candidates += 1;
                Ok(false)
            }
        }
    }

    async fn delete_if_unreferenced(
        &self,
        key: &str,
        now_ms: u64,
        report: &mut GrepGcReport,
    ) -> Result<bool> {
        let outcome = delete_if_aged(&self.store, key, GREP_GC_GRACE_WINDOW_MS, now_ms)
            .await
            .map_err(|error| core_store_error(key, &error))?;
        if outcome.retained_reason().is_some() {
            report.retained_candidates += 1;
        }
        if outcome.deleted() {
            count_deleted_key(key, report);
            return Ok(true);
        }
        Ok(false)
    }
}

/// Opaque, namespace-bound resume position for a bounded grep collection.
///
/// It carries where the enumeration stopped and nothing else. Grep's whole
/// keyspace is one prefix, so one key is the whole position — and because
/// every resumed pass re-reads liveness and the root, a stale or forged
/// cursor can only re-examine work or defer it to a later pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GrepGcCursor {
    namespace_id: NamespaceId,
    #[serde(default)]
    last_key: Option<String>,
}

impl PageCursor for GrepGcCursor {
    const KIND: &'static str = "grep_gc";
}

impl NamespaceCursor for GrepGcCursor {
    fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }

    fn key_prefix(&self) -> String {
        namespace_prefix(&self.namespace_id)
    }
}

impl GrepGcCursor {
    fn initial(namespace_id: &NamespaceId) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            last_key: None,
        }
    }

    fn after(namespace_id: &NamespaceId, key: String) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            last_key: Some(key),
        }
    }

    /// Whether an earlier pass already examined this key.
    fn covers(&self, key: &str) -> bool {
        self.last_key
            .as_deref()
            .is_some_and(|last_key| key <= last_key)
    }

    fn decode(token: &str, namespace_id: &NamespaceId) -> Result<Self> {
        decode_namespace_cursor(token, namespace_id).map_err(|error| match error {
            NamespaceCursorError::ForeignNamespace => {
                CoreError::InvalidGcConfig(error.to_string()).into()
            }
            NamespaceCursorError::Malformed(_) | NamespaceCursorError::OutsideKeyspace => {
                malformed_gc_cursor()
            }
        })
    }

    fn encode(&self) -> Result<String> {
        encode_cursor(self).map_err(|error| {
            CoreError::Internal(format!("failed to encode grep GC cursor: {error}")).into()
        })
    }
}

fn malformed_gc_cursor() -> GrepError {
    CoreError::InvalidGcConfig("cursor is malformed".to_owned()).into()
}

fn reorganize_report(
    namespace_id: &NamespaceId,
    outcome: GrepReorganizeOutcome,
) -> GrepReorganizeReport {
    GrepReorganizeReport {
        namespace_id: namespace_id.clone(),
        outcome,
    }
}

fn distinct_run_ordinals_at_level(segments: &[GrepSegmentRef], level: u32) -> usize {
    segments
        .iter()
        .filter(|segment| segment.level == level)
        .map(|segment| segment.run_ordinal)
        .collect::<BTreeSet<_>>()
        .len()
}

struct MergedRange {
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    next_cursor: String,
    exhausted: bool,
    rows: u64,
}

async fn merge_snapshot_range<S: ObjectStore + ?Sized>(
    store: &S,
    block_cache: &GrepBlockCache,
    namespace_id: &NamespaceId,
    snapshot: &[&GrepSegmentRef],
    cursor: &str,
    max_rows: usize,
) -> Result<MergedRange> {
    let mut readers = Vec::with_capacity(snapshot.len());
    for chunk in snapshot.chunks(MAX_GREP_WORKER_IO) {
        readers.extend(
            try_join_all(chunk.iter().map(|segment| {
                SegmentRangeReader::open(store, block_cache, namespace_id, segment, cursor)
            }))
            .await?,
        );
    }
    let mut merged = MergedRange {
        postings: BTreeMap::new(),
        next_cursor: String::new(),
        exhausted: false,
        rows: 0,
    };
    let mut last_key = String::new();
    while merged.rows < max_rows as u64 {
        for reader in &mut readers {
            reader.refill(store, block_cache).await?;
        }
        let mut lowest: Option<(usize, String)> = None;
        for (position, reader) in readers.iter().enumerate() {
            if let Some(key) = reader.peek_key() {
                if lowest
                    .as_ref()
                    .is_none_or(|(_, current)| key < current.as_str())
                {
                    lowest = Some((position, key.to_owned()));
                }
            }
        }
        let Some((position, _)) = lowest else {
            merged.exhausted = true;
            return Ok(merged);
        };
        let (key, row) = readers[position].pop();
        reorganize_snapshot_row(&mut merged, row, readers[position].object_key())?;
        for reader in &mut readers {
            loop {
                reader.refill(store, block_cache).await?;
                if reader.peek_key() != Some(key.as_str()) {
                    break;
                }
                let (_, duplicate) = reader.pop();
                reorganize_snapshot_row(&mut merged, duplicate, reader.object_key())?;
            }
        }
        last_key = key;
    }
    let mut any_left = false;
    for reader in &mut readers {
        reader.refill(store, block_cache).await?;
        if reader.peek_key().is_some() {
            any_left = true;
            break;
        }
    }
    if any_left {
        merged.next_cursor = format!("{last_key}\0");
    } else {
        merged.exhausted = true;
    }
    Ok(merged)
}

fn reorganize_snapshot_row(
    merged: &mut MergedRange,
    row: IndexRow,
    object_key: &str,
) -> Result<()> {
    let IndexRow::GramPostings { gram, .. } = &row;
    let gram = *gram;
    let batch = row.postings().map_err(|error| GrepError::CorruptIndex {
        message: format!(
            "index segment `{object_key}` carries an unreadable posting batch: {error}"
        ),
    })?;
    merged.postings.entry(gram).or_default().extend(batch);
    merged.rows += 1;
    Ok(())
}

struct SegmentRangeReader {
    object_key: String,
    payload_checksum: String,
    entries: Arc<Vec<SegmentIndexEntry>>,
    next_entry: usize,
    current: Option<CurrentDataBlock>,
    start: String,
}

struct CurrentDataBlock {
    block: Arc<DecodedDataBlock<IndexRow>>,
    next_row: usize,
}

impl SegmentRangeReader {
    async fn open<S: ObjectStore + ?Sized>(
        store: &S,
        block_cache: &GrepBlockCache,
        namespace_id: &NamespaceId,
        segment: &GrepSegmentRef,
        cursor: &str,
    ) -> Result<Self> {
        let object_key = segment_key(namespace_id, &segment.segment_id);
        let entries = load_index_block(
            store,
            block_cache,
            &object_key,
            &segment.payload_checksum,
            &segment.index_block,
        )
        .await?;
        let start = if cursor.is_empty() {
            GRAM_ROW_PREFIX
        } else {
            cursor
        };
        let range = index_blocks_for_key_range(&entries, start, None);
        Ok(Self {
            object_key,
            payload_checksum: segment.payload_checksum.clone(),
            next_entry: range.start,
            entries,
            current: None,
            start: start.to_owned(),
        })
    }

    fn object_key(&self) -> &str {
        &self.object_key
    }

    fn peek_key(&self) -> Option<&str> {
        self.current
            .as_ref()
            .map(|current| current.block.row_keys[current.next_row].as_str())
    }

    fn pop(&mut self) -> (String, IndexRow) {
        let current = self.current.as_mut().expect("peek_key should precede pop");
        let key = current.block.row_keys[current.next_row].clone();
        let row = current.block.rows[current.next_row].clone();
        current.next_row += 1;
        if current.next_row == current.block.row_keys.len() {
            self.current = None;
        }
        (key, row)
    }

    async fn refill<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        block_cache: &GrepBlockCache,
    ) -> Result<()> {
        while self.current.is_none() && self.next_entry < self.entries.len() {
            let entry = &self.entries[self.next_entry];
            self.next_entry += 1;
            let block = load_data_block(
                store,
                block_cache,
                &self.object_key,
                &self.payload_checksum,
                &entry.block,
            )
            .await?;
            let next_row = block
                .row_keys
                .partition_point(|key| key.as_str() < self.start.as_str());
            if next_row < block.row_keys.len() {
                self.current = Some(CurrentDataBlock { block, next_row });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamespaceLiveness {
    Live,
    Gone,
    Unknown,
}

/// Whether the namespace grep holds state for still exists.
///
/// The feed is the oracle: it refuses a deleted namespace with
/// `namespace_deleted` and an absent one with `namespace_not_found`, so
/// asking it for changes after the last possible sequence answers liveness
/// with one head read and no history. Anything else is an unknown answer,
/// and unknown never authorizes a delete.
async fn namespace_liveness(reads: &NamespaceReads<'_>) -> NamespaceLiveness {
    match live_namespace_probe(reads).await {
        Ok(()) => NamespaceLiveness::Live,
        Err(error) => match error.code() {
            ErrorCode::NamespaceDeleted | ErrorCode::NamespaceNotFound => NamespaceLiveness::Gone,
            _ => NamespaceLiveness::Unknown,
        },
    }
}

async fn ensure_live_namespace(reads: &NamespaceReads<'_>) -> Result<()> {
    live_namespace_probe(reads).await
}

async fn live_namespace_probe(reads: &NamespaceReads<'_>) -> Result<()> {
    reads.head_seq().await?;
    Ok(())
}

fn live_grep_keys(root: &LoadedGrepRoot) -> BTreeSet<String> {
    let namespace_id = root.manifest_state().namespace_id();
    let mut live = BTreeSet::from([
        root_key(namespace_id),
        manifest_key(namespace_id, root.manifest_id()),
    ]);
    live.extend(
        root.manifest_state()
            .segments()
            .iter()
            .map(|segment| segment_key(namespace_id, &segment.segment_id)),
    );
    if let Some(reorganize) = &root.manifest_state().index().reorganize {
        live.extend(
            reorganize
                .snapshot_segment_ids
                .iter()
                .chain(&reorganize.output_segment_ids)
                .map(|segment_id| segment_key(namespace_id, segment_id)),
        );
    }
    live
}

fn count_deleted_key(key: &str, report: &mut GrepGcReport) {
    if parse_key(key).is_some_and(|parsed| matches!(parsed.kind, GrepKeyKind::Segment { .. })) {
        report.deleted_segments += 1;
    } else {
        report.deleted_other_objects += 1;
    }
}

fn ensure_publication_budget(timer: &impl MonotonicTimer, started_ms: u64) -> Result<()> {
    let elapsed_ms = timer.monotonic_now_ms().saturating_sub(started_ms);
    if elapsed_ms <= METADATA_PUBLICATION_BUDGET_MS {
        return Ok(());
    }
    Err(CoreError::MetadataPublicationBudgetExceeded {
        elapsed_ms,
        budget_ms: METADATA_PUBLICATION_BUDGET_MS,
    }
    .into())
}

fn core_state_error(error: crate::root::GrepManifestStateError) -> GrepError {
    CoreError::Internal(format!("failed to build grep root state: {error}")).into()
}

fn core_store_error(object_key: &str, error: &ObjectStoreError) -> GrepError {
    GrepError::StoreUnavailable {
        object_key: object_key.to_owned(),
        message: error.message(),
        class: StoreFailureClass::of(error),
    }
}

fn grep_immutable_write_error(error: ImmutableWriteError) -> GrepError {
    let object_key = error.object_key().to_owned();
    match error {
        ImmutableWriteError::DifferentObject { object_key } => GrepError::CorruptIndex {
            message: format!("immutable object `{object_key}` contains different bytes"),
        },
        ImmutableWriteError::Transport { object_key, source } => {
            core_store_error(&object_key, &source)
        }
        error => GrepError::CorruptIndex {
            message: format!("index segment `{object_key}`: {error}"),
        },
    }
}
