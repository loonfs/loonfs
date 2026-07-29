//! Explicit grep building, checkpointed backfill, reorganization, and
//! garbage collection.
//!
//! Reorganization is the same idea as `loonfs-core`'s metadata
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
use crate::root::{
    advance_grep_root, load_grep_root, seed_grep_root, ChangeFeedResume, GrepIndexState,
    GrepLifecycle, GrepReorganizeState, GrepRootError, GrepRootState, GrepSegmentRef,
    LoadedGrepRoot,
};
use crate::service::is_indexable_text_content;
use crate::{GrepError, Result};
use futures::future::try_join_all;
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::wire::sst_blocks::{
    index_blocks_for_key_range, DecodedDataBlock, SegmentBlocksBuilder, SegmentIndexEntry,
};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    sha256_digest, ChangeSeq, CheckpointId, ContentRef, ContentStoreId, IndexSegmentId, InodeId,
    NamespaceId, RevisionNo,
};
use loonfs_core::content::read_durable_content_bytes;
use loonfs_core::control::{
    load_namespace_catalog_entry, load_namespace_head_control, ControlObjectLoadError,
};
use loonfs_core::grep::{
    load_grep_change_feed, load_grep_checkpoint_revision_page, GrepChangeFeed,
};
use loonfs_core::limits::METADATA_PUBLICATION_BUDGET_MS;
use loonfs_core::{
    Error as CoreError, MetadataProjectionLoadError, MonotonicTimer, NamespaceEngine,
    StdMonotonicTimer, StoreFailureClass,
};
use loonfs_objectstore::{ImmutableWriteError, ObjectStore, ObjectStoreError};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// User-checkpoint lifetime used by one backfill attempt. An expired attempt
/// is safely replaced by a fresh checkpoint and a cursor reset.
pub const GREP_BACKFILL_CHECKPOINT_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Unreferenced grep objects receive one hour of unconditional protection
/// before grep-owned garbage collection may delete them.
pub const GREP_GC_GRACE_WINDOW_MS: u64 = 60 * 60 * 1000;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepEnableOutcome {
    Enabled { target_seq: ChangeSeq },
    AlreadyEnabled { built_through_seq: ChangeSeq },
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

/// Counts from one namespace's grep garbage-collection pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrepGcReport {
    pub deleted_segments: u64,
    pub deleted_other_objects: u64,
    pub namespace_reaped: bool,
    pub retained_candidates: u64,
    pub namespace_degraded: bool,
}

/// Namespace-independent writer for the grep-owned durable keyspace.
///
/// Calls are explicit and bounded. Per-namespace drivers decide when to call
/// them without changing the durable protocol.
#[derive(Debug, Clone)]
pub struct GrepWorker<S> {
    store: S,
    writer_id: String,
    writer_session_id: String,
    writer_version: String,
    block_cache: Arc<GrepBlockCache>,
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    /// Creates a worker over one object-store handle and writer identity.
    pub fn new(
        store: S,
        writer_id: impl Into<String>,
        writer_session_id: impl Into<String>,
        writer_version: impl Into<String>,
    ) -> Self {
        Self {
            store,
            writer_id: writer_id.into(),
            writer_session_id: writer_session_id.into(),
            writer_version: writer_version.into(),
            block_cache: Arc::new(GrepBlockCache::new(MAX_CACHED_GREP_BLOCKS)),
        }
    }

    /// Enables grep by pinning a checkpoint and CAS-publishing a fresh
    /// backfilling root. Enabling an active root is idempotent.
    pub async fn enable(&self, namespace_id: &NamespaceId) -> Result<GrepEnableOutcome> {
        if let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        {
            if !matches!(current.state().lifecycle(), GrepLifecycle::Disabled) {
                return Ok(GrepEnableOutcome::AlreadyEnabled {
                    built_through_seq: current.state().index().built_through_seq,
                });
            }
        }

        let checkpoint = self.create_backfill_checkpoint(namespace_id).await?;
        let current = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?;
        if let Some(current) = &current {
            if !matches!(current.state().lifecycle(), GrepLifecycle::Disabled) {
                self.release_checkpoint_if_unreferenced(
                    namespace_id,
                    &checkpoint.checkpoint_id,
                    current.state(),
                )
                .await?;
                return Ok(GrepEnableOutcome::AlreadyEnabled {
                    built_through_seq: current.state().index().built_through_seq,
                });
            }
        }
        let next_run_ordinal = current
            .as_ref()
            .map_or(0, |root| root.state().index().next_run_ordinal);
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
            Ok(_) => Ok(GrepEnableOutcome::Enabled {
                target_seq: checkpoint.checkpoint_seq,
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
        ensure_live_namespace(&self.store, namespace_id).await?;
        let Some(current) = load_grep_root(&self.store, namespace_id)
            .await
            .map_err(GrepError::from)?
        else {
            return Ok(GrepDisableOutcome::NotEnabled);
        };
        if matches!(current.state().lifecycle(), GrepLifecycle::Disabled) {
            return Ok(GrepDisableOutcome::NotEnabled);
        }
        let checkpoint_id = match current.state().lifecycle() {
            GrepLifecycle::Backfilling { checkpoint_id, .. } => Some(checkpoint_id.clone()),
            GrepLifecycle::Steady | GrepLifecycle::Disabled => None,
        };
        let next = GrepRootState::new(
            namespace_id.clone(),
            GrepLifecycle::Disabled,
            GrepIndexState::new(
                current.state().index().built_through_seq,
                0,
                None,
                current.state().index().next_run_ordinal,
            ),
            Vec::new(),
        )
        .map_err(core_state_error)?;
        match self.advance_root(&current, &next).await {
            Ok(_) => {
                if let Some(checkpoint_id) = checkpoint_id {
                    self.engine(namespace_id)?
                        .release_checkpoint(&checkpoint_id)
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
        if matches!(current.state().lifecycle(), GrepLifecycle::Disabled) {
            return Ok(build_report(namespace_id, GrepBuildOutcome::NotEnabled));
        }
        let content_store_id = load_namespace_catalog_entry(&self.store, namespace_id)
            .await
            .map_err(CoreError::from)?
            .content_store_id()
            .clone();

        let unit = match current.state().lifecycle() {
            GrepLifecycle::Backfilling {
                backfill_cursor,
                checkpoint_id,
            } => {
                let page = load_grep_checkpoint_revision_page(
                    &self.store,
                    namespace_id,
                    checkpoint_id,
                    current_time_ms()?,
                    backfill_cursor,
                    policy.max_files_per_step,
                )
                .await?;
                let Some(page) = page else {
                    return self.restart_backfill(namespace_id, &current).await;
                };
                if page.checkpoint_seq != current.state().index().built_through_seq {
                    return self.restart_backfill(namespace_id, &current).await;
                }
                collect_backfill_unit(
                    &self.store,
                    &content_store_id,
                    current.state().index().built_through_seq,
                    backfill_cursor,
                    page,
                    policy,
                )
                .await?
            }
            GrepLifecycle::Steady => {
                match collect_incremental_unit(
                    &self.store,
                    namespace_id,
                    &content_store_id,
                    current.state().index().built_through_seq,
                    current.state().index().next_delta_index,
                    policy,
                )
                .await?
                {
                    IncrementalCollection::Unit(unit) => unit,
                    IncrementalCollection::UpToDate => {
                        return Ok(build_report(
                            namespace_id,
                            GrepBuildOutcome::UpToDate {
                                built_through_seq: current.state().index().built_through_seq,
                            },
                        ));
                    }
                    IncrementalCollection::RebootstrapRequired => {
                        return self.restart_backfill(namespace_id, &current).await;
                    }
                }
            }
            GrepLifecycle::Disabled => unreachable!("disabled returned above"),
        };

        self.publish_build_unit(namespace_id, current, unit, policy)
            .await
    }

    fn engine(&self, namespace_id: &NamespaceId) -> Result<NamespaceEngine<S>> {
        Ok(NamespaceEngine::builder(self.store.clone())
            .namespace_id(namespace_id.clone())
            .writer_id(self.writer_id.clone())
            .writer_session_id(self.writer_session_id.clone())
            .writer_version(self.writer_version.clone())
            .build()
            .map_err(|error| {
                CoreError::Internal(format!("failed to build grep worker engine: {error}"))
            })?)
    }

    async fn seed_root(
        &self,
        state: &GrepRootState,
    ) -> std::result::Result<LoadedGrepRoot, GrepRootError> {
        seed_grep_root(&self.store, state, &self.writer_version).await
    }

    async fn advance_root(
        &self,
        current: &LoadedGrepRoot,
        next: &GrepRootState,
    ) -> std::result::Result<LoadedGrepRoot, GrepRootError> {
        advance_grep_root(&self.store, current, next, &self.writer_version).await
    }

    async fn create_backfill_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<loonfs_api::CreateCheckpointResponse> {
        Ok(self
            .engine(namespace_id)?
            .create_checkpoint(
                GREP_BACKFILL_CHECKPOINT_NAME.to_owned(),
                Some(GREP_BACKFILL_CHECKPOINT_TTL_MS),
            )
            .await?)
    }

    async fn release_checkpoint_if_unreferenced(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
        root: &GrepRootState,
    ) -> Result<()> {
        if !root_names_checkpoint(root, checkpoint_id) {
            self.engine(namespace_id)?
                .release_checkpoint(checkpoint_id)
                .await?;
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
            self.release_checkpoint_if_unreferenced(namespace_id, checkpoint_id, winner.state())
                .await
        } else {
            self.engine(namespace_id)?
                .release_checkpoint(checkpoint_id)
                .await?;
            Ok(())
        }
    }

    async fn restart_backfill(
        &self,
        namespace_id: &NamespaceId,
        current: &LoadedGrepRoot,
    ) -> Result<GrepBuildReport> {
        let previous_checkpoint_id = match current.state().lifecycle() {
            GrepLifecycle::Backfilling { checkpoint_id, .. } => Some(checkpoint_id.clone()),
            GrepLifecycle::Steady | GrepLifecycle::Disabled => None,
        };
        let checkpoint = self.create_backfill_checkpoint(namespace_id).await?;
        let next = backfilling_root(
            namespace_id,
            checkpoint.checkpoint_seq,
            checkpoint.checkpoint_id.clone(),
            current.state().index().next_run_ordinal,
        )?;
        match self.advance_root(current, &next).await {
            Ok(_) => {
                if let Some(previous_checkpoint_id) = previous_checkpoint_id {
                    if previous_checkpoint_id != checkpoint.checkpoint_id {
                        self.engine(namespace_id)?
                            .release_checkpoint(&previous_checkpoint_id)
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
            current.state().index().next_run_ordinal,
            rows,
            policy.max_rows_per_segment,
            INDEX_GRAMS_DELTA_LEVEL,
        )
        .await?;
        let segments_written = new_segments.len() as u64;
        let mut segments = current.state().segments().to_vec();
        segments.extend(new_segments);
        let next_run_ordinal =
            current.state().index().next_run_ordinal + u64::from(segments_written > 0);
        let (lifecycle, completed_checkpoint_id) = match current.state().lifecycle() {
            GrepLifecycle::Backfilling { checkpoint_id, .. } => match unit.backfill_cursor {
                Some(backfill_cursor) => (
                    GrepLifecycle::Backfilling {
                        backfill_cursor,
                        checkpoint_id: checkpoint_id.clone(),
                    },
                    None,
                ),
                None => (GrepLifecycle::Steady, Some(checkpoint_id.clone())),
            },
            GrepLifecycle::Steady => (GrepLifecycle::Steady, None),
            GrepLifecycle::Disabled => unreachable!("disabled returned before collection"),
        };
        let materialized = matches!(lifecycle, GrepLifecycle::Steady);
        let next = GrepRootState::new(
            namespace_id.clone(),
            lifecycle,
            GrepIndexState::new(
                unit.built_through_seq,
                unit.next_delta_index,
                current.state().index().reorganize.clone(),
                next_run_ordinal,
            ),
            segments,
        )
        .map_err(core_state_error)?;
        ensure_publication_budget(&timer, publication_started_ms)?;
        match self.advance_root(&current, &next).await {
            Ok(_) => {
                if let Some(checkpoint_id) = completed_checkpoint_id {
                    self.engine(namespace_id)?
                        .release_checkpoint(&checkpoint_id)
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
) -> Result<GrepRootState> {
    GrepRootState::new(
        namespace_id.clone(),
        GrepLifecycle::Backfilling {
            backfill_cursor: String::new(),
            checkpoint_id,
        },
        GrepIndexState::new(target_seq, 0, None, next_run_ordinal),
        Vec::new(),
    )
    .map_err(core_state_error)
}

fn root_names_checkpoint(root: &GrepRootState, checkpoint_id: &CheckpointId) -> bool {
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
    next_delta_index: u32,
    backfill_cursor: Option<String>,
}

enum IncrementalCollection {
    Unit(CollectedIndexUnit),
    UpToDate,
    RebootstrapRequired,
}

async fn collect_backfill_unit<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    target_seq: ChangeSeq,
    cursor: &str,
    page: loonfs_core::grep::GrepCheckpointRevisionPage,
    policy: GramIndexBuildPolicy,
) -> Result<CollectedIndexUnit> {
    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: target_seq,
        built_through_seq: target_seq,
        next_delta_index: 0,
        backfill_cursor: Some(cursor.to_owned()),
    };
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut budget_reached = false;
    for (row_key, revision) in page.revisions {
        if revision.committed_seq <= target_seq {
            if revision.content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
                unit.skipped_revisions += 1;
            } else {
                planned_content_bytes += revision.content_ref.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    content_ref: revision.content_ref,
                });
            }
        }
        unit.backfill_cursor = Some(row_key);
        if planned_content_bytes >= policy.max_content_bytes_per_step.get() {
            budget_reached = true;
            break;
        }
    }
    if page.exhausted && !budget_reached {
        unit.backfill_cursor = None;
    }
    load_and_fold_revision_contents(store, content_store_id, &pending, &mut unit).await?;
    Ok(unit)
}

async fn collect_incremental_unit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    built_through_seq: ChangeSeq,
    next_delta_index: u32,
    policy: GramIndexBuildPolicy,
) -> Result<IncrementalCollection> {
    let resume = ChangeFeedResume::new(built_through_seq, next_delta_index);
    let feed = load_grep_change_feed(store, namespace_id, resume.after_seq()).await?;
    let GrepChangeFeed::Records { records, .. } = feed else {
        return Ok(IncrementalCollection::RebootstrapRequired);
    };
    if records.is_empty() {
        return Ok(IncrementalCollection::UpToDate);
    }
    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: built_through_seq,
        built_through_seq,
        next_delta_index,
        backfill_cursor: None,
    };
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut examined_files = 0usize;
    'records: for record in records {
        let start_delta_index = resume.start_delta_index(record.seq).map_err(|_| {
            CoreError::Internal("grep delta cursor does not fit in memory".to_owned())
        })?;
        if start_delta_index > record.deltas.len() {
            return Ok(IncrementalCollection::RebootstrapRequired);
        }
        for (delta_index, delta) in record.deltas.iter().enumerate().skip(start_delta_index) {
            if let WalDelta::AppendFileRevision {
                inode_id,
                revision_no,
                content_ref,
                ..
            } = &delta.delta
            {
                let would_exceed_content_budget = planned_content_bytes > 0
                    && planned_content_bytes.saturating_add(content_ref.size_bytes)
                        > policy.max_content_bytes_per_step.get();
                if examined_files >= policy.max_files_per_step.get() || would_exceed_content_budget
                {
                    if delta_index > 0 {
                        unit.built_through_seq = record.seq;
                        unit.run_seq = record.seq;
                        unit.next_delta_index = u32::try_from(delta_index).map_err(|_| {
                            CoreError::Internal("grep delta cursor overflow".to_owned())
                        })?;
                    }
                    break 'records;
                }
                examined_files += 1;
                if content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
                    unit.skipped_revisions += 1;
                } else {
                    planned_content_bytes += content_ref.size_bytes;
                    pending.push(PendingRevisionContent {
                        inode_id: *inode_id,
                        revision_no: *revision_no,
                        content_ref: content_ref.clone(),
                    });
                }
                if examined_files >= policy.max_files_per_step.get()
                    || planned_content_bytes >= policy.max_content_bytes_per_step.get()
                {
                    unit.built_through_seq = record.seq;
                    unit.run_seq = record.seq;
                    let next_delta_index = delta_index.checked_add(1).ok_or_else(|| {
                        CoreError::Internal("grep delta cursor overflow".to_owned())
                    })?;
                    if next_delta_index < record.deltas.len() {
                        unit.next_delta_index = u32::try_from(next_delta_index).map_err(|_| {
                            CoreError::Internal("grep delta cursor overflow".to_owned())
                        })?;
                    } else {
                        unit.next_delta_index = 0;
                    }
                    break 'records;
                }
            }
        }
        unit.built_through_seq = record.seq;
        unit.run_seq = record.seq;
        unit.next_delta_index = 0;
    }
    if unit.built_through_seq == built_through_seq && unit.next_delta_index == next_delta_index {
        return Ok(IncrementalCollection::UpToDate);
    }
    load_and_fold_revision_contents(store, content_store_id, &pending, &mut unit).await?;
    Ok(IncrementalCollection::Unit(unit))
}

struct PendingRevisionContent {
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
}

async fn load_and_fold_revision_contents<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    pending: &[PendingRevisionContent],
    unit: &mut CollectedIndexUnit,
) -> Result<()> {
    for chunk in pending.chunks(MAX_GREP_WORKER_IO) {
        let contents = try_join_all(chunk.iter().map(|revision| {
            read_durable_content_bytes(store, content_store_id, &revision.content_ref)
        }))
        .await
        .map_err(CoreError::from)?;
        for (revision, content) in chunk.iter().zip(contents) {
            if !is_indexable_text_content(&content.bytes) {
                unit.skipped_revisions += 1;
                continue;
            }
            let posting = GramPosting {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
            };
            for gram in extract_grams(&content.bytes) {
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
        if matches!(current.state().lifecycle(), GrepLifecycle::Disabled) {
            return Ok(reorganize_report(
                namespace_id,
                GrepReorganizeOutcome::NotEnabled,
            ));
        }
        let (reorganize, next_run_ordinal) = match current.state().index().reorganize.clone() {
            Some(reorganize) => (reorganize, current.state().index().next_run_ordinal),
            None => {
                let l0_runs = distinct_run_ordinals_at_level(
                    current.state().segments(),
                    INDEX_GRAMS_DELTA_LEVEL,
                );
                let mid_runs = distinct_run_ordinals_at_level(
                    current.state().segments(),
                    INDEX_GRAMS_MID_LEVEL,
                );
                let (snapshot_segment_ids, output_level) = if l0_runs >= policy.max_l0_runs.get() {
                    (
                        current
                            .state()
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
                            .state()
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
                        run_ordinal: current.state().index().next_run_ordinal,
                    },
                    current.state().index().next_run_ordinal + 1,
                )
            }
        };
        let snapshot = reorganize
            .snapshot_segment_ids
            .iter()
            .map(|segment_id| {
                current
                    .state()
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
        let run_seq = snapshot
            .iter()
            .map(|segment| segment.run_seq)
            .max()
            .unwrap_or(current.state().index().built_through_seq);
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
        let mut segments = current.state().segments().to_vec();
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
        let next = GrepRootState::new(
            namespace_id.clone(),
            current.state().lifecycle().clone(),
            GrepIndexState::new(
                current.state().index().built_through_seq,
                current.state().index().next_delta_index,
                reorganize,
                next_run_ordinal,
            ),
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

    /// Collects one namespace's grep keyspace. A live namespace retains its
    /// verified root and every segment it names; a deleted or absent namespace
    /// has its entire grep prefix reaped after the grace window.
    pub async fn garbage_collect_namespace(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
    ) -> Result<GrepGcReport> {
        let prefix = namespace_prefix(namespace_id);
        let keys = self
            .store
            .list_prefix(&prefix)
            .await
            .map_err(|error| core_store_error(&prefix, &error))?;
        let mut report = GrepGcReport::default();
        self.collect_namespace_garbage(namespace_id, &keys, now_ms, &mut report)
            .await?;
        Ok(report)
    }

    async fn collect_namespace_garbage(
        &self,
        namespace_id: &NamespaceId,
        keys: &[String],
        now_ms: u64,
        report: &mut GrepGcReport,
    ) -> Result<()> {
        match namespace_liveness(&self.store, namespace_id).await {
            NamespaceLiveness::Gone => {
                // A verified deleted namespace head is already the absorbing
                // gate for this pointer: `enable` refuses that tombstone, so
                // no legal writer can re-reference grep state after this
                // liveness check. Pointer deletion needs no second state.
                let mut deleted_any = false;
                for key in keys {
                    if namespace_liveness(&self.store, namespace_id).await
                        != NamespaceLiveness::Gone
                    {
                        report.retained_candidates += 1;
                        continue;
                    }
                    if delete_if_aged(&self.store, key, now_ms, report).await? {
                        count_deleted_key(key, report);
                        deleted_any = true;
                    }
                }
                if deleted_any {
                    report.namespace_reaped = true;
                }
            }
            NamespaceLiveness::Live => {
                let root = match load_grep_root(&self.store, namespace_id).await {
                    Ok(root) => root,
                    Err(_) => {
                        report.namespace_degraded = true;
                        report.retained_candidates += keys.len() as u64;
                        return Ok(());
                    }
                };
                let live = root
                    .as_ref()
                    .map(live_grep_keys)
                    .unwrap_or_else(|| BTreeSet::from([root_key(namespace_id)]));
                // Grep segments and manifests are immutable. An identical
                // rebuild can only recreate the same derived bytes at the
                // same key; pointer advance verifies and heals its manifest
                // after CAS, so aged unreachable objects need no condemned
                // state before deletion.
                for key in keys.iter().filter(|key| !live.contains(*key)) {
                    let fresh = match load_grep_root(&self.store, namespace_id).await {
                        Ok(root) => root,
                        Err(_) => {
                            report.namespace_degraded = true;
                            report.retained_candidates += 1;
                            continue;
                        }
                    };
                    if fresh
                        .as_ref()
                        .is_some_and(|root| live_grep_keys(root).contains(key))
                    {
                        report.retained_candidates += 1;
                        continue;
                    }
                    if delete_if_aged(&self.store, key, now_ms, report).await? {
                        count_deleted_key(key, report);
                    }
                }
            }
            NamespaceLiveness::Unknown => {
                report.namespace_degraded = true;
                report.retained_candidates += keys.len() as u64;
            }
        }
        Ok(())
    }
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

async fn namespace_liveness<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NamespaceLiveness {
    match load_namespace_head_control(store, namespace_id).await {
        Ok(head) if head.state.state == NamespaceState::Deleted => NamespaceLiveness::Gone,
        Ok(_) => NamespaceLiveness::Live,
        Err(ControlObjectLoadError::MissingObject { .. }) => NamespaceLiveness::Gone,
        Err(_) => NamespaceLiveness::Unknown,
    }
}

async fn ensure_live_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if head.state.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        }
        .into());
    }
    Ok(())
}

fn live_grep_keys(root: &LoadedGrepRoot) -> BTreeSet<String> {
    let namespace_id = root.state().namespace_id();
    let mut live = BTreeSet::from([
        root_key(namespace_id),
        manifest_key(namespace_id, root.manifest_envelope().manifest_id()),
    ]);
    live.extend(
        root.state()
            .segments()
            .iter()
            .map(|segment| segment_key(namespace_id, &segment.segment_id)),
    );
    if let Some(reorganize) = &root.state().index().reorganize {
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

async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    now_ms: u64,
    report: &mut GrepGcReport,
) -> Result<bool> {
    let Some(metadata) = store
        .head(key)
        .await
        .map_err(|error| core_store_error(key, &error))?
    else {
        return Ok(false);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        report.retained_candidates += 1;
        return Ok(false);
    };
    if now_ms.saturating_sub(last_modified_ms) < GREP_GC_GRACE_WINDOW_MS {
        report.retained_candidates += 1;
        return Ok(false);
    }
    store
        .delete(key)
        .await
        .map_err(|error| core_store_error(key, &error))?;
    Ok(true)
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

fn core_state_error(error: crate::root::GrepRootStateError) -> GrepError {
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

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> Result<u64> {
    // Worker checkpoint expiry is resolved at this API boundary; durable replay remains deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|error| {
            CoreError::Internal(format!("system clock before unix epoch: {error}")).into()
        })
}
