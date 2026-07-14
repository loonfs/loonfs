//! Gram index building: budgeted maintenance steps that read eligible file
//! content, extract grams, write index segments, and publish manifests
//! advancing the `index.grams` watermark (format spec, sections 4.2.2 and 5).
//!
//! Like reorganization, every step reads the live manifest, does a bounded
//! amount of work, and ends in one manifest publication through the normal
//! root compare-and-swap — the manifest is the only progress record. Two
//! modes share that discipline:
//!
//! - **Backfill** (the feature value carries `backfill_cursor`): walk the
//!   revisions family in row-key order from the cursor, indexing revisions
//!   committed at or below the enable-time watermark. The cursor is the
//!   resume point; queries stay refused until the walk completes.
//! - **Steady state**: replay the WAL change feed after `built_through_seq`
//!   and index the file revisions that appeared, advancing the watermark to
//!   the last fully consumed commit.
//!
//! Content is read server-side through the verified durable-content path.
//! Eligibility (text sniffing, the size cap) is writer-side policy decided
//! here at index time, never on the write path; the query path applies the
//! same rule to unindexed data so the search contract stays uniform.

use super::flush::{
    ensure_metadata_publication_budget, next_manifest_id_after, MANIFEST_ALLOCATION_RETRY_LIMIT,
};
use super::load::{load_namespace_manifest_envelope_if_present, load_verified_manifest_tables};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::runs::{CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, MAX_MAINTENANCE_TABLE_IO};
use super::scan::string_prefix_upper_bound;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{
    load_namespace_head_control, read_metadata_root_object, read_wal_floor_seq_or_zero,
};
use crate::storage::content::{read_durable_content_bytes, write_immutable_object};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use futures::future::try_join_all;
use loonfs_api::wire::index_grams::{
    extract_grams, Gram, GramIndexFoldState, GramPosting, IndexGramsFeature, IndexRow,
    INDEX_FAMILY_GRAMS, INDEX_GRAMS_FEATURE_KEY, INDEX_GRAMS_FORMAT_VERSION,
    INDEX_GRAMS_MAX_FILE_BYTES,
};
use loonfs_api::wire::manifest::{
    hex_encode_bytes, IndexFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    sha256_digest, ChangeSeq, ContentRef, ContentStoreId, IndexSegmentId, InodeId,
    ManifestObjectId, NamespaceId, RevisionNo,
};
use loonfs_objectstore::keys::{index_segment, metadata_manifest_object};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;

/// Postings per gram-postings row. A writer-side default: readers take
/// whatever batches rows carry.
const GRAM_POSTING_BATCH_TARGET: usize = 256;
/// Bytes of leading content the eligibility sniff inspects.
const ELIGIBILITY_SAMPLE_BYTES: usize = 8 * 1024;
/// Largest filter block inlined into an index descriptor, matching the
/// metadata-segment rule so gram probes on small delta segments skip the
/// object fetch.
const INLINE_INDEX_FILTER_MAX_BYTES: u32 = 1024;

// Grams-family segment levels. The levels are writer-side fold bookkeeping
// only: queries, garbage collection, and forks treat every referenced
// grams segment alike, so tiering never changes what reads see. Folds run
// in two tiers so base rewrites amortize — deltas fold into a fresh mid
// run when they pass `max_l0_segments` (cost proportional to recent
// churn), and mids fold together with the base into a fresh base when
// they pass `max_mid_runs` (the rare whole-index rewrite).

/// Level of the delta segments build steps write, the metadata L0 level.
pub(super) const INDEX_GRAMS_DELTA_LEVEL: u32 = CHECKPOINT_L0_RUN_LEVEL;
/// Level of a completed delta fold's outputs. Deliberately equal to
/// [`CHECKPOINT_BASE_RUN_LEVEL`], the level pre-tiering folds stamped on
/// their whole-set outputs: a legacy base counts as one mid run under
/// this policy (its segments share one `run_seq`), keeps serving reads
/// unchanged, and once real mid runs accumulate past the threshold the
/// base fold rewrites it into the level-2 base — a self-healing
/// migration with no `index.grams` version bump.
pub(super) const INDEX_GRAMS_MID_LEVEL: u32 = CHECKPOINT_BASE_RUN_LEVEL;
/// Level of a completed mid-plus-base fold's outputs. A serialized
/// [`GramIndexFoldState`] without an `output_level` decodes to this level
/// (the codec's `default_fold_output_level`), because pre-tiering states
/// always described a whole-set fold.
pub(super) const INDEX_GRAMS_BASE_LEVEL: u32 = 2;

/// Writer-side budgets for one build step. Work stops at a commit boundary
/// once a budget is exceeded; the next step resumes from the published
/// watermark or cursor.
#[derive(Debug, Clone, Copy)]
pub struct GramIndexBuildPolicy {
    /// Revisions examined per step.
    pub max_files_per_step: usize,
    /// Content bytes read per step.
    pub max_content_bytes_per_step: u64,
    /// Rows per written index segment.
    pub max_rows_per_segment: usize,
    /// Delta-level segments that trigger a fold into a fresh mid run, the
    /// same threshold shape as metadata L0 runs. A delta fold reads only
    /// the deltas, so its cost tracks recent churn, not index size.
    pub max_l0_segments: usize,
    /// Runs of mid segments that trigger folding the mids and the base
    /// into a fresh base — the rare step that rewrites the whole index,
    /// reached once per `max_mid_runs` delta folds. One delta fold's
    /// outputs are one run (they share one `run_seq`), however many
    /// segments the per-segment row cap split them into.
    pub max_mid_runs: usize,
    /// Rows one fold step merges before publishing and yielding; the
    /// cursor in the feature value makes the walk resumable. The budget
    /// is soft: rows with equal keys are consumed as one atomic group,
    /// so a step may overshoot by the size of one such group. The resume
    /// cursor is `last key + "\0"`, and splitting a group across steps
    /// would strand its tail behind the cursor and drop postings.
    pub max_fold_rows_per_step: usize,
}

impl GramIndexBuildPolicy {
    /// Zero-valued budgets would report progress without doing work (a
    /// zero file budget consumes no commit yet returns up to date); every
    /// step normalizes to at least one unit of each budget.
    fn normalized(self) -> Self {
        Self {
            max_files_per_step: self.max_files_per_step.max(1),
            max_content_bytes_per_step: self.max_content_bytes_per_step.max(1),
            max_rows_per_segment: self.max_rows_per_segment.max(1),
            max_l0_segments: self.max_l0_segments.max(1),
            max_mid_runs: self.max_mid_runs.max(1),
            max_fold_rows_per_step: self.max_fold_rows_per_step.max(1),
        }
    }
}

impl Default for GramIndexBuildPolicy {
    fn default() -> Self {
        Self {
            max_files_per_step: 256,
            max_content_bytes_per_step: 64 * 1024 * 1024,
            max_rows_per_segment: 65_536,
            max_l0_segments: 8,
            max_mid_runs: 8,
            max_fold_rows_per_step: 131_072,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GramIndexBuildReport {
    pub namespace_id: NamespaceId,
    pub outcome: GramIndexBuildOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GramIndexBuildOutcome {
    /// The namespace has no `index.grams` feature entry.
    NotEnabled,
    /// The feature value names a version this build does not implement;
    /// the step refuses to touch it and reads are unaffected.
    UnsupportedFeatureVersion { found: u32 },
    /// The index already covers the current head.
    UpToDate { built_through_seq: ChangeSeq },
    /// One bounded unit of work landed in a published manifest.
    Published {
        built_through_seq: ChangeSeq,
        indexed_revisions: u64,
        skipped_revisions: u64,
        segments_written: u64,
        /// False while a backfill cursor remains in the feature value.
        materialized: bool,
    },
    /// Someone published a newer root first; rerun against it.
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GramIndexEnableOutcome {
    /// The feature entry was published; backfill starts on the next step.
    Enabled {
        built_through_seq: ChangeSeq,
    },
    AlreadyEnabled {
        built_through_seq: ChangeSeq,
    },
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GramIndexDisableOutcome {
    /// The feature entry and every index segment reference were dropped;
    /// the segments become garbage-collection candidates.
    Disabled,
    NotEnabled,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GramIndexFoldReport {
    pub namespace_id: NamespaceId,
    pub outcome: GramIndexFoldOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GramIndexFoldOutcome {
    NotEnabled,
    UnsupportedFeatureVersion {
        found: u32,
    },
    /// Delta segments and mid runs both below their fold thresholds and no
    /// fold in flight; nothing rewritten.
    NotNeeded {
        l0_segments: usize,
        mid_runs: usize,
    },
    /// One bounded key range merged into fresh segments at the fold's
    /// output level and published. `completed` when this step finished the
    /// walk and swapped the snapshot out for the outputs.
    StepPublished {
        merged_rows: u64,
        segments_written: u64,
        completed: bool,
    },
    Superseded,
}

/// True when content participates in the gram index: within the size cap
/// and text by the grep-family sniff (no NUL byte and valid UTF-8 in the
/// leading sample; a sample that ends inside a multi-byte character still
/// counts as valid). The query path applies the same rule to unindexed
/// data, so eligibility is uniform whether or not a revision was indexed.
pub(crate) fn is_indexable_text_content(content: &[u8]) -> bool {
    if content.len() as u64 > INDEX_GRAMS_MAX_FILE_BYTES {
        return false;
    }
    let sample = &content[..content.len().min(ELIGIBILITY_SAMPLE_BYTES)];
    if sample.contains(&0) {
        return false;
    }
    match std::str::from_utf8(sample) {
        Ok(_) => true,
        // `error_len() == None` means the sample ends mid-character, which
        // says nothing about the content; anything else is real non-text.
        Err(error) => error.error_len().is_none() && error.valid_up_to() > 0,
    }
}

/// Publishes the `index.grams` feature entry, scheduling backfill over the
/// namespace's existing revisions. Idempotent: enabling an enabled
/// namespace reports its current watermark.
pub(crate) async fn enable_grams_index<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<GramIndexEnableOutcome> {
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();

    if let Some(value) = previous.payload.features.get(INDEX_GRAMS_FEATURE_KEY) {
        let feature = decode_feature(namespace_id, value)?;
        return Ok(GramIndexEnableOutcome::AlreadyEnabled {
            built_through_seq: feature.built_through_seq,
        });
    }

    // Backfill covers revisions committed at or below this manifest's head;
    // everything after it arrives through WAL replay once backfill ends,
    // which is why the watermark starts here rather than at zero.
    let built_through_seq = previous.payload.head_seq;
    let feature = IndexGramsFeature {
        version: INDEX_GRAMS_FORMAT_VERSION,
        built_through_seq,
        backfill_cursor: Some(String::new()),
        fold: None,
    };
    let predecessor_object_id = previous.payload.manifest_object_id.clone();
    // Manifest publishers must gate the root swap behind the publication
    // budget (format spec, "Garbage collection", rule 1); an enable is a
    // publisher like any other.
    let timer = StdMonotonicTimer::default();
    let publication_started_ms = timer.monotonic_now_ms();
    let manifest = write_index_successor_manifest(
        store,
        namespace_id,
        previous,
        previous.payload.index_files.clone(),
        feature,
        &context.writer_version,
    )
    .await?;
    ensure_metadata_publication_budget(&timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &predecessor_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => {
            Ok(GramIndexEnableOutcome::Enabled { built_through_seq })
        }
        ManifestPublicationOutcome::Superseded(_) | ManifestPublicationOutcome::RootCasRaceLost => {
            Ok(GramIndexEnableOutcome::Superseded)
        }
    }
}

/// Removes the feature entry and every index segment reference. The
/// segments become unreachable and garbage collection reclaims them.
pub(crate) async fn disable_grams_index<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<GramIndexDisableOutcome> {
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();
    if !previous
        .payload
        .features
        .contains_key(INDEX_GRAMS_FEATURE_KEY)
    {
        return Ok(GramIndexDisableOutcome::NotEnabled);
    }

    let mut payload = previous.payload.clone();
    payload.features.remove(INDEX_GRAMS_FEATURE_KEY);
    payload
        .index_files
        .retain(|descriptor| descriptor.family != INDEX_FAMILY_GRAMS);
    let predecessor_object_id = previous.payload.manifest_object_id.clone();
    let timer = StdMonotonicTimer::default();
    let publication_started_ms = timer.monotonic_now_ms();
    let manifest = write_successor_manifest_from_payload(
        store,
        namespace_id,
        previous,
        payload,
        &context.writer_version,
    )
    .await?;
    ensure_metadata_publication_budget(&timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &predecessor_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(GramIndexDisableOutcome::Disabled),
        ManifestPublicationOutcome::Superseded(_) | ManifestPublicationOutcome::RootCasRaceLost => {
            Ok(GramIndexDisableOutcome::Superseded)
        }
    }
}

/// Runs at most one gram index build unit against the live manifest:
/// backfill while the feature value carries a cursor, WAL-replay catch-up
/// after. Callers invoke this repeatedly; every call re-reads durable
/// state, so any two calls compose — including across process restarts.
#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "build_grams_index", key_class = "manifest")
)]
pub(crate) async fn build_grams_index_step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: GramIndexBuildPolicy,
) -> Result<GramIndexBuildReport> {
    let policy = policy.normalized();
    let timer = StdMonotonicTimer::default();
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();

    let Some(value) = previous.payload.features.get(INDEX_GRAMS_FEATURE_KEY) else {
        return Ok(GramIndexBuildReport {
            namespace_id: namespace_id.clone(),
            outcome: GramIndexBuildOutcome::NotEnabled,
        });
    };
    let feature = match IndexGramsFeature::from_value(value) {
        Ok(feature) => feature,
        Err(loonfs_api::wire::index_grams::IndexGramsCodecError::UnsupportedFeatureVersion {
            found,
            ..
        }) => {
            return Ok(GramIndexBuildReport {
                namespace_id: namespace_id.clone(),
                outcome: GramIndexBuildOutcome::UnsupportedFeatureVersion { found },
            });
        }
        Err(error) => {
            return Err(CoreError::NamespaceCorrupt(format!(
                "namespace `{}` carries an unreadable index.grams feature value: {error}",
                namespace_id.as_str()
            )));
        }
    };

    let catalog = load_namespace_catalog_entry(store, namespace_id).await?;
    let content_store_id = catalog.content_store_id().clone();

    let unit = if let Some(cursor) = feature.backfill_cursor.clone() {
        collect_backfill_unit(&tables, &feature, cursor, store, &content_store_id, policy).await?
    } else {
        match collect_wal_unit(store, namespace_id, &feature, &content_store_id, policy).await? {
            Some(unit) => unit,
            None => {
                return Ok(GramIndexBuildReport {
                    namespace_id: namespace_id.clone(),
                    outcome: GramIndexBuildOutcome::UpToDate {
                        built_through_seq: feature.built_through_seq,
                    },
                });
            }
        }
    };

    let rows = gram_postings_rows(unit.postings)?;
    // The publication budget starts at the first object write: it exists
    // to bound how long written-but-unpublished objects can age toward the
    // GC grace window, and content reads write nothing. One oversized
    // commit may take arbitrarily long to read yet still publish.
    let publication_started_ms = timer.monotonic_now_ms();
    let new_segments = write_index_segments(
        store,
        namespace_id,
        unit.run_seq,
        rows,
        policy.max_rows_per_segment,
    )
    .await?;
    let segments_written = new_segments.len() as u64;

    let mut index_files = previous.payload.index_files.clone();
    index_files.extend(new_segments);
    let next_feature = IndexGramsFeature {
        version: INDEX_GRAMS_FORMAT_VERSION,
        built_through_seq: unit.built_through_seq,
        backfill_cursor: unit.backfill_cursor.clone(),
        // A build step never touches an in-flight fold; the walk resumes
        // against whatever segments its snapshot pinned.
        fold: feature.fold.clone(),
    };
    let materialized = next_feature.is_materialized();
    let predecessor_object_id = previous.payload.manifest_object_id.clone();
    let manifest = write_index_successor_manifest(
        store,
        namespace_id,
        previous,
        index_files,
        next_feature,
        &context.writer_version,
    )
    .await?;
    ensure_metadata_publication_budget(&timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &predecessor_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(GramIndexBuildReport {
            namespace_id: namespace_id.clone(),
            outcome: GramIndexBuildOutcome::Published {
                built_through_seq: unit.built_through_seq,
                indexed_revisions: unit.indexed_revisions,
                skipped_revisions: unit.skipped_revisions,
                segments_written,
                materialized,
            },
        }),
        ManifestPublicationOutcome::Superseded(_) | ManifestPublicationOutcome::RootCasRaceLost => {
            Ok(GramIndexBuildReport {
                namespace_id: namespace_id.clone(),
                outcome: GramIndexBuildOutcome::Superseded,
            })
        }
    }
}

/// One bounded unit of collected work, before segments are written.
struct CollectedIndexUnit {
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    indexed_revisions: u64,
    skipped_revisions: u64,
    /// The run sequence stamped on the segments this unit writes.
    run_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    backfill_cursor: Option<String>,
}

async fn collect_backfill_unit<S: ObjectStore + ?Sized>(
    tables: &super::scan::VerifiedMetadataTables<'_, S>,
    feature: &IndexGramsFeature,
    cursor: String,
    store: &S,
    content_store_id: &ContentStoreId,
    policy: GramIndexBuildPolicy,
) -> Result<CollectedIndexUnit> {
    // Resume strictly after the cursor row key; the empty cursor starts at
    // the family prefix. The walk reads the manifest, never old WAL, so
    // enablement works regardless of what retention has discarded.
    let lower_bound = if cursor.is_empty() {
        "revision-".to_owned()
    } else {
        format!("{cursor}\0")
    };
    let upper_bound = string_prefix_upper_bound("revision-");
    let page = tables
        .scan_range_page_with_keys(
            MetadataTableFamily::Revisions,
            &lower_bound,
            upper_bound.as_deref(),
            policy.max_files_per_step.max(1),
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let walk_complete = page.len() < policy.max_files_per_step.max(1);

    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: feature.built_through_seq,
        built_through_seq: feature.built_through_seq,
        backfill_cursor: Some(cursor),
    };
    // Select first, fetch after: the byte budget is charged from each
    // reference's declared size, which the verified content read proves
    // equal to the fetched length, so the walk stops at exactly the row
    // the fetch-then-count serial loop stopped at.
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut budget_reached = false;
    for (row_key, row) in page {
        let MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            content_ref,
            ..
        } = row
        else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "revisions family returned a non-revision row at `{row_key}`"
            )));
        };
        // Revisions past the enable-time watermark belong to WAL replay;
        // the cursor still advances over them so the walk terminates.
        if committed_seq <= feature.built_through_seq {
            // The size cap is checked from the reference before any
            // fetch; oversized files cost nothing to skip.
            if content_ref.size_bytes > INDEX_GRAMS_MAX_FILE_BYTES {
                unit.skipped_revisions += 1;
            } else {
                planned_content_bytes += content_ref.size_bytes;
                pending.push(PendingRevisionContent {
                    inode_id,
                    revision_no,
                    content_ref,
                });
            }
        }
        unit.backfill_cursor = Some(row_key);
        if planned_content_bytes >= policy.max_content_bytes_per_step {
            budget_reached = true;
            break;
        }
    }
    if walk_complete && !budget_reached {
        unit.backfill_cursor = None;
    }
    fetch_and_fold_revision_contents(store, content_store_id, &pending, &mut unit).await?;
    Ok(unit)
}

/// Collects the next WAL-replay unit, or `None` when the index already
/// covers the head.
async fn collect_wal_unit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    feature: &IndexGramsFeature,
    content_store_id: &ContentStoreId,
    policy: GramIndexBuildPolicy,
) -> Result<Option<CollectedIndexUnit>> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if feature.built_through_seq >= head.seq {
        return Ok(None);
    }
    let floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if feature.built_through_seq < floor_seq {
        // While the feature is enabled, retention never advances past the
        // watermark, so a watermark below the floor means someone moved the
        // floor by hand. The index must be rebuilt (disable, then enable)
        // rather than silently skip the missing history.
        return Err(CoreError::CheckpointUnavailable(format!(
            "index.grams watermark {} is below the retention floor {}; \
             disable and re-enable the index to rebuild it",
            feature.built_through_seq.0, floor_seq.0
        )));
    }

    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: Some(feature.built_through_seq),
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;

    let mut unit = CollectedIndexUnit {
        postings: BTreeMap::new(),
        indexed_revisions: 0,
        skipped_revisions: 0,
        run_seq: feature.built_through_seq,
        built_through_seq: feature.built_through_seq,
        backfill_cursor: None,
    };
    // Select first, fetch after: the byte budget is charged from each
    // reference's declared size, which the verified content read proves
    // equal to the fetched length, so the unit consumes exactly the
    // commits the fetch-then-count serial loop consumed.
    let mut pending = Vec::new();
    let mut planned_content_bytes = 0u64;
    let mut examined_files = 0usize;
    'commits: for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq <= feature.built_through_seq {
                continue;
            }
            // A commit is consumed whole or not at all: the watermark is a
            // commit boundary, so budgets are checked between commits.
            if examined_files >= policy.max_files_per_step
                || planned_content_bytes >= policy.max_content_bytes_per_step
            {
                break 'commits;
            }
            for delta in &record.deltas {
                if let WalDelta::AppendFileRevision {
                    inode_id,
                    revision_no,
                    content_ref,
                    ..
                } = &delta.delta
                {
                    examined_files += 1;
                    // The size cap is checked from the reference before
                    // any fetch; oversized files cost nothing to skip.
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
                }
            }
            unit.built_through_seq = record.seq;
            unit.run_seq = record.seq;
        }
    }
    if unit.built_through_seq == feature.built_through_seq {
        return Ok(None);
    }
    fetch_and_fold_revision_contents(store, content_store_id, &pending, &mut unit).await?;
    Ok(Some(unit))
}

/// One revision a build unit selected for indexing, before its content
/// fetch: everything eligibility already decided, so the fetch stage runs
/// without re-deriving it.
struct PendingRevisionContent {
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
}

/// Fetches the selected revisions' contents in chunks of
/// [`MAX_MAINTENANCE_TABLE_IO`] and folds each eligible file's grams into
/// the unit, strictly in selection order. Selection already excluded
/// oversized revisions, so every entry here costs one content read; the
/// text sniff still runs on the fetched bytes and drops non-text files
/// after the read, exactly as the serial path did.
async fn fetch_and_fold_revision_contents<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    pending: &[PendingRevisionContent],
    unit: &mut CollectedIndexUnit,
) -> Result<()> {
    for chunk in pending.chunks(MAX_MAINTENANCE_TABLE_IO) {
        let contents = try_join_all(chunk.iter().map(|revision| {
            read_durable_content_bytes(store, content_store_id, &revision.content_ref)
        }))
        .await?;
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

/// Batches collected postings into rows, in ascending row-key order.
fn gram_postings_rows(postings: BTreeMap<Gram, Vec<GramPosting>>) -> Result<Vec<IndexRow>> {
    let mut rows = Vec::new();
    for (gram, mut gram_postings) in postings {
        // Discovery order is commit order, not inode order; batches must be
        // strictly ascending by durable identity.
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

/// Writes the unit's index segments and returns their descriptors.
async fn write_index_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    rows: Vec<IndexRow>,
    max_rows_per_segment: usize,
) -> Result<Vec<IndexFileRef>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut requests = Vec::new();
    for (segment_index, segment_rows) in rows.chunks(max_rows_per_segment.max(1)).enumerate() {
        let segment_index = u32::try_from(segment_index)
            .map_err(|_| CoreError::Internal("index segment index overflow".to_owned()))?;
        requests.push((segment_index, segment_rows.to_vec()));
    }
    let mut descriptors = Vec::with_capacity(requests.len());
    let mut pending = requests.into_iter();
    loop {
        let chunk = pending
            .by_ref()
            .take(MAX_MAINTENANCE_TABLE_IO)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        descriptors.extend(
            try_join_all(chunk.into_iter().map(|(segment_index, segment_rows)| {
                write_index_segment(store, namespace_id, run_seq, segment_index, segment_rows)
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
    segment_index: u32,
    rows: Vec<IndexRow>,
) -> Result<IndexFileRef> {
    let segment_id = IndexSegmentId::generate();
    let object_key = index_segment(namespace_id.as_str(), segment_id.as_str());
    let mut builder = SegmentBlocksBuilder::default();
    for row in &rows {
        builder
            .push(&row.row_key(), &row.filter_key(), row)
            .map_err(|err| {
                CoreError::Internal(format!(
                    "failed to build index segment `{object_key}`: {err}"
                ))
            })?;
    }
    let built = builder.finish().map_err(|err| {
        CoreError::Internal(format!(
            "failed to build index segment `{object_key}`: {err}"
        ))
    })?;
    write_immutable_object(store, &object_key, &built.bytes).await?;
    let filter_inline = (built.filter.stored_len <= INLINE_INDEX_FILTER_MAX_BYTES).then(|| {
        let start = built.filter.offset as usize;
        hex_encode_bytes(&built.bytes[start..start + built.filter.stored_len as usize])
    });
    Ok(IndexFileRef {
        owner_namespace_id: namespace_id.clone(),
        segment_id,
        object_key,
        family: INDEX_FAMILY_GRAMS.to_owned(),
        run_seq,
        level: INDEX_GRAMS_DELTA_LEVEL,
        segment_index,
        row_count: built.row_count,
        min_key: built.min_key,
        max_key: built.max_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
        payload_checksum: sha256_digest(&built.bytes),
    })
}

/// Runs at most one gram index fold step.
///
/// Folds are tiered, partitioned, and resumable. When no fold is in
/// flight, delta segments past `max_l0_segments` start a delta fold —
/// snapshot the deltas only, outputs at the mid level, base untouched —
/// and otherwise mid runs past `max_mid_runs` start a base fold, which
/// snapshots the mids and the base together and rewrites them at the base
/// level. One fold runs at a time: segments that cross a threshold while
/// another fold is mid-walk wait for the tick after it completes.
///
/// The first step snapshots the segment set the fold will consume. Every
/// step then merges one bounded key range from that snapshot into fresh
/// segments at the fold's output level and publishes a manifest recording
/// the outputs plus a resume cursor in the feature value. Until the final
/// step swaps the snapshot out, both the snapshot inputs and the fold
/// outputs stay referenced and served; postings are add-only, so readers
/// that union them see duplicates, never gaps. Segments written while the
/// fold runs stay out of the snapshot and survive it.
#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "fold_grams_index", key_class = "manifest")
)]
pub(crate) async fn fold_grams_index_step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: GramIndexBuildPolicy,
) -> Result<GramIndexFoldReport> {
    let policy = policy.normalized();
    let timer = StdMonotonicTimer::default();
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();

    let Some(value) = previous.payload.features.get(INDEX_GRAMS_FEATURE_KEY) else {
        return Ok(GramIndexFoldReport {
            namespace_id: namespace_id.clone(),
            outcome: GramIndexFoldOutcome::NotEnabled,
        });
    };
    let mut feature = match IndexGramsFeature::from_value(value) {
        Ok(feature) => feature,
        Err(loonfs_api::wire::index_grams::IndexGramsCodecError::UnsupportedFeatureVersion {
            found,
            ..
        }) => {
            return Ok(GramIndexFoldReport {
                namespace_id: namespace_id.clone(),
                outcome: GramIndexFoldOutcome::UnsupportedFeatureVersion { found },
            });
        }
        // A value this build should understand but cannot decode is
        // namespace corruption, exactly as the build path treats it;
        // folding against corrupt state must not proceed or report
        // NotNeeded.
        Err(error) => {
            return Err(CoreError::NamespaceCorrupt(format!(
                "namespace `{}` carries an unreadable index.grams feature value: {error}",
                namespace_id.as_str()
            )));
        }
    };

    let grams_segments: Vec<IndexFileRef> = previous
        .payload
        .index_files
        .iter()
        .filter(|descriptor| descriptor.family == INDEX_FAMILY_GRAMS)
        .cloned()
        .collect();
    let fold_state = match feature.fold.clone() {
        Some(state) => state,
        None => {
            let l0_segments = grams_segments
                .iter()
                .filter(|descriptor| descriptor.level == INDEX_GRAMS_DELTA_LEVEL)
                .count();
            // Runs, not segments: one fold's outputs all carry the
            // snapshot's max run_seq, however many segments the row cap
            // split them into, so distinct run_seqs at the mid level count
            // completed delta folds — the same run counting as metadata's
            // `l0_run_count`. Counting segments would tie the base-fold
            // cadence to output sizing and collapse the amortization.
            let mid_runs = grams_segments
                .iter()
                .filter(|descriptor| descriptor.level == INDEX_GRAMS_MID_LEVEL)
                .map(|descriptor| descriptor.run_seq)
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            let (snapshot, output_level) = if l0_segments >= policy.max_l0_segments {
                // Delta fold: consume the deltas only and write one fresh
                // mid run. Whatever the base has grown to, this fold's
                // cost is the recent churn.
                let deltas = grams_segments
                    .iter()
                    .filter(|descriptor| descriptor.level == INDEX_GRAMS_DELTA_LEVEL)
                    .map(|descriptor| descriptor.segment_id.as_str().to_owned())
                    .collect();
                (deltas, INDEX_GRAMS_MID_LEVEL)
            } else if mid_runs >= policy.max_mid_runs {
                // Base fold: consume everything already folded once — the
                // mid runs, the base, and any legacy pre-tiering base
                // (also at the mid level) — and rewrite it as a fresh
                // base. The rare expensive step.
                let folded = grams_segments
                    .iter()
                    .filter(|descriptor| descriptor.level != INDEX_GRAMS_DELTA_LEVEL)
                    .map(|descriptor| descriptor.segment_id.as_str().to_owned())
                    .collect();
                (folded, INDEX_GRAMS_BASE_LEVEL)
            } else {
                return Ok(GramIndexFoldReport {
                    namespace_id: namespace_id.clone(),
                    outcome: GramIndexFoldOutcome::NotNeeded {
                        l0_segments,
                        mid_runs,
                    },
                });
            };
            GramIndexFoldState {
                snapshot,
                outputs: Vec::new(),
                cursor: String::new(),
                output_level,
            }
        }
    };

    let snapshot: Vec<&IndexFileRef> = fold_state
        .snapshot
        .iter()
        .map(|segment_id| {
            grams_segments
                .iter()
                .find(|descriptor| descriptor.segment_id.as_str() == segment_id)
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "fold snapshot references segment `{segment_id}` missing from the manifest"
                    ))
                })
        })
        .collect::<Result<_>>()?;
    let merged = merge_snapshot_range(
        store,
        &snapshot,
        &fold_state.cursor,
        policy.max_fold_rows_per_step,
    )
    .await?;
    let rows = gram_postings_rows(merged.postings)?;
    let run_seq = snapshot
        .iter()
        .map(|descriptor| descriptor.run_seq)
        .max()
        .unwrap_or(previous.payload.head_seq);

    // The publication budget starts at the first object write (format
    // spec, "Garbage collection", rule 1); reading snapshot blocks writes
    // nothing.
    let publication_started_ms = timer.monotonic_now_ms();
    let new_segments = write_index_folded_segments(
        store,
        namespace_id,
        run_seq,
        rows,
        policy.max_rows_per_segment,
        // The level rides the durable fold state, so a fold resumed after
        // a restart — or by another writer — keeps stamping the level the
        // trigger chose.
        fold_state.output_level,
    )
    .await?;
    let segments_written = new_segments.len() as u64;

    let mut index_files = previous.payload.index_files.clone();
    index_files.extend(new_segments.iter().cloned());
    let mut next_state = fold_state.clone();
    next_state.outputs.extend(
        new_segments
            .iter()
            .map(|descriptor| descriptor.segment_id.as_str().to_owned()),
    );
    let completed = merged.exhausted;
    if completed {
        let snapshot_ids: std::collections::BTreeSet<&str> =
            next_state.snapshot.iter().map(String::as_str).collect();
        index_files.retain(|descriptor| {
            descriptor.family != INDEX_FAMILY_GRAMS
                || !snapshot_ids.contains(descriptor.segment_id.as_str())
        });
        feature.fold = None;
    } else {
        next_state.cursor = merged.next_cursor;
        feature.fold = Some(next_state);
    }

    let predecessor_object_id = previous.payload.manifest_object_id.clone();
    let manifest = write_index_successor_manifest(
        store,
        namespace_id,
        previous,
        index_files,
        feature,
        &context.writer_version,
    )
    .await?;
    ensure_metadata_publication_budget(&timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &predecessor_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(GramIndexFoldReport {
            namespace_id: namespace_id.clone(),
            outcome: GramIndexFoldOutcome::StepPublished {
                merged_rows: merged.rows,
                segments_written,
                completed,
            },
        }),
        ManifestPublicationOutcome::Superseded(_) | ManifestPublicationOutcome::RootCasRaceLost => {
            Ok(GramIndexFoldReport {
                namespace_id: namespace_id.clone(),
                outcome: GramIndexFoldOutcome::Superseded,
            })
        }
    }
}

/// One bounded range of merged snapshot rows.
struct MergedRange {
    postings: BTreeMap<Gram, Vec<GramPosting>>,
    /// Row key the next step resumes from; meaningful only when not
    /// exhausted.
    next_cursor: String,
    exhausted: bool,
    rows: u64,
}

/// K-way merges snapshot rows with keys at or past `cursor`, reading one
/// data block per segment at a time, until the row budget fills or every
/// segment is exhausted.
async fn merge_snapshot_range<S: ObjectStore + ?Sized>(
    store: &S,
    snapshot: &[&IndexFileRef],
    cursor: &str,
    max_rows: usize,
) -> Result<MergedRange> {
    let mut readers = Vec::with_capacity(snapshot.len());
    for descriptor in snapshot {
        readers.push(SegmentRangeReader::open(store, descriptor, cursor).await?);
    }
    let mut merged = MergedRange {
        postings: BTreeMap::new(),
        next_cursor: String::new(),
        exhausted: false,
        rows: 0,
    };
    let mut last_key = String::new();
    while merged.rows < max_rows as u64 {
        for reader in readers.iter_mut() {
            reader.refill(store).await?;
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
        fold_snapshot_row(&mut merged, row, readers[position].object_key())?;
        // Exact duplicates of this key are legal, across segments and
        // within one: the row key is `gram-{hex}-{first batch inode}`, and
        // the same (gram, first-inode) pair arises whenever two runs — or
        // two batches of one run — start a batch at the same posting. The
        // resume cursor is `key\0`, which skips everything at or below
        // `key`, so the whole equal-key group must be consumed before the
        // budget is re-checked or an unconsumed twin would be stranded
        // behind the cursor and its postings dropped at the completing
        // swap. This is what makes the row budget soft (see
        // `GramIndexBuildPolicy::max_fold_rows_per_step`).
        for reader in readers.iter_mut() {
            loop {
                // Peek is only meaningful after a refill, mirroring the
                // loop head: a drained block must not end the group early.
                reader.refill(store).await?;
                if reader.peek_key() != Some(key.as_str()) {
                    break;
                }
                let (_, duplicate) = reader.pop();
                fold_snapshot_row(&mut merged, duplicate, reader.object_key())?;
            }
        }
        last_key = key;
    }
    // Budget filled: resume strictly after the last merged row. Peek once
    // more so a walk that ended exactly at the keyspace end completes now
    // instead of publishing an empty trailing step.
    let mut any_left = false;
    for reader in readers.iter_mut() {
        reader.refill(store).await?;
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

/// Folds one snapshot row's posting batch into the merged range and
/// charges it against the row budget.
fn fold_snapshot_row(merged: &mut MergedRange, row: IndexRow, object_key: &str) -> Result<()> {
    let IndexRow::GramPostings { gram, .. } = &row;
    let gram = *gram;
    let batch = row.postings().map_err(|error| {
        CoreError::NamespaceCorrupt(format!(
            "index segment `{object_key}` carries an unreadable posting batch: {error}"
        ))
    })?;
    merged.postings.entry(gram).or_default().extend(batch);
    merged.rows += 1;
    Ok(())
}

/// Streaming reader over one snapshot segment's rows from a start key:
/// the segment index is fetched once, data blocks one at a time.
struct SegmentRangeReader {
    object_key: String,
    entries: Vec<loonfs_api::wire::sst_blocks::SegmentIndexEntry>,
    next_entry: usize,
    pending: std::collections::VecDeque<(String, IndexRow)>,
    start: String,
}

impl SegmentRangeReader {
    async fn open<S: ObjectStore + ?Sized>(
        store: &S,
        descriptor: &IndexFileRef,
        cursor: &str,
    ) -> Result<Self> {
        use loonfs_api::wire::sst_blocks::{decode_index_block, index_blocks_for_key_range};
        let index_bytes =
            fetch_index_section(store, &descriptor.object_key, &descriptor.index_block).await?;
        let entries =
            decode_index_block(&index_bytes, &descriptor.index_block).map_err(|error| {
                CoreError::NamespaceCorrupt(format!(
                    "index segment `{}` carries an unreadable index block: {error}",
                    descriptor.object_key
                ))
            })?;
        let start = if cursor.is_empty() { "gram-" } else { cursor };
        let range = index_blocks_for_key_range(&entries, start, None);
        Ok(Self {
            object_key: descriptor.object_key.clone(),
            next_entry: range.start,
            entries,
            pending: std::collections::VecDeque::new(),
            start: start.to_owned(),
        })
    }

    fn object_key(&self) -> &str {
        &self.object_key
    }

    fn peek_key(&self) -> Option<&str> {
        self.pending.front().map(|(key, _)| key.as_str())
    }

    fn pop(&mut self) -> (String, IndexRow) {
        self.pending
            .pop_front()
            .expect("pop is called only after peek_key returned a key")
    }

    async fn refill<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        use loonfs_api::wire::sst_blocks::decode_data_block_rows;
        while self.pending.is_empty() && self.next_entry < self.entries.len() {
            let entry = &self.entries[self.next_entry];
            self.next_entry += 1;
            let block_bytes = fetch_index_section(store, &self.object_key, &entry.block).await?;
            let block = decode_data_block_rows::<IndexRow>(&block_bytes, &entry.block).map_err(
                |error| {
                    CoreError::NamespaceCorrupt(format!(
                        "index segment `{}` carries an unreadable data block: {error}",
                        self.object_key
                    ))
                },
            )?;
            for (key, row) in block.row_keys.into_iter().zip(block.rows) {
                if key.as_str() >= self.start.as_str() {
                    self.pending.push_back((key, row));
                }
            }
        }
        Ok(())
    }
}

/// Fetches one descriptor-named section by byte range, refusing handles
/// whose bounds do not fit the address space.
async fn fetch_index_section<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    handle: &loonfs_api::wire::sst_blocks::BlockHandle,
) -> Result<Vec<u8>> {
    let end_exclusive = handle
        .offset
        .checked_add(u64::from(handle.stored_len))
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "index segment `{object_key}` descriptor names bytes past the address space"
            ))
        })?;
    let bytes = store
        .get(
            object_key,
            Some(loonfs_objectstore::ByteRange {
                start_inclusive: handle.offset,
                end_exclusive,
            }),
        )
        .await
        .map_err(|error| CoreError::store(object_key, &error))?
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "manifest references missing index segment `{object_key}`"
            ))
        })?;
    Ok(bytes.to_vec())
}

/// [`write_index_segments`] restamped at a fold's output level: the mid
/// level for a delta fold, the base level for a mid-plus-base fold.
async fn write_index_folded_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    rows: Vec<IndexRow>,
    max_rows_per_segment: usize,
    output_level: u32,
) -> Result<Vec<IndexFileRef>> {
    let mut descriptors =
        write_index_segments(store, namespace_id, run_seq, rows, max_rows_per_segment).await?;
    for descriptor in &mut descriptors {
        descriptor.level = output_level;
    }
    Ok(descriptors)
}

fn decode_feature(
    namespace_id: &NamespaceId,
    value: &serde_json::Value,
) -> Result<IndexGramsFeature> {
    IndexGramsFeature::from_value(value).map_err(|error| {
        CoreError::NamespaceCorrupt(format!(
            "namespace `{}` carries an unreadable index.grams feature value: {error}",
            namespace_id.as_str()
        ))
    })
}

async fn write_index_successor_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestEnvelope,
    index_files: Vec<IndexFileRef>,
    feature: IndexGramsFeature,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope> {
    let mut payload = previous.payload.clone();
    payload.index_files = index_files;
    payload
        .features
        .insert(INDEX_GRAMS_FEATURE_KEY.to_owned(), feature.to_value());
    write_successor_manifest_from_payload(store, namespace_id, previous, payload, writer_version)
        .await
}

/// Writes a successor manifest whose payload differs from `previous` only
/// in the caller's edits: same head, same metadata files, next manifest id.
/// The same allocation pattern as reorganization and flush — collisions
/// re-generate the object id, identical checksums are idempotent successes.
async fn write_successor_manifest_from_payload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestEnvelope,
    mut payload: NamespaceManifestPayload,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope> {
    let manifest_id = next_manifest_id_after(previous.payload.manifest_id)?;
    payload.manifest_id = manifest_id;
    for _allocation_attempt in 0..MANIFEST_ALLOCATION_RETRY_LIMIT {
        let manifest_object_id = ManifestObjectId::generate(manifest_id);
        let manifest_key = metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
        match load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            &manifest_object_id,
            &manifest_key,
        )
        .await
        {
            Ok(Some(_existing)) => continue,
            Ok(None) => {}
            Err(error) => {
                return Err(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::ManifestLoad(error),
                ))
            }
        }
        let mut candidate = payload.clone();
        candidate.manifest_object_id = manifest_object_id;
        let manifest = NamespaceManifestEnvelope::from_payload(writer_version, candidate)
            .map_err(|err| CoreError::Internal(format!("failed to build index manifest: {err}")))?;
        match write_namespace_manifest(store, &manifest).await {
            Ok(()) => return Ok(manifest),
            Err(MetadataProjectionLoadError::ManifestLoad(
                super::error::ManifestLoadError::ManifestConflict { .. },
            )) => continue,
            Err(error) => return Err(CoreError::MetadataProjection(error)),
        }
    }
    Err(CoreError::Internal(
        "index manifest allocation retry exhausted".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_state_codec_default_matches_the_base_level() {
        // The decode default for a legacy fold state lives in `loonfs-api`
        // while the level scheme lives here; this pin keeps the two from
        // drifting apart. A state without an output level described a
        // whole-set fold and must complete at the base.
        let legacy: GramIndexFoldState = serde_json::from_value(serde_json::json!({
            "snapshot": [],
            "outputs": [],
            "cursor": ""
        }))
        .expect("decode legacy fold state");
        assert_eq!(legacy.output_level, INDEX_GRAMS_BASE_LEVEL);
        assert_ne!(INDEX_GRAMS_DELTA_LEVEL, INDEX_GRAMS_MID_LEVEL);
        assert_ne!(INDEX_GRAMS_MID_LEVEL, INDEX_GRAMS_BASE_LEVEL);
    }

    #[test]
    fn eligibility_accepts_text_and_rejects_binary_and_oversize() {
        assert!(is_indexable_text_content(b"plain text\n"));
        assert!(is_indexable_text_content(&[]));
        assert!(is_indexable_text_content("Grüße".as_bytes()));
        assert!(!is_indexable_text_content(b"bin\0ary"));
        assert!(!is_indexable_text_content(&[0xff, 0xfe, 0x00, 0x01]));
        // The cap is a format constant of feature version 1; one byte over
        // it is ineligible.
        let oversized = vec![b'a'; INDEX_GRAMS_MAX_FILE_BYTES as usize + 1];
        assert!(!is_indexable_text_content(&oversized));
        // Invalid UTF-8 that is not a truncated tail is rejected.
        assert!(!is_indexable_text_content(&[b'a', 0xc3, b'a']));
    }

    #[test]
    fn eligibility_tolerates_a_sample_split_mid_character() {
        // A multi-byte character straddling the sample boundary must not
        // disqualify the file: build the sample so it ends mid-character.
        let mut content = vec![b'a'; super::ELIGIBILITY_SAMPLE_BYTES - 1];
        content.extend_from_slice("é".as_bytes());
        content.extend_from_slice(b" more text");
        assert!(is_indexable_text_content(&content));
    }

    #[test]
    fn gram_rows_sort_dedup_and_batch_postings() {
        let mut postings = BTreeMap::new();
        postings.insert(
            Gram(*b"abc"),
            vec![
                GramPosting {
                    inode_id: InodeId(9),
                    revision_no: RevisionNo(1),
                },
                GramPosting {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(1),
                },
                GramPosting {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(1),
                },
            ],
        );
        let rows = gram_postings_rows(postings).expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].postings().expect("postings"),
            vec![
                GramPosting {
                    inode_id: InodeId(2),
                    revision_no: RevisionNo(1),
                },
                GramPosting {
                    inode_id: InodeId(9),
                    revision_no: RevisionNo(1),
                },
            ]
        );
    }
}
