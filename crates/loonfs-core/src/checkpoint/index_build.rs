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
//! - **Steady state**: replay the WAL after `built_through_seq` — the
//!   change feed is the designed index-building extension point — and index
//!   the file revisions that appeared, advancing the watermark to the last
//!   fully consumed commit.
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
use super::runs::{CHECKPOINT_L0_RUN_LEVEL, MAX_MAINTENANCE_TABLE_IO};
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
    extract_grams, Gram, GramPosting, IndexGramsFeature, IndexRow, INDEX_FAMILY_GRAMS,
    INDEX_GRAMS_FEATURE_KEY, INDEX_GRAMS_FORMAT_VERSION,
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

/// Writer-side budgets for one build step. Work stops at a commit boundary
/// once a budget is exceeded; the next step resumes from the published
/// watermark or cursor.
#[derive(Debug, Clone, Copy)]
pub struct GramIndexBuildPolicy {
    /// Revisions examined per step.
    pub max_files_per_step: usize,
    /// Content bytes read per step.
    pub max_content_bytes_per_step: u64,
    /// Revisions larger than this are never indexed.
    pub max_file_bytes: u64,
    /// Rows per written index segment.
    pub max_rows_per_segment: usize,
}

impl Default for GramIndexBuildPolicy {
    fn default() -> Self {
        Self {
            max_files_per_step: 256,
            max_content_bytes_per_step: 64 * 1024 * 1024,
            max_file_bytes: 8 * 1024 * 1024,
            max_rows_per_segment: 65_536,
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

/// True when content participates in the gram index: within the size cap
/// and text by the grep-family sniff (no NUL byte and valid UTF-8 in the
/// leading sample; a sample that ends inside a multi-byte character still
/// counts as valid). The query path applies the same rule to unindexed
/// data, so eligibility is uniform whether or not a revision was indexed.
pub(crate) fn is_indexable_text_content(content: &[u8], max_file_bytes: u64) -> bool {
    if content.len() as u64 > max_file_bytes {
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
    };
    let manifest = write_index_successor_manifest(
        store,
        namespace_id,
        previous,
        previous.payload.index_files.clone(),
        feature,
        &context.writer_version,
    )
    .await?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
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
    let manifest = write_successor_manifest_from_payload(
        store,
        namespace_id,
        previous,
        payload,
        &context.writer_version,
    )
    .await?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
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
    let timer = StdMonotonicTimer::default();
    let publication_started_ms = timer.monotonic_now_ms();
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
    };
    let materialized = next_feature.is_materialized();
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
    let mut content_bytes_read = 0u64;
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
            let indexed = index_revision_content(
                store,
                content_store_id,
                inode_id,
                revision_no,
                &content_ref,
                policy,
                &mut unit.postings,
                &mut content_bytes_read,
            )
            .await?;
            if indexed {
                unit.indexed_revisions += 1;
            } else {
                unit.skipped_revisions += 1;
            }
        }
        unit.backfill_cursor = Some(row_key);
        if content_bytes_read >= policy.max_content_bytes_per_step {
            return Ok(unit);
        }
    }
    if walk_complete {
        unit.backfill_cursor = None;
    }
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
        // Retention advancement is clamped to the watermark while the
        // feature is enabled, so this indicates operator surgery; the index
        // must be rebuilt (disable, then enable) rather than silently skip
        // history.
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
    let mut content_bytes_read = 0u64;
    let mut examined_files = 0usize;
    'commits: for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq <= feature.built_through_seq {
                continue;
            }
            // A commit is consumed whole or not at all: the watermark is a
            // commit boundary, so budgets are checked between commits.
            if examined_files >= policy.max_files_per_step
                || content_bytes_read >= policy.max_content_bytes_per_step
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
                    let indexed = index_revision_content(
                        store,
                        content_store_id,
                        *inode_id,
                        *revision_no,
                        content_ref,
                        policy,
                        &mut unit.postings,
                        &mut content_bytes_read,
                    )
                    .await?;
                    if indexed {
                        unit.indexed_revisions += 1;
                    } else {
                        unit.skipped_revisions += 1;
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
    Ok(Some(unit))
}

/// Reads one revision's content and folds its grams into the unit's
/// postings. Returns whether the revision was indexed (false: ineligible).
#[allow(clippy::too_many_arguments)]
async fn index_revision_content<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: &ContentRef,
    policy: GramIndexBuildPolicy,
    postings: &mut BTreeMap<Gram, Vec<GramPosting>>,
    content_bytes_read: &mut u64,
) -> Result<bool> {
    // The size cap is checked from the reference before any fetch; the
    // sniff needs bytes, so oversized files cost nothing to skip.
    if content_ref.size_bytes > policy.max_file_bytes {
        return Ok(false);
    }
    let content = read_durable_content_bytes(store, content_store_id, content_ref).await?;
    *content_bytes_read += content.bytes.len() as u64;
    if !is_indexable_text_content(&content.bytes, policy.max_file_bytes) {
        return Ok(false);
    }
    let posting = GramPosting {
        inode_id,
        revision_no,
    };
    for gram in extract_grams(&content.bytes) {
        postings.entry(gram).or_default().push(posting);
    }
    Ok(true)
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
        level: CHECKPOINT_L0_RUN_LEVEL,
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
    fn eligibility_accepts_text_and_rejects_binary_and_oversize() {
        assert!(is_indexable_text_content(b"plain text\n", 1024));
        assert!(is_indexable_text_content(&[], 1024));
        assert!(is_indexable_text_content("Grüße".as_bytes(), 1024));
        assert!(!is_indexable_text_content(b"bin\0ary", 1024));
        assert!(!is_indexable_text_content(&[0xff, 0xfe, 0x00, 0x01], 1024));
        assert!(!is_indexable_text_content(b"too big", 3));
        // Invalid UTF-8 that is not a truncated tail is rejected.
        assert!(!is_indexable_text_content(&[b'a', 0xc3, b'a'], 1024));
    }

    #[test]
    fn eligibility_tolerates_a_sample_split_mid_character() {
        // A multi-byte character straddling the sample boundary must not
        // disqualify the file: build the sample so it ends mid-character.
        let mut content = vec![b'a'; super::ELIGIBILITY_SAMPLE_BYTES - 1];
        content.extend_from_slice("é".as_bytes());
        content.extend_from_slice(b" more text");
        assert!(is_indexable_text_content(
            &content,
            2 * super::ELIGIBILITY_SAMPLE_BYTES as u64
        ));
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
