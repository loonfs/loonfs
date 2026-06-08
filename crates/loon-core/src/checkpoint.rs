use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::CommitHeadPublishError;
use crate::content::write_immutable_object;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::loading::read_head_object;
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    MetadataStateBuilder, RevisionRecord, SubtreeTombstoneRecord,
};
use loon_api::wire::checkpoint::{
    checkpoint_page_checksum_sha256, checkpoint_segment_payload_checksum_sha256,
    decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
    encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd,
    CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointPage, CheckpointRow,
    CheckpointRunManifest, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
    CheckpointSegmentKey, CheckpointSegmentPayload, CheckpointTableFamily, CheckpointTableManifest,
};
use loon_api::wire::control::{
    ControlObjectKind, HeadState, HeadStateEnvelope, ProgressStateEnvelope,
};
use loon_api::{
    generate_checkpoint_run_id, validate_checkpoint_run_id, AdvanceRetentionResponse, ChangeSeq,
    CreateCheckpointResponse, FenceToken, InodeId, NamespaceId,
};
use loon_objectstore::keys::{
    checkpoint_manifest, checkpoint_run_table, derived_progress,
    CheckpointTableFamily as ObjectStoreCheckpointTableFamily, DerivedWorkClass,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use thiserror::Error;

const HEAD_UPDATE_RETRY_LIMIT: usize = 8;
// V1 does not require any derived work classes to be caught up before the
// retention floor advances. This hook stays in place so future retention gates
// can add progress requirements without restructuring the flow.
const REQUIRED_RETENTION_PROGRESS_CLASSES: &[DerivedWorkClass] = &[];
const DEFAULT_MAX_CHECKPOINT_L0_RUNS: usize = 8;
const DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT: usize = 4096;
const SMALL_SCAN_CACHE_SEGMENT_LIMIT: usize = 4;
#[cfg(test)]
const MAX_CHECKPOINT_L0_RUNS: usize = DEFAULT_MAX_CHECKPOINT_L0_RUNS;
const CHECKPOINT_L0_RUN_LEVEL: u32 = 0;
const CHECKPOINT_BASE_RUN_LEVEL: u32 = 1;
const CHECKPOINT_TABLE_FAMILIES: [CheckpointTableFamily; 7] = [
    CheckpointTableFamily::Inodes,
    CheckpointTableFamily::DirentryBinds,
    CheckpointTableFamily::DirentryChildBinds,
    CheckpointTableFamily::DirentryUnbinds,
    CheckpointTableFamily::Revisions,
    CheckpointTableFamily::Tombstones,
    CheckpointTableFamily::CommitReceipts,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataLsmPolicy {
    pub max_l0_runs: usize,
    pub max_rows_per_segment: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataTableCacheConfig {
    pub enabled: bool,
    pub max_blocks: usize,
    pub max_decoded_bytes: Option<usize>,
}

impl Default for MetadataTableCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_blocks: 256,
            max_decoded_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataTableCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MetadataTableBlockKind {
    SegmentPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetadataTableCacheKey {
    table_digest: String,
    block_kind: MetadataTableBlockKind,
    block_offset: u64,
}

#[derive(Debug, Clone)]
struct DecodedMetadataTableBlock {
    rows: Vec<CheckpointRow>,
    segment_seq: ChangeSeq,
    family: CheckpointTableFamily,
    segment_index: u32,
    segment_key: CheckpointSegmentKey,
    row_count: u64,
    min_key: String,
    max_key: String,
    page_checksums_sha256: Vec<String>,
    decoded_byte_len: usize,
}

#[derive(Debug)]
pub struct MetadataTableCache {
    config: MetadataTableCacheConfig,
    inner: Mutex<MetadataTableCacheInner>,
    stats: MetadataTableCacheStatsInner,
}

#[derive(Debug, Default)]
struct MetadataTableCacheInner {
    entries: HashMap<MetadataTableCacheKey, DecodedMetadataTableBlock>,
    order: VecDeque<MetadataTableCacheKey>,
    decoded_byte_len: usize,
}

#[derive(Debug, Default)]
struct MetadataTableCacheStatsInner {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    evictions: AtomicUsize,
}

impl MetadataTableCache {
    pub fn new(config: MetadataTableCacheConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(MetadataTableCacheInner::default()),
            stats: MetadataTableCacheStatsInner::default(),
        }
    }

    pub fn stats(&self) -> MetadataTableCacheStats {
        MetadataTableCacheStats {
            hits: self.stats.hits.load(Ordering::SeqCst),
            misses: self.stats.misses.load(Ordering::SeqCst),
            inserts: self.stats.inserts.load(Ordering::SeqCst),
            evictions: self.stats.evictions.load(Ordering::SeqCst),
        }
    }

    fn get(&self, key: &MetadataTableCacheKey) -> Option<DecodedMetadataTableBlock> {
        if !self.config.enabled || self.config.max_blocks == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock poisoned");
        let Some(block) = inner.entries.get(key).cloned() else {
            self.stats.misses.fetch_add(1, Ordering::SeqCst);
            return None;
        };
        inner.touch(key);
        self.stats.hits.fetch_add(1, Ordering::SeqCst);
        Some(block)
    }

    fn insert(&self, key: MetadataTableCacheKey, block: DecodedMetadataTableBlock) {
        if !self.config.enabled || self.config.max_blocks == 0 {
            return;
        }
        let mut inner = self
            .inner
            .lock()
            .expect("metadata table cache lock poisoned");
        if let Some(previous) = inner.entries.insert(key.clone(), block.clone()) {
            inner.decoded_byte_len = inner
                .decoded_byte_len
                .saturating_sub(previous.decoded_byte_len);
        }
        inner.decoded_byte_len = inner
            .decoded_byte_len
            .saturating_add(block.decoded_byte_len);
        inner.touch(&key);
        self.stats.inserts.fetch_add(1, Ordering::SeqCst);
        while inner.entries.len() > self.config.max_blocks
            || self
                .config
                .max_decoded_bytes
                .map(|max_decoded_bytes| inner.decoded_byte_len > max_decoded_bytes)
                .unwrap_or(false)
        {
            let Some(evicted) = inner.order.pop_front() else {
                break;
            };
            if let Some(block) = inner.entries.remove(&evicted) {
                inner.decoded_byte_len = inner
                    .decoded_byte_len
                    .saturating_sub(block.decoded_byte_len);
                self.stats.evictions.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

impl MetadataTableCacheInner {
    fn touch(&mut self, key: &MetadataTableCacheKey) {
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
    }
}

impl Default for MetadataLsmPolicy {
    fn default() -> Self {
        Self {
            max_l0_runs: DEFAULT_MAX_CHECKPOINT_L0_RUNS,
            max_rows_per_segment: DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
        }
    }
}

fn l0_run_count(payload: &CheckpointManifestPayload) -> usize {
    payload
        .runs
        .iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .count()
}

fn runs_in_scan_order(payload: &CheckpointManifestPayload) -> Vec<&CheckpointRunManifest> {
    let mut runs = payload.runs.iter().collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.level
            .cmp(&right.level)
            .then(right.run_seq.cmp(&left.run_seq))
            .then(left.run_id.cmp(&right.run_id))
    });
    runs
}

fn runs_in_materialization_order(
    payload: &CheckpointManifestPayload,
) -> Vec<&CheckpointRunManifest> {
    let mut runs = payload.runs.iter().collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.level.cmp(&left.level))
            .then(left.run_id.cmp(&right.run_id))
    });
    runs
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedCheckpointMaterialization {
    pub(crate) manifest: CheckpointManifestEnvelope,
    pub(crate) metadata_state: MetadataState,
}

pub(crate) struct VerifiedCheckpointTables<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: NamespaceId,
    manifest_object_key: String,
    manifest: CheckpointManifestEnvelope,
    segment_cache: RefCell<HashMap<String, Vec<CheckpointRow>>>,
}

impl<S: ObjectStore + ?Sized> VerifiedCheckpointTables<'_, S> {
    pub(crate) fn manifest(&self) -> &CheckpointManifestEnvelope {
        &self.manifest
    }

    pub(crate) fn get(
        &self,
        family: CheckpointTableFamily,
        key: &str,
    ) -> Result<Option<CheckpointRow>, CheckpointLoadError> {
        Ok(self
            .scan_prefix_with_cache_mode(family, key, MetadataTableCacheMode::Populate)?
            .into_iter()
            .find(|row| row.row_key_for_family(family) == key))
    }

    pub(crate) fn scan_prefix(
        &self,
        family: CheckpointTableFamily,
        prefix: &str,
    ) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
        let matching_segment_count = self.matching_segment_count(family, prefix)?;
        let cache_mode = if matching_segment_count <= SMALL_SCAN_CACHE_SEGMENT_LIMIT {
            MetadataTableCacheMode::Populate
        } else {
            MetadataTableCacheMode::ReadOnly
        };
        self.scan_prefix_with_cache_mode(family, prefix, cache_mode)
    }

    fn scan_prefix_with_cache_mode(
        &self,
        family: CheckpointTableFamily,
        prefix: &str,
        cache_mode: MetadataTableCacheMode,
    ) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
        let mut rows = Vec::new();
        for run in runs_in_scan_order(&self.manifest.payload) {
            self.scan_manifest_tables(
                family,
                prefix,
                CheckpointTableLoadContext {
                    manifest_object_key: &self.manifest_object_key,
                    segment_seq_expectation: CheckpointSegmentSeqExpectation::Descriptor,
                    row_seq_min: None,
                    row_seq_max: run.run_seq,
                },
                &run.tables,
                |family, segment_seq, segment_index| {
                    checkpoint_run_table(
                        self.namespace_id.as_str(),
                        segment_seq.0,
                        &run.run_id,
                        checkpoint_table_family(family),
                        segment_index,
                    )
                },
                CheckpointTableScanOutput {
                    rows: &mut rows,
                    cache_mode,
                },
            )?;
        }
        Ok(rows)
    }

    fn matching_segment_count(
        &self,
        family: CheckpointTableFamily,
        prefix: &str,
    ) -> Result<usize, CheckpointLoadError> {
        let mut count = 0;
        for run in &self.manifest.payload.runs {
            count +=
                count_matching_segments(&self.manifest_object_key, &run.tables, family, prefix)?;
        }
        Ok(count)
    }

    fn scan_manifest_tables<ExpectedObjectKey>(
        &self,
        family: CheckpointTableFamily,
        prefix: &str,
        context: CheckpointTableLoadContext<'_>,
        tables: &[CheckpointTableManifest],
        mut expected_object_key: ExpectedObjectKey,
        output: CheckpointTableScanOutput<'_>,
    ) -> Result<(), CheckpointLoadError>
    where
        ExpectedObjectKey: FnMut(CheckpointTableFamily, ChangeSeq, u32) -> String,
    {
        let table = checkpoint_table_for_family(context.manifest_object_key, tables, family)?;
        for descriptor in &table.segments {
            let expected_segment_seq = context.expected_segment_seq(descriptor)?;
            let expected_key =
                expected_object_key(family, expected_segment_seq, descriptor.segment_index);
            if descriptor.object_key != expected_key {
                return Err(CheckpointLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            if !descriptor_may_contain_prefix(descriptor, prefix) {
                continue;
            }
            let segment_rows = self.segment_rows(context, family, descriptor, output.cache_mode)?;
            output.rows.extend(
                segment_rows
                    .into_iter()
                    .filter(|row| row.row_key_for_family(family).starts_with(prefix)),
            );
        }
        Ok(())
    }

    fn segment_rows(
        &self,
        context: CheckpointTableLoadContext<'_>,
        family: CheckpointTableFamily,
        descriptor: &CheckpointSegmentDescriptor,
        cache_mode: MetadataTableCacheMode,
    ) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
        if let Some(rows) = self.segment_cache.borrow().get(&descriptor.object_key) {
            return Ok(rows.clone());
        }
        let rows = load_checkpoint_segment_rows_with_cache(
            self.store,
            self.table_cache,
            &self.namespace_id,
            context,
            family,
            descriptor,
            cache_mode,
        )?;
        self.segment_cache
            .borrow_mut()
            .insert(descriptor.object_key.clone(), rows.clone());
        Ok(rows)
    }
}

struct CheckpointTableScanOutput<'a> {
    rows: &'a mut Vec<CheckpointRow>,
    cache_mode: MetadataTableCacheMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointLoadErrorKind {
    Corrupt,
    Store,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum CheckpointLoadError {
    #[error("missing checkpoint manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to read checkpoint manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("checkpoint manifest codec error for `{object_key}`: {message}")]
    ManifestCodec { object_key: String, message: String },
    #[error(
        "checkpoint manifest namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "checkpoint manifest seq mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    ManifestSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("checkpoint manifest `{object_key}` is not verified")]
    ManifestNotVerified { object_key: String },
    #[error("checkpoint manifest `{object_key}` is missing table family `{family:?}`")]
    MissingTableFamily {
        object_key: String,
        family: CheckpointTableFamily,
    },
    #[error("checkpoint manifest `{object_key}` repeats table family `{family:?}`")]
    DuplicateTableFamily {
        object_key: String,
        family: CheckpointTableFamily,
    },
    #[error("checkpoint manifest `{object_key}` has invalid runs: {message}")]
    RunManifestMismatch { object_key: String, message: String },
    #[error("missing checkpoint segment `{object_key}`")]
    MissingSegment { object_key: String },
    #[error("failed to read checkpoint segment `{object_key}`: {message}")]
    ReadSegment { object_key: String, message: String },
    #[error("checkpoint segment codec error for `{object_key}`: {message}")]
    SegmentCodec { object_key: String, message: String },
    #[error(
        "checkpoint segment namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "checkpoint segment seq mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error(
        "checkpoint segment family mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentFamilyMismatch {
        object_key: String,
        expected: CheckpointTableFamily,
        actual: CheckpointTableFamily,
    },
    #[error(
        "checkpoint segment index mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentIndexMismatch {
        object_key: String,
        expected: u32,
        actual: u32,
    },
    #[error(
        "checkpoint segment key mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentKeyMismatch {
        object_key: String,
        expected: CheckpointSegmentKey,
        actual: CheckpointSegmentKey,
    },
    #[error("checkpoint segment key mismatch for `{object_key}`: expected `{expected}`")]
    SegmentObjectKeyMismatch {
        object_key: String,
        expected: String,
    },
    #[error("checkpoint segment descriptor mismatch for `{object_key}`: {message}")]
    SegmentDescriptorMismatch { object_key: String, message: String },
    #[error("checkpoint page shape mismatch for `{object_key}` page {page_index}: {message}")]
    PageShapeMismatch {
        object_key: String,
        page_index: u32,
        message: String,
    },
    #[error(
        "checkpoint page checksum mismatch for `{object_key}` page {page_index}: expected `{expected}`, actual `{actual}`"
    )]
    PageChecksumMismatch {
        object_key: String,
        page_index: u32,
        expected: String,
        actual: String,
    },
    #[error(
        "checkpoint row key mismatch for `{object_key}` page {page_index} row {row_index}: expected `{expected}`, actual `{actual}`"
    )]
    RowKeyMismatch {
        object_key: String,
        page_index: u32,
        row_index: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "checkpoint row kind mismatch for `{object_key}` family `{family:?}`: found `{row_kind}`"
    )]
    TableRowKindMismatch {
        object_key: String,
        family: CheckpointTableFamily,
        row_kind: String,
    },
    #[error("checkpoint rows do not reproduce authoritative metadata")]
    MetadataMismatch,
}

impl CheckpointLoadError {
    pub fn kind(&self) -> CheckpointLoadErrorKind {
        match self {
            Self::ReadManifest { .. } | Self::ReadSegment { .. } => CheckpointLoadErrorKind::Store,
            _ => CheckpointLoadErrorKind::Corrupt,
        }
    }
}

pub(crate) fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<CreateCheckpointResponse, CoreError> {
    create_checkpoint_with_policy(store, namespace_id, context, MetadataLsmPolicy::default())
}

pub(crate) fn create_checkpoint_with_policy<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> Result<CreateCheckpointResponse, CoreError> {
    let basis = {
        let _span = tracing::info_span!("loon.phase", phase = "scan_namespace_state").entered();
        load_verified_namespace_basis(store, namespace_id)
    }?;
    let checkpoint_seq = basis.head.seq;

    match load_verified_checkpoint_materialization_if_present(store, namespace_id, checkpoint_seq) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let manifest = build_checkpoint_manifest_for_basis(
                store,
                namespace_id,
                &basis,
                &context.writer_version,
                policy,
            )?;
            let materialized = load_checkpoint_materialization_from_manifest(
                store,
                namespace_id,
                &checkpoint_manifest(namespace_id.as_str(), checkpoint_seq.0),
                &manifest,
            )
            .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?;
            if !metadata_states_equivalent(&basis.metadata_state, &materialized) {
                return Err(CoreError::Basis(BasisLoadError::CheckpointLoad(
                    CheckpointLoadError::MetadataMismatch,
                )));
            }

            // `write_checkpoint_manifest` owns the idempotent "manifest already
            // exists" path. Any checkpoint load error it returns must surface.
            write_checkpoint_manifest(store, &manifest).map_err(CoreError::Basis)?;
        }
        Err(error) => return Err(CoreError::Basis(BasisLoadError::CheckpointLoad(error))),
    }

    let resulting_head =
        publish_checkpoint_hint_seq(store, namespace_id, checkpoint_seq, &context.writer_version)?;

    Ok(CreateCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_seq,
        checkpoint_hint_seq: resulting_head.checkpoint_hint_seq,
        checkpoint_hint_points_at_checkpoint: resulting_head.checkpoint_hint_seq
            == Some(checkpoint_seq),
    })
}

pub(crate) struct CheckpointMetadataWriteRequest<'a> {
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) checkpoint_seq: ChangeSeq,
    pub(crate) active_fence_token: FenceToken,
    pub(crate) next_inode_id: InodeId,
    pub(crate) retention_floor_seq: ChangeSeq,
    pub(crate) metadata_state: &'a MetadataState,
    pub(crate) writer_version: &'a str,
}

pub(crate) fn write_verified_checkpoint_from_metadata<S: ObjectStore + ?Sized>(
    store: &S,
    request: CheckpointMetadataWriteRequest<'_>,
) -> Result<CheckpointManifestEnvelope, CoreError> {
    let run_id = generate_checkpoint_run_id();
    let tables = build_checkpoint_tables(
        store,
        request.namespace_id,
        &run_id,
        request.checkpoint_seq,
        request.metadata_state,
        request.writer_version,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )?;
    let materialized = load_checkpoint_materialization_from_tables(
        store,
        request.namespace_id,
        &checkpoint_manifest(request.namespace_id.as_str(), request.checkpoint_seq.0),
        &run_id,
        request.checkpoint_seq,
        &tables,
    )
    .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?;
    if !metadata_states_equivalent(request.metadata_state, &materialized) {
        return Err(CoreError::Basis(BasisLoadError::CheckpointLoad(
            CheckpointLoadError::MetadataMismatch,
        )));
    }

    let manifest = CheckpointManifestEnvelope::from_payload(
        request.writer_version,
        CheckpointManifestPayload {
            namespace_id: request.namespace_id.clone(),
            checkpoint_seq: request.checkpoint_seq,
            base_seq: request.checkpoint_seq,
            active_fence_token: request.active_fence_token,
            next_inode_id: request.next_inode_id,
            retention_floor_seq: request.retention_floor_seq,
            verified: true,
            runs: vec![CheckpointRunManifest {
                run_id,
                run_seq: request.checkpoint_seq,
                level: CHECKPOINT_BASE_RUN_LEVEL,
                tables,
            }],
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    write_checkpoint_manifest(store, &manifest).map_err(CoreError::Basis)?;
    Ok(manifest)
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "project_checkpoint")
)]
fn build_checkpoint_manifest_for_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &crate::basis::VerifiedNamespaceBasis,
    writer_version: &str,
    policy: MetadataLsmPolicy,
) -> Result<CheckpointManifestEnvelope, CoreError> {
    let checkpoint_seq = basis.head.seq;
    let previous_checkpoint = match basis.head.checkpoint_hint_seq {
        Some(previous_seq) if previous_seq < checkpoint_seq => Some(
            load_verified_checkpoint_materialization(store, namespace_id, previous_seq)
                .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?,
        ),
        _ => None,
    };

    let (base_seq, runs) = match previous_checkpoint {
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs => {
            let run_id = generate_checkpoint_run_id();
            let mut runs = previous.manifest.payload.runs.clone();
            runs.push(CheckpointRunManifest {
                run_id: run_id.clone(),
                run_seq: checkpoint_seq,
                level: CHECKPOINT_L0_RUN_LEVEL,
                tables: build_checkpoint_l0_run_tables(
                    store,
                    namespace_id,
                    checkpoint_seq,
                    &run_id,
                    previous.manifest.payload.checkpoint_seq,
                    &basis.metadata_state,
                    writer_version,
                )?,
            });
            (previous.manifest.payload.base_seq, runs)
        }
        Some(_) => {
            let run_id = generate_checkpoint_run_id();
            let run_tables = build_checkpoint_tables(
                store,
                namespace_id,
                &run_id,
                checkpoint_seq,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )?;
            debug_assert_checkpoint_table_segments_do_not_overlap(&run_tables);
            (
                checkpoint_seq,
                vec![CheckpointRunManifest {
                    run_id,
                    run_seq: checkpoint_seq,
                    level: CHECKPOINT_BASE_RUN_LEVEL,
                    tables: run_tables,
                }],
            )
        }
        _ => {
            let run_id = generate_checkpoint_run_id();
            let run_tables = build_checkpoint_tables(
                store,
                namespace_id,
                &run_id,
                checkpoint_seq,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )?;
            (
                checkpoint_seq,
                vec![CheckpointRunManifest {
                    run_id,
                    run_seq: checkpoint_seq,
                    level: CHECKPOINT_BASE_RUN_LEVEL,
                    tables: run_tables,
                }],
            )
        }
    };

    CheckpointManifestEnvelope::from_payload(
        writer_version,
        CheckpointManifestPayload {
            namespace_id: namespace_id.clone(),
            checkpoint_seq,
            base_seq,
            active_fence_token: basis.head.active_fence_token,
            next_inode_id: basis.head.next_inode_id,
            retention_floor_seq: basis.head.retention_floor_seq,
            verified: true,
            runs,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))
}

pub(crate) fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    for _attempt in 0..HEAD_UPDATE_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let head = loaded_head.envelope.state;
        let Some(target_floor) = head.checkpoint_hint_seq else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "namespace `{}` has no published checkpoint",
                namespace_id.as_str()
            )));
        };
        let _ = load_verified_checkpoint_materialization(store, namespace_id, target_floor)
            .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?;
        ensure_required_retention_progress(store, namespace_id, target_floor)?;

        if head.retention_floor_seq >= target_floor {
            return Ok(AdvanceRetentionResponse {
                namespace_id: namespace_id.clone(),
                retention_floor_seq: head.retention_floor_seq,
            });
        }

        let next_head = HeadState {
            namespace_id: head.namespace_id.clone(),
            seq: head.seq,
            active_fence_token: head.active_fence_token,
            next_inode_id: head.next_inode_id,
            name_policy: head.name_policy,
            checkpoint_hint_seq: head.checkpoint_hint_seq,
            retention_floor_seq: target_floor,
            visible_wal_tip: head.visible_wal_tip.clone(),
        };
        match compare_and_swap_head(
            store,
            &loaded_head.object_key,
            loaded_head.metadata.etag.as_deref(),
            &context.writer_version,
            &next_head,
        ) {
            Ok(()) => {
                return Ok(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: target_floor,
                });
            }
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(error) => return Err(CoreError::Store(error.to_string())),
        }
    }

    Err(CoreError::Store(
        "retention floor compare-and-swap retry exhausted".to_owned(),
    ))
}

pub(crate) fn load_verified_checkpoint_materialization<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
) -> Result<LoadedCheckpointMaterialization, CheckpointLoadError> {
    load_verified_checkpoint_materialization_if_present(store, namespace_id, checkpoint_seq)?
        .ok_or_else(|| CheckpointLoadError::MissingManifest {
            object_key: checkpoint_manifest(namespace_id.as_str(), checkpoint_seq.0),
        })
}

pub(crate) fn load_verified_checkpoint_tables_with_cache<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
) -> Result<VerifiedCheckpointTables<'a, S>, CheckpointLoadError> {
    let manifest_key = checkpoint_manifest(namespace_id.as_str(), checkpoint_seq.0);
    let manifest = {
        let _span = tracing::info_span!(
            "loon.phase",
            phase = "load_checkpoint_manifest",
            key_class = "checkpoint_table"
        )
        .entered();
        let Some(manifest_bytes) =
            store
                .get(&manifest_key, None)
                .map_err(|err| CheckpointLoadError::ReadManifest {
                    object_key: manifest_key.clone(),
                    message: err.to_string(),
                })?
        else {
            return Err(CheckpointLoadError::MissingManifest {
                object_key: manifest_key.clone(),
            });
        };
        decode_checkpoint_manifest_json(&manifest_bytes).map_err(|err| {
            CheckpointLoadError::ManifestCodec {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            }
        })
    }?;
    validate_checkpoint_manifest(namespace_id, checkpoint_seq, &manifest_key, &manifest)?;
    validate_checkpoint_materialization_ranges(&manifest_key, &manifest.payload)?;
    for run in &manifest.payload.runs {
        validate_checkpoint_run_id(&run.run_id).map_err(|error| {
            CheckpointLoadError::RunManifestMismatch {
                object_key: manifest_key.clone(),
                message: error.to_string(),
            }
        })?;
    }
    let tables = VerifiedCheckpointTables {
        store,
        table_cache,
        namespace_id: namespace_id.clone(),
        manifest_object_key: manifest_key,
        manifest,
        segment_cache: RefCell::new(HashMap::new()),
    };
    Ok(tables)
}

pub(crate) fn checkpoint_basis_head(
    current_head: &HeadState,
    manifest: &CheckpointManifestEnvelope,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: manifest.payload.checkpoint_seq,
        // The manifest records the checkpoint-time fence token. That may lag the
        // live head if lease takeover advanced the fence without any WAL replay.
        active_fence_token: manifest.payload.active_fence_token,
        next_inode_id: manifest.payload.next_inode_id,
        name_policy: current_head.name_policy,
        checkpoint_hint_seq: current_head.checkpoint_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: None,
    }
}

fn load_verified_checkpoint_materialization_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
) -> Result<Option<LoadedCheckpointMaterialization>, CheckpointLoadError> {
    let manifest_key = checkpoint_manifest(namespace_id.as_str(), checkpoint_seq.0);
    let manifest = {
        let _span = tracing::info_span!(
            "loon.phase",
            phase = "load_checkpoint_manifest",
            key_class = "checkpoint_table"
        )
        .entered();
        let Some(manifest_bytes) =
            store
                .get(&manifest_key, None)
                .map_err(|err| CheckpointLoadError::ReadManifest {
                    object_key: manifest_key.clone(),
                    message: err.to_string(),
                })?
        else {
            return Ok(None);
        };
        let manifest = decode_checkpoint_manifest_json(&manifest_bytes).map_err(|err| {
            CheckpointLoadError::ManifestCodec {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            }
        })?;
        Ok(Some(manifest))
    }?;
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    validate_checkpoint_manifest(namespace_id, checkpoint_seq, &manifest_key, &manifest)?;
    let metadata_state = load_checkpoint_materialization_from_manifest(
        store,
        namespace_id,
        &manifest_key,
        &manifest,
    )?;
    Ok(Some(LoadedCheckpointMaterialization {
        manifest,
        metadata_state,
    }))
}

fn build_checkpoint_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_id: &str,
    checkpoint_seq: ChangeSeq,
    metadata_state: &MetadataState,
    writer_version: &str,
    max_rows_per_segment: usize,
) -> Result<Vec<CheckpointTableManifest>, CoreError> {
    build_checkpoint_tables_from_rows(
        store,
        namespace_id,
        checkpoint_seq,
        writer_version,
        |family| checkpoint_rows_for_family(metadata_state, family),
        CheckpointTableSegmentation::Base {
            max_rows_per_segment,
        },
        |family, segment_index| {
            checkpoint_run_table(
                namespace_id.as_str(),
                checkpoint_seq.0,
                run_id,
                checkpoint_table_family(family),
                segment_index,
            )
        },
    )
}

fn debug_assert_checkpoint_table_segments_do_not_overlap(tables: &[CheckpointTableManifest]) {
    #[cfg(debug_assertions)]
    for table in tables {
        let mut previous_max_key: Option<&str> = None;
        for descriptor in &table.segments {
            if let Some(previous) = previous_max_key {
                debug_assert!(
                    previous < descriptor.min_key.as_str(),
                    "overlapping checkpoint segment ranges for `{:?}`",
                    table.family
                );
            }
            previous_max_key = Some(descriptor.max_key.as_str());
        }
    }
}

fn build_checkpoint_l0_run_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    run_id: &str,
    after_seq: ChangeSeq,
    metadata_state: &MetadataState,
    writer_version: &str,
) -> Result<Vec<CheckpointTableManifest>, CoreError> {
    build_checkpoint_tables_from_rows(
        store,
        namespace_id,
        checkpoint_seq,
        writer_version,
        |family| checkpoint_rows_for_family_after_seq(metadata_state, family, after_seq),
        CheckpointTableSegmentation::Full,
        |family, segment_index| {
            checkpoint_run_table(
                namespace_id.as_str(),
                checkpoint_seq.0,
                run_id,
                checkpoint_table_family(family),
                segment_index,
            )
        },
    )
}

#[derive(Debug, Clone, Copy)]
enum CheckpointTableSegmentation {
    Base { max_rows_per_segment: usize },
    Full,
}

struct CheckpointSegmentRows {
    segment_key: CheckpointSegmentKey,
    rows: Vec<CheckpointRow>,
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "write_checkpoint_tables", key_class = "checkpoint_table")
)]
fn build_checkpoint_tables_from_rows<S, RowsForFamily, ObjectKeyForSegment>(
    store: &S,
    namespace_id: &NamespaceId,
    segment_seq: ChangeSeq,
    writer_version: &str,
    mut rows_for_family: RowsForFamily,
    segmentation: CheckpointTableSegmentation,
    mut object_key_for_segment: ObjectKeyForSegment,
) -> Result<Vec<CheckpointTableManifest>, CoreError>
where
    S: ObjectStore + ?Sized,
    RowsForFamily: FnMut(CheckpointTableFamily) -> Vec<CheckpointRow>,
    ObjectKeyForSegment: FnMut(CheckpointTableFamily, u32) -> String,
{
    let mut tables = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let rows = rows_for_family(family);
        if rows.is_empty() {
            tables.push(CheckpointTableManifest {
                family,
                segments: Vec::new(),
            });
            continue;
        }

        let segments = segment_checkpoint_rows(family, rows, segmentation);
        let mut descriptors = Vec::with_capacity(segments.len());
        for (segment_index, segment_rows) in segments.into_iter().enumerate() {
            let segment_index = u32::try_from(segment_index)
                .map_err(|_| CoreError::Store("checkpoint segment index overflow".to_owned()))?;
            let object_key = object_key_for_segment(family, segment_index);
            descriptors.push(write_checkpoint_segment(
                store,
                CheckpointSegmentWriteRequest {
                    namespace_id,
                    segment_seq,
                    family,
                    segment_index,
                    segment_key: segment_rows.segment_key,
                    rows: segment_rows.rows,
                    object_key,
                    writer_version,
                },
            )?);
        }
        tables.push(CheckpointTableManifest {
            family,
            segments: descriptors,
        });
    }
    Ok(tables)
}

struct CheckpointSegmentWriteRequest<'a> {
    namespace_id: &'a NamespaceId,
    segment_seq: ChangeSeq,
    family: CheckpointTableFamily,
    segment_index: u32,
    segment_key: CheckpointSegmentKey,
    rows: Vec<CheckpointRow>,
    object_key: String,
    writer_version: &'a str,
}

fn write_checkpoint_segment<S: ObjectStore + ?Sized>(
    store: &S,
    request: CheckpointSegmentWriteRequest<'_>,
) -> Result<CheckpointSegmentDescriptor, CoreError> {
    let row_keys = request
        .rows
        .iter()
        .map(|row| row.row_key_for_family(request.family))
        .collect::<Vec<_>>();
    let page = CheckpointPage {
        page_index: 0,
        min_key: row_keys.first().cloned().unwrap_or_default(),
        max_key: row_keys.last().cloned().unwrap_or_default(),
        row_keys,
        rows: request.rows,
    };
    let payload = CheckpointSegmentPayload {
        namespace_id: request.namespace_id.clone(),
        segment_seq: request.segment_seq,
        family: request.family,
        segment_index: request.segment_index,
        segment_key: request.segment_key,
        row_count: page.rows.len() as u64,
        min_key: page.min_key.clone(),
        max_key: page.max_key.clone(),
        pages: vec![page],
    };
    let envelope = CheckpointSegmentEnvelope::from_payload(request.writer_version, payload)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    let encoded = encode_checkpoint_segment_envelope_zstd(&envelope)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    write_immutable_object(store, &request.object_key, &encoded)?;
    Ok(CheckpointSegmentDescriptor {
        object_key: request.object_key,
        segment_seq: request.segment_seq,
        segment_index: request.segment_index,
        segment_key: envelope.payload.segment_key.clone(),
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: checkpoint_segment_payload_checksum_sha256(&envelope.payload)
            .map_err(|err| CoreError::Store(err.to_string()))?,
        page_checksums_sha256: envelope
            .page_checksums_sha256()
            .map_err(|err| CoreError::Store(err.to_string()))?,
    })
}

fn segment_checkpoint_rows(
    family: CheckpointTableFamily,
    rows: Vec<CheckpointRow>,
    segmentation: CheckpointTableSegmentation,
) -> Vec<CheckpointSegmentRows> {
    match segmentation {
        CheckpointTableSegmentation::Full => vec![CheckpointSegmentRows {
            segment_key: CheckpointSegmentKey::Full,
            rows,
        }],
        CheckpointTableSegmentation::Base {
            max_rows_per_segment,
        } => match family {
            CheckpointTableFamily::DirentryBinds | CheckpointTableFamily::DirentryUnbinds => {
                segment_rows_by_parent(rows)
            }
            CheckpointTableFamily::Inodes
            | CheckpointTableFamily::DirentryChildBinds
            | CheckpointTableFamily::Revisions
            | CheckpointTableFamily::Tombstones
            | CheckpointTableFamily::CommitReceipts => {
                segment_rows_by_row_key_range(rows, max_rows_per_segment.max(1))
            }
        },
    }
}

fn segment_rows_by_parent(rows: Vec<CheckpointRow>) -> Vec<CheckpointSegmentRows> {
    let mut grouped: BTreeMap<InodeId, Vec<CheckpointRow>> = BTreeMap::new();
    for row in rows {
        if let Some(parent_inode_id) = checkpoint_row_parent_inode_id(&row) {
            grouped.entry(parent_inode_id).or_default().push(row);
        }
    }

    grouped
        .into_iter()
        .map(|(parent_inode_id, rows)| CheckpointSegmentRows {
            segment_key: CheckpointSegmentKey::DirentryParent { parent_inode_id },
            rows,
        })
        .collect()
}

fn segment_rows_by_row_key_range(
    rows: Vec<CheckpointRow>,
    max_rows_per_segment: usize,
) -> Vec<CheckpointSegmentRows> {
    rows.chunks(max_rows_per_segment)
        .enumerate()
        .map(|(shard, rows)| CheckpointSegmentRows {
            segment_key: CheckpointSegmentKey::RowKeyRange {
                shard: u32::try_from(shard).unwrap_or(u32::MAX),
            },
            rows: rows.to_vec(),
        })
        .collect()
}

fn checkpoint_row_parent_inode_id(row: &CheckpointRow) -> Option<InodeId> {
    match row {
        CheckpointRow::DirentryBind {
            parent_inode_id, ..
        }
        | CheckpointRow::DirentryUnbind {
            parent_inode_id, ..
        } => Some(*parent_inode_id),
        _ => None,
    }
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "write_checkpoint_manifest", key_class = "checkpoint_table")
)]
fn write_checkpoint_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &CheckpointManifestEnvelope,
) -> Result<(), BasisLoadError> {
    let manifest_key = checkpoint_manifest(
        manifest.payload.namespace_id.as_str(),
        manifest.payload.checkpoint_seq.0,
    );
    let manifest_bytes = encode_checkpoint_manifest_json(manifest).map_err(|err| {
        BasisLoadError::CheckpointLoad(CheckpointLoadError::ManifestCodec {
            object_key: manifest_key.clone(),
            message: err.to_string(),
        })
    })?;
    match store.put_if_absent(&manifest_key, &manifest_bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            let Some(existing) = load_verified_checkpoint_materialization_if_present(
                store,
                &manifest.payload.namespace_id,
                manifest.payload.checkpoint_seq,
            )
            .map_err(BasisLoadError::CheckpointLoad)?
            else {
                return Err(BasisLoadError::CheckpointLoad(
                    CheckpointLoadError::MissingManifest {
                        object_key: manifest_key,
                    },
                ));
            };
            if existing.manifest.payload.checkpoint_seq == manifest.payload.checkpoint_seq {
                Ok(())
            } else {
                Err(BasisLoadError::CheckpointLoad(
                    CheckpointLoadError::ManifestSeqMismatch {
                        object_key: manifest_key,
                        expected: manifest.payload.checkpoint_seq,
                        actual: existing.manifest.payload.checkpoint_seq,
                    },
                ))
            }
        }
        Err(error) => Err(BasisLoadError::CheckpointLoad(
            CheckpointLoadError::ReadManifest {
                object_key: manifest_key,
                message: error.to_string(),
            },
        )),
    }
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "publish_compacted_head", key_class = "namespace_head")
)]
fn publish_checkpoint_hint_seq<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    writer_version: &str,
) -> Result<HeadState, CoreError> {
    for _attempt in 0..HEAD_UPDATE_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let current_head = loaded_head.envelope.state;
        if current_head.checkpoint_hint_seq >= Some(checkpoint_seq) {
            return Ok(current_head);
        }

        let next_head = HeadState {
            namespace_id: current_head.namespace_id.clone(),
            seq: current_head.seq,
            active_fence_token: current_head.active_fence_token,
            next_inode_id: current_head.next_inode_id,
            name_policy: current_head.name_policy,
            checkpoint_hint_seq: Some(checkpoint_seq),
            retention_floor_seq: current_head.retention_floor_seq,
            visible_wal_tip: current_head.visible_wal_tip.clone(),
        };
        match compare_and_swap_head(
            store,
            &loaded_head.object_key,
            loaded_head.metadata.etag.as_deref(),
            writer_version,
            &next_head,
        ) {
            Ok(()) => return Ok(next_head),
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(error) => return Err(CoreError::Store(error.to_string())),
        }
    }

    Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
}

fn compare_and_swap_head<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_head_etag: Option<&str>,
    writer_version: &str,
    next_head: &HeadState,
) -> Result<(), ObjectStoreError> {
    let expected_head_etag = expected_head_etag.ok_or_else(|| {
        ObjectStoreError::Transport(format!("missing head etag for `{object_key}`"))
    })?;
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        writer_version,
        next_head.clone(),
    )
    .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
    let encoded = serde_json::to_vec(&envelope)
        .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
    store
        .compare_and_swap(object_key, expected_head_etag, &encoded)
        .map(|_| ())
}

fn ensure_required_retention_progress<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    target_floor: ChangeSeq,
) -> Result<(), CoreError> {
    for work_class in REQUIRED_RETENTION_PROGRESS_CLASSES {
        let object_key = derived_progress(namespace_id.as_str(), *work_class);
        let work_class_name = work_class.as_str();
        let Some(bytes) = store
            .get(&object_key, None)
            .map_err(|err| CoreError::Store(err.to_string()))?
        else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class_name}` is missing for namespace `{}`",
                namespace_id.as_str()
            )));
        };
        let progress: ProgressStateEnvelope =
            serde_json::from_slice(&bytes).map_err(|err| CoreError::Store(err.to_string()))?;
        if progress.state.through_seq < target_floor {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class_name}` only covers {:?} for namespace `{}`",
                progress.state.through_seq,
                namespace_id.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_checkpoint_manifest(
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    object_key: &str,
    manifest: &CheckpointManifestEnvelope,
) -> Result<(), CheckpointLoadError> {
    if manifest.payload.namespace_id != *namespace_id {
        return Err(CheckpointLoadError::ManifestNamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: namespace_id.clone(),
            actual: manifest.payload.namespace_id.clone(),
        });
    }
    if manifest.payload.checkpoint_seq != checkpoint_seq {
        return Err(CheckpointLoadError::ManifestSeqMismatch {
            object_key: object_key.to_owned(),
            expected: checkpoint_seq,
            actual: manifest.payload.checkpoint_seq,
        });
    }
    if !manifest.payload.verified {
        return Err(CheckpointLoadError::ManifestNotVerified {
            object_key: object_key.to_owned(),
        });
    }
    Ok(())
}

fn validate_checkpoint_materialization_ranges(
    object_key: &str,
    payload: &CheckpointManifestPayload,
) -> Result<(), CheckpointLoadError> {
    if payload.base_seq > payload.checkpoint_seq {
        return Err(CheckpointLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "base_seq {:?} is after checkpoint_seq {:?}",
                payload.base_seq, payload.checkpoint_seq
            ),
        });
    }

    if payload.runs.is_empty() {
        return Err(CheckpointLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "checkpoint manifest must reference at least one run".to_owned(),
        });
    }

    let mut saw_base_seq_run = false;
    let mut saw_checkpoint_seq_run = false;
    let mut seen_run_ids = Vec::new();
    for run in &payload.runs {
        validate_checkpoint_run_id(&run.run_id).map_err(|error| {
            CheckpointLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: error.to_string(),
            }
        })?;
        if seen_run_ids.contains(&run.run_id.as_str()) {
            return Err(CheckpointLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate checkpoint run id `{}`", run.run_id),
            });
        }
        seen_run_ids.push(run.run_id.as_str());
        if run.run_seq < payload.base_seq || run.run_seq > payload.checkpoint_seq {
            return Err(CheckpointLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "checkpoint run `{}` seq {:?} is outside [{:?}, {:?}]",
                    run.run_id, run.run_seq, payload.base_seq, payload.checkpoint_seq
                ),
            });
        }
        saw_base_seq_run |= run.run_seq == payload.base_seq;
        saw_checkpoint_seq_run |= run.run_seq == payload.checkpoint_seq;
    }

    if !saw_base_seq_run {
        return Err(CheckpointLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "checkpoint manifest has no run at base_seq {:?}",
                payload.base_seq
            ),
        });
    }
    if !saw_checkpoint_seq_run {
        return Err(CheckpointLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "checkpoint manifest has no run at checkpoint_seq {:?}",
                payload.checkpoint_seq
            ),
        });
    }

    Ok(())
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "load_checkpoint_tables", key_class = "checkpoint_table")
)]
fn load_checkpoint_materialization_from_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_key: &str,
    manifest: &CheckpointManifestEnvelope,
) -> Result<MetadataState, CheckpointLoadError> {
    let mut metadata_state = MetadataStateBuilder::default();
    validate_checkpoint_materialization_ranges(manifest_object_key, &manifest.payload)?;
    for run in runs_in_materialization_order(&manifest.payload) {
        append_checkpoint_tables_to_metadata(
            store,
            namespace_id,
            CheckpointTableLoadContext {
                manifest_object_key,
                segment_seq_expectation: CheckpointSegmentSeqExpectation::Descriptor,
                row_seq_min: None,
                row_seq_max: run.run_seq,
            },
            &run.tables,
            &mut metadata_state,
            |family, segment_seq, segment_index| {
                checkpoint_run_table(
                    namespace_id.as_str(),
                    segment_seq.0,
                    &run.run_id,
                    checkpoint_table_family(family),
                    segment_index,
                )
            },
        )?;
    }

    Ok(metadata_state.finish())
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "load_checkpoint_tables", key_class = "checkpoint_table")
)]
fn load_checkpoint_materialization_from_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_key: &str,
    run_id: &str,
    segment_seq: ChangeSeq,
    tables: &[CheckpointTableManifest],
) -> Result<MetadataState, CheckpointLoadError> {
    let mut metadata_state = MetadataStateBuilder::default();
    append_checkpoint_tables_to_metadata(
        store,
        namespace_id,
        CheckpointTableLoadContext {
            manifest_object_key,
            segment_seq_expectation: CheckpointSegmentSeqExpectation::Exact(segment_seq),
            row_seq_min: None,
            row_seq_max: segment_seq,
        },
        tables,
        &mut metadata_state,
        |family, segment_seq, segment_index| {
            checkpoint_run_table(
                namespace_id.as_str(),
                segment_seq.0,
                run_id,
                checkpoint_table_family(family),
                segment_index,
            )
        },
    )?;
    Ok(metadata_state.finish())
}

#[derive(Clone, Copy)]
struct CheckpointTableLoadContext<'a> {
    manifest_object_key: &'a str,
    segment_seq_expectation: CheckpointSegmentSeqExpectation,
    row_seq_min: Option<ChangeSeq>,
    row_seq_max: ChangeSeq,
}

#[derive(Clone, Copy)]
enum CheckpointSegmentSeqExpectation {
    Exact(ChangeSeq),
    Descriptor,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataTableCacheMode {
    Bypass,
    Populate,
    ReadOnly,
}

impl CheckpointTableLoadContext<'_> {
    fn expected_segment_seq(
        &self,
        descriptor: &CheckpointSegmentDescriptor,
    ) -> Result<ChangeSeq, CheckpointLoadError> {
        match self.segment_seq_expectation {
            CheckpointSegmentSeqExpectation::Exact(expected) => {
                if descriptor.segment_seq != expected {
                    return Err(CheckpointLoadError::SegmentSeqMismatch {
                        object_key: descriptor.object_key.clone(),
                        expected,
                        actual: descriptor.segment_seq,
                    });
                }
                Ok(expected)
            }
            CheckpointSegmentSeqExpectation::Descriptor => {
                if descriptor.segment_seq > self.row_seq_max {
                    return Err(CheckpointLoadError::SegmentSeqMismatch {
                        object_key: descriptor.object_key.clone(),
                        expected: self.row_seq_max,
                        actual: descriptor.segment_seq,
                    });
                }
                Ok(descriptor.segment_seq)
            }
        }
    }

    fn row_seq_max(&self, descriptor: &CheckpointSegmentDescriptor) -> ChangeSeq {
        match self.segment_seq_expectation {
            CheckpointSegmentSeqExpectation::Exact(_) => self.row_seq_max,
            CheckpointSegmentSeqExpectation::Descriptor => descriptor.segment_seq,
        }
    }
}

fn append_checkpoint_tables_to_metadata<S, ExpectedObjectKey>(
    store: &S,
    namespace_id: &NamespaceId,
    context: CheckpointTableLoadContext<'_>,
    tables: &[CheckpointTableManifest],
    metadata_state: &mut MetadataStateBuilder,
    mut expected_object_key: ExpectedObjectKey,
) -> Result<(), CheckpointLoadError>
where
    S: ObjectStore + ?Sized,
    ExpectedObjectKey: FnMut(CheckpointTableFamily, ChangeSeq, u32) -> String,
{
    let ordered_tables = ordered_checkpoint_tables(context.manifest_object_key, tables)?;
    let mut direntry_bind_rows = Vec::new();
    let mut direntry_child_bind_rows = Vec::new();
    for table in ordered_tables {
        for descriptor in &table.segments {
            let expected_segment_seq = context.expected_segment_seq(descriptor)?;
            let expected_key =
                expected_object_key(table.family, expected_segment_seq, descriptor.segment_index);
            if descriptor.object_key != expected_key {
                return Err(CheckpointLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            let rows = load_checkpoint_segment_rows(
                store,
                namespace_id,
                context,
                table.family,
                descriptor,
            )?;
            match table.family {
                CheckpointTableFamily::DirentryBinds => {
                    direntry_bind_rows.extend(rows.iter().cloned());
                }
                CheckpointTableFamily::DirentryChildBinds => {
                    direntry_child_bind_rows.extend(rows.iter().cloned());
                }
                _ => {}
            }
            append_rows_to_metadata(metadata_state, table.family, &descriptor.object_key, &rows)?;
        }
    }

    validate_direntry_child_bind_index(
        context.manifest_object_key,
        direntry_bind_rows,
        direntry_child_bind_rows,
    )
}

fn load_checkpoint_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: CheckpointTableLoadContext<'_>,
    family: CheckpointTableFamily,
    descriptor: &CheckpointSegmentDescriptor,
) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
    load_checkpoint_segment_rows_with_cache(
        store,
        None,
        namespace_id,
        context,
        family,
        descriptor,
        MetadataTableCacheMode::Bypass,
    )
}

fn load_checkpoint_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    namespace_id: &NamespaceId,
    context: CheckpointTableLoadContext<'_>,
    family: CheckpointTableFamily,
    descriptor: &CheckpointSegmentDescriptor,
    cache_mode: MetadataTableCacheMode,
) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
    let expected_segment_seq = context.expected_segment_seq(descriptor)?;
    let cache_key = MetadataTableCacheKey {
        table_digest: descriptor.payload_checksum_sha256.clone(),
        block_kind: MetadataTableBlockKind::SegmentPayload,
        block_offset: 0,
    };
    if cache_mode != MetadataTableCacheMode::Bypass {
        if let Some(block) = table_cache.and_then(|cache| cache.get(&cache_key)) {
            validate_cached_checkpoint_block(family, expected_segment_seq, descriptor, &block)?;
            validate_checkpoint_row_seq_range(
                &descriptor.object_key,
                &block.rows,
                context.row_seq_min,
                context.row_seq_max(descriptor),
            )?;
            return Ok(block.rows);
        }
    }

    let Some(bytes) = store.get(&descriptor.object_key, None).map_err(|err| {
        CheckpointLoadError::ReadSegment {
            object_key: descriptor.object_key.clone(),
            message: err.to_string(),
        }
    })?
    else {
        return Err(CheckpointLoadError::MissingSegment {
            object_key: descriptor.object_key.clone(),
        });
    };
    let segment = decode_checkpoint_segment_envelope_zstd(&bytes).map_err(|err| {
        CheckpointLoadError::SegmentCodec {
            object_key: descriptor.object_key.clone(),
            message: err.to_string(),
        }
    })?;
    let rows = validate_checkpoint_segment(
        namespace_id,
        expected_segment_seq,
        family,
        descriptor,
        &segment,
    )?;
    validate_checkpoint_row_seq_range(
        &descriptor.object_key,
        &rows,
        context.row_seq_min,
        context.row_seq_max(descriptor),
    )?;
    if cache_mode == MetadataTableCacheMode::Populate {
        if let Some(cache) = table_cache {
            cache.insert(
                cache_key,
                DecodedMetadataTableBlock {
                    rows: rows.clone(),
                    segment_seq: expected_segment_seq,
                    family,
                    segment_index: descriptor.segment_index,
                    segment_key: descriptor.segment_key.clone(),
                    row_count: descriptor.row_count,
                    min_key: descriptor.min_key.clone(),
                    max_key: descriptor.max_key.clone(),
                    page_checksums_sha256: descriptor.page_checksums_sha256.clone(),
                    decoded_byte_len: decoded_checkpoint_block_weight(family, &rows),
                },
            );
        }
    }
    Ok(rows)
}

fn validate_cached_checkpoint_block(
    family: CheckpointTableFamily,
    expected_segment_seq: ChangeSeq,
    descriptor: &CheckpointSegmentDescriptor,
    block: &DecodedMetadataTableBlock,
) -> Result<(), CheckpointLoadError> {
    if block.segment_seq != expected_segment_seq {
        return Err(CheckpointLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: expected_segment_seq,
            actual: block.segment_seq,
        });
    }
    if block.family != family {
        return Err(CheckpointLoadError::SegmentFamilyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: family,
            actual: block.family,
        });
    }
    if block.segment_index != descriptor.segment_index {
        return Err(CheckpointLoadError::SegmentIndexMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_index,
            actual: block.segment_index,
        });
    }
    if block.segment_key != descriptor.segment_key {
        return Err(CheckpointLoadError::SegmentKeyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_key.clone(),
            actual: block.segment_key.clone(),
        });
    }
    if descriptor.row_count != block.row_count
        || descriptor.min_key != block.min_key
        || descriptor.max_key != block.max_key
        || descriptor.page_checksums_sha256 != block.page_checksums_sha256
    {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "cached segment descriptor mismatch".to_owned(),
        });
    }
    Ok(())
}

fn decoded_checkpoint_block_weight(family: CheckpointTableFamily, rows: &[CheckpointRow]) -> usize {
    let row_weight = rows
        .iter()
        .map(|row| 64 + row.row_key_for_family(family).len() + decoded_checkpoint_row_weight(row))
        .sum::<usize>();
    row_weight.saturating_add(128)
}

fn decoded_checkpoint_row_weight(row: &CheckpointRow) -> usize {
    match row {
        CheckpointRow::Inode { .. } => 32,
        CheckpointRow::DirentryBind {
            name_key,
            display_name,
            ..
        } => 96 + name_key.len() + display_name.len(),
        CheckpointRow::DirentryUnbind { name_key, .. } => 96 + name_key.len(),
        CheckpointRow::Revision { content_ref, .. } => 96 + content_ref.digest.len(),
        CheckpointRow::Tombstone { .. } => 32,
        CheckpointRow::CommitReceipt {
            commit_id,
            semantic_commit_fingerprint_sha256,
            results,
            ..
        } => {
            96 + commit_id.as_str().len()
                + semantic_commit_fingerprint_sha256.len()
                + results.len().saturating_mul(64)
        }
    }
}

fn ordered_checkpoint_tables<'a>(
    manifest_object_key: &str,
    tables: &'a [CheckpointTableManifest],
) -> Result<Vec<&'a CheckpointTableManifest>, CheckpointLoadError> {
    let mut ordered = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut matching = tables.iter().filter(|table| table.family == family);
        let Some(table) = matching.next() else {
            return Err(CheckpointLoadError::MissingTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        };
        if matching.next().is_some() {
            return Err(CheckpointLoadError::DuplicateTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        }
        ordered.push(table);
    }
    Ok(ordered)
}

fn checkpoint_table_for_family<'a>(
    manifest_object_key: &str,
    tables: &'a [CheckpointTableManifest],
    family: CheckpointTableFamily,
) -> Result<&'a CheckpointTableManifest, CheckpointLoadError> {
    ordered_checkpoint_tables(manifest_object_key, tables)?
        .into_iter()
        .find(|table| table.family == family)
        .ok_or(CheckpointLoadError::MissingTableFamily {
            object_key: manifest_object_key.to_owned(),
            family,
        })
}

fn count_matching_segments(
    manifest_object_key: &str,
    tables: &[CheckpointTableManifest],
    family: CheckpointTableFamily,
    prefix: &str,
) -> Result<usize, CheckpointLoadError> {
    let table = checkpoint_table_for_family(manifest_object_key, tables, family)?;
    Ok(table
        .segments
        .iter()
        .filter(|descriptor| descriptor_may_contain_prefix(descriptor, prefix))
        .count())
}

fn descriptor_may_contain_prefix(descriptor: &CheckpointSegmentDescriptor, prefix: &str) -> bool {
    if descriptor.row_count == 0 {
        return false;
    }
    if prefix.is_empty() {
        return true;
    }
    if descriptor.max_key.as_str() < prefix {
        return false;
    }
    if let Some(upper_bound) = string_prefix_upper_bound(prefix) {
        if descriptor.min_key >= upper_bound {
            return false;
        }
    }
    true
}

fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        if bytes[index] != u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn validate_checkpoint_segment(
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    family: CheckpointTableFamily,
    descriptor: &CheckpointSegmentDescriptor,
    segment: &CheckpointSegmentEnvelope,
) -> Result<Vec<CheckpointRow>, CheckpointLoadError> {
    if segment.payload.namespace_id != *namespace_id {
        return Err(CheckpointLoadError::SegmentNamespaceMismatch {
            object_key: descriptor.object_key.clone(),
            expected: namespace_id.clone(),
            actual: segment.payload.namespace_id.clone(),
        });
    }
    if segment.payload.segment_seq != checkpoint_seq {
        return Err(CheckpointLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: checkpoint_seq,
            actual: segment.payload.segment_seq,
        });
    }
    if segment.payload.family != family {
        return Err(CheckpointLoadError::SegmentFamilyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: family,
            actual: segment.payload.family,
        });
    }
    if segment.payload.segment_index != descriptor.segment_index {
        return Err(CheckpointLoadError::SegmentIndexMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_index,
            actual: segment.payload.segment_index,
        });
    }
    if segment.payload.segment_key != descriptor.segment_key {
        return Err(CheckpointLoadError::SegmentKeyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_key.clone(),
            actual: segment.payload.segment_key.clone(),
        });
    }

    let actual_payload_checksum = checkpoint_segment_payload_checksum_sha256(&segment.payload)
        .map_err(|err| CheckpointLoadError::SegmentCodec {
            object_key: descriptor.object_key.clone(),
            message: err.to_string(),
        })?;
    if descriptor.payload_checksum_sha256 != actual_payload_checksum {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "payload checksum mismatch: expected `{}`, actual `{}`",
                descriptor.payload_checksum_sha256, actual_payload_checksum
            ),
        });
    }

    let mut collected_rows = Vec::new();
    let mut collected_page_checksums = Vec::new();
    for page in &segment.payload.pages {
        let checksum = checkpoint_page_checksum_sha256(page).map_err(|err| {
            CheckpointLoadError::SegmentCodec {
                object_key: descriptor.object_key.clone(),
                message: err.to_string(),
            }
        })?;
        collected_page_checksums.push(checksum.clone());
        validate_checkpoint_page(family, descriptor, page, &checksum)?;
        collected_rows.extend(page.rows.iter().cloned());
    }

    if descriptor.page_checksums_sha256 != collected_page_checksums {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "page checksum descriptor mismatch".to_owned(),
        });
    }

    if segment.payload.row_count != collected_rows.len() as u64 {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "row count mismatch: expected {}, actual {}",
                segment.payload.row_count,
                collected_rows.len()
            ),
        });
    }
    let row_keys = collected_rows
        .iter()
        .map(|row| row.row_key_for_family(family))
        .collect::<Vec<_>>();
    if let (Some(first), Some(last)) = (row_keys.first(), row_keys.last()) {
        if segment.payload.min_key != *first || segment.payload.max_key != *last {
            return Err(CheckpointLoadError::SegmentDescriptorMismatch {
                object_key: descriptor.object_key.clone(),
                message: "payload min/max key mismatch".to_owned(),
            });
        }
    } else if segment.payload.row_count != 0 {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "non-zero row count with no rows".to_owned(),
        });
    }

    if descriptor.row_count != segment.payload.row_count
        || descriptor.min_key != segment.payload.min_key
        || descriptor.max_key != segment.payload.max_key
    {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "descriptor row summary mismatch".to_owned(),
        });
    }

    Ok(collected_rows)
}

fn validate_checkpoint_page(
    family: CheckpointTableFamily,
    descriptor: &CheckpointSegmentDescriptor,
    page: &CheckpointPage,
    checksum: &str,
) -> Result<(), CheckpointLoadError> {
    if page.row_keys.len() != page.rows.len() {
        return Err(CheckpointLoadError::PageShapeMismatch {
            object_key: descriptor.object_key.clone(),
            page_index: page.page_index,
            message: format!(
                "row_keys length {} does not match rows length {}",
                page.row_keys.len(),
                page.rows.len()
            ),
        });
    }

    let expected_checksum = descriptor
        .page_checksums_sha256
        .get(page.page_index as usize)
        .cloned()
        .unwrap_or_default();
    if expected_checksum != *checksum {
        return Err(CheckpointLoadError::PageChecksumMismatch {
            object_key: descriptor.object_key.clone(),
            page_index: page.page_index,
            expected: expected_checksum,
            actual: checksum.to_owned(),
        });
    }

    for (index, row) in page.rows.iter().enumerate() {
        if !checkpoint_row_matches_family(row, family) {
            return Err(CheckpointLoadError::TableRowKindMismatch {
                object_key: descriptor.object_key.clone(),
                family,
                row_kind: checkpoint_row_kind(row).to_owned(),
            });
        }
        let actual = row.row_key_for_family(family);
        let expected = page.row_keys.get(index).cloned().unwrap_or_default();
        if actual != expected {
            return Err(CheckpointLoadError::RowKeyMismatch {
                object_key: descriptor.object_key.clone(),
                page_index: page.page_index,
                row_index: index,
                expected,
                actual,
            });
        }
    }

    if let (Some(first), Some(last)) = (page.row_keys.first(), page.row_keys.last()) {
        if page.min_key != *first || page.max_key != *last {
            return Err(CheckpointLoadError::PageShapeMismatch {
                object_key: descriptor.object_key.clone(),
                page_index: page.page_index,
                message: "page min/max key mismatch".to_owned(),
            });
        }
    }

    Ok(())
}

fn validate_checkpoint_row_seq_range(
    object_key: &str,
    rows: &[CheckpointRow],
    min_seq: Option<ChangeSeq>,
    max_seq: ChangeSeq,
) -> Result<(), CheckpointLoadError> {
    for (row_index, row) in rows.iter().enumerate() {
        let row_seq = checkpoint_row_commit_seq(row);
        if let Some(min_seq) = min_seq {
            if row_seq < min_seq {
                return Err(CheckpointLoadError::SegmentDescriptorMismatch {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "row {row_index} seq `{row_seq:?}` is before expected min `{min_seq:?}`"
                    ),
                });
            }
        }
        if row_seq > max_seq {
            return Err(CheckpointLoadError::SegmentDescriptorMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "row {row_index} seq `{row_seq:?}` is after expected max `{max_seq:?}`"
                ),
            });
        }
    }
    Ok(())
}

fn append_rows_to_metadata(
    metadata_state: &mut MetadataStateBuilder,
    family: CheckpointTableFamily,
    object_key: &str,
    rows: &[CheckpointRow],
) -> Result<(), CheckpointLoadError> {
    for row in rows {
        match (family, row) {
            (
                CheckpointTableFamily::Inodes,
                CheckpointRow::Inode {
                    inode_id,
                    inode_kind,
                    created_seq,
                },
            ) => metadata_state.push_inode(InodeRecord {
                inode_id: *inode_id,
                inode_kind: inode_kind.clone(),
                created_seq: *created_seq,
            }),
            (
                CheckpointTableFamily::DirentryBinds,
                CheckpointRow::DirentryBind {
                    parent_inode_id,
                    name_key,
                    display_name,
                    child_inode_id,
                    bind_seq,
                    bind_delta_index,
                },
            ) => metadata_state.push_direntry_bind(DirentryBindRecord {
                parent_inode_id: *parent_inode_id,
                name_key: name_key.clone(),
                display_name: display_name.clone(),
                child_inode_id: *child_inode_id,
                bind_seq: *bind_seq,
                bind_delta_index: *bind_delta_index,
            }),
            (CheckpointTableFamily::DirentryChildBinds, CheckpointRow::DirentryBind { .. }) => {}
            (
                CheckpointTableFamily::DirentryUnbinds,
                CheckpointRow::DirentryUnbind {
                    parent_inode_id,
                    name_key,
                    child_inode_id,
                    bind_seq,
                    bind_delta_index,
                    unbind_seq,
                    unbind_delta_index,
                },
            ) => metadata_state.push_direntry_unbind(DirentryUnbindRecord {
                parent_inode_id: *parent_inode_id,
                name_key: name_key.clone(),
                child_inode_id: *child_inode_id,
                bind_seq: *bind_seq,
                bind_delta_index: *bind_delta_index,
                unbind_seq: *unbind_seq,
                unbind_delta_index: *unbind_delta_index,
            }),
            (
                CheckpointTableFamily::Revisions,
                CheckpointRow::Revision {
                    inode_id,
                    revision_no,
                    committed_seq,
                    revision_delta_index,
                    content_ref,
                },
            ) => metadata_state.push_revision(RevisionRecord {
                inode_id: *inode_id,
                revision_no: *revision_no,
                committed_seq: *committed_seq,
                revision_delta_index: *revision_delta_index,
                content_ref: content_ref.clone(),
            }),
            (
                CheckpointTableFamily::Tombstones,
                CheckpointRow::Tombstone {
                    root_inode_id,
                    tombstone_seq,
                    tombstone_delta_index,
                },
            ) => metadata_state.push_subtree_tombstone(SubtreeTombstoneRecord {
                root_inode_id: *root_inode_id,
                tombstone_seq: *tombstone_seq,
                tombstone_delta_index: *tombstone_delta_index,
            }),
            (
                CheckpointTableFamily::CommitReceipts,
                CheckpointRow::CommitReceipt {
                    commit_id,
                    semantic_commit_fingerprint_sha256,
                    committed_seq,
                    results,
                },
            ) => metadata_state.push_commit_receipt(CommitReceiptRecord {
                commit_id: commit_id.clone(),
                semantic_commit_fingerprint_sha256: semantic_commit_fingerprint_sha256.clone(),
                committed_seq: *committed_seq,
                results: results.clone(),
            }),
            _ => {
                return Err(CheckpointLoadError::TableRowKindMismatch {
                    object_key: object_key.to_owned(),
                    family,
                    row_kind: checkpoint_row_kind(row).to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn validate_direntry_child_bind_index(
    object_key: &str,
    mut direntry_bind_rows: Vec<CheckpointRow>,
    mut direntry_child_bind_rows: Vec<CheckpointRow>,
) -> Result<(), CheckpointLoadError> {
    direntry_bind_rows
        .sort_by_key(|row| row.row_key_for_family(CheckpointTableFamily::DirentryChildBinds));
    direntry_child_bind_rows
        .sort_by_key(|row| row.row_key_for_family(CheckpointTableFamily::DirentryChildBinds));

    if direntry_bind_rows != direntry_child_bind_rows {
        return Err(CheckpointLoadError::SegmentDescriptorMismatch {
            object_key: object_key.to_owned(),
            message: "direntry-child-binds index does not match canonical direntry-binds"
                .to_owned(),
        });
    }

    Ok(())
}

fn metadata_states_equivalent(left: &MetadataState, right: &MetadataState) -> bool {
    CHECKPOINT_TABLE_FAMILIES.into_iter().all(|family| {
        checkpoint_rows_for_family(left, family) == checkpoint_rows_for_family(right, family)
    })
}

fn checkpoint_rows_for_family(
    metadata_state: &MetadataState,
    family: CheckpointTableFamily,
) -> Vec<CheckpointRow> {
    let mut rows = match family {
        CheckpointTableFamily::Inodes => metadata_state
            .inodes()
            .iter()
            .map(|inode| CheckpointRow::Inode {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::DirentryBinds | CheckpointTableFamily::DirentryChildBinds => {
            metadata_state
                .direntry_binds()
                .iter()
                .map(|direntry| CheckpointRow::DirentryBind {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                    child_inode_id: direntry.child_inode_id,
                    bind_seq: direntry.bind_seq,
                    bind_delta_index: direntry.bind_delta_index,
                })
                .collect::<Vec<_>>()
        }
        CheckpointTableFamily::DirentryUnbinds => metadata_state
            .direntry_unbinds()
            .iter()
            .map(|unbind| CheckpointRow::DirentryUnbind {
                parent_inode_id: unbind.parent_inode_id,
                name_key: unbind.name_key.clone(),
                child_inode_id: unbind.child_inode_id,
                bind_seq: unbind.bind_seq,
                bind_delta_index: unbind.bind_delta_index,
                unbind_seq: unbind.unbind_seq,
                unbind_delta_index: unbind.unbind_delta_index,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Revisions => metadata_state
            .revisions()
            .iter()
            .map(|revision| CheckpointRow::Revision {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                revision_delta_index: revision.revision_delta_index,
                content_ref: revision.content_ref.clone(),
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Tombstones => metadata_state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| CheckpointRow::Tombstone {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_delta_index: tombstone.tombstone_delta_index,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::CommitReceipts => metadata_state
            .commit_receipts()
            .iter()
            .map(|record| CheckpointRow::CommitReceipt {
                commit_id: record.commit_id.clone(),
                semantic_commit_fingerprint_sha256: record
                    .semantic_commit_fingerprint_sha256
                    .clone(),
                committed_seq: record.committed_seq,
                results: record.results.clone(),
            })
            .collect::<Vec<_>>(),
    };
    rows.sort_by_key(|row| row.row_key_for_family(family));
    rows
}

fn checkpoint_rows_for_family_after_seq(
    metadata_state: &MetadataState,
    family: CheckpointTableFamily,
    after_seq: ChangeSeq,
) -> Vec<CheckpointRow> {
    checkpoint_rows_for_family(metadata_state, family)
        .into_iter()
        .filter(|row| checkpoint_row_commit_seq(row) > after_seq)
        .collect()
}

fn checkpoint_row_commit_seq(row: &CheckpointRow) -> ChangeSeq {
    match row {
        CheckpointRow::Inode { created_seq, .. } => *created_seq,
        CheckpointRow::DirentryBind { bind_seq, .. } => *bind_seq,
        CheckpointRow::DirentryUnbind { unbind_seq, .. } => *unbind_seq,
        CheckpointRow::Revision { committed_seq, .. } => *committed_seq,
        CheckpointRow::Tombstone { tombstone_seq, .. } => *tombstone_seq,
        CheckpointRow::CommitReceipt { committed_seq, .. } => *committed_seq,
    }
}

fn checkpoint_table_family(family: CheckpointTableFamily) -> ObjectStoreCheckpointTableFamily {
    match family {
        CheckpointTableFamily::Inodes => ObjectStoreCheckpointTableFamily::Inodes,
        CheckpointTableFamily::DirentryBinds => ObjectStoreCheckpointTableFamily::DirentryBinds,
        CheckpointTableFamily::DirentryChildBinds => {
            ObjectStoreCheckpointTableFamily::DirentryChildBinds
        }
        CheckpointTableFamily::DirentryUnbinds => ObjectStoreCheckpointTableFamily::DirentryUnbinds,
        CheckpointTableFamily::Revisions => ObjectStoreCheckpointTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => ObjectStoreCheckpointTableFamily::Tombstones,
        CheckpointTableFamily::CommitReceipts => ObjectStoreCheckpointTableFamily::CommitReceipts,
    }
}

fn checkpoint_row_kind(row: &CheckpointRow) -> &'static str {
    match row {
        CheckpointRow::Inode { .. } => "inode",
        CheckpointRow::DirentryBind { .. } => "direntry_bind",
        CheckpointRow::DirentryUnbind { .. } => "direntry_unbind",
        CheckpointRow::Revision { .. } => "revision",
        CheckpointRow::Tombstone { .. } => "tombstone",
        CheckpointRow::CommitReceipt { .. } => "commit_receipt",
    }
}

fn checkpoint_row_matches_family(row: &CheckpointRow, family: CheckpointTableFamily) -> bool {
    matches!(
        (family, row),
        (CheckpointTableFamily::Inodes, CheckpointRow::Inode { .. })
            | (
                CheckpointTableFamily::DirentryBinds,
                CheckpointRow::DirentryBind { .. }
            )
            | (
                CheckpointTableFamily::DirentryChildBinds,
                CheckpointRow::DirentryBind { .. }
            )
            | (
                CheckpointTableFamily::DirentryUnbinds,
                CheckpointRow::DirentryUnbind { .. }
            )
            | (
                CheckpointTableFamily::Revisions,
                CheckpointRow::Revision { .. }
            )
            | (
                CheckpointTableFamily::Tombstones,
                CheckpointRow::Tombstone { .. }
            )
            | (
                CheckpointTableFamily::CommitReceipts,
                CheckpointRow::CommitReceipt { .. }
            )
    )
}

#[cfg(test)]
mod tests {
    use super::{
        advance_retention_floor, build_checkpoint_manifest_for_basis, build_checkpoint_tables,
        build_checkpoint_tables_from_rows, checkpoint_basis_head, checkpoint_rows_for_family,
        checkpoint_table_family, create_checkpoint, create_checkpoint_with_policy,
        load_checkpoint_materialization_from_manifest, load_verified_checkpoint_materialization,
        metadata_states_equivalent, publish_checkpoint_hint_seq, write_checkpoint_manifest,
        CheckpointLoadError, CheckpointTableSegmentation, MetadataLsmPolicy, MetadataTableCache,
        MetadataTableCacheConfig, CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
        CHECKPOINT_TABLE_FAMILIES, DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT, MAX_CHECKPOINT_L0_RUNS,
    };
    use crate::basis::{load_verified_namespace_basis, BasisLoadError};
    use crate::metadata::MetadataState;
    use crate::namespace::lifecycle::bootstrap_namespace;
    use crate::path::mutation::{move_path, put_file_bytes, write_file_bytes};
    use crate::{CoreError, CoreErrorKind, MutationContext, PutFileBehavior};
    use loon_api::wire::checkpoint::{
        encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd,
        CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointPage, CheckpointRow,
        CheckpointRunManifest, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
        CheckpointSegmentKey, CheckpointSegmentPayload,
        CheckpointTableFamily as ApiCheckpointTableFamily,
    };
    use loon_api::{generate_checkpoint_run_id, ChangeSeq, NamespaceId};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{
        checkpoint_manifest, checkpoint_run_table, namespace_head, CheckpointTableFamily,
    };
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn checkpoint_round_trip_uses_checkpoint_basis_for_mixed_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/second.txt",
            b"second\n",
            &context,
            None,
        )
        .expect("write second");
        put_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello again\n",
            PutFileBehavior::ReplaceExisting,
            &context,
            None,
        )
        .expect("replace");

        let before = load_verified_namespace_basis(&store, &namespace_id).expect("basis before");
        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let after = load_verified_namespace_basis(&store, &namespace_id).expect("basis after");

        assert_eq!(after.head.checkpoint_hint_seq, Some(before.head.seq));
        assert_eq!(before.head.seq, after.head.seq);
        assert!(metadata_states_equivalent(
            &before.metadata_state,
            &after.metadata_state
        ));
    }

    #[test]
    fn checkpoint_round_trip_preserves_direntry_unbind_rows() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        move_path(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            "/docs/moved.txt",
            &context,
            None,
        )
        .expect("move hello");

        let before = load_verified_namespace_basis(&store, &namespace_id).expect("basis before");
        assert_eq!(before.metadata_state.direntry_unbinds().len(), 1);
        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let after = load_verified_namespace_basis(&store, &namespace_id).expect("basis after");

        assert_eq!(
            after.metadata_state.direntry_unbinds(),
            before.metadata_state.direntry_unbinds()
        );
        assert!(metadata_states_equivalent(
            &before.metadata_state,
            &after.metadata_state
        ));
    }

    #[test]
    fn checkpoint_round_trip_supports_empty_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(basis.head.checkpoint_hint_seq, Some(ChangeSeq(0)));
    }

    #[test]
    fn strict_checkpoint_consumption_fails_when_manifest_is_corrupted() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");

        let manifest_key = checkpoint_manifest(namespace_id.as_str(), 1);
        store
            .put_overwrite(&manifest_key, br#"{"bad":"json"}"#)
            .expect("corrupt manifest");

        match load_verified_namespace_basis(&store, &namespace_id) {
            Err(BasisLoadError::CheckpointLoad(CheckpointLoadError::ManifestCodec { .. })) => {}
            other => panic!("expected manifest codec checkpoint load error, got {other:?}"),
        }
    }

    #[test]
    fn create_checkpoint_surfaces_conflicting_invalid_manifest() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let manifest_key = checkpoint_manifest(namespace_id.as_str(), 1);
        let store = ConflictOnManifestCreateStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            manifest_key,
            br#"{"bad":"json"}"#.to_vec(),
        );
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");

        match create_checkpoint(&store, &namespace_id, &context) {
            Err(CoreError::Basis(BasisLoadError::CheckpointLoad(
                CheckpointLoadError::ManifestCodec { .. },
            ))) => {}
            other => panic!("expected manifest codec checkpoint load error, got {other:?}"),
        }

        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(basis.head.checkpoint_hint_seq, None);
    }

    #[test]
    fn retention_advancement_requires_published_checkpoint_and_updates_floor_only() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        match advance_retention_floor(&store, &namespace_id, &context) {
            Err(CoreError::CheckpointUnavailable(_)) => {}
            other => panic!("expected checkpoint unavailable, got {other:?}"),
        }

        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let advanced =
            advance_retention_floor(&store, &namespace_id, &context).expect("advance retention");
        assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));

        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(basis.head.retention_floor_seq, ChangeSeq(1));
        assert_eq!(
            store
                .list_prefix(&format!("namespaces/{}/wal/", namespace_id.as_str()))
                .expect("list wal")
                .len(),
            1
        );
        assert!(store
            .head(&checkpoint_manifest(namespace_id.as_str(), 1))
            .expect("manifest head")
            .is_some());
    }

    #[test]
    fn checkpoint_materialization_uses_written_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");

        let materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(1))
                .expect("load materialized checkpoint");
        let current = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let basis_head = checkpoint_basis_head(&current.head, &materialized.manifest);
        assert_eq!(basis_head.seq, ChangeSeq(1));
        assert!(metadata_states_equivalent(
            &materialized.metadata_state,
            &current.metadata_state
        ));

        let segment_key = base_segment_object_keys_for_family(
            &materialized.manifest,
            ApiCheckpointTableFamily::Revisions,
        )
        .into_iter()
        .next()
        .expect("revision segment");
        assert!(store.head(&segment_key).expect("head segment").is_some());
    }

    #[test]
    fn checkpoint_l0_run_materialization_matches_full_basis() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        let first = create_checkpoint(&store, &namespace_id, &context).expect("first checkpoint");
        let first_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first checkpoint");

        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/second.txt",
            b"second\n",
            &context,
            None,
        )
        .expect("write second");
        let second = create_checkpoint(&store, &namespace_id, &context).expect("second checkpoint");
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let second_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, second.checkpoint_seq)
                .expect("load second checkpoint");

        assert_eq!(
            second_materialized.manifest.payload.base_seq,
            first.checkpoint_seq
        );
        assert_eq!(
            base_run(&second_materialized.manifest).tables,
            base_run(&first_materialized.manifest).tables
        );
        let l0_runs = l0_runs(&second_materialized.manifest);
        assert_eq!(l0_runs.len(), 1);
        assert_eq!(l0_runs[0].run_seq, second.checkpoint_seq);
        assert_eq!(l0_runs[0].level, CHECKPOINT_L0_RUN_LEVEL);
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &second_materialized.metadata_state
        ));
    }

    #[test]
    fn checkpoint_l0_run_missing_table_fails_load() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        create_checkpoint(&store, &namespace_id, &context).expect("first checkpoint");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/second.txt",
            b"second\n",
            &context,
            None,
        )
        .expect("write second");
        let second = create_checkpoint(&store, &namespace_id, &context).expect("second checkpoint");
        let materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, second.checkpoint_seq)
                .expect("load materialized checkpoint");
        let deleted_key = l0_runs(&materialized.manifest)[0]
            .tables
            .iter()
            .flat_map(|table| table.segments.iter())
            .next()
            .expect("l0 run segment")
            .object_key
            .clone();
        store.delete(&deleted_key).expect("delete l0 segment");

        match load_verified_checkpoint_materialization(&store, &namespace_id, second.checkpoint_seq)
        {
            Err(CheckpointLoadError::MissingSegment { object_key }) => {
                assert_eq!(object_key, deleted_key);
            }
            other => panic!("expected missing l0 segment, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_materialization_rejects_off_pattern_table_keys() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        let first = write_file_and_checkpoint(&store, &namespace_id, &context, 1);
        let first_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, first)
                .expect("load first checkpoint");
        let manifest_key = checkpoint_manifest(namespace_id.as_str(), first.0);
        let mut bad_base_manifest = first_materialized.manifest.clone();
        let expected_base_key = {
            let base_run = bad_base_manifest
                .payload
                .runs
                .iter_mut()
                .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
                .expect("base run");
            let base_descriptor = base_run.tables[0]
                .segments
                .get_mut(0)
                .expect("base segment");
            base_descriptor.object_key = format!("{}-wrong", base_descriptor.object_key);
            checkpoint_run_table(
                namespace_id.as_str(),
                base_run.run_seq.0,
                &base_run.run_id,
                CheckpointTableFamily::Inodes,
                0,
            )
        };

        match load_checkpoint_materialization_from_manifest(
            &store,
            &namespace_id,
            &manifest_key,
            &bad_base_manifest,
        ) {
            Err(CheckpointLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
                assert_eq!(expected, expected_base_key);
            }
            other => panic!("expected base table key mismatch, got {other:?}"),
        }

        let second = write_file_and_checkpoint(&store, &namespace_id, &context, 2);
        let second_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, second)
                .expect("load second checkpoint");
        let manifest_key = checkpoint_manifest(namespace_id.as_str(), second.0);
        let mut bad_l0_manifest = second_materialized.manifest.clone();
        let expected_l0_key = {
            let l0_run = bad_l0_manifest
                .payload
                .runs
                .iter_mut()
                .find(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
                .expect("l0 run");
            let l0_descriptor = l0_run.tables[0].segments.get_mut(0).expect("l0 segment");
            l0_descriptor.object_key = format!("{}-wrong", l0_descriptor.object_key);
            checkpoint_run_table(
                namespace_id.as_str(),
                l0_run.run_seq.0,
                &l0_run.run_id,
                CheckpointTableFamily::Inodes,
                0,
            )
        };

        match load_checkpoint_materialization_from_manifest(
            &store,
            &namespace_id,
            &manifest_key,
            &bad_l0_manifest,
        ) {
            Err(CheckpointLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
                assert_eq!(expected, expected_l0_key);
            }
            other => panic!("expected l0 table key mismatch, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_run_rejects_rows_after_run_seq() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        let first = write_file_and_checkpoint(&store, &namespace_id, &context, 1);
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/file-2.txt",
            b"file 2\n",
            &context,
            None,
        )
        .expect("write second file");
        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let malformed_run_id = "run_00000000000000000000000000000001";
        let malformed_run_tables = build_checkpoint_tables_from_rows(
            &store,
            &namespace_id,
            first,
            &context.writer_version,
            |family| checkpoint_rows_for_family(&basis.metadata_state, family),
            CheckpointTableSegmentation::Full,
            |family, segment_index| {
                checkpoint_run_table(
                    namespace_id.as_str(),
                    first.0,
                    malformed_run_id,
                    checkpoint_table_family(family),
                    segment_index,
                )
            },
        )
        .expect("write malformed run tables");
        let checkpoint_run_id = "run_00000000000000000000000000000002";
        let checkpoint_run_tables = build_checkpoint_tables_from_rows(
            &store,
            &namespace_id,
            basis.head.seq,
            &context.writer_version,
            |_| Vec::new(),
            CheckpointTableSegmentation::Full,
            |family, segment_index| {
                checkpoint_run_table(
                    namespace_id.as_str(),
                    basis.head.seq.0,
                    checkpoint_run_id,
                    checkpoint_table_family(family),
                    segment_index,
                )
            },
        )
        .expect("write empty checkpoint run tables");
        let manifest = CheckpointManifestEnvelope::from_payload(
            &context.writer_version,
            CheckpointManifestPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis.head.seq,
                base_seq: first,
                active_fence_token: basis.head.active_fence_token,
                next_inode_id: basis.head.next_inode_id,
                retention_floor_seq: basis.head.retention_floor_seq,
                verified: true,
                runs: vec![
                    CheckpointRunManifest {
                        run_id: malformed_run_id.to_owned(),
                        run_seq: first,
                        level: CHECKPOINT_BASE_RUN_LEVEL,
                        tables: malformed_run_tables,
                    },
                    CheckpointRunManifest {
                        run_id: checkpoint_run_id.to_owned(),
                        run_seq: basis.head.seq,
                        level: CHECKPOINT_L0_RUN_LEVEL,
                        tables: checkpoint_run_tables,
                    },
                ],
            },
        )
        .expect("build malformed manifest");

        match load_checkpoint_materialization_from_manifest(
            &store,
            &namespace_id,
            &checkpoint_manifest(namespace_id.as_str(), basis.head.seq.0),
            &manifest,
        ) {
            Err(CheckpointLoadError::SegmentDescriptorMismatch { message, .. }) => {
                assert!(message.contains("after expected max"));
            }
            other => panic!("expected row range mismatch, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_l0_runs_chain_across_successive_checkpoints() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        for index in 1..=4 {
            write_file_and_checkpoint(&store, &namespace_id, &context, index);
        }

        let materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(4))
                .expect("load chained checkpoint");
        assert_eq!(materialized.manifest.payload.base_seq, ChangeSeq(1));
        let l0_runs = l0_runs(&materialized.manifest);
        assert_eq!(l0_runs.len(), 3);
        for (offset, run) in l0_runs.iter().enumerate() {
            let seq = ChangeSeq(offset as u64 + 2);
            assert_eq!(run.run_seq, seq);
            assert_eq!(run.level, CHECKPOINT_L0_RUN_LEVEL);
        }
    }

    #[test]
    fn metadata_lsm_policy_default_matches_l0_run_cap() {
        assert_eq!(
            MetadataLsmPolicy::default().max_l0_runs,
            MAX_CHECKPOINT_L0_RUNS
        );
        assert_eq!(
            MetadataLsmPolicy::default().max_rows_per_segment,
            DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT
        );
    }

    #[test]
    fn checkpoint_policy_compacts_when_l0_runs_exceed_threshold() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let policy = MetadataLsmPolicy {
            max_l0_runs: 2,
            ..MetadataLsmPolicy::default()
        };
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        for index in 1..=4 {
            write_file_and_checkpoint_with_policy(&store, &namespace_id, &context, index, policy);
        }

        let capped = load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(3))
            .expect("load capped checkpoint");
        assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
        assert_eq!(l0_runs(&capped.manifest).len(), 2);

        let compacted =
            load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(4))
                .expect("load compacted checkpoint");
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(compacted.manifest.payload.base_seq, ChangeSeq(4));
        assert!(l0_runs(&compacted.manifest).is_empty());
        assert_eq!(compacted.manifest.payload.runs.len(), 1);
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &compacted.metadata_state
        ));
    }

    #[test]
    fn checkpoint_rejects_segment_descriptor_payload_key_mismatch() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");

        let checkpoint = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load checkpoint");
        let mut manifest = materialized.manifest;
        let revisions = manifest
            .payload
            .runs
            .iter_mut()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
            .tables
            .iter_mut()
            .find(|table| table.family == ApiCheckpointTableFamily::Revisions)
            .expect("revision table");
        let descriptor = revisions.segments.first_mut().expect("revision segment");
        descriptor.segment_key = CheckpointSegmentKey::RowKeyRange { shard: u32::MAX };

        let manifest_key = checkpoint_manifest(namespace_id.as_str(), checkpoint.checkpoint_seq.0);
        let writer_version = manifest.writer_version.clone();
        let updated_manifest =
            CheckpointManifestEnvelope::from_payload(writer_version, manifest.payload)
                .expect("updated manifest");
        let manifest_bytes =
            encode_checkpoint_manifest_json(&updated_manifest).expect("encode manifest");
        store
            .put_overwrite(&manifest_key, &manifest_bytes)
            .expect("overwrite manifest");

        match load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        ) {
            Err(CheckpointLoadError::SegmentKeyMismatch { .. }) => {}
            other => panic!("expected segment key mismatch, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_base_run_tables_have_sorted_segment_coverage() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        for index in 0..6 {
            let path = format!("/docs/file-{index}.txt");
            write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
                .expect("write file");
        }
        let policy = MetadataLsmPolicy {
            max_rows_per_segment: 2,
            ..MetadataLsmPolicy::default()
        };

        let checkpoint = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("checkpoint");
        let materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load checkpoint");

        let revisions = materialized
            .manifest
            .payload
            .runs
            .iter()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
            .tables
            .iter()
            .find(|table| table.family == ApiCheckpointTableFamily::Revisions)
            .expect("revision table");
        assert!(revisions.segments.len() >= 3);
        assert!(revisions.segments.iter().all(|descriptor| {
            matches!(
                descriptor.segment_key,
                CheckpointSegmentKey::RowKeyRange { .. }
            )
        }));

        let direntries = materialized
            .manifest
            .payload
            .runs
            .iter()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
            .tables
            .iter()
            .find(|table| table.family == ApiCheckpointTableFamily::DirentryBinds)
            .expect("direntry table");
        assert!(direntries.segments.iter().all(|descriptor| {
            matches!(
                descriptor.segment_key,
                CheckpointSegmentKey::DirentryParent { .. }
            )
        }));

        for table in &base_run(&materialized.manifest).tables {
            let mut previous_max_key: Option<&str> = None;
            for descriptor in &table.segments {
                assert!(descriptor.min_key.as_str() <= descriptor.max_key.as_str());
                if let Some(previous) = previous_max_key {
                    assert!(previous < descriptor.min_key.as_str());
                }
                previous_max_key = Some(descriptor.max_key.as_str());
            }
        }
    }

    #[test]
    fn large_table_scan_does_not_insert_metadata_cache_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        for index in 0..8 {
            let path = format!("/docs/file-{index}.txt");
            write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
                .expect("write file");
        }
        let policy = MetadataLsmPolicy {
            max_rows_per_segment: 1,
            ..MetadataLsmPolicy::default()
        };
        let checkpoint = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("checkpoint");
        let cache = super::MetadataTableCache::new(Default::default());
        let tables = super::load_verified_checkpoint_tables_with_cache(
            &store,
            Some(&cache),
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load tables");

        let before = cache.stats();
        let revisions = tables
            .scan_prefix(ApiCheckpointTableFamily::Revisions, "revision-")
            .expect("scan revisions");
        let after = cache.stats();

        assert!(revisions.len() >= 8);
        assert_eq!(after.inserts, before.inserts);
        assert!(after.misses > before.misses);
    }

    #[test]
    fn metadata_cache_budget_counts_decoded_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/file.txt",
            b"file\n",
            &context,
            None,
        )
        .expect("write file");
        let checkpoint = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
            enabled: true,
            max_blocks: 256,
            max_decoded_bytes: Some(1),
        });
        let tables = super::load_verified_checkpoint_tables_with_cache(
            &store,
            Some(&cache),
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load tables");

        let key = "inode-00000000000000000001";
        assert!(tables
            .get(ApiCheckpointTableFamily::Inodes, key)
            .expect("get inode")
            .is_some());

        let stats = cache.stats();
        assert!(stats.inserts > 0);
        assert!(stats.evictions > 0);
    }

    #[test]
    fn whole_run_compaction_rewrites_base_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let policy = MetadataLsmPolicy {
            max_l0_runs: 1,
            max_rows_per_segment: 2,
        };
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/hot/original.txt",
            b"hot\n",
            &context,
            None,
        )
        .expect("write hot");
        write_file_bytes(
            &store,
            &namespace_id,
            "/cold/original.txt",
            b"cold\n",
            &context,
            None,
        )
        .expect("write cold");

        let first = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("first checkpoint");
        let first_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first checkpoint");
        let first_run_keys = run_segment_object_keys(&first_materialized.manifest);

        write_file_bytes(
            &store,
            &namespace_id,
            "/hot/after-l0.txt",
            b"hot 2\n",
            &context,
            None,
        )
        .expect("write hot l0");
        create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("l0 checkpoint");
        write_file_bytes(
            &store,
            &namespace_id,
            "/hot/after-compact.txt",
            b"hot 3\n",
            &context,
            None,
        )
        .expect("write hot compact");
        let compacted = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("compacted checkpoint");
        let compacted_materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            compacted.checkpoint_seq,
        )
        .expect("load compacted checkpoint");
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
        let compacted_run_prefix = format!(
            "namespaces/{}/checkpoints/{:020}/runs/",
            namespace_id.as_str(),
            compacted.checkpoint_seq.0
        );

        assert_eq!(
            compacted_materialized.manifest.payload.base_seq,
            compacted.checkpoint_seq
        );
        assert!(l0_runs(&compacted_materialized.manifest).is_empty());
        assert_eq!(compacted_materialized.manifest.payload.runs.len(), 1);
        assert!(!compacted_run_keys.is_empty());
        assert!(compacted_run_keys
            .iter()
            .all(|key| key.starts_with(&compacted_run_prefix)));
        assert!(compacted_run_keys
            .iter()
            .all(|key| !first_run_keys.contains(key)));
        assert_checkpoint_rows_have_unique_keys(&compacted_materialized.metadata_state);
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &compacted_materialized.metadata_state
        ));
    }

    #[test]
    fn whole_run_compaction_resegments_row_key_range_families_with_l0_runs() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let policy = MetadataLsmPolicy {
            max_l0_runs: 1,
            max_rows_per_segment: 2,
        };
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        for index in 0..6 {
            let path = format!("/docs/file-{index}.txt");
            write_file_bytes(&store, &namespace_id, &path, b"initial\n", &context, None)
                .expect("write initial file");
        }

        let first = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("first checkpoint");
        let first_materialized =
            load_verified_checkpoint_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first checkpoint");
        let revision_keys_before = base_segment_object_keys_for_family(
            &first_materialized.manifest,
            ApiCheckpointTableFamily::Revisions,
        );
        assert!(revision_keys_before.len() >= 3);

        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/file-0.txt",
            b"l0\n",
            &context,
            None,
        )
        .expect("write l0 revision");
        create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("l0 checkpoint");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/file-0.txt",
            b"compact\n",
            &context,
            None,
        )
        .expect("write compaction revision");
        let compacted = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("compacted checkpoint");
        let compacted_materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            compacted.checkpoint_seq,
        )
        .expect("load compacted checkpoint");
        let revision_keys_after = base_segment_object_keys_for_family(
            &compacted_materialized.manifest,
            ApiCheckpointTableFamily::Revisions,
        );
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");

        assert!(l0_runs(&compacted_materialized.manifest).is_empty());
        assert_eq!(compacted_materialized.manifest.payload.runs.len(), 1);
        assert!(revision_keys_after
            .iter()
            .all(|key| !revision_keys_before.contains(key)));
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &compacted_materialized.metadata_state
        ));
        assert_checkpoint_rows_have_unique_keys(&compacted_materialized.metadata_state);
    }

    #[test]
    fn checkpoint_writes_and_validates_direntry_child_bind_index() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");

        let checkpoint = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load checkpoint");
        let child_table = materialized
            .manifest
            .payload
            .runs
            .iter()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
            .tables
            .iter()
            .find(|table| {
                table.family
                    == loon_api::wire::checkpoint::CheckpointTableFamily::DirentryChildBinds
            })
            .expect("child bind table");
        let child_segment = child_table.segments.first().expect("child bind segment");
        assert!(child_segment
            .min_key
            .starts_with("direntry-child-000000000000000000"));

        let deleted_key = child_segment.object_key.clone();
        store.delete(&deleted_key).expect("delete child index");
        match load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        ) {
            Err(CheckpointLoadError::MissingSegment { object_key }) => {
                assert_eq!(object_key, deleted_key);
            }
            other => panic!("expected missing child-bind segment, got {other:?}"),
        }
    }

    #[test]
    fn checkpoint_rejects_child_bind_index_that_diverges_from_canonical_binds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/other.txt",
            b"other\n",
            &context,
            None,
        )
        .expect("write other");

        let checkpoint = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized = load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        )
        .expect("load checkpoint before corruption");
        let mut manifest = materialized.manifest;
        let mut child_index_rows = checkpoint_rows_for_family(
            &materialized.metadata_state,
            ApiCheckpointTableFamily::DirentryChildBinds,
        );
        assert!(child_index_rows.len() >= 2);
        child_index_rows[0] = child_index_rows[1].clone();
        child_index_rows.sort_by_key(|row| {
            row.row_key_for_family(ApiCheckpointTableFamily::DirentryChildBinds)
        });

        let child_table = manifest
            .payload
            .runs
            .iter_mut()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
            .tables
            .iter_mut()
            .find(|table| table.family == ApiCheckpointTableFamily::DirentryChildBinds)
            .expect("child bind table");
        let child_segment = child_table
            .segments
            .first_mut()
            .expect("child bind segment");
        rewrite_checkpoint_segment(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
            ApiCheckpointTableFamily::DirentryChildBinds,
            child_segment,
            child_index_rows,
            &context.writer_version,
        );

        let manifest_key = checkpoint_manifest(namespace_id.as_str(), checkpoint.checkpoint_seq.0);
        let writer_version = manifest.writer_version.clone();
        let updated_manifest =
            CheckpointManifestEnvelope::from_payload(writer_version, manifest.payload)
                .expect("updated manifest");
        let manifest_bytes =
            encode_checkpoint_manifest_json(&updated_manifest).expect("encode updated manifest");
        store
            .put_overwrite(&manifest_key, &manifest_bytes)
            .expect("overwrite manifest");

        assert_child_index_mismatch(load_verified_checkpoint_materialization(
            &store,
            &namespace_id,
            checkpoint.checkpoint_seq,
        ));
    }

    fn rewrite_checkpoint_segment(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        checkpoint_seq: ChangeSeq,
        family: ApiCheckpointTableFamily,
        descriptor: &mut CheckpointSegmentDescriptor,
        rows: Vec<CheckpointRow>,
        writer_version: &str,
    ) {
        let row_keys = rows
            .iter()
            .map(|row| row.row_key_for_family(family))
            .collect::<Vec<_>>();
        let min_key = row_keys.first().cloned().unwrap_or_default();
        let max_key = row_keys.last().cloned().unwrap_or_default();
        let page = CheckpointPage {
            page_index: 0,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            row_keys,
            rows,
        };
        let payload = CheckpointSegmentPayload {
            namespace_id: namespace_id.clone(),
            segment_seq: checkpoint_seq,
            family,
            segment_index: descriptor.segment_index,
            segment_key: descriptor.segment_key.clone(),
            row_count: page.rows.len() as u64,
            min_key,
            max_key,
            pages: vec![page],
        };
        let envelope = CheckpointSegmentEnvelope::from_payload(writer_version, payload)
            .expect("rewritten segment");
        let encoded =
            encode_checkpoint_segment_envelope_zstd(&envelope).expect("encode rewritten segment");
        store
            .put_overwrite(&descriptor.object_key, &encoded)
            .expect("overwrite segment");

        descriptor.row_count = envelope.payload.row_count;
        descriptor.min_key = envelope.payload.min_key.clone();
        descriptor.max_key = envelope.payload.max_key.clone();
        descriptor.payload_checksum_sha256 = envelope.payload_checksum_sha256.clone();
        descriptor.page_checksums_sha256 = envelope
            .page_checksums_sha256()
            .expect("rewritten page checksums");
    }

    fn assert_child_index_mismatch<T>(result: Result<T, CheckpointLoadError>) {
        match result {
            Err(CheckpointLoadError::SegmentDescriptorMismatch { message, .. }) => {
                assert!(message.contains("direntry-child-binds index"));
            }
            Err(other) => panic!("expected child index mismatch, got {other:?}"),
            Ok(_) => panic!("expected child index mismatch"),
        }
    }

    fn base_run(manifest: &CheckpointManifestEnvelope) -> &CheckpointRunManifest {
        manifest
            .payload
            .runs
            .iter()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
    }

    fn l0_runs(manifest: &CheckpointManifestEnvelope) -> Vec<&CheckpointRunManifest> {
        manifest
            .payload
            .runs
            .iter()
            .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
            .collect()
    }

    fn run_segment_object_keys(manifest: &CheckpointManifestEnvelope) -> Vec<String> {
        manifest
            .payload
            .runs
            .iter()
            .flat_map(|run| {
                run.tables.iter().flat_map(|table| {
                    table
                        .segments
                        .iter()
                        .map(|descriptor| descriptor.object_key.clone())
                })
            })
            .collect()
    }

    fn base_segment_object_keys_for_family(
        manifest: &CheckpointManifestEnvelope,
        family: ApiCheckpointTableFamily,
    ) -> Vec<String> {
        base_run(manifest)
            .tables
            .iter()
            .find(|table| table.family == family)
            .expect("table")
            .segments
            .iter()
            .map(|descriptor| descriptor.object_key.clone())
            .collect()
    }

    fn assert_checkpoint_rows_have_unique_keys(metadata_state: &MetadataState) {
        for family in CHECKPOINT_TABLE_FAMILIES {
            let rows = checkpoint_rows_for_family(metadata_state, family);
            let mut seen = BTreeSet::new();
            for row in rows {
                let row_key = row.row_key_for_family(family);
                assert!(
                    seen.insert(row_key.clone()),
                    "duplicate checkpoint row key `{row_key}` in {family:?}"
                );
            }
        }
    }

    #[test]
    fn checkpoint_l0_run_cap_collapses_back_to_base_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        for index in 1..=10 {
            write_file_and_checkpoint(&store, &namespace_id, &context, index);
        }

        let capped = load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(9))
            .expect("load capped checkpoint");
        assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
        assert_eq!(l0_runs(&capped.manifest).len(), MAX_CHECKPOINT_L0_RUNS);

        let collapsed =
            load_verified_checkpoint_materialization(&store, &namespace_id, ChangeSeq(10))
                .expect("load collapsed checkpoint");
        assert_eq!(collapsed.manifest.payload.base_seq, ChangeSeq(10));
        assert!(l0_runs(&collapsed.manifest).is_empty());
        assert_eq!(collapsed.manifest.payload.runs.len(), 1);
    }

    #[test]
    fn unreferenced_checkpoint_run_is_ignored_by_basis_load() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");
        let first = create_checkpoint(&store, &namespace_id, &context).expect("first checkpoint");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/second.txt",
            b"second\n",
            &context,
            None,
        )
        .expect("write second");

        let basis_before = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let orphan_manifest = build_checkpoint_manifest_for_basis(
            &store,
            &namespace_id,
            &basis_before,
            &context.writer_version,
            MetadataLsmPolicy::default(),
        )
        .expect("build orphan checkpoint");
        write_checkpoint_manifest(&store, &orphan_manifest).expect("write orphan checkpoint");

        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(
            basis_after.head.checkpoint_hint_seq,
            Some(first.checkpoint_seq)
        );
        assert_eq!(basis_after.head.seq, orphan_manifest.payload.checkpoint_seq);
        assert!(metadata_states_equivalent(
            &basis_before.metadata_state,
            &basis_after.metadata_state
        ));
    }

    #[test]
    fn older_checkpoint_can_publish_after_head_advances() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/hello.txt",
            b"hello\n",
            &context,
            None,
        )
        .expect("write hello");

        let basis_before = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let run_id = generate_checkpoint_run_id();
        let tables = build_checkpoint_tables(
            &store,
            &namespace_id,
            &run_id,
            basis_before.head.seq,
            &basis_before.metadata_state,
            &context.writer_version,
            MetadataLsmPolicy::default().max_rows_per_segment,
        )
        .expect("build checkpoint tables");
        let manifest = CheckpointManifestEnvelope::from_payload(
            &context.writer_version,
            CheckpointManifestPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_before.head.seq,
                base_seq: basis_before.head.seq,
                active_fence_token: basis_before.head.active_fence_token,
                next_inode_id: basis_before.head.next_inode_id,
                retention_floor_seq: basis_before.head.retention_floor_seq,
                verified: true,
                runs: vec![CheckpointRunManifest {
                    run_id,
                    run_seq: basis_before.head.seq,
                    level: CHECKPOINT_BASE_RUN_LEVEL,
                    tables,
                }],
            },
        )
        .expect("build manifest");
        write_checkpoint_manifest(&store, &manifest).expect("write manifest");

        write_file_bytes(
            &store,
            &namespace_id,
            "/docs/second.txt",
            b"second\n",
            &context,
            None,
        )
        .expect("write second");
        let published = publish_checkpoint_hint_seq(
            &store,
            &namespace_id,
            basis_before.head.seq,
            &context.writer_version,
        )
        .expect("publish snapshot hint");

        assert_eq!(published.seq, ChangeSeq(2));
        assert_eq!(published.checkpoint_hint_seq, Some(ChangeSeq(1)));

        let after = load_verified_namespace_basis(&store, &namespace_id).expect("basis after");
        assert_eq!(after.head.seq, ChangeSeq(2));
        assert_eq!(after.head.checkpoint_hint_seq, Some(ChangeSeq(1)));
    }

    #[test]
    fn checkpoint_hint_cas_retry_exhaustion_is_stale_head() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let store = HeadCasFailureStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_head(namespace_id.as_str()),
        );
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        store.fail_head_cas();
        let error = publish_checkpoint_hint_seq(
            &store,
            &namespace_id,
            ChangeSeq(0),
            &context.writer_version,
        )
        .expect_err("checkpoint hint publication should exhaust CAS retries");

        assert_eq!(error.kind(), CoreErrorKind::StaleHead);
    }

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "test-writer".to_owned(),
            writer_version: "test-writer/0.1.0".to_owned(),
            now_ms: 1_000,
            lease_duration_ms: 60_000,
        }
    }

    fn write_file_and_checkpoint(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        index: u64,
    ) -> ChangeSeq {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file {index}\n");
        write_file_bytes(store, namespace_id, &path, bytes.as_bytes(), context, None)
            .expect("write file");
        create_checkpoint(store, namespace_id, context)
            .expect("create checkpoint")
            .checkpoint_seq
    }

    fn write_file_and_checkpoint_with_policy(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        index: u64,
        policy: MetadataLsmPolicy,
    ) -> ChangeSeq {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file {index}\n");
        write_file_bytes(store, namespace_id, &path, bytes.as_bytes(), context, None)
            .expect("write file");
        create_checkpoint_with_policy(store, namespace_id, context, policy)
            .expect("create checkpoint")
            .checkpoint_seq
    }

    struct HeadCasFailureStore {
        inner: LocalFsStore,
        head_key: String,
        fail_head_cas: Mutex<bool>,
    }

    impl HeadCasFailureStore {
        fn new(inner: LocalFsStore, head_key: String) -> Self {
            Self {
                inner,
                head_key,
                fail_head_cas: Mutex::new(false),
            }
        }

        fn fail_head_cas(&self) {
            *self
                .fail_head_cas
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        }
    }

    impl ObjectStore for HeadCasFailureStore {
        fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key)
        }

        fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
            self.inner.get(key, range)
        }

        fn put(
            &self,
            key: &str,
            bytes: &[u8],
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key
                && matches!(&mode, PutMode::CompareAndSwap { .. })
                && *self
                    .fail_head_cas
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                return Err(ObjectStoreError::PreconditionFailed);
            }
            self.inner.put(key, bytes, mode)
        }

        fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key)
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list_prefix(prefix)
        }
    }

    struct ConflictOnManifestCreateStore {
        inner: LocalFsStore,
        manifest_key: String,
        replacement_bytes: Vec<u8>,
        injected: Mutex<bool>,
    }

    impl ConflictOnManifestCreateStore {
        fn new(inner: LocalFsStore, manifest_key: String, replacement_bytes: Vec<u8>) -> Self {
            Self {
                inner,
                manifest_key,
                replacement_bytes,
                injected: Mutex::new(false),
            }
        }
    }

    impl ObjectStore for ConflictOnManifestCreateStore {
        fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key)
        }

        fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
            self.inner.get(key, range)
        }

        fn put(
            &self,
            key: &str,
            bytes: &[u8],
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.manifest_key && matches!(&mode, PutMode::CreateIfAbsent) {
                let mut injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !*injected {
                    *injected = true;
                    self.inner.put_overwrite(key, &self.replacement_bytes)?;
                    return Err(ObjectStoreError::Conflict);
                }
            }
            self.inner.put(key, bytes, mode)
        }

        fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key)
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list_prefix(prefix)
        }
    }
}
