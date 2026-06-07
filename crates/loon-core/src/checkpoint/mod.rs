mod cache;
mod row;

use self::cache::{DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCacheKey};
pub use self::cache::{MetadataTableCache, MetadataTableCacheConfig, MetadataTableCacheStats};
use self::row::{
    manifest_row_commit_seq, manifest_row_kind, manifest_row_matches_family,
    manifest_rows_for_family, manifest_rows_for_family_after_seq, metadata_states_equivalent,
};
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    MetadataStateBuilder, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::namespace::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::namespace::control::read_head_object;
use crate::storage::content::write_immutable_object;
use loon_api::wire::control::{
    ControlObjectKind, HeadState, HeadStateEnvelope, ProgressStateEnvelope,
};
use loon_api::wire::manifest::{
    decode_metadata_sst_envelope_zstd, decode_namespace_manifest_json,
    encode_metadata_sst_envelope_zstd, encode_namespace_manifest_json,
    metadata_page_checksum_sha256, metadata_sst_payload_checksum_sha256, MetadataFileRef,
    MetadataPage, MetadataRow, MetadataSegmentKey, MetadataSstEnvelope, MetadataSstPayload,
    MetadataTableFamily, NamespaceCheckpointRecord, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loon_api::{
    generate_checkpoint_id, generate_metadata_table_id, validate_checkpoint_id,
    validate_metadata_table_id, AdvanceRetentionResponse, ChangeSeq, CreateCheckpointResponse,
    InodeId, NamespaceId,
};
use loon_objectstore::keys::{
    derived_progress, metadata_sst, namespace_manifest, DerivedWorkClass,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
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
const CHECKPOINT_TABLE_FAMILIES: [MetadataTableFamily; 7] = [
    MetadataTableFamily::Inodes,
    MetadataTableFamily::DirentryBinds,
    MetadataTableFamily::DirentryChildBinds,
    MetadataTableFamily::DirentryUnbinds,
    MetadataTableFamily::Revisions,
    MetadataTableFamily::Tombstones,
    MetadataTableFamily::CommitReceipts,
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTableManifest {
    family: MetadataTableFamily,
    segments: Vec<MetadataFileRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataRunManifest {
    run_seq: ChangeSeq,
    level: u32,
    tables: Vec<MetadataTableManifest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataLsmPolicy {
    pub max_l0_runs: usize,
    pub max_rows_per_segment: usize,
}

impl Default for MetadataLsmPolicy {
    fn default() -> Self {
        Self {
            max_l0_runs: DEFAULT_MAX_CHECKPOINT_L0_RUNS,
            max_rows_per_segment: DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
        }
    }
}

fn l0_run_count(payload: &NamespaceManifestPayload) -> usize {
    runs_from_metadata_files(payload)
        .into_iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .count()
}

fn runs_in_scan_order(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs = runs_from_metadata_files(payload);
    runs.sort_by(|left, right| {
        left.level
            .cmp(&right.level)
            .then(right.run_seq.cmp(&left.run_seq))
    });
    runs
}

fn runs_in_materialization_order(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs = runs_from_metadata_files(payload);
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.level.cmp(&left.level))
    });
    runs
}

fn runs_from_metadata_files(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs: BTreeMap<(ChangeSeq, u32), BTreeMap<MetadataTableFamily, Vec<MetadataFileRef>>> =
        BTreeMap::new();
    for metadata_file in &payload.metadata_files {
        runs.entry((metadata_file.run_seq, metadata_file.level))
            .or_default()
            .entry(metadata_file.family)
            .or_default()
            .push(metadata_file.clone());
    }
    runs.into_iter()
        .map(|((run_seq, level), tables_by_family)| MetadataRunManifest {
            run_seq,
            level,
            tables: CHECKPOINT_TABLE_FAMILIES
                .into_iter()
                .map(|family| {
                    let mut segments = tables_by_family.get(&family).cloned().unwrap_or_default();
                    segments.sort_by_key(|segment| segment.segment_index);
                    MetadataTableManifest { family, segments }
                })
                .collect(),
        })
        .collect()
}

fn flatten_manifest_tables(tables: Vec<MetadataTableManifest>) -> Vec<MetadataFileRef> {
    tables
        .into_iter()
        .flat_map(|table| table.segments)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedManifestMaterialization {
    pub(crate) manifest: NamespaceManifestEnvelope,
    pub(crate) metadata_state: MetadataState,
}

pub(crate) struct VerifiedMetadataTables<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    manifest_object_key: String,
    manifest: NamespaceManifestEnvelope,
    segment_cache: RefCell<HashMap<String, Vec<MetadataRow>>>,
}

impl<S: ObjectStore + ?Sized> VerifiedMetadataTables<'_, S> {
    pub(crate) fn manifest(&self) -> &NamespaceManifestEnvelope {
        &self.manifest
    }

    pub(crate) fn get(
        &self,
        family: MetadataTableFamily,
        key: &str,
    ) -> Result<Option<MetadataRow>, ManifestLoadError> {
        Ok(self
            .scan_prefix_with_cache_mode(family, key, MetadataTableCacheMode::Populate)?
            .into_iter()
            .find(|row| row.row_key_for_family(family) == key))
    }

    pub(crate) fn scan_prefix(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
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
        family: MetadataTableFamily,
        prefix: &str,
        cache_mode: MetadataTableCacheMode,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        let mut rows = Vec::new();
        for run in runs_in_scan_order(&self.manifest.payload) {
            self.scan_manifest_tables(
                family,
                prefix,
                MetadataTableLoadContext {
                    manifest_object_key: &self.manifest_object_key,
                    segment_seq_expectation: MetadataSstSeqExpectation::Descriptor,
                    row_seq_min: None,
                    row_seq_max: run.run_seq,
                },
                &run.tables,
                MetadataTableScanOutput {
                    rows: &mut rows,
                    cache_mode,
                },
            )?;
        }
        Ok(rows)
    }

    fn matching_segment_count(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
    ) -> Result<usize, ManifestLoadError> {
        let mut count = 0;
        for run in runs_in_scan_order(&self.manifest.payload) {
            count +=
                count_matching_segments(&self.manifest_object_key, &run.tables, family, prefix)?;
        }
        Ok(count)
    }

    fn scan_manifest_tables(
        &self,
        family: MetadataTableFamily,
        prefix: &str,
        context: MetadataTableLoadContext<'_>,
        tables: &[MetadataTableManifest],
        output: MetadataTableScanOutput<'_>,
    ) -> Result<(), ManifestLoadError> {
        let table = manifest_table_for_family(context.manifest_object_key, tables, family)?;
        for descriptor in &table.segments {
            context.expected_segment_seq(descriptor)?;
            let expected_key = metadata_file_object_key(descriptor);
            if descriptor.object_key != expected_key {
                return Err(ManifestLoadError::SegmentObjectKeyMismatch {
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
        context: MetadataTableLoadContext<'_>,
        family: MetadataTableFamily,
        descriptor: &MetadataFileRef,
        cache_mode: MetadataTableCacheMode,
    ) -> Result<Vec<MetadataRow>, ManifestLoadError> {
        if let Some(rows) = self.segment_cache.borrow().get(&descriptor.object_key) {
            return Ok(rows.clone());
        }
        let rows = load_manifest_segment_rows_with_cache(
            self.store,
            self.table_cache,
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

struct MetadataTableScanOutput<'a> {
    rows: &'a mut Vec<MetadataRow>,
    cache_mode: MetadataTableCacheMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestLoadErrorKind {
    Corrupt,
    Store,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ManifestLoadError {
    #[error("missing namespace manifest `{object_key}`")]
    MissingManifest { object_key: String },
    #[error("failed to read namespace manifest `{object_key}`: {message}")]
    ReadManifest { object_key: String, message: String },
    #[error("namespace manifest codec error for `{object_key}`: {message}")]
    ManifestCodec { object_key: String, message: String },
    #[error(
        "namespace manifest namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "namespace manifest seq mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    ManifestSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error("namespace manifest `{object_key}` is not verified")]
    ManifestNotVerified { object_key: String },
    #[error("namespace manifest `{object_key}` is missing table family `{family:?}`")]
    MissingTableFamily {
        object_key: String,
        family: MetadataTableFamily,
    },
    #[error("namespace manifest `{object_key}` repeats table family `{family:?}`")]
    DuplicateTableFamily {
        object_key: String,
        family: MetadataTableFamily,
    },
    #[error("namespace manifest `{object_key}` has invalid runs: {message}")]
    RunManifestMismatch { object_key: String, message: String },
    #[error("missing metadata SST `{object_key}`")]
    MissingSegment { object_key: String },
    #[error("failed to read metadata SST `{object_key}`: {message}")]
    ReadSegment { object_key: String, message: String },
    #[error("metadata SST codec error for `{object_key}`: {message}")]
    SegmentCodec { object_key: String, message: String },
    #[error(
        "metadata SST namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error(
        "metadata SST seq mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentSeqMismatch {
        object_key: String,
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    #[error(
        "metadata SST family mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentFamilyMismatch {
        object_key: String,
        expected: MetadataTableFamily,
        actual: MetadataTableFamily,
    },
    #[error(
        "metadata SST index mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    SegmentIndexMismatch {
        object_key: String,
        expected: u32,
        actual: u32,
    },
    #[error(
        "metadata SST key mismatch for `{object_key}`: expected `{expected:?}`, actual `{actual:?}`"
    )]
    SegmentKeyMismatch {
        object_key: String,
        expected: MetadataSegmentKey,
        actual: MetadataSegmentKey,
    },
    #[error("metadata SST key mismatch for `{object_key}`: expected `{expected}`")]
    SegmentObjectKeyMismatch {
        object_key: String,
        expected: String,
    },
    #[error("metadata SST descriptor mismatch for `{object_key}`: {message}")]
    SegmentDescriptorMismatch { object_key: String, message: String },
    #[error("manifest page shape mismatch for `{object_key}` page {page_index}: {message}")]
    PageShapeMismatch {
        object_key: String,
        page_index: u32,
        message: String,
    },
    #[error(
        "manifest page checksum mismatch for `{object_key}` page {page_index}: expected `{expected}`, actual `{actual}`"
    )]
    PageChecksumMismatch {
        object_key: String,
        page_index: u32,
        expected: String,
        actual: String,
    },
    #[error(
        "metadata row key mismatch for `{object_key}` page {page_index} row {row_index}: expected `{expected}`, actual `{actual}`"
    )]
    RowKeyMismatch {
        object_key: String,
        page_index: u32,
        row_index: usize,
        expected: String,
        actual: String,
    },
    #[error(
        "metadata row kind mismatch for `{object_key}` family `{family:?}`: found `{row_kind}`"
    )]
    TableRowKindMismatch {
        object_key: String,
        family: MetadataTableFamily,
        row_kind: String,
    },
    #[error("metadata rows do not reproduce authoritative metadata")]
    MetadataMismatch,
}

impl ManifestLoadError {
    pub fn kind(&self) -> ManifestLoadErrorKind {
        match self {
            Self::ReadManifest { .. } | Self::ReadSegment { .. } => ManifestLoadErrorKind::Store,
            _ => ManifestLoadErrorKind::Corrupt,
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
    let manifest_seq = basis.head.seq;
    let checkpoint_record = NamespaceCheckpointRecord {
        checkpoint_id: generate_checkpoint_id(),
        checkpoint_seq: manifest_seq,
        manifest_seq,
        created_at_ms: context.now_ms,
    };

    match load_verified_manifest_materialization_if_present(store, namespace_id, manifest_seq) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let manifest = build_namespace_manifest_for_basis(
                store,
                namespace_id,
                &basis,
                &context.writer_version,
                policy,
                Some(checkpoint_record),
            )?;
            let materialized = load_manifest_materialization_from_manifest(
                store,
                namespace_id,
                &namespace_manifest(namespace_id.as_str(), manifest_seq.0),
                &manifest,
            )
            .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
            if !metadata_states_equivalent(&basis.metadata_state, &materialized) {
                return Err(CoreError::Basis(BasisLoadError::ManifestLoad(
                    ManifestLoadError::MetadataMismatch,
                )));
            }

            // `write_namespace_manifest` owns the idempotent "manifest already
            // exists" path. Any manifest load error it returns must surface.
            write_namespace_manifest(store, &manifest).map_err(CoreError::Basis)?;
        }
        Err(error) => return Err(CoreError::Basis(BasisLoadError::ManifestLoad(error))),
    }

    let materialized = load_verified_manifest_materialization(store, namespace_id, manifest_seq)
        .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
    let checkpoint = checkpoint_record_for_manifest(&materialized.manifest).ok_or_else(|| {
        CoreError::CheckpointUnavailable(format!(
            "namespace `{}` manifest at seq {:?} has no checkpoint record",
            namespace_id.as_str(),
            manifest_seq
        ))
    })?;
    let resulting_head =
        publish_checkpoint_hint_seq(store, namespace_id, manifest_seq, &context.writer_version)?;

    Ok(CreateCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        checkpoint_seq: manifest_seq,
        checkpoint_hint_seq: resulting_head.checkpoint_hint_seq,
        checkpoint_hint_points_at_checkpoint: resulting_head.checkpoint_hint_seq
            == Some(manifest_seq),
    })
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "project_manifest")
)]
fn build_namespace_manifest_for_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &crate::namespace::basis::VerifiedNamespaceBasis,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    checkpoint_to_add: Option<NamespaceCheckpointRecord>,
) -> Result<NamespaceManifestEnvelope, CoreError> {
    let manifest_seq = basis.head.seq;
    let previous_manifest = match basis.head.checkpoint_hint_seq {
        Some(previous_seq) if previous_seq < manifest_seq => Some(
            load_verified_manifest_materialization(store, namespace_id, previous_seq)
                .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?,
        ),
        _ => None,
    };

    let mut checkpoints = previous_manifest
        .as_ref()
        .map(|previous| previous.manifest.payload.checkpoints.clone())
        .unwrap_or_default();
    if let Some(checkpoint) = checkpoint_to_add {
        checkpoints.push(checkpoint);
    }

    let (base_seq, metadata_files) = match previous_manifest {
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs => {
            let mut metadata_files = previous.manifest.payload.metadata_files.clone();
            metadata_files.extend(flatten_manifest_tables(build_manifest_l0_run_tables(
                store,
                namespace_id,
                manifest_seq,
                previous.manifest.payload.manifest_seq,
                &basis.metadata_state,
                writer_version,
            )?));
            (previous.manifest.payload.base_seq, metadata_files)
        }
        Some(_) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                manifest_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (manifest_seq, flatten_manifest_tables(run_tables))
        }
        _ => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                manifest_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )?;
            (manifest_seq, flatten_manifest_tables(run_tables))
        }
    };

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_seq,
            base_seq,
            active_fence_token: basis.head.active_fence_token,
            next_inode_id: basis.head.next_inode_id,
            retention_floor_seq: basis.head.retention_floor_seq,
            verified: true,
            fork: None,
            checkpoints,
            metadata_files,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))
}

fn checkpoint_record_for_manifest(
    manifest: &NamespaceManifestEnvelope,
) -> Option<&NamespaceCheckpointRecord> {
    manifest.payload.checkpoints.iter().find(|checkpoint| {
        checkpoint.checkpoint_seq == manifest.payload.manifest_seq
            && checkpoint.manifest_seq == manifest.payload.manifest_seq
    })
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
        let _ = load_verified_manifest_materialization(store, namespace_id, target_floor)
            .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
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

pub(crate) fn load_verified_manifest_materialization<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
) -> Result<LoadedManifestMaterialization, ManifestLoadError> {
    load_verified_manifest_materialization_if_present(store, namespace_id, manifest_seq)?
        .ok_or_else(|| ManifestLoadError::MissingManifest {
            object_key: namespace_manifest(namespace_id.as_str(), manifest_seq.0),
        })
}

pub(crate) fn load_verified_manifest_tables_with_cache<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
) -> Result<VerifiedMetadataTables<'a, S>, ManifestLoadError> {
    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_seq.0);
    let manifest = {
        let _span = tracing::info_span!(
            "loon.phase",
            phase = "load_namespace_manifest",
            key_class = "manifest_table"
        )
        .entered();
        let Some(manifest_bytes) =
            store
                .get(&manifest_key, None)
                .map_err(|err| ManifestLoadError::ReadManifest {
                    object_key: manifest_key.clone(),
                    message: err.to_string(),
                })?
        else {
            return Err(ManifestLoadError::MissingManifest {
                object_key: manifest_key.clone(),
            });
        };
        decode_namespace_manifest_json(&manifest_bytes).map_err(|err| {
            ManifestLoadError::ManifestCodec {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            }
        })
    }?;
    validate_namespace_manifest(namespace_id, manifest_seq, &manifest_key, &manifest)?;
    validate_manifest_materialization_ranges(&manifest_key, &manifest.payload)?;
    let tables = VerifiedMetadataTables {
        store,
        table_cache,
        manifest_object_key: manifest_key,
        manifest,
        segment_cache: RefCell::new(HashMap::new()),
    };
    Ok(tables)
}

pub(crate) fn manifest_basis_head(
    current_head: &HeadState,
    manifest: &NamespaceManifestEnvelope,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: manifest.payload.manifest_seq,
        // The manifest records the manifest-time fence token. That may lag the
        // live head if lease takeover advanced the fence without any WAL replay.
        active_fence_token: manifest.payload.active_fence_token,
        next_inode_id: manifest.payload.next_inode_id,
        name_policy: current_head.name_policy,
        checkpoint_hint_seq: current_head.checkpoint_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: None,
    }
}

fn load_verified_manifest_materialization_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
) -> Result<Option<LoadedManifestMaterialization>, ManifestLoadError> {
    let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_seq.0);
    let manifest = {
        let _span = tracing::info_span!(
            "loon.phase",
            phase = "load_namespace_manifest",
            key_class = "manifest_table"
        )
        .entered();
        let Some(manifest_bytes) =
            store
                .get(&manifest_key, None)
                .map_err(|err| ManifestLoadError::ReadManifest {
                    object_key: manifest_key.clone(),
                    message: err.to_string(),
                })?
        else {
            return Ok(None);
        };
        let manifest = decode_namespace_manifest_json(&manifest_bytes).map_err(|err| {
            ManifestLoadError::ManifestCodec {
                object_key: manifest_key.clone(),
                message: err.to_string(),
            }
        })?;
        Ok(Some(manifest))
    }?;
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    validate_namespace_manifest(namespace_id, manifest_seq, &manifest_key, &manifest)?;
    let metadata_state =
        load_manifest_materialization_from_manifest(store, namespace_id, &manifest_key, &manifest)?;
    Ok(Some(LoadedManifestMaterialization {
        manifest,
        metadata_state,
    }))
}

fn build_manifest_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
    level: u32,
    metadata_state: &MetadataState,
    writer_version: &str,
    max_rows_per_segment: usize,
) -> Result<Vec<MetadataTableManifest>, CoreError> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        manifest_seq,
        level,
        writer_version,
        |family| manifest_rows_for_family(metadata_state, family),
        MetadataTableSegmentation::Base {
            max_rows_per_segment,
        },
    )
}

fn debug_assert_manifest_table_segments_do_not_overlap(tables: &[MetadataTableManifest]) {
    #[cfg(debug_assertions)]
    for table in tables {
        let mut previous_max_key: Option<&str> = None;
        for descriptor in &table.segments {
            if let Some(previous) = previous_max_key {
                debug_assert!(
                    previous < descriptor.min_key.as_str(),
                    "overlapping metadata SST ranges for `{:?}`",
                    table.family
                );
            }
            previous_max_key = Some(descriptor.max_key.as_str());
        }
    }
}

fn build_manifest_l0_run_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
    after_seq: ChangeSeq,
    metadata_state: &MetadataState,
    writer_version: &str,
) -> Result<Vec<MetadataTableManifest>, CoreError> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        manifest_seq,
        CHECKPOINT_L0_RUN_LEVEL,
        writer_version,
        |family| manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
        MetadataTableSegmentation::Full,
    )
}

#[derive(Debug, Clone, Copy)]
enum MetadataTableSegmentation {
    Base { max_rows_per_segment: usize },
    Full,
}

struct MetadataSstRows {
    segment_key: MetadataSegmentKey,
    rows: Vec<MetadataRow>,
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "write_manifest_tables", key_class = "manifest_table")
)]
fn build_manifest_tables_from_rows<S, RowsForFamily>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    writer_version: &str,
    mut rows_for_family: RowsForFamily,
    segmentation: MetadataTableSegmentation,
) -> Result<Vec<MetadataTableManifest>, CoreError>
where
    S: ObjectStore + ?Sized,
    RowsForFamily: FnMut(MetadataTableFamily) -> Vec<MetadataRow>,
{
    let mut tables = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let rows = rows_for_family(family);
        if rows.is_empty() {
            tables.push(MetadataTableManifest {
                family,
                segments: Vec::new(),
            });
            continue;
        }

        let segments = segment_manifest_rows(family, rows, segmentation);
        let mut descriptors = Vec::with_capacity(segments.len());
        for (segment_index, segment_rows) in segments.into_iter().enumerate() {
            let segment_index = u32::try_from(segment_index)
                .map_err(|_| CoreError::Store("metadata SST index overflow".to_owned()))?;
            let table_id = generate_metadata_table_id();
            let object_key = metadata_sst(namespace_id.as_str(), &table_id);
            descriptors.push(write_manifest_segment(
                store,
                MetadataSstWriteRequest {
                    namespace_id,
                    table_id,
                    run_seq,
                    level,
                    family,
                    segment_index,
                    segment_key: segment_rows.segment_key,
                    rows: segment_rows.rows,
                    object_key,
                    writer_version,
                },
            )?);
        }
        tables.push(MetadataTableManifest {
            family,
            segments: descriptors,
        });
    }
    Ok(tables)
}

struct MetadataSstWriteRequest<'a> {
    namespace_id: &'a NamespaceId,
    table_id: String,
    run_seq: ChangeSeq,
    level: u32,
    family: MetadataTableFamily,
    segment_index: u32,
    segment_key: MetadataSegmentKey,
    rows: Vec<MetadataRow>,
    object_key: String,
    writer_version: &'a str,
}

fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    request: MetadataSstWriteRequest<'_>,
) -> Result<MetadataFileRef, CoreError> {
    let row_keys = request
        .rows
        .iter()
        .map(|row| row.row_key_for_family(request.family))
        .collect::<Vec<_>>();
    let page = MetadataPage {
        page_index: 0,
        min_key: row_keys.first().cloned().unwrap_or_default(),
        max_key: row_keys.last().cloned().unwrap_or_default(),
        row_keys,
        rows: request.rows,
    };
    let payload = MetadataSstPayload {
        namespace_id: request.namespace_id.clone(),
        table_id: request.table_id.clone(),
        run_seq: request.run_seq,
        level: request.level,
        family: request.family,
        segment_index: request.segment_index,
        segment_key: request.segment_key,
        row_count: page.rows.len() as u64,
        min_key: page.min_key.clone(),
        max_key: page.max_key.clone(),
        pages: vec![page],
    };
    let envelope = MetadataSstEnvelope::from_payload(request.writer_version, payload)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    let encoded = encode_metadata_sst_envelope_zstd(&envelope)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    write_immutable_object(store, &request.object_key, &encoded)?;
    Ok(MetadataFileRef {
        owner_namespace_id: request.namespace_id.clone(),
        table_id: request.table_id,
        object_key: request.object_key,
        run_seq: request.run_seq,
        level: request.level,
        family: request.family,
        segment_index: request.segment_index,
        segment_key: envelope.payload.segment_key.clone(),
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: metadata_sst_payload_checksum_sha256(&envelope.payload)
            .map_err(|err| CoreError::Store(err.to_string()))?,
        page_checksums_sha256: envelope
            .page_checksums_sha256()
            .map_err(|err| CoreError::Store(err.to_string()))?,
    })
}

fn segment_manifest_rows(
    family: MetadataTableFamily,
    rows: Vec<MetadataRow>,
    segmentation: MetadataTableSegmentation,
) -> Vec<MetadataSstRows> {
    match segmentation {
        MetadataTableSegmentation::Full => vec![MetadataSstRows {
            segment_key: MetadataSegmentKey::Full,
            rows,
        }],
        MetadataTableSegmentation::Base {
            max_rows_per_segment,
        } => match family {
            MetadataTableFamily::DirentryBinds | MetadataTableFamily::DirentryUnbinds => {
                segment_rows_by_parent(rows)
            }
            MetadataTableFamily::Inodes
            | MetadataTableFamily::DirentryChildBinds
            | MetadataTableFamily::Revisions
            | MetadataTableFamily::Tombstones
            | MetadataTableFamily::CommitReceipts => {
                segment_rows_by_row_key_range(rows, max_rows_per_segment.max(1))
            }
        },
    }
}

fn segment_rows_by_parent(rows: Vec<MetadataRow>) -> Vec<MetadataSstRows> {
    let mut grouped: BTreeMap<InodeId, Vec<MetadataRow>> = BTreeMap::new();
    for row in rows {
        if let Some(parent_inode_id) = manifest_row_parent_inode_id(&row) {
            grouped.entry(parent_inode_id).or_default().push(row);
        }
    }

    grouped
        .into_iter()
        .map(|(parent_inode_id, rows)| MetadataSstRows {
            segment_key: MetadataSegmentKey::DirentryParent { parent_inode_id },
            rows,
        })
        .collect()
}

fn segment_rows_by_row_key_range(
    rows: Vec<MetadataRow>,
    max_rows_per_segment: usize,
) -> Vec<MetadataSstRows> {
    rows.chunks(max_rows_per_segment)
        .enumerate()
        .map(|(shard, rows)| MetadataSstRows {
            segment_key: MetadataSegmentKey::RowKeyRange {
                shard: u32::try_from(shard).unwrap_or(u32::MAX),
            },
            rows: rows.to_vec(),
        })
        .collect()
}

fn manifest_row_parent_inode_id(row: &MetadataRow) -> Option<InodeId> {
    match row {
        MetadataRow::DirentryBind {
            parent_inode_id, ..
        }
        | MetadataRow::DirentryUnbind {
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
    fields(phase = "write_namespace_manifest", key_class = "manifest_table")
)]
pub(crate) fn write_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), BasisLoadError> {
    let manifest_key = namespace_manifest(
        manifest.payload.namespace_id.as_str(),
        manifest.payload.manifest_seq.0,
    );
    let manifest_bytes = encode_namespace_manifest_json(manifest).map_err(|err| {
        BasisLoadError::ManifestLoad(ManifestLoadError::ManifestCodec {
            object_key: manifest_key.clone(),
            message: err.to_string(),
        })
    })?;
    match store.put_if_absent(&manifest_key, &manifest_bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            let Some(existing) = load_verified_manifest_materialization_if_present(
                store,
                &manifest.payload.namespace_id,
                manifest.payload.manifest_seq,
            )
            .map_err(BasisLoadError::ManifestLoad)?
            else {
                return Err(BasisLoadError::ManifestLoad(
                    ManifestLoadError::MissingManifest {
                        object_key: manifest_key,
                    },
                ));
            };
            if existing.manifest.payload.manifest_seq == manifest.payload.manifest_seq {
                Ok(())
            } else {
                Err(BasisLoadError::ManifestLoad(
                    ManifestLoadError::ManifestSeqMismatch {
                        object_key: manifest_key,
                        expected: manifest.payload.manifest_seq,
                        actual: existing.manifest.payload.manifest_seq,
                    },
                ))
            }
        }
        Err(error) => Err(BasisLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest {
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
    manifest_seq: ChangeSeq,
    writer_version: &str,
) -> Result<HeadState, CoreError> {
    for _attempt in 0..HEAD_UPDATE_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let current_head = loaded_head.envelope.state;
        if current_head.checkpoint_hint_seq >= Some(manifest_seq) {
            return Ok(current_head);
        }

        let next_head = HeadState {
            namespace_id: current_head.namespace_id.clone(),
            seq: current_head.seq,
            active_fence_token: current_head.active_fence_token,
            next_inode_id: current_head.next_inode_id,
            name_policy: current_head.name_policy,
            checkpoint_hint_seq: Some(manifest_seq),
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

fn validate_namespace_manifest(
    namespace_id: &NamespaceId,
    manifest_seq: ChangeSeq,
    object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), ManifestLoadError> {
    if manifest.payload.namespace_id != *namespace_id {
        return Err(ManifestLoadError::ManifestNamespaceMismatch {
            object_key: object_key.to_owned(),
            expected: namespace_id.clone(),
            actual: manifest.payload.namespace_id.clone(),
        });
    }
    if manifest.payload.manifest_seq != manifest_seq {
        return Err(ManifestLoadError::ManifestSeqMismatch {
            object_key: object_key.to_owned(),
            expected: manifest_seq,
            actual: manifest.payload.manifest_seq,
        });
    }
    if !manifest.payload.verified {
        return Err(ManifestLoadError::ManifestNotVerified {
            object_key: object_key.to_owned(),
        });
    }
    validate_manifest_checkpoint_records(object_key, &manifest.payload)?;
    Ok(())
}

fn validate_manifest_checkpoint_records(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    let mut seen_checkpoint_ids = Vec::new();
    for checkpoint in &payload.checkpoints {
        validate_checkpoint_id(&checkpoint.checkpoint_id).map_err(|error| {
            ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: error.to_string(),
            }
        })?;
        if seen_checkpoint_ids.contains(&checkpoint.checkpoint_id.as_str()) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate checkpoint id `{}`", checkpoint.checkpoint_id),
            });
        }
        seen_checkpoint_ids.push(checkpoint.checkpoint_id.as_str());
        if checkpoint.checkpoint_seq > checkpoint.manifest_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "checkpoint `{}` seq {:?} is after manifest seq {:?}",
                    checkpoint.checkpoint_id, checkpoint.checkpoint_seq, checkpoint.manifest_seq
                ),
            });
        }
        if checkpoint.manifest_seq > payload.manifest_seq {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "checkpoint `{}` references future manifest seq {:?}",
                    checkpoint.checkpoint_id, checkpoint.manifest_seq
                ),
            });
        }
    }
    Ok(())
}

fn validate_manifest_materialization_ranges(
    object_key: &str,
    payload: &NamespaceManifestPayload,
) -> Result<(), ManifestLoadError> {
    if payload.base_seq > payload.manifest_seq {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "base_seq {:?} is after manifest_seq {:?}",
                payload.base_seq, payload.manifest_seq
            ),
        });
    }

    if payload.metadata_files.is_empty() {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: "namespace manifest must reference at least one metadata file".to_owned(),
        });
    }

    let mut saw_base_seq_file = false;
    let mut saw_manifest_seq_file = false;
    let mut seen_table_ids = Vec::new();
    for metadata_file in &payload.metadata_files {
        validate_metadata_table_id(&metadata_file.table_id).map_err(|error| {
            ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: error.to_string(),
            }
        })?;
        if seen_table_ids.contains(&metadata_file.table_id.as_str()) {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!("duplicate metadata table id `{}`", metadata_file.table_id),
            });
        }
        seen_table_ids.push(metadata_file.table_id.as_str());
        if metadata_file.run_seq < payload.base_seq || metadata_file.run_seq > payload.manifest_seq
        {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: object_key.to_owned(),
                message: format!(
                    "metadata file `{}` run seq {:?} is outside [{:?}, {:?}]",
                    metadata_file.table_id,
                    metadata_file.run_seq,
                    payload.base_seq,
                    payload.manifest_seq
                ),
            });
        }
        saw_base_seq_file |= metadata_file.run_seq == payload.base_seq;
        saw_manifest_seq_file |= metadata_file.run_seq == payload.manifest_seq;
    }

    if !saw_base_seq_file {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at base_seq {:?}",
                payload.base_seq
            ),
        });
    }
    if !saw_manifest_seq_file {
        return Err(ManifestLoadError::RunManifestMismatch {
            object_key: object_key.to_owned(),
            message: format!(
                "namespace manifest has no metadata file at manifest_seq {:?}",
                payload.manifest_seq
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
    fields(phase = "load_manifest_tables", key_class = "manifest_table")
)]
fn load_manifest_materialization_from_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<MetadataState, ManifestLoadError> {
    let mut metadata_state = MetadataStateBuilder::default();
    validate_manifest_materialization_ranges(manifest_object_key, &manifest.payload)?;
    for run in runs_in_materialization_order(&manifest.payload) {
        append_manifest_tables_to_metadata(
            store,
            namespace_id,
            MetadataTableLoadContext {
                manifest_object_key,
                segment_seq_expectation: MetadataSstSeqExpectation::Descriptor,
                row_seq_min: None,
                row_seq_max: run.run_seq,
            },
            &run.tables,
            &mut metadata_state,
        )?;
    }

    Ok(metadata_state.finish())
}

#[derive(Clone, Copy)]
struct MetadataTableLoadContext<'a> {
    manifest_object_key: &'a str,
    segment_seq_expectation: MetadataSstSeqExpectation,
    row_seq_min: Option<ChangeSeq>,
    row_seq_max: ChangeSeq,
}

#[derive(Clone, Copy)]
enum MetadataSstSeqExpectation {
    Descriptor,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataTableCacheMode {
    Bypass,
    Populate,
    ReadOnly,
}

impl MetadataTableLoadContext<'_> {
    fn expected_segment_seq(
        &self,
        descriptor: &MetadataFileRef,
    ) -> Result<ChangeSeq, ManifestLoadError> {
        match self.segment_seq_expectation {
            MetadataSstSeqExpectation::Descriptor => {
                if descriptor.run_seq > self.row_seq_max {
                    return Err(ManifestLoadError::SegmentSeqMismatch {
                        object_key: descriptor.object_key.clone(),
                        expected: self.row_seq_max,
                        actual: descriptor.run_seq,
                    });
                }
                Ok(descriptor.run_seq)
            }
        }
    }

    fn row_seq_max(&self, descriptor: &MetadataFileRef) -> ChangeSeq {
        match self.segment_seq_expectation {
            MetadataSstSeqExpectation::Descriptor => descriptor.run_seq,
        }
    }
}

fn metadata_file_object_key(descriptor: &MetadataFileRef) -> String {
    metadata_sst(
        descriptor.owner_namespace_id.as_str(),
        descriptor.table_id.as_str(),
    )
}

fn append_manifest_tables_to_metadata<S>(
    store: &S,
    _namespace_id: &NamespaceId,
    context: MetadataTableLoadContext<'_>,
    tables: &[MetadataTableManifest],
    metadata_state: &mut MetadataStateBuilder,
) -> Result<(), ManifestLoadError>
where
    S: ObjectStore + ?Sized,
{
    let ordered_tables = ordered_manifest_tables(context.manifest_object_key, tables)?;
    let mut direntry_bind_rows = Vec::new();
    let mut direntry_child_bind_rows = Vec::new();
    for table in ordered_tables {
        for descriptor in &table.segments {
            context.expected_segment_seq(descriptor)?;
            let expected_key = metadata_file_object_key(descriptor);
            if descriptor.object_key != expected_key {
                return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            let rows = load_manifest_segment_rows(store, context, table.family, descriptor)?;
            match table.family {
                MetadataTableFamily::DirentryBinds => {
                    direntry_bind_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::DirentryChildBinds => {
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

fn load_manifest_segment_rows<S: ObjectStore + ?Sized>(
    store: &S,
    context: MetadataTableLoadContext<'_>,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
) -> Result<Vec<MetadataRow>, ManifestLoadError> {
    load_manifest_segment_rows_with_cache(
        store,
        None,
        context,
        family,
        descriptor,
        MetadataTableCacheMode::Bypass,
    )
}

fn load_manifest_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    context: MetadataTableLoadContext<'_>,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    cache_mode: MetadataTableCacheMode,
) -> Result<Vec<MetadataRow>, ManifestLoadError> {
    let expected_segment_seq = context.expected_segment_seq(descriptor)?;
    let cache_key = MetadataTableCacheKey {
        table_digest: descriptor.payload_checksum_sha256.clone(),
        block_kind: MetadataTableBlockKind::SegmentPayload,
        block_offset: 0,
    };
    if cache_mode != MetadataTableCacheMode::Bypass {
        if let Some(block) = table_cache.and_then(|cache| cache.get(&cache_key)) {
            validate_cached_manifest_block(family, expected_segment_seq, descriptor, &block)?;
            validate_manifest_row_seq_range(
                &descriptor.object_key,
                &block.rows,
                context.row_seq_min,
                context.row_seq_max(descriptor),
            )?;
            return Ok(block.rows);
        }
    }

    let Some(bytes) =
        store
            .get(&descriptor.object_key, None)
            .map_err(|err| ManifestLoadError::ReadSegment {
                object_key: descriptor.object_key.clone(),
                message: err.to_string(),
            })?
    else {
        return Err(ManifestLoadError::MissingSegment {
            object_key: descriptor.object_key.clone(),
        });
    };
    let segment = decode_metadata_sst_envelope_zstd(&bytes).map_err(|err| {
        ManifestLoadError::SegmentCodec {
            object_key: descriptor.object_key.clone(),
            message: err.to_string(),
        }
    })?;
    let rows = validate_manifest_segment(expected_segment_seq, family, descriptor, &segment)?;
    validate_manifest_row_seq_range(
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
                    decoded_byte_len: decoded_manifest_block_weight(family, &rows),
                },
            );
        }
    }
    Ok(rows)
}

fn validate_cached_manifest_block(
    family: MetadataTableFamily,
    expected_segment_seq: ChangeSeq,
    descriptor: &MetadataFileRef,
    block: &DecodedMetadataTableBlock,
) -> Result<(), ManifestLoadError> {
    if block.segment_seq != expected_segment_seq {
        return Err(ManifestLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: expected_segment_seq,
            actual: block.segment_seq,
        });
    }
    if block.family != family {
        return Err(ManifestLoadError::SegmentFamilyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: family,
            actual: block.family,
        });
    }
    if block.segment_index != descriptor.segment_index {
        return Err(ManifestLoadError::SegmentIndexMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_index,
            actual: block.segment_index,
        });
    }
    if block.segment_key != descriptor.segment_key {
        return Err(ManifestLoadError::SegmentKeyMismatch {
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
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "cached segment descriptor mismatch".to_owned(),
        });
    }
    Ok(())
}

fn decoded_manifest_block_weight(family: MetadataTableFamily, rows: &[MetadataRow]) -> usize {
    let row_weight = rows
        .iter()
        .map(|row| 64 + row.row_key_for_family(family).len() + decoded_manifest_row_weight(row))
        .sum::<usize>();
    row_weight.saturating_add(128)
}

fn decoded_manifest_row_weight(row: &MetadataRow) -> usize {
    match row {
        MetadataRow::Inode { .. } => 32,
        MetadataRow::DirentryBind {
            name_key,
            display_name,
            ..
        } => 96 + name_key.len() + display_name.len(),
        MetadataRow::DirentryUnbind { name_key, .. } => 96 + name_key.len(),
        MetadataRow::Revision { content_ref, .. } => 96 + content_ref.digest.len(),
        MetadataRow::Tombstone { .. } => 32,
        MetadataRow::CommitReceipt {
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

fn ordered_manifest_tables<'a>(
    manifest_object_key: &str,
    tables: &'a [MetadataTableManifest],
) -> Result<Vec<&'a MetadataTableManifest>, ManifestLoadError> {
    let mut ordered = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut matching = tables.iter().filter(|table| table.family == family);
        let Some(table) = matching.next() else {
            return Err(ManifestLoadError::MissingTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        };
        if matching.next().is_some() {
            return Err(ManifestLoadError::DuplicateTableFamily {
                object_key: manifest_object_key.to_owned(),
                family,
            });
        }
        ordered.push(table);
    }
    Ok(ordered)
}

fn manifest_table_for_family<'a>(
    manifest_object_key: &str,
    tables: &'a [MetadataTableManifest],
    family: MetadataTableFamily,
) -> Result<&'a MetadataTableManifest, ManifestLoadError> {
    ordered_manifest_tables(manifest_object_key, tables)?
        .into_iter()
        .find(|table| table.family == family)
        .ok_or(ManifestLoadError::MissingTableFamily {
            object_key: manifest_object_key.to_owned(),
            family,
        })
}

fn count_matching_segments(
    manifest_object_key: &str,
    tables: &[MetadataTableManifest],
    family: MetadataTableFamily,
    prefix: &str,
) -> Result<usize, ManifestLoadError> {
    let table = manifest_table_for_family(manifest_object_key, tables, family)?;
    Ok(table
        .segments
        .iter()
        .filter(|descriptor| descriptor_may_contain_prefix(descriptor, prefix))
        .count())
}

fn descriptor_may_contain_prefix(descriptor: &MetadataFileRef, prefix: &str) -> bool {
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

fn validate_manifest_segment(
    manifest_seq: ChangeSeq,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    segment: &MetadataSstEnvelope,
) -> Result<Vec<MetadataRow>, ManifestLoadError> {
    if segment.payload.namespace_id != descriptor.owner_namespace_id {
        return Err(ManifestLoadError::SegmentNamespaceMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.owner_namespace_id.clone(),
            actual: segment.payload.namespace_id.clone(),
        });
    }
    if segment.payload.table_id != descriptor.table_id {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "table id mismatch: expected `{}`, actual `{}`",
                descriptor.table_id, segment.payload.table_id
            ),
        });
    }
    if segment.payload.run_seq != manifest_seq {
        return Err(ManifestLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: manifest_seq,
            actual: segment.payload.run_seq,
        });
    }
    if segment.payload.level != descriptor.level {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: format!(
                "level mismatch: expected `{}`, actual `{}`",
                descriptor.level, segment.payload.level
            ),
        });
    }
    if segment.payload.family != family {
        return Err(ManifestLoadError::SegmentFamilyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: family,
            actual: segment.payload.family,
        });
    }
    if segment.payload.segment_index != descriptor.segment_index {
        return Err(ManifestLoadError::SegmentIndexMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_index,
            actual: segment.payload.segment_index,
        });
    }
    if segment.payload.segment_key != descriptor.segment_key {
        return Err(ManifestLoadError::SegmentKeyMismatch {
            object_key: descriptor.object_key.clone(),
            expected: descriptor.segment_key.clone(),
            actual: segment.payload.segment_key.clone(),
        });
    }

    let actual_payload_checksum =
        metadata_sst_payload_checksum_sha256(&segment.payload).map_err(|err| {
            ManifestLoadError::SegmentCodec {
                object_key: descriptor.object_key.clone(),
                message: err.to_string(),
            }
        })?;
    if descriptor.payload_checksum_sha256 != actual_payload_checksum {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
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
        let checksum =
            metadata_page_checksum_sha256(page).map_err(|err| ManifestLoadError::SegmentCodec {
                object_key: descriptor.object_key.clone(),
                message: err.to_string(),
            })?;
        collected_page_checksums.push(checksum.clone());
        validate_manifest_page(family, descriptor, page, &checksum)?;
        collected_rows.extend(page.rows.iter().cloned());
    }

    if descriptor.page_checksums_sha256 != collected_page_checksums {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "page checksum descriptor mismatch".to_owned(),
        });
    }

    if segment.payload.row_count != collected_rows.len() as u64 {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
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
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
                object_key: descriptor.object_key.clone(),
                message: "payload min/max key mismatch".to_owned(),
            });
        }
    } else if segment.payload.row_count != 0 {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "non-zero row count with no rows".to_owned(),
        });
    }

    if descriptor.row_count != segment.payload.row_count
        || descriptor.min_key != segment.payload.min_key
        || descriptor.max_key != segment.payload.max_key
    {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "descriptor row summary mismatch".to_owned(),
        });
    }

    Ok(collected_rows)
}

fn validate_manifest_page(
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    page: &MetadataPage,
    checksum: &str,
) -> Result<(), ManifestLoadError> {
    if page.row_keys.len() != page.rows.len() {
        return Err(ManifestLoadError::PageShapeMismatch {
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
        return Err(ManifestLoadError::PageChecksumMismatch {
            object_key: descriptor.object_key.clone(),
            page_index: page.page_index,
            expected: expected_checksum,
            actual: checksum.to_owned(),
        });
    }

    for (index, row) in page.rows.iter().enumerate() {
        if !manifest_row_matches_family(row, family) {
            return Err(ManifestLoadError::TableRowKindMismatch {
                object_key: descriptor.object_key.clone(),
                family,
                row_kind: manifest_row_kind(row).to_owned(),
            });
        }
        let actual = row.row_key_for_family(family);
        let expected = page.row_keys.get(index).cloned().unwrap_or_default();
        if actual != expected {
            return Err(ManifestLoadError::RowKeyMismatch {
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
            return Err(ManifestLoadError::PageShapeMismatch {
                object_key: descriptor.object_key.clone(),
                page_index: page.page_index,
                message: "page min/max key mismatch".to_owned(),
            });
        }
    }

    Ok(())
}

fn validate_manifest_row_seq_range(
    object_key: &str,
    rows: &[MetadataRow],
    min_seq: Option<ChangeSeq>,
    max_seq: ChangeSeq,
) -> Result<(), ManifestLoadError> {
    for (row_index, row) in rows.iter().enumerate() {
        let row_seq = manifest_row_commit_seq(row);
        if let Some(min_seq) = min_seq {
            if row_seq < min_seq {
                return Err(ManifestLoadError::SegmentDescriptorMismatch {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "row {row_index} seq `{row_seq:?}` is before expected min `{min_seq:?}`"
                    ),
                });
            }
        }
        if row_seq > max_seq {
            return Err(ManifestLoadError::SegmentDescriptorMismatch {
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
    family: MetadataTableFamily,
    object_key: &str,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    for row in rows {
        match (family, row) {
            (
                MetadataTableFamily::Inodes,
                MetadataRow::Inode {
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
                MetadataTableFamily::DirentryBinds,
                MetadataRow::DirentryBind {
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
            (MetadataTableFamily::DirentryChildBinds, MetadataRow::DirentryBind { .. }) => {}
            (
                MetadataTableFamily::DirentryUnbinds,
                MetadataRow::DirentryUnbind {
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
                MetadataTableFamily::Revisions,
                MetadataRow::Revision {
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
                MetadataTableFamily::Tombstones,
                MetadataRow::Tombstone {
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
                MetadataTableFamily::CommitReceipts,
                MetadataRow::CommitReceipt {
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
                return Err(ManifestLoadError::TableRowKindMismatch {
                    object_key: object_key.to_owned(),
                    family,
                    row_kind: manifest_row_kind(row).to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn validate_direntry_child_bind_index(
    object_key: &str,
    mut direntry_bind_rows: Vec<MetadataRow>,
    mut direntry_child_bind_rows: Vec<MetadataRow>,
) -> Result<(), ManifestLoadError> {
    direntry_bind_rows
        .sort_by_key(|row| row.row_key_for_family(MetadataTableFamily::DirentryChildBinds));
    direntry_child_bind_rows
        .sort_by_key(|row| row.row_key_for_family(MetadataTableFamily::DirentryChildBinds));

    if direntry_bind_rows != direntry_child_bind_rows {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: object_key.to_owned(),
            message: "direntry-child-binds index does not match canonical direntry-binds"
                .to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // These tests use panic in impossible match arms to preserve precise failure messages.

    use super::{
        advance_retention_floor, build_manifest_tables, build_manifest_tables_from_rows,
        build_namespace_manifest_for_basis, create_checkpoint, create_checkpoint_with_policy,
        flatten_manifest_tables, load_manifest_materialization_from_manifest,
        load_verified_manifest_materialization, manifest_basis_head, manifest_rows_for_family,
        metadata_states_equivalent, publish_checkpoint_hint_seq, runs_from_metadata_files,
        write_namespace_manifest, ManifestLoadError, MetadataLsmPolicy, MetadataRunManifest,
        MetadataTableCache, MetadataTableCacheConfig, MetadataTableSegmentation,
        CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
        DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT, MAX_CHECKPOINT_L0_RUNS,
    };
    use crate::error::CoreError;
    use crate::metadata::MetadataState;
    use crate::namespace::basis::{load_verified_namespace_basis, BasisLoadError};
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::ops::{move_path, put_file_bytes, write_file_bytes};
    use crate::{ErrorCode, MutationContext, PutFileBehavior};
    use loon_api::wire::manifest::{
        encode_metadata_sst_envelope_zstd, encode_namespace_manifest_json, MetadataFileRef,
        MetadataPage, MetadataRow, MetadataSegmentKey, MetadataSstEnvelope, MetadataSstPayload,
        MetadataTableFamily as ApiMetadataTableFamily, NamespaceManifestEnvelope,
        NamespaceManifestPayload,
    };
    use loon_api::{validate_checkpoint_id, ChangeSeq, NamespaceId};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{metadata_sst, namespace_head, namespace_manifest};
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn manifest_round_trip_uses_manifest_basis_for_mixed_namespace() {
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
    fn manifest_round_trip_preserves_direntry_unbind_rows() {
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
    fn manifest_round_trip_supports_empty_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(basis.head.checkpoint_hint_seq, Some(ChangeSeq(0)));
        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(0))
                .expect("load namespace manifest");
        assert_eq!(materialized.manifest.payload.checkpoints.len(), 1);
        let checkpoint = &materialized.manifest.payload.checkpoints[0];
        assert!(validate_checkpoint_id(&checkpoint.checkpoint_id).is_ok());
        assert_eq!(checkpoint.checkpoint_seq, ChangeSeq(0));
        assert_eq!(checkpoint.manifest_seq, ChangeSeq(0));
    }

    #[test]
    fn strict_manifest_consumption_fails_when_manifest_is_corrupted() {
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

        let manifest_key = namespace_manifest(namespace_id.as_str(), 1);
        store
            .put_overwrite(&manifest_key, br#"{"bad":"json"}"#)
            .expect("corrupt manifest");

        match load_verified_namespace_basis(&store, &namespace_id) {
            Err(BasisLoadError::ManifestLoad(ManifestLoadError::ManifestCodec { .. })) => {}
            other => panic!("expected manifest codec manifest load error, got {other:?}"),
        }
    }

    #[test]
    fn create_checkpoint_surfaces_conflicting_invalid_manifest() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        let manifest_key = namespace_manifest(namespace_id.as_str(), 1);
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
            Err(CoreError::Basis(BasisLoadError::ManifestLoad(
                ManifestLoadError::ManifestCodec { .. },
            ))) => {}
            other => panic!("expected manifest codec manifest load error, got {other:?}"),
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
            other => panic!("expected manifest unavailable, got {other:?}"),
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
            .head(&namespace_manifest(namespace_id.as_str(), 1))
            .expect("manifest head")
            .is_some());
    }

    #[test]
    fn manifest_materialization_uses_written_segments() {
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
            load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(1))
                .expect("load materialized manifest");
        let current = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let basis_head = manifest_basis_head(&current.head, &materialized.manifest);
        assert_eq!(basis_head.seq, ChangeSeq(1));
        assert!(metadata_states_equivalent(
            &materialized.metadata_state,
            &current.metadata_state
        ));

        let segment_key = base_segment_object_keys_for_family(
            &materialized.manifest,
            ApiMetadataTableFamily::Revisions,
        )
        .into_iter()
        .next()
        .expect("revision segment");
        assert!(store.head(&segment_key).expect("head segment").is_some());
    }

    #[test]
    fn manifest_l0_run_materialization_matches_full_basis() {
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
            load_verified_manifest_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first manifest");

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
            load_verified_manifest_materialization(&store, &namespace_id, second.checkpoint_seq)
                .expect("load second manifest");

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
        assert_eq!(second_materialized.manifest.payload.checkpoints.len(), 2);
        assert_eq!(
            second_materialized.manifest.payload.checkpoints[0].checkpoint_id,
            first.checkpoint_id
        );
        assert_eq!(
            second_materialized.manifest.payload.checkpoints[1].checkpoint_id,
            second.checkpoint_id
        );
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &second_materialized.metadata_state
        ));
    }

    #[test]
    fn manifest_l0_run_missing_table_fails_load() {
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
            load_verified_manifest_materialization(&store, &namespace_id, second.checkpoint_seq)
                .expect("load materialized manifest");
        let deleted_key = l0_runs(&materialized.manifest)[0]
            .tables
            .iter()
            .flat_map(|table| table.segments.iter())
            .next()
            .expect("l0 run segment")
            .object_key
            .clone();
        store.delete(&deleted_key).expect("delete l0 segment");

        match load_verified_manifest_materialization(&store, &namespace_id, second.checkpoint_seq) {
            Err(ManifestLoadError::MissingSegment { object_key }) => {
                assert_eq!(object_key, deleted_key);
            }
            other => panic!("expected missing l0 segment, got {other:?}"),
        }
    }

    #[test]
    fn manifest_materialization_rejects_off_pattern_table_keys() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");
        let first = write_file_and_checkpoint(&store, &namespace_id, &context, 1);
        let first_materialized =
            load_verified_manifest_materialization(&store, &namespace_id, first)
                .expect("load first manifest");
        let manifest_key = namespace_manifest(namespace_id.as_str(), first.0);
        let mut bad_base_manifest = first_materialized.manifest.clone();
        let expected_base_key = {
            let base_descriptor = bad_base_manifest
                .payload
                .metadata_files
                .iter_mut()
                .find(|metadata_file| {
                    metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                        && metadata_file.family == ApiMetadataTableFamily::Inodes
                })
                .expect("base metadata file");
            let expected = metadata_sst(
                base_descriptor.owner_namespace_id.as_str(),
                base_descriptor.table_id.as_str(),
            );
            base_descriptor.object_key = format!("{}-wrong", base_descriptor.object_key);
            expected
        };

        match load_manifest_materialization_from_manifest(
            &store,
            &namespace_id,
            &manifest_key,
            &bad_base_manifest,
        ) {
            Err(ManifestLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
                assert_eq!(expected, expected_base_key);
            }
            other => panic!("expected base table key mismatch, got {other:?}"),
        }

        let second = write_file_and_checkpoint(&store, &namespace_id, &context, 2);
        let second_materialized =
            load_verified_manifest_materialization(&store, &namespace_id, second)
                .expect("load second manifest");
        let manifest_key = namespace_manifest(namespace_id.as_str(), second.0);
        let mut bad_l0_manifest = second_materialized.manifest.clone();
        let expected_l0_key = {
            let l0_descriptor = bad_l0_manifest
                .payload
                .metadata_files
                .iter_mut()
                .find(|metadata_file| {
                    metadata_file.level == CHECKPOINT_L0_RUN_LEVEL
                        && metadata_file.family == ApiMetadataTableFamily::Inodes
                })
                .expect("l0 metadata file");
            let expected = metadata_sst(
                l0_descriptor.owner_namespace_id.as_str(),
                l0_descriptor.table_id.as_str(),
            );
            l0_descriptor.object_key = format!("{}-wrong", l0_descriptor.object_key);
            expected
        };

        match load_manifest_materialization_from_manifest(
            &store,
            &namespace_id,
            &manifest_key,
            &bad_l0_manifest,
        ) {
            Err(ManifestLoadError::SegmentObjectKeyMismatch { expected, .. }) => {
                assert_eq!(expected, expected_l0_key);
            }
            other => panic!("expected l0 table key mismatch, got {other:?}"),
        }
    }

    #[test]
    fn manifest_run_rejects_rows_after_run_seq() {
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
        let malformed_run_tables = build_manifest_tables_from_rows(
            &store,
            &namespace_id,
            first,
            CHECKPOINT_BASE_RUN_LEVEL,
            &context.writer_version,
            |family| manifest_rows_for_family(&basis.metadata_state, family),
            MetadataTableSegmentation::Full,
        )
        .expect("write malformed run tables");
        let metadata_ssts = build_manifest_tables_from_rows(
            &store,
            &namespace_id,
            basis.head.seq,
            CHECKPOINT_L0_RUN_LEVEL,
            &context.writer_version,
            |family| {
                super::manifest_rows_for_family_after_seq(&basis.metadata_state, family, first)
            },
            MetadataTableSegmentation::Full,
        )
        .expect("write empty metadata run tables");
        let mut metadata_files = flatten_manifest_tables(malformed_run_tables);
        metadata_files.extend(flatten_manifest_tables(metadata_ssts));
        let manifest = NamespaceManifestEnvelope::from_payload(
            &context.writer_version,
            NamespaceManifestPayload {
                namespace_id: namespace_id.clone(),
                manifest_seq: basis.head.seq,
                base_seq: first,
                active_fence_token: basis.head.active_fence_token,
                next_inode_id: basis.head.next_inode_id,
                retention_floor_seq: basis.head.retention_floor_seq,
                verified: true,
                fork: None,
                checkpoints: Vec::new(),
                metadata_files,
            },
        )
        .expect("build malformed manifest");

        match load_manifest_materialization_from_manifest(
            &store,
            &namespace_id,
            &namespace_manifest(namespace_id.as_str(), basis.head.seq.0),
            &manifest,
        ) {
            Err(ManifestLoadError::SegmentDescriptorMismatch { message, .. }) => {
                assert!(message.contains("after expected max"));
            }
            other => panic!("expected row range mismatch, got {other:?}"),
        }
    }

    #[test]
    fn manifest_l0_runs_chain_across_successive_manifests() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        for index in 1..=4 {
            write_file_and_checkpoint(&store, &namespace_id, &context, index);
        }

        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(4))
                .expect("load chained manifest");
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
    fn manifest_policy_compacts_when_l0_runs_exceed_threshold() {
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

        let capped = load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(3))
            .expect("load capped manifest");
        assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
        assert_eq!(l0_runs(&capped.manifest).len(), 2);

        let compacted = load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(4))
            .expect("load compacted manifest");
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(compacted.manifest.payload.base_seq, ChangeSeq(4));
        assert!(l0_runs(&compacted.manifest).is_empty());
        assert_eq!(
            runs_from_metadata_files(&compacted.manifest.payload).len(),
            1
        );
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &compacted.metadata_state
        ));
    }

    #[test]
    fn manifest_rejects_segment_descriptor_payload_key_mismatch() {
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

        let manifest = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, manifest.checkpoint_seq)
                .expect("load manifest");
        let mut manifest = materialized.manifest;
        let descriptor = manifest
            .payload
            .metadata_files
            .iter_mut()
            .find(|metadata_file| {
                metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                    && metadata_file.family == ApiMetadataTableFamily::Revisions
            })
            .expect("revision metadata file");
        descriptor.segment_key = MetadataSegmentKey::RowKeyRange { shard: u32::MAX };

        let manifest_key =
            namespace_manifest(namespace_id.as_str(), manifest.payload.manifest_seq.0);
        let writer_version = manifest.writer_version.clone();
        let manifest_seq = manifest.payload.manifest_seq;
        let updated_manifest =
            NamespaceManifestEnvelope::from_payload(writer_version, manifest.payload)
                .expect("updated manifest");
        let manifest_bytes =
            encode_namespace_manifest_json(&updated_manifest).expect("encode manifest");
        store
            .put_overwrite(&manifest_key, &manifest_bytes)
            .expect("overwrite manifest");

        match load_verified_manifest_materialization(&store, &namespace_id, manifest_seq) {
            Err(ManifestLoadError::SegmentKeyMismatch { .. }) => {}
            other => panic!("expected segment key mismatch, got {other:?}"),
        }
    }

    #[test]
    fn manifest_base_run_tables_have_sorted_segment_coverage() {
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

        let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("manifest");
        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, manifest.checkpoint_seq)
                .expect("load manifest");

        let base = base_run(&materialized.manifest);
        let revisions = base
            .tables
            .iter()
            .find(|table| table.family == ApiMetadataTableFamily::Revisions)
            .expect("revision table");
        assert!(revisions.segments.len() >= 3);
        assert!(revisions.segments.iter().all(|descriptor| {
            matches!(
                descriptor.segment_key,
                MetadataSegmentKey::RowKeyRange { .. }
            )
        }));

        let direntries = base
            .tables
            .iter()
            .find(|table| table.family == ApiMetadataTableFamily::DirentryBinds)
            .expect("direntry table");
        assert!(direntries.segments.iter().all(|descriptor| {
            matches!(
                descriptor.segment_key,
                MetadataSegmentKey::DirentryParent { .. }
            )
        }));

        for table in &base.tables {
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
        let manifest = create_checkpoint_with_policy(&store, &namespace_id, &context, policy)
            .expect("manifest");
        let cache = super::MetadataTableCache::new(Default::default());
        let tables = super::load_verified_manifest_tables_with_cache(
            &store,
            Some(&cache),
            &namespace_id,
            manifest.checkpoint_seq,
        )
        .expect("load tables");

        let before = cache.stats();
        let revisions = tables
            .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
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
        let manifest = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let cache = MetadataTableCache::new(MetadataTableCacheConfig {
            enabled: true,
            max_blocks: 256,
            max_decoded_bytes: Some(1),
        });
        let tables = super::load_verified_manifest_tables_with_cache(
            &store,
            Some(&cache),
            &namespace_id,
            manifest.checkpoint_seq,
        )
        .expect("load tables");

        let key = "inode-00000000000000000001";
        assert!(tables
            .get(ApiMetadataTableFamily::Inodes, key)
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
            load_verified_manifest_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first manifest");
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
        let compacted_materialized =
            load_verified_manifest_materialization(&store, &namespace_id, compacted.checkpoint_seq)
                .expect("load compacted manifest");
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
        let compacted_run_prefix = format!(
            "namespaces/{}/compacted/metadata/tbl_",
            namespace_id.as_str()
        );

        assert_eq!(
            compacted_materialized.manifest.payload.base_seq,
            compacted.checkpoint_seq
        );
        assert!(l0_runs(&compacted_materialized.manifest).is_empty());
        assert_eq!(
            runs_from_metadata_files(&compacted_materialized.manifest.payload).len(),
            1
        );
        assert!(!compacted_run_keys.is_empty());
        assert!(compacted_run_keys
            .iter()
            .all(|key| key.starts_with(&compacted_run_prefix)));
        assert!(compacted_run_keys
            .iter()
            .all(|key| !first_run_keys.contains(key)));
        assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
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
            load_verified_manifest_materialization(&store, &namespace_id, first.checkpoint_seq)
                .expect("load first manifest");
        let revision_keys_before = base_segment_object_keys_for_family(
            &first_materialized.manifest,
            ApiMetadataTableFamily::Revisions,
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
        let compacted_materialized =
            load_verified_manifest_materialization(&store, &namespace_id, compacted.checkpoint_seq)
                .expect("load compacted manifest");
        let revision_keys_after = base_segment_object_keys_for_family(
            &compacted_materialized.manifest,
            ApiMetadataTableFamily::Revisions,
        );
        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");

        assert!(l0_runs(&compacted_materialized.manifest).is_empty());
        assert_eq!(
            runs_from_metadata_files(&compacted_materialized.manifest.payload).len(),
            1
        );
        assert!(revision_keys_after
            .iter()
            .all(|key| !revision_keys_before.contains(key)));
        assert!(metadata_states_equivalent(
            &basis_after.metadata_state,
            &compacted_materialized.metadata_state
        ));
        assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
    }

    #[test]
    fn manifest_writes_and_validates_direntry_child_bind_index() {
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

        let manifest = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, manifest.checkpoint_seq)
                .expect("load manifest");
        let base = base_run(&materialized.manifest);
        let child_table = base
            .tables
            .iter()
            .find(|table| {
                table.family == loon_api::wire::manifest::MetadataTableFamily::DirentryChildBinds
            })
            .expect("child bind table");
        let child_segment = child_table.segments.first().expect("child bind segment");
        assert!(child_segment
            .min_key
            .starts_with("direntry-child-000000000000000000"));

        let deleted_key = child_segment.object_key.clone();
        store.delete(&deleted_key).expect("delete child index");
        match load_verified_manifest_materialization(&store, &namespace_id, manifest.checkpoint_seq)
        {
            Err(ManifestLoadError::MissingSegment { object_key }) => {
                assert_eq!(object_key, deleted_key);
            }
            other => panic!("expected missing child-bind segment, got {other:?}"),
        }
    }

    #[test]
    fn manifest_rejects_child_bind_index_that_diverges_from_canonical_binds() {
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

        let manifest = create_checkpoint(&store, &namespace_id, &context).expect("checkpoint");
        let materialized =
            load_verified_manifest_materialization(&store, &namespace_id, manifest.checkpoint_seq)
                .expect("load manifest before corruption");
        let mut manifest = materialized.manifest;
        let mut child_index_rows = manifest_rows_for_family(
            &materialized.metadata_state,
            ApiMetadataTableFamily::DirentryChildBinds,
        );
        assert!(child_index_rows.len() >= 2);
        child_index_rows[0] = child_index_rows[1].clone();
        child_index_rows
            .sort_by_key(|row| row.row_key_for_family(ApiMetadataTableFamily::DirentryChildBinds));

        let child_descriptor = manifest
            .payload
            .metadata_files
            .iter_mut()
            .find(|metadata_file| {
                metadata_file.level == CHECKPOINT_BASE_RUN_LEVEL
                    && metadata_file.family == ApiMetadataTableFamily::DirentryChildBinds
            })
            .expect("child bind metadata file");
        rewrite_manifest_segment(
            &store,
            &namespace_id,
            manifest.payload.manifest_seq,
            ApiMetadataTableFamily::DirentryChildBinds,
            child_descriptor,
            child_index_rows,
            &context.writer_version,
        );

        let manifest_key =
            namespace_manifest(namespace_id.as_str(), manifest.payload.manifest_seq.0);
        let writer_version = manifest.writer_version.clone();
        let manifest_seq = manifest.payload.manifest_seq;
        let updated_manifest =
            NamespaceManifestEnvelope::from_payload(writer_version, manifest.payload)
                .expect("updated manifest");
        let manifest_bytes =
            encode_namespace_manifest_json(&updated_manifest).expect("encode updated manifest");
        store
            .put_overwrite(&manifest_key, &manifest_bytes)
            .expect("overwrite manifest");

        assert_child_index_mismatch(load_verified_manifest_materialization(
            &store,
            &namespace_id,
            manifest_seq,
        ));
    }

    fn rewrite_manifest_segment(
        store: &LocalFsStore,
        _namespace_id: &NamespaceId,
        manifest_seq: ChangeSeq,
        family: ApiMetadataTableFamily,
        descriptor: &mut MetadataFileRef,
        rows: Vec<MetadataRow>,
        writer_version: &str,
    ) {
        let row_keys = rows
            .iter()
            .map(|row| row.row_key_for_family(family))
            .collect::<Vec<_>>();
        let min_key = row_keys.first().cloned().unwrap_or_default();
        let max_key = row_keys.last().cloned().unwrap_or_default();
        let page = MetadataPage {
            page_index: 0,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            row_keys,
            rows,
        };
        let payload = MetadataSstPayload {
            namespace_id: descriptor.owner_namespace_id.clone(),
            table_id: descriptor.table_id.clone(),
            run_seq: manifest_seq,
            level: descriptor.level,
            family,
            segment_index: descriptor.segment_index,
            segment_key: descriptor.segment_key.clone(),
            row_count: page.rows.len() as u64,
            min_key,
            max_key,
            pages: vec![page],
        };
        let envelope =
            MetadataSstEnvelope::from_payload(writer_version, payload).expect("rewritten segment");
        let encoded =
            encode_metadata_sst_envelope_zstd(&envelope).expect("encode rewritten segment");
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

    fn assert_child_index_mismatch<T>(result: Result<T, ManifestLoadError>) {
        match result {
            Err(ManifestLoadError::SegmentDescriptorMismatch { message, .. }) => {
                assert!(message.contains("direntry-child-binds index"));
            }
            Err(other) => panic!("expected child index mismatch, got {other:?}"),
            Ok(_) => panic!("expected child index mismatch"),
        }
    }

    fn base_run(manifest: &NamespaceManifestEnvelope) -> MetadataRunManifest {
        runs_from_metadata_files(&manifest.payload)
            .into_iter()
            .find(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
            .expect("base run")
    }

    fn l0_runs(manifest: &NamespaceManifestEnvelope) -> Vec<MetadataRunManifest> {
        runs_from_metadata_files(&manifest.payload)
            .into_iter()
            .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
            .collect()
    }

    fn run_segment_object_keys(manifest: &NamespaceManifestEnvelope) -> Vec<String> {
        runs_from_metadata_files(&manifest.payload)
            .into_iter()
            .flat_map(|run| {
                run.tables.into_iter().flat_map(|table| {
                    table
                        .segments
                        .into_iter()
                        .map(|descriptor| descriptor.object_key.clone())
                })
            })
            .collect()
    }

    fn base_segment_object_keys_for_family(
        manifest: &NamespaceManifestEnvelope,
        family: ApiMetadataTableFamily,
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

    fn assert_manifest_rows_have_unique_keys(metadata_state: &MetadataState) {
        for family in CHECKPOINT_TABLE_FAMILIES {
            let rows = manifest_rows_for_family(metadata_state, family);
            let mut seen = BTreeSet::new();
            for row in rows {
                let row_key = row.row_key_for_family(family);
                assert!(
                    seen.insert(row_key.clone()),
                    "duplicate metadata row key `{row_key}` in {family:?}"
                );
            }
        }
    }

    #[test]
    fn manifest_l0_run_cap_collapses_back_to_base_manifest() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        for index in 1..=10 {
            write_file_and_checkpoint(&store, &namespace_id, &context, index);
        }

        let capped = load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(9))
            .expect("load capped manifest");
        assert_eq!(capped.manifest.payload.base_seq, ChangeSeq(1));
        assert_eq!(l0_runs(&capped.manifest).len(), MAX_CHECKPOINT_L0_RUNS);

        let collapsed =
            load_verified_manifest_materialization(&store, &namespace_id, ChangeSeq(10))
                .expect("load collapsed manifest");
        assert_eq!(collapsed.manifest.payload.base_seq, ChangeSeq(10));
        assert!(l0_runs(&collapsed.manifest).is_empty());
        assert_eq!(
            runs_from_metadata_files(&collapsed.manifest.payload).len(),
            1
        );
    }

    #[test]
    fn unreferenced_manifest_run_is_ignored_by_basis_load() {
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
        let orphan_manifest = build_namespace_manifest_for_basis(
            &store,
            &namespace_id,
            &basis_before,
            &context.writer_version,
            MetadataLsmPolicy::default(),
            None,
        )
        .expect("build orphan manifest");
        write_namespace_manifest(&store, &orphan_manifest).expect("write orphan manifest");

        let basis_after = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(
            basis_after.head.checkpoint_hint_seq,
            Some(first.checkpoint_seq)
        );
        assert_eq!(basis_after.head.seq, orphan_manifest.payload.manifest_seq);
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
        let tables = build_manifest_tables(
            &store,
            &namespace_id,
            basis_before.head.seq,
            CHECKPOINT_BASE_RUN_LEVEL,
            &basis_before.metadata_state,
            &context.writer_version,
            MetadataLsmPolicy::default().max_rows_per_segment,
        )
        .expect("build metadata tables");
        let manifest = NamespaceManifestEnvelope::from_payload(
            &context.writer_version,
            NamespaceManifestPayload {
                namespace_id: namespace_id.clone(),
                manifest_seq: basis_before.head.seq,
                base_seq: basis_before.head.seq,
                active_fence_token: basis_before.head.active_fence_token,
                next_inode_id: basis_before.head.next_inode_id,
                retention_floor_seq: basis_before.head.retention_floor_seq,
                verified: true,
                fork: None,
                checkpoints: Vec::new(),
                metadata_files: flatten_manifest_tables(tables),
            },
        )
        .expect("build manifest");
        write_namespace_manifest(&store, &manifest).expect("write manifest");

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
        .expect("publish checkpoint hint");

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

        assert_eq!(error.code(), ErrorCode::StaleHead);
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
