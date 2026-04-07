use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::loading::read_head_object;
use crate::metadata::{
    DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::services::{write_immutable_object, CoreError};
use loon_api::{
    checkpoint_page_checksum_sha256, checkpoint_segment_payload_checksum_sha256,
    decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
    encode_checkpoint_manifest_json, encode_checkpoint_segment_envelope_zstd,
    AdvanceRetentionResponse, ChangeSeq, CheckpointManifestEnvelope, CheckpointManifestPayload,
    CheckpointPage, CheckpointRow, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
    CheckpointSegmentPayload, CheckpointTableFamily, CheckpointTableManifest, ControlObjectKind,
    CreateCheckpointResponse, HeadState, HeadStateEnvelope, NamespaceId,
};
use loon_objectstore::keys::{
    derived_progress, snapshot_manifest, snapshot_table, SnapshotTableFamily,
};
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const HEAD_UPDATE_RETRY_LIMIT: usize = 8;
// V1 does not require any derived work classes to be caught up before the
// retention floor advances. This hook stays in place so future retention gates
// can add progress requirements without restructuring the flow.
const REQUIRED_RETENTION_PROGRESS_CLASSES: &[&str] = &[];
const CHECKPOINT_TABLE_FAMILIES: [CheckpointTableFamily; 4] = [
    CheckpointTableFamily::Inodes,
    CheckpointTableFamily::Direntries,
    CheckpointTableFamily::Revisions,
    CheckpointTableFamily::Tombstones,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LoadedCheckpointMaterialization {
    pub(crate) manifest: CheckpointManifestEnvelope,
    pub(crate) metadata_state: MetadataState,
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

pub fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &crate::services::MutationContext,
) -> Result<CreateCheckpointResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let checkpoint_seq = basis.head.seq;

    match load_verified_checkpoint_materialization_if_present(store, namespace_id, checkpoint_seq) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let tables = build_checkpoint_tables(
                store,
                namespace_id,
                checkpoint_seq,
                &basis.metadata_state,
                &context.writer_version,
            )?;
            let materialized = load_checkpoint_materialization_from_tables(
                store,
                namespace_id,
                checkpoint_seq,
                &snapshot_manifest(namespace_id.as_str(), checkpoint_seq.0),
                &tables,
            )
            .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?;
            if !metadata_states_equivalent(&basis.metadata_state, &materialized) {
                return Err(CoreError::Basis(BasisLoadError::CheckpointLoad(
                    CheckpointLoadError::MetadataMismatch,
                )));
            }

            let manifest = CheckpointManifestEnvelope::from_payload(
                &context.writer_version,
                CheckpointManifestPayload {
                    namespace_id: namespace_id.clone(),
                    checkpoint_seq,
                    active_fence_token: basis.head.active_fence_token,
                    next_inode_id: basis.head.next_inode_id,
                    retention_floor_seq: basis.head.retention_floor_seq,
                    verified: true,
                    tables,
                },
            )
            .map_err(|err| CoreError::Store(err.to_string()))?;
            // `write_checkpoint_manifest` owns the idempotent "manifest already
            // exists" path. Any checkpoint load error it returns must surface.
            write_checkpoint_manifest(store, &manifest).map_err(CoreError::Basis)?;
        }
        Err(error) => return Err(CoreError::Basis(BasisLoadError::CheckpointLoad(error))),
    }

    let resulting_head =
        publish_snapshot_hint_seq(store, namespace_id, checkpoint_seq, &context.writer_version)?;

    Ok(CreateCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_seq,
        snapshot_hint_seq: resulting_head.snapshot_hint_seq,
        snapshot_hint_points_at_checkpoint: resulting_head.snapshot_hint_seq
            == Some(checkpoint_seq),
    })
}

pub fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &crate::services::MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    for _attempt in 0..HEAD_UPDATE_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let head = loaded_head.envelope.state;
        let Some(target_floor) = head.snapshot_hint_seq else {
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
            snapshot_hint_seq: head.snapshot_hint_seq,
            retention_floor_seq: target_floor,
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
            object_key: snapshot_manifest(namespace_id.as_str(), checkpoint_seq.0),
        })
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
        snapshot_hint_seq: current_head.snapshot_hint_seq,
        retention_floor_seq: current_head.retention_floor_seq,
    }
}

fn load_verified_checkpoint_materialization_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
) -> Result<Option<LoadedCheckpointMaterialization>, CheckpointLoadError> {
    let manifest_key = snapshot_manifest(namespace_id.as_str(), checkpoint_seq.0);
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
    validate_checkpoint_manifest(namespace_id, checkpoint_seq, &manifest_key, &manifest)?;
    let metadata_state = load_checkpoint_materialization_from_tables(
        store,
        namespace_id,
        checkpoint_seq,
        &manifest_key,
        &manifest.payload.tables,
    )?;
    Ok(Some(LoadedCheckpointMaterialization {
        manifest,
        metadata_state,
    }))
}

fn build_checkpoint_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    metadata_state: &MetadataState,
    writer_version: &str,
) -> Result<Vec<CheckpointTableManifest>, CoreError> {
    let mut tables = Vec::with_capacity(CHECKPOINT_TABLE_FAMILIES.len());
    for family in CHECKPOINT_TABLE_FAMILIES {
        let rows = checkpoint_rows_for_family(metadata_state, family);
        if rows.is_empty() {
            tables.push(CheckpointTableManifest {
                family,
                segments: Vec::new(),
            });
            continue;
        }

        let row_keys = rows.iter().map(CheckpointRow::row_key).collect::<Vec<_>>();
        let page = CheckpointPage {
            page_index: 0,
            min_key: row_keys.first().cloned().unwrap_or_default(),
            max_key: row_keys.last().cloned().unwrap_or_default(),
            row_keys,
            rows,
        };
        let payload = CheckpointSegmentPayload {
            namespace_id: namespace_id.clone(),
            checkpoint_seq,
            family,
            segment_index: 0,
            row_count: page.rows.len() as u64,
            min_key: page.min_key.clone(),
            max_key: page.max_key.clone(),
            pages: vec![page],
        };
        let envelope = CheckpointSegmentEnvelope::from_payload(writer_version, payload)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        let encoded = encode_checkpoint_segment_envelope_zstd(&envelope)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        let object_key = snapshot_table(
            namespace_id.as_str(),
            checkpoint_seq.0,
            snapshot_table_family(family),
            0,
        );
        write_immutable_object(store, &object_key, &encoded)?;
        tables.push(CheckpointTableManifest {
            family,
            segments: vec![CheckpointSegmentDescriptor {
                object_key,
                segment_index: 0,
                row_count: envelope.payload.row_count,
                min_key: envelope.payload.min_key.clone(),
                max_key: envelope.payload.max_key.clone(),
                payload_checksum_sha256: checkpoint_segment_payload_checksum_sha256(
                    &envelope.payload,
                )
                .map_err(|err| CoreError::Store(err.to_string()))?,
                page_checksums_sha256: envelope
                    .page_checksums_sha256()
                    .map_err(|err| CoreError::Store(err.to_string()))?,
            }],
        });
    }
    Ok(tables)
}

fn write_checkpoint_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    manifest: &CheckpointManifestEnvelope,
) -> Result<(), BasisLoadError> {
    let manifest_key = snapshot_manifest(
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

fn publish_snapshot_hint_seq<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    writer_version: &str,
) -> Result<HeadState, CoreError> {
    for _attempt in 0..HEAD_UPDATE_RETRY_LIMIT {
        let loaded_head = read_head_object(store, namespace_id)
            .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
        let current_head = loaded_head.envelope.state;
        if current_head.snapshot_hint_seq >= Some(checkpoint_seq) {
            return Ok(current_head);
        }

        let next_head = HeadState {
            namespace_id: current_head.namespace_id.clone(),
            seq: current_head.seq,
            active_fence_token: current_head.active_fence_token,
            next_inode_id: current_head.next_inode_id,
            name_policy: current_head.name_policy,
            snapshot_hint_seq: Some(checkpoint_seq),
            retention_floor_seq: current_head.retention_floor_seq,
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

    Err(CoreError::Store(
        "snapshot hint compare-and-swap retry exhausted".to_owned(),
    ))
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
        let object_key = derived_progress(namespace_id.as_str(), work_class);
        let Some(bytes) = store
            .get(&object_key, None)
            .map_err(|err| CoreError::Store(err.to_string()))?
        else {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class}` is missing for namespace `{}`",
                namespace_id.as_str()
            )));
        };
        let progress: loon_api::ProgressStateEnvelope =
            serde_json::from_slice(&bytes).map_err(|err| CoreError::Store(err.to_string()))?;
        if progress.state.through_seq < target_floor {
            return Err(CoreError::CheckpointUnavailable(format!(
                "required derived progress `{work_class}` only covers {:?} for namespace `{}`",
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

fn load_checkpoint_materialization_from_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    manifest_object_key: &str,
    tables: &[CheckpointTableManifest],
) -> Result<MetadataState, CheckpointLoadError> {
    let ordered_tables = ordered_checkpoint_tables(manifest_object_key, tables)?;
    let mut metadata_state = MetadataState::default();

    for table in ordered_tables {
        for descriptor in &table.segments {
            let expected_key = snapshot_table(
                namespace_id.as_str(),
                checkpoint_seq.0,
                snapshot_table_family(table.family),
                descriptor.segment_index,
            );
            if descriptor.object_key != expected_key {
                return Err(CheckpointLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
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
                checkpoint_seq,
                table.family,
                descriptor,
                &segment,
            )?;
            append_rows_to_metadata(
                &mut metadata_state,
                table.family,
                &descriptor.object_key,
                &rows,
            )?;
        }
    }

    Ok(metadata_state)
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
    if segment.payload.checkpoint_seq != checkpoint_seq {
        return Err(CheckpointLoadError::SegmentSeqMismatch {
            object_key: descriptor.object_key.clone(),
            expected: checkpoint_seq,
            actual: segment.payload.checkpoint_seq,
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
        validate_checkpoint_page(descriptor, page, &checksum)?;
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
        .map(CheckpointRow::row_key)
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
        let actual = row.row_key();
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

fn append_rows_to_metadata(
    metadata_state: &mut MetadataState,
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
            ) => metadata_state.inodes.push(InodeRecord {
                inode_id: *inode_id,
                inode_kind: inode_kind.clone(),
                created_seq: *created_seq,
            }),
            (
                CheckpointTableFamily::Direntries,
                CheckpointRow::Direntry {
                    parent_inode_id,
                    name_key,
                    display_name,
                    child_inode_id,
                    bind_seq,
                    bind_op_index,
                },
            ) => metadata_state.direntries.push(DirentryRecord {
                parent_inode_id: *parent_inode_id,
                name_key: name_key.clone(),
                display_name: display_name.clone(),
                child_inode_id: *child_inode_id,
                bind_seq: *bind_seq,
                bind_op_index: *bind_op_index,
            }),
            (
                CheckpointTableFamily::Revisions,
                CheckpointRow::Revision {
                    inode_id,
                    revision_no,
                    committed_seq,
                    revision_op_index,
                    content_manifest_digest,
                },
            ) => metadata_state.revisions.push(RevisionRecord {
                inode_id: *inode_id,
                revision_no: *revision_no,
                committed_seq: *committed_seq,
                revision_op_index: *revision_op_index,
                content_manifest_digest: content_manifest_digest.clone(),
            }),
            (
                CheckpointTableFamily::Tombstones,
                CheckpointRow::Tombstone {
                    root_inode_id,
                    tombstone_seq,
                    tombstone_op_index,
                },
            ) => metadata_state
                .subtree_tombstones
                .push(SubtreeTombstoneRecord {
                    root_inode_id: *root_inode_id,
                    tombstone_seq: *tombstone_seq,
                    tombstone_op_index: *tombstone_op_index,
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
            .inodes
            .iter()
            .map(|inode| CheckpointRow::Inode {
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind.clone(),
                created_seq: inode.created_seq,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Direntries => metadata_state
            .direntries
            .iter()
            .map(|direntry| CheckpointRow::Direntry {
                parent_inode_id: direntry.parent_inode_id,
                name_key: direntry.name_key.clone(),
                display_name: direntry.display_name.clone(),
                child_inode_id: direntry.child_inode_id,
                bind_seq: direntry.bind_seq,
                bind_op_index: direntry.bind_op_index,
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Revisions => metadata_state
            .revisions
            .iter()
            .map(|revision| CheckpointRow::Revision {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                revision_op_index: revision.revision_op_index,
                content_manifest_digest: revision.content_manifest_digest.clone(),
            })
            .collect::<Vec<_>>(),
        CheckpointTableFamily::Tombstones => metadata_state
            .subtree_tombstones
            .iter()
            .map(|tombstone| CheckpointRow::Tombstone {
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
                tombstone_op_index: tombstone.tombstone_op_index,
            })
            .collect::<Vec<_>>(),
    };
    rows.sort_by_key(CheckpointRow::row_key);
    rows
}

fn snapshot_table_family(family: CheckpointTableFamily) -> SnapshotTableFamily {
    match family {
        CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
        CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
        CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
    }
}

fn checkpoint_row_kind(row: &CheckpointRow) -> &'static str {
    match row {
        CheckpointRow::Inode { .. } => "inode",
        CheckpointRow::Direntry { .. } => "direntry",
        CheckpointRow::Revision { .. } => "revision",
        CheckpointRow::Tombstone { .. } => "tombstone",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        advance_retention_floor, build_checkpoint_tables, checkpoint_basis_head, create_checkpoint,
        load_verified_checkpoint_materialization, metadata_states_equivalent,
        publish_snapshot_hint_seq, write_checkpoint_manifest, CheckpointLoadError,
    };
    use crate::{
        bootstrap_namespace, load_verified_namespace_basis, put_file_bytes, write_file_bytes,
        BasisLoadError, CoreError, MutationContext, PutFileBehavior,
    };
    use loon_api::{ChangeSeq, CheckpointManifestEnvelope, CheckpointManifestPayload, NamespaceId};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{snapshot_manifest, snapshot_table, SnapshotTableFamily};
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[test]
    fn checkpoint_round_trip_uses_checkpoint_basis_for_mixed_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
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

        assert_eq!(after.head.snapshot_hint_seq, Some(before.head.seq));
        assert_eq!(before.head.seq, after.head.seq);
        assert!(metadata_states_equivalent(
            &before.metadata_state,
            &after.metadata_state
        ));
    }

    #[test]
    fn checkpoint_round_trip_supports_empty_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context, false).expect("bootstrap");

        create_checkpoint(&store, &namespace_id, &context).expect("create checkpoint");
        let basis = load_verified_namespace_basis(&store, &namespace_id).expect("basis");
        assert_eq!(basis.head.snapshot_hint_seq, Some(ChangeSeq(0)));
    }

    #[test]
    fn strict_checkpoint_consumption_fails_when_manifest_is_corrupted() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
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

        let manifest_key = snapshot_manifest(namespace_id.as_str(), 1);
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
        let namespace_id = NamespaceId::from("demo");
        let context = test_context();
        let manifest_key = snapshot_manifest(namespace_id.as_str(), 1);
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
        assert_eq!(basis.head.snapshot_hint_seq, None);
    }

    #[test]
    fn retention_advancement_requires_published_checkpoint_and_updates_floor_only() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
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
            .head(&snapshot_manifest(namespace_id.as_str(), 1))
            .expect("manifest head")
            .is_some());
    }

    #[test]
    fn checkpoint_materialization_uses_written_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
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

        let segment_key =
            snapshot_table(namespace_id.as_str(), 1, SnapshotTableFamily::Revisions, 0);
        assert!(store.head(&segment_key).expect("head segment").is_some());
    }

    #[test]
    fn older_checkpoint_can_publish_after_head_advances() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::from("demo");
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
        let tables = build_checkpoint_tables(
            &store,
            &namespace_id,
            basis_before.head.seq,
            &basis_before.metadata_state,
            &context.writer_version,
        )
        .expect("build checkpoint tables");
        let manifest = CheckpointManifestEnvelope::from_payload(
            &context.writer_version,
            CheckpointManifestPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_before.head.seq,
                active_fence_token: basis_before.head.active_fence_token,
                next_inode_id: basis_before.head.next_inode_id,
                retention_floor_seq: basis_before.head.retention_floor_seq,
                verified: true,
                tables,
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
        let published = publish_snapshot_hint_seq(
            &store,
            &namespace_id,
            basis_before.head.seq,
            &context.writer_version,
        )
        .expect("publish snapshot hint");

        assert_eq!(published.seq, ChangeSeq(2));
        assert_eq!(published.snapshot_hint_seq, Some(ChangeSeq(1)));

        let after = load_verified_namespace_basis(&store, &namespace_id).expect("basis after");
        assert_eq!(after.head.seq, ChangeSeq(2));
        assert_eq!(after.head.snapshot_hint_seq, Some(ChangeSeq(1)));
    }

    fn test_context() -> MutationContext {
        MutationContext {
            writer_id: "test-writer".to_owned(),
            writer_version: "test-writer/0.1.0".to_owned(),
            now_ms: 1_000,
            lease_duration_ms: 60_000,
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
