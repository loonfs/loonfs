//! Read-side manifest loading.
//!
//! There are three intentionally separate levels here:
//!
//! 1. `load_namespace_manifest_envelope` validates only the manifest envelope.
//! 2. `load_verified_manifest_tables` validates the manifest and table
//!    descriptors without fetching SST row payloads.
//! 3. `load_manifest_materialization_for_inspection` is the expensive
//!    inspection/debug path that loads every referenced row into `MetadataState`.

use super::cache::{
    DecodedMetadataTableBlock, MetadataTableBlockKind, MetadataTableCache, MetadataTableCacheKey,
};
use super::error::ManifestLoadError;
#[cfg(test)]
use super::row::manifest_row_kind;
use super::runs::runs_in_materialization_order;
#[cfg(test)]
use super::runs::{MetadataTableManifest, MAX_MAINTENANCE_TABLE_IO};
#[cfg(test)]
use super::scan::ManifestMaterializationForInspection;
use super::scan::{ordered_manifest_tables, VerifiedMetadataTables};
use super::validate::{
    validate_manifest_materialization_ranges, validate_manifest_row_seq_range,
    validate_manifest_segment, validate_namespace_manifest,
};
#[cfg(test)]
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    MetadataStateBuilder, RevisionRecord, SubtreeTombstoneRecord,
};
#[cfg(test)]
use futures::future::try_join_all;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::{
    decode_metadata_sst_envelope_zstd, decode_namespace_manifest_json, MetadataFileRef,
    MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
};
use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};
use loonfs_objectstore::keys::{metadata_manifest, metadata_table};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::Instrument;

#[cfg(test)]
pub(crate) async fn load_manifest_materialization_for_inspection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<ManifestMaterializationForInspection, ManifestLoadError> {
    load_manifest_materialization_for_inspection_if_present(store, namespace_id, manifest_id)
        .await?
        .ok_or_else(|| ManifestLoadError::MissingManifest {
            object_key: metadata_manifest(namespace_id.as_str(), manifest_id),
        })
}

/// Loads and validates only the manifest envelope, without fetching its
/// metadata tables. This is enough for callers that need manifest framing,
/// not table descriptors or rows.
pub(crate) async fn load_namespace_manifest_envelope<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<NamespaceManifestEnvelope, ManifestLoadError> {
    let manifest_key = metadata_manifest(namespace_id.as_str(), manifest_id);
    load_namespace_manifest_envelope_if_present(store, namespace_id, manifest_id, &manifest_key)
        .await?
        .ok_or(ManifestLoadError::MissingManifest {
            object_key: manifest_key,
        })
}

/// Loads the current manifest's verified table descriptors without fetching
/// metadata SST row payloads or constructing `MetadataState`.
pub(crate) async fn load_verified_manifest_tables<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<VerifiedMetadataTables<'a, S>, ManifestLoadError> {
    load_verified_manifest_tables_with_cache(store, None, namespace_id, manifest_id).await
}

pub(crate) async fn load_verified_manifest_tables_with_cache<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    table_cache: Option<&'a MetadataTableCache>,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<VerifiedMetadataTables<'a, S>, ManifestLoadError> {
    let manifest_key = metadata_manifest(namespace_id.as_str(), manifest_id);
    let manifest = {
        let Some(manifest_bytes) = store
            .get(&manifest_key, None)
            .instrument(tracing::info_span!(
                "loon.phase",
                phase = "load_namespace_manifest",
                key_class = "manifest_table"
            ))
            .await
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
    validate_namespace_manifest(namespace_id, manifest_id, &manifest_key, &manifest)?;
    validate_manifest_materialization_ranges(&manifest_key, &manifest.payload)?;
    validate_manifest_table_descriptors(&manifest_key, &manifest)?;
    let tables = VerifiedMetadataTables {
        store,
        table_cache,
        manifest_object_key: manifest_key,
        manifest,
        segment_cache: Mutex::new(HashMap::new()),
    };
    Ok(tables)
}

pub(crate) fn head_from_manifest(
    current_head: &HeadState,
    manifest: &NamespaceManifestEnvelope,
) -> HeadState {
    HeadState {
        namespace_id: current_head.namespace_id.clone(),
        seq: manifest.payload.head_seq,
        head_commit_id: manifest.payload.head_commit_id.clone(),
        // The manifest records the manifest-time writer epoch. That may lag the
        // live head if writer takeover advanced the epoch without WAL replay.
        writer_epoch: manifest.payload.writer_epoch,
        writer: current_head.writer.clone(),
        next_inode_id: manifest.payload.next_inode_id,
        retention_floor_seq: current_head.retention_floor_seq,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: current_head.state,
    }
}

#[cfg(test)]
pub(super) async fn load_manifest_materialization_for_inspection_if_present<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
) -> Result<Option<ManifestMaterializationForInspection>, ManifestLoadError> {
    let manifest_key = metadata_manifest(namespace_id.as_str(), manifest_id);
    let manifest = load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        manifest_id,
        &manifest_key,
    )
    .await?;
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    let metadata_state = load_manifest_metadata_state_for_inspection_from_manifest(
        store,
        namespace_id,
        &manifest_key,
        &manifest,
    )
    .await?;
    Ok(Some(ManifestMaterializationForInspection {
        manifest,
        metadata_state,
    }))
}

pub(super) async fn load_namespace_manifest_envelope_if_present<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_id: ManifestId,
    manifest_key: &str,
) -> Result<Option<NamespaceManifestEnvelope>, ManifestLoadError> {
    let Some(manifest_bytes) = store
        .get(manifest_key, None)
        .instrument(tracing::info_span!(
            "loon.phase",
            phase = "load_namespace_manifest",
            key_class = "manifest_table"
        ))
        .await
        .map_err(|err| ManifestLoadError::ReadManifest {
            object_key: manifest_key.to_owned(),
            message: err.to_string(),
        })?
    else {
        return Ok(None);
    };
    let manifest = decode_namespace_manifest_json(&manifest_bytes).map_err(|err| {
        ManifestLoadError::ManifestCodec {
            object_key: manifest_key.to_owned(),
            message: err.to_string(),
        }
    })?;
    validate_namespace_manifest(namespace_id, manifest_id, manifest_key, &manifest)?;
    Ok(Some(manifest))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "load_manifest_tables", key_class = "manifest_table")
)]
#[cfg(test)]
pub(super) async fn load_manifest_metadata_state_for_inspection_from_manifest<
    S: ObjectStore + ?Sized,
>(
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
        )
        .await?;
    }

    Ok(metadata_state.finish())
}

fn validate_manifest_table_descriptors(
    manifest_object_key: &str,
    manifest: &NamespaceManifestEnvelope,
) -> Result<(), ManifestLoadError> {
    for run in runs_in_materialization_order(&manifest.payload) {
        let ordered_tables = ordered_manifest_tables(manifest_object_key, &run.tables)?;
        let mut direntry_bind_rows = 0u64;
        let mut direntry_child_bind_rows = 0u64;
        let mut revision_rows = 0u64;
        let mut revision_by_inode_desc_rows = 0u64;
        let context = MetadataTableLoadContext {
            manifest_object_key,
            segment_seq_expectation: MetadataSstSeqExpectation::Descriptor,
            row_seq_min: None,
            row_seq_max: run.run_seq,
        };

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
                match table.family {
                    MetadataTableFamily::DirentryBinds => {
                        direntry_bind_rows =
                            direntry_bind_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::DirentryChildBinds => {
                        direntry_child_bind_rows =
                            direntry_child_bind_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::Revisions => {
                        revision_rows = revision_rows.saturating_add(descriptor.row_count);
                    }
                    MetadataTableFamily::RevisionsByInodeDesc => {
                        revision_by_inode_desc_rows =
                            revision_by_inode_desc_rows.saturating_add(descriptor.row_count);
                    }
                    _ => {}
                }
            }
        }

        if direntry_bind_rows != direntry_child_bind_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: manifest_object_key.to_owned(),
                message: format!(
                    "metadata run {:?} has {direntry_bind_rows} direntry bind rows but {direntry_child_bind_rows} child-bind index rows",
                    run.run_seq
                ),
            });
        }
        if revision_rows != revision_by_inode_desc_rows {
            return Err(ManifestLoadError::RunManifestMismatch {
                object_key: manifest_object_key.to_owned(),
                message: format!(
                    "metadata run {:?} has {revision_rows} revision rows but {revision_by_inode_desc_rows} revision index rows",
                    run.run_seq
                ),
            });
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct MetadataTableLoadContext<'a> {
    pub(super) manifest_object_key: &'a str,
    pub(super) segment_seq_expectation: MetadataSstSeqExpectation,
    pub(super) row_seq_min: Option<ChangeSeq>,
    pub(super) row_seq_max: ChangeSeq,
}

#[derive(Clone, Copy)]
pub(super) enum MetadataSstSeqExpectation {
    Descriptor,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataTableCacheMode {
    Bypass,
    Populate,
    ReadOnly,
}

impl MetadataTableLoadContext<'_> {
    pub(super) fn expected_segment_seq(
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

    pub(super) fn row_seq_max(&self, descriptor: &MetadataFileRef) -> ChangeSeq {
        match self.segment_seq_expectation {
            MetadataSstSeqExpectation::Descriptor => descriptor.run_seq,
        }
    }
}

pub(super) fn metadata_file_object_key(descriptor: &MetadataFileRef) -> String {
    metadata_table(
        descriptor.owner_namespace_id.as_str(),
        descriptor.table_id.as_str(),
    )
}

#[cfg(test)]
pub(super) async fn append_manifest_tables_to_metadata<S>(
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
    let mut revision_rows = Vec::new();
    let mut revision_by_inode_desc_rows = Vec::new();
    for table in ordered_tables {
        let mut descriptors = Vec::with_capacity(table.segments.len());
        for descriptor in &table.segments {
            context.expected_segment_seq(descriptor)?;
            let expected_key = metadata_file_object_key(descriptor);
            if descriptor.object_key != expected_key {
                return Err(ManifestLoadError::SegmentObjectKeyMismatch {
                    object_key: descriptor.object_key.clone(),
                    expected: expected_key,
                });
            }
            descriptors.push(descriptor);
        }

        let mut loaded_segments = Vec::with_capacity(descriptors.len());
        for chunk in descriptors.chunks(MAX_MAINTENANCE_TABLE_IO) {
            loaded_segments.extend(
                try_join_all(chunk.iter().map(|descriptor| {
                    load_manifest_segment_rows(store, context, table.family, descriptor)
                }))
                .await?,
            );
        }

        for (descriptor, rows) in descriptors.into_iter().zip(loaded_segments) {
            match table.family {
                MetadataTableFamily::DirentryBinds => {
                    direntry_bind_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::DirentryChildBinds => {
                    direntry_child_bind_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::Revisions => {
                    revision_rows.extend(rows.iter().cloned());
                }
                MetadataTableFamily::RevisionsByInodeDesc => {
                    revision_by_inode_desc_rows.extend(rows.iter().cloned());
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
    )?;
    validate_revision_by_inode_desc_index(
        context.manifest_object_key,
        revision_rows,
        revision_by_inode_desc_rows,
    )
}

#[cfg(test)]
pub(super) async fn load_manifest_segment_rows<S: ObjectStore + ?Sized>(
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
    .await
}

pub(super) async fn load_manifest_segment_rows_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    context: MetadataTableLoadContext<'_>,
    family: MetadataTableFamily,
    descriptor: &MetadataFileRef,
    cache_mode: MetadataTableCacheMode,
) -> Result<Vec<MetadataRow>, ManifestLoadError> {
    let expected_segment_seq = context.expected_segment_seq(descriptor)?;
    let cache_key = MetadataTableCacheKey {
        table_digest: descriptor.payload_checksum.clone(),
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

    let Some(bytes) = store
        .get(&descriptor.object_key, None)
        .await
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
                    decoded_byte_len: decoded_manifest_block_weight(family, &rows),
                },
            );
        }
    }
    Ok(rows)
}

pub(super) fn validate_cached_manifest_block(
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
    {
        return Err(ManifestLoadError::SegmentDescriptorMismatch {
            object_key: descriptor.object_key.clone(),
            message: "cached segment descriptor mismatch".to_owned(),
        });
    }
    Ok(())
}

pub(super) fn decoded_manifest_block_weight(
    family: MetadataTableFamily,
    rows: &[MetadataRow],
) -> usize {
    let row_weight = rows
        .iter()
        .map(|row| 64 + row.row_key_for_family(family).len() + decoded_manifest_row_weight(row))
        .sum::<usize>();
    row_weight.saturating_add(128)
}

pub(super) fn decoded_manifest_row_weight(row: &MetadataRow) -> usize {
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
            semantic_commit_fingerprint,
            message,
            ..
        } => {
            96 + commit_id.as_str().len()
                + semantic_commit_fingerprint.len()
                + message.as_ref().map_or(0, String::len)
        }
    }
}

#[cfg(test)]
pub(super) fn append_rows_to_metadata(
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
            (MetadataTableFamily::RevisionsByInodeDesc, MetadataRow::Revision { .. }) => {}
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
                    semantic_commit_fingerprint,
                    committed_seq,
                    message,
                },
            ) => metadata_state.push_commit_receipt(CommitReceiptRecord {
                commit_id: commit_id.clone(),
                semantic_commit_fingerprint: semantic_commit_fingerprint.clone(),
                committed_seq: *committed_seq,
                message: message.clone(),
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

pub(super) fn validate_direntry_child_bind_index(
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

pub(super) fn validate_revision_by_inode_desc_index(
    object_key: &str,
    mut revision_rows: Vec<MetadataRow>,
    mut revision_by_inode_desc_rows: Vec<MetadataRow>,
) -> Result<(), ManifestLoadError> {
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataTableFamily::Revisions,
        &revision_rows,
    )?;
    validate_revision_rows_have_unique_keys(
        object_key,
        MetadataTableFamily::RevisionsByInodeDesc,
        &revision_by_inode_desc_rows,
    )?;

    revision_rows.sort_by_key(revision_logical_key);
    revision_by_inode_desc_rows.sort_by_key(revision_logical_key);

    if revision_rows != revision_by_inode_desc_rows {
        return Err(ManifestLoadError::RevisionIndexMismatch {
            object_key: object_key.to_owned(),
        });
    }

    Ok(())
}

fn validate_revision_rows_have_unique_keys(
    object_key: &str,
    family: MetadataTableFamily,
    rows: &[MetadataRow],
) -> Result<(), ManifestLoadError> {
    let mut seen = BTreeSet::new();
    for row in rows {
        let row_key = revision_logical_key(row);
        if !seen.insert(row_key.clone()) {
            return Err(ManifestLoadError::DuplicateRevisionRow {
                object_key: object_key.to_owned(),
                family,
                row_key,
            });
        }
    }
    Ok(())
}

fn revision_logical_key(row: &MetadataRow) -> String {
    row.row_key_for_family(MetadataTableFamily::Revisions)
}
