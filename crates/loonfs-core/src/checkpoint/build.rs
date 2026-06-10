//! Segments metadata rows into runs and writes the immutable metadata
//! SST objects a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq};
use super::runs::{
    MetadataTableManifest, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
    MAX_MAINTENANCE_TABLE_IO,
};
use crate::error::CoreError;
use crate::metadata::MetadataState;
use crate::storage::content::write_immutable_object;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{
    encode_metadata_sst_envelope_zstd, MetadataFileRef, MetadataPage, MetadataRow,
    MetadataSegmentKey, MetadataSstEnvelope, MetadataSstPayload, MetadataTableFamily,
};
use loonfs_api::{generate_metadata_table_id, ChangeSeq, InodeId, NamespaceId};
use loonfs_objectstore::keys::metadata_sst;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;

pub(super) async fn build_manifest_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    metadata_state: &MetadataState,
    writer_version: &str,
    max_rows_per_segment: usize,
) -> Result<Vec<MetadataTableManifest>, CoreError> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        run_seq,
        level,
        writer_version,
        |family| manifest_rows_for_family(metadata_state, family),
        MetadataTableSegmentation::Base {
            max_rows_per_segment,
        },
    )
    .await
}

pub(super) fn debug_assert_manifest_table_segments_do_not_overlap(
    _tables: &[MetadataTableManifest],
) {
    #[cfg(debug_assertions)]
    for table in _tables {
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

pub(super) async fn build_manifest_l0_run_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    after_seq: ChangeSeq,
    metadata_state: &MetadataState,
    writer_version: &str,
) -> Result<Vec<MetadataTableManifest>, CoreError> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        run_seq,
        CHECKPOINT_L0_RUN_LEVEL,
        writer_version,
        |family| manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
        MetadataTableSegmentation::Full,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MetadataTableSegmentation {
    Base { max_rows_per_segment: usize },
    Full,
}

pub(super) struct MetadataSstRows {
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
pub(super) async fn build_manifest_tables_from_rows<S, RowsForFamily>(
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
        let mut requests = Vec::with_capacity(segments.len());
        for (segment_index, segment_rows) in segments.into_iter().enumerate() {
            let segment_index = u32::try_from(segment_index)
                .map_err(|_| CoreError::Store("metadata SST index overflow".to_owned()))?;
            let table_id = generate_metadata_table_id();
            let object_key = metadata_sst(namespace_id.as_str(), &table_id);
            requests.push(MetadataSstWriteRequest {
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
            });
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
                try_join_all(
                    chunk
                        .into_iter()
                        .map(|request| write_manifest_segment(store, request)),
                )
                .await?,
            );
        }
        tables.push(MetadataTableManifest {
            family,
            segments: descriptors,
        });
    }
    Ok(tables)
}

pub(super) struct MetadataSstWriteRequest<'a> {
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

pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
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
    write_immutable_object(store, &request.object_key, &encoded).await?;
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
        payload_checksum: envelope.payload_checksum.clone(),
    })
}

pub(super) fn segment_manifest_rows(
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

pub(super) fn segment_rows_by_parent(rows: Vec<MetadataRow>) -> Vec<MetadataSstRows> {
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

pub(super) fn segment_rows_by_row_key_range(
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

pub(super) fn manifest_row_parent_inode_id(row: &MetadataRow) -> Option<InodeId> {
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
