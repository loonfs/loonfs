//! Segments metadata rows into runs and writes the immutable metadata
//! SST objects a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq};
use super::runs::{
    MetadataTableManifest, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
    MAX_MAINTENANCE_TABLE_IO,
};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataState;
use crate::storage::content::write_immutable_object;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
use loonfs_api::{sha256_digest, ChangeSeq, MetadataTableId, NamespaceId};
use loonfs_objectstore::keys::metadata_table;
use loonfs_objectstore::ObjectStore;

pub(super) async fn build_manifest_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    metadata_state: &MetadataState,
    max_rows_per_segment: usize,
) -> Result<Vec<MetadataTableManifest>> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        run_seq,
        level,
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
) -> Result<Vec<MetadataTableManifest>> {
    build_manifest_tables_from_rows(
        store,
        namespace_id,
        run_seq,
        CHECKPOINT_L0_RUN_LEVEL,
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
    mut rows_for_family: RowsForFamily,
    segmentation: MetadataTableSegmentation,
) -> Result<Vec<MetadataTableManifest>>
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

        let segments = segment_manifest_rows(rows, segmentation);
        let mut requests = Vec::with_capacity(segments.len());
        for (segment_index, segment_rows) in segments.into_iter().enumerate() {
            let segment_index = u32::try_from(segment_index)
                .map_err(|_| CoreError::Internal("metadata SST index overflow".to_owned()))?;
            let table_id = MetadataTableId::generate();
            let object_key = metadata_table(namespace_id.as_str(), table_id.as_str());
            requests.push(MetadataSstWriteRequest {
                namespace_id,
                table_id,
                run_seq,
                level,
                family,
                segment_index,
                rows: segment_rows.rows,
                object_key,
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
    table_id: MetadataTableId,
    run_seq: ChangeSeq,
    level: u32,
    family: MetadataTableFamily,
    segment_index: u32,
    rows: Vec<MetadataRow>,
    object_key: String,
}

pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    request: MetadataSstWriteRequest<'_>,
) -> Result<MetadataFileRef> {
    let mut builder = SegmentBlocksBuilder::default();
    for row in &request.rows {
        let row_key = row.row_key_for_family(request.family);
        let filter_key = row.filter_key_for_family(request.family);
        builder.push(&row_key, &filter_key, row).map_err(|err| {
            CoreError::Internal(format!(
                "failed to build metadata SST `{}`: {err}",
                request.object_key
            ))
        })?;
    }
    let built = builder.finish().map_err(|err| {
        CoreError::Internal(format!(
            "failed to build metadata SST `{}`: {err}",
            request.object_key
        ))
    })?;
    write_immutable_object(store, &request.object_key, &built.bytes).await?;
    Ok(MetadataFileRef {
        owner_namespace_id: request.namespace_id.clone(),
        table_id: request.table_id,
        object_key: request.object_key,
        run_seq: request.run_seq,
        level: request.level,
        family: request.family,
        segment_index: request.segment_index,
        row_count: built.row_count,
        min_key: built.min_key,
        max_key: built.max_key,
        index_block: built.index,
        filter_block: built.filter,
        payload_checksum: sha256_digest(&built.bytes),
    })
}

pub(super) fn segment_manifest_rows(
    rows: Vec<MetadataRow>,
    segmentation: MetadataTableSegmentation,
) -> Vec<MetadataSstRows> {
    match segmentation {
        MetadataTableSegmentation::Full => vec![MetadataSstRows { rows }],
        MetadataTableSegmentation::Base {
            max_rows_per_segment,
        } => segment_rows_by_row_key_range(rows, max_rows_per_segment.max(1)),
    }
}

pub(super) fn segment_rows_by_row_key_range(
    rows: Vec<MetadataRow>,
    max_rows_per_segment: usize,
) -> Vec<MetadataSstRows> {
    rows.chunks(max_rows_per_segment)
        .map(|rows| MetadataSstRows {
            rows: rows.to_vec(),
        })
        .collect()
}
