//! Segments metadata rows into runs and writes the immutable metadata
//! SST objects a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq};
use super::runs::{
    MetadataTableManifest, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_TABLE_FAMILIES,
    MAX_MAINTENANCE_TABLE_IO,
};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataState;
use bytes::Bytes;
use futures::future::try_join_all;
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::wire::manifest::{MetadataFileRef, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
pub(super) use loonfs_api::wire::sst_blocks::DEFAULT_INLINE_FILTER_MAX_BYTES as INLINE_SEGMENT_FILTER_MAX_BYTES;
use loonfs_api::{sha256_digest, ChangeSeq, MetadataCompactionId, MetadataTableId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_table, metadata_table};
use loonfs_objectstore::ObjectStore;
use std::num::NonZeroUsize;

pub(super) async fn build_manifest_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    metadata_state: &MetadataState,
    max_rows_per_segment: NonZeroUsize,
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

/// The layout invariant manifest load enforces for real, checked eagerly on a
/// run a test just built. Production merges write through
/// [`super::compaction_output::MergeSegmentWriter`], which never holds a run's
/// tables in this shape.
#[cfg(test)]
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
    Base { max_rows_per_segment: NonZeroUsize },
    Full,
}

pub(super) struct MetadataSstRows {
    rows: Vec<MetadataRow>,
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_manifest_tables", key_class = "metadata_sst")
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
    let destination = MetadataTableDestination::Published { namespace_id };
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
            requests.push(destination.write_request(
                run_seq,
                level,
                family,
                segment_index,
                segment_rows.rows,
            ));
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

/// The two physical locations a logically identical metadata table may use.
/// Keeping namespace and staging-job identity here makes the object key a
/// consequence of the destination instead of a caller-supplied coordinate.
#[derive(Debug, Clone, Copy)]
pub(super) enum MetadataTableDestination<'a> {
    Published {
        namespace_id: &'a NamespaceId,
    },
    CompactionStaging {
        namespace_id: &'a NamespaceId,
        job_id: &'a MetadataCompactionId,
    },
}

impl<'a> MetadataTableDestination<'a> {
    fn namespace_id(self) -> &'a NamespaceId {
        match self {
            Self::Published { namespace_id } | Self::CompactionStaging { namespace_id, .. } => {
                namespace_id
            }
        }
    }

    fn object_key(self, table_id: &MetadataTableId) -> String {
        match self {
            Self::Published { namespace_id } => metadata_table(namespace_id, table_id),
            Self::CompactionStaging {
                namespace_id,
                job_id,
            } => metadata_compaction_table(namespace_id, job_id, table_id),
        }
    }

    pub(super) fn write_request(
        self,
        run_seq: ChangeSeq,
        level: u32,
        family: MetadataTableFamily,
        segment_index: u32,
        rows: Vec<MetadataRow>,
    ) -> MetadataSstWriteRequest<'a> {
        MetadataSstWriteRequest {
            destination: self,
            table_id: MetadataTableId::generate(),
            run_seq,
            level,
            family,
            segment_index,
            rows,
        }
    }
}

pub(super) struct MetadataSstWriteRequest<'a> {
    destination: MetadataTableDestination<'a>,
    table_id: MetadataTableId,
    run_seq: ChangeSeq,
    level: u32,
    family: MetadataTableFamily,
    segment_index: u32,
    rows: Vec<MetadataRow>,
}

pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    request: MetadataSstWriteRequest<'_>,
) -> Result<MetadataFileRef> {
    let object_key = request.destination.object_key(&request.table_id);
    let mut builder = SegmentBlocksBuilder::default();
    for row in &request.rows {
        let row_key = row.row_key_for_family(request.family);
        let filter_key = row.filter_key_for_family(request.family);
        builder.push(&row_key, &filter_key, row).map_err(|err| {
            CoreError::Internal(format!(
                "failed to build metadata SST `{}`: {err}",
                object_key
            ))
        })?;
    }
    let built = builder.finish().map_err(|err| {
        CoreError::Internal(format!(
            "failed to build metadata SST `{}`: {err}",
            object_key
        ))
    })?;
    store
        .put_immutable_verified(&object_key, Bytes::from(built.bytes.clone()))
        .await?;
    let filter_inline = (built.filter.stored_len <= INLINE_SEGMENT_FILTER_MAX_BYTES).then(|| {
        let start = built.filter.offset as usize;
        hex_encode_bytes(&built.bytes[start..start + built.filter.stored_len as usize])
    });
    Ok(MetadataFileRef {
        owner_namespace_id: request.destination.namespace_id().clone(),
        table_id: request.table_id,
        object_key,
        run_seq: request.run_seq,
        level: request.level,
        family: request.family,
        segment_index: request.segment_index,
        row_count: built.row_count,
        min_key: built.min_key,
        max_key: built.max_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
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
        } => segment_rows_by_row_key_range(rows, max_rows_per_segment),
    }
}

pub(super) fn segment_rows_by_row_key_range(
    rows: Vec<MetadataRow>,
    max_rows_per_segment: NonZeroUsize,
) -> Vec<MetadataSstRows> {
    rows.chunks(max_rows_per_segment.get())
        .map(|rows| MetadataSstRows {
            rows: rows.to_vec(),
        })
        .collect()
}

#[cfg(test)]
mod destination_tests {
    use super::*;

    #[test]
    fn destinations_preserve_published_and_staging_layouts() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let job_id =
            MetadataCompactionId::parse("cmp_00000000000000000000000000000001").expect("job id");
        let table_id =
            MetadataTableId::parse("tbl_00000000000000000000000000000001").expect("table id");

        assert_eq!(
            MetadataTableDestination::Published {
                namespace_id: &namespace_id,
            }
            .object_key(&table_id),
            "namespaces/ns-1/metadata/tables/\
             tbl_00000000000000000000000000000001.sst.zst",
        );
        assert_eq!(
            MetadataTableDestination::CompactionStaging {
                namespace_id: &namespace_id,
                job_id: &job_id,
            }
            .object_key(&table_id),
            "namespaces/ns-1/metadata/compactions/\
             cmp_00000000000000000000000000000001/tables/\
             tbl_00000000000000000000000000000001.sst.zst",
        );
    }
}
