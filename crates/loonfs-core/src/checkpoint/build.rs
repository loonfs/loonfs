//! Segments metadata rows into runs and writes the immutable metadata
//! segments a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq};
use super::runs::{
    MetadataFamilySegments, CHECKPOINT_L0_RUN_LEVEL, CHECKPOINT_ROW_FAMILIES,
    MAX_MAINTENANCE_SEGMENT_IO,
};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataState;
use bytes::Bytes;
use futures::future::try_join_all;
use loonfs_api::wire::hex::hex_encode_bytes;
use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
use loonfs_api::wire::sst_blocks::SegmentBlocksBuilder;
pub(super) use loonfs_api::wire::sst_blocks::DEFAULT_INLINE_FILTER_MAX_BYTES as INLINE_SEGMENT_FILTER_MAX_BYTES;
use loonfs_api::{sha256_digest, ChangeSeq, MetadataCompactionId, MetadataSegmentId, NamespaceId};
use loonfs_objectstore::keys::{metadata_compaction_segment, metadata_segment};
use loonfs_objectstore::ObjectStore;
use std::num::NonZeroUsize;

pub(super) async fn build_manifest_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    metadata_state: &MetadataState,
    max_rows_per_segment: NonZeroUsize,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        run_seq,
        level,
        |family| manifest_rows_for_family(metadata_state, family),
        MetadataSegmentation::Base {
            max_rows_per_segment,
        },
    )
    .await
}

/// Checks newly built test segments for the same non-overlap invariant that
/// manifest loading enforces. Production merges stream through
/// [`super::compaction_output::MergeSegmentWriter`].
#[cfg(test)]
pub(super) fn debug_assert_manifest_segments_do_not_overlap(
    _segments_by_family: &[MetadataFamilySegments],
) {
    #[cfg(debug_assertions)]
    for family_segments in _segments_by_family {
        let mut previous_max_key: Option<&str> = None;
        for descriptor in &family_segments.segments {
            if let Some(previous) = previous_max_key {
                debug_assert!(
                    previous < descriptor.min_key.as_str(),
                    "overlapping metadata segment ranges for `{:?}`",
                    family_segments.family
                );
            }
            previous_max_key = Some(descriptor.max_key.as_str());
        }
    }
}

pub(super) async fn build_manifest_l0_run_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    after_seq: ChangeSeq,
    metadata_state: &MetadataState,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        run_seq,
        CHECKPOINT_L0_RUN_LEVEL,
        |family| manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
        MetadataSegmentation::Full,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MetadataSegmentation {
    Base { max_rows_per_segment: NonZeroUsize },
    Full,
}

pub(super) struct MetadataSegmentRows {
    rows: Vec<MetadataRow>,
}

#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_manifest_segments", key_class = "metadata_segment")
)]
pub(super) async fn build_manifest_segments_from_rows<S, RowsForFamily>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    level: u32,
    mut rows_for_family: RowsForFamily,
    segmentation: MetadataSegmentation,
) -> Result<Vec<MetadataFamilySegments>>
where
    S: ObjectStore + ?Sized,
    RowsForFamily: FnMut(MetadataRowFamily) -> Vec<MetadataRow>,
{
    let destination = MetadataSegmentDestination::Published { namespace_id };
    let mut segments_by_family = Vec::with_capacity(CHECKPOINT_ROW_FAMILIES.len());
    for family in CHECKPOINT_ROW_FAMILIES {
        let rows = rows_for_family(family);
        if rows.is_empty() {
            segments_by_family.push(MetadataFamilySegments {
                family,
                segments: Vec::new(),
            });
            continue;
        }

        let segments = segment_manifest_rows(rows, segmentation);
        let mut requests = Vec::with_capacity(segments.len());
        for (segment_index, segment_rows) in segments.into_iter().enumerate() {
            let segment_index = u32::try_from(segment_index)
                .map_err(|_| CoreError::Internal("metadata segment index overflow".to_owned()))?;
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
                .take(MAX_MAINTENANCE_SEGMENT_IO)
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
        segments_by_family.push(MetadataFamilySegments {
            family,
            segments: descriptors,
        });
    }
    Ok(segments_by_family)
}

/// Selects the published or compaction-staging location for a segment.
#[derive(Debug, Clone, Copy)]
pub(super) enum MetadataSegmentDestination<'a> {
    Published {
        namespace_id: &'a NamespaceId,
    },
    CompactionStaging {
        namespace_id: &'a NamespaceId,
        job_id: &'a MetadataCompactionId,
    },
}

impl<'a> MetadataSegmentDestination<'a> {
    fn namespace_id(self) -> &'a NamespaceId {
        match self {
            Self::Published { namespace_id } | Self::CompactionStaging { namespace_id, .. } => {
                namespace_id
            }
        }
    }

    fn object_key(self, segment_id: &MetadataSegmentId) -> String {
        match self {
            Self::Published { namespace_id } => metadata_segment(namespace_id, segment_id),
            Self::CompactionStaging {
                namespace_id,
                job_id,
            } => metadata_compaction_segment(namespace_id, job_id, segment_id),
        }
    }

    pub(super) fn write_request(
        self,
        run_seq: ChangeSeq,
        level: u32,
        family: MetadataRowFamily,
        segment_index: u32,
        rows: Vec<MetadataRow>,
    ) -> MetadataSegmentWriteRequest<'a> {
        MetadataSegmentWriteRequest {
            destination: self,
            segment_id: MetadataSegmentId::generate(),
            run_seq,
            level,
            family,
            segment_index,
            rows,
        }
    }
}

pub(super) struct MetadataSegmentWriteRequest<'a> {
    destination: MetadataSegmentDestination<'a>,
    segment_id: MetadataSegmentId,
    run_seq: ChangeSeq,
    level: u32,
    family: MetadataRowFamily,
    segment_index: u32,
    rows: Vec<MetadataRow>,
}

pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    request: MetadataSegmentWriteRequest<'_>,
) -> Result<MetadataSegmentRef> {
    let object_key = request.destination.object_key(&request.segment_id);
    let mut builder = SegmentBlocksBuilder::default();
    for row in &request.rows {
        let row_key = row.row_key_for_family(request.family);
        let filter_key = row.filter_key_for_family(request.family);
        builder.push(&row_key, &filter_key, row).map_err(|err| {
            CoreError::Internal(format!(
                "failed to build metadata segment `{}`: {err}",
                object_key
            ))
        })?;
    }
    let built = builder.finish().map_err(|err| {
        CoreError::Internal(format!(
            "failed to build metadata segment `{}`: {err}",
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
    Ok(MetadataSegmentRef {
        owner_namespace_id: request.destination.namespace_id().clone(),
        segment_id: request.segment_id,
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
        object_checksum: sha256_digest(&built.bytes),
    })
}

pub(super) fn segment_manifest_rows(
    rows: Vec<MetadataRow>,
    segmentation: MetadataSegmentation,
) -> Vec<MetadataSegmentRows> {
    match segmentation {
        MetadataSegmentation::Full => vec![MetadataSegmentRows { rows }],
        MetadataSegmentation::Base {
            max_rows_per_segment,
        } => segment_rows_by_row_key_range(rows, max_rows_per_segment),
    }
}

pub(super) fn segment_rows_by_row_key_range(
    rows: Vec<MetadataRow>,
    max_rows_per_segment: NonZeroUsize,
) -> Vec<MetadataSegmentRows> {
    rows.chunks(max_rows_per_segment.get())
        .map(|rows| MetadataSegmentRows {
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
        let segment_id =
            MetadataSegmentId::parse("seg_00000000000000000000000000000001").expect("segment id");

        assert_eq!(
            MetadataSegmentDestination::Published {
                namespace_id: &namespace_id,
            }
            .object_key(&segment_id),
            "namespaces/ns-1/metadata/segments/\
             seg_00000000000000000000000000000001.sst.zst",
        );
        assert_eq!(
            MetadataSegmentDestination::CompactionStaging {
                namespace_id: &namespace_id,
                job_id: &job_id,
            }
            .object_key(&segment_id),
            "namespaces/ns-1/metadata/compactions/\
             cmp_00000000000000000000000000000001/segments/\
             seg_00000000000000000000000000000001.sst.zst",
        );
    }
}
