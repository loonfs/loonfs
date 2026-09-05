//! Segments metadata rows into runs and writes the immutable metadata
//! segments a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq};
use super::runs::{MetadataFamilySegments, MetadataLsmPolicy, CHECKPOINT_ROW_FAMILIES};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataState;
use bytes::Bytes;
use futures::future::try_join_all;
use loonfs_api::wire::manifest::{MetadataRow, MetadataRowFamily, MetadataSegmentRef};
#[cfg(test)]
pub(super) use loonfs_api::wire::sst_blocks::DEFAULT_INLINE_FILTER_MAX_BYTES as INLINE_SEGMENT_FILTER_MAX_BYTES;
use loonfs_api::wire::sst_blocks::{BuiltSegmentBlocks, SegmentBlocksBuilder};
use loonfs_api::{sha256_digest, ChangeSeq, MetadataCompactionId, MetadataSegmentId, NamespaceId};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use std::future::Future;
use std::num::NonZeroUsize;

/// Writes segment requests in bounded concurrent waves.
pub async fn write_segments_in_waves<Requests, Write, WriteFuture, Descriptor, Error>(
    requests: Requests,
    max_io: NonZeroUsize,
    mut write_segment: Write,
) -> std::result::Result<Vec<Descriptor>, Error>
where
    Requests: IntoIterator,
    Write: FnMut(Requests::Item) -> WriteFuture,
    WriteFuture: Future<Output = std::result::Result<Descriptor, Error>>,
{
    let mut descriptors = Vec::new();
    let mut pending = requests.into_iter();
    loop {
        let chunk = pending.by_ref().take(max_io.get()).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        descriptors.extend(try_join_all(chunk.into_iter().map(&mut write_segment)).await?);
    }
    Ok(descriptors)
}

pub(super) async fn build_manifest_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    metadata_state: &MetadataState,
    policy: MetadataLsmPolicy,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        |family| manifest_rows_for_family(metadata_state, family),
        policy,
    )
    .await
}

/// Checks newly built test segments for the same non-overlap invariant that
/// manifest loading enforces. Production merges stream through
/// [`MetadataSegmentWriter`].
#[cfg(test)]
pub(super) fn debug_assert_manifest_segments_do_not_overlap(
    _segments_by_family: &[MetadataFamilySegments],
) {
    #[cfg(debug_assertions)]
    for family_segments in _segments_by_family {
        let mut previous_max_row_key: Option<&str> = None;
        for descriptor in &family_segments.segments {
            if let Some(previous) = previous_max_row_key {
                debug_assert!(
                    previous < descriptor.min_row_key.as_str(),
                    "overlapping metadata segment ranges for `{:?}`",
                    family_segments.family
                );
            }
            previous_max_row_key = Some(descriptor.max_row_key.as_str());
        }
    }
}

pub(super) async fn build_manifest_delta_run_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
    metadata_state: &MetadataState,
    policy: MetadataLsmPolicy,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        |family| manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
        policy,
    )
    .await
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
    mut rows_for_family: RowsForFamily,
    policy: MetadataLsmPolicy,
) -> Result<Vec<MetadataFamilySegments>>
where
    S: ObjectStore + ?Sized,
    RowsForFamily: FnMut(MetadataRowFamily) -> Vec<MetadataRow>,
{
    let destination = MetadataSegmentDestination::Published { namespace_id };
    let mut segments_by_family = Vec::with_capacity(CHECKPOINT_ROW_FAMILIES.len());
    for family in CHECKPOINT_ROW_FAMILIES {
        let mut writer = MetadataSegmentWriter::new(family, destination);
        for row in rows_for_family(family) {
            writer.push(row, &mut |_| {})?;
            writer.roll_full_segments(store, policy).await?;
        }
        segments_by_family.push(MetadataFamilySegments {
            family,
            segments: writer.finish(store).await?,
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

    /// Returns the job id used to derive a compaction segment's key.
    fn compaction_job_id(self) -> Option<MetadataCompactionId> {
        match self {
            Self::Published { .. } => None,
            Self::CompactionStaging { job_id, .. } => Some(job_id.clone()),
        }
    }
}

/// Writes the encoded segment and returns the descriptor that binds its bytes.
pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    destination: MetadataSegmentDestination<'_>,
    family: MetadataRowFamily,
    segment_index: u32,
    built: BuiltSegmentBlocks,
) -> Result<MetadataSegmentRef> {
    let segment_id = MetadataSegmentId::generate();
    let filter_inline = built.inline_filter_hex();
    let descriptor = MetadataSegmentRef {
        owner_namespace_id: destination.namespace_id().clone(),
        segment_id,
        compaction_job_id: destination.compaction_job_id(),
        family,
        segment_index,
        row_count: built.row_count,
        min_row_key: built.min_row_key,
        max_row_key: built.max_row_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
        object_checksum: sha256_digest(&built.bytes),
    };
    store
        .put_immutable_verified(
            &metadata_segment_object_key(&descriptor),
            Bytes::from(built.bytes),
        )
        .await?;
    Ok(descriptor)
}

/// Encodes rows immediately and rolls at the byte target or row limit.
/// One final row may cross the byte target; decoded rows are never buffered.
pub(super) struct MetadataSegmentWriter<'a> {
    family: MetadataRowFamily,
    destination: MetadataSegmentDestination<'a>,
    builder: SegmentBlocksBuilder,
    segments: Vec<MetadataSegmentRef>,
}

impl<'a> MetadataSegmentWriter<'a> {
    pub(super) fn new(
        family: MetadataRowFamily,
        destination: MetadataSegmentDestination<'a>,
    ) -> Self {
        Self {
            family,
            destination,
            builder: SegmentBlocksBuilder::default(),
            segments: Vec::new(),
        }
    }

    pub(super) fn push(
        &mut self,
        row: MetadataRow,
        fold_encoded_row: &mut impl FnMut(&[u8]),
    ) -> Result<()> {
        let encoded = self
            .builder
            .push_with_encoded_row(
                &row.row_key_for_family(self.family),
                &row.filter_key_for_family(self.family),
                &row,
            )
            .map_err(|error| {
                CoreError::Internal(format!("failed to encode metadata segment: {error}"))
            })?;
        fold_encoded_row(&encoded);
        Ok(())
    }

    pub(super) async fn roll_full_segments<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        policy: MetadataLsmPolicy,
    ) -> Result<()> {
        if self.builder.row_count() >= policy.max_rows_per_segment.get() as u64
            || self.builder.decoded_data_bytes() >= policy.target_segment_bytes.get()
        {
            self.write_segment(store).await?;
        }
        Ok(())
    }

    pub(super) async fn finish<S: ObjectStore + ?Sized>(
        mut self,
        store: &S,
    ) -> Result<Vec<MetadataSegmentRef>> {
        if self.builder.row_count() > 0 {
            self.write_segment(store).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized>(&mut self, store: &S) -> Result<()> {
        let segment_index = u32::try_from(self.segments.len())
            .map_err(|_| CoreError::Internal("metadata segment index overflow".to_owned()))?;
        let built = std::mem::take(&mut self.builder)
            .finish()
            .map_err(|error| {
                CoreError::Internal(format!("failed to encode metadata segment: {error}"))
            })?;
        self.segments.push(
            write_manifest_segment(store, self.destination, self.family, segment_index, built)
                .await?,
        );
        Ok(())
    }
}
