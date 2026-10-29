//! One namespace's grep index segments: writing runs, and the block loads
//! that merges read them through.

use crate::cache::GrepBlockCache;
use crate::codec::IndexRow;
use crate::index_read::{load_data_block_span, load_index_block};
use crate::keyspace::segment_key;
use crate::manifest::GrepSegmentRef;
use crate::worker::GREP_WORKER_REQUESTS_IN_FLIGHT;
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs::engine::SegmentBlockLoader;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::segment_blocks::{
    DecodedDataBlock, SegmentBlocksBuilder, SegmentIndexEntry,
};
use loonfs_types::{IndexSegmentId, NamespaceId, RunNo};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// One namespace's grep segments in one store, read through `block_cache`
/// when it is set.
pub(crate) struct GrepSegments<'a, S: ?Sized> {
    pub(crate) store: &'a S,
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) block_cache: Option<&'a GrepBlockCache>,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Returns the object key of the segment `segment_id`.
    pub(crate) fn object_key(&self, segment_id: &IndexSegmentId) -> String {
        segment_key(self.namespace_id, segment_id)
    }

    /// Writes `rows` as one run of segments at `level`, with at most
    /// `max_rows_per_segment` rows in each segment, and returns the segments'
    /// descriptors in row order.
    pub(crate) async fn write_run(
        &self,
        run_no: RunNo,
        level: u32,
        rows: Vec<IndexRow>,
        max_rows_per_segment: NonZeroUsize,
    ) -> Result<Vec<GrepSegmentRef>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let requests: Vec<_> = rows
            .chunks(max_rows_per_segment.get())
            .map(<[_]>::to_vec)
            .collect();
        stream::iter(requests)
            .map(|segment_rows| self.write_segment(run_no, level, segment_rows))
            .buffered(GREP_WORKER_REQUESTS_IN_FLIGHT)
            .try_collect()
            .await
    }

    async fn write_segment(
        &self,
        run_no: RunNo,
        level: u32,
        rows: Vec<IndexRow>,
    ) -> Result<GrepSegmentRef> {
        let segment_id = IndexSegmentId::generate();
        let object_key = self.object_key(&segment_id);
        let mut builder = SegmentBlocksBuilder::default();
        for row in &rows {
            builder
                .push(&row.row_key(), &row.filter_key(), row)
                .expect("a grep index row should encode in ascending key order");
        }
        let built = builder.build().expect("a grep index segment should encode");
        let filter_inline = built.inline_filter_hex();
        self.store
            .put_immutable_verified(&object_key, bytes::Bytes::from(built.bytes))
            .await
            .map_err(|error| GrepError::store(&object_key, &error))?;
        Ok(GrepSegmentRef {
            segment_id,
            run_no,
            level,
            row_count: built.row_count,
            min_row_key: built.min_row_key,
            max_row_key: built.max_row_key,
            index_block: built.index,
            filter_block: built.filter,
            filter_inline,
        })
    }
}

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<IndexRow, GrepSegmentRef> for GrepSegments<'_, S> {
    type Error = GrepError;

    async fn load_index(&self, segment: GrepSegmentRef) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        let object_key = self.object_key(&segment.segment_id);
        load_index_block(self.store, self.block_cache, &object_key, &segment).await
    }

    async fn load_data_blocks(
        &self,
        segment: GrepSegmentRef,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<Vec<Arc<DecodedDataBlock<IndexRow>>>> {
        let object_key = self.object_key(&segment.segment_id);
        load_data_block_span(self.store, &object_key, &entries).await
    }
}
