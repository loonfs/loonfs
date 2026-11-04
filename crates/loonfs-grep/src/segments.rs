//! One namespace's grep index segments: writing runs, the block loads that
//! merges read them through, and the reads of one key range that lookups
//! make.

mod keys;

use crate::cache::GrepBlockCache;
use crate::codec::{lookup, ContentValue, IndexRow, ValueRow};
use crate::entry::grep_extension_name;
use crate::index_read::{
    index_segment_corrupt, load_data_block, load_data_block_span, load_filter_block,
    load_index_block,
};
use crate::service::GREP_READS_IN_FLIGHT;
use crate::worker::GREP_WORKER_REQUESTS_IN_FLIGHT;
use crate::{GrepError, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use keys::KeyLookup;
use loonfs::engine::{LoadedBlocks, SegmentBlockLoader};
use loonfs::MAX_PENDING_SEGMENT_BYTES;
use loonfs_objectstore::keys::extension_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::hex::hex_decode_bytes;
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::format::segment_blocks::{
    decode_filter_block, index_blocks_for_key_range, key_range_may_intersect, SegmentBlocksBuilder,
    SegmentFilter, SegmentIndexEntry, DEFAULT_TARGET_SEGMENT_BYTES,
};
use loonfs_types::{IndexSegmentId, NamespaceId};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Returns the value table rows among `rows`, which a read of a run's value
/// table keys selected, or `IndexCorrupt` for a row of another family.
fn value_rows(object_key: &str, rows: impl IntoIterator<Item = IndexRow>) -> Result<Vec<ValueRow>> {
    rows.into_iter()
        .map(|row| match row {
            IndexRow::Value(row) => Ok(row),
            _ => Err(GrepError::IndexCorrupt {
                message: format!(
                    "the run of `{object_key}` has a row of another family under a value table key"
                ),
            }),
        })
        .collect()
}

/// The rows of one segment in a key range, and the key that a bounded read
/// stopped after.
pub(crate) struct RangeRows {
    /// The rows in the range, in key order, with their keys.
    pub(crate) rows: Vec<(String, IndexRow)>,
    /// The last key of the last data block read, when blocks of the range
    /// remain unread.
    pub(crate) read_through: Option<String>,
}

/// The grep segments that one namespace reads, which it or a fork source
/// owns, and the segments that it writes under its own prefix, read through
/// `block_cache` when it is set.
pub(crate) struct GrepSegments<'a, S: ?Sized> {
    pub(crate) store: &'a S,
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) block_cache: Option<&'a GrepBlockCache>,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Returns the object key of `descriptor`'s segment, under its owner's
    /// prefix.
    pub(crate) fn object_key(&self, descriptor: &ExtensionSegmentRef) -> String {
        extension_segment_object_key(&grep_extension_name(), descriptor)
    }

    /// Writes `rows`, which are in key order, as the segments of one run
    /// through a [`RunWriter`], and returns the segments' descriptors in key
    /// order.
    pub(crate) async fn write_run(
        &self,
        rows: Vec<IndexRow>,
        max_rows_per_segment: NonZeroUsize,
    ) -> Result<Vec<ExtensionSegmentRef>> {
        let mut writer = RunWriter::new(self, max_rows_per_segment);
        for row in &rows {
            writer.push(row).await?;
        }
        writer.finish().await
    }

    /// Returns the filter of `descriptor`, from the descriptor's inline copy
    /// when it has one, and otherwise from the segment's filter block.
    async fn filter(&self, descriptor: &ExtensionSegmentRef) -> Result<Arc<SegmentFilter>> {
        let object_key = self.object_key(descriptor);
        match &descriptor.filter_inline {
            Some(inline) => {
                let filter_bytes =
                    hex_decode_bytes(inline).map_err(|error| GrepError::IndexCorrupt {
                        message: format!(
                            "index segment `{object_key}` carries undecodable inline filter hex: \
                             {error}"
                        ),
                    })?;
                let filter = decode_filter_block(&filter_bytes, &descriptor.filter_block)
                    .map_err(|error| index_segment_corrupt(&object_key, "filter block", &error))?;
                Ok(Arc::new(filter))
            }
            None => load_filter_block(self.store, self.block_cache, &object_key, descriptor).await,
        }
    }

    /// Reads the rows of `descriptor` whose keys are in `[lower_bound,
    /// upper_bound)`, when the segment's key range overlaps the range and its
    /// filter admits `probe`, from at most `max_blocks` data blocks. A read
    /// of a value table passes no probe, because the key range decides it.
    ///
    /// The range's data blocks are independent ranged GETs, so they fan out
    /// with at most [`GREP_READS_IN_FLIGHT`] in flight, and `buffered`
    /// returns them in key order.
    pub(crate) async fn rows_in_range(
        &self,
        descriptor: &ExtensionSegmentRef,
        probe: Option<&str>,
        lower_bound: &str,
        upper_bound: Option<&str>,
        max_blocks: usize,
    ) -> Result<RangeRows> {
        let mut range_rows = RangeRows {
            rows: Vec::new(),
            read_through: None,
        };
        if !key_range_may_intersect(
            &descriptor.min_row_key,
            &descriptor.max_row_key,
            descriptor.row_count,
            lower_bound,
            upper_bound,
        ) {
            return Ok(range_rows);
        }
        if let Some(probe) = probe {
            if !self.filter(descriptor).await?.may_contain(probe) {
                return Ok(range_rows);
            }
        }
        let object_key = self.object_key(descriptor);
        let entries =
            load_index_block(self.store, self.block_cache, &object_key, descriptor).await?;
        let mut range = index_blocks_for_key_range(&entries, lower_bound, upper_bound);
        if range.len() > max_blocks {
            range.end = range.start + max_blocks;
            range_rows.read_through = Some(entries[range.end - 1].last_row_key.clone());
        }
        let mut blocks = stream::iter(range)
            .map(|index| {
                load_data_block(
                    self.store,
                    self.block_cache,
                    &object_key,
                    &descriptor.segment_id,
                    &entries[index].block,
                )
            })
            .buffered(GREP_READS_IN_FLIGHT);
        while let Some(block) = blocks.try_next().await? {
            range_rows.rows.extend(
                block
                    .row_keys
                    .iter()
                    .zip(&block.rows)
                    .filter(|(row_key, _)| {
                        row_key.as_str() >= lower_bound
                            && upper_bound.is_none_or(|upper_bound| row_key.as_str() < upper_bound)
                    })
                    .map(|(row_key, row)| (row_key.clone(), row.clone())),
            );
        }
        Ok(range_rows)
    }

    /// Returns the values at `ordinals` in the value table of the run whose
    /// segments are `run`. Every ordinal has to be in the table, because a
    /// posting refers to it.
    pub(crate) async fn table_values(
        &self,
        run: &[ExtensionSegmentRef],
        ordinals: &BTreeSet<u64>,
    ) -> Result<BTreeMap<u64, ContentValue>> {
        let lookups: Vec<KeyLookup> = ordinals
            .iter()
            .map(|ordinal| KeyLookup::new(lookup::value_row_key(*ordinal), None))
            .collect();
        let rows = self
            .rows_at_keys(run, &lookups, GREP_READS_IN_FLIGHT)
            .await?;
        let object_key = run
            .first()
            .map(|segment| self.object_key(segment))
            .unwrap_or_default();
        let mut values = BTreeMap::new();
        for row in value_rows(&object_key, rows.into_iter().map(|found| found.row))? {
            values.insert(row.ordinal, row.value());
        }
        if let Some(missing) = ordinals
            .iter()
            .find(|ordinal| !values.contains_key(ordinal))
        {
            return Err(GrepError::IndexCorrupt {
                message: format!(
                    "a posting in the run of `{object_key}` refers to ordinal {missing}, which \
                     the run's value table doesn't list"
                ),
            });
        }
        Ok(values)
    }
}

/// Finishes the segment that `builder` has the rows of, which `owner` stores
/// under its own prefix, and returns its descriptor and its bytes.
fn prepare_segment(
    owner: &NamespaceId,
    builder: SegmentBlocksBuilder,
) -> (ExtensionSegmentRef, Bytes) {
    let built = builder.build().expect("a grep index segment should encode");
    let filter_inline = built.inline_filter_hex();
    let descriptor = ExtensionSegmentRef {
        owner_namespace_id: owner.clone(),
        segment_id: IndexSegmentId::generate(),
        row_count: built.row_count,
        min_row_key: built.min_row_key,
        max_row_key: built.max_row_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
    };
    (descriptor, Bytes::from(built.bytes))
}

impl<S: ObjectStore + ?Sized> SegmentBlockLoader<IndexRow, ExtensionSegmentRef>
    for GrepSegments<'_, S>
{
    type Error = GrepError;

    async fn load_index(
        &self,
        segment: ExtensionSegmentRef,
    ) -> Result<Arc<Vec<SegmentIndexEntry>>> {
        let object_key = self.object_key(&segment);
        load_index_block(self.store, self.block_cache, &object_key, &segment).await
    }

    async fn load_data_blocks(
        &self,
        segment: ExtensionSegmentRef,
        entries: Vec<SegmentIndexEntry>,
    ) -> Result<LoadedBlocks<IndexRow>> {
        let object_key = self.object_key(&segment);
        let blocks = load_data_block_span(
            self.store,
            self.block_cache,
            &object_key,
            &segment.segment_id,
            &entries,
        )
        .await?;
        // A grep step bounds the blocks it keeps by its own input budget.
        Ok(LoadedBlocks {
            blocks,
            reservation: None,
        })
    }
}

/// Writes one run's rows as a caller produces them, in key order.
///
/// The writer rolls a segment at the row limit or at
/// [`DEFAULT_TARGET_SEGMENT_BYTES`] of decoded rows, as a metadata segment
/// writer does, and keeps at most [`GREP_WORKER_REQUESTS_IN_FLIGHT`] puts and
/// [`MAX_PENDING_SEGMENT_BYTES`] of segment bodies in flight.
pub(crate) struct RunWriter<'s, 'a, S: ?Sized> {
    segments: &'s GrepSegments<'a, S>,
    max_rows_per_segment: NonZeroUsize,
    builder: SegmentBlocksBuilder,
    written: Vec<ExtensionSegmentRef>,
    /// The puts in flight, each of which returns its body's size.
    pending: FuturesUnordered<BoxFuture<'a, Result<usize>>>,
    pending_bytes: usize,
}

impl<'s, 'a, S: ObjectStore + ?Sized> RunWriter<'s, 'a, S> {
    pub(crate) fn new(
        segments: &'s GrepSegments<'a, S>,
        max_rows_per_segment: NonZeroUsize,
    ) -> Self {
        Self {
            segments,
            max_rows_per_segment,
            builder: SegmentBlocksBuilder::default(),
            written: Vec::new(),
            pending: FuturesUnordered::new(),
            pending_bytes: 0,
        }
    }

    /// Adds `row`, whose key is above the key of every row before it.
    pub(crate) async fn push(&mut self, row: &IndexRow) -> Result<()> {
        self.builder
            .push(&row.row_key(), &row.filter_key(), row)
            .expect("a grep index row should encode in ascending key order");
        if self.builder.row_count() >= self.max_rows_per_segment.get() as u64
            || self.builder.decoded_data_bytes() >= DEFAULT_TARGET_SEGMENT_BYTES
        {
            self.write_segment().await?;
        }
        Ok(())
    }

    /// Awaits `work` while polling the puts in flight, so the puts progress
    /// while the caller waits for reads. A failed put fails the call.
    pub(crate) async fn run<T>(&mut self, work: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::pin!(work);
        loop {
            tokio::select! {
                biased;
                result = self.wait_for_put(), if !self.pending.is_empty() => {
                    result?;
                }
                result = &mut work => return result,
            }
        }
    }

    /// Writes the last segment, waits for every put, and returns the run's
    /// segments in key order.
    pub(crate) async fn finish(mut self) -> Result<Vec<ExtensionSegmentRef>> {
        if self.builder.row_count() > 0 {
            self.write_segment().await?;
        }
        while !self.pending.is_empty() {
            self.wait_for_put().await?;
        }
        Ok(self.written)
    }

    async fn write_segment(&mut self) -> Result<()> {
        self.wait_for_capacity(0).await?;
        let (descriptor, bytes) = prepare_segment(
            self.segments.namespace_id,
            std::mem::take(&mut self.builder),
        );
        let object_key = self.segments.object_key(&descriptor);
        self.written.push(descriptor);
        let byte_count = bytes.len();
        self.wait_for_capacity(byte_count).await?;
        let store = self.segments.store;
        let mut put = async move {
            store
                .put_immutable_verified(&object_key, bytes)
                .await
                .map_err(|error| GrepError::store(&object_key, &error))?;
            Ok(byte_count)
        }
        .boxed();
        // Start the request before encoding the next segment, without
        // spawning a task.
        match put.as_mut().now_or_never() {
            Some(result) => {
                result?;
            }
            None => {
                self.pending_bytes += byte_count;
                self.pending.push(put);
            }
        }
        Ok(())
    }

    async fn wait_for_capacity(&mut self, bytes: usize) -> Result<()> {
        while !self.pending.is_empty()
            && (self.pending.len() >= GREP_WORKER_REQUESTS_IN_FLIGHT
                || self.pending_bytes.saturating_add(bytes) > MAX_PENDING_SEGMENT_BYTES)
        {
            self.wait_for_put().await?;
        }
        Ok(())
    }

    async fn wait_for_put(&mut self) -> Result<()> {
        if let Some(bytes) = self.pending.try_next().await? {
            self.pending_bytes -= bytes;
        }
        Ok(())
    }
}
