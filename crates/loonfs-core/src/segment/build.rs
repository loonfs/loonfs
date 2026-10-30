//! Segments metadata rows into runs and writes the immutable metadata
//! segments a manifest references.

use super::cache::MetadataSegmentCache;
use super::index_block_load::{insert_written_segment, SegmentWriter};
use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq, with_layouts};
use super::runs::{MetadataFamilySegments, MetadataRunPolicy};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::limits::MAX_PENDING_SEGMENT_BYTES;
use crate::metadata::MetadataState;
use crate::namespace::store::NamespaceStore;
use crate::phase::Phase;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, METADATA_SEGMENT_ENCODING,
};
#[cfg(test)]
pub(crate) use loonfs_types::format::segment_blocks::DEFAULT_MAX_INLINE_FILTER_BYTES as INLINE_SEGMENT_FILTER_MAX_BYTES;
use loonfs_types::format::segment_blocks::{BuiltSegmentBlocks, SegmentBlocksBuilder};
use loonfs_types::{ChangeSeq, ContentId, MetadataSegmentId, NamespaceId};
use std::collections::HashMap;
use std::future::Future;

impl<S: ObjectStore> NamespaceStore<S> {
    pub(crate) async fn build_manifest_segments(
        &self,
        metadata_state: &MetadataState,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
    ) -> Result<Vec<MetadataFamilySegments>> {
        self.build_manifest_segments_from_rows(|family| {
            with_layouts(
                family,
                manifest_rows_for_family(metadata_state, family),
                layouts,
            )
        })
        .await
    }

    pub(crate) async fn build_manifest_delta_run_segments(
        &self,
        after_seq: ChangeSeq,
        metadata_state: &MetadataState,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
    ) -> Result<Vec<MetadataFamilySegments>> {
        self.build_manifest_segments_from_rows(|family| {
            with_layouts(
                family,
                manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
                layouts,
            )
        })
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = Phase::WriteManifestSegments.as_str(), key_class = "metadata_segment")
    )]
    pub(crate) async fn build_manifest_segments_from_rows<RowsForFamily>(
        &self,
        mut rows_for_family: RowsForFamily,
    ) -> Result<Vec<MetadataFamilySegments>>
    where
        RowsForFamily: FnMut(MetadataRowFamily) -> Vec<MetadataRow>,
    {
        let mut puts =
            MetadataSegmentPuts::new(&self.store, &self.segment_cache, SegmentWriter::Fold);
        let mut segments_by_family = Vec::with_capacity(MetadataRowFamily::ALL.len());
        for family in MetadataRowFamily::ALL {
            let mut writer = MetadataSegmentWriter::new(family, &self.namespace_id);
            for row in rows_for_family(family) {
                writer.push(row, &mut |_| {});
                writer
                    .roll_full_segments(&mut puts, self.run_policy)
                    .await?;
            }
            segments_by_family.push(MetadataFamilySegments {
                family,
                segments: writer.finish(&mut puts).await?,
            });
        }
        puts.finish().await?;
        Ok(segments_by_family)
    }

    #[cfg(test)]
    pub(crate) async fn write_manifest_segment(
        &self,
        family: MetadataRowFamily,
        built: BuiltSegmentBlocks,
    ) -> Result<MetadataSegmentRef> {
        let (descriptor, bytes) = prepare_manifest_segment(&self.namespace_id, family, built);
        let object_key = metadata_segment_object_key(&descriptor);
        self.store
            .put_immutable_verified(&object_key, bytes)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))?;
        Ok(descriptor)
    }
}

/// Checks newly built test segments for the same non-overlap invariant that
/// manifest loading enforces. Production merges stream through
/// [`MetadataSegmentWriter`].
#[cfg(test)]
pub(crate) fn debug_assert_manifest_segments_do_not_overlap(
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

fn prepare_manifest_segment(
    namespace_id: &NamespaceId,
    family: MetadataRowFamily,
    built: BuiltSegmentBlocks,
) -> (MetadataSegmentRef, Bytes) {
    let segment_id = MetadataSegmentId::generate();
    let filter_inline = built.inline_filter_hex();
    let descriptor = MetadataSegmentRef {
        owner_namespace_id: namespace_id.clone(),
        segment_id,
        family,
        encoding: METADATA_SEGMENT_ENCODING,
        row_count: built.row_count,
        min_row_key: built.min_row_key,
        max_row_key: built.max_row_key,
        index_block: built.index,
        filter_block: built.filter,
        filter_inline,
    };
    (descriptor, Bytes::from(built.bytes))
}

/// The segment puts in flight for one fold or compaction, bounded by
/// `STORE_WRITES_IN_FLIGHT` puts and `MAX_PENDING_SEGMENT_BYTES` of bodies.
/// When a put succeeds, LoonFS inserts the segment's blocks into
/// `segment_cache`, as [`insert_written_segment`] describes for `writer`.
pub(crate) struct MetadataSegmentPuts<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    segment_cache: &'a MetadataSegmentCache,
    writer: SegmentWriter,
    pending: FuturesUnordered<BoxFuture<'a, Result<usize>>>,
    pending_bytes: usize,
}

impl<'a, S: ObjectStore + ?Sized> MetadataSegmentPuts<'a, S> {
    pub(crate) fn new(
        store: &'a S,
        segment_cache: &'a MetadataSegmentCache,
        writer: SegmentWriter,
    ) -> Self {
        Self {
            store,
            segment_cache,
            writer,
            pending: FuturesUnordered::new(),
            pending_bytes: 0,
        }
    }

    async fn wait_for_capacity(&mut self, bytes: usize) -> Result<()> {
        while !self.pending.is_empty()
            && (self.pending.len() >= STORE_WRITES_IN_FLIGHT
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

    /// Awaits `work` while polling pending puts, so puts progress while the
    /// merge waits for input reads. A failed put fails the call.
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

    async fn put(&mut self, descriptor: &MetadataSegmentRef, bytes: Bytes) -> Result<()> {
        let byte_count = bytes.len();
        self.wait_for_capacity(byte_count).await?;
        let (store, segment_cache, writer) = (self.store, self.segment_cache, self.writer);
        let object_key = metadata_segment_object_key(descriptor);
        let descriptor = descriptor.clone();
        let mut put = async move {
            store
                .put_immutable_verified(&object_key, bytes.clone())
                .await
                .map_err(|error| CoreError::store(&object_key, &error))?;
            // A segment this process can't decode must not be published in a manifest.
            insert_written_segment(segment_cache, &descriptor, &bytes, writer)?;
            Ok(byte_count)
        }
        .boxed();
        // Start the request before encoding the next segment, without spawning a task.
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

    pub(crate) async fn finish(mut self) -> Result<()> {
        while !self.pending.is_empty() {
            self.wait_for_put().await?;
        }
        Ok(())
    }
}

/// Encodes rows immediately and rolls at the byte target or row limit.
/// One final row may cross the byte target; decoded rows are never buffered.
pub(crate) struct MetadataSegmentWriter<'a> {
    family: MetadataRowFamily,
    namespace_id: &'a NamespaceId,
    builder: SegmentBlocksBuilder,
    /// The parent filter key that the open segment's last row added, so a
    /// run of rows under one parent adds it once.
    last_parent_filter_key: Option<String>,
    segments: Vec<MetadataSegmentRef>,
}

impl<'a> MetadataSegmentWriter<'a> {
    pub(crate) fn new(family: MetadataRowFamily, namespace_id: &'a NamespaceId) -> Self {
        Self {
            family,
            namespace_id,
            builder: SegmentBlocksBuilder::default(),
            last_parent_filter_key: None,
            segments: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, row: MetadataRow, fold_encoded_row: &mut impl FnMut(&[u8])) {
        // Rows arrive in key order, so a parent's rows are adjacent, and
        // comparing with the previous row's parent adds each parent once.
        if let Some(parent_filter_key) = row.parent_filter_key_for_family(self.family) {
            if self.last_parent_filter_key.as_ref() != Some(&parent_filter_key) {
                self.builder.push_filter_key(&parent_filter_key);
                self.last_parent_filter_key = Some(parent_filter_key);
            }
        }
        let encoded = self
            .builder
            .push_with_encoded_row(
                &row.row_key_for_family(self.family),
                &row.filter_key_for_family(self.family),
                &row,
            )
            .expect("a metadata segment row should encode in ascending key order");
        fold_encoded_row(&encoded);
    }

    pub(crate) async fn roll_full_segments<S: ObjectStore + ?Sized>(
        &mut self,
        puts: &mut MetadataSegmentPuts<'_, S>,
        run_policy: MetadataRunPolicy,
    ) -> Result<()> {
        if self.builder.row_count() >= run_policy.max_rows_per_segment.get() as u64
            || self.builder.decoded_data_bytes() >= run_policy.target_segment_bytes.get()
        {
            self.write_segment(puts).await?;
        }
        Ok(())
    }

    pub(crate) async fn finish<S: ObjectStore + ?Sized>(
        mut self,
        puts: &mut MetadataSegmentPuts<'_, S>,
    ) -> Result<Vec<MetadataSegmentRef>> {
        if self.builder.row_count() > 0 {
            self.write_segment(puts).await?;
        }
        Ok(self.segments)
    }

    async fn write_segment<S: ObjectStore + ?Sized>(
        &mut self,
        puts: &mut MetadataSegmentPuts<'_, S>,
    ) -> Result<()> {
        puts.wait_for_capacity(0).await?;
        // The next segment has a filter of its own, which needs the parent
        // filter key of its first row.
        self.last_parent_filter_key = None;
        let built = std::mem::take(&mut self.builder)
            .build()
            .expect("a metadata segment should encode");
        let (descriptor, bytes) = prepare_manifest_segment(self.namespace_id, self.family, built);
        puts.put(&descriptor, bytes).await?;
        self.segments.push(descriptor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::SegmentBlockKind;
    use super::super::index_block_load::{segment_block_cache_key, segment_object_bytes};
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        BlockingStore, BufferWatchStore, ConcurrencyWatchStore, KeyPredicate, OperationSelector,
    };
    use loonfs_types::format::manifest::InodeRecord;
    use loonfs_types::format::segment_blocks::decode_index_block;
    use loonfs_types::{ActorId, CommitId, InodeId, InodeKind};
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Returns a new segment descriptor whose index and filter blocks lie past
    /// the end of every body this module's tests put, so a put of zeros under
    /// it publishes no blocks to the segment cache.
    fn descriptor(namespace_id: &NamespaceId) -> MetadataSegmentRef {
        let mut builder = SegmentBlocksBuilder::default();
        builder.push("row", "row", &"row").expect("row");
        let mut descriptor = prepare_manifest_segment(
            namespace_id,
            MetadataRowFamily::Inodes,
            builder.build().expect("segment"),
        )
        .0;
        descriptor.index_block.offset = u64::MAX;
        descriptor.filter_block.offset = u64::MAX;
        descriptor
    }

    #[tokio::test]
    async fn segment_puts_bound_bytes_and_count_and_admit_oversized_bodies_alone() {
        let budget = MAX_PENDING_SEGMENT_BYTES;
        for (initial_sizes, next_size) in [
            (vec![budget / 4; 4], budget / 4),
            (vec![budget - 1], 2),
            (vec![1], budget + 1),
            (vec![budget + 1], 1),
            (vec![1; STORE_WRITES_IN_FLIGHT], 1),
        ] {
            let directory = tempdir().expect("tempdir");
            let namespace_id = NamespaceId::parse("segment-byte-budget").expect("namespace id");
            let blocked = Arc::new(BlockingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::metadata_segment(),
                OperationSelector::Put,
            ));
            let concurrency =
                ConcurrencyWatchStore::new(Arc::clone(&blocked), KeyPredicate::metadata_segment());
            let store = BufferWatchStore::new(&concurrency, KeyPredicate::metadata_segment());
            let cache = crate::cache::MetadataSegmentCache::unshared(0);
            let mut puts = MetadataSegmentPuts::new(&store, &cache, SegmentWriter::Fold);
            blocked.arm();
            for size in &initial_sizes {
                puts.put(&descriptor(&namespace_id), Bytes::from(vec![0; *size]))
                    .await
                    .expect("initial put");
            }
            let next_descriptor = descriptor(&namespace_id);
            let mut next = Box::pin(puts.put(&next_descriptor, Bytes::from(vec![0; next_size])));
            // A put of 8 MiB or more compares its key before it writes, so it
            // reaches the store only while `next` drives the pending puts.
            let earlier_puts_arrive = async {
                while concurrency.puts().total < initial_sizes.len() {
                    tokio::task::yield_now().await;
                }
            };
            let next_waited = tokio::select! {
                biased;
                _ = &mut next => false,
                () = earlier_puts_arrive => true,
            };
            assert!(next_waited, "the next put should wait for capacity");
            assert_eq!(concurrency.puts().total, initial_sizes.len());
            assert_eq!(
                store.peaks().peak_live_bytes,
                initial_sizes.iter().sum::<usize>() as u64
            );
            blocked.release();
            next.await.expect("put after capacity is available");
            puts.finish().await.expect("verified puts");
            assert_eq!(concurrency.puts().total, initial_sizes.len() + 1);
            assert!(concurrency.puts().peak_in_flight <= STORE_WRITES_IN_FLIGHT);
            assert!(
                store.peaks().peak_live_bytes <= budget.max(next_size).max(initial_sizes[0]) as u64
            );
            assert_eq!(
                store.peaks().total_bytes,
                (initial_sizes.iter().sum::<usize>() + next_size) as u64
            );
        }
    }

    /// Returns a segment of 20,000 inode rows, each with a commit ID of its
    /// own, so the rows compress poorly and the segment is larger than a
    /// segment that LoonFS fetches whole.
    fn large_inode_segment(namespace_id: &NamespaceId) -> (MetadataSegmentRef, Bytes) {
        let family = MetadataRowFamily::Inodes;
        let mut builder = SegmentBlocksBuilder::default();
        for inode in 0..20_000 {
            let row = MetadataRow::Inode(InodeRecord {
                inode_id: InodeId(u128::from(inode) + 2),
                inode_kind: InodeKind::File,
                committed_seq: ChangeSeq(inode + 1),
                commit_id: CommitId::generate(),
                committed_by: ActorId::loonfs(),
                committed_at_ms: 1_000 + inode,
            });
            builder
                .push(
                    &row.row_key_for_family(family),
                    &row.filter_key_for_family(family),
                    &row,
                )
                .expect("inode rows in key order");
        }
        let (descriptor, bytes) =
            prepare_manifest_segment(namespace_id, family, builder.build().expect("segment"));
        assert!(segment_object_bytes(&descriptor) > 256 * 1024);
        (descriptor, bytes)
    }

    /// Writes the segment to a new store through `MetadataSegmentPuts` for
    /// `writer`, which inserts the segment's blocks into `cache`.
    async fn put_segment(
        cache: &MetadataSegmentCache,
        writer: SegmentWriter,
        descriptor: &MetadataSegmentRef,
        bytes: &Bytes,
    ) {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let mut puts = MetadataSegmentPuts::new(&store, cache, writer);
        puts.put(descriptor, bytes.clone()).await.expect("put");
        puts.finish().await.expect("finish the puts");
    }

    /// Checks that a fold inserts every block of a segment larger than a
    /// segment that LoonFS fetches whole, that a compaction inserts only the
    /// segment's index and filter blocks, and that a fold's data blocks are
    /// charged to the cache's budget and evicted like any other block.
    #[tokio::test]
    async fn a_fold_inserts_every_block_of_a_large_segment_and_a_compaction_none_of_its_data() {
        let namespace_id = NamespaceId::parse("written-blocks").expect("namespace id");
        let (descriptor, bytes) = large_inode_segment(&namespace_id);
        let index_handle = descriptor.index_block;
        let index_start = loonfs_types::usize_from_u64(index_handle.offset);
        let index = decode_index_block(
            &bytes[index_start..index_start + index_handle.stored_bytes as usize],
            &index_handle,
        )
        .expect("index");
        assert!(
            index.len() >= 4,
            "the segment has {} data blocks",
            index.len()
        );
        let data_key = |position: usize| {
            segment_block_cache_key(
                &descriptor,
                SegmentBlockKind::Data,
                index[position].block.offset,
            )
        };
        let mut fold_bytes = 0;
        for (writer, data_blocks) in [
            (SegmentWriter::Fold, index.len()),
            (SegmentWriter::Compaction, 0),
        ] {
            let cache = MetadataSegmentCache::unshared(usize::MAX);
            put_segment(&cache, writer, &descriptor, &bytes).await;
            let stats = cache.stats();
            assert_eq!(stats.inserts, 2 + data_blocks, "{writer:?}");
            assert_eq!(
                cache.get(&data_key(0)).is_some(),
                writer == SegmentWriter::Fold,
                "{writer:?}"
            );
            if writer == SegmentWriter::Fold {
                fold_bytes = stats.cached_decoded_bytes;
            }
        }

        let budget = fold_bytes / 2;
        let cache = MetadataSegmentCache::unshared(budget);
        put_segment(&cache, SegmentWriter::Fold, &descriptor, &bytes).await;
        let stats = cache.stats();
        assert!(stats.evictions > 0, "{stats:?}");
        assert!(stats.cached_decoded_bytes <= budget, "{stats:?}");
        assert!(
            cache.get(&data_key(0)).is_none(),
            "the oldest data block is evicted"
        );
        assert!(
            cache.get(&data_key(index.len() - 1)).is_some(),
            "the newest data block stays"
        );
    }
}
