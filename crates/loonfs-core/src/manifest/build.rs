//! Segments metadata rows into runs and writes the immutable metadata
//! segments a manifest references.

use super::row::{manifest_rows_for_family, manifest_rows_for_family_after_seq, with_layouts};
use super::runs::{MetadataFamilySegments, MetadataLsmPolicy, MANIFEST_ROW_FAMILIES};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataState;
use crate::store_waves::STORE_WRITE_WAVE;
use bytes::Bytes;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, TryStreamExt};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{
    MetadataRow, MetadataRowFamily, MetadataSegmentRef, METADATA_SEGMENT_ENCODING,
};
#[cfg(test)]
pub(super) use loonfs_types::format::sst_blocks::DEFAULT_INLINE_FILTER_MAX_BYTES as INLINE_SEGMENT_FILTER_MAX_BYTES;
use loonfs_types::format::sst_blocks::{BuiltSegmentBlocks, SegmentBlocksBuilder};
use loonfs_types::{ChangeSeq, ContentId, MetadataSegmentId, NamespaceId};
use std::collections::HashMap;
use std::future::Future;

/// Most encoded segment bytes one fold or compaction holds while their puts
/// run. The `STORE_WRITE_WAVE` count alone allows 128 MiB of 8 MiB segments;
/// 32 MiB is about four such segments and still admits a fold's usual seven
/// small segments at once.
const MAX_PENDING_SEGMENT_BYTES: usize = 32 * 1024 * 1024;

pub(super) async fn build_manifest_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    metadata_state: &MetadataState,
    layouts: &HashMap<ContentId, ContentLayoutRecord>,
    policy: MetadataLsmPolicy,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        |family| {
            with_layouts(
                family,
                manifest_rows_for_family(metadata_state, family),
                layouts,
            )
        },
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
    layouts: &HashMap<ContentId, ContentLayoutRecord>,
    policy: MetadataLsmPolicy,
) -> Result<Vec<MetadataFamilySegments>> {
    build_manifest_segments_from_rows(
        store,
        namespace_id,
        |family| {
            with_layouts(
                family,
                manifest_rows_for_family_after_seq(metadata_state, family, after_seq),
                layouts,
            )
        },
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
    let mut puts = MetadataSegmentPuts::new(store);
    let mut segments_by_family = Vec::with_capacity(MANIFEST_ROW_FAMILIES.len());
    for family in MANIFEST_ROW_FAMILIES {
        let mut writer = MetadataSegmentWriter::new(family, namespace_id);
        for row in rows_for_family(family) {
            writer.push(row, &mut |_| {})?;
            writer.roll_full_segments(&mut puts, policy).await?;
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
pub(super) async fn write_manifest_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    family: MetadataRowFamily,

    built: BuiltSegmentBlocks,
) -> Result<MetadataSegmentRef> {
    let (descriptor, bytes) = prepare_manifest_segment(namespace_id, family, built);
    store
        .put_immutable_verified(&metadata_segment_object_key(&descriptor), bytes)
        .await?;
    Ok(descriptor)
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
/// `STORE_WRITE_WAVE` puts and `MAX_PENDING_SEGMENT_BYTES` of bodies.
pub(super) struct MetadataSegmentPuts<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
    pending: FuturesUnordered<BoxFuture<'a, Result<usize>>>,
    pending_bytes: usize,
}

impl<'a, S: ObjectStore + ?Sized> MetadataSegmentPuts<'a, S> {
    pub(super) fn new(store: &'a S) -> Self {
        Self {
            store,
            pending: FuturesUnordered::new(),
            pending_bytes: 0,
        }
    }

    async fn wait_for_capacity(&mut self, bytes: usize) -> Result<()> {
        while !self.pending.is_empty()
            && (self.pending.len() >= STORE_WRITE_WAVE
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
    pub(super) async fn run<T>(&mut self, work: impl Future<Output = Result<T>>) -> Result<T> {
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
        let store = self.store;
        let object_key = metadata_segment_object_key(descriptor);
        let mut put = async move {
            store.put_immutable_verified(&object_key, bytes).await?;
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

    pub(super) async fn finish(mut self) -> Result<()> {
        while !self.pending.is_empty() {
            self.wait_for_put().await?;
        }
        Ok(())
    }
}

/// Encodes rows immediately and rolls at the byte target or row limit.
/// One final row may cross the byte target; decoded rows are never buffered.
pub(super) struct MetadataSegmentWriter<'a> {
    family: MetadataRowFamily,
    namespace_id: &'a NamespaceId,
    builder: SegmentBlocksBuilder,
    segments: Vec<MetadataSegmentRef>,
}

impl<'a> MetadataSegmentWriter<'a> {
    pub(super) fn new(family: MetadataRowFamily, namespace_id: &'a NamespaceId) -> Self {
        Self {
            family,
            namespace_id,
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
        puts: &mut MetadataSegmentPuts<'_, S>,
        policy: MetadataLsmPolicy,
    ) -> Result<()> {
        if self.builder.row_count() >= policy.max_rows_per_segment.get() as u64
            || self.builder.decoded_data_bytes() >= policy.target_segment_bytes.get()
        {
            self.write_segment(puts).await?;
        }
        Ok(())
    }

    pub(super) async fn finish<S: ObjectStore + ?Sized>(
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
        let built = std::mem::take(&mut self.builder)
            .finish()
            .map_err(|error| {
                CoreError::Internal(format!("failed to encode metadata segment: {error}"))
            })?;
        let (descriptor, bytes) = prepare_manifest_segment(self.namespace_id, self.family, built);
        puts.put(&descriptor, bytes).await?;
        self.segments.push(descriptor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        BlockingStore, BufferWatchStore, ConcurrencyWatchStore, KeyPredicate, OperationClass,
    };
    use tempfile::tempdir;

    fn descriptor(namespace_id: &NamespaceId) -> MetadataSegmentRef {
        let mut builder = SegmentBlocksBuilder::default();
        builder.push("row", "row", &"row").expect("row");
        prepare_manifest_segment(
            namespace_id,
            MetadataRowFamily::Inodes,
            builder.finish().expect("segment"),
        )
        .0
    }

    #[tokio::test]
    async fn segment_puts_bound_bytes_and_count_and_admit_oversized_bodies_alone() {
        let budget = MAX_PENDING_SEGMENT_BYTES;
        for (initial_sizes, next_size) in [
            (vec![budget / 4; 4], budget / 4),
            (vec![budget - 1], 2),
            (vec![1], budget + 1),
            (vec![budget + 1], 1),
            (vec![1; STORE_WRITE_WAVE], 1),
        ] {
            let directory = tempdir().expect("tempdir");
            let namespace_id = NamespaceId::parse("segment-byte-budget").expect("namespace id");
            let blocked = BlockingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::metadata_segment(),
                OperationClass::Put,
            );
            let concurrency =
                ConcurrencyWatchStore::new(&blocked, KeyPredicate::metadata_segment());
            let store = BufferWatchStore::new(&concurrency, KeyPredicate::metadata_segment());
            let mut puts = MetadataSegmentPuts::new(&store);
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
            assert!(concurrency.puts().peak_in_flight <= STORE_WRITE_WAVE);
            assert!(
                store.peaks().peak_live_bytes <= budget.max(next_size).max(initial_sizes[0]) as u64
            );
            assert_eq!(
                store.peaks().total_bytes,
                (initial_sizes.iter().sum::<usize>() + next_size) as u64
            );
        }
    }
}
