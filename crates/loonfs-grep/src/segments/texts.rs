//! The lookups of content values' `texts` rows in an index's segments,
//! which tell a build step or a query's tail scan whether the index has
//! examined a value.

use super::GrepSegments;
use crate::codec::{lookup, ContentValue, IndexRow};
use crate::index_read::{load_data_block, load_index_block};
use crate::{GrepError, Result};
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys::after_row_key;
use loonfs_types::format::manifest::ExtensionSegmentRef;
use loonfs_types::format::segment_blocks::{
    index_blocks_for_key_range, key_range_may_intersect, BlockHandle,
};
use std::collections::{BTreeMap, BTreeSet};

/// The `texts` row of one value that a lookup reads.
struct TextLookup<'a> {
    value: &'a ContentValue,
    /// The row's filter key.
    probe: String,
    row_key: String,
    /// The exclusive bound just after `row_key`.
    upper_bound: String,
}

/// One data block of one segment, and the lookups whose rows the block may
/// contain.
///
/// The block refers to its segment and its lookups by position, so a read of
/// the block is a future without borrowed arguments. (A server's router
/// requires a handler's future to be `Send`, and a stream whose closures take
/// borrowed arguments makes the future fail that check.)
struct TextBlock {
    /// The position of the block's segment in the segments that the lookup
    /// reads.
    segment: usize,
    handle: BlockHandle,
    /// The positions of the lookups whose keys the block may contain.
    lookups: Vec<usize>,
}

impl<S: ObjectStore + ?Sized> GrepSegments<'_, S> {
    /// Returns whether each of `values` that has a `texts` row in `segments`
    /// was eligible, and leaves out the values without a row.
    ///
    /// The lookups read each segment's filter once, and probe it for every
    /// value whose row key is in the segment's key range. Then they read the
    /// index block of each segment whose filter admits a key, and each data
    /// block that an admitted key falls in, once. So however many values a
    /// call looks up, it reads each segment's filter, index block, and data
    /// blocks at most once, and it reads nothing from a segment whose key
    /// range covers none of the values. At most `in_flight` filter and index
    /// reads, and at most `in_flight` data block reads, are in flight at once.
    ///
    /// Rows of one value in two segments record the same eligibility, because
    /// eligibility is a property of the bytes, so a disagreement is index
    /// corruption.
    pub(crate) async fn text_rows(
        &self,
        segments: &[ExtensionSegmentRef],
        values: &BTreeSet<ContentValue>,
        in_flight: usize,
    ) -> Result<BTreeMap<ContentValue, bool>> {
        let lookups: Vec<TextLookup<'_>> = values
            .iter()
            .map(|value| {
                let content_id = value.content_id();
                let row_key = lookup::text_row_key(&content_id, value.size_bytes);
                TextLookup {
                    value,
                    probe: lookup::text_probe(&content_id),
                    upper_bound: after_row_key(&row_key),
                    row_key,
                }
            })
            .collect();
        // Each segment's data blocks are read right after its filter and
        // index block, rather than after every segment's filter, so a block
        // that a whole-segment load cached is read before later segments'
        // loads can evict it.
        let mut reads = stream::iter(0..segments.len())
            .map(|segment| self.text_blocks(segment, &segments[segment], &lookups))
            .buffered(in_flight)
            .map_ok(|blocks| stream::iter(blocks).map(Ok::<_, GrepError>))
            .try_flatten()
            .map_ok(|block| {
                let descriptor = &segments[block.segment];
                async move {
                    let data = load_data_block(
                        self.store,
                        self.block_cache,
                        &self.object_key(descriptor),
                        &descriptor.segment_id,
                        &block.handle,
                    )
                    .await?;
                    Ok((block, data))
                }
            })
            .try_buffered(in_flight);
        let mut texts = BTreeMap::new();
        while let Some((block, data)) = reads.try_next().await? {
            for lookup in block.lookups.iter().map(|position| &lookups[*position]) {
                let Ok(position) = data
                    .row_keys
                    .binary_search_by(|row_key| row_key.as_str().cmp(&lookup.row_key))
                else {
                    continue;
                };
                let IndexRow::Text(row) = &data.rows[position] else {
                    return Err(GrepError::IndexCorrupt {
                        message: format!(
                            "index segment `{}` has a row of another family under a `texts` key",
                            self.object_key(&segments[block.segment])
                        ),
                    });
                };
                if texts.insert(*lookup.value, row.eligible) == Some(!row.eligible) {
                    return Err(GrepError::IndexCorrupt {
                        message: format!(
                            "the index records content `{}` of {} bytes as both eligible and \
                             ineligible",
                            lookup.value.content_id(),
                            lookup.value.size_bytes
                        ),
                    });
                }
            }
        }
        Ok(texts)
    }

    /// Returns the data blocks of `descriptor` (the segment at position
    /// `segment`) that may contain the rows of `lookups`, each with the
    /// lookups whose keys the block may contain.
    ///
    /// Only the keys in the segment's key range that the segment's filter
    /// admits are looked up in the segment's index.
    async fn text_blocks(
        &self,
        segment: usize,
        descriptor: &ExtensionSegmentRef,
        lookups: &[TextLookup<'_>],
    ) -> Result<Vec<TextBlock>> {
        let in_range: Vec<usize> = (0..lookups.len())
            .filter(|position| {
                let lookup = &lookups[*position];
                key_range_may_intersect(
                    &descriptor.min_row_key,
                    &descriptor.max_row_key,
                    descriptor.row_count,
                    &lookup.row_key,
                    Some(&lookup.upper_bound),
                )
            })
            .collect();
        if in_range.is_empty() {
            return Ok(Vec::new());
        }
        let filter = self.filter(descriptor).await?;
        let admitted: Vec<usize> = in_range
            .into_iter()
            .filter(|position| filter.may_contain(&lookups[*position].probe))
            .collect();
        if admitted.is_empty() {
            return Ok(Vec::new());
        }
        let entries = load_index_block(
            self.store,
            self.block_cache,
            &self.object_key(descriptor),
            descriptor,
        )
        .await?;
        let mut blocks: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for position in admitted {
            let lookup = &lookups[position];
            if let Some(index) =
                index_blocks_for_key_range(&entries, &lookup.row_key, Some(&lookup.upper_bound))
                    .next()
            {
                blocks.entry(index).or_default().push(position);
            }
        }
        Ok(blocks
            .into_iter()
            .map(|(index, lookups)| TextBlock {
                segment,
                handle: entries[index].block,
                lookups,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::GrepSegments;
    use crate::cache::GrepBlockCache;
    use crate::codec::{ContentValue, IndexRow};
    use crate::index_read::{segment_object_bytes, MAX_WHOLE_SEGMENT_LOAD_BYTES};
    use loonfs::engine::DecodedBlockCacheConfig;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    use loonfs_types::{ContentId, NamespaceId};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;

    const SEGMENTS: u64 = 4;
    /// Enough `texts` rows that a segment's filter isn't inline.
    const SMALL_SEGMENT_ROWS: u64 = 1_024;
    /// Enough `texts` rows that a segment is too large to read whole.
    const LARGE_SEGMENT_ROWS: u64 = 32_768;
    /// The values of each segment that the lookup asks for.
    const FOUND_PER_SEGMENT: u64 = 16;

    fn value(id: u64) -> ContentValue {
        ContentValue::new(&ContentId::from_be_bytes(u128::from(id).to_be_bytes()), id)
    }

    /// Checks that a lookup of many values reads each segment's filter,
    /// index block, and data block once, rather than once for each value, and
    /// that a block cache turns those reads into one load of each segment.
    #[tokio::test]
    async fn a_texts_lookup_reads_each_segment_once_whatever_the_number_of_values() {
        texts_lookup_reads(SMALL_SEGMENT_ROWS, false, [3, 1]).await;
    }

    /// Checks that through a block cache, a lookup in a segment too large to
    /// read whole reads the segment's filter and index block in one GET, and
    /// then the data block.
    #[tokio::test]
    async fn a_cached_lookup_in_a_large_segment_reads_its_filter_and_index_in_one_get() {
        texts_lookup_reads(LARGE_SEGMENT_ROWS, true, [3, 2]).await;
    }

    /// Writes `SEGMENTS` segments of `rows_per_segment` rows each, larger
    /// than the whole-segment load when `large`, looks up values in all of
    /// them, and checks the GETs per segment of an uncached lookup and of a
    /// lookup through a block cache, in that order.
    async fn texts_lookup_reads(rows_per_segment: u64, large: bool, gets_per_segment: [usize; 2]) {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let namespace_id = NamespaceId::parse("texts-lookups").expect("namespace id");
        let uncached = GrepSegments {
            store: &store,
            namespace_id: &namespace_id,
            block_cache: None,
        };
        // The `n`th row has the value with the odd ID `2n + 1`, in segment
        // `n % SEGMENTS`, so every segment's key range covers every value,
        // as a build step's run usually does, because content IDs are
        // random. The lookup asks for the first values of each segment, and
        // as many values with even IDs between them, which have no row.
        let mut segments = Vec::new();
        let mut expected = BTreeMap::new();
        let mut values = BTreeSet::new();
        for segment in 0..SEGMENTS {
            let mut rows = Vec::new();
            for row in 0..rows_per_segment {
                let n = row * SEGMENTS + segment;
                let eligible = !n.is_multiple_of(3);
                rows.push(IndexRow::text(&value(2 * n + 1), eligible));
                if row < FOUND_PER_SEGMENT {
                    expected.insert(value(2 * n + 1), eligible);
                    values.extend([value(2 * n + 1), value(2 * n + 2)]);
                }
            }
            rows.sort_by_key(IndexRow::row_key);
            let run = uncached
                .write_run(rows, NonZeroUsize::MAX)
                .await
                .expect("write a run");
            assert_eq!(run.len(), 1);
            assert!(run[0].filter_inline.is_none());
            let object_bytes = segment_object_bytes("texts", &run[0]).expect("the segment's size");
            assert_eq!(
                object_bytes > MAX_WHOLE_SEGMENT_LOAD_BYTES,
                large,
                "a segment of {object_bytes} bytes"
            );
            segments.extend(run);
        }

        let cache = GrepBlockCache::new(DecodedBlockCacheConfig::new(16 << 20));
        for (block_cache, gets_per_segment) in
            [None, Some(&cache)].into_iter().zip(gets_per_segment)
        {
            store.reset();
            let found = GrepSegments {
                block_cache,
                ..uncached
            }
            .text_rows(&segments, &values, 8)
            .await
            .expect("look up the values");
            assert_eq!(found, expected);
            assert_eq!(
                store.take_gets().len(),
                gets_per_segment * segments.len(),
                "with a cache: {}",
                block_cache.is_some()
            );
        }
    }
}
