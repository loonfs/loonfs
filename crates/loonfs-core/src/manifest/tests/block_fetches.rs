//! The data blocks that a read fetches: a lookup fetches the one block that
//! has its key, a bounded scan fetches the blocks of its key range, and each
//! read inserts every block that it fetches into the block cache, so a later
//! read through another view sends no GET.

use super::bounded_pages::rows;
use super::*;
use crate::segment::block_memo::SessionBlockMemo;
use loonfs_types::format::segment_blocks::index_blocks_for_key_range;

const FAMILY: ApiMetadataRowFamily = ApiMetadataRowFamily::DirentryBinds;

/// Writes `rows` into one segment whose data blocks close at
/// `block_target_bytes` decoded, and returns the segment with its index.
async fn segment_with_block_target(
    store: &impl ObjectStore,
    rows: &[MetadataRow],
    block_target_bytes: usize,
) -> (MetadataSegmentRef, Arc<Vec<SegmentIndexEntry>>) {
    let mut builder = SegmentBlocksBuilder::new(
        NonZeroUsize::new(block_target_bytes).expect("a positive block target"),
    );
    for row in rows {
        builder
            .push(
                &row.row_key_for_family(FAMILY),
                &row.filter_key_for_family(FAMILY),
                row,
            )
            .expect("encode fixture row");
    }
    let descriptor = crate::test_support::namespace_store(
        store,
        &NamespaceId::parse("block-fetches").expect("namespace"),
    )
    .write_manifest_segment(FAMILY, builder.build().expect("finish segment"))
    .await
    .expect("write fixture segment");
    let index = index_block_load::load_segment_index(
        store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("index");
    (descriptor, index)
}

fn one_segment_runs(descriptor: MetadataSegmentRef) -> Vec<MetadataRunManifest> {
    vec![MetadataRunManifest {
        run_no: RunNo(1),
        run_seq: CommitSeq(1),
        tier: RunTier::Delta,
        segments: vec![MetadataFamilySegments {
            family: FAMILY,
            segments: vec![descriptor],
        }],
    }]
}

/// The ranges of the GETs since the last take that read data blocks, which
/// all sit before the segment's filter block.
fn data_gets(
    store: &RecordingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
) -> Vec<(u64, u64)> {
    store
        .take_gets()
        .into_iter()
        .filter_map(|(_, range)| range)
        .filter(|(start, _)| *start < descriptor.filter_block.offset)
        .collect()
}

/// The stored byte range of `index[blocks]` in the segment object.
fn stored_range(index: &[SegmentIndexEntry], blocks: std::ops::Range<usize>) -> (u64, u64) {
    let last = index[blocks.end - 1].block;
    (
        index[blocks.start].block.offset,
        last.offset + u64::from(last.stored_bytes),
    )
}

/// The first row of block `position` of `index`, among `rows` in key order.
fn first_row_of_block<'a>(
    rows: &'a [MetadataRow],
    index: &[SegmentIndexEntry],
    position: usize,
) -> &'a MetadataRow {
    rows.iter()
        .find(|row| row.row_key_for_family(FAMILY) > index[position - 1].last_row_key)
        .expect("the block has a first row")
}

/// Checks that a cold lookup of a key in a segment of many small blocks sends
/// one data GET, of exactly the block that has the key, that the same lookup
/// through a fresh view sends no GET, and that a lookup of a key in the next
/// block sends one GET of that block alone.
#[tokio::test]
async fn a_lookup_fetches_exactly_the_block_that_has_its_key() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    assert!(
        index.len() > 40,
        "the fixture has many blocks: {}",
        index.len()
    );
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let block = 20;
    let lookup = |row: &MetadataRow| {
        (
            row.row_key_for_family(FAMILY),
            row.filter_key_for_family(FAMILY),
        )
    };

    for (position, through_fresh_view) in [(block, false), (block, true), (block + 1, true)] {
        let row = first_row_of_block(&expected, &index, position);
        let (key, probe) = lookup(row);
        let view = scan::VerifiedMetadataSegments::from_runs(
            &store,
            &cache,
            one_segment_runs(descriptor.clone()),
        );
        store.reset();
        let found = view
            .find_row(FAMILY, &key, &probe, scan::BlocksPerRefill::One)
            .await
            .expect("look up the key");
        assert_eq!(found.as_ref(), Some(row));
        if through_fresh_view && position == block {
            assert_eq!(
                store.take_gets(),
                Vec::new(),
                "the first lookup inserted its block into the block cache"
            );
        } else if through_fresh_view {
            assert_eq!(
                store.take_gets(),
                vec![(
                    metadata_segment_object_key(&descriptor),
                    Some(stored_range(&index, position..position + 1))
                )],
                "a lookup in the next block fetches that block alone"
            );
        } else {
            assert_eq!(
                data_gets(&store, &descriptor),
                vec![stored_range(&index, position..position + 1)],
                "a cold lookup fetches the one block that has its key"
            );
        }
    }
}

/// Checks that a cold scan of a key range sends ranged GETs that cover the
/// range's blocks and nothing past them, one GET for a range under 1 MiB,
/// and that the same scan through a fresh view sends no GET.
#[tokio::test]
async fn a_bounded_scan_fetches_the_blocks_of_its_key_range_and_nothing_past_them() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    assert!(
        index.len() > 60,
        "the fixture has many blocks: {}",
        index.len()
    );

    // Each case is the first and the last block of the range.
    for (first, last) in [(10, 40), (41, index.len() - 2)] {
        let lower = format!("{}\0", index[first - 1].last_row_key);
        let upper = index[last].last_row_key.clone();
        assert_eq!(
            index_blocks_for_key_range(&index, &lower, Some(&upper)),
            first..last + 1
        );
        let cache = MetadataSegmentCache::unshared(usize::MAX);
        let wanted: Vec<_> = expected
            .iter()
            .filter(|row| {
                let key = row.row_key_for_family(FAMILY);
                key >= lower && key < upper
            })
            .cloned()
            .collect();
        for through_fresh_view in [false, true] {
            let view = scan::VerifiedMetadataSegments::from_runs(
                &store,
                &cache,
                one_segment_runs(descriptor.clone()),
            );
            store.reset();
            let page: Vec<_> = view
                .scan_range(
                    FAMILY,
                    &lower,
                    Some(&upper),
                    usize::MAX,
                    None,
                    scan::BlocksPerRefill::KeyRange,
                )
                .await
                .expect("scan the key range")
                .into_iter()
                .map(|(_, row)| row)
                .collect();
            assert_eq!(page, wanted);
            if through_fresh_view {
                assert_eq!(store.take_gets(), Vec::new(), "blocks {first}..={last}");
                continue;
            }
            let mut gets = data_gets(&store, &descriptor);
            gets.sort_unstable();
            let (range_start, range_end) = stored_range(&index, first..last + 1);
            assert_eq!(gets.first().map(|get| get.0), Some(range_start));
            assert_eq!(gets.last().map(|get| get.1), Some(range_end));
            assert!(
                gets.windows(2).all(|pair| pair[0].1 == pair[1].0),
                "the GETs cover blocks {first}..={last} once: {gets:?}"
            );
            if range_end - range_start <= crate::limits::MAX_SCAN_REFILL_BYTES {
                assert_eq!(gets.len(), 1, "blocks {first}..={last}: {gets:?}");
            }
        }
    }
}

/// Checks that a read-ahead scan of a key range inside one block fetches
/// that block and the blocks after it, up to the refill's stored and decoded
/// caps, in one GET, and that
/// later scans of the key ranges in those blocks, through a fresh view, send
/// no GET.
#[tokio::test]
async fn a_read_ahead_scan_fetches_the_blocks_after_its_key_range() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let first = 10;
    // The blocks from `first` that fit in one refill of a scan with one
    // segment open.
    let (mut stored_bytes, mut decoded_bytes) = (0, 0);
    let end = first
        + index[first..]
            .iter()
            .take_while(|entry| {
                stored_bytes += u64::from(entry.block.stored_bytes);
                decoded_bytes += entry.block.decoded_bytes as usize;
                stored_bytes <= crate::limits::MAX_SCAN_REFILL_BYTES
                    && decoded_bytes <= crate::limits::MAX_SCAN_DECODED_BYTES
            })
            .count();
    assert!(end > first + 4, "a refill reads ahead {first}..{end}");
    let block_range = |block: usize| {
        (
            format!("{}\0", index[block - 1].last_row_key),
            index[block].last_row_key.clone(),
        )
    };
    let scan = |lower: &str, upper: &str, blocks_per_refill| {
        let (lower, upper) = (lower.to_owned(), upper.to_owned());
        let (store, cache, descriptor) = (&store, &cache, descriptor.clone());
        async move {
            scan::VerifiedMetadataSegments::from_runs(store, cache, one_segment_runs(descriptor))
                .scan_range(
                    FAMILY,
                    &lower,
                    Some(&upper),
                    usize::MAX,
                    None,
                    blocks_per_refill,
                )
                .await
                .expect("scan the key range")
                .len()
        }
    };

    let (lower, upper) = block_range(first);
    store.reset();
    assert!(scan(&lower, &upper, scan::BlocksPerRefill::ReadAhead).await > 0);
    assert_eq!(
        data_gets(&store, &descriptor),
        vec![stored_range(&index, first..end)],
        "one GET of the range's block and the blocks after it"
    );
    for block in first + 1..end {
        let (lower, upper) = block_range(block);
        assert!(scan(&lower, &upper, scan::BlocksPerRefill::KeyRange).await > 0);
        assert_eq!(store.take_gets(), Vec::new(), "block {block} is cached");
    }
}

/// Checks that a warm scan over a fully cached segment keeps at most its
/// decoded budget of blocks and one more at once, though the cache has every
/// block of the segment.
#[tokio::test]
async fn a_warm_scan_over_a_cached_segment_keeps_at_most_its_budget_and_one_block() {
    use crate::limits::MAX_SCAN_DECODED_BYTES;
    use loonfs_types::format::manifest::lookup_keys;

    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let segment_decoded_bytes: usize = index
        .iter()
        .map(|entry| entry.block.decoded_bytes as usize)
        .sum();
    assert!(
        segment_decoded_bytes > 2 * MAX_SCAN_DECODED_BYTES,
        "the segment is larger than a scan's budget: {segment_decoded_bytes}"
    );
    let smallest_block = index
        .iter()
        .map(|entry| entry.block.decoded_bytes as usize)
        .min()
        .expect("a block");
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let prefix = lookup_keys::direntry_parent_prefix(loonfs_types::ROOT_INODE_ID);
    let upper = string_prefix_upper_bound(&prefix);
    for (pass, blocks_per_refill) in [
        scan::BlocksPerRefill::KeyRange,
        scan::BlocksPerRefill::ReadAhead,
    ]
    .into_iter()
    .enumerate()
    {
        let view = scan::VerifiedMetadataSegments::from_runs(
            &store,
            &cache,
            one_segment_runs(descriptor.clone()),
        );
        store.reset();
        let page = view
            .scan_range(
                FAMILY,
                &prefix,
                upper.as_deref(),
                usize::MAX,
                None,
                blocks_per_refill,
            )
            .await
            .expect("scan the segment");
        assert_eq!(page.len(), expected.len());
        if pass == 0 {
            continue;
        }
        assert_eq!(store.take_gets(), Vec::new(), "the segment is cached");
        assert!(
            view.peak_resident_blocks() <= MAX_SCAN_DECODED_BYTES / smallest_block + 1,
            "a warm scan kept {} blocks",
            view.peak_resident_blocks()
        );
    }
}

/// Checks that a refill's caps count from the first block that it fetches:
/// a read-ahead scan whose key range starts in blocks that an earlier refill
/// cached and ends in the first block past them fetches from that block in
/// one GET as long as a cold refill from that block, not a GET shortened by
/// the cached blocks in front of it.
#[tokio::test]
async fn a_refill_that_starts_in_cached_blocks_fetches_a_full_refill_after_them() {
    use crate::limits::{MAX_SCAN_DECODED_BYTES, MAX_SCAN_REFILL_BYTES};

    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    // The blocks from `start` that one refill of a scan with one segment
    // open takes when none of them is cached.
    let refill_end = |start: usize| {
        let (mut stored_bytes, mut decoded_bytes) = (0, 0);
        start
            + index[start..]
                .iter()
                .take_while(|entry| {
                    stored_bytes += u64::from(entry.block.stored_bytes);
                    decoded_bytes += entry.block.decoded_bytes as usize;
                    stored_bytes <= MAX_SCAN_REFILL_BYTES && decoded_bytes <= MAX_SCAN_DECODED_BYTES
                })
                .count()
    };
    let scan = |lower: String, upper: String| {
        let (store, cache, descriptor) = (&store, &cache, descriptor.clone());
        async move {
            scan::VerifiedMetadataSegments::from_runs(store, cache, one_segment_runs(descriptor))
                .scan_range(
                    FAMILY,
                    &lower,
                    Some(&upper),
                    usize::MAX,
                    None,
                    scan::BlocksPerRefill::ReadAhead,
                )
                .await
                .expect("scan the key range")
                .len()
        }
    };
    let first = 10;
    let cached_end = refill_end(first);
    assert!(
        refill_end(cached_end) < index.len(),
        "the segment has blocks past a second refill"
    );
    store.reset();
    assert!(
        scan(
            format!("{}\0", index[first - 1].last_row_key),
            index[first].last_row_key.clone()
        )
        .await
            > 0
    );
    assert_eq!(
        data_gets(&store, &descriptor),
        vec![stored_range(&index, first..cached_end)]
    );

    // The key range covers the last two cached blocks and the first block
    // past them.
    store.reset();
    assert!(
        scan(
            format!("{}\0", index[cached_end - 3].last_row_key),
            index[cached_end].last_row_key.clone()
        )
        .await
            > 0
    );
    assert_eq!(
        data_gets(&store, &descriptor),
        vec![stored_range(&index, cached_end..refill_end(cached_end))],
        "one GET from the first uncached block, as long as a cold refill"
    );
}

/// Checks that a scan's refills stay within their fixed caps whatever else
/// the process is doing: a scan over many more blocks than one refill takes
/// fetches each refill with one GET of at most `MAX_SCAN_REFILL_BYTES`
/// stored and at most `MAX_SCAN_DECODED_BYTES` decoded, the GETs cover the
/// range once, and each refill stops only where its next block would pass
/// one of the caps.
#[tokio::test]
async fn a_scan_refill_stays_within_its_stored_and_decoded_caps() {
    use crate::limits::{MAX_SCAN_DECODED_BYTES, MAX_SCAN_REFILL_BYTES};

    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let (first, last) = (10, index.len() - 10);
    let lower = format!("{}\0", index[first - 1].last_row_key);
    let upper = index[last].last_row_key.clone();
    let blocks_between = |start: u64, end: u64| -> Vec<&SegmentIndexEntry> {
        index
            .iter()
            .filter(|entry| entry.block.offset >= start && entry.block.offset < end)
            .collect()
    };
    let range_stored_bytes: u64 = index[first..=last]
        .iter()
        .map(|entry| u64::from(entry.block.stored_bytes))
        .sum();
    assert!(
        range_stored_bytes > 2 * MAX_SCAN_REFILL_BYTES,
        "the range needs several refills: {range_stored_bytes}"
    );
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let view = scan::VerifiedMetadataSegments::from_runs(
        &store,
        &cache,
        one_segment_runs(descriptor.clone()),
    );
    store.reset();
    let page: Vec<_> = view
        .scan_range(
            FAMILY,
            &lower,
            Some(&upper),
            usize::MAX,
            None,
            scan::BlocksPerRefill::KeyRange,
        )
        .await
        .expect("scan the key range")
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let wanted: Vec<_> = expected
        .iter()
        .filter(|row| {
            let key = row.row_key_for_family(FAMILY);
            key >= lower && key < upper
        })
        .cloned()
        .collect();
    assert_eq!(page, wanted);

    let mut gets = data_gets(&store, &descriptor);
    gets.sort_unstable();
    let (range_start, range_end) = stored_range(&index, first..last + 1);
    assert_eq!(gets.first().map(|get| get.0), Some(range_start));
    assert_eq!(gets.last().map(|get| get.1), Some(range_end));
    assert!(
        gets.windows(2).all(|pair| pair[0].1 == pair[1].0),
        "the GETs cover blocks {first}..={last} once: {gets:?}"
    );
    for (number, (start, end)) in gets.iter().enumerate() {
        let blocks = blocks_between(*start, *end);
        let stored: u64 = blocks
            .iter()
            .map(|entry| u64::from(entry.block.stored_bytes))
            .sum();
        let decoded: usize = blocks
            .iter()
            .map(|entry| entry.block.decoded_bytes as usize)
            .sum();
        assert!(
            stored <= MAX_SCAN_REFILL_BYTES,
            "refill {number}: {stored} stored"
        );
        assert!(
            decoded <= MAX_SCAN_DECODED_BYTES,
            "refill {number}: {decoded} decoded"
        );
        // A refill stops only at a cap: the next refill's first block
        // wouldn't have fit this one.
        if let Some((next_start, next_end)) = gets.get(number + 1) {
            let next = blocks_between(*next_start, *next_end)[0];
            assert!(
                stored + u64::from(next.block.stored_bytes) > MAX_SCAN_REFILL_BYTES
                    || decoded + next.block.decoded_bytes as usize > MAX_SCAN_DECODED_BYTES,
                "refill {number} stopped short of both caps: {stored} stored, {decoded} decoded"
            );
        }
    }
}

/// Checks that a cold page of a directory listing whose entries span several
/// blocks of one segment fetches those blocks in one GET, and that the same
/// page through a fresh view sends no GET.
#[tokio::test]
async fn a_cold_listing_page_fetches_its_blocks_in_one_get() {
    use loonfs_types::format::manifest::lookup_keys;

    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let prefix = lookup_keys::direntry_parent_prefix(loonfs_types::ROOT_INODE_ID);
    let page_rows = 1_000;
    let page_blocks = index_blocks_for_key_range(
        &index,
        "",
        Some(&expected[page_rows].row_key_for_family(FAMILY)),
    );
    assert!(page_blocks.len() > 4, "the page spans {page_blocks:?}");

    for through_fresh_view in [false, true] {
        let view = scan::VerifiedMetadataSegments::from_runs(
            &store,
            &cache,
            one_segment_runs(descriptor.clone()),
        );
        store.reset();
        // A listing page's scan, without the parent's filter probe, which the
        // fixture's filter doesn't hold.
        let page = view
            .scan_range(
                FAMILY,
                &prefix,
                string_prefix_upper_bound(&prefix).as_deref(),
                page_rows,
                None,
                scan::BlocksPerRefill::ReadAhead,
            )
            .await
            .expect("list the directory");
        assert_eq!(page.len(), page_rows);
        if through_fresh_view {
            assert_eq!(
                store.take_gets(),
                Vec::new(),
                "the page's blocks are cached"
            );
            continue;
        }
        let gets = data_gets(&store, &descriptor);
        assert_eq!(gets.len(), 1, "one GET for the page's blocks: {gets:?}");
        let (start, end) = gets[0];
        assert_eq!(start, index[0].block.offset);
        assert!(end >= stored_range(&index, page_blocks.clone()).1);
        assert!(end - start <= crate::limits::MAX_SCAN_REFILL_BYTES);
    }
}

/// Checks that a segment that a read fetches whole is checked against the
/// checksum that its descriptor records, by a lookup's index load and by a
/// compaction's, and that a wrong recorded checksum is corruption.
#[tokio::test]
async fn a_segment_read_whole_is_checked_against_its_descriptors_checksum() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let (descriptor, _) = segment_with_block_target(&store, &rows(0, 2, 1), 64 * 1024).await;
    assert!(
        index_block_load::segment_object_bytes(&descriptor) <= 256 * 1024,
        "the fixture segment is small enough to fetch whole"
    );
    index_block_load::load_segment_index_for_compaction(&store, &descriptor)
        .await
        .expect("a segment that matches its checksum loads");
    let mut wrong = descriptor;
    wrong.checksum = loonfs_types::Checksum::crc64nvme(b"another segment");
    let object_key = metadata_segment_object_key(&wrong);
    assert_matches!(
        index_block_load::load_segment_index(&store, None, &SessionBlockMemo::default(), &wrong)
            .await,
        Err(ManifestLoadError::SegmentChecksumMismatch { object_key: key, .. })
            if key == object_key
    );
    let error = index_block_load::load_segment_index_for_compaction(&store, &wrong)
        .await
        .expect_err("a compaction's whole read checks the checksum too");
    assert_matches!(
        &error,
        ManifestLoadError::SegmentChecksumMismatch { object_key: key, .. } if *key == object_key
    );
    assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
}
