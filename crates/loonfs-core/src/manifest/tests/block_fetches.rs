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
            .find_row(FAMILY, &key, &probe)
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

/// Checks that a scan reserves each refill's decoded bytes from the read
/// working memory: with all but four blocks' worth reserved by another read,
/// every refill's GET covers at most four blocks, with no room left every
/// refill takes one block, and the scan releases its reservations when it
/// ends.
#[tokio::test]
async fn a_scan_refill_loads_no_more_than_the_read_working_memory_has_left() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 8_192, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let (first, last) = (10, 40);
    let lower = format!("{}\0", index[first - 1].last_row_key);
    let upper = index[last].last_row_key.clone();
    let decoded_bytes = |blocks: std::ops::Range<usize>| -> usize {
        index[blocks]
            .iter()
            .map(|entry| entry.block.decoded_bytes as usize)
            .sum()
    };
    let pool = Arc::new(crate::read_working_memory::ReadWorkingMemory::new(
        64 * 1024 * 1024,
        None,
    ));
    let cache = || {
        MetadataSegmentCache::new(
            Arc::new(SharedSegmentBlocks::new(usize::MAX, usize::MAX)),
            ObjectStoreScope::new(0),
            Arc::clone(&pool),
        )
    };
    let (store, descriptor, lower, upper) = (&store, &descriptor, &lower, &upper);
    let scan = |cache: MetadataSegmentCache| async move {
        let view = scan::VerifiedMetadataSegments::from_runs(
            store,
            &cache,
            one_segment_runs(descriptor.clone()),
        );
        view.scan_range(
            FAMILY,
            lower,
            Some(upper),
            usize::MAX,
            None,
            scan::BlocksPerRefill::KeyRange,
        )
        .await
    };
    // The test's reservation stands in for another scan that keeps its
    // blocks; four of the range's blocks, which all decode to about the same
    // size, fit in the rest.
    let room = decoded_bytes(first..first + 4);
    let held = pool.reserve(pool.limit() - room, 0);
    let wanted: Vec<_> = expected
        .iter()
        .filter(|row| {
            let key = row.row_key_for_family(FAMILY);
            key >= *lower && key < *upper
        })
        .cloned()
        .collect();
    store.reset();
    let page: Vec<_> = scan(cache())
        .await
        .expect("scan the key range")
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    assert_eq!(page, wanted);
    assert_eq!(pool.in_use(), held.bytes(), "the scan released its blocks");

    let mut gets = data_gets(store, descriptor);
    gets.sort_unstable();
    let (range_start, range_end) = stored_range(&index, first..last + 1);
    assert_eq!(gets.first().map(|get| get.0), Some(range_start));
    assert_eq!(gets.last().map(|get| get.1), Some(range_end));
    assert!(
        gets.windows(2).all(|pair| pair[0].1 == pair[1].0),
        "the GETs cover blocks {first}..={last} once: {gets:?}"
    );
    for (start, end) in &gets {
        let blocks = index
            .iter()
            .filter(|entry| entry.block.offset >= *start && entry.block.offset < *end)
            .count();
        assert!(
            (1..=4).contains(&blocks),
            "a refill covers {blocks} blocks, more than the four that fit: {gets:?}"
        );
    }

    // With less than one block free, each refill still takes its first
    // block, above the limit, and the scan reads the range one block at a
    // time.
    drop(held);
    let held = pool.reserve(pool.limit(), pool.limit());
    store.reset();
    let page: Vec<_> = scan(cache())
        .await
        .expect("scan the key range over the limit")
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    assert_eq!(page, wanted);
    assert_eq!(pool.in_use(), held.bytes(), "the scan released its blocks");
    let gets = data_gets(store, descriptor);
    assert_eq!(
        gets.len(),
        last + 1 - first,
        "one GET for each block: {gets:?}"
    );
    for (start, end) in gets {
        let blocks = index
            .iter()
            .filter(|entry| entry.block.offset >= start && entry.block.offset < end)
            .count();
        assert_eq!(blocks, 1, "a refill over the limit takes one block");
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
                scan::BlocksPerRefill::KeyRange,
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
