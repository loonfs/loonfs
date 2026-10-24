//! Read-ahead windows: the GETs that a scan over many small blocks sends, a
//! lookup's window when the read working memory is short, the blocks of a
//! window that a scan decodes, and the stored bytes that a visit keeps.

use super::super::block_load::load_manifest_segment_rows_in_key_range_with_cache;
use super::super::build::write_manifest_segment;
use super::super::data_block_load::read_ahead_window_len;
use super::bounded_pages::rows;
use super::*;
use crate::cache::ReadWorkingMemory;
use crate::limits::READAHEAD_WINDOW_BYTES;
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
    let descriptor = write_manifest_segment(
        store,
        &NamespaceId::parse("read-ahead").expect("namespace"),
        FAMILY,
        builder.build().expect("finish segment"),
    )
    .await
    .expect("write fixture segment");
    let index = index_block_load::load_segment_index(
        store,
        None,
        &load::SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("index");
    (descriptor, index)
}

fn one_segment_runs(descriptor: MetadataSegmentRef) -> Vec<MetadataRunManifest> {
    vec![MetadataRunManifest {
        run_no: RunNo(1),
        run_seq: ChangeSeq(1),
        tier: RunTier::Delta,
        segments: vec![MetadataFamilySegments {
            family: FAMILY,
            segments: vec![descriptor],
        }],
    }]
}

/// Returns the number of windows that a read fetches from the first block of
/// `index` to the last one when nothing has the blocks yet.
fn windows_over(index: &[SegmentIndexEntry], window_bytes: usize) -> usize {
    let mut windows = 0;
    let mut start = 0;
    while start < index.len() {
        start += read_ahead_window_len(&index[start..], window_bytes);
        windows += 1;
    }
    windows
}

/// Checks that a cold scan over a segment of many small blocks sends one
/// data GET for each 1 MiB of stored blocks, and that each GET reads one
/// window. The blocks are stored in about 4 KiB each, so a window covers
/// about 230 of them.
#[tokio::test]
async fn a_scan_over_small_blocks_sends_one_get_for_each_window_of_stored_bytes() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 16_384, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let stored_bytes: u64 = index
        .iter()
        .map(|entry| u64::from(entry.block.stored_bytes))
        .sum();
    let mean_block_bytes = stored_bytes / index.len() as u64;
    assert!(
        (3 * 1024..6 * 1024).contains(&mean_block_bytes),
        "the fixture's blocks are small: {mean_block_bytes} bytes"
    );
    let filter_offset = descriptor.filter_block.offset;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let view =
        scan::VerifiedMetadataSegments::from_runs(&store, &cache, one_segment_runs(descriptor));
    store.reset();

    let page = view
        .scan_range_page(FAMILY, "", None, expected.len())
        .await
        .expect("scan the segment");

    assert_eq!(page, expected);
    let data_gets: Vec<u64> = store
        .take_gets()
        .into_iter()
        .filter_map(|(_, range)| range)
        .filter(|(start, _)| *start < filter_offset)
        .map(|(start, end)| end - start)
        .collect();
    let windows = windows_over(&index, READAHEAD_WINDOW_BYTES);
    assert_eq!(data_gets.len(), windows, "{} blocks", index.len());
    let full_windows =
        loonfs_types::usize_from_u64(stored_bytes.div_ceil(READAHEAD_WINDOW_BYTES as u64));
    assert!(
        (full_windows..=full_windows + 1).contains(&windows),
        "about {stored_bytes} / 1 MiB windows, not {windows}"
    );
    assert!(data_gets
        .iter()
        .all(|bytes| *bytes <= READAHEAD_WINDOW_BYTES as u64));
}

/// Checks that a lookup's window shrinks to the read working memory that's
/// free, down to the missed block alone, and that every lookup still returns
/// its block. The memo is the only place that keeps a lookup's window, so a
/// window larger than the free memory would be fetched only to be evicted.
#[tokio::test]
async fn a_lookup_window_shrinks_to_the_free_read_working_memory() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let (descriptor, index) =
        segment_with_block_target(&store, &rows(0, 4_096, 1), 12 * 1024).await;
    let mut expected = Vec::new();
    for entry in index.iter() {
        expected.push(
            data_block_load::load_segment_data_block_with_readahead(
                &store,
                None,
                None,
                &descriptor,
                std::slice::from_ref(entry),
            )
            .await
            .expect("a block read alone"),
        );
    }
    // Each case is the pool's limit and the bytes that another read already
    // holds in it, so the free memory is their difference.
    for (limit, held) in [
        (usize::MAX, 0),
        (256 * 1024, 0),
        (256 * 1024, 200 * 1024),
        (64 * 1024, 64 * 1024),
        (0, 0),
    ] {
        let free_bytes = limit - held;
        let pool = Arc::new(ReadWorkingMemory::new(limit, None));
        assert!(pool.try_reserve(held));
        let memo = load::SessionBlockMemo::new(Arc::clone(&pool));
        store.reset();
        let block = data_block_load::load_segment_data_block_with_readahead(
            &store,
            None,
            Some(&memo),
            &descriptor,
            &index,
        )
        .await
        .expect("a lookup with little free memory reads its block");
        assert_eq!(block, expected[0]);
        let window = read_ahead_window_len(&index, READAHEAD_WINDOW_BYTES.min(free_bytes));
        let last = index[window - 1].block;
        assert_eq!(
            store.take_gets(),
            vec![(
                metadata_segment_object_key(&descriptor),
                Some((
                    index[0].block.offset,
                    last.offset + u64::from(last.stored_bytes)
                ))
            )],
            "one GET reads the window that fits {free_bytes} free bytes"
        );
        // The lookup's later blocks come from the window that the memo kept,
        // or from GETs of their own, and none of the reads fails.
        for position in 1..index.len() {
            let block = data_block_load::load_segment_data_block_with_readahead(
                &store,
                None,
                Some(&memo),
                &descriptor,
                &index[position..],
            )
            .await
            .expect("a later lookup with little free memory");
            assert_eq!(block, expected[position]);
        }
        drop(memo);
        pool.release(held);
        assert_eq!(pool.in_use(), 0);
    }
}

/// Checks that a bounded load returns the blocks of its key range alone,
/// while its window reads on past the range's last block to the segment's
/// last block, so that the read's later lookups and scans of nearby keys find
/// their blocks in the memo.
#[tokio::test]
async fn a_bounded_load_returns_its_key_range_and_its_window_stops_at_the_segment_end() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 1_536, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    assert_eq!(
        read_ahead_window_len(&index[10..], READAHEAD_WINDOW_BYTES),
        index.len() - 10,
        "one window from block 10 reaches the segment's last block"
    );
    let lower = format!("{}\0", index[9].last_row_key);
    let upper = index[12].last_row_key.clone();
    let filter_offset = descriptor.filter_block.offset;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    store.reset();

    let loaded = load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        Some(&cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        ChangeSeq(1),
        &lower,
        Some(&upper),
        usize::MAX,
        scan::Readahead::Enabled,
    )
    .await
    .expect("bounded load");

    let keys: Vec<_> = expected
        .iter()
        .map(|row| row.row_key_for_family(FAMILY))
        .filter(|key| key > &index[9].last_row_key && key <= &index[12].last_row_key)
        .collect();
    assert_eq!(
        loaded.row_keys().cloned().collect::<Vec<_>>(),
        keys,
        "the load returns blocks 10 through 12, the blocks of its key range"
    );
    let last = index[index.len() - 1].block;
    let data_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter_map(|(_, range)| range)
        .filter(|(start, _)| *start < filter_offset)
        .collect();
    assert_eq!(
        data_gets,
        vec![(
            index[10].block.offset,
            last.offset + u64::from(last.stored_bytes)
        )],
        "one GET reads from block 10 to the segment's last block"
    );
}

/// Checks that a cold scan decodes only the blocks that it reads, and keeps
/// the rest of its window as stored bytes in the read's memo. A scan that
/// decoded its whole window would decode about 230 blocks of this fixture for
/// a page of one row, and every scan in flight would hold that many.
#[tokio::test]
async fn a_scan_decodes_only_the_blocks_it_reads() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 16_384, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let view =
        scan::VerifiedMetadataSegments::from_runs(&store, &cache, one_segment_runs(descriptor));
    store.reset();

    let first = view
        .scan_range_page_with_keys(FAMILY, "", None, 1)
        .await
        .expect("first page");
    assert_eq!(first.len(), 1);
    assert_eq!(
        (cache.stats().inserts, store.count(OperationSelector::Read)),
        (3, 2),
        "the page reads the index and one window, and decodes the index, the filter that \
         the index's GET reads, and one data block"
    );
    let window = read_ahead_window_len(&index, READAHEAD_WINDOW_BYTES);
    let rows_in_window: usize = expected
        .iter()
        .take_while(|row| row.row_key_for_family(FAMILY) <= index[window - 1].last_row_key)
        .count();
    let lower = format!("{}\0", first[0].0);
    let rest = view
        .scan_range_page_with_keys(FAMILY, &lower, None, rows_in_window - 1)
        .await
        .expect("the rest of the window");
    assert_eq!(rest.len(), rows_in_window - 1);
    assert_eq!(
        (cache.stats().inserts, store.count(OperationSelector::Read)),
        (window + 2, 2),
        "the read's next page decodes each block of the window when it reads it"
    );
}

/// Checks that a visit of a range fetches each window once, keeps only the
/// stored bytes of its window while the caller visits rows, and doesn't read
/// past its range. The visit drops its decoded blocks before each visit of
/// rows, so the caller's own sets can use the rest of the read working
/// memory.
#[tokio::test]
async fn a_visit_keeps_only_the_stored_bytes_of_its_window_while_it_visits_rows() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 16_384, 1);
    let (descriptor, index) = segment_with_block_target(&store, &expected, 12 * 1024).await;
    let upper = index[index.len() / 2].last_row_key.clone();
    let in_range = index_blocks_for_key_range(&index, "", Some(&upper));
    let filter_offset = descriptor.filter_block.offset;
    let pool = Arc::new(ReadWorkingMemory::default());
    let cache = MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(usize::MAX, usize::MAX)),
        ObjectStoreScope::new(0),
        Arc::clone(&pool),
        READAHEAD_WINDOW_BYTES,
    );
    store.reset();

    let mut visited = Vec::new();
    let mut peak_visit_bytes = 0;
    let flow = super::super::visit_segment_rows(
        &store,
        Some(&cache),
        &[super::super::ScanDescriptor {
            descriptor: &descriptor,
            max_seq: ChangeSeq(1),
        }],
        "",
        Some(&upper),
        |row| {
            peak_visit_bytes = peak_visit_bytes.max(pool.in_use());
            visited.push(row);
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )
    .await
    .expect("visit");

    assert!(flow.is_continue());
    let wanted: Vec<_> = expected
        .iter()
        .filter(|row| row.row_key_for_family(FAMILY) < upper)
        .cloned()
        .collect();
    assert_eq!(visited, wanted);
    let data_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter_map(|(_, range)| range)
        .filter(|(start, _)| *start < filter_offset)
        .collect();
    assert_eq!(
        data_gets.len(),
        windows_over(&index[..in_range.end], READAHEAD_WINDOW_BYTES),
        "one GET for each window of the range"
    );
    let range_end = index[in_range.end - 1].block;
    assert!(data_gets
        .iter()
        .all(|(_, end)| *end <= range_end.offset + u64::from(range_end.stored_bytes)));
    assert!(
        peak_visit_bytes <= READAHEAD_WINDOW_BYTES + 64 * 1024,
        "the memo keeps one window of stored bytes, not decoded blocks: {peak_visit_bytes}"
    );
    assert_eq!(pool.in_use(), 0);
}
