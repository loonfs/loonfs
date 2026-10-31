//! Bounded loading must return the same raw-row prefix as an unbounded scan.

use super::*;
use crate::manifest::tests::inspection_materialization::load_manifest_segment_rows_in_key_range_with_cache;
use crate::segment::block_memo::SessionBlockMemo;
use crate::segment::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey};
use crate::segment::segment_block_cache::{
    MemorySegmentBlocks, SegmentBlockCache, SegmentBlockLoad,
};
use loonfs_test_support::stores::{replace_object, ConcurrencyWatchStore};
use loonfs_types::ROOT_INODE_ID;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const FAMILY: ApiMetadataRowFamily = ApiMetadataRowFamily::DirentryBinds;

pub(super) fn rows(start: usize, count: usize, sequence: u64) -> Vec<MetadataRow> {
    (start..start + count)
        .map(|index| {
            // Long, valid names ensure this fixture exceeds the eager-object
            // threshold even though each row has its own tiny data block.
            let mut state = index as u64 + 1;
            let suffix: String = (0..200)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    char::from(b'a' + (state % 26) as u8)
                })
                .collect();
            let name = format!("file-{index:06}-{suffix}");
            MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
                parent_inode_id: ROOT_INODE_ID,
                name_key: NameKey::parse(&name).expect("name"),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                    display_name: loonfs_types::DisplayName::parse(&name).expect("display name"),
                },
                child_inode_id: InodeId(u128::from(index as u64 + 2)),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 4_200,
                committed_seq: ChangeSeq(sequence),
                delta_index: 0,
            })
        })
        .collect()
}

async fn segment(store: &impl ObjectStore, rows: &[MetadataRow]) -> MetadataSegmentRef {
    let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
    for row in rows {
        builder
            .push(
                &row.row_key_for_family(FAMILY),
                &row.filter_key_for_family(FAMILY),
                row,
            )
            .expect("encode fixture row");
    }
    let built = builder.build().expect("finish segment");
    assert!(
        built.bytes.len() > 288 * 1024,
        "exercise ranged, not eager reads"
    );
    crate::test_support::namespace_store(
        store,
        &NamespaceId::parse("bounded-pages").expect("namespace"),
    )
    .write_manifest_segment(FAMILY, built)
    .await
    .expect("write fixture segment")
}

/// A view whose reads fetch `window_bytes` ahead of a data block they miss,
/// so that a test's windows can be smaller than its segment.
fn cache_with_window(max_decoded_bytes: usize, window_bytes: usize) -> MetadataSegmentCache {
    MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(
            max_decoded_bytes,
            max_decoded_bytes,
        )),
        ObjectStoreScope::new(0),
        crate::segment::cache::unshared_read_working_memory(),
        window_bytes,
    )
}

/// The stored bytes of `blocks` data blocks of `descriptor` from `start`. A
/// read-ahead window of that size from `start` covers exactly those blocks.
async fn window_of_blocks(
    store: &impl ObjectStore,
    descriptor: &MetadataSegmentRef,
    start: usize,
    blocks: usize,
) -> usize {
    let index =
        index_block_load::load_segment_index(store, None, &SessionBlockMemo::default(), descriptor)
            .await
            .expect("index");
    let window_bytes = index[start + blocks].block.offset - index[start].block.offset;
    usize::try_from(window_bytes).expect("a test window fits usize")
}

#[tokio::test]
async fn bounded_pages_match_full_rows_across_windows_ranges_and_eviction() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    // A window of 32 blocks from the first one, and of about 32 from the
    // others (the fixture's blocks are about the same size), so the ranges
    // and limits below start and end on both sides of a window's end. One
    // window of the default size would cover the whole segment.
    let window_bytes = window_of_blocks(&store, &descriptor, 0, 32).await;
    // A one-byte budget keeps no block, and a 64 KiB budget keeps a few data
    // blocks and evicts them during the wide ranges.
    const EVICTING_BUDGET: usize = 64 * 1024;
    for budget in [1, EVICTING_BUDGET, 8 * 1024 * 1024] {
        let cache = cache_with_window(budget, window_bytes);
        for (start, end, limit) in [
            (0, 1024, 0),
            (0, 1024, 1),
            (0, 1024, 32),
            (0, 1024, 33),
            (31, 1024, 65),
            (32, 97, 64),
            (33, 97, 65),
            (1000, 1024, 100),
            (10, 10, 4),
            (0, 1024, usize::MAX),
        ] {
            let lower = expected[start].row_key_for_family(FAMILY);
            let upper = expected.get(end).map(|row| row.row_key_for_family(FAMILY));
            for readahead in [scan::Readahead::Enabled, scan::Readahead::Disabled] {
                let loaded = load_manifest_segment_rows_in_key_range_with_cache(
                    &store,
                    Some(&cache),
                    &SessionBlockMemo::default(),
                    &descriptor,
                    ChangeSeq(1),
                    &lower,
                    upper.as_deref(),
                    limit,
                    readahead,
                )
                .await
                .expect("bounded load");
                let actual: Vec<_> = loaded
                    .rows_in_key_range(&lower, upper.as_deref())
                    .take(limit)
                    .map(|(_, row)| row.clone())
                    .collect();
                let wanted: Vec<_> = expected[start..end].iter().take(limit).cloned().collect();
                assert_eq!(
                    actual, wanted,
                    "range {start}..{end}, limit {limit}, budget {budget}"
                );
            }
        }
        match budget {
            1 => assert_eq!(cache.stats().inserts, 0, "a block is heavier than one byte"),
            EVICTING_BUDGET => {
                assert!(cache.stats().evictions > 0, "exercise real cache eviction");
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn bounded_pages_merge_overlapping_runs_and_binding_versions() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let first = rows(0, 1024, 1);
    let second = rows(512, 1024, 2);
    let a = segment(&store, &first).await;
    let b = segment(&store, &second).await;
    let mut expected = first;
    expected.extend(second);
    expected.sort_by_key(|row| row.row_key_for_family(FAMILY));
    let runs: Vec<_> = [a, b]
        .into_iter()
        .enumerate()
        .map(|(index, descriptor)| MetadataRunManifest {
            run_no: RunNo(index as u64 + 1),
            run_seq: ChangeSeq(index as u64 + 1),
            tier: RunTier::Delta,
            segments: vec![MetadataFamilySegments {
                family: FAMILY,
                segments: vec![descriptor],
            }],
        })
        .collect();
    let cache = MetadataSegmentCache::unshared(1);
    // Single-row pages walk a window across the start of the overlap; a full
    // single-row traversal re-reads both indexes for every row.
    for (limit, window) in [
        (1, 480..544),
        (31, 0..expected.len()),
        (33, 0..expected.len()),
        (65, 0..expected.len()),
        (1000, 0..expected.len()),
    ] {
        let mut actual = Vec::new();
        let mut lower = expected[window.start].row_key_for_family(FAMILY);
        while actual.len() < window.len() || window.end == expected.len() {
            let view = scan::VerifiedMetadataSegments::from_runs(&store, &cache, runs.clone());
            let page = view
                .scan_range_page_with_keys(FAMILY, &lower, None, limit)
                .await
                .expect("merged page");
            let peak = view.peak_page_rows();
            assert_eq!(peak, page.len());
            assert!(peak <= limit, "owned scan rows must fit one page");
            if page.is_empty() {
                break;
            }
            lower = format!("{}\0", page.last().expect("nonempty").0);
            actual.extend(page.into_iter().map(|(_, row)| row));
        }
        assert_eq!(
            actual,
            expected[window.clone()],
            "merged traversal with page limit {limit}"
        );
    }
}

#[tokio::test]
async fn bounded_pages_open_and_refill_segments_in_a_concurrent_window() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let mut runs = Vec::new();
    let mut expected = Vec::new();
    for sequence in 1..=20 {
        let segment_rows = rows(0, 1024, sequence);
        let descriptor = segment(&store, &segment_rows).await;
        expected.extend(segment_rows);
        runs.push(MetadataRunManifest {
            run_no: RunNo(sequence),
            run_seq: ChangeSeq(sequence),
            tier: RunTier::Delta,
            segments: vec![MetadataFamilySegments {
                family: FAMILY,
                segments: vec![descriptor],
            }],
        });
    }
    let limit = 20 * 33;
    expected.sort_by_key(|row| row.row_key_for_family(FAMILY));

    for warm_first_window in [false, true] {
        let lower = if warm_first_window {
            rows(512, 1, 1)[0].row_key_for_family(FAMILY)
        } else {
            String::new()
        };
        let cache = MetadataSegmentCache::unshared(64 * 1024 * 1024);
        if warm_first_window {
            for run in &runs {
                load_manifest_segment_rows_in_key_range_with_cache(
                    &store,
                    Some(&cache),
                    &SessionBlockMemo::default(),
                    &run.segments[0].segments[0],
                    run.run_seq,
                    &lower,
                    None,
                    1,
                    scan::Readahead::Enabled,
                )
                .await
                .expect("warm the first read-ahead window");
            }
        }
        let watched = ConcurrencyWatchStore::new(
            BlockingStore::new(
                LocalFsStore::new(temp.path()).expect("store"),
                KeyPredicate::any(),
                OperationSelector::Read,
            ),
            KeyPredicate::any(),
        );
        watched.inner().arm();
        let view = scan::VerifiedMetadataSegments::from_runs(&watched, &cache, runs.clone());
        let mut page = std::pin::pin!(view.scan_range_page(FAMILY, &lower, None, limit));
        // Yield between polls so Tokio can replenish the cooperative budget
        // after cache hits, before the next store request starts.
        for _ in 0..64 {
            assert!(futures::poll!(page.as_mut()).is_pending());
            if watched.reads().total == 16 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(futures::poll!(page.as_mut()).is_pending());
        let waiting = watched.reads();
        assert_eq!(waiting.total, 16, "warm first window: {warm_first_window}");
        assert_eq!(waiting.peak_in_flight, 16);
        watched.inner().release();
        let wanted: Vec<_> = expected
            .iter()
            .filter(|row| row.row_key_for_family(FAMILY) >= lower)
            .take(limit)
            .cloned()
            .collect();
        assert_eq!(page.await.expect("merged page"), wanted);
        assert_eq!(view.peak_page_rows(), limit);
        assert_eq!(watched.reads().peak_in_flight, 16);
    }
}

#[tokio::test]
async fn disabled_readahead_loads_a_cold_key_range_in_one_data_get() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected: Vec<_> = (1..=1024)
        .flat_map(|sequence| rows(1, 1, sequence))
        .collect();
    let mut segment_rows = rows(0, 1, 1);
    segment_rows.extend(expected.clone());
    segment_rows.extend(rows(2, 2, 1));
    let descriptor = segment(&store, &segment_rows).await;
    let index = index_block_load::load_segment_index(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("index");
    let first = index[1].block;
    let last = index[1025].block;
    let data_range = (first.offset, last.offset + u64::from(last.stored_bytes));
    let filter_offset = descriptor.filter_block.offset;
    let object_key = metadata_segment_object_key(&descriptor);
    let runs = vec![MetadataRunManifest {
        run_no: RunNo(1),
        run_seq: ChangeSeq(1024),
        tier: RunTier::Delta,
        segments: vec![MetadataFamilySegments {
            family: FAMILY,
            segments: vec![descriptor],
        }],
    }];
    let cache = MetadataSegmentCache::unshared(1);
    let view = scan::VerifiedMetadataSegments::from_runs(&store, &cache, runs);
    let filter_probe = expected[0].filter_key_for_family(FAMILY);
    let prefix = format!("{filter_probe}-");
    store.reset();
    let actual = view
        .scan_prefix_for_lookup(FAMILY, &prefix, &filter_probe, scan::Readahead::Disabled)
        .await
        .expect("prefix scan");
    assert_eq!(actual, expected);
    let data_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter(|(_, range)| range.is_none_or(|(start, _)| start < filter_offset))
        .collect();
    assert_eq!(data_gets, vec![(object_key, Some(data_range))]);
}

/// Checks that a page decodes only the block it reads, that a later page of
/// the same read finds its block in the window that the read's memo keeps,
/// and that a page of another read fetches a window of its own. A window's
/// other blocks are kept as stored bytes in the memo of the read that fetched
/// them, and not in the block cache.
#[tokio::test]
async fn bounded_pages_keep_their_window_in_the_memo_of_their_read() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let memo = SessionBlockMemo::default();
    let page = |position: usize, memo| {
        let lower = expected[position].row_key_for_family(FAMILY);
        let (store, cache, descriptor) = (&store, &cache, &descriptor);
        async move {
            load_manifest_segment_rows_in_key_range_with_cache(
                store,
                Some(cache),
                memo,
                descriptor,
                ChangeSeq(1),
                &lower,
                None,
                1,
                scan::Readahead::Enabled,
            )
            .await
            .expect("page")
            .rows()
            .count()
        }
    };

    assert_eq!(page(0, &memo).await, 1, "the page reads one block");
    assert_eq!(
        cache.stats().inserts,
        3,
        "the page decodes the index, the filter that the index's GET reads, and the one \
         data block it reads"
    );
    store.reset();
    page(1, &memo).await;
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "the read's next page finds its block in the memo"
    );
    page(2, &SessionBlockMemo::default()).await;
    assert_eq!(
        store.count(OperationSelector::Read),
        1,
        "a page of another read fetches a window of its own"
    );
}

/// A memory block cache that records each data block that a read asks it
/// for, and counts each data block that LoonFS decodes into it: the block
/// that a miss loads, and each block inserted without a read asking for it.
struct DecodeCountingBlocks {
    memory: MemorySegmentBlocks,
    reads: Mutex<BTreeSet<u64>>,
    decodes: AtomicUsize,
}

#[async_trait]
impl SegmentBlockCache for DecodeCountingBlocks {
    async fn get_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
    ) -> Option<DecodedMetadataSegmentBlock> {
        self.memory.get_block(key, handle).await
    }

    fn insert_block(
        &self,
        key: MetadataSegmentCacheKey,
        block: DecodedMetadataSegmentBlock,
        stored: &[u8],
    ) {
        if key.block_kind == SegmentBlockKind::Data {
            self.decodes.fetch_add(1, Ordering::SeqCst);
        }
        self.memory.insert_block(key, block, stored);
    }

    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        _handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
        let data = key.block_kind == SegmentBlockKind::Data;
        if data {
            self.reads
                .lock()
                .expect("reads lock")
                .insert(key.block_offset);
        }
        self.memory
            .get_or_load_with_hit(
                key,
                |_| {},
                move || async move {
                    let loaded = load.await?;
                    if data {
                        self.decodes.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(loaded.block)
                },
            )
            .await
    }
}

/// Checks that a lookup's scan over several read-ahead windows of data
/// blocks decodes each block it reads once, with one ranged GET per window,
/// when the block cache keeps fewer blocks than one window. The scan reads
/// one block per refill, so a window that the scan decoded whole and the
/// cache then evicted would be fetched and decoded again from each later
/// block of the window.
#[tokio::test]
async fn a_lookup_scan_decodes_each_block_it_reads_once_through_a_small_block_cache() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected: Vec<_> = (1..=96).flat_map(|sequence| rows(1, 1, sequence)).collect();
    let mut segment_rows = rows(0, 1, 1);
    segment_rows.extend(expected.clone());
    segment_rows.extend(rows(2, 1024, 1));
    let descriptor = segment(&store, &segment_rows).await;
    let index = index_block_load::load_segment_index(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("index");
    // A budget of four blocks, so the cache keeps fewer blocks than a window,
    // and windows of 32 blocks, so the scan reads several of them. One
    // window of the default size would cover the whole segment.
    let probe = MetadataSegmentCache::unshared(usize::MAX);
    data_block_load::load_segment_data_block_with_readahead(
        &store,
        Some(&probe),
        None,
        &descriptor,
        std::slice::from_ref(&index[1]),
    )
    .await
    .expect("one block");
    let shared = Arc::new(SharedSegmentBlocks::new(
        4 * probe.stats().cached_decoded_bytes,
        usize::MAX,
    ));
    let blocks = Arc::new(DecodeCountingBlocks {
        memory: MemorySegmentBlocks::new(Arc::clone(&shared), ObjectStoreScope::new(0)),
        reads: Mutex::default(),
        decodes: AtomicUsize::new(0),
    });
    let cache = MetadataSegmentCache::with_blocks(
        blocks.clone(),
        shared,
        ObjectStoreScope::new(0),
        crate::segment::cache::unshared_read_working_memory(),
        window_of_blocks(&store, &descriptor, 1, 32).await,
    );
    let runs = vec![MetadataRunManifest {
        run_no: RunNo(1),
        run_seq: ChangeSeq(96),
        tier: RunTier::Delta,
        segments: vec![MetadataFamilySegments {
            family: FAMILY,
            segments: vec![descriptor],
        }],
    }];
    let view = scan::VerifiedMetadataSegments::from_runs(&store, &cache, runs);
    let filter_probe = expected[0].filter_key_for_family(FAMILY);
    store.reset();

    let actual = view
        .scan_prefix_for_lookup(
            FAMILY,
            &format!("{filter_probe}-"),
            &filter_probe,
            scan::Readahead::Enabled,
        )
        .await
        .expect("prefix scan");
    assert_eq!(actual, expected);
    let reads = blocks.reads.lock().expect("reads lock").len();
    let gets = store.count(OperationSelector::Read);
    assert!(
        reads > 64,
        "the scan reads the blocks of several windows, not {reads}"
    );
    assert_eq!(
        blocks.decodes.load(Ordering::SeqCst),
        reads,
        "decodes of the {reads} blocks that the scan read, with {gets} GETs"
    );
    assert!(
        gets <= 6,
        "one GET for each window and the index, not {gets}"
    );
}

#[tokio::test]
async fn bounded_pages_reject_corruption_when_the_damaged_block_is_requested() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    let index = index_block_load::load_segment_index(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("index");
    let key = metadata_segment_object_key(&descriptor);
    let mut bytes = store
        .get(&key, None)
        .await
        .expect("get object")
        .expect("object exists")
        .to_vec();
    bytes[loonfs_types::usize_from_u64(index[64].block.offset)] ^= 1;
    replace_object(&store, &key, Bytes::from(bytes))
        .await
        .expect("corrupt fixture");
    // The first page's window covers the damaged block 64 as stored bytes, and
    // the page doesn't reach that block, so the page doesn't decode it.
    load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
        ChangeSeq(1),
        "",
        None,
        1,
        scan::Readahead::Enabled,
    )
    .await
    .expect("unaffected first page");
    let lower = expected[64].row_key_for_family(FAMILY);
    assert!(
        load_manifest_segment_rows_in_key_range_with_cache(
            &store,
            None,
            &SessionBlockMemo::default(),
            &descriptor,
            ChangeSeq(1),
            &lower,
            None,
            1,
            scan::Readahead::Enabled,
        )
        .await
        .is_err(),
        "requested corrupt data must fail validation"
    );
}

#[tokio::test]
async fn lookup_readahead_retains_bytes_and_checks_rows_only_when_requested() {
    for count in [16, 1024] {
        let temp = tempdir().expect("tempdir");
        let store =
            RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
        let expected = rows(0, count, 1);
        let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
        for row in &expected {
            builder
                .push(
                    &row.row_key_for_family(FAMILY),
                    &row.filter_key_for_family(FAMILY),
                    row,
                )
                .expect("encode row");
        }
        let built = builder.build().expect("segment");
        let descriptor = crate::test_support::namespace_store(
            &store,
            &NamespaceId::parse("lookup-readahead").expect("namespace"),
        )
        .write_manifest_segment(FAMILY, built)
        .await
        .expect("write segment");
        assert_eq!(
            index_block_load::segment_object_bytes(&descriptor) <= 256 * 1024,
            count == 16
        );
        let index = index_block_load::load_segment_index(
            &store,
            None,
            &SessionBlockMemo::default(),
            &descriptor,
        )
        .await
        .expect("index");
        let key = metadata_segment_object_key(&descriptor);
        let mut bytes = store
            .get(&key, None)
            .await
            .expect("read segment")
            .expect("segment")
            .to_vec();
        bytes[loonfs_types::usize_from_u64(index[2].block.offset)] ^= 1;
        replace_object(&store, &key, Bytes::from(bytes))
            .await
            .expect("corrupt unrequested block");
        let memo = SessionBlockMemo::default();
        store.reset();
        for position in [0, 1, 2] {
            let lower = expected[position].row_key_for_family(FAMILY);
            let loaded = load_manifest_segment_rows_in_key_range_with_cache(
                &store,
                None,
                &memo,
                &descriptor,
                ChangeSeq(1),
                &lower,
                loonfs_types::format::segment_blocks::string_prefix_upper_bound(&lower).as_deref(),
                1,
                scan::Readahead::Enabled,
            )
            .await;
            if position == 2 {
                assert!(matches!(
                    loaded,
                    Err(ManifestLoadError::SegmentCodec { .. })
                ));
            } else {
                let loaded = loaded.expect("intact requested block");
                assert_eq!(loaded.rows().collect::<Vec<_>>(), vec![&expected[position]]);
            }
            assert_eq!(
                store.count(OperationSelector::Read),
                if count == 16 { 1 } else { 2 }
            );
        }
    }
}

#[tokio::test]
async fn read_working_memory_counts_large_indexes_and_filters() {
    use crate::cache::ReadWorkingMemory;
    use crate::heap_bytes::index_block_heap_bytes;

    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let mut expected = rows(0, 2048, 1);
    for (index, row) in expected.iter_mut().enumerate() {
        let MetadataRow::DirentryBinding(binding) = row else {
            panic!("binding row")
        };
        let name = format!("file-{index:06}-{}", "x".repeat(200));
        binding.name_key = NameKey::parse(&name).expect("name");
        binding.state = loonfs_types::format::manifest::DirentryBindingState::Bound {
            display_name: loonfs_types::DisplayName::parse(&name).expect("display name"),
        };
    }
    let mut descriptor = segment(&store, &expected).await;
    descriptor.filter_inline = None;
    let pool = Arc::new(ReadWorkingMemory::new(64 * 1024 * 1024, None));
    let memo = SessionBlockMemo::new(Arc::clone(&pool));
    store.reset();
    let index = index_block_load::load_segment_index(&store, None, &memo, &descriptor)
        .await
        .expect("index");
    let index_bytes = index_block_heap_bytes(&index);
    let stored_data_bytes: usize = index
        .iter()
        .map(|entry| entry.block.stored_bytes as usize)
        .sum();
    assert!(
        index_bytes > stored_data_bytes,
        "index {index_bytes}, stored data {stored_data_bytes}"
    );
    let index_reservation = pool.in_use();
    assert!(
        index_reservation > index_bytes,
        "the index and memo containers are charged"
    );
    assert_eq!(store.count(OperationSelector::Read), 1);
    let filter = index_block_load::load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("filter");
    let filter_bytes = filter.decoded_bytes() + 2 * std::mem::size_of::<usize>();
    assert!(pool.in_use() >= index_reservation + filter_bytes);
    assert_eq!(store.count(OperationSelector::Read), 2);
    assert_eq!(
        *index_block_load::load_segment_index(&store, None, &memo, &descriptor)
            .await
            .expect("retained index"),
        *index
    );
    assert_eq!(store.count(OperationSelector::Read), 2);
    drop(memo);
    assert_eq!(pool.in_use(), 0);
    assert_eq!(index.len(), 2048);
    assert!(filter.may_contain(&expected[0].filter_key_for_family(FAMILY)));
}
