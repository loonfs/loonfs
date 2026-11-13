//! Bounded loading must return the same raw-row prefix as an unbounded scan.

use super::*;
use crate::manifest::tests::inspection_state::load_manifest_segment_rows_in_key_range_with_cache;
use crate::segment::block_memo::SessionBlockMemo;
use crate::segment::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey};
use crate::segment::segment_block_cache::{
    MemorySegmentBlocks, SegmentBlockCache, SegmentBlockLoad,
};
use loonfs_test_support::stores::{replace_object, ConcurrencyWatchStore};
use loonfs_types::ROOT_INODE_ID;
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
                    revision: Some(crate::test_support::revision_summary()),
                },
                child_inode_id: InodeId(u128::from(index as u64 + 2)),
                child_kind: loonfs_types::InodeKind::File,
                child_created_by: loonfs_types::ActorId::loonfs(),
                child_created_at_ms: 4_200,
                committed_seq: CommitSeq(sequence),
                delta_index: 0,
                commit_id: crate::test_support::binding_commit_id(),
                committed_by: loonfs_types::ActorId::loonfs(),
                committed_at_ms: 0,
                bind_position: loonfs_types::format::manifest::DeltaPosition {
                    seq: CommitSeq(sequence),
                    delta_index: 0,
                },
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

#[tokio::test]
async fn bounded_pages_match_full_rows_across_ranges_and_eviction() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    // A one-byte budget keeps no block, and a 64 KiB budget keeps a few data
    // blocks and evicts them during the wide ranges.
    const EVICTING_BUDGET: usize = 64 * 1024;
    for budget in [1, EVICTING_BUDGET, 8 * 1024 * 1024] {
        let cache = MetadataSegmentCache::unshared(budget);
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
            for blocks_per_refill in [scan::BlocksPerRefill::One, scan::BlocksPerRefill::KeyRange] {
                let loaded = load_manifest_segment_rows_in_key_range_with_cache(
                    &store,
                    Some(&cache),
                    &SessionBlockMemo::default(),
                    &descriptor,
                    CommitSeq(1),
                    &lower,
                    upper.as_deref(),
                    limit,
                    blocks_per_refill,
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
async fn bounded_pages_combine_overlapping_runs_and_binding_versions() {
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
            run_seq: CommitSeq(index as u64 + 1),
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
                .scan_range(
                    FAMILY,
                    &lower,
                    None,
                    limit,
                    None,
                    scan::BlocksPerRefill::One,
                )
                .await
                .expect("combined page");
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
            "combined traversal with page limit {limit}"
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
            run_seq: CommitSeq(sequence),
            tier: RunTier::Delta,
            segments: vec![MetadataFamilySegments {
                family: FAMILY,
                segments: vec![descriptor],
            }],
        });
    }
    let limit = 20 * 33;
    expected.sort_by_key(|row| row.row_key_for_family(FAMILY));

    for warm_first_blocks in [false, true] {
        let lower = if warm_first_blocks {
            rows(512, 1, 1)[0].row_key_for_family(FAMILY)
        } else {
            String::new()
        };
        let cache = MetadataSegmentCache::unshared(64 * 1024 * 1024);
        if warm_first_blocks {
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
                    scan::BlocksPerRefill::One,
                )
                .await
                .expect("warm the first block");
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
        let mut page = std::pin::pin!(view.scan_range(
            FAMILY,
            &lower,
            None,
            limit,
            None,
            scan::BlocksPerRefill::One
        ));
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
        assert_eq!(waiting.total, 16, "warm first blocks: {warm_first_blocks}");
        assert_eq!(waiting.peak_in_flight, 16);
        watched.inner().release();
        let wanted: Vec<_> = expected
            .iter()
            .filter(|row| row.row_key_for_family(FAMILY) >= lower)
            .take(limit)
            .cloned()
            .collect();
        let rows: Vec<_> = page
            .await
            .expect("combined page")
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        assert_eq!(rows, wanted);
        assert_eq!(view.peak_page_rows(), limit);
        assert_eq!(watched.reads().peak_in_flight, 16);
    }
}

#[tokio::test]
async fn a_key_range_scan_loads_a_cold_key_range_in_one_data_get() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    // One edge's rows sort newest first.
    let expected: Vec<_> = (1..=1024)
        .rev()
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
        run_seq: CommitSeq(1024),
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
        .scan_range(
            FAMILY,
            &prefix,
            string_prefix_upper_bound(&prefix).as_deref(),
            usize::MAX,
            Some(&filter_probe),
            scan::BlocksPerRefill::KeyRange,
        )
        .await
        .expect("prefix scan")
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let data_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter(|(_, range)| range.is_none_or(|(start, _)| start < filter_offset))
        .collect();
    assert_eq!(data_gets, vec![(object_key, Some(data_range))]);
}

/// A memory block cache that counts the inserts of each data block, which
/// a read makes for each block that it fetches and decodes.
struct InsertCountingBlocks {
    memory: MemorySegmentBlocks,
    data_inserts: Mutex<BTreeMap<u64, usize>>,
}

#[async_trait]
impl SegmentBlockCache for InsertCountingBlocks {
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
            *self
                .data_inserts
                .lock()
                .expect("inserts lock")
                .entry(key.block_offset)
                .or_default() += 1;
        }
        self.memory.insert_block(key, block, stored);
    }

    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
        self.memory.get_or_load_block(key, handle, load).await
    }
}

/// Checks that a scan of one key prefix over many data blocks fetches them in
/// one GET and decodes each block once, when the block cache keeps fewer
/// blocks than the scan reads.
#[tokio::test]
async fn a_prefix_scan_through_a_small_block_cache_fetches_and_decodes_each_block_once() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    // One edge's rows sort newest first.
    let expected: Vec<_> = (1..=96)
        .rev()
        .flat_map(|sequence| rows(1, 1, sequence))
        .collect();
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
    // A budget of four blocks, so the cache evicts the scan's blocks.
    let probe = MetadataSegmentCache::unshared(usize::MAX);
    data_block_load::load_segment_data_block_span(
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
    let blocks = Arc::new(InsertCountingBlocks {
        memory: MemorySegmentBlocks::new(Arc::clone(&shared), ObjectStoreScope::new(0)),
        data_inserts: Mutex::default(),
    });
    let cache = MetadataSegmentCache::with_blocks(
        blocks.clone(),
        shared,
        ObjectStoreScope::new(0),
        crate::segment::cache::unshared_read_working_memory(),
    );
    let runs = vec![MetadataRunManifest {
        run_no: RunNo(1),
        run_seq: CommitSeq(96),
        tier: RunTier::Delta,
        segments: vec![MetadataFamilySegments {
            family: FAMILY,
            segments: vec![descriptor.clone()],
        }],
    }];
    let view = scan::VerifiedMetadataSegments::from_runs(&store, &cache, runs);
    let filter_probe = expected[0].filter_key_for_family(FAMILY);
    store.reset();

    let actual = view
        .scan_range(
            FAMILY,
            &format!("{filter_probe}-"),
            string_prefix_upper_bound(&format!("{filter_probe}-")).as_deref(),
            usize::MAX,
            Some(&filter_probe),
            scan::BlocksPerRefill::KeyRange,
        )
        .await
        .expect("prefix scan")
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let data_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter_map(|(_, range)| range)
        .filter(|(start, _)| *start < descriptor.filter_block.offset)
        .collect();
    // The key range ends in block 97, whose last row key is past the prefix
    // and which can still hold a key below it.
    let first = index[1].block;
    let last = index[97].block;
    assert_eq!(
        data_gets,
        vec![(first.offset, last.offset + u64::from(last.stored_bytes))],
        "one GET reads the blocks of the prefix, which are under 1 MiB"
    );
    let inserts = blocks.data_inserts.lock().expect("inserts lock");
    assert_eq!(
        inserts.len(),
        97,
        "the scan fetches every block of its key range"
    );
    assert!(
        inserts.values().all(|count| *count == 1),
        "each block is decoded and inserted once: {inserts:?}"
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
    // The first page fetches block 0 alone, so the damaged block 64 doesn't
    // fail it.
    load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
        CommitSeq(1),
        "",
        None,
        1,
        scan::BlocksPerRefill::One,
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
            CommitSeq(1),
            &lower,
            None,
            1,
            scan::BlocksPerRefill::One,
        )
        .await
        .is_err(),
        "requested corrupt data must fail validation"
    );
}

/// Checks that a damaged data block fails only the reads that decode it. A
/// read of a segment small enough to fetch whole decodes every block of the
/// segment, so each read of it fails, and a ranged read fetches and decodes
/// only the block that it asks for.
#[tokio::test]
async fn a_damaged_block_fails_only_the_reads_that_decode_it() {
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
            &NamespaceId::parse("damaged-block").expect("namespace"),
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
                CommitSeq(1),
                &lower,
                string_prefix_upper_bound(&lower).as_deref(),
                1,
                scan::BlocksPerRefill::One,
            )
            .await;
            let whole_object = count == 16;
            if whole_object || position == 2 {
                assert!(matches!(
                    loaded,
                    Err(ManifestLoadError::SegmentCodec { .. })
                ));
            } else {
                let loaded = loaded.expect("intact requested block");
                assert_eq!(loaded.rows().collect::<Vec<_>>(), vec![&expected[position]]);
            }
            // A whole-object read publishes nothing when a block fails, so
            // each read fetches the object again. A ranged read fetches the
            // index once and then each block that it asks for.
            assert_eq!(
                store.count(OperationSelector::Read),
                if whole_object {
                    position + 1
                } else {
                    position + 2
                }
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
            revision: Some(crate::test_support::revision_summary()),
        };
    }
    let mut descriptor = segment(&store, &expected).await;
    descriptor.filter_inline = None;
    let read_working_memory = Arc::new(ReadWorkingMemory::new(64 * 1024 * 1024, None));
    let memo = SessionBlockMemo::new(Arc::clone(&read_working_memory));
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
    let index_reservation = read_working_memory.in_use();
    assert!(
        index_reservation > index_bytes,
        "the index and memo containers are charged"
    );
    assert_eq!(store.count(OperationSelector::Read), 1);
    let filter = index_block_load::load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("filter");
    let filter_bytes = filter.decoded_bytes() + 2 * std::mem::size_of::<usize>();
    assert!(read_working_memory.in_use() >= index_reservation + filter_bytes);
    assert_eq!(store.count(OperationSelector::Read), 2);
    assert_eq!(
        *index_block_load::load_segment_index(&store, None, &memo, &descriptor)
            .await
            .expect("retained index"),
        *index
    );
    assert_eq!(store.count(OperationSelector::Read), 2);
    drop(memo);
    assert_eq!(read_working_memory.in_use(), 0);
    assert_eq!(index.len(), 2048);
    assert!(filter.may_contain(&expected[0].filter_key_for_family(FAMILY)));
}

/// Checks that the pages of a view's inode rows, read one after another,
/// list each of the view's inodes once in ID order, when the manifest and
/// the WAL tail both have rows and the tail has more rows than a page.
#[tokio::test]
async fn inode_pages_list_each_inode_once_when_the_tail_has_more_rows_than_a_page() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("inode-pages").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let fixture = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
    let paths = |name: &str| -> Vec<String> {
        (0..4).map(|index| format!("/{name}-{index}.txt")).collect()
    };
    let folded = paths("folded");
    fixture
        .write_files_bytes(&folded, b"folded\n")
        .await
        .expect("write the files that the fold moves into the manifest");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let unfolded = [paths("first"), paths("second")].concat();
    for commit in unfolded.chunks(4) {
        fixture
            .write_files_bytes(commit, b"unfolded\n")
            .await
            .expect("write the files that stay in the tail");
    }

    let mut expected = vec![ROOT_INODE_ID];
    for path in folded.iter().chain(&unfolded) {
        expected.push(crate::test_support::visible_inode_id(&store, &namespace_id, path).await);
    }
    expected.sort();

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let loaded = namespace
        .load_current_metadata_view()
        .await
        .expect("load the current view");
    let tail_inodes: usize = loaded
        .wal_tail
        .row_states(loaded.visible_seq())
        .map(|state| state.inodes().len())
        .sum();
    assert_eq!(
        tail_inodes,
        unfolded.len(),
        "the tail has the inodes of the files written after the fold"
    );
    let view = loaded.metadata_view();
    for limit in [1, 3, 100] {
        let mut listed = Vec::new();
        loop {
            let page = view
                .inodes_page(listed.last().copied(), limit)
                .await
                .expect("inode page");
            assert!(page.len() <= limit, "limit {limit}");
            listed.extend(page.iter().map(|inode| inode.inode_id));
            if page.len() < limit {
                break;
            }
        }
        assert_eq!(listed, expected, "limit {limit}");
    }
}
