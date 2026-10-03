//! Bounded loading must return the same raw-row prefix as an unbounded scan.

use super::super::block_load::load_manifest_segment_rows_in_key_range_with_cache;
use super::super::build::write_manifest_segment;
use super::*;
use loonfs_test_support::stores::ConcurrencyWatchStore;

const FAMILY: ApiMetadataRowFamily = ApiMetadataRowFamily::DirentryBinds;

fn rows(start: usize, count: usize, sequence: u64) -> Vec<MetadataRow> {
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
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse(&name).expect("name"),
                state: loonfs_types::format::manifest::DirentryBindingState::Bound {
                    display_name: loonfs_types::DisplayName::parse(&name).expect("display name"),
                },
                child_inode_id: InodeId(index as u64 + 2),
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
    let built = builder.finish().expect("finish segment");
    assert!(
        built.bytes.len() > 288 * 1024,
        "exercise ranged, not eager reads"
    );
    write_manifest_segment(
        store,
        &NamespaceId::parse("bounded-pages").expect("namespace"),
        FAMILY,
        built,
    )
    .await
    .expect("write fixture segment")
}

#[tokio::test]
async fn bounded_pages_match_full_rows_across_windows_ranges_and_eviction() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    for budget in [1, 8 * 1024 * 1024] {
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
            for readahead in [scan::Readahead::Enabled, scan::Readahead::Disabled] {
                let loaded = load_manifest_segment_rows_in_key_range_with_cache(
                    &store,
                    Some(&cache),
                    &load::SessionBlockMemo::default(),
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
        if budget == 1 {
            assert!(cache.stats().evictions > 0, "exercise real cache eviction");
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
async fn bounded_pages_open_and_refill_segments_in_concurrent_waves() {
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
                    &load::SessionBlockMemo::default(),
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
                OperationClass::Read,
            ),
            KeyPredicate::any(),
        );
        watched.inner().arm();
        let view = scan::VerifiedMetadataSegments::from_runs(&watched, &cache, runs.clone());
        let mut page = std::pin::pin!(view.scan_range_page(FAMILY, &lower, None, limit));
        // The watcher yields once before the request reaches the holding store.
        assert!(futures::poll!(page.as_mut()).is_pending());
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
    let index = block_fetch::load_segment_index(
        &store,
        None,
        &load::SessionBlockMemo::default(),
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

#[tokio::test]
async fn bounded_pages_preserve_readahead_without_reading_the_entire_range() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let first = load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        Some(&cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        ChangeSeq(1),
        "",
        None,
        1,
        scan::Readahead::Enabled,
    )
    .await
    .expect("first page");
    assert_eq!(
        first.rows().count(),
        32,
        "only the first aligned window is loaded"
    );
    store.reset();
    let lower = expected[1].row_key_for_family(FAMILY);
    load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        Some(&cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        ChangeSeq(1),
        &lower,
        None,
        1,
        scan::Readahead::Enabled,
    )
    .await
    .expect("next page");
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "next page uses read-ahead"
    );
}

#[tokio::test]
async fn bounded_pages_reject_corruption_when_the_damaged_block_is_requested() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    let index = block_fetch::load_segment_index(
        &store,
        None,
        &load::SessionBlockMemo::default(),
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
    bytes[index[64].block.offset as usize] ^= 1;
    store
        .put_overwrite(&key, Bytes::from(bytes))
        .await
        .expect("corrupt fixture");
    // Unrequested, non-read-ahead data need not be eagerly validated.
    load_manifest_segment_rows_in_key_range_with_cache(
        &store,
        None,
        &load::SessionBlockMemo::default(),
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
            &load::SessionBlockMemo::default(),
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
        let built = builder.finish().expect("segment");
        let descriptor = write_manifest_segment(
            &store,
            &NamespaceId::parse("lookup-readahead").expect("namespace"),
            FAMILY,
            built,
        )
        .await
        .expect("write segment");
        assert_eq!(
            block_fetch::segment_object_len(&descriptor) <= 256 * 1024,
            count == 16
        );
        let index = block_fetch::load_segment_index(
            &store,
            None,
            &load::SessionBlockMemo::default(),
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
        bytes[index[2].block.offset as usize] ^= 1;
        store
            .put_overwrite(&key, Bytes::from(bytes))
            .await
            .expect("corrupt unrequested block");
        let memo = load::SessionBlockMemo::default();
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
                loonfs_types::format::sst_blocks::string_prefix_upper_bound(&lower).as_deref(),
                1,
                scan::Readahead::Stored,
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
                store.count(OperationClass::Read),
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
    let pool = Arc::new(ReadWorkingMemory::default());
    let memo = load::SessionBlockMemo::new(Arc::clone(&pool));
    store.reset();
    let index = block_fetch::load_segment_index(&store, None, &memo, &descriptor)
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
    assert_eq!(store.count(OperationClass::Read), 1);
    let filter = block_fetch::load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("filter");
    let filter_bytes = filter.decoded_bytes() + 2 * std::mem::size_of::<usize>();
    assert!(pool.in_use() >= index_reservation + filter_bytes);
    assert_eq!(store.count(OperationClass::Read), 2);
    assert_eq!(
        *block_fetch::load_segment_index(&store, None, &memo, &descriptor)
            .await
            .expect("retained index"),
        *index
    );
    assert_eq!(store.count(OperationClass::Read), 2);
    drop(memo);
    assert_eq!(pool.in_use(), 0);
    assert_eq!(index.len(), 2048);
    assert!(filter.may_contain(&expected[0].filter_key_for_family(FAMILY)));
}
