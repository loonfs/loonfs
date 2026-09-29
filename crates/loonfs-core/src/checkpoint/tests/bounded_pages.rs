//! Bounded loading must return the same raw-row prefix as an unbounded scan.

use super::super::block_load::load_manifest_segment_rows_in_key_range_with_cache;
use super::super::build::write_manifest_segment;
use super::*;

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
                state: loonfs_api::wire::manifest::DirentryBindingState::Bound {
                    display_name: loonfs_api::DisplayName::parse(&name).expect("display name"),
                },
                child_inode_id: InodeId(index as u64 + 2),
                child_kind: loonfs_api::InodeKind::File,
                child_created_by: loonfs_api::ActorId::loonfs(),
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
        let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
            max_decoded_bytes: budget,
            ..MetadataSegmentCacheConfig::default()
        });
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
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
        max_decoded_bytes: 1,
        ..MetadataSegmentCacheConfig::default()
    });
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
async fn bounded_pages_preserve_readahead_without_reading_the_entire_range() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let expected = rows(0, 1024, 1);
    let descriptor = segment(&store, &expected).await;
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
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
                loonfs_api::wire::sst_blocks::string_prefix_upper_bound(&lower).as_deref(),
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
