//! Bounded loading must return the same raw-row prefix as an unbounded scan.

use super::super::block_load::load_manifest_segment_rows_in_key_range_with_cache;
use super::super::build::{write_manifest_segment, MetadataSegmentDestination};
use super::*;

const FAMILY: ApiMetadataRowFamily = ApiMetadataRowFamily::DirentryBinds;

fn rows(start: usize, count: usize, generation: u64) -> Vec<MetadataRow> {
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
            MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse(&name).expect("name"),
                display_name: loonfs_api::DisplayName::parse(&name).expect("display name"),
                child_inode_id: InodeId(index as u64 + 2),
                bind_seq: ChangeSeq(generation),
                bind_delta_index: 0,
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
        MetadataSegmentDestination::Published {
            namespace_id: &NamespaceId::parse("bounded-pages").expect("namespace"),
        },
        FAMILY,
        0,
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
async fn bounded_pages_merge_overlapping_runs_and_binding_generations() {
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
    });
    for limit in [1, 31, 33, 65, 1000] {
        let mut actual = Vec::new();
        let mut lower = String::new();
        loop {
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
        assert_eq!(actual, expected, "merged traversal with page limit {limit}");
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
