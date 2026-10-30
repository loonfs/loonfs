//! Checkpoint metadata cache, scan sharing, filters, and lookup behavior.

use super::*;
use crate::block_cache::DecodedBlockCacheStats;
use crate::segment::block_memo::SessionBlockMemo;
use crate::segment::cache::{DecodedMetadataSegmentBlock, MetadataSegmentCacheKey};
use crate::segment::index_block_load::load_segment_filter;
use crate::segment::segment_block_cache::{SegmentBlockCache, SegmentBlockLoad};
use loonfs_test_support::stores::{replace_object, ConcurrencyWatchStore, MemoryStore};
use std::assert_matches;
use std::sync::Mutex;

async fn eight_files_one_row_per_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> ManifestNo {
    bootstrap_namespace(store, namespace_id, context)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        crate::test_support::NamespaceFixture::new(store, namespace_id, context)
            .write_file_bytes(&path, b"file\n", None)
            .await
            .expect("write file");
    }
    let policy = MetadataRunPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    checkpoint_then_compact(store, namespace_id, context, policy).await
}

#[tokio::test]
async fn a_byte_budgeted_cache_admits_wide_scans_and_holds_to_its_budget() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    eight_files_one_row_per_segment(&store, &namespace_id, &context).await;
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    // The default cache config carries a decoded-byte budget, so a scan wider
    // than the small-scan limit populates the cache instead of reading through.
    let cache = super::MetadataSegmentCache::unshared(usize::MAX);
    let segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load segments");

    let revisions = segments
        .scan_prefix(ApiMetadataRowFamily::Revisions, "revision-")
        .await
        .expect("scan revisions");
    let after_first = cache.stats();
    assert!(revisions.len() >= 8);
    assert!(
        after_first.inserts >= 8,
        "a wide scan against a byte-budgeted cache should admit every segment"
    );

    // A fresh view has no per-view segment memo; only the shared cache can
    // answer, so the repeated scan must issue no segment fetches.
    let fresh_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load fresh segments");
    store.reset();
    let repeated = fresh_segments
        .scan_prefix(ApiMetadataRowFamily::Revisions, "revision-")
        .await
        .expect("repeated scan");
    let after_repeat = cache.stats();

    assert_eq!(repeated, revisions);
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "a warm wide scan should be served entirely from the cache"
    );
    assert!(after_repeat.hits > after_first.hits);

    let docs_inode_id = crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await;
    let lower_bound =
        loonfs_types::format::manifest::lookup_keys::direntry_parent_prefix(docs_inode_id);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let before_range = cache.stats();
    let page = segments
        .scan_range_page(
            ApiMetadataRowFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("scan range page");
    assert_eq!(page.len(), 8);
    assert!(
        cache.stats().inserts > before_range.inserts + 4,
        "a wide range scan should admit its segments to the cache"
    );

    let fresh_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load fresh segments");
    store.reset();
    let repeated_page = fresh_segments
        .scan_range_page(
            ApiMetadataRowFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("repeated scan range page");
    assert_eq!(repeated_page, page);
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "a warm range scan should be served entirely from the cache"
    );

    let degenerate = MetadataSegmentCache::unshared(1);
    let degenerate_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&degenerate),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load segments against a degenerate budget");
    let root_inode_key =
        loonfs_types::format::manifest::lookup_keys::inode_key(loonfs_types::ROOT_INODE_ID);
    assert!(degenerate_segments
        .find_row(
            ApiMetadataRowFamily::Inodes,
            &root_inode_key,
            &root_inode_key
        )
        .await
        .expect("get inode")
        .is_some());
    // Every block is heavier than the budget, so the cache keeps none of
    // them and the lookup reads each block from the store.
    let degenerate_stats = degenerate.stats();
    assert!(degenerate_stats.misses > 0);
    assert_eq!(
        (
            degenerate_stats.inserts,
            degenerate_stats.evictions,
            degenerate_stats.cached_decoded_bytes
        ),
        (0, 0, 0)
    );
}

#[tokio::test]
async fn concurrent_scans_share_one_fetch_per_segment() {
    // The local-filesystem store finishes reads on blocking threads, so the
    // two scans below would interleave differently on every run. Here each
    // segment read yields once and then finishes, so the scans interleave the
    // same way every time, and each one still finds the other's reads in
    // flight.
    let store = RecordingStore::metadata_segments(ConcurrencyWatchStore::new(
        MemoryStore::new(loonfs_types::ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::metadata_segment(),
    ));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    eight_files_one_row_per_segment(&store, &namespace_id, &context).await;
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    // Concurrent scans over one shared cache must not multiply fetches:
    // single-flight covers blocks racing before the first insert lands, and
    // population covers everything after.
    let cache = super::MetadataSegmentCache::unshared(usize::MAX);
    // A solo pass over its own cold cache measures the true per-scan
    // fetch count.
    let solo_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load solo segments");
    store.reset();
    let solo = solo_segments
        .scan_prefix(ApiMetadataRowFamily::Revisions, "revision-")
        .await
        .expect("solo scan");
    let solo_fetches = store.count(OperationSelector::Read);
    assert!(solo.len() >= 8);
    assert!(solo_fetches >= 8, "solo scan should fetch every segment");

    // Concurrent requests race over a second cold cache, each with its own
    // segments view; single-flight is what keeps the pair at the solo count.
    let paired_cache = super::MetadataSegmentCache::unshared(usize::MAX);
    let first_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load first segments");
    let second_segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load second segments");
    store.reset();
    let (first, second) = tokio::join!(
        first_segments.scan_prefix(ApiMetadataRowFamily::Revisions, "revision-"),
        second_segments.scan_prefix(ApiMetadataRowFamily::Revisions, "revision-"),
    );
    let paired_fetches = store.count(OperationSelector::Read);
    assert_eq!(first.expect("first scan"), second.expect("second scan"));
    assert_eq!(
        paired_fetches, solo_fetches,
        "concurrent scans over one shared cache should share one fetch per segment"
    );
}

#[tokio::test]
async fn cached_manifest_carries_its_scan_order_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 0..4 {
        let path = format!("/docs/file-{index}.txt");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&path, b"file\n", None)
            .await
            .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/tail.txt", b"tail\n", None)
        .await
        .expect("write tail file");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create second checkpoint");
    let manifest_number = current_manifest_number(&store, &namespace_id).await;

    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let first = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load first segments");
    let second = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load second segments");

    assert!(
        first.scan_runs.len() >= 2,
        "two checkpoints should leave more than one run to order"
    );
    assert_eq!(
        *first.scan_runs,
        runs_newest_first(first.manifest().payload()),
        "the cached run list must equal the manifest's newest-first grouping"
    );
    assert!(
        Arc::ptr_eq(&first.scan_runs, &second.scan_runs),
        "views over one cached manifest should share one derived run list"
    );
}

/// Loads the namespace's manifest through `cache`, scans every revision
/// row, and loads the manifest again. Returns the number of GETs for the
/// manifest that the second load sent.
async fn scan_between_manifest_loads(
    store: &RecordingStore<MemoryStore>,
    cache: &MetadataSegmentCache,
    namespace_id: &NamespaceId,
    manifest_no: &ManifestNo,
) -> usize {
    let segments =
        super::load_manifest_segments_for_inspection(store, Some(cache), namespace_id, manifest_no)
            .await
            .expect("load segments");
    segments
        .scan_prefix(ApiMetadataRowFamily::Revisions, "revision-")
        .await
        .expect("scan revisions");
    store.reset();
    super::load_manifest_segments_for_inspection(store, Some(cache), namespace_id, manifest_no)
        .await
        .expect("load segments again");
    let manifest_key = metadata_manifest_object(namespace_id, manifest_no);
    store
        .take_get_keys()
        .iter()
        .filter(|key| **key == manifest_key)
        .count()
}

/// Checks that manifests and segment blocks are charged to budgets of their
/// own. A scan whose blocks are four times the blocks' budget evicts blocks
/// but not the manifest, so the next load of the manifest sends no GET, and
/// the manifest budget is charged the manifest alone. With a zero manifest
/// budget, the cache keeps no manifest, and the blocks' budget is charged
/// the same blocks as before.
#[tokio::test]
async fn manifests_and_blocks_are_charged_to_budgets_of_their_own() {
    // The scan loads its blocks with several loads in flight, and the
    // evictions depend on the order in which the loads finish. An in-memory
    // store finishes each load the first time it is polled, so both scans
    // below load their blocks in the same order.
    let store = RecordingStore::new(
        MemoryStore::new(loonfs_types::ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    eight_files_one_row_per_segment(&store, &namespace_id, &test_context()).await;
    let manifest_no = current_manifest_number(&store, &namespace_id).await;
    let cache = |max_segment_bytes, max_manifest_bytes| {
        MetadataSegmentCache::new(
            Arc::new(SharedSegmentBlocks::new(
                max_segment_bytes,
                max_manifest_bytes,
            )),
            ObjectStoreScope::new(0),
            crate::segment::cache::unshared_read_working_memory(),
            crate::limits::READAHEAD_WINDOW_BYTES,
        )
    };
    let unbounded = cache(usize::MAX, usize::MAX);
    scan_between_manifest_loads(&store, &unbounded, &namespace_id, &manifest_no).await;
    let (blocks_bytes, manifest) = (
        unbounded.stats().cached_decoded_bytes,
        unbounded.manifest_stats(),
    );
    assert_eq!(manifest.inserts, 1, "{manifest:?}");

    let mut blocks = Vec::new();
    for max_manifest_bytes in [manifest.cached_decoded_bytes, 0] {
        let cache = cache(blocks_bytes / 4, max_manifest_bytes);
        let manifest_gets =
            scan_between_manifest_loads(&store, &cache, &namespace_id, &manifest_no).await;
        let (stats, manifests) = (cache.stats(), cache.manifest_stats());
        assert!(stats.evictions > 0, "the scan outgrows the blocks' budget");
        if max_manifest_bytes == 0 {
            assert_eq!(manifest_gets, 1, "a zero budget keeps no manifest");
            assert_eq!(manifests, DecodedBlockCacheStats::default());
        } else {
            assert_eq!(manifest_gets, 0, "the manifest stays through the scan");
            assert_eq!(
                (
                    manifests.inserts,
                    manifests.hits,
                    manifests.evictions,
                    manifests.cached_decoded_bytes
                ),
                (1, 1, 0, max_manifest_bytes),
                "the manifest budget is charged the manifest alone"
            );
        }
        blocks.push((stats.inserts, stats.evictions, stats.cached_decoded_bytes));
    }
    assert_eq!(
        blocks[0], blocks[1],
        "the blocks' budget is charged no manifest"
    );
}

#[tokio::test]
async fn segment_range_page_merges_base_and_delta_in_row_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/a.txt", b"a\n", None)
        .await
        .expect("write a");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/c.txt", b"c\n", None)
        .await
        .expect("write c");

    let policy = MetadataRunPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/b.txt", b"b\n", None)
        .await
        .expect("write b");
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        super::load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");

    let docs_inode_id = crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await;
    let lower_bound =
        loonfs_types::format::manifest::lookup_keys::direntry_parent_prefix(docs_inode_id);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let page = segments
        .scan_range_page(
            ApiMetadataRowFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            2,
        )
        .await
        .expect("scan range page");
    let display_names = page
        .into_iter()
        .filter_map(|row| match row {
            MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
                state: loonfs_types::format::manifest::DirentryBindingState::Bound { display_name },
                ..
            }) => Some(display_name.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(display_names, vec!["a.txt", "b.txt"]);
}

#[tokio::test]
async fn lookup_skips_segments_whose_filter_rules_the_name_out() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    // "beta" lands in the base run; a second checkpoint puts "alpha" and
    // "gamma" in a delta run. The delta bind segment's key range then straddles
    // "beta", so min/max pruning cannot exclude it — only its bloom filter
    // can prove the name absent.
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/beta.txt", b"beta\n", None)
        .await
        .expect("write beta");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/alpha.txt", b"alpha\n", None)
        .await
        .expect("write alpha");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/gamma.txt", b"gamma\n", None)
        .await
        .expect("write gamma");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("second checkpoint");

    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let segments = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("load segments");

    // Resolve /docs, then look up beta's bind under it through the filtered
    // scan the visibility adapter uses.
    let docs_binds = segments
        .scan_prefix(
            ApiMetadataRowFamily::DirentryBinds,
            &loonfs_types::format::manifest::lookup_keys::direntry_parent_prefix(
                loonfs_types::ROOT_INODE_ID,
            ),
        )
        .await
        .expect("scan root binds");
    let docs_inode = docs_binds
        .iter()
        .find_map(|row| match row {
            MetadataRow::DirentryBinding(crate::metadata::DirentryBindingRecord {
                child_inode_id,
                ..
            }) => Some(*child_inode_id),
            _ => None,
        })
        .expect("docs directory bind");
    let encoded_name = loonfs_types::format::manifest::hex_encode_row_key_component("beta.txt");
    let filter_probe = format!(
        "{}{encoded_name}",
        loonfs_types::format::manifest::lookup_keys::direntry_parent_prefix(docs_inode)
    );
    let prefix = format!("{filter_probe}-");
    let rows = segments
        .scan_prefix_for_lookup(
            ApiMetadataRowFamily::DirentryBinds,
            &prefix,
            &filter_probe,
            super::scan::Readahead::Disabled,
        )
        .await
        .expect("filtered lookup");

    assert_eq!(rows.len(), 1, "beta's bind should still be found");
    let stats = cache.stats();
    // The probe contains `/docs`'s random inode ID, so the delta segment's
    // filter rejects it in most runs of the test and admits it by a false
    // positive in the others. The lookup records a skip in the first case and
    // a false positive in the second.
    let delta_bind = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .max_by_key(|run| run.run_seq)
        .expect("the second checkpoint's run")
        .segments
        .iter()
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryBinds)
        .expect("the second checkpoint's bind segment");
    let filter = load_segment_filter(&store, None, &SessionBlockMemo::default(), delta_bind)
        .await
        .expect("load the delta filter");
    if filter.may_contain(&filter_probe) {
        assert!(
            stats.filter_false_positives >= 1,
            "the delta filter admits the probe, stats: {stats:?}"
        );
    } else {
        assert!(
            stats.filter_skips >= 1,
            "the delta bind segment should be skipped by its filter, stats: {stats:?}"
        );
    }
}

#[tokio::test]
async fn a_view_reuses_decoded_blocks_without_a_shared_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    let manifest_number = {
        checkpoint_then_compact(
            &store,
            &namespace_id,
            &context,
            MetadataRunPolicy::default(),
        )
        .await;
        current_manifest_number(&store, &namespace_id).await
    };

    // The cold-boot shape: a view with no shared cache attached. Repeating
    // a lookup must not re-fetch — the per-view memo is the only reuse this
    // configuration has, and without it a cold list degrades from one fetch
    // per block to one fetch per lookup.
    let segments =
        super::load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");
    let other = loonfs_types::format::manifest::lookup_keys::inode_key(
        crate::test_support::visible_inode_id(&store, &namespace_id, "/docs").await,
    );
    store.reset();
    let key = loonfs_types::format::manifest::lookup_keys::inode_key(loonfs_types::ROOT_INODE_ID);
    assert!(segments
        .find_row(ApiMetadataRowFamily::Inodes, &key, &key)
        .await
        .expect("first lookup")
        .is_some());
    let first_lookup_gets = store.count(OperationSelector::Read);
    assert!(first_lookup_gets > 0, "a cold lookup fetches blocks");

    assert!(segments
        .find_row(ApiMetadataRowFamily::Inodes, &key, &key)
        .await
        .expect("repeated lookup")
        .is_some());
    assert!(segments
        .find_row(ApiMetadataRowFamily::Inodes, &other, &other)
        .await
        .expect("second-key lookup")
        .is_some());
    assert_eq!(
        store.count(OperationSelector::Read),
        first_lookup_gets,
        "later lookups through the same view should reuse decoded blocks"
    );
}

#[tokio::test]
async fn a_zero_block_memo_refetches_data_blocks_for_reads_and_folds() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    checkpoint_then_compact(
        &store,
        &namespace_id,
        &context,
        MetadataRunPolicy::default(),
    )
    .await;
    let manifest_number = current_manifest_number(&store, &namespace_id).await;

    let cache = Arc::new(MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(0, 0)),
        ObjectStoreScope::new(0),
        Arc::new(crate::cache::ReadWorkingMemory::new(0, None)),
        crate::limits::READAHEAD_WINDOW_BYTES,
    ));
    let namespace = crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    };
    let read_view = super::load_manifest_segments_for_inspection(
        &namespace.store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("read view");
    let fold_view = namespace
        .load_manifest_projection(crate::test_support::observed_now())
        .await
        .expect("fold view")
        .manifest_segments;
    let key = loonfs_types::format::manifest::lookup_keys::inode_key(loonfs_types::ROOT_INODE_ID);
    for segments in [&read_view, &fold_view] {
        store.reset();
        assert!(segments
            .find_row(ApiMetadataRowFamily::Inodes, &key, &key)
            .await
            .expect("first lookup")
            .is_some());
        let first_lookup_reads = store.count(OperationSelector::Read);
        assert!(segments
            .find_row(ApiMetadataRowFamily::Inodes, &key, &key)
            .await
            .expect("repeated lookup")
            .is_some());
        assert!(
            store.count(OperationSelector::Read) > first_lookup_reads,
            "a zero memo should keep no data block for the repeated lookup"
        );
    }
}

#[tokio::test]
async fn point_lookups_skip_inline_filtered_runs_without_fetches() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    // Three checkpoints append three delta runs whose direntry key ranges
    // straddle one another (each run binds names from both ends of the
    // alphabet), so range pruning alone cannot narrow a name lookup below
    // several candidate segments — the shape a bulk-loaded directory's
    // unfolded backlog takes.
    for names in [["a.txt", "z.txt"], ["b.txt", "y.txt"], ["c.txt", "x.txt"]] {
        for name in names {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .write_file_bytes(&format!("/docs/{name}"), b"content\n", None)
                .await
                .expect("write file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");
    let direntry_descriptors: Vec<_> = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryBinds)
        .collect();
    assert!(direntry_descriptors.len() >= 3);
    assert!(
        direntry_descriptors
            .iter()
            .all(|descriptor| descriptor.filter_inline.is_some()),
        "small delta-run segments should inline their filters in the manifest"
    );

    let materialized = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        segments.manifest().payload().manifest_no,
    )
    .await
    .expect("materialize manifest");
    let binding = materialized
        .metadata_state
        .direntry_binds()
        .iter()
        .find(|binding| binding.name_key.as_str() == "x.txt")
        .expect("binding for x.txt")
        .clone();
    let prefix = lookup_keys::direntry_bind_prefix(binding.parent_inode_id, &binding.name_key);
    let probe = lookup_keys::direntry_bind_probe(binding.parent_inode_id, &binding.name_key);

    store.reset();
    let rows = segments
        .scan_prefix_for_lookup(
            ApiMetadataRowFamily::DirentryBinds,
            &prefix,
            &probe,
            super::scan::Readahead::Enabled,
        )
        .await
        .expect("point lookup");
    assert_eq!(rows.len(), 1, "exactly one bind row for the probed name");
    assert_eq!(
        store.count(OperationSelector::Read),
        1,
        "inline filters reject the other runs without fetches, and the one \
         admitted small segment loads whole with a single ranged GET"
    );

    // The same lookup against the same manifest with the inline copies
    // stripped must return the same rows through fetched filter blocks —
    // the inline copy is an accelerator, never an answer of its own.
    let mut stripped_payload = segments.manifest().payload().clone();
    stripped_payload.manifest_no = ManifestNo(stripped_payload.manifest_no.0 + 1);
    stripped_payload.manifest_no = stripped_payload
        .manifest_no
        .successor()
        .expect("next number");
    for descriptor in stripped_payload
        .runs
        .iter_mut()
        .flat_map(|run| &mut run.segments)
    {
        descriptor.filter_inline = None;
    }
    let stripped_object_id = stripped_payload.manifest_no;
    let stripped = encode_namespace_manifest_json(stripped_payload)
        .expect("stripped manifest envelope")
        .into_envelope();
    write_namespace_manifest(&store, stripped.payload().clone())
        .await
        .expect("write stripped manifest");
    let stripped_segments =
        load_manifest_segments_for_inspection(&store, None, &namespace_id, &stripped_object_id)
            .await
            .expect("load stripped segments");
    store.reset();
    let stripped_rows = stripped_segments
        .scan_prefix_for_lookup(
            ApiMetadataRowFamily::DirentryBinds,
            &prefix,
            &probe,
            super::scan::Readahead::Enabled,
        )
        .await
        .expect("point lookup without inline filters");
    assert_eq!(stripped_rows, rows);
    assert!(
        store.count(OperationSelector::Read) > 1,
        "without inline copies the ruled-out runs pay filter fetches"
    );
}

#[tokio::test]
async fn corrupt_inline_filter_fails_the_lookup() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");
    let descriptor = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|descriptor| {
            descriptor.family == ApiMetadataRowFamily::DirentryBinds
                && descriptor.filter_inline.is_some()
        })
        .expect("inline-filtered direntry segment")
        .clone();

    // Flip one nibble of the inline copy: the handle's CRC no longer
    // matches, so the read must fail instead of consulting corrupt bits.
    let mut tampered = descriptor.clone();
    let mut inline = tampered.filter_inline.take().expect("inline filter");
    let flipped = if inline.ends_with('0') { '1' } else { '0' };
    inline.pop();
    inline.push(flipped);
    tampered.filter_inline = Some(inline);

    let memo = SessionBlockMemo::default();
    load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("intact inline filter decodes");
    let error = load_segment_filter(&store, None, &SessionBlockMemo::default(), &tampered)
        .await
        .expect_err("tampered inline filter must fail");
    assert_matches!(
        error,
        ManifestLoadError::SegmentCodec { .. },
        "unexpected error: {error:?}"
    );
}

/// One checkpointed direntry segment, with the inline filter copy stripped
/// from its descriptor so both the filter and the index have to come from
/// somewhere other than the manifest — the shape a base segment, whose
/// filter is too big to inline, already has.
async fn checkpointed_direntry_segment() -> (
    tempfile::TempDir,
    RecordingStore<LocalFsStore>,
    MetadataSegmentRef,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 0..4 {
        let path = format!("/docs/file-{index}.txt");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&path, b"file\n", None)
            .await
            .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");
    let mut descriptor = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryBinds)
        .expect("a direntry segment")
        .clone();
    descriptor.filter_inline = None;
    (temp_dir, store, descriptor)
}

#[tokio::test]
async fn a_cold_filter_load_fetches_the_index_in_the_same_read() {
    let (_temp_dir, store, descriptor) = checkpointed_direntry_segment().await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let memo = SessionBlockMemo::default();

    store.reset();
    load_segment_filter(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("load filter");
    assert_eq!(
        store.count(OperationSelector::Read),
        1,
        "a cold filter load pays exactly one fetch"
    );

    // That fetch published the index into the view memo, so asking for it
    // costs nothing.
    let index = index_block_load::load_segment_index(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("load index");
    assert_eq!(store.count(OperationSelector::Read), 1);
    assert!(!index.is_empty(), "a segment holds at least one data block");
}

/// One checkpointed direntry segment rebuilt with a one-byte block target,
/// so every row lands in its own data block. A handful of test rows would
/// otherwise fit in one block, and the coalescing span path only says
/// anything with several. Returns the segment's index, read with no caches,
/// so a test can drive the span path without an index load filling the
/// decoded cache first.
async fn multi_block_direntry_segment() -> (
    tempfile::TempDir,
    RecordingStore<LocalFsStore>,
    MetadataSegmentRef,
    Arc<Vec<SegmentIndexEntry>>,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 0..12 {
        let path = format!("/docs/file-{index:02}.txt");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&path, b"file\n", None)
            .await
            .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");
    let mut descriptor = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryBinds)
        .expect("a direntry segment")
        .clone();
    descriptor.filter_inline = None;

    let rows = segment_rows(&store, &descriptor).await;
    let mut builder = SegmentBlocksBuilder::new(NonZeroUsize::MIN);
    for row in &rows {
        builder
            .push(
                &row.row_key_for_family(descriptor.family),
                &row.filter_key_for_family(descriptor.family),
                row,
            )
            .expect("rebuilt rows should encode");
    }
    let built = builder.build().expect("rebuilt segment");
    replace_object(
        &store,
        &metadata_segment_object_key(&descriptor),
        Bytes::from(built.bytes.clone()),
    )
    .await
    .expect("overwrite segment");
    descriptor.row_count = built.row_count;
    descriptor.min_row_key = built.min_row_key;
    descriptor.max_row_key = built.max_row_key;
    descriptor.index_block = built.index;
    descriptor.filter_block = built.filter;
    // A one-byte target closes every row's block, including the last. The
    // segment's max key must still name that row so keyed scans can find it.
    assert_eq!(
        descriptor.max_row_key,
        rows.last()
            .expect("the segment holds rows")
            .row_key_for_family(descriptor.family),
        "the rebuilt descriptor must carry the segment's last row key"
    );

    let index = index_block_load::load_segment_index(
        &store,
        None,
        &SessionBlockMemo::default(),
        &descriptor,
    )
    .await
    .expect("rebuilt segment index");
    assert!(
        index.len() >= 9,
        "the span tests need several data blocks, got {}",
        index.len()
    );
    (temp_dir, store, descriptor, index)
}

/// Every row of one segment, in row-key order, read straight through with
/// no caches.
async fn segment_rows(
    store: &RecordingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
) -> Vec<MetadataRow> {
    let memo = SessionBlockMemo::default();
    let index = index_block_load::load_segment_index(store, None, &memo, descriptor)
        .await
        .expect("segment index");
    data_block_load::load_segment_data_block_span(store, None, Some(&memo), descriptor, &index)
        .await
        .expect("segment data blocks")
        .iter()
        .flat_map(|block| block.rows.iter().cloned())
        .collect()
}

/// One segment object's whole body, which the seeding helpers slice.
async fn segment_object_bytes(
    store: &RecordingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
) -> Bytes {
    store
        .get(&metadata_segment_object_key(descriptor), None)
        .await
        .expect("read segment object")
        .expect("segment object present")
}

fn stored_block_bytes(object: &Bytes, handle: &BlockHandle) -> Bytes {
    let start = loonfs_types::usize_from_u64(handle.offset);
    object.slice(start..start + handle.stored_bytes as usize)
}

/// Puts the named blocks into the decoded cache, as an earlier read through
/// this process would have left them.
fn seed_decoded_blocks(
    cache: &MetadataSegmentCache,
    object: &Bytes,
    descriptor: &MetadataSegmentRef,
    index: &[SegmentIndexEntry],
    positions: &[usize],
) {
    for position in positions {
        let handle = index[*position].block;
        let decoded = decode_data_block(&stored_block_bytes(object, &handle), &handle)
            .expect("seeded block decodes");
        cache.insert(
            index_block_load::segment_block_cache_key(
                descriptor,
                SegmentBlockKind::Data,
                handle.offset,
            ),
            data_block_load::decoded_data_cache_block(decoded),
        );
    }
}

/// One wide read over a whole segment through `cache`, with the per-view
/// memo a fresh request would carry, and the segment fetches it paid.
async fn wide_read(
    store: &RecordingStore<LocalFsStore>,
    cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    index: &[SegmentIndexEntry],
) -> (Vec<Arc<DecodedDataBlock>>, usize) {
    let memo = SessionBlockMemo::default();
    store.reset();
    let blocks =
        data_block_load::load_segment_data_block_span(store, cache, Some(&memo), descriptor, index)
            .await
            .expect("wide read");
    (blocks, store.count(OperationSelector::Read))
}

#[tokio::test]
async fn a_warm_span_remembers_shared_hits_in_its_bounded_memo() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let (expected, _) = wide_read(&store, Some(&cache), &descriptor, &index).await;
    let memo = SessionBlockMemo::default();
    store.reset();
    let before = cache.stats();

    let first = data_block_load::load_segment_data_block_span(
        &store,
        Some(&cache),
        Some(&memo),
        &descriptor,
        &index,
    )
    .await
    .expect("first warm span");
    let after_first = cache.stats();
    assert_eq!(after_first.hits - before.hits, index.len());
    assert_eq!(first, expected);

    let repeated = data_block_load::load_segment_data_block_span(
        &store,
        Some(&cache),
        Some(&memo),
        &descriptor,
        &index,
    )
    .await
    .expect("repeated span");
    assert_eq!(
        cache.stats().hits,
        after_first.hits,
        "memo avoids repeated shared-cache access"
    );
    assert_eq!(store.count(OperationSelector::Read), 0);
    for (first, repeated) in first.iter().zip(&repeated) {
        assert!(
            Arc::ptr_eq(first, repeated),
            "memo shares decoded blocks, not copies"
        );
    }

    // Maintenance callers without a memo still use the shared cache normally.
    let without_memo = data_block_load::load_segment_data_block_span(
        &store,
        Some(&cache),
        None,
        &descriptor,
        &index,
    )
    .await
    .expect("span without memo");
    assert_eq!(without_memo, expected);
    assert_eq!(cache.stats().hits - after_first.hits, index.len());
    assert_eq!(store.count(OperationSelector::Read), 0);
}

#[tokio::test]
async fn a_wide_read_coalesces_the_blocks_the_decoded_cache_did_not_answer() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let (expected, _) = wide_read(&store, None, &descriptor, &index).await;

    let decoded = [0usize, 4];
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let object = segment_object_bytes(&store, &descriptor).await;
    seed_decoded_blocks(&cache, &object, &descriptor, &index, &decoded);

    let (read, gets) = wide_read(&store, Some(&cache), &descriptor, &index).await;

    assert_eq!(read, expected);
    assert_eq!(
        gets, 2,
        "one ranged GET per run of consecutive blocks nothing answered"
    );
}

/// A block cache that keeps blocks in a map, and records the offset and the
/// handle of every get and the offset and the stored bytes of every insert.
#[derive(Default)]
struct CountingBlockCache {
    blocks: Mutex<HashMap<MetadataSegmentCacheKey, DecodedMetadataSegmentBlock>>,
    gets: Mutex<Vec<(u64, BlockHandle)>>,
    inserts: Mutex<Vec<(u64, Bytes)>>,
}

#[async_trait]
impl SegmentBlockCache for CountingBlockCache {
    async fn get_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
    ) -> Option<DecodedMetadataSegmentBlock> {
        let mut gets = self.gets.lock().expect("gets lock");
        gets.push((key.block_offset, *handle));
        self.blocks.lock().expect("blocks lock").get(key).cloned()
    }

    fn insert_block(
        &self,
        key: MetadataSegmentCacheKey,
        block: DecodedMetadataSegmentBlock,
        stored: &[u8],
    ) {
        let mut inserts = self.inserts.lock().expect("inserts lock");
        inserts.push((key.block_offset, Bytes::copy_from_slice(stored)));
        self.blocks.lock().expect("blocks lock").insert(key, block);
    }

    async fn get_or_load_block(
        &self,
        key: &MetadataSegmentCacheKey,
        handle: &BlockHandle,
        load: SegmentBlockLoad<'_>,
    ) -> Result<DecodedMetadataSegmentBlock, ManifestLoadError> {
        if let Some(block) = self.get_block(key, handle).await {
            return Ok(block);
        }
        let loaded = load.await?;
        self.insert_block(key.clone(), loaded.block.clone(), &loaded.stored);
        Ok(loaded.block)
    }
}

/// Checks that a wide read passes each block's handle with every get and
/// each block's stored bytes with every insert, and that a second read gets
/// each block once from the block cache and inserts nothing.
#[tokio::test]
async fn a_wide_read_passes_handles_and_stored_bytes_to_the_block_cache() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let object = segment_object_bytes(&store, &descriptor).await;
    let handles: HashMap<u64, BlockHandle> = index
        .iter()
        .map(|entry| (entry.block.offset, entry.block))
        .collect();
    let blocks = Arc::new(CountingBlockCache::default());
    let cache = MetadataSegmentCache::with_blocks(
        blocks.clone(),
        Arc::new(SharedSegmentBlocks::new(usize::MAX, usize::MAX)),
        ObjectStoreScope::new(0),
        crate::segment::cache::unshared_read_working_memory(),
        crate::limits::READAHEAD_WINDOW_BYTES,
    );

    let (cold, cold_reads) = wide_read(&store, Some(&cache), &descriptor, &index).await;
    assert_eq!(cold_reads, 1, "one ranged GET reads the whole span");
    let cold_gets = blocks.gets.lock().expect("gets lock").len();
    let inserts = blocks.inserts.lock().expect("inserts lock").clone();
    let inserted: BTreeSet<u64> = inserts.iter().map(|(offset, _)| *offset).collect();
    assert_eq!(inserted, handles.keys().copied().collect::<BTreeSet<_>>());
    for (offset, stored) in &inserts {
        assert_eq!(
            stored,
            &stored_block_bytes(&object, &handles[offset]),
            "the insert of the block at {offset} passes its stored bytes"
        );
    }

    let (warm, warm_reads) = wide_read(&store, Some(&cache), &descriptor, &index).await;
    assert_eq!((warm, warm_reads), (cold, 0));
    assert_eq!(
        blocks.inserts.lock().expect("inserts lock").len(),
        inserts.len()
    );
    let gets = blocks.gets.lock().expect("gets lock");
    assert_eq!(gets.len() - cold_gets, index.len());
    for (offset, handle) in gets.iter() {
        assert_eq!(
            handle, &handles[offset],
            "the get of the block at {offset} passes its handle"
        );
    }
}

/// Checks that when the block cache keeps nothing (its budget is zero), a
/// lookup's memo records the stored bytes of the read-ahead window's other
/// blocks, so the reads of those blocks within the same read send no GET
/// after the window's one.
#[tokio::test]
async fn a_read_ahead_window_stays_in_the_memo_when_the_block_cache_keeps_nothing() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let (expected, _) = wide_read(&store, None, &descriptor, &index).await;
    let cache = MetadataSegmentCache::unshared(0);
    let memo = SessionBlockMemo::default();
    store.reset();

    for position in 0..index.len() {
        let block = data_block_load::load_segment_data_block_with_readahead(
            &store,
            Some(&cache),
            Some(&memo),
            &descriptor,
            &index[position..],
        )
        .await
        .expect("a read of a block in the window");
        assert_eq!(block, expected[position]);
    }
    assert_eq!(cache.stats().cached_decoded_bytes, 0);
    assert_eq!(
        store.count(OperationSelector::Read),
        1,
        "one ranged GET reads the window, and the memo has the rest of it"
    );
}

#[tokio::test]
async fn checkpoint_delta_update_does_not_read_existing_metadata_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/first.txt", b"first\n", None)
        .await
        .expect("write first");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/second.txt", b"second\n", None)
        .await
        .expect("write second");
    store.reset();

    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create delta checkpoint");

    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "delta checkpoint update should use the WAL tail and copy existing metadata file refs"
    );
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_no)
            .await
            .expect("load checkpoint manifest");
    // One delta run per checkpoint, the first included.
    assert_eq!(delta_runs(&materialized.manifest).len(), 2);
}

#[tokio::test]
async fn two_loaded_views_share_one_read_working_pool_and_return_the_same_rows() {
    use crate::cache::ReadWorkingMemory;

    let (_root, store, descriptor) = checkpointed_direntry_segment().await;
    let namespace_id = &descriptor.owner_namespace_id;
    let manifest_number = current_manifest_number(&store, namespace_id).await;
    let cache = |pool| {
        MetadataSegmentCache::new(
            Arc::new(SharedSegmentBlocks::new(0, 0)),
            ObjectStoreScope::new(0),
            pool,
            crate::limits::READAHEAD_WINDOW_BYTES,
        )
    };
    let probe_pool = Arc::new(ReadWorkingMemory::new(64 * 1024 * 1024, None));
    let probe_cache = cache(Arc::clone(&probe_pool));
    let probe = load_manifest_segments_for_inspection(
        &store,
        Some(&probe_cache),
        namespace_id,
        &manifest_number,
    )
    .await
    .expect("probe view");
    let expected = probe
        .scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
        .await
        .expect("probe rows");
    let working_bytes = probe_pool.in_use();
    assert!(working_bytes > 0);
    drop(probe);
    assert_eq!(probe_pool.in_use(), 0);
    let pool = Arc::new(ReadWorkingMemory::new(working_bytes, None));
    let shared_cache = cache(Arc::clone(&pool));
    let first = load_manifest_segments_for_inspection(
        &store,
        Some(&shared_cache),
        namespace_id,
        &manifest_number,
    )
    .await
    .expect("first view");
    let second = load_manifest_segments_for_inspection(
        &store,
        Some(&shared_cache),
        namespace_id,
        &manifest_number,
    )
    .await
    .expect("second view");
    for view in [&first, &second] {
        assert_eq!(
            view.scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
                .await
                .expect("rows"),
            expected
        );
    }
    assert_eq!(pool.in_use(), working_bytes);
    store.reset();
    assert_eq!(
        first
            .scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
            .await
            .expect("first reread"),
        expected
    );
    assert_eq!(store.count(OperationSelector::Read), 0);
    store.reset();
    assert_eq!(
        second
            .scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
            .await
            .expect("second reread"),
        expected
    );
    assert!(store.count(OperationSelector::Read) > 0);
    drop(first);
    assert_eq!(pool.in_use(), 0);
    second
        .scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
        .await
        .expect("read after release");
    assert_eq!(pool.in_use(), working_bytes);
    store.reset();
    second
        .scan_prefix(ApiMetadataRowFamily::DirentryBinds, "")
        .await
        .expect("retained reread");
    assert_eq!(store.count(OperationSelector::Read), 0);
    drop(second);
    assert_eq!(pool.in_use(), 0);
}

async fn current_segment_refs<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<MetadataSegmentRef> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
        .payload()
        .runs
        .iter()
        .flat_map(|run| run.segments.iter().cloned())
        .collect()
}

/// The number of blocks that inserting `descriptors` adds to a segment cache:
/// each segment's index and filter blocks, and its data blocks, because the
/// fixture's segments are small enough to fetch whole.
async fn written_segment_blocks<S: ObjectStore + ?Sized>(
    store: &S,
    descriptors: &[MetadataSegmentRef],
) -> usize {
    let mut blocks = 0;
    for descriptor in descriptors {
        assert!(index_block_load::segment_object_bytes(descriptor) <= 256 * 1024);
        let index =
            index_block_load::load_segment_index_for_compaction(store, None, None, descriptor)
                .await
                .expect("load the segment index");
        blocks += 2 + index.len();
    }
    blocks
}

/// Checks that a compaction unit reads its input segments without the
/// segment cache, and inserts the blocks of the segments it writes and nothing
/// else.
#[tokio::test]
async fn compactions_read_input_without_the_cache_and_insert_only_their_output() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 0..4 {
        let path = format!("/docs/file-{index}.txt");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&path, b"file\n", None)
            .await
            .expect("write file");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold the tail");
    }
    let input: BTreeSet<String> = current_segment_refs(&store, &namespace_id)
        .await
        .iter()
        .map(metadata_segment_object_key)
        .collect();
    let cache = Arc::new(MetadataSegmentCache::unshared(usize::MAX));
    let outcome = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.segment_cache = Arc::clone(&cache);
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::CompactImmediately,
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run a compaction unit");
    assert_matches!(outcome, CompactionUnitOutcome::Published { .. });
    let output: Vec<_> = current_segment_refs(&store, &namespace_id)
        .await
        .into_iter()
        .filter(|descriptor| !input.contains(&metadata_segment_object_key(descriptor)))
        .collect();
    assert!(!output.is_empty(), "the compaction writes segments");
    let stats = cache.stats();
    assert_eq!(
        (stats.hits, stats.misses),
        (0, 0),
        "a compaction looks up no input block"
    );
    assert_eq!(
        stats.inserts,
        written_segment_blocks(&store, &output).await,
        "a compaction inserts its output's blocks and nothing else"
    );
}
