//! Checkpoint metadata cache, scan sharing, filters, and lookup behavior.

use super::*;
use loonfs_test_support::stores::replace_object;
use std::assert_matches;

async fn eight_files_one_row_per_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> ManifestNo {
    bootstrap_namespace(store, namespace_id, context)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(store, namespace_id, &path, b"file\n", context, None)
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

    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-bind-{:020}-", docs_inode_id.0);
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
    let root_inode_key = "inode-00000000000000000001";
    assert!(degenerate_segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, root_inode_key, root_inode_key)
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
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
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
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/tail.txt",
        b"tail\n",
        &context,
        None,
    )
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

#[tokio::test]
async fn segment_range_page_merges_base_and_delta_in_row_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/docs/a.txt", b"a\n", &context, None)
        .await
        .expect("write a");
    write_file_bytes(&store, &namespace_id, "/docs/c.txt", b"c\n", &context, None)
        .await
        .expect("write c");

    let policy = MetadataRunPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    write_file_bytes(&store, &namespace_id, "/docs/b.txt", b"b\n", &context, None)
        .await
        .expect("write b");
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    let manifest_number = current_manifest_number(&store, &namespace_id).await;
    let segments =
        super::load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_number)
            .await
            .expect("load segments");

    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-bind-{:020}-", docs_inode_id.0);
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
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/beta.txt",
        b"beta\n",
        &context,
        None,
    )
    .await
    .expect("write beta");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha\n",
        &context,
        None,
    )
    .await
    .expect("write alpha");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/gamma.txt",
        b"gamma\n",
        &context,
        None,
    )
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
            "direntry-bind-00000000000000000001-",
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
    let filter_probe = format!("direntry-bind-{:020}-{encoded_name}", docs_inode.0);
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
    assert!(
        stats.filter_skips >= 1,
        "the delta bind segment should be skipped by its filter, stats: {stats:?}"
    );
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
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
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
    store.reset();
    let key = "inode-00000000000000000001";
    assert!(segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, key, key)
        .await
        .expect("first lookup")
        .is_some());
    let first_lookup_gets = store.count(OperationSelector::Read);
    assert!(first_lookup_gets > 0, "a cold lookup fetches blocks");

    assert!(segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, key, key)
        .await
        .expect("repeated lookup")
        .is_some());
    let other = "inode-00000000000000000002";
    assert!(segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, other, other)
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
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
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

    let cache = MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(0)),
        ObjectStoreScope::new(0),
        Arc::new(crate::cache::ReadWorkingMemory::new(0, None)),
    );
    let read_view = super::load_manifest_segments_for_inspection(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_number,
    )
    .await
    .expect("read view");
    let fold_view = fold::load_manifest_projection(
        &store,
        &namespace_id,
        Some(&cache),
        crate::test_support::observed_now(),
    )
    .await
    .expect("fold view")
    .manifest_segments;
    let key = "inode-00000000000000000001";
    for segments in [&read_view, &fold_view] {
        store.reset();
        assert!(segments
            .get_for_lookup(ApiMetadataRowFamily::Inodes, key, key)
            .await
            .expect("first lookup")
            .is_some());
        let first_lookup_reads = store.count(OperationSelector::Read);
        assert!(segments
            .get_for_lookup(ApiMetadataRowFamily::Inodes, key, key)
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
            write_file_bytes(
                &store,
                &namespace_id,
                &format!("/docs/{name}"),
                b"content\n",
                &context,
                None,
            )
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
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
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

    let memo = super::load::SessionBlockMemo::default();
    super::load::load_segment_filter(&store, None, &memo, &descriptor)
        .await
        .expect("intact inline filter decodes");
    let error = super::load::load_segment_filter(
        &store,
        None,
        &super::load::SessionBlockMemo::default(),
        &tampered,
    )
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
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
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
    let memo = load::SessionBlockMemo::default();

    store.reset();
    load::load_segment_filter(&store, Some(&cache), &memo, &descriptor)
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
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
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
        &load::SessionBlockMemo::default(),
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
    let memo = load::SessionBlockMemo::default();
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
    let memo = load::SessionBlockMemo::default();
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
    let memo = load::SessionBlockMemo::default();
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

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/first.txt",
        b"first\n",
        &context,
        None,
    )
    .await
    .expect("write first");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create first checkpoint");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
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
            Arc::new(SharedSegmentBlocks::new(0)),
            ObjectStoreScope::new(0),
            pool,
        )
    };
    let probe_pool = Arc::new(ReadWorkingMemory::default());
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
    load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest")
        .state
        .envelope
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
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
        fold::fold_wal(&store, &namespace_id)
            .await
            .expect("fold the tail");
    }
    let input: BTreeSet<String> = current_segment_refs(&store, &namespace_id)
        .await
        .iter()
        .map(metadata_segment_object_key)
        .collect();
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let outcome = compact_unit(
        &store,
        Some(&cache),
        &namespace_id,
        loonfs_types::CompactorEpoch(0),
        MetadataRunPolicy::default(),
        MetadataCompactionPolicy::CompactImmediately,
        crate::manifest::CompactionAdmission::AnySize,
        &crate::manifest::MetadataCompactionCancellation::default(),
        crate::test_support::std_timer(),
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
