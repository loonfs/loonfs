//! Checkpoint metadata cache, scan sharing, filters, and lookup behavior.

use super::*;

/// Builds eight one-row segments and returns the reorganized manifest.
async fn eight_files_one_row_per_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> ManifestNo {
    bootstrap_namespace(store, namespace_id, context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(store, namespace_id, &path, b"file\n", context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    checkpoint_then_reorganize(store, namespace_id, context, policy).await
}

#[tokio::test]
async fn a_byte_budgeted_cache_admits_wide_scans_and_holds_to_its_budget() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    eight_files_one_row_per_segment(&store, &namespace_id, &context).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // The default cache config carries a decoded-byte budget, so a scan wider
    // than the small-scan limit populates the cache instead of reading through.
    let cache = super::MetadataSegmentCache::new(Default::default());
    let segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
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
    let fresh_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
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
        store.count(OperationClass::Read),
        0,
        "a warm wide scan should be served entirely from the cache"
    );
    assert!(after_repeat.hits > after_first.hits);

    // Scan the same cache through the range API.
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

    let fresh_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
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
        store.count(OperationClass::Read),
        0,
        "a warm range scan should be served entirely from the cache"
    );

    // A one-byte budget must still serve lookups while evicting entries.
    let degenerate = MetadataSegmentCache::new(MetadataSegmentCacheConfig {
        max_decoded_bytes: 1,
    });
    let degenerate_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&degenerate),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load segments against a degenerate budget");
    let root_inode_key = "inode-00000000000000000001";
    assert!(degenerate_segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, root_inode_key, root_inode_key)
        .await
        .expect("get inode")
        .is_some());
    let degenerate_stats = degenerate.stats();
    assert!(degenerate_stats.inserts > 0);
    assert!(degenerate_stats.evictions > 0);
}

#[tokio::test]
async fn concurrent_scans_share_one_fetch_per_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    eight_files_one_row_per_segment(&store, &namespace_id, &context).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // Concurrent scans over one shared cache must not multiply fetches:
    // single-flight covers blocks racing before the first insert lands, and
    // population covers everything after.
    let cache = super::MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    // A solo pass over its own cold cache measures the true per-scan
    // fetch count.
    let solo_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load solo segments");
    store.reset();
    let solo = solo_segments
        .scan_prefix(ApiMetadataRowFamily::Revisions, "revision-")
        .await
        .expect("solo scan");
    let solo_fetches = store.count(OperationClass::Read);
    assert!(solo.len() >= 8);
    assert!(solo_fetches >= 8, "solo scan should fetch every segment");

    // Concurrent requests race over a second cold cache, each with its own
    // segments view; single-flight is what keeps the pair at the solo count.
    let paired_cache = super::MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    let first_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first segments");
    let second_segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second segments");
    store.reset();
    let (first, second) = tokio::join!(
        first_segments.scan_prefix(ApiMetadataRowFamily::Revisions, "revision-"),
        second_segments.scan_prefix(ApiMetadataRowFamily::Revisions, "revision-"),
    );
    let paired_fetches = store.count(OperationClass::Read);
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
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;

    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    let first = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first segments");
    let second = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second segments");

    assert!(
        first.scan_runs.len() >= 2,
        "two checkpoints should leave more than one run to order"
    );
    assert_eq!(
        *first.scan_runs,
        runs_in_scan_order(&first.manifest().payload),
        "the cached run list must equal the manifest's scan-order grouping"
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
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/docs/a.txt", b"a\n", &context, None)
        .await
        .expect("write a");
    write_file_bytes(&store, &namespace_id, "/docs/c.txt", b"c\n", &context, None)
        .await
        .expect("write c");

    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    write_file_bytes(&store, &namespace_id, "/docs/b.txt", b"b\n", &context, None)
        .await
        .expect("write b");
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = super::load_verified_manifest_segments_with_cache(
        &store,
        None,
        &namespace_id,
        &manifest_object_id,
    )
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
            MetadataRow::DirentryBind { display_name, .. } => {
                Some(display_name.as_str().to_owned())
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(display_names, vec!["a.txt", "b.txt"]);
}

#[tokio::test]
async fn maintenance_materialization_does_not_populate_metadata_segment_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let manifest_no = eight_files_one_row_per_segment(&store, &namespace_id, &context).await;
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    let before = cache.stats();

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no)
            .await
            .expect("load materialized manifest");
    let after = cache.stats();

    assert!(flatten_manifest_segments(base_tier(&materialized.manifest)).len() > 1);
    assert_eq!(after, before);
}

#[tokio::test]
async fn lookup_skips_segments_whose_filter_rules_the_name_out() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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

    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
    let segments = super::load_verified_manifest_segments_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
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
            MetadataRow::DirentryBind { child_inode_id, .. } => Some(*child_inode_id),
            _ => None,
        })
        .expect("docs directory bind");
    let encoded_name = loonfs_api::wire::manifest::hex_encode_row_key_component("beta.txt");
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
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = {
        checkpoint_then_reorganize(
            &store,
            &namespace_id,
            &context,
            MetadataLsmPolicy::default(),
        )
        .await;
        current_manifest_object_id(&store, &namespace_id).await
    };

    // The cold-boot shape: a view with no shared cache attached. Repeating
    // a lookup must not re-fetch — the per-view memo is the only reuse this
    // configuration has, and without it a cold list degrades from one fetch
    // per block to one fetch per lookup.
    let segments =
        super::load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
            .await
            .expect("load segments");
    store.reset();
    let key = "inode-00000000000000000001";
    assert!(segments
        .get_for_lookup(ApiMetadataRowFamily::Inodes, key, key)
        .await
        .expect("first lookup")
        .is_some());
    let first_lookup_gets = store.count(OperationClass::Read);
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
        store.count(OperationClass::Read),
        first_lookup_gets,
        "later lookups through the same view should reuse decoded blocks"
    );
}

#[tokio::test]
async fn point_lookups_skip_inline_filtered_runs_without_fetches() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load segments");
    let direntry_descriptors: Vec<_> = segments
        .manifest()
        .payload
        .segments
        .iter()
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
        segments.manifest().payload.manifest_no,
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
    let prefix =
        lookup_keys::direntry_bind_prefix(binding.parent_inode_id, binding.name_key.as_str());
    let probe =
        lookup_keys::direntry_bind_probe(binding.parent_inode_id, binding.name_key.as_str());

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
        store.count(OperationClass::Read),
        1,
        "inline filters reject the other runs without fetches, and the one \
         admitted small segment loads whole with a single ranged GET"
    );

    // The same lookup against the same manifest with the inline copies
    // stripped must return the same rows through fetched filter blocks —
    // the inline copy is an accelerator, never an answer of its own.
    let mut stripped_payload = segments.manifest().payload.clone();
    stripped_payload.manifest_no = ManifestNo(stripped_payload.manifest_no.0 + 1);
    stripped_payload.manifest_object_id = ManifestObjectId::generate(stripped_payload.manifest_no);
    for descriptor in &mut stripped_payload.segments {
        descriptor.filter_inline = None;
    }
    let stripped_object_id = stripped_payload.manifest_object_id.clone();
    let stripped = NamespaceManifestEnvelope::from_payload(stripped_payload)
        .expect("stripped manifest envelope");
    write_namespace_manifest(&store, &stripped)
        .await
        .expect("write stripped manifest");
    let stripped_segments =
        load_verified_manifest_segments(&store, &namespace_id, &stripped_object_id)
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
        store.count(OperationClass::Read) > 1,
        "without inline copies the ruled-out runs pay filter fetches"
    );
}

#[tokio::test]
async fn corrupt_inline_filter_fails_the_lookup() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load segments");
    let descriptor = segments
        .manifest()
        .payload
        .segments
        .iter()
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
    assert!(
        matches!(error, ManifestLoadError::SegmentCodec { .. }),
        "unexpected error: {error:?}"
    );
}

/// One checkpointed direntry segment, with the inline filter copy stripped
/// from its descriptor so both the filter and the index have to come from
/// somewhere other than the manifest — the shape a base segment, whose
/// filter is too big to inline, already has.
async fn checkpointed_direntry_segment() -> (
    tempfile::TempDir,
    CountingStore<LocalFsStore>,
    MetadataSegmentRef,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load segments");
    let mut descriptor = segments
        .manifest()
        .payload
        .segments
        .iter()
        .find(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryBinds)
        .expect("a direntry segment")
        .clone();
    descriptor.filter_inline = None;
    (temp_dir, store, descriptor)
}

/// A decoded block cache with `blocks` beneath it, which is what a runtime
/// built with a local cache hands the read paths. A fresh one stands for a
/// fresh process: only the local tier carries anything over.
fn segment_cache_over(blocks: &Arc<RecordingStoredMetadataBlockCache>) -> MetadataSegmentCache {
    MetadataSegmentCache::with_stored_block_cache_and_observer(
        MetadataSegmentCacheConfig::default(),
        Some(Arc::clone(blocks) as Arc<dyn StoredMetadataBlockCache>),
        None,
    )
}

fn stored_key(
    descriptor: &MetadataSegmentRef,
    kind: StoredMetadataBlockKind,
    offset: u64,
) -> StoredMetadataBlockKey {
    StoredMetadataBlockKey {
        object_checksum: descriptor.object_checksum.clone(),
        kind,
        offset,
    }
}

/// Fills `blocks` the way one cold read fills it, and returns the index that
/// read decoded.
async fn warm_local_block_cache(
    store: &CountingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
    blocks: &Arc<RecordingStoredMetadataBlockCache>,
) -> Arc<Vec<SegmentIndexEntry>> {
    let cache = segment_cache_over(blocks);
    let memo = load::SessionBlockMemo::default();
    load::load_segment_filter(store, Some(&cache), &memo, descriptor)
        .await
        .expect("warm the filter");
    block_fetch::load_segment_index(store, Some(&cache), &memo, descriptor)
        .await
        .expect("warm the index")
}

#[tokio::test]
async fn a_cold_local_block_cache_takes_every_section_one_fetch_produced() {
    let (_temp_dir, store, descriptor) = checkpointed_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let cache = segment_cache_over(&blocks);
    let memo = load::SessionBlockMemo::default();

    store.reset();
    load::load_segment_filter(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("load filter");

    assert_eq!(
        blocks.calls().first(),
        Some(&RecordedStoredMetadataBlockCall::Get {
            key: stored_key(
                &descriptor,
                StoredMetadataBlockKind::Filter,
                descriptor.filter_block.offset
            ),
            hit: false,
        }),
        "a filter load probes the local cache before it asks the store"
    );
    assert_eq!(
        store.count(OperationClass::Read),
        1,
        "a cold filter load pays exactly one fetch"
    );

    // That fetch published the index into the view memo, so asking for it
    // costs nothing and tells this test which data blocks the segment holds.
    let index = block_fetch::load_segment_index(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("load index");
    assert_eq!(store.count(OperationClass::Read), 1);
    assert!(!index.is_empty(), "a segment holds at least one data block");

    let offered: Vec<_> = blocks
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            RecordedStoredMetadataBlockCall::Insert { key, .. } => Some(key),
            RecordedStoredMetadataBlockCall::Get { .. }
            | RecordedStoredMetadataBlockCall::Invalidate { .. } => None,
        })
        .collect();
    let mut expected = vec![
        stored_key(
            &descriptor,
            StoredMetadataBlockKind::Index,
            descriptor.index_block.offset,
        ),
        stored_key(
            &descriptor,
            StoredMetadataBlockKind::Filter,
            descriptor.filter_block.offset,
        ),
    ];
    expected.extend(index.iter().map(|entry| {
        stored_key(
            &descriptor,
            StoredMetadataBlockKind::Data,
            entry.block.offset,
        )
    }));
    for key in expected {
        assert!(
            offered.contains(&key),
            "the fetch did not offer {key:?} to the local cache"
        );
    }
}

#[tokio::test]
async fn a_warm_local_block_cache_answers_index_and_filter_without_the_store() {
    let (_temp_dir, store, descriptor) = checkpointed_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let cold_index = warm_local_block_cache(&store, &descriptor, &blocks).await;

    let cache = segment_cache_over(&blocks);
    let memo = load::SessionBlockMemo::default();
    store.reset();
    let warm_index = block_fetch::load_segment_index(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("index from the local cache");
    load::load_segment_filter(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("filter from the local cache");

    assert_eq!(warm_index, cold_index);
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "a warm index and filter should read no segment bytes"
    );
    for (kind, offset) in [
        (
            StoredMetadataBlockKind::Index,
            descriptor.index_block.offset,
        ),
        (
            StoredMetadataBlockKind::Filter,
            descriptor.filter_block.offset,
        ),
    ] {
        assert!(
            blocks
                .calls()
                .contains(&RecordedStoredMetadataBlockCall::Get {
                    key: stored_key(&descriptor, kind, offset),
                    hit: true,
                }),
            "the {kind:?} section should have been served by the local cache"
        );
    }
}

#[tokio::test]
async fn a_local_block_cache_entry_that_does_not_decode_is_dropped_and_refetched() {
    let (_temp_dir, store, descriptor) = checkpointed_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let cold_index = warm_local_block_cache(&store, &descriptor, &blocks).await;
    let index_key = stored_key(
        &descriptor,
        StoredMetadataBlockKind::Index,
        descriptor.index_block.offset,
    );
    blocks.corrupt(&index_key);

    let cache = segment_cache_over(&blocks);
    let memo = load::SessionBlockMemo::default();
    store.reset();
    let repaired = block_fetch::load_segment_index(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("a local entry that does not decode must not fail the read");

    assert_eq!(repaired, cold_index);
    assert_eq!(
        store.count(OperationClass::Read),
        1,
        "the bad entry costs one refetch and no retry loop"
    );
    assert_eq!(
        blocks
            .calls()
            .iter()
            .filter(|call| **call
                == RecordedStoredMetadataBlockCall::Invalidate {
                    key: index_key.clone()
                })
            .count(),
        1,
        "the entry that did not decode should have been dropped once"
    );
}

#[tokio::test]
async fn a_closed_local_block_cache_reads_as_a_miss() {
    let (_temp_dir, store, descriptor) = checkpointed_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let cold_index = warm_local_block_cache(&store, &descriptor, &blocks).await;
    blocks.close().await.expect("close the local cache");
    let calls_before = blocks.call_count();

    let cache = segment_cache_over(&blocks);
    let memo = load::SessionBlockMemo::default();
    store.reset();
    let index = block_fetch::load_segment_index(&store, Some(&cache), &memo, &descriptor)
        .await
        .expect("a closed local cache must not fail the read");

    assert_eq!(index, cold_index);
    assert_eq!(store.count(OperationClass::Read), 1);
    assert_eq!(
        blocks.call_count(),
        calls_before,
        "a closed cache neither serves nor records"
    );
}

/// One checkpointed direntry segment rebuilt with a one-byte block target,
/// so every row lands in its own data block. A handful of test rows would
/// otherwise fit in one block, and the coalescing span path only says
/// anything with several. Returns the segment's index, read with no caches,
/// so a test can drive the span path without an index load filling either
/// cache first.
async fn multi_block_direntry_segment() -> (
    tempfile::TempDir,
    CountingStore<LocalFsStore>,
    MetadataSegmentRef,
    Arc<Vec<SegmentIndexEntry>>,
) {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let segments = load_verified_manifest_segments(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load segments");
    let mut descriptor = segments
        .manifest()
        .payload
        .segments
        .iter()
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
    let built = builder.finish().expect("rebuilt segment");
    store
        .put_overwrite(
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
    descriptor.object_checksum = loonfs_api::sha256_digest(&built.bytes);
    // A one-byte target closes a block on every push, the last row
    // included, so this fixture carries the shape that used to drop a
    // segment's max key. Keyed scans prune on that key, so it has to name
    // the last row here as it would at any block size.
    assert_eq!(
        descriptor.max_row_key,
        rows.last()
            .expect("the segment holds rows")
            .row_key_for_family(descriptor.family),
        "the rebuilt descriptor must carry the segment's last row key"
    );

    let index = block_fetch::load_segment_index(
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
/// no caches of either kind.
async fn segment_rows(
    store: &CountingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
) -> Vec<MetadataRow> {
    let memo = load::SessionBlockMemo::default();
    let index = block_fetch::load_segment_index(store, None, &memo, descriptor)
        .await
        .expect("segment index");
    data_block_load::load_segment_data_block_span(store, None, &memo, descriptor, &index)
        .await
        .expect("segment data blocks")
        .iter()
        .flat_map(|block| block.rows.iter().cloned())
        .collect()
}

/// One segment object's whole body, which the seeding helpers slice.
async fn segment_object_bytes(
    store: &CountingStore<LocalFsStore>,
    descriptor: &MetadataSegmentRef,
) -> Bytes {
    store
        .get(&metadata_segment_object_key(descriptor), None)
        .await
        .expect("read segment object")
        .expect("segment object present")
}

fn stored_block_bytes(object: &Bytes, handle: &BlockHandle) -> Bytes {
    let start = handle.offset as usize;
    object.slice(start..start + handle.stored_len as usize)
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
            block_fetch::segment_block_cache_key(
                descriptor,
                MetadataSegmentBlockKind::Data,
                handle.offset,
            ),
            data_block_load::decoded_data_cache_block(descriptor.family, decoded),
        );
    }
}

/// Puts the named blocks into the local stored-block cache, as an earlier
/// process's reads would have left them.
fn seed_local_blocks(
    blocks: &RecordingStoredMetadataBlockCache,
    object: &Bytes,
    descriptor: &MetadataSegmentRef,
    index: &[SegmentIndexEntry],
    positions: &[usize],
) {
    for position in positions {
        let handle = index[*position].block;
        blocks.insert(
            stored_key(descriptor, StoredMetadataBlockKind::Data, handle.offset),
            stored_block_bytes(object, &handle),
        );
    }
}

/// One wide read over a whole segment through `cache`, with the per-view
/// memo a fresh request would carry, and the segment fetches it paid.
async fn wide_read(
    store: &CountingStore<LocalFsStore>,
    cache: Option<&MetadataSegmentCache>,
    descriptor: &MetadataSegmentRef,
    index: &[SegmentIndexEntry],
) -> (Vec<Arc<DecodedDataBlock>>, usize) {
    let memo = load::SessionBlockMemo::default();
    store.reset();
    let blocks =
        data_block_load::load_segment_data_block_span(store, cache, &memo, descriptor, index)
            .await
            .expect("wide read");
    (blocks, store.count(OperationClass::Read))
}

#[tokio::test]
async fn a_narrow_data_block_load_fills_the_local_cache_and_then_reads_from_it() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let entry = &index[0];
    let data_key = stored_key(
        &descriptor,
        StoredMetadataBlockKind::Data,
        entry.block.offset,
    );

    let cold_cache = segment_cache_over(&blocks);
    store.reset();
    let cold = data_block_load::load_segment_data_block(
        &store,
        Some(&cold_cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        entry,
    )
    .await
    .expect("cold data block load");

    assert_eq!(
        store.count(OperationClass::Read),
        1,
        "a cold data block costs exactly one fetch"
    );
    assert_eq!(
        blocks.calls(),
        vec![
            RecordedStoredMetadataBlockCall::Get {
                key: data_key.clone(),
                hit: false,
            },
            RecordedStoredMetadataBlockCall::Insert {
                key: data_key.clone(),
                bytes: entry.block.stored_len as usize,
            },
        ],
        "a cold load probes the local cache, misses, and offers what it fetched"
    );

    let warm_cache = segment_cache_over(&blocks);
    store.reset();
    let warm = data_block_load::load_segment_data_block(
        &store,
        Some(&warm_cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        entry,
    )
    .await
    .expect("warm data block load");

    assert_eq!(warm, cold);
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "a warm data block reads no segment bytes"
    );
    assert_eq!(
        blocks.calls().last(),
        Some(&RecordedStoredMetadataBlockCall::Get {
            key: data_key,
            hit: true,
        }),
        "the warm load was served by the local cache"
    );
}

#[tokio::test]
async fn a_wide_read_coalesces_the_blocks_the_decoded_cache_did_not_answer() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let (expected, _) = wide_read(&store, None, &descriptor, &index).await;

    let decoded = [0usize, 4];
    let cache = MetadataSegmentCache::new(MetadataSegmentCacheConfig::default());
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
async fn a_span_load_never_reads_or_writes_the_local_block_cache() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let (expected, _) = wide_read(&store, None, &descriptor, &index).await;

    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let object = segment_object_bytes(&store, &descriptor).await;
    let every_block: Vec<usize> = (0..index.len()).collect();
    seed_local_blocks(&blocks, &object, &descriptor, &index, &every_block);
    let calls_before = blocks.call_count();
    let cache = segment_cache_over(&blocks);

    let (read, gets) = wide_read(&store, Some(&cache), &descriptor, &index).await;

    assert_eq!(read, expected);
    assert_eq!(gets, 1, "the unresolved selection should stay one span");
    assert_eq!(
        blocks.call_count(),
        calls_before,
        "a span load must perform no local-cache gets or inserts"
    );
}

#[tokio::test]
async fn a_corrupt_local_entry_on_a_narrow_load_is_dropped_and_refetched() {
    let (_temp_dir, store, descriptor, index) = multi_block_direntry_segment().await;
    let blocks = Arc::new(RecordingStoredMetadataBlockCache::new());
    let object = segment_object_bytes(&store, &descriptor).await;
    let corrupt = 5usize;
    let entry = &index[corrupt];
    let expected = decode_data_block(&stored_block_bytes(&object, &entry.block), &entry.block)
        .expect("authoritative block should decode");
    seed_local_blocks(&blocks, &object, &descriptor, &index, &[corrupt]);
    let corrupt_key = stored_key(
        &descriptor,
        StoredMetadataBlockKind::Data,
        entry.block.offset,
    );
    blocks.corrupt(&corrupt_key);

    let cache = segment_cache_over(&blocks);
    let calls_before = blocks.call_count();
    store.reset();
    let read = data_block_load::load_segment_data_block(
        &store,
        Some(&cache),
        &load::SessionBlockMemo::default(),
        &descriptor,
        entry,
    )
    .await
    .expect("narrow load should recover from a corrupt local copy");
    let calls = blocks.calls();
    let during = &calls[calls_before..];

    assert_eq!(read.as_ref(), &expected);
    assert_eq!(
        store.count(OperationClass::Read),
        1,
        "the bad entry costs one point fetch and no retry loop"
    );
    assert_eq!(
        during,
        [
            RecordedStoredMetadataBlockCall::Get {
                key: corrupt_key.clone(),
                hit: true,
            },
            RecordedStoredMetadataBlockCall::Invalidate {
                key: corrupt_key.clone(),
            },
            RecordedStoredMetadataBlockCall::Insert {
                key: corrupt_key,
                bytes: entry.block.stored_len as usize,
            },
        ],
        "the narrow load should drop the bad copy and offer the fetched bytes"
    );
}

#[tokio::test]
async fn checkpoint_delta_update_does_not_read_existing_metadata_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
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
        store.count(OperationClass::Read),
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
