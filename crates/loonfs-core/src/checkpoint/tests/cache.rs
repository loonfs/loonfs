//! Checkpoint metadata cache, scan sharing, filters, and lookup behavior.

use super::*;

#[tokio::test]
async fn byte_budgeted_cache_admits_large_table_scans() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // The default cache config carries a decoded-byte budget, so a scan wider
    // than the small-scan limit populates the cache instead of reading through.
    let cache = super::MetadataTableCache::new(Default::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    let revisions = tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
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
    let fresh_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load fresh tables");
    store.reset();
    let repeated = fresh_tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
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
}

#[tokio::test]
async fn concurrent_scans_share_one_fetch_per_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    // Concurrent scans over one shared cache must not multiply fetches:
    // single-flight covers blocks racing before the first insert lands, and
    // population covers everything after.
    let cache = super::MetadataTableCache::new(MetadataTableCacheConfig::default());
    // A solo pass over its own cold cache measures the true per-scan
    // fetch count.
    let solo_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load solo tables");
    store.reset();
    let solo = solo_tables
        .scan_prefix(ApiMetadataTableFamily::Revisions, "revision-")
        .await
        .expect("solo scan");
    let solo_fetches = store.count(OperationClass::Read);
    assert!(solo.len() >= 8);
    assert!(solo_fetches >= 8, "solo scan should fetch every segment");

    // Concurrent requests race over a second cold cache, each with its own
    // tables view; single-flight is what keeps the pair at the solo count.
    let paired_cache = super::MetadataTableCache::new(MetadataTableCacheConfig::default());
    let first_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first tables");
    let second_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&paired_cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second tables");
    store.reset();
    let (first, second) = tokio::join!(
        first_tables.scan_prefix(ApiMetadataTableFamily::Revisions, "revision-"),
        second_tables.scan_prefix(ApiMetadataTableFamily::Revisions, "revision-"),
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

    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let first = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load first tables");
    let second = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load second tables");

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
async fn table_range_page_merges_base_and_l0_in_row_key_order() {
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
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        None,
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-{:020}-", docs_inode_id.0);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let page = tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
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
async fn byte_budgeted_cache_admits_large_range_scans() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = super::MetadataTableCache::new(Default::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    // The list preload path: one page-shaped range scan over a directory
    // whose bind rows span more segments than the small-scan limit.
    let docs_inode_id = InodeId(2);
    let lower_bound = format!("direntry-{:020}-", docs_inode_id.0);
    let upper_bound = super::string_prefix_upper_bound(&lower_bound);
    let page = tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("scan range page");
    let after_first = cache.stats();
    assert_eq!(page.len(), 8);
    assert!(
        after_first.inserts > 4,
        "a wide range scan should admit its segments to the cache"
    );

    let fresh_tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load fresh tables");
    store.reset();
    let repeated = fresh_tables
        .scan_range_page(
            ApiMetadataTableFamily::DirentryBinds,
            &lower_bound,
            upper_bound.as_deref(),
            8,
        )
        .await
        .expect("repeated scan range page");

    assert_eq!(repeated, page);
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "a warm range scan should be served entirely from the cache"
    );
}

#[tokio::test]
async fn maintenance_materialization_does_not_populate_metadata_table_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..8 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"file\n", &context, None)
            .await
            .expect("write file");
    }
    let policy = MetadataLsmPolicy {
        max_rows_per_segment: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    let manifest_id = checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let before = cache.stats();

    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_id)
            .await
            .expect("load materialized manifest");
    let after = cache.stats();

    assert!(flatten_manifest_tables(base_run(&materialized.manifest).tables).len() > 1);
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
    // "gamma" in an L0 run. The L0 bind segment's key range then straddles
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
    let cache = MetadataTableCache::new(MetadataTableCacheConfig::default());
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    // Resolve /docs, then look up beta's bind under it through the filtered
    // scan the visibility adapter uses.
    let docs_binds = tables
        .scan_prefix(
            ApiMetadataTableFamily::DirentryBinds,
            "direntry-00000000000000000001-",
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
    let filter_probe = format!("direntry-{:020}-{encoded_name}", docs_inode.0);
    let prefix = format!("{filter_probe}-");
    let rows = tables
        .scan_prefix_for_lookup(
            ApiMetadataTableFamily::DirentryBinds,
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
        "the L0 bind segment should be skipped by its filter, stats: {stats:?}"
    );
}

#[tokio::test]
async fn metadata_cache_budget_counts_decoded_blocks() {
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
        "/docs/file.txt",
        b"file\n",
        &context,
        None,
    )
    .await
    .expect("write file");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    let manifest_object_id = current_manifest_object_id(&store, &namespace_id).await;
    let cache = MetadataTableCache::new(MetadataTableCacheConfig {
        max_decoded_bytes: 1,
    });
    let tables = super::load_verified_manifest_tables_with_cache(
        &store,
        Some(&cache),
        &namespace_id,
        &manifest_object_id,
    )
    .await
    .expect("load tables");

    let key = "inode-00000000000000000001";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
        .await
        .expect("get inode")
        .is_some());

    let stats = cache.stats();
    assert!(stats.inserts > 0);
    assert!(stats.evictions > 0);
}

#[tokio::test]
async fn a_view_reuses_decoded_blocks_without_a_shared_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
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
    let tables = super::load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    store.reset();
    let key = "inode-00000000000000000001";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
        .await
        .expect("first lookup")
        .is_some());
    let first_lookup_gets = store.count(OperationClass::Read);
    assert!(first_lookup_gets > 0, "a cold lookup fetches blocks");

    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, key, key)
        .await
        .expect("repeated lookup")
        .is_some());
    let other = "inode-00000000000000000002";
    assert!(tables
        .get_for_lookup(ApiMetadataTableFamily::Inodes, other, other)
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
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // Three checkpoints append three L0 runs whose direntry key ranges
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
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    let direntry_descriptors: Vec<_> = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| descriptor.family == ApiMetadataTableFamily::DirentryBinds)
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
        tables.manifest().payload.manifest_id,
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
    let rows = tables
        .scan_prefix_for_lookup(
            ApiMetadataTableFamily::DirentryBinds,
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
    let mut stripped_payload = tables.manifest().payload.clone();
    stripped_payload.manifest_id = ManifestId(stripped_payload.manifest_id.0 + 1);
    stripped_payload.manifest_object_id = ManifestObjectId::generate(stripped_payload.manifest_id);
    for descriptor in &mut stripped_payload.metadata_files {
        descriptor.filter_inline = None;
    }
    let stripped_object_id = stripped_payload.manifest_object_id.clone();
    let stripped = NamespaceManifestEnvelope::from_payload(stripped_payload)
        .expect("stripped manifest envelope");
    write_namespace_manifest(&store, &stripped)
        .await
        .expect("write stripped manifest");
    let stripped_tables = load_verified_manifest_tables(&store, &namespace_id, &stripped_object_id)
        .await
        .expect("load stripped tables");
    store.reset();
    let stripped_rows = stripped_tables
        .scan_prefix_for_lookup(
            ApiMetadataTableFamily::DirentryBinds,
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
    let tables = load_verified_manifest_tables(&store, &namespace_id, &manifest_object_id)
        .await
        .expect("load tables");
    let descriptor = tables
        .manifest()
        .payload
        .metadata_files
        .iter()
        .find(|descriptor| {
            descriptor.family == ApiMetadataTableFamily::DirentryBinds
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

#[tokio::test]
async fn checkpoint_l0_update_does_not_read_existing_metadata_ssts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
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
        .expect("create L0 checkpoint");

    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "L0 checkpoint update should use the WAL tail and copy existing metadata file refs"
    );
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load checkpoint manifest");
    // One L0 run per checkpoint, the first included.
    assert_eq!(l0_runs(&materialized.manifest).len(), 2);
}
