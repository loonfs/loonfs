//! Listing ages and store requests made by a complete collection.

use super::*;
use loonfs_objectstore::keys::{content_blob, metadata_segment_object_key};
use loonfs_types::format::manifest::MetadataRowFamily;

/// Checks that a namespace's collection sends no HEAD request and reads no
/// WAL object, and that a pool's collection sends one HEAD request, for the
/// anchor's check of the member's current manifest's successor. Both find the
/// newest manifest with a listing.
#[tokio::test]
async fn only_a_pool_collection_heads_a_successor() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("no-heads").expect("namespace");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let successor = manifest
        .manifest()
        .manifest_no
        .successor()
        .expect("successor");
    let pool_id = crate::test_support::load_pool_id(&store, &namespace_id).await;
    let orphan = content_blob(&pool_id, &numbered_content_id(3));
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    let recorded = RecordingStore::new(
        MetadataMapStore::aged(store, KeyPredicate::any()),
        KeyPredicate::any(),
    );
    let heads = || {
        let mut operations = recorded.snapshot();
        loonfs_test_support::stores::sort_head_runs(&mut operations);
        operations
            .into_iter()
            .filter_map(|operation| match operation {
                loonfs_test_support::stores::RecordedOperation::Head { key } => Some(key),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(&context(GC_GRACE_WINDOW_MS + 1).clock())
        .await
        .expect("collection");
    assert_eq!(heads(), Vec::<String>::new());
    assert_eq!(wal_reads(&recorded), Vec::<String>::new());
    recorded.reset();
    let report = collect_pool(&recorded, &pool_id, &context(GC_GRACE_WINDOW_MS + 1)).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert_eq!(
        heads(),
        vec![metadata_manifest_object(&namespace_id, &successor)]
    );
    let target = NamespaceId::parse("retired-no-heads").expect("target");
    let fixture =
        crate::test_support::NamespaceFixture::new(&recorded, &namespace_id, &context(1_000));
    fixture
        .engine()
        .fork_namespace(
            fixture
                .engine()
                .find_fork_target(&target)
                .await
                .expect("find the fork target"),
            &loonfs_test_support::test_actor(),
            crate::test_support::fold_before_fork(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    crate::test_support::NamespaceFixture::new(&recorded, &target, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete target");
    recorded.reset();
    let retired = crate::test_support::namespace_store(&recorded, &target)
        .gc(&context(1_000 + GC_GRACE_WINDOW_MS).clock())
        .await
        .expect("retire fork");
    assert_eq!(retired.deleted_snapshots_by_owner.fork, 2);
    // A tombstone needs no successor check.
    assert_eq!(recorded.counts().heads, 0);
}

/// Returns the keys of the WAL objects that `recorded` read.
fn wal_reads<S: ObjectStore>(recorded: &RecordingStore<S>) -> Vec<String> {
    recorded
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            loonfs_test_support::stores::RecordedOperation::Get { key, .. }
            | loonfs_test_support::stores::RecordedOperation::GetWithMetadata { key, .. } => {
                Some(key)
            }
            _ => None,
        })
        .filter(|key| loonfs_objectstore::layout::wal_no_of(key).is_some())
        .collect()
}

/// Checks that a collection of an active namespace with an unfolded WAL tail
/// reads no WAL object and sends no HEAD request, and keeps every WAL object
/// above the newest manifest's `folded_wal_no` and every segment that the
/// newest manifest lists.
#[tokio::test]
async fn a_collection_keeps_an_unfolded_tail_without_reading_it() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("unfolded-tail").expect("namespace");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    let setup = context(1_000);
    for name in ["three", "four", "five"] {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/docs/{name}.txt"),
            &format!("gc-{name}"),
            &setup,
        )
        .await;
    }
    let folded_wal_no = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest")
        .payload()
        .folded_wal_no;
    let wal_keys = store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("list the WAL");
    let tail: Vec<_> = wal_keys
        .into_iter()
        .filter(|key| {
            loonfs_objectstore::layout::wal_no_of(key).is_some_and(|number| number > folded_wal_no)
        })
        .collect();
    assert!(tail.len() >= 3, "the setup leaves an unfolded tail");
    let segments =
        crate::manifest::tests::segment_keys_of_the_current_manifest(&store, &namespace_id).await;
    assert!(!segments.is_empty());

    let recorded = RecordingStore::new(
        MetadataMapStore::aged(store, KeyPredicate::any()),
        KeyPredicate::any(),
    );
    crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(&context(GC_GRACE_WINDOW_MS + 1).clock())
        .await
        .expect("collection");
    assert_eq!(recorded.counts().heads, 0);
    assert_eq!(wal_reads(&recorded), Vec::<String>::new());
    for key in tail.iter().chain(&segments) {
        assert!(
            recorded.inner().head(key).await.expect("head").is_some(),
            "the collection keeps `{key}`"
        );
    }
}

/// Checks that a fold published after a collection's find, and before the
/// collection lists anything, loses nothing: the collection keeps the WAL
/// objects that the fold moved into the new manifest, and the segments that
/// the new manifest lists.
#[tokio::test]
async fn a_fold_during_a_collection_keeps_what_the_new_manifest_needs() {
    let directory = tempdir().expect("directory");
    let seed = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("fold-during-gc").expect("namespace");
    folded_namespace_with_two_files(&seed, &namespace_id).await;
    let setup = context(1_000);
    for name in ["three", "four", "five"] {
        write_test_file(
            &seed,
            &namespace_id,
            &format!("/docs/{name}.txt"),
            &format!("gc-{name}"),
            &setup,
        )
        .await;
    }
    let found = crate::test_support::namespace_store(&seed, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let moved: Vec<_> = seed
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("list the WAL")
        .into_iter()
        .filter(|key| {
            loonfs_objectstore::layout::wal_no_of(key)
                .is_some_and(|number| number > found.payload().folded_wal_no)
        })
        .collect();
    assert!(moved.len() >= 3, "the setup leaves an unfolded tail");
    // Every object that the setup wrote is past the grace window for the
    // collection, and every object that the fold writes is younger than the
    // grace window.
    let clock =
        context(now_after_newest_object(&seed, &namespace_id, GC_GRACE_WINDOW_MS + 1).await);

    // The collection parks at its snapshot listing, which is after its find
    // and before every listing of a candidate family.
    let gated = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::List,
    );
    gated.block_next();
    let namespace = crate::test_support::namespace_store(&gated, &namespace_id);
    let collection_clock = clock.clock();
    let (report, segments) = tokio::join!(namespace.gc(&collection_clock), async {
        gated.wait_until_blocked().await;
        crate::test_support::namespace_store(&seed, &namespace_id)
            .fold_wal()
            .await
            .expect("fold the tail");
        let segments =
            crate::manifest::tests::segment_keys_of_the_current_manifest(&seed, &namespace_id)
                .await;
        gated.release();
        segments
    });
    report.expect("the collection must finish");
    let folded = crate::test_support::namespace_store(&seed, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    assert!(folded.manifest().manifest_no > found.manifest().manifest_no);
    assert!(moved
        .iter()
        .all(|key| loonfs_objectstore::layout::wal_no_of(key)
            .is_some_and(|number| number <= folded.payload().folded_wal_no)));
    for key in moved
        .iter()
        .chain(&segments)
        .chain([&found.object_key, &folded.object_key])
    {
        assert!(
            seed.head(key).await.expect("head").is_some(),
            "the collection keeps `{key}`"
        );
    }
}

#[tokio::test]
async fn listed_times_retain_undated_content_and_delete_aged_content() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("listed-ages").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
        .create()
        .await
        .expect("namespace");
    let pool_id = crate::test_support::load_pool_id(&store, &namespace_id).await;
    let undated_key = content_blob(&pool_id, &loonfs_types::ContentId::generate());
    let aged_key = content_blob(&pool_id, &loonfs_types::ContentId::generate());
    for key in [&undated_key, &aged_key] {
        store
            .put_if_absent(key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
    }
    let aged = MetadataMapStore::aged(store, KeyPredicate::any());
    let undated = MetadataMapStore::without_last_modified(aged, KeyPredicate::exact(&undated_key));
    let recorded = RecordingStore::new(undated, KeyPredicate::any());
    let report = collect_pool(&recorded, &pool_id, &context(GC_GRACE_WINDOW_MS + 1)).await;
    assert_eq!(report.retained.no_provider_timestamp, 1);
    assert_eq!(report.deleted.content_objects, 1);
    let deleted: Vec<_> = recorded
        .snapshot()
        .into_iter()
        .filter_map(|op| match op {
            loonfs_test_support::stores::RecordedOperation::Delete { key } => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(deleted, [aged_key]);
    assert!(recorded
        .inner()
        .head(&undated_key)
        .await
        .expect("head retained")
        .is_some());
}

fn layout_segments(manifest: &crate::namespace::control::LoadedManifest) -> BTreeSet<String> {
    manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .map(metadata_segment_object_key)
        .collect()
}

/// Checks that a pool member's snapshot view on an earlier manifest adds extent filter reads
/// only for the layout segments that the current manifest doesn't list: a
/// layout compaction gives the snapshot's manifest layout segments of its own,
/// and a writer claim leaves the layout segments as they were.
#[tokio::test]
async fn snapshot_views_add_segment_reads_only_for_uncovered_layout_segments() {
    use crate::compaction::CompactionUnitOutcome;

    for own_runs in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("layout-views").expect("namespace");
        let store = Arc::new(MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let setup = context(1_000);
        let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("namespace")
            .pool_id;
        for name in ["one", "two"] {
            write_test_file(
                &store,
                &namespace_id,
                &format!("/docs/{name}"),
                name,
                &setup,
            )
            .await;
            crate::test_support::namespace_store(&store, &namespace_id)
                .fold_wal()
                .await
                .expect("fold");
        }
        let previous = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("previous");
        let snapshot = create_snapshot(&store, &namespace_id, &setup)
            .await
            .expect("take a snapshot of the previous manifest");
        if own_runs {
            let fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            )
            .with_run_policy(crate::test_support::one_delta_run_policy());
            for _ in 0..16 {
                let outcome = fixture
                    .engine()
                    .compact_metadata_unit(
                        None,
                        loonfs_types::CompactorEpoch(0),
                        &crate::compaction::MetadataCompactionCancellation::default(),
                    )
                    .await
                    .expect("compact");
                if matches!(
                    outcome,
                    CompactionUnitOutcome::Published {
                        group: loonfs_types::MetadataFamilyGroup::ContentLayouts,
                        ..
                    }
                ) {
                    break;
                }
            }
        } else {
            crate::test_support::namespace_store(&store, &namespace_id)
                .acquire_writer_epoch(&setup.writer_id)
                .await
                .expect("replace the previous manifest");
        }
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current");
        assert!(current.manifest().manifest_no > previous.manifest().manifest_no);
        assert_eq!(
            layout_segments(&previous) != layout_segments(&current),
            own_runs
        );
        let now_ms = GC_GRACE_WINDOW_MS + 1;
        let rooted = RecordingStore::metadata_segments(Arc::clone(&store));
        collect_pool(&rooted, &pool_id, &context(now_ms)).await;
        // Reads within one collection have no fixed order, so both lists are
        // compared sorted.
        let mut rooted_reads = rooted.take_get_keys();
        rooted_reads.sort();
        crate::test_support::namespace_store(&store, &namespace_id)
            .delete_snapshot_record(&snapshot.snapshot_id)
            .await
            .expect("release the snapshot");
        let current_only = RecordingStore::metadata_segments(Arc::clone(&store));
        collect_pool(&current_only, &pool_id, &context(now_ms)).await;
        let mut current_reads = current_only.take_get_keys();
        current_reads.sort();
        if own_runs {
            assert!(rooted_reads.len() > current_reads.len());
            assert!(layout_segments(&previous)
                .iter()
                .all(|key| rooted_reads.contains(key) && !current_reads.contains(key)));
        } else {
            assert_eq!(rooted_reads, current_reads);
        }
        assert!(current_reads
            .iter()
            .all(|key| layout_segments(&current).contains(key)));
    }
}

/// Checks that a pool's collection reads the extent filter of each layout segment once,
/// when every snapshot view adds one delta run over a base that the current
/// manifest no longer lists, and that it loads those snapshots' manifests with
/// more than one read and at most `STORE_READS_IN_FLIGHT` reads in flight.
#[tokio::test]
async fn a_collection_reads_each_extent_filter_once_however_many_snapshot_views_list_its_segment() {
    use crate::compaction::CompactionUnitOutcome;

    const DELTA_RUNS: usize = 8;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("shared-layout-base").expect("namespace");
    let store = RecordingStore::metadata_segments(ConcurrencyWatchStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    ));
    let setup_store = store.inner().inner();
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(setup_store, &namespace_id, &setup)
        .create()
        .await
        .expect("namespace")
        .pool_id;
    let mut segments = BTreeSet::new();
    let mut base = BTreeSet::new();
    for index in 0..=DELTA_RUNS {
        let path = format!("/docs/{index}");
        write_test_file(
            setup_store,
            &namespace_id,
            &path,
            &format!("file-{index}"),
            &setup,
        )
        .await;
        create_snapshot(setup_store, &namespace_id, &setup)
            .await
            .expect("fold and take a snapshot");
        let folded = crate::test_support::namespace_store(setup_store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("folded manifest");
        if index == 0 {
            base = layout_segments(&folded);
        }
        segments.extend(layout_segments(&folded));
    }
    assert_eq!((base.len(), segments.len()), (1, DELTA_RUNS + 1));
    // Compacting the layouts replaces the base, so each fold's manifest except
    // the last adds a layout segment that no earlier root lists.
    let fixture = crate::test_support::NamespaceFixture::new(
        setup_store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .with_run_policy(crate::test_support::one_delta_run_policy());
    while !matches!(
        fixture
            .engine()
            .compact_metadata_unit(
                None,
                loonfs_types::CompactorEpoch(0),
                &crate::compaction::MetadataCompactionCancellation::default()
            )
            .await
            .expect("compact"),
        CompactionUnitOutcome::Published {
            group: loonfs_types::MetadataFamilyGroup::ContentLayouts,
            ..
        }
    ) {}
    let current = crate::test_support::namespace_store(setup_store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("compacted manifest");
    assert!(layout_segments(&current).is_disjoint(&base));
    segments.extend(layout_segments(&current));
    // A snapshot keeps each fold's manifest, so every manifest is a root.
    let now_ms = now_after_newest_object(setup_store, &namespace_id, 1).await;
    collect_pool(&store, &pool_id, &context(now_ms)).await;

    // Each extent filter read is one GET. A read per view would read the
    // base's filter once for each of the eight views that list it.
    let mut reads = std::collections::BTreeMap::<String, usize>::new();
    for key in store.take_get_keys() {
        *reads.entry(key).or_default() += 1;
    }
    assert_eq!(reads.keys().cloned().collect::<BTreeSet<_>>(), segments);
    assert!(reads.values().all(|&reads| reads == 1), "{reads:?}");
    let manifest_reads = store.inner().reads();
    assert!(manifest_reads.peak_in_flight > 1);
    assert!(manifest_reads.peak_in_flight <= STORE_READS_IN_FLIGHT);
}
