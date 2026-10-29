//! Listing ages and store requests made by a complete pass.

use super::*;
use loonfs_objectstore::keys::{content_blob, metadata_segment_object_key};
use loonfs_types::format::manifest::MetadataRowFamily;

/// Checks that a pass sends HEAD requests only for discovery's probe above
/// the hinted manifest and for the anchor's check of the current manifest's
/// successor.
#[tokio::test]
async fn a_pass_heads_only_the_discovery_probe_and_successor() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("no-heads").expect("namespace");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let successor = manifest
        .state
        .manifest()
        .manifest_no
        .successor()
        .expect("successor");
    let hinted = crate::test_support::namespace_store(&store, &namespace_id)
        .load_hint()
        .await
        .expect("hint")
        .state
        .manifest_no;
    let orphan = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    let recorded = RecordingStore::new(
        MetadataMapStore::aged(store, KeyPredicate::any()),
        KeyPredicate::any(),
    );
    let report = crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(None, &options(), &context(GRACE_MS + 1))
        .await
        .expect("pass");
    assert_eq!(report.deleted.content_objects, 1);
    let mut operations = recorded.snapshot();
    loonfs_test_support::stores::sort_head_runs(&mut operations);
    let heads: Vec<_> = operations
        .into_iter()
        .filter_map(|operation| match operation {
            loonfs_test_support::stores::RecordedOperation::Head { key } => Some(key),
            _ => None,
        })
        .collect();
    // The hint lags, so the probe finds the manifest after the hinted one,
    // and then probes one window above that.
    assert!(hinted < manifest.state.manifest().manifest_no);
    let window = u64::try_from(crate::limits::MANIFEST_PROBE_WINDOW).expect("a small window");
    let mut expected: Vec<_> = (hinted.0 + 1..=hinted.0 + 1 + window)
        .map(|number| metadata_manifest_object(&namespace_id, &ManifestNo(number)))
        .collect();
    expected.push(metadata_manifest_object(&namespace_id, &successor));
    assert_eq!(heads, expected);
    let target = NamespaceId::parse("retired-no-heads").expect("target");
    crate::test_support::NamespaceFixture::new(&recorded, &namespace_id, &context(1_000))
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    crate::test_support::NamespaceFixture::new(&recorded, &target, &context(1_000))
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete target");
    recorded.reset();
    let retired = crate::test_support::namespace_store(&recorded, &target)
        .gc(None, &options(), &context(1_000 + retirement_ms()))
        .await
        .expect("retire fork");
    assert_eq!(retired.deleted_checkpoints_by_owner.fork, 1);
    // The fork's hint lags at its first manifest, so discovery's probe finds
    // the manifest after it and probes one window above that, and a tombstone
    // needs no successor check.
    assert_eq!(
        recorded.counts().heads,
        1 + crate::limits::MANIFEST_PROBE_WINDOW
    );
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
    let undated_key = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    let aged_key = content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    for key in [&undated_key, &aged_key] {
        store
            .put_if_absent(key, Bytes::from_static(b"orphan"))
            .await
            .expect("orphan");
    }
    let aged = MetadataMapStore::aged(store, KeyPredicate::any());
    let undated = MetadataMapStore::without_last_modified(aged, KeyPredicate::exact(&undated_key));
    let recorded = RecordingStore::new(undated, KeyPredicate::any());
    let report = crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(None, &options(), &context(GRACE_MS + 1))
        .await
        .expect("pass");
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
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn superseded_views_add_segment_reads_only_for_uncovered_layout_segments() {
    use crate::manifest::CompactionUnitOutcome;

    for own_runs in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("layout-views").expect("namespace");
        let store = Arc::new(MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("namespace");
        for name in ["one", "two"] {
            write_test_file(
                &store,
                &namespace_id,
                &format!("/docs/{name}"),
                name,
                &context(1_000),
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
        if own_runs {
            for _ in 0..16 {
                let outcome = crate::test_support::NamespaceFixture::new(
                    &store,
                    &namespace_id,
                    &crate::test_support::mutation_context("test-writer", 0),
                )
                .engine()
                .compact_metadata_unit(
                    None,
                    MetadataCompactionPolicy::CompactImmediately,
                    crate::manifest::CompactionAdmission::AnySize,
                    loonfs_types::CompactorEpoch(0),
                    &crate::manifest::MetadataCompactionCancellation::default(),
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
                .acquire_writer_epoch(&context(1_000))
                .await
                .expect("supersede");
        }
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("current");
        assert_eq!(
            layout_segments(&previous) != layout_segments(&current),
            own_runs
        );
        let now_ms = GRACE_MS + 1;
        let recent = MetadataMapStore::new(
            Arc::clone(&store),
            KeyPredicate::exact(&current.object_key),
            move |mut meta| {
                meta.last_modified_at_ms = Some(now_ms);
                meta
            },
        );
        let rooted = RecordingStore::metadata_segments(recent);
        crate::test_support::namespace_store(&rooted, &namespace_id)
            .gc(None, &options(), &context(now_ms))
            .await
            .expect("root predecessor");
        // Reads within one pass have no fixed order, so both lists are
        // compared sorted.
        let mut rooted_reads = rooted.take_get_keys();
        rooted_reads.sort();
        let current_only = RecordingStore::metadata_segments(Arc::clone(&store));
        crate::test_support::namespace_store(&current_only, &namespace_id)
            .gc(None, &options(), &context(now_ms))
            .await
            .expect("current only");
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

/// Checks that a pass reads each layout segment once in each of its two layout
/// scans, when every rooted manifest adds one delta run over a base that the
/// current manifest no longer lists, and that it loads those manifests with
/// more than one read and at most `STORE_READS_IN_FLIGHT` reads in flight.
#[tokio::test]
async fn a_pass_reads_each_layout_segment_once_per_scan_however_many_roots_list_it() {
    use crate::manifest::CompactionUnitOutcome;

    const DELTA_RUNS: usize = 8;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("shared-layout-base").expect("namespace");
    let store = RecordingStore::metadata_segments(ConcurrencyWatchStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    ));
    let setup_store = store.inner().inner();
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(setup_store, &namespace_id, &setup)
        .create()
        .await
        .expect("namespace");
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
        crate::test_support::namespace_store(setup_store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold");
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
    while !matches!(
        crate::test_support::NamespaceFixture::new(
            setup_store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 0)
        )
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::CompactImmediately,
            crate::manifest::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::manifest::MetadataCompactionCancellation::default()
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
    // Every successor is younger than the grace, so every manifest is a root.
    let now_ms = now_after_newest_object(setup_store, &namespace_id, 1).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(now_ms))
        .await
        .expect("pass");

    // Without a segment cache, each read of a small segment is one GET. A
    // merge per view would read the base once for each of the eight views
    // that list it.
    let mut reads = std::collections::BTreeMap::<String, usize>::new();
    for key in store.take_get_keys() {
        *reads.entry(key).or_default() += 1;
    }
    assert_eq!(reads.keys().cloned().collect::<BTreeSet<_>>(), segments);
    assert!(reads.values().all(|&reads| reads == 2), "{reads:?}");
    let manifest_reads = store.inner().reads();
    assert!(manifest_reads.peak_in_flight > 1);
    assert!(manifest_reads.peak_in_flight <= STORE_READS_IN_FLIGHT);
}
