//! Replaced manifests: a manifest that is neither current nor kept by a snapshot, and the
//! segments only it lists, go by age alone.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::compaction::CompactionUnitOutcome;
use crate::error::MetadataProjectionLoadError;
use crate::manifest::tests::{dropped_segments, segment_keys};
use crate::manifest::ManifestLoadError;
use crate::namespace::control::LoadedManifest;
use loonfs_types::MetadataFamilyGroup;

/// A time strictly past the grace window for every object that reports an
/// ancient age.
const NOW_MS: u64 = GC_GRACE_WINDOW_MS + 1;

/// Writes and folds two files over `store`, whose objects all report an
/// ancient age, then collects every manifest before the current one, and
/// returns the current manifest.
async fn seed_segments<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> LoadedManifest {
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for name in ["one", "two"] {
        write_test_file(store, namespace_id, &format!("/docs/{name}"), name, &setup).await;
        crate::test_support::namespace_store(store, namespace_id)
            .fold_wal()
            .await
            .expect("fold");
    }
    assert!(store
        .list_prefix(&snapshot_prefix(namespace_id))
        .await
        .expect("list snapshots")
        .is_empty());
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    namespace
        .gc(&context(NOW_MS).clock())
        .await
        .expect("collect the earlier manifests");
    let previous = namespace
        .load_current_manifest()
        .await
        .expect("previous manifest");
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(namespace_id))
            .await
            .expect("list manifests"),
        std::slice::from_ref(&previous.object_key)
    );
    previous
}

async fn compact_bindings<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> LoadedManifest {
    let outcome = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .with_run_policy(crate::test_support::one_delta_run_policy())
    .engine()
    .compact_metadata_unit(
        None,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("compact bindings");
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published {
            group: MetadataFamilyGroup::Bindings,
            ..
        }
    );
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    namespace
        .load_current_manifest()
        .await
        .expect("compacted manifest")
}

async fn assert_objects_exist<S: ObjectStore>(store: &S, keys: &BTreeSet<String>, exists: bool) {
    for key in keys {
        assert_eq!(
            store.head(key).await.expect("head object").is_some(),
            exists,
            "{key}"
        );
    }
}

async fn exists<S: ObjectStore>(store: &S, key: &str) -> bool {
    store.head(key).await.expect("head object").is_some()
}

/// Checks that the first collection after a bindings compaction deletes the
/// replaced manifest and every segment that only it lists, once both are
/// older than their grace windows, and keeps the current manifest and its
/// segments, so a lazy read through a view on the replaced manifest finds a
/// segment missing.
#[tokio::test]
async fn a_replaced_manifest_and_its_own_segments_go_at_the_first_pass_once_old_enough() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("replaced").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let previous = seed_segments(&store, &namespace_id).await;
    // Load without shared caches and leave path reads until after the
    // collection, so resolving /docs/one needs the bindings that the
    // compaction drops.
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("retain the previous view");
    let current = compact_bindings(&store, &namespace_id).await;
    assert_eq!(
        current.manifest().manifest_no.0,
        previous.manifest().manifest_no.0 + 1
    );
    let current_segments = segment_keys(&current);
    let dropped = dropped_segments(&previous, &current);

    // The current manifest is seconds old, and every other object reports
    // an ancient age.
    let recent = MetadataMapStore::new(
        &store,
        KeyPredicate::exact(&current.object_key),
        |mut metadata| {
            metadata.last_modified_at_ms = Some(NOW_MS - 1_000);
            metadata
        },
    );
    let report = crate::test_support::namespace_store(&recent, &namespace_id)
        .gc(&context(NOW_MS).clock())
        .await
        .expect("collection");
    assert!(!exists(&store, &previous.object_key).await);
    assert_objects_exist(&store, &dropped, false).await;
    assert!(exists(&store, &current.object_key).await);
    assert_objects_exist(&store, &current_segments, true).await;
    assert_eq!(report.deleted.manifests, 1);
    assert_eq!(report.deleted.metadata_segments, dropped.len() as u64);

    let error = view
        .read_file_bytes(
            "/docs/one",
            None,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect_err("the replaced view's bindings are gone");
    assert_matches!(
        error,
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::MissingSegment { .. }
        ))
    );
}

/// Checks that a collection keeps a replaced manifest while it is younger
/// than the grace window, and the segments that only it lists until they are
/// strictly older than the grace window, and that each goes at the first
/// collection after that.
#[tokio::test]
async fn a_replaced_manifest_and_its_segments_stay_within_their_grace_windows() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("replaced-grace").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let previous = seed_segments(&store, &namespace_id).await;
    let current = compact_bindings(&store, &namespace_id).await;
    let dropped = dropped_segments(&previous, &current);

    // The replaced manifest and its dropped segments report the time of
    // the compaction, and every other object reports an ancient age.
    let replaced_at_ms = NOW_MS;
    let young_keys = dropped
        .iter()
        .cloned()
        .chain([previous.object_key.clone()])
        .collect::<BTreeSet<_>>();
    let young = MetadataMapStore::new(
        &store,
        KeyPredicate::new(move |key| young_keys.contains(key)),
        move |mut metadata| {
            metadata.last_modified_at_ms = Some(replaced_at_ms);
            metadata
        },
    );
    let collect = async |at_ms| {
        crate::test_support::namespace_store(&young, &namespace_id)
            .gc(&context(at_ms).clock())
            .await
            .expect("collection")
    };

    let report = collect(replaced_at_ms + GC_GRACE_WINDOW_MS - 1).await;
    assert!(exists(&store, &previous.object_key).await);
    assert_objects_exist(&store, &dropped, true).await;
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(
        report.retained.within_grace_window,
        1 + dropped.len() as u64
    );

    // The manifest goes at the grace window, and a segment that is exactly
    // the grace window old stays.
    let report = collect(replaced_at_ms + GC_GRACE_WINDOW_MS).await;
    assert!(!exists(&store, &previous.object_key).await);
    assert_objects_exist(&store, &dropped, true).await;
    assert_eq!(report.deleted.manifests, 1);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.retained.within_grace_window, dropped.len() as u64);

    // A segment must be strictly older than the grace window before it goes.
    let report = collect(replaced_at_ms + GC_GRACE_WINDOW_MS + 1).await;
    assert_objects_exist(&store, &dropped, false).await;
    assert_eq!(report.deleted.metadata_segments, dropped.len() as u64);
    assert_eq!(report.retained.within_grace_window, 0);
    assert!(exists(&store, &current.object_key).await);
    assert_objects_exist(&store, &segment_keys(&current), true).await;
}

/// Checks that a user snapshot on a replaced manifest keeps the manifest and
/// every segment it lists however old they are, and that the first
/// collection after the snapshot is deleted removes the manifest and the segments
/// that only it listed.
#[tokio::test]
async fn a_replaced_manifest_kept_by_a_snapshot_keeps_its_segments_until_the_snapshot_is_released()
{
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("replaced-snapshot").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let previous = seed_segments(&store, &namespace_id).await;
    let snapshot = create_snapshot(&store, &namespace_id, &context(1_000))
        .await
        .expect("take a snapshot of the previous manifest");
    let current = compact_bindings(&store, &namespace_id).await;
    let dropped = dropped_segments(&previous, &current);

    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&context(NOW_MS).clock())
        .await
        .expect("collection with the snapshot");
    assert!(exists(&store, &previous.object_key).await);
    assert_objects_exist(&store, &segment_keys(&previous), true).await;
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.deleted.metadata_segments, 0);

    crate::test_support::namespace_store(&store, &namespace_id)
        .delete_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("release the snapshot");
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&context(NOW_MS).clock())
        .await
        .expect("collection after the release");
    assert!(!exists(&store, &previous.object_key).await);
    assert_objects_exist(&store, &dropped, false).await;
    assert_eq!(report.deleted.manifests, 1);
    assert_eq!(report.deleted.metadata_segments, dropped.len() as u64);
    assert!(exists(&store, &current.object_key).await);
    assert_objects_exist(&store, &segment_keys(&current), true).await;
}

/// Checks that a collection reads the current manifest and no replaced
/// manifest, however many young replaced manifests the namespace has, and
/// deletes none of them.
#[tokio::test]
async fn a_pass_reads_no_replaced_manifest() {
    const REPLACED: u64 = 6;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("replaced-reads").expect("namespace");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one", "one", &setup).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    namespace.fold_wal().await.expect("fold");
    let first = namespace
        .load_current_manifest()
        .await
        .expect("first manifest")
        .manifest()
        .manifest_no;
    // Each writer claim publishes a manifest.
    for index in 0..REPLACED {
        namespace
            .acquire_writer_epoch(&writer_at(&format!("writer-{index}"), 1_000).writer_id)
            .await
            .expect("claim the writer epoch");
    }
    let current = namespace
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert_eq!(current.manifest().manifest_no.0, first.0 + REPLACED);
    let manifests = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");

    let recorded = RecordingStore::new(
        Arc::clone(&store),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    );
    // Every manifest is younger than the grace.
    let now_ms = now_after_newest_object(&store, &namespace_id, 1).await;
    let report = crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(&context(now_ms).clock())
        .await
        .expect("collection");
    let read: BTreeSet<_> = recorded
        .take_get_keys()
        .into_iter()
        .filter(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number <= current.manifest().manifest_no)
        })
        .collect();
    assert_eq!(read, BTreeSet::from([current.object_key.clone()]));
    assert_eq!(report.deleted.manifests, 0);
    for key in &manifests {
        assert!(exists(&store, key).await, "{key}");
    }
}
