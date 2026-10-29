//! Grace roots for manifests whose successors recently stopped listing segments.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::manifest::CompactionUnitOutcome;
use crate::namespace::control::LoadedManifest;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_types::MetadataFamilyGroup;
use std::assert_matches;

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
        .list_prefix(&pin_prefix(namespace_id))
        .await
        .expect("list pins")
        .is_empty());
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("previous manifest")
}

async fn compact_bindings<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> LoadedManifest {
    let outcome = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
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
    .expect("compact bindings");
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published {
            group: MetadataFamilyGroup::Bindings,
            ..
        }
    );
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("compacted manifest")
}

fn segment_keys(manifest: &LoadedManifest) -> BTreeSet<String> {
    manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(metadata_segment_object_key)
        .collect()
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

#[tokio::test]
async fn recently_superseded_manifests_keep_old_segments_and_lazy_views_until_the_grace_expires() {
    for grace_window_ms in [GRACE_MS, GC_MIN_GRACE_WINDOW_MS] {
        for lagging_hint in [false, true] {
            let directory = tempdir().expect("directory");
            let namespace_id = NamespaceId::parse("superseded").expect("namespace");
            let store = MetadataMapStore::aged(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            );
            let namespace = crate::test_support::namespace_store(&store, &namespace_id);
            let raise_hint = |manifest_no| namespace.raise_hint(manifest_no, None);
            let previous = seed_segments(&store, &namespace_id).await;
            // Load without shared caches and leave path reads until after GC,
            // so resolving /docs/one needs the bindings compaction drops.
            let view = namespace
                .load_current_metadata_view()
                .await
                .expect("retain the previous view");
            let current = compact_bindings(&store, &namespace_id).await;
            assert_eq!(
                current.state.manifest().manifest_no.0,
                previous.state.manifest().manifest_no.0 + 1
            );
            // A publication leaves the hint where it was, and the runtime
            // that published the manifest raises the hint afterward. The
            // lagging-hint case models a raise that hasn't happened yet.
            if !lagging_hint {
                raise_hint(current.state.manifest().manifest_no)
                    .await
                    .expect("raise the hint after the publications");
            }
            let current_segments = segment_keys(&current);
            let dropped: BTreeSet<_> = segment_keys(&previous)
                .difference(&current_segments)
                .cloned()
                .collect();
            assert!(!dropped.is_empty());
            let anchor = crate::test_support::namespace_store(&store, &namespace_id)
                .load_read_anchor(crate::test_support::observed_now())
                .await
                .expect("current anchor");
            assert_eq!(
                previous.state.manifest().manifest_no < anchor.hint.state.manifest_no,
                !lagging_hint
            );
            if lagging_hint {
                assert!(previous.state.manifest().manifest_no > anchor.hint.state.manifest_no);
            }

            let published_at_ms = UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + GRACE_MS + 1;
            let recent = MetadataMapStore::new(
                &store,
                KeyPredicate::exact(&current.object_key),
                move |mut metadata| {
                    metadata.last_modified_at_ms = Some(published_at_ms);
                    metadata
                },
            );
            let options = GcOptions {
                grace_window_ms,
                ..Default::default()
            };
            let retained = crate::test_support::namespace_store(&recent, &namespace_id)
                .gc(
                    None,
                    &options,
                    &context(published_at_ms + grace_window_ms - 1),
                )
                .await
                .expect("collect within successor grace");
            assert_objects_exist(&store, &dropped, true).await;
            assert!(store
                .head(&previous.object_key)
                .await
                .expect("previous manifest")
                .is_some());
            assert_eq!(retained.deleted.metadata_segments, 0);
            assert_eq!(retained.retained.within_grace_window, 0);
            let file = view
                .read_file_bytes(
                    "/docs/one",
                    None,
                    &ReadAccess::live(Authorizer::Unrestricted),
                )
                .await
                .expect("lazy read through the superseded view");
            assert_eq!(file.bytes, b"body\n");

            // Once the hint catches up, only the successor's age can protect
            // its predecessor. The exact pass grace applies, even below the
            // longer namespace retirement grace.
            raise_hint(current.state.manifest().manifest_no)
                .await
                .expect("advance hint");
            let collected = crate::test_support::namespace_store(&recent, &namespace_id)
                .gc(None, &options, &context(published_at_ms + grace_window_ms))
                .await
                .expect("collect at successor grace boundary");
            assert_objects_exist(&store, &dropped, false).await;
            assert!(store
                .head(&previous.object_key)
                .await
                .expect("previous manifest")
                .is_none());
            assert_objects_exist(&store, &current_segments, true).await;
            assert!(store
                .head(&current.object_key)
                .await
                .expect("current manifest")
                .is_some());
            assert_eq!(collected.deleted.metadata_segments, dropped.len() as u64);
            assert!(collected.deleted.manifests > 0);
            assert_eq!(
                retained.retained.referenced - collected.retained.referenced,
                dropped.len() as u64 + collected.deleted.manifests
            );
        }
    }
}

#[tokio::test]
async fn a_recent_successor_roots_its_predecessor_even_when_it_is_no_longer_current() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("older-predecessor").expect("namespace");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let previous = seed_segments(&store, &namespace_id).await;
    let successor = compact_bindings(&store, &namespace_id).await;
    let dropped: BTreeSet<_> = segment_keys(&previous)
        .difference(&segment_keys(&successor))
        .cloned()
        .collect();
    assert!(!dropped.is_empty());
    crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context(1_000))
        .await
        .expect("supersede the compaction manifest");
    // The runtime that published the claim raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    assert!(current.state.manifest().manifest_no > successor.state.manifest().manifest_no);
    let now_ms = UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + GRACE_MS + 1;
    let recent = Arc::new(MetadataMapStore::new(
        Arc::clone(&store),
        KeyPredicate::exact(&successor.object_key),
        move |mut metadata| {
            metadata.last_modified_at_ms = Some(now_ms);
            metadata
        },
    ));
    let recorded = RecordingStore::new(
        Arc::clone(&recent),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    );
    crate::test_support::namespace_store(&recorded, &namespace_id)
        .gc(None, &options(), &context(now_ms))
        .await
        .expect("root an older predecessor");
    // Discovery's probe and the anchor's check each send a HEAD request for
    // the missing manifest after the current one.
    assert_eq!(recorded.counts().heads, 2);
    let read: BTreeSet<_> = recorded
        .take_get_keys()
        .into_iter()
        .filter(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number <= current.state.manifest().manifest_no)
        })
        .collect();
    assert_eq!(
        read,
        BTreeSet::from([previous.object_key.clone(), current.object_key.clone()])
    );
    assert_objects_exist(&store, &dropped, true).await;
    assert!(store
        .head(&previous.object_key)
        .await
        .expect("previous manifest")
        .is_some());

    store
        .delete(&successor.object_key)
        .await
        .expect("remove successor");
    crate::test_support::namespace_store(&recent, &namespace_id)
        .gc(None, &options(), &context(now_ms))
        .await
        .expect("collect with missing successor");
    assert_objects_exist(&store, &dropped, false).await;
    assert!(store
        .head(&previous.object_key)
        .await
        .expect("previous manifest")
        .is_none());
    assert_objects_exist(&store, &segment_keys(&current), true).await;
}

#[tokio::test]
async fn a_successor_without_a_provider_timestamp_keeps_its_predecessor_and_segments_rooted() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("undated-successor").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let previous = seed_segments(&store, &namespace_id).await;
    let current = compact_bindings(&store, &namespace_id).await;
    // The runtime that published the compaction raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let dropped: BTreeSet<_> = segment_keys(&previous)
        .difference(&segment_keys(&current))
        .cloned()
        .collect();
    assert!(!dropped.is_empty());
    let undated =
        MetadataMapStore::without_last_modified(&store, KeyPredicate::exact(&current.object_key));
    let aged = context(UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + GRACE_MS + 1);
    let retained = crate::test_support::namespace_store(&undated, &namespace_id)
        .gc(None, &options(), &aged)
        .await
        .expect("collect with undated successor");
    assert_objects_exist(&store, &dropped, true).await;
    assert!(store
        .head(&previous.object_key)
        .await
        .expect("previous manifest")
        .is_some());
    assert_eq!(retained.deleted.metadata_segments, 0);
    assert_eq!(retained.retained.no_provider_timestamp, 0);

    let collected = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged)
        .await
        .expect("collect with aged successor timestamp");
    assert_objects_exist(&store, &dropped, false).await;
    assert!(store
        .head(&previous.object_key)
        .await
        .expect("previous manifest")
        .is_none());
    assert_objects_exist(&store, &segment_keys(&current), true).await;
    assert_eq!(
        retained.retained.referenced - collected.retained.referenced,
        dropped.len() as u64 + 1
    );
}
