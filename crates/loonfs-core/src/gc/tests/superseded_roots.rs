//! Grace roots for manifests whose successors recently stopped listing segments.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::checkpoint::{flush_wal, reorganize_metadata_step, MetadataReorganizeOutcome};
use crate::namespace::control::{load_current_manifest, raise_hint, LoadedManifest};
use loonfs_api::MetadataFamilyGroup;
use loonfs_objectstore::keys::metadata_segment_object_key;

async fn seed_segments<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> LoadedManifest {
    let setup = context(1_000);
    create(store, namespace_id, &setup)
        .await
        .expect("bootstrap");
    for name in ["one", "two"] {
        write_test_file(store, namespace_id, &format!("/docs/{name}"), name, &setup).await;
        flush_wal(store, namespace_id).await.expect("flush");
    }
    assert!(store
        .list_prefix(&checkpoint_prefix(namespace_id))
        .await
        .expect("list pins")
        .is_empty());
    load_current_manifest(store, namespace_id)
        .await
        .expect("previous manifest")
}

async fn compact_bindings<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> LoadedManifest {
    let outcome = reorganize_metadata_step(
        store,
        namespace_id,
        loonfs_api::CompactorEpoch(0),
        Default::default(),
        MetadataCompactionPolicy::CompactImmediately,
    )
    .await
    .expect("compact bindings");
    assert!(matches!(
        outcome,
        MetadataReorganizeOutcome::UnitPublished {
            group: MetadataFamilyGroup::Bindings,
            ..
        }
    ));
    load_current_manifest(store, namespace_id)
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
            let store = FailStore::new(
                MetadataMapStore::aged(
                    LocalFsStore::new(directory.path()).expect("store"),
                    KeyPredicate::any(),
                ),
                KeyPredicate::hint(&namespace_id),
                OperationClass::CompareAndSwap,
                InjectedError::Transport("hint unavailable".into()),
            );
            if lagging_hint {
                store.fail_all();
            }
            let previous = seed_segments(&store, &namespace_id).await;
            // Load without shared caches and leave path reads until after GC,
            // so resolving /docs/one needs the bindings compaction drops.
            let view = load_current_metadata_view(&store, &namespace_id)
                .await
                .expect("retain the previous view");
            let current = compact_bindings(&store, &namespace_id).await;
            assert_eq!(
                current.state.manifest().manifest_no.0,
                previous.state.manifest().manifest_no.0 + 1
            );
            let current_segments = segment_keys(&current);
            let dropped: BTreeSet<_> = segment_keys(&previous)
                .difference(&current_segments)
                .cloned()
                .collect();
            assert!(!dropped.is_empty());
            let anchor = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
                .await
                .expect("current anchor");
            assert_eq!(
                previous.state.manifest().manifest_no < anchor.hint.state.manifest_no,
                !lagging_hint
            );
            if lagging_hint {
                assert!(previous.state.manifest().manifest_no > anchor.hint.state.manifest_no);
            }

            let published_at_ms = UNREFERENCED_SEGMENT_MIN_AGE_MS + GRACE_MS + 1;
            let recent = MetadataMapStore::new(
                &store,
                KeyPredicate::exact(&current.object_key),
                move |mut metadata| {
                    metadata.last_modified_ms = Some(published_at_ms);
                    metadata
                },
            );
            let config = GcConfig { grace_window_ms };
            let retained = gc_namespace(
                &recent,
                &namespace_id,
                &config,
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
                .get_file_bytes(
                    &store,
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
            store.clear();
            raise_hint(
                &store,
                &namespace_id,
                current.state.manifest().manifest_no,
                None,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .expect("advance hint");
            let collected = gc_namespace(
                &recent,
                &namespace_id,
                &config,
                &context(published_at_ms + grace_window_ms),
            )
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
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let previous = seed_segments(&store, &namespace_id).await;
    let successor = compact_bindings(&store, &namespace_id).await;
    let dropped: BTreeSet<_> = segment_keys(&previous)
        .difference(&segment_keys(&successor))
        .cloned()
        .collect();
    assert!(!dropped.is_empty());
    crate::namespace::writer_epoch::acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("supersede the compaction manifest");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("current manifest");
    assert!(current.state.manifest().manifest_no > successor.state.manifest().manifest_no);
    let now_ms = UNREFERENCED_SEGMENT_MIN_AGE_MS + GRACE_MS + 1;
    let recent = MetadataMapStore::new(
        &store,
        KeyPredicate::exact(&successor.object_key),
        move |mut metadata| {
            metadata.last_modified_ms = Some(now_ms);
            metadata
        },
    );
    gc_namespace(&recent, &namespace_id, &config(), &context(now_ms))
        .await
        .expect("root an older predecessor");
    assert_objects_exist(&store, &dropped, true).await;
    assert!(store
        .head(&previous.object_key)
        .await
        .expect("previous manifest")
        .is_some());

    // A missing successor leaves the predecessor an ordinary candidate.
    store
        .delete(&successor.object_key)
        .await
        .expect("remove successor");
    gc_namespace(&recent, &namespace_id, &config(), &context(now_ms))
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
    let dropped: BTreeSet<_> = segment_keys(&previous)
        .difference(&segment_keys(&current))
        .cloned()
        .collect();
    assert!(!dropped.is_empty());
    let undated =
        MetadataMapStore::without_last_modified(&store, KeyPredicate::exact(&current.object_key));
    let aged = context(UNREFERENCED_SEGMENT_MIN_AGE_MS + GRACE_MS + 1);
    let retained = gc_namespace(&undated, &namespace_id, &config(), &aged)
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

    let collected = gc_namespace(&store, &namespace_id, &config(), &aged)
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
