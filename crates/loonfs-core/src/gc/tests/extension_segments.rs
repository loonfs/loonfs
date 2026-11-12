//! Extension segments under the core segment rule: a segment stays while a
//! root lists it, and goes once it's unlisted and older than the segment
//! minimum age.

use super::*;
use crate::manifest::tests::extensions::{extension_run, extension_segment};
use loonfs_objectstore::keys::{extension_prefix, extension_segment_object_key};
use loonfs_types::format::manifest::{ExtensionEntry, ExtensionSegmentRef};
use loonfs_types::ExtensionName;

fn grep() -> ExtensionName {
    ExtensionName::parse("grep").expect("extension name")
}

/// Publishes the grep entry with one run of `segment` over the current
/// manifest, numbered from its next run number.
async fn publish_run(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    segment: ExtensionSegmentRef,
) {
    let engine =
        crate::test_support::NamespaceFixture::new(store, namespace_id, &context(1_000)).engine();
    let basis = engine.load_extension_basis().await.expect("basis");
    let published = engine
        .publish_extension(&basis, &grep(), |base| {
            (
                ExtensionEntry::new(
                    1,
                    vec![extension_run(base, vec![segment.clone()])],
                    serde_json::Map::new(),
                ),
                base.successor().expect("run"),
            )
        })
        .await
        .expect("publish");
    assert!(published.published.is_some());
}

/// Writes the object of `segment` and returns its key.
async fn write_segment(store: &LocalFsStore, segment: &ExtensionSegmentRef) -> String {
    let key = extension_segment_object_key(&grep(), segment);
    store
        .put_if_absent(&key, Bytes::from_static(b"segment"))
        .await
        .expect("segment");
    key
}

async fn collect(store: &LocalFsStore, namespace_id: &NamespaceId) -> GcResponse {
    let aged = MetadataMapStore::aged(store, KeyPredicate::any());
    crate::test_support::namespace_store(&aged, namespace_id)
        .gc(
            &options(),
            &context(UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1).clock(),
        )
        .await
        .expect("collection")
}

/// Checks that a collection keeps the extension segments that the current
/// manifest or a snapshot's manifest lists, and an unrecognized key under the
/// extension's prefix, and deletes an aged segment once no root lists it,
/// including the segments of a tombstone, which lists none.
#[tokio::test]
async fn an_extension_segment_goes_once_no_root_lists_it() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("extension-gc").expect("namespace");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("create");
    write_test_file(&store, &namespace_id, "/one", "one", &setup).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let first = extension_segment(&namespace_id, 1);
    let first_key = write_segment(&store, &first).await;
    publish_run(&store, &namespace_id, first).await;
    let snapshot = create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("take a snapshot of the first entry");
    let second = extension_segment(&namespace_id, 2);
    let second_key = write_segment(&store, &second).await;
    publish_run(&store, &namespace_id, second).await;
    let orphan_key = write_segment(&store, &extension_segment(&namespace_id, 3)).await;
    let unrecognized = format!("{}hint.json", extension_prefix(&namespace_id, &grep()));
    store
        .put_if_absent(&unrecognized, Bytes::from_static(b"{}"))
        .await
        .expect("unrecognized key");

    let report = collect(&store, &namespace_id).await;
    assert_eq!(report.deleted.extension_segments, 1, "{report:?}");
    assert!(store.head(&orphan_key).await.expect("orphan").is_none());
    for kept in [&first_key, &second_key, &unrecognized] {
        assert!(store.head(kept).await.expect("kept").is_some(), "{kept}");
    }

    crate::test_support::namespace_store(&store, &namespace_id)
        .delete_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("delete the snapshot");
    let report = collect(&store, &namespace_id).await;
    assert_eq!(report.deleted.extension_segments, 1, "{report:?}");
    assert!(store.head(&first_key).await.expect("first").is_none());
    assert!(store.head(&second_key).await.expect("second").is_some());

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete");
    let report = collect(&store, &namespace_id).await;
    assert_eq!(report.deleted.extension_segments, 1, "{report:?}");
    assert!(store.head(&second_key).await.expect("second").is_none());
    assert!(store
        .head(&unrecognized)
        .await
        .expect("unrecognized")
        .is_some());
}

/// Checks that a fork's first manifest lists the segments of its source's
/// entry, and that a segment the fork inherited stays after the source
/// stops listing it, because the fork snapshot keeps the source's manifest that
/// lists it, and goes once the fork's retirement releases the snapshot.
#[tokio::test]
async fn an_inherited_extension_segment_stays_until_no_fork_snapshot_keeps_it() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("extension-source").expect("namespace");
    let fork = NamespaceId::parse("extension-fork").expect("namespace");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("create");
    write_test_file(&store, &source, "/one", "one", &setup).await;
    crate::test_support::namespace_store(&store, &source)
        .fold_wal()
        .await
        .expect("fold");
    let inherited = extension_segment(&source, 1);
    let inherited_key = write_segment(&store, &inherited).await;
    publish_run(&store, &source, inherited.clone()).await;
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &fork,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let first = crate::test_support::namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect("the fork's first manifest");
    assert_eq!(
        first.payload().extensions[&grep()]
            .segments()
            .collect::<Vec<_>>(),
        [&inherited]
    );
    let replacement = extension_segment(&source, 2);
    write_segment(&store, &replacement).await;
    publish_run(&store, &source, replacement).await;

    for namespace_id in [&source, &fork] {
        let report = collect(&store, namespace_id).await;
        assert_eq!(report.deleted.extension_segments, 0, "{namespace_id}");
    }
    assert!(store
        .head(&inherited_key)
        .await
        .expect("inherited")
        .is_some());

    crate::test_support::NamespaceFixture::new(&store, &fork, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete the fork");
    let retired = crate::test_support::namespace_store(&store, &fork)
        .gc(&options(), &context(setup.now_ms + retirement_ms()).clock())
        .await
        .expect("retire the fork");
    assert_eq!(retired.deleted_snapshots_by_owner.fork, 1);
    let report = collect(&store, &source).await;
    assert_eq!(report.deleted.extension_segments, 1, "{report:?}");
    assert!(store
        .head(&inherited_key)
        .await
        .expect("inherited")
        .is_none());
}
