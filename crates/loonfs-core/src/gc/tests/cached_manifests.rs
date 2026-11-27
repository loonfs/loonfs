//! Collections load manifests through the segment cache, so a second pass over
//! an unchanged namespace reads no manifest.

use super::*;
use loonfs_objectstore::keys::metadata_manifest_prefix;

/// Creates a namespace with two snapshots, each on a manifest that a later
/// fold replaced, and returns the namespace's pool.
async fn namespace_with_kept_manifests<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> PoolId {
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("create")
        .pool_id;
    for name in ["one", "two"] {
        write_test_file(store, namespace_id, &format!("/docs/{name}"), name, &setup).await;
        create_snapshot(store, namespace_id, &setup)
            .await
            .expect("take a snapshot");
    }
    write_test_file(store, namespace_id, "/docs/three", "three", &setup).await;
    crate::test_support::namespace_store(store, namespace_id)
        .fold_as_test_writer()
        .await
        .expect("fold");
    pool_id
}

/// Checks that a namespace collection loads the current manifest and the
/// manifests that the snapshots keep through the segment cache: the first pass
/// sends one GET for each, and a second pass over the unchanged namespace
/// sends no manifest GET.
#[tokio::test]
async fn a_second_namespace_collection_reads_no_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("cached-manifests").expect("namespace");
    let local = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    namespace_with_kept_manifests(&local, &namespace_id).await;
    let store = RecordingStore::new(
        Arc::clone(&local),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    );
    let namespace = crate::test_support::namespace_store_with_segment_cache(&store, &namespace_id);

    let first = namespace
        .gc(&context(2_000).clock())
        .await
        .expect("first collection");
    assert_eq!(
        store.take_get_keys().len(),
        3,
        "the current manifest and two kept"
    );
    let second = namespace
        .gc(&context(2_000).clock())
        .await
        .expect("second collection");
    assert_eq!(store.take_get_keys(), Vec::<String>::new());
    assert_eq!(first.deleted, second.deleted);
}

/// Checks that a pool collection loads a member's current manifest and the
/// manifests that the member's snapshots keep through the segment cache: a
/// second pass over the unchanged pool sends no manifest GET.
#[tokio::test]
async fn a_second_pool_collection_reads_no_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("cached-member").expect("namespace");
    let local = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let pool_id = namespace_with_kept_manifests(&local, &namespace_id).await;
    let store = RecordingStore::new(
        Arc::clone(&local),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
    );
    let pool = crate::PoolStore {
        segment_cache: Arc::new(crate::test_support::segment_cache()),
        ..crate::test_support::pool_store(&store, &pool_id)
    };

    let first = pool
        .gc(None, &context(2_000).clock())
        .await
        .expect("first collection");
    assert_eq!(
        store.take_get_keys().len(),
        3,
        "the current manifest and two kept"
    );
    let second = pool
        .gc(None, &context(2_000).clock())
        .await
        .expect("second collection");
    assert_eq!(store.take_get_keys(), Vec::<String>::new());
    assert_eq!(first.deleted, second.deleted);
}
