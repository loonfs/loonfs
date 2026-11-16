//! Namespaces whose newest manifest is initializing: a collection publishes
//! the tombstone of an abandoned creation, and the source's snapshot cleanup
//! keeps the fork snapshot that an initializing target records.

use super::*;
use crate::test_support::{half_done_fork, write_fork_snapshot};
use loonfs_types::format::manifest::NamespaceManifestPayload;

fn source_and_target() -> (NamespaceId, NamespaceId) {
    (
        NamespaceId::parse("source").expect("source"),
        NamespaceId::parse("target").expect("target"),
    )
}

/// Checks that a collection leaves an initializing namespace inside the grace
/// window after its manifest 1 and reports when it publishes the tombstone,
/// publishes the tombstone once the window has passed, and that a later
/// collection retires the namespace and deletes its fork snapshot.
#[tokio::test]
async fn the_collection_tombstones_an_abandoned_namespace_creation_after_the_grace_window() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (_, manifest_1) = half_done_fork(&store, &source, &target, &context(1_000)).await;
    write_fork_snapshot(&store, &manifest_1).await;
    let fork_snapshot_id = manifest_1
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .source_snapshot_id
        .clone();
    let created_at_ms = manifest_1.created_at_ms;
    let current = || async {
        crate::test_support::namespace_store(&store, &target)
            .load_current_manifest()
            .await
            .expect("the current manifest")
            .payload()
            .clone()
    };

    let young = collect(
        &store,
        &target,
        &context(created_at_ms + GC_GRACE_WINDOW_MS - 1),
    )
    .await;
    assert_eq!(
        young.next_reclamation_at_ms,
        Some(created_at_ms + GC_GRACE_WINDOW_MS)
    );
    assert_eq!(young.retires_at_ms, None);
    assert_eq!(current().await, manifest_1);

    let abandoned_at_ms = created_at_ms + GC_GRACE_WINDOW_MS;
    collect(&store, &target, &context(abandoned_at_ms)).await;
    assert_eq!(
        current().await,
        NamespaceManifestPayload {
            manifest_no: ManifestNo(2),
            ..manifest_1.tombstone(abandoned_at_ms)
        }
    );
    assert!(snapshot_record_exists(&store, &source, &fork_snapshot_id).await);

    collect(
        &store,
        &target,
        &context(created_at_ms + 2 * GC_GRACE_WINDOW_MS),
    )
    .await;
    assert_eq!(
        crate::test_support::namespace_store(&store, &target)
            .load_boundary()
            .await
            .expect("load the boundary")
            .map(|loaded| loaded.state.boundary),
        Some(ManifestBoundary::Retired {})
    );
    assert!(!snapshot_record_exists(&store, &source, &fork_snapshot_id).await);
}

/// Checks that the source's snapshot cleanup keeps a fork snapshot whose
/// target's newest manifest is an initializing manifest 1 that records it,
/// at any age, and that the pool's collection lists none of the target's
/// snapshots.
#[tokio::test]
async fn snapshot_cleanup_keeps_a_fork_snapshot_whose_target_is_initializing() {
    let directory = tempdir().expect("directory");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (source, target) = source_and_target();
    let (_, manifest_1) = half_done_fork(&store, &source, &target, &context(1_000)).await;
    write_fork_snapshot(&store, &manifest_1).await;
    let aged = MetadataMapStore::aged(store.as_ref(), KeyPredicate::any());
    let report = crate::test_support::namespace_store(&aged, &source)
        .gc(&context(1_000 + 2 * GC_GRACE_WINDOW_MS).clock())
        .await
        .expect("collect the source");
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    let basis = manifest_1.fork_basis.as_ref().expect("a fork basis");
    assert!(snapshot_record_exists(&store, &source, &basis.source_snapshot_id).await);
    // The pool's collection reads only the initializing target's newest
    // manifest.
    let recording = RecordingStore::new(
        Arc::clone(&store),
        KeyPredicate::prefix(snapshot_prefix(&target)),
    );
    collect_pool(&recording, &manifest_1.pool_id, &context(1_000)).await;
    assert_eq!(recording.count(OperationSelector::Any), 0);
    assert!(snapshot_record_exists(&store, &source, &basis.source_snapshot_id).await);
}

/// Checks that the collection of a deleted namespace keeps the namespace's
/// own fork point snapshot whose manifest is missing, without an error, and
/// that retirement deletes the snapshot. A call that finishes a fork writes
/// such a snapshot when it found an initializing manifest that a delete and a
/// collection then removed during the call.
#[tokio::test]
async fn a_deleted_namespaces_fork_point_snapshot_on_a_missing_manifest_is_left_to_retirement() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let (source, target) = source_and_target();
    let (_, manifest_1) = half_done_fork(&store, &source, &target, &context(1_000)).await;
    let namespace = crate::test_support::namespace_store(&store, &target);
    let crate::namespace::control::NewestManifest::Manifest(found) = namespace
        .find_newest_manifest()
        .await
        .expect("find manifest 1")
    else {
        panic!("manifest 1 is the newest manifest");
    };
    let deleted_at_ms = 1_000 + GC_GRACE_WINDOW_MS;
    namespace
        .tombstone_initializing_namespace(found.clone(), &context(deleted_at_ms).clock())
        .await
        .expect("delete the target");
    let aged = MetadataMapStore::aged(
        &store,
        KeyPredicate::exact(metadata_manifest_object(&target, &ManifestNo(1))),
    );
    collect(&aged, &target, &context(deleted_at_ms)).await;
    assert!(namespace
        .load_manifest_by_no(ManifestNo(1))
        .await
        .expect("load manifest 1")
        .is_none());

    namespace
        .write_fork_point_snapshot(&found, &context(deleted_at_ms).clock())
        .await
        .expect("the stale call's fork point snapshot");
    let fork_point_snapshot_id = &manifest_1
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .fork_point_snapshot_id;
    collect(&store, &target, &context(deleted_at_ms + 1)).await;
    assert!(snapshot_record_exists(&store, &target, fork_point_snapshot_id).await);
    collect(
        &store,
        &target,
        &context(deleted_at_ms + GC_GRACE_WINDOW_MS),
    )
    .await;
    assert!(!snapshot_record_exists(&store, &target, fork_point_snapshot_id).await);
}
