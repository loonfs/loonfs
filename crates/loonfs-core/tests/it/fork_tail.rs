//! A fork of a head with an unfolded WAL tail: a call that stops after the
//! fork's manifest 1 or manifest 2 is finished by the next call with the
//! same request, and the fork point snapshot keeps the content of the tail's
//! replaced revisions until the fork is retired.

use crate::common::*;
use loonfs_core::test_support::{
    compact_every_group, folded_manifest, namespace_store, pool_store, NamespaceFixture,
};
use loonfs_core::{CoreError, RetentionTarget};
use loonfs_objectstore::keys::{
    content_blob, metadata_manifest_object, metadata_manifest_prefix, snapshot_prefix,
    snapshot_record, wal_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::ids::{namespace_id, numbered_content_id};
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationSelector};
use loonfs_types::format::control::NamespaceStatus;
use loonfs_types::{ManifestNo, NamespaceId};
use std::assert_matches;
use tempfile::tempdir;

/// Creates `source` with a folded `/kept.txt`, and a WAL tail that puts
/// `/file.txt`, whose first revision is the content object of
/// `numbered_content_id(1)`, and replaces that revision.
async fn source_with_a_replaced_revision<S: ObjectStore>(store: &S, source: &NamespaceId) {
    let fixture = NamespaceFixture::new(store, source, &default_writer());
    fixture.create().await.expect("create the source");
    fixture
        .write_file_bytes("/kept.txt", b"kept", None)
        .await
        .expect("a folded file");
    folded_manifest(&fixture.namespace)
        .await
        .expect("fold the source");
    fixture
        .write_file_bytes_with_content_id("/file.txt", b"first", numbered_content_id(1))
        .await
        .expect("the first revision");
    fixture
        .write_file_bytes("/file.txt", b"second", None)
        .await
        .expect("the revision that replaces it in the tail");
}

/// Forks the head of `source` into `target`, reading the head by discovery,
/// at the time that the source's commits record, so the tail is young.
async fn fork_head<S: ObjectStore>(
    store: &S,
    source: &NamespaceId,
    target: &NamespaceId,
) -> Result<loonfs_types::NamespaceMetadata, CoreError> {
    let engine = NamespaceFixture::new(store, source, &default_writer()).engine();
    engine
        .fork_namespace(
            &loonfs_core::test_support::administrator(&engine),
            engine.find_fork_target(target).await?,
            &loonfs_test_support::test_actor(),
            None,
        )
        .await
        .map(|outcome| {
            outcome
                .created()
                .expect("a fork of a tail with no old commit")
        })
}

/// Returns the number of the target's manifests and its newest manifest's
/// status.
async fn manifests<S: ObjectStore>(store: &S, target: &NamespaceId) -> (usize, NamespaceStatus) {
    let count = store
        .list_prefix(&metadata_manifest_prefix(target))
        .await
        .expect("list the manifests")
        .len();
    let newest = namespace_store(store, target)
        .load_current_manifest()
        .await
        .expect("the newest manifest");
    (count, newest.payload().status)
}

/// Checks that a call that stopped after the fork's manifest 2, at its fork
/// point snapshot's put, is finished by a second call, which writes that
/// snapshot on manifest 2 and publishes the active manifest 3.
#[tokio::test]
async fn a_fork_that_stopped_after_manifest_2_is_finished_by_the_same_request() {
    let directory = tempdir().expect("tempdir");
    let [source, target] = ["source", "target"].map(namespace_id);
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&target)),
        OperationSelector::Put,
        InjectedError::PermissionDenied("the call stops".to_owned()),
    );
    source_with_a_replaced_revision(&store, &source).await;
    store.fail_next(1);
    fork_head(&store, &source, &target)
        .await
        .expect_err("the fork point snapshot's put fails");
    let initializing = NamespaceStatus::Initializing {};
    assert_eq!(manifests(&store, &target).await, (2, initializing));

    let forked = fork_head(&store, &source, &target)
        .await
        .expect("the same request finishes the fork");
    assert_eq!(
        manifests(&store, &target).await,
        (3, NamespaceStatus::Active {})
    );
    let basis = forked.fork_basis.expect("a fork basis");
    assert_eq!(basis.fork_point_snapshot_id.manifest_no(), ManifestNo(2));
    assert!(store
        .head(&snapshot_record(&target, &basis.fork_point_snapshot_id))
        .await
        .expect("head")
        .is_some());
    let file = read_file_bytes(&store, &target, "/file.txt")
        .await
        .expect("read the fork");
    assert_eq!(file.bytes, b"second");
}

/// Checks that a call that stopped after the fork's manifest 1 is finished
/// by a second call that folds the source's tail again from the source's
/// WAL objects, which the source folded in between, and that the second call
/// returns `snapshot_not_found` when one of those WAL objects is gone.
#[tokio::test]
async fn a_fork_that_stopped_after_manifest_1_folds_the_sources_tail_again() {
    for wal_object_gone in [false, true] {
        let directory = tempdir().expect("tempdir");
        let [source, target] = ["source", "target"].map(namespace_id);
        let store = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::exact(metadata_manifest_object(&target, &ManifestNo(2))),
            OperationSelector::Put,
            InjectedError::Transport("the call stops".to_owned()),
        );
        source_with_a_replaced_revision(&store, &source).await;
        store.fail_next(1);
        fork_head(&store, &source, &target)
            .await
            .expect_err("the manifest 2 put fails");
        assert_eq!(
            manifests(&store, &target).await,
            (1, NamespaceStatus::Initializing {})
        );
        folded_manifest(&namespace_store(&store, &source))
            .await
            .expect("the source folds its tail");
        if wal_object_gone {
            // The newest WAL object has the tail's last commit.
            let newest = store
                .list_prefix(&wal_prefix(&source))
                .await
                .expect("list the source's WAL")
                .pop()
                .expect("a WAL object");
            store
                .delete(&newest)
                .await
                .expect("a collection deletes a folded WAL object");
            let error = fork_head(&store, &source, &target)
                .await
                .expect_err("the tail is gone");
            assert_matches!(error, CoreError::SnapshotNotFound { .. });
            continue;
        }
        let forked = fork_head(&store, &source, &target)
            .await
            .expect("the same request folds the tail again");
        assert_eq!(
            manifests(&store, &target).await,
            (3, NamespaceStatus::Active {})
        );
        let basis = forked.fork_basis.expect("a fork basis");
        assert_eq!(forked.head_seq, basis.fork_point_seq);
        assert_eq!(forked.retention_floor_seq, basis.fork_point_seq);
        let file = read_file_bytes(&store, &target, "/file.txt")
            .await
            .expect("read the fork");
        assert_eq!(file.bytes, b"second");
    }
}

/// Checks that the content of a revision that a commit in the source's tail
/// replaced survives a pool collection while the fork exists, when the fork
/// point snapshot is the only root that refers to it, and that retiring the
/// fork deletes both of its fork snapshots, after which the pool's
/// collection deletes the content.
#[tokio::test]
async fn a_fork_point_snapshot_keeps_a_replaced_revisions_content_until_the_fork_retires() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let [source, fork] = ["source", "fork"].map(namespace_id);
    source_with_a_replaced_revision(&store, &source).await;
    let forked = fork_head(&store, &source, &fork).await.expect("fork");
    let pool_id = forked.pool_id.clone();
    let basis = namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect("the fork's manifest")
        .payload()
        .fork_basis
        .clone()
        .expect("a fork basis");
    // Neither namespace's current view keeps the first revision: each one's
    // floor passes the replacement, and a compaction of its two runs removes
    // the revision.
    folded_manifest(&namespace_store(&store, &source))
        .await
        .expect("the source folds its tail");
    namespace_engine(&store, &source, &default_writer())
        .advance_retention_floor(RetentionTarget::Head)
        .await
        .expect("advance the source's floor");
    for namespace_id in [&source, &fork] {
        compact_every_group(&store, namespace_id).await;
    }
    let mut aged = default_writer();
    aged.now_ms = u64::MAX / 4;
    let first = content_blob(&pool_id, &numbered_content_id(1));
    pool_store(&store, &pool_id)
        .gc(None, &aged.clock())
        .await
        .expect("collect the pool");
    assert!(store.head(&first).await.expect("head").is_some());

    namespace_engine(&store, &fork, &default_writer())
        .delete_namespace(
            &loonfs_core::test_support::administrator_of(&store, &fork),
            Default::default(),
        )
        .await
        .expect("delete the fork");
    let retired = namespace_store(&store, &fork)
        .gc(&aged.clock())
        .await
        .expect("retire the fork");
    assert_eq!(retired.deleted_snapshots_by_owner.fork, 2);
    for key in [
        snapshot_record(&source, &basis.source_snapshot_id),
        snapshot_record(&fork, &basis.fork_point_snapshot_id),
    ] {
        assert!(store.head(&key).await.expect("head").is_none(), "{key}");
    }
    pool_store(&store, &pool_id)
        .gc(None, &aged.clock())
        .await
        .expect("collect the pool");
    assert!(store.head(&first).await.expect("head").is_none());
}
