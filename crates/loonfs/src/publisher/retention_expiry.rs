//! The worker's steps of the session's expiry after a retention advance.

use super::*;
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::DeleteDirectoryBehavior;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_survives_a_panicked_expiry_step_and_a_later_worker_expires_the_deletion() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicWalPutStore::new(temp_dir.path(), &namespace_id));
    let writer = test_writer_with_interval(store.clone() as SharedStore, 0).await;
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let commit = |request: CommitIntent| {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(request))
                .await
        })
    };
    commit(create_directory_request("create", "deleted"))
        .await
        .expect("create task")
        .expect("create");
    let deleted = commit(CommitIntent::single(
        CommitId::parse("delete").expect("commit id"),
        actor.clone(),
        None,
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/deleted").expect("path"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
    ))
    .await
    .expect("delete task")
    .expect("delete");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            FilesystemChange::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("the delete reports its deletion");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    maintenance.fold_wal(&namespace_id).await.expect("fold");

    // The advance's position move starts the worker's expiry step, and the
    // step's WAL put panics.
    store.arm_blocking_panic();
    maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    store.wait_until_blocked().await;
    let queued = commit(create_directory_request("queued", "queued"));
    wait_for_queued_candidates(&namespace.session().publisher, 1).await;
    store.release_into_panic();
    queued
        .await
        .expect("queued task")
        .expect("the worker publishes queued work after the panic");
    let drain_error = writer
        .drain()
        .await
        .expect_err("drain surfaces the contained panic");
    assert!(
        drain_error
            .to_string()
            .contains("1 publisher task(s) panicked"),
        "{drain_error}"
    );

    // The next worker finds the floor again and expires the deletion before
    // it publishes the undelete.
    commit(create_directory_request("after", "after"))
        .await
        .expect("after task")
        .expect("a commit after the panic");
    let error = namespace
        .undelete(inode_id, deleted.committed_seq, &actor)
        .await
        .expect_err("an undelete after the expiry");
    assert_eq!(error.code(), ErrorCode::NotDeleted, "{error:?}");
}
