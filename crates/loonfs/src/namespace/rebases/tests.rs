//! Replay commits that race another writer of the rebase's new fork.

use super::super::merges::{PlannedCommit, PlannedMerge};
use crate::{ActorId, CommitId, DeleteOptions, ErrorCode, LoonFs};
use loonfs_core::MergeRequest;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::{DeleteDirectoryBehavior, MergeConflictKind};
use std::sync::Arc;

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

/// Checks that a replay commit that deletes a directory expects the new
/// fork's head. Another writer creates `/d/new.txt` in the new fork between
/// the plan and the commit, so the commit fails with `stale_head`, and the
/// next plan reports `/d` as a `subtree` conflict and keeps the file.
///
/// The fork's handle is dropped right after the fork deletes `/d`, so the
/// runtime keeps the fork's head on the fork's first manifest, and the plan
/// has to read the manifest that its fold of the fork publishes.
#[tokio::test]
async fn a_replay_that_deletes_a_directory_plans_again_after_the_new_fork_changes() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store)
        .writer_id(writer_id("rebase-writer"))
        .build()
        .await
        .expect("runtime");
    let [main_id, fork_id, rebase_id] = ["main", "main-fork", "main-rebase"].map(namespace_id);
    runtime
        .create_namespace(&main_id, &actor())
        .await
        .expect("main");
    let main = runtime.open_namespace(&main_id).expect("open main");
    for path in ["/d/x.txt", "/d/y.txt"] {
        main.put_file(path, b"a", &actor()).await.expect("seed");
    }
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    let deleted = fork
        .delete_path_with_options(
            "/d",
            &actor(),
            &DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete /d in the fork");
    drop(fork);
    runtime.drain().await.expect("end the fork's session");
    runtime
        .fork_namespace_for_rebase(&main_id, &fork_id, &rebase_id, &actor())
        .await
        .expect("the rebase's new fork");
    let rebase = runtime
        .open_namespace(&rebase_id)
        .expect("open the new fork");
    let request = MergeRequest {
        source_namespace_id: fork_id.clone(),
        through_seq: None,
        after_seq: None,
        commit_id: CommitId::parse("replay").expect("commit id"),
        actor_id: actor(),
        message: None,
    };
    let PlannedMerge::Candidate(planned) = rebase.plan_replay(&request, None).await.expect("plan")
    else {
        panic!("expected a plan");
    };
    let PlannedCommit {
        candidate,
        slot,
        record,
        report,
    } = *planned;
    assert_eq!(record.source_through_seq, deleted.committed_seq);
    assert_eq!(report, Vec::new());
    rebase
        .put_file("/d/new.txt", b"new", &actor())
        .await
        .expect("another writer of the new fork");

    let error = rebase
        .submit_merge(candidate, &slot)
        .await
        .expect_err("the new fork's head moved");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    let replan = MergeRequest {
        after_seq: Some(record.source_after_seq),
        through_seq: Some(record.source_through_seq),
        ..request
    };
    let PlannedMerge::Candidate(replanned) = rebase
        .plan_replay(&replan, Some(slot))
        .await
        .expect("the second plan")
    else {
        panic!("expected a plan");
    };
    let kinds: Vec<_> = replanned
        .report
        .iter()
        .map(|conflict| (conflict.kind, conflict.ours.is_none()))
        .collect();
    assert_eq!(kinds, [(MergeConflictKind::Subtree, true)]);
    rebase
        .submit_merge(replanned.candidate, &replanned.slot)
        .await
        .expect("the second plan commits");
    assert_eq!(
        rebase
            .read_file("/d/new.txt")
            .await
            .expect("the file")
            .bytes,
        b"new"
    );
}
