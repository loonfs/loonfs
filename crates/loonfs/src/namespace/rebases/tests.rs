//! Replay commits that race another writer of the rebase's new fork,
//! retries of a replay commit that committed, which return its conflict
//! report, and a rebase that the fork's own retention floor refuses.

mod merges_after_rebase;

use super::super::merges::{PlannedCommit, PlannedMerge};
use crate::test_support::maintenance_past_the_grace_window;
use crate::{
    ActorId, CommitId, DeleteOptions, ErrorCode, LoonFs, Maintenance, MergeForkOptions,
    NamespaceId, PutFileOptions, ReplayForkResponse, Writable,
};
use loonfs_core::MergeRequest;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::{DeleteDirectoryBehavior, DestinationBehavior, ManifestNo, MergeConflictKind};
use std::sync::Arc;

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

/// Checks that a replay commit of a fork whose `/b.txt` is a piece in its
/// WAL tail applies the file without folding the fork, and that a retry of
/// the replay commit returns the new fork with the replay commit's empty
/// conflict report and folds neither the fork nor the new fork. Each has a
/// later commit in its WAL tail when the retry runs, and neither publishes a
/// manifest.
#[tokio::test]
async fn a_replay_and_its_retry_fold_nothing() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store, crate::Context::Service)
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
    main.put_file("/a.txt", b"a", &actor()).await.expect("seed");
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    fork.put_file("/b.txt", b"b", &actor())
        .await
        .expect("write in the fork");
    runtime
        .fork_namespace_for_rebase(&main_id, &fork_id, &rebase_id, &actor())
        .await
        .expect("the rebase's new fork");
    let rebase = runtime
        .open_namespace(&rebase_id)
        .expect("open the new fork");
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("replay").expect("commit id")),
        ..MergeForkOptions::default()
    };
    let maintenance = rebase.maintenance();
    let unfolded = manifest_nos(&maintenance, [&fork_id, &rebase_id]).await;
    let replayed = rebase
        .replay_fork_with_options(&fork_id, &actor(), &options)
        .await
        .expect("the replay");
    assert_eq!(replayed.conflicts, Vec::new());
    assert_eq!(
        manifest_nos(&maintenance, [&fork_id, &rebase_id]).await[0],
        unfolded[0],
        "the replay publishes no manifest of the fork"
    );
    let file = rebase.read_file("/b.txt").await.expect("the replayed file");
    assert_eq!(file.bytes, b"b");
    for namespace in [&fork, &rebase] {
        namespace
            .put_file("/later.txt", b"later", &actor())
            .await
            .expect("a later commit");
    }
    let before = manifest_nos(&maintenance, [&fork_id, &rebase_id]).await;
    let retried = rebase
        .replay_fork_with_options(&fork_id, &actor(), &options)
        .await
        .expect("the retry");
    assert_eq!(retried.conflicts, Vec::new());
    assert_eq!(
        manifest_nos(&maintenance, [&fork_id, &rebase_id]).await,
        before
    );
}

/// Builds a runtime on `store` with the writer `writer`.
async fn runtime_on(store: Arc<LocalFsStore>, writer: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(writer_id(writer))
        .build()
        .await
        .expect("runtime")
}

/// Creates `main` with `/a.txt`, forks it as `main-fork`, changes `/a.txt` in
/// both, and creates the rebase's new fork `main-rebase`, so a replay commit
/// of the fork reports one `content` conflict. Returns the fork's ID and the
/// new fork's ID.
async fn rebase_with_one_conflict(runtime: &LoonFs<Writable>) -> (NamespaceId, NamespaceId) {
    let [main_id, fork_id, rebase_id] = ["main", "main-fork", "main-rebase"].map(namespace_id);
    runtime
        .create_namespace(&main_id, &actor())
        .await
        .expect("main");
    let main = runtime.open_namespace(&main_id).expect("open main");
    main.put_file("/a.txt", b"base", &actor())
        .await
        .expect("seed");
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::default()
    };
    fork.put_file_with_options("/a.txt", b"ours", &actor(), &replace)
        .await
        .expect("change the file in the fork");
    main.put_file_with_options("/a.txt", b"theirs", &actor(), &replace)
        .await
        .expect("change the file in main");
    runtime
        .fork_namespace_for_rebase(&main_id, &fork_id, &rebase_id, &actor())
        .await
        .expect("the rebase's new fork");
    (fork_id, rebase_id)
}

/// Replays `fork_id` into `rebase_id` under the commit ID `replay`.
async fn replay(
    runtime: &LoonFs<Writable>,
    fork_id: &NamespaceId,
    rebase_id: &NamespaceId,
) -> ReplayForkResponse {
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("replay").expect("commit id")),
        ..MergeForkOptions::default()
    };
    runtime
        .open_namespace(rebase_id)
        .expect("open the new fork")
        .replay_fork_with_options(fork_id, &actor(), &options)
        .await
        .expect("the replay")
}

/// Checks that a retry of a replay commit that committed returns the
/// conflict report that the first request returned, at the same head.
#[tokio::test]
async fn a_retried_replay_commit_returns_its_conflict_report() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = runtime_on(store, "rebase-writer").await;
    let (fork_id, rebase_id) = rebase_with_one_conflict(&runtime).await;

    let replayed = replay(&runtime, &fork_id, &rebase_id).await;
    let kinds: Vec<_> = replayed
        .conflicts
        .iter()
        .map(|conflict| conflict.kind)
        .collect();
    assert_eq!(kinds, [MergeConflictKind::Content]);
    assert_eq!(replayed.commit.committed_seq, replayed.namespace.head_seq);
    assert_eq!(replayed.commit.namespace_id, rebase_id);
    let retried = replay(&runtime, &fork_id, &rebase_id).await;
    assert_eq!(retried.commit, replayed.commit);
    assert_eq!(retried.conflicts, replayed.conflicts);
    assert_eq!(retried.namespace.head_seq, replayed.namespace.head_seq);
}

/// Checks that a new runtime on the same store returns a committed replay
/// commit's conflict report, which it can only read from the replay
/// commit's merge record.
#[tokio::test]
async fn a_fresh_runtime_returns_a_committed_replay_commits_conflict_report() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = runtime_on(store.clone(), "rebase-writer").await;
    let (fork_id, rebase_id) = rebase_with_one_conflict(&runtime).await;
    let replayed = replay(&runtime, &fork_id, &rebase_id).await;
    assert_eq!(replayed.conflicts.len(), 1);
    runtime.drain().await.expect("drain the first runtime");
    drop(runtime);

    let fresh = runtime_on(store, "rebase-writer-2").await;
    let retried = replay(&fresh, &fork_id, &rebase_id).await;
    assert_eq!(retried.conflicts, replayed.conflicts);
    assert_eq!(retried.namespace.head_seq, replayed.namespace.head_seq);
}

/// Checks that a rebase of a fork whose own retention floor is above the
/// start of the fork's next merge returns `below_retention_floor` before it
/// creates the new fork, and folds nothing: main keeps its manifest and the
/// commit in its WAL tail.
#[tokio::test]
async fn a_rebase_of_a_fork_below_its_own_floor_creates_nothing() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
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
    main.put_file("/a.txt", b"a", &actor()).await.expect("seed");
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    for path in ["/b.txt", "/c.txt"] {
        fork.put_file(path, b"b", &actor())
            .await
            .expect("write in the fork");
    }
    let maintenance = fork.maintenance();
    maintenance.fold_wal(&fork_id).await.expect("fold the fork");
    maintenance_past_the_grace_window(store)
        .await
        .advance_retention_floor(&fork_id)
        .await
        .expect("advance the fork's floor");
    main.put_file("/later.txt", b"later", &actor())
        .await
        .expect("a commit in main's tail");
    let before = manifest_nos(&maintenance, [&main_id, &fork_id]).await;

    let error = runtime
        .fork_namespace_for_rebase(&main_id, &fork_id, &rebase_id, &actor())
        .await
        .expect_err("the fork's floor");
    assert_eq!(error.code(), ErrorCode::BelowRetentionFloor);
    assert_eq!(
        manifest_nos(&maintenance, [&main_id, &fork_id]).await,
        before
    );
    let missing = runtime
        .namespace(&rebase_id)
        .metadata()
        .await
        .expect_err("no new fork");
    assert_eq!(missing.code(), ErrorCode::NamespaceNotFound);
}

/// Returns the current manifest number of each namespace.
async fn manifest_nos(
    maintenance: &Maintenance,
    namespace_ids: [&NamespaceId; 2],
) -> Vec<Option<ManifestNo>> {
    let mut numbers = Vec::new();
    for namespace_id in namespace_ids {
        let diagnostics = maintenance
            .diagnostics(namespace_id)
            .await
            .expect("diagnostics");
        numbers.push(diagnostics.current_manifest_no);
    }
    numbers
}

/// Checks that a replay commit that deletes a directory expects the new
/// fork's head. Another writer creates `/d/new.txt` in the new fork between
/// the plan and the commit, so the commit fails with `stale_head`, and the
/// next plan reports `/d` as a `subtree` conflict and keeps the file.
///
/// The fork's handle is dropped right after the fork deletes `/d`, so the
/// runtime keeps the fork's head on the fork's first manifest, and the plan
/// has to read the delete from the fork's WAL tail.
#[tokio::test]
async fn a_replay_that_deletes_a_directory_plans_again_after_the_new_fork_changes() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store, crate::Context::Service)
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
        reservation,
        record,
        report,
        ..
    } = *planned;
    assert_eq!(record.source_through_seq, deleted.committed_seq);
    assert_eq!(report, Vec::new());
    rebase
        .put_file("/d/new.txt", b"new", &actor())
        .await
        .expect("another writer of the new fork");

    let error = rebase
        .submit_merge(candidate, &reservation)
        .await
        .expect_err("the new fork's head moved");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    let replan = MergeRequest {
        after_seq: Some(record.source_after_seq),
        through_seq: Some(record.source_through_seq),
        ..request
    };
    let PlannedMerge::Candidate(replanned) = rebase
        .plan_replay(&replan, Some(reservation))
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
        .submit_merge(replanned.candidate, &replanned.reservation)
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
