//! Merges of a fork and of the new forks that rebases made from it: a merge
//! of a new fork covers the range of the fork that its replay commits
//! replayed, so the fork can't merge or rebase again, and of two merges that
//! cover the same range, the second to commit is refused.

use crate::namespace::merges::PlannedMerge;
use crate::{
    ActorId, Commit, CommitId, ErrorCode, LoonFs, MergeForkOptions, Namespace, NamespaceId,
    ReplayForkResponse, Writable,
};
use loonfs_core::MergeRequest;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{request_lines, KeyPredicate, RecordingStore};
use loonfs_types::{InodeId, NextMergeRangeOutcome};
use std::sync::Arc;
use tempfile::TempDir;

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

/// `main`, and its fork `main-fork`, which created `/b.txt` and deleted
/// `/keep.txt`.
struct Diverged {
    _directory: TempDir,
    runtime: LoonFs<Writable>,
    main: Namespace<Writable>,
    fork: Namespace<Writable>,
    /// The inode of `/keep.txt`, which the fork deleted.
    keep: InodeId,
}

/// Creates the namespaces of [`Diverged`] in a runtime on a local store.
async fn diverged() -> Diverged {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    diverged_on(directory, store).await
}

/// Creates the namespaces of [`Diverged`] in a runtime on `store`, which
/// stores its objects in `directory`.
async fn diverged_on(directory: TempDir, store: crate::SharedObjectStore) -> Diverged {
    let runtime = LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(writer_id("rebase-writer"))
        .build()
        .await
        .expect("runtime");
    let [main_id, fork_id] = ["main", "main-fork"].map(namespace_id);
    runtime
        .create_namespace(&main_id, &actor())
        .await
        .expect("main");
    let main = runtime.open_namespace(&main_id).expect("open main");
    for path in ["/a.txt", "/keep.txt"] {
        main.put_file(path, b"base", &actor()).await.expect("seed");
    }
    let keep = main.stat("/keep.txt").await.expect("stat").inode_id;
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    fork.put_file("/b.txt", b"b", &actor())
        .await
        .expect("create a file in the fork");
    fork.delete_path("/keep.txt", &actor())
        .await
        .expect("delete a file in the fork");
    Diverged {
        _directory: directory,
        runtime,
        main,
        fork,
        keep,
    }
}

impl Diverged {
    /// Creates `new_fork_name` as a rebase's new fork of `main` for
    /// `fork_id`, and replays `fork_id` into it.
    async fn rebase(
        &self,
        fork_id: &NamespaceId,
        new_fork_name: &str,
    ) -> (Namespace<Writable>, ReplayForkResponse) {
        let new_fork_id = namespace_id(new_fork_name);
        self.runtime
            .fork_namespace_for_rebase(self.main.id(), fork_id, &new_fork_id, &actor())
            .await
            .expect("the rebase's new fork");
        let new_fork = self
            .runtime
            .open_namespace(&new_fork_id)
            .expect("open the new fork");
        let replayed = new_fork
            .replay_fork(fork_id, &actor())
            .await
            .expect("the replay");
        assert_eq!(replayed.conflicts, Vec::new());
        (new_fork, replayed)
    }

    /// Restores `/keep.txt` in `new_fork`, as a resolver that undoes the
    /// fork's delete would, after `replayed` deleted it.
    async fn restore_keep(&self, new_fork: &Namespace<Writable>, replayed: &ReplayForkResponse) {
        new_fork
            .restore_deleted(self.keep, replayed.commit.committed_seq, &actor())
            .await
            .expect("restore the deleted file");
    }

    /// Merges `source` into `main`.
    async fn merge(&self, source: &NamespaceId) -> crate::Result<Commit> {
        self.main.merge_fork(source, &actor()).await
    }

    /// Returns the outcome of the next-range query of the fork into `main`.
    async fn outcome(&self) -> NextMergeRangeOutcome {
        self.main
            .next_merge_range(self.fork.id())
            .await
            .expect("the next range")
            .outcome
    }

    /// Checks that `main` still has `/keep.txt`, which a resolver restored
    /// in a new fork.
    async fn assert_keep_restored(&self) {
        self.main
            .stat("/keep.txt")
            .await
            .expect("the file that the resolver restored stays");
    }
}

/// Returns the outcome of the next-range query of the fork after
/// `new_fork`'s merge covered the fork's range that `replayed` applied.
fn covered(new_fork: &NamespaceId, replayed: &ReplayForkResponse) -> NextMergeRangeOutcome {
    NextMergeRangeOutcome::Covered {
        merge: replayed
            .commit
            .merge
            .clone()
            .expect("a replay commit's merge record"),
        covered_by_namespace_id: new_fork.clone(),
    }
}

/// Checks that once a new fork merged, the fork's later commit doesn't reach
/// `main`: the next-range query reports `covered`, and a merge and a rebase
/// of the fork return `invalid_request`, whose message identifies the new
/// fork. The resolver restored a file that the fork deleted, and main's row
/// for that file is from before the fork point, so a later merge of the fork
/// could delete the file again without a conflict.
#[tokio::test]
async fn a_fork_whose_new_fork_merged_can_neither_merge_nor_rebase_again() {
    let family = diverged().await;
    let (new_fork, replayed) = family.rebase(family.fork.id(), "main-rebase").await;
    family.restore_keep(&new_fork, &replayed).await;
    family
        .merge(new_fork.id())
        .await
        .expect("merge the new fork");
    family
        .fork
        .put_file("/later.txt", b"later", &actor())
        .await
        .expect("a later commit in the fork");
    assert_eq!(family.outcome().await, covered(new_fork.id(), &replayed));

    let refused = namespace_id("main-rebase-2");
    let merge_error = family
        .merge(family.fork.id())
        .await
        .expect_err("a merge of the fork");
    let rebase_error = family
        .runtime
        .fork_namespace_for_rebase(family.main.id(), family.fork.id(), &refused, &actor())
        .await
        .expect_err("a rebase of the fork");
    for error in [merge_error, rebase_error] {
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "{error:?}");
        assert_eq!(
            error.invalid_request_param().as_deref(),
            Some("source_namespace_id")
        );
        assert!(
            error.to_string().contains("`main-rebase`"),
            "the message identifies the new fork: {error}"
        );
    }
    family
        .runtime
        .open_namespace(&refused)
        .expect("open the refused new fork")
        .metadata()
        .await
        .expect_err("the refused rebase creates no new fork");
    family.assert_keep_restored().await;
    family
        .main
        .stat("/later.txt")
        .await
        .expect_err("the fork's later commit stays in the fork");
}

/// Checks that a merge of the last new fork of a chain covers the fork's
/// replayed range too, after the first new fork was deleted, so after both
/// new forks are deleted, the next-range query of the fork reports
/// `covered` by the last new fork, which is what a merge session's recovery
/// reads.
#[tokio::test]
async fn a_merge_of_the_last_new_fork_of_a_chain_covers_the_fork() {
    let family = diverged().await;
    let (first, replayed) = family.rebase(family.fork.id(), "main-rebase").await;
    family.restore_keep(&first, &replayed).await;
    let (second, _) = family.rebase(first.id(), "main-rebase-2").await;
    first.delete().await.expect("delete the first new fork");
    family
        .merge(second.id())
        .await
        .expect("merge the second new fork");
    second.delete().await.expect("delete the second new fork");

    assert_eq!(family.outcome().await, covered(second.id(), &replayed));
    family.assert_keep_restored().await;
}

/// Checks that of two new forks that rebases of the fork made from the same
/// range, a merge of the second is refused with `merge_range_conflict` for
/// the fork once the first merged, before it creates the fork's inodes in
/// main a second time.
#[tokio::test]
async fn a_second_new_fork_of_the_fork_is_refused_after_its_sibling_merged() {
    let family = diverged().await;
    let (first, _) = family.rebase(family.fork.id(), "main-rebase").await;
    let (second, _) = family.rebase(family.fork.id(), "main-rebase-2").await;
    family.merge(first.id()).await.expect("merge the first");

    let error = family
        .merge(second.id())
        .await
        .expect_err("the second covers the same range of the fork");
    assert_eq!(error.code(), ErrorCode::MergeRangeConflict, "{error:?}");
    let details = error.to_error_response().details.expect("details");
    assert_eq!(details.source_namespace_id.as_ref(), Some(family.fork.id()));
}

/// Checks that when a merge of the fork and a merge of its new fork are
/// both planned before either commits, the second to commit is refused and
/// commits nothing, so it creates no inode that the first created. A merge
/// of the new fork after the fork's merge returns `merge_range_conflict`,
/// and a merge of the fork after the new fork's merge returns
/// `invalid_request`.
#[tokio::test]
async fn a_merge_of_the_fork_and_of_its_new_fork_refuse_the_second_to_commit() {
    for new_fork_first in [true, false] {
        let family = diverged().await;
        let (new_fork, _) = family.rebase(family.fork.id(), "main-rebase").await;
        let (first, late) = if new_fork_first {
            (new_fork.id(), family.fork.id())
        } else {
            (family.fork.id(), new_fork.id())
        };
        let request = MergeRequest {
            source_namespace_id: late.clone(),
            through_seq: None,
            after_seq: None,
            commit_id: CommitId::parse("late").expect("commit id"),
            actor_id: actor(),
            message: None,
        };
        let PlannedMerge::Candidate(planned) = family
            .main
            .plan_merge(&request, None)
            .await
            .expect("plan the late merge")
        else {
            panic!("the late merge hasn't committed");
        };
        drop(planned.reservation);
        let committed = family.merge(first).await.expect("the first merge");

        let reservation = family
            .main
            .session()
            .reserve_merge()
            .expect("a reservation");
        let error = family
            .main
            .submit_merge(planned.candidate, &reservation)
            .await
            .expect_err("the late merge covers the first merge's range");
        // Once the new fork merged, the fork's own merge is refused because
        // the new fork covered the fork.
        let expected = if new_fork_first {
            ErrorCode::InvalidRequest
        } else {
            ErrorCode::MergeRangeConflict
        };
        assert_eq!(
            error.code(),
            expected,
            "new fork first: {new_fork_first}: {error:?}"
        );
        let head_seq = family.main.metadata().await.expect("main").head_seq;
        assert_eq!(
            head_seq, committed.committed_seq,
            "the refused merge commits nothing, so it creates no inode"
        );
    }
}

/// Checks that a retry of a new fork's merge returns the first attempt's
/// commit after the new fork gained a commit, and leaves the fork's range in
/// main's merge index where the first attempt put it.
#[tokio::test]
async fn a_retry_of_the_new_forks_merge_returns_the_first_attempts_commit() {
    let family = diverged().await;
    let (new_fork, replayed) = family.rebase(family.fork.id(), "main-rebase").await;
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("merge-new-fork").expect("commit id")),
        ..MergeForkOptions::default()
    };
    let first = family
        .main
        .merge_fork_with_options(new_fork.id(), &actor(), &options)
        .await
        .expect("the first attempt");
    new_fork
        .put_file("/more.txt", b"more", &actor())
        .await
        .expect("a later commit in the new fork");

    let retried = family
        .main
        .merge_fork_with_options(new_fork.id(), &actor(), &options)
        .await
        .expect("the retry");

    assert_eq!(retried, first);
    assert_eq!(family.outcome().await, covered(new_fork.id(), &replayed));
    family
        .main
        .stat("/more.txt")
        .await
        .expect_err("the retry applies nothing new");
}

/// Checks the exact requests of a merge of a rebase's new fork in a writer
/// runtime with fresh positions on every namespace. The merge takes the
/// range of the fork that the replay commit covered from the new fork's
/// commits, which it pages through anyway, and its two lookups of the fork
/// in main's merge index (at the plan and at admission) send no request
/// here, so it sends the requests of an ordinary merge
/// (`a_merge_with_fresh_positions_sends_these_requests_in_order`), less the
/// extent write, which the replay commit already made.
#[tokio::test]
async fn a_merge_of_a_new_fork_with_fresh_positions_sends_these_requests_in_order() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let family = diverged_on(directory, store.clone()).await;
    let (new_fork, _) = family.rebase(family.fork.id(), "main-rebase").await;
    store.reset();

    family.merge(new_fork.id()).await.expect("the merge");

    let expected = [
        // The receipt lookup's `strong` read of `main`.
        "head main MetadataManifest 00000000000000000004",
        "get main Boundary",
        // The snapshot of the new fork's current manifest, and the check that
        // it's still current.
        "put main-rebase Snapshot",
        "head main-rebase MetadataManifest 00000000000000000005",
        "get main-rebase Boundary",
        // The commit.
        "put main WalObject 00000000000000000004",
    ];
    assert_eq!(request_lines(&store.take()), expected);
}
