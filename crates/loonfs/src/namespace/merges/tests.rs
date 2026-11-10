//! Merges that race another writer of the namespace they merge into, merges
//! that keep their reservation, merges from a source that isn't a live fork,
//! and merges on a runtime that has merges turned off.

use super::{missing_fork, MergeOutcome, PlannedCommit, PlannedMerge};
use crate::publish::{CommitIntent, FilesystemOperation};
use crate::publisher::MergeReservation;
use crate::{
    ActorId, Commit, CommitId, CoreError, DeleteOptions, ErrorCode, ExecutionBudget, LoonFs,
    MergeForkOptions, MoveOptions, Namespace, NamespaceSessionStatus, Result, Writable,
};
use futures::future::BoxFuture;
use loonfs_core::MergeRequest;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::{
    AbsolutePath, DeleteDirectoryBehavior, ManifestNo, MergeConflictKind, FEATURE_MERGES,
};
use std::assert_matches;
use std::sync::Arc;
use tempfile::TempDir;

struct Family {
    _directory: TempDir,
    runtime: LoonFs<Writable>,
    main: Namespace<Writable>,
    fork: Namespace<Writable>,
}

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

/// Creates `main` with `/a.txt`, `/d/x.txt`, and `/d/y.txt`, forks it, and
/// writes `/b.txt` in the fork.
async fn family() -> Family {
    let directory = tempfile::tempdir().expect("tempdir");
    let main_id = namespace_id("main");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store)
        .writer_id(writer_id("merge-writer"))
        .build()
        .await
        .expect("runtime");
    runtime
        .create_namespace(&main_id, &actor())
        .await
        .expect("main");
    let main = runtime.open_namespace(&main_id).expect("open main");
    for path in ["/a.txt", "/d/x.txt", "/d/y.txt"] {
        main.put_file(path, b"a", &actor()).await.expect("seed");
    }
    let fork_id = namespace_id("main-fork");
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open fork");
    fork.put_file("/b.txt", b"b", &actor())
        .await
        .expect("write in the fork");
    Family {
        _directory: directory,
        runtime,
        main,
        fork,
    }
}

impl Family {
    fn request(&self, commit_id: &str) -> MergeRequest {
        MergeRequest {
            source_namespace_id: self.fork.id().clone(),
            through_seq: None,
            after_seq: None,
            commit_id: CommitId::parse(commit_id).expect("commit id"),
            actor_id: actor(),
            message: None,
        }
    }

    /// Plans `request` under a reservation that the plan takes.
    async fn plan(&self, request: &MergeRequest) -> PlannedCommit {
        match self.main.plan_merge(request, None).await.expect("plan") {
            PlannedMerge::Candidate(planned) => *planned,
            PlannedMerge::Committed(commit) => panic!("expected a plan, got {commit:?}"),
        }
    }

    /// Deletes `/d` in the fork, so a merge deletes a directory.
    async fn delete_d_in_the_fork(&self) {
        self.fork
            .delete_path_with_options(
                "/d",
                &actor(),
                &DeleteOptions {
                    behavior: DeleteDirectoryBehavior::Recursive,
                    ..DeleteOptions::default()
                },
            )
            .await
            .expect("delete the directory in the fork");
    }

    /// Returns the current manifest numbers of `main` and the fork.
    async fn manifest_nos(&self) -> Vec<Option<ManifestNo>> {
        let maintenance = self.main.maintenance();
        let mut numbers = Vec::new();
        for namespace in [&self.main, &self.fork] {
            let diagnostics = maintenance
                .diagnostics(namespace.id())
                .await
                .expect("diagnostics");
            numbers.push(diagnostics.current_manifest_no);
        }
        numbers
    }

    /// Commits a file in `main` that no merge here touches.
    async fn write_main(&self, path: &str) {
        self.main
            .put_file(path, b"main", &actor())
            .await
            .expect("an unrelated write in main");
    }

    /// Creates 1,024 directories in the fork below a directory whose path
    /// has 15 components of 240 bytes, merges them into `main`, and then
    /// renames each directory in the fork and back again, so the fork's next
    /// merge has an empty net change. The renames commit 256 moves at a
    /// time, which stays below the namespace's admitted bytes.
    ///
    /// Each directory's path is about 3,600 bytes, and a path cache entry
    /// with the path's components and its hash table slot estimates at about
    /// 7.7 KB. The next plan reads each directory at the range's start and
    /// at its end, so its path caches estimate at about 15 MiB, which is
    /// more than the reservation floor of 8 MiB.
    async fn undo_renames_in_the_fork(&self) {
        let parent = format!("/{}", vec!["p".repeat(240); 15].join("/"));
        let path = |name: &str, index: usize| {
            AbsolutePath::parse(format!("{parent}/{name}-{index:04}")).expect("path")
        };
        let commit = |operations| {
            self.fork.commit(CommitIntent {
                commit_id: CommitId::generate(),
                actor_id: actor(),
                subject: None,
                message: None,
                operations,
                preconditions: Vec::new(),
            })
        };
        let parents = FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(&parent).expect("path"),
            parents: true,
        };
        let creates = (0..1024).map(|index| FilesystemOperation::CreateDirectory {
            path: path("d", index),
            parents: false,
        });
        commit(std::iter::once(parents).chain(creates).collect())
            .await
            .expect("create the directories in the fork");
        self.main
            .merge_fork(self.fork.id(), &actor())
            .await
            .expect("merge the directories");
        for (from, to) in [("d", "m"), ("m", "d")] {
            for first in (0..1024).step_by(256) {
                let moves = (first..first + 256).map(|index| {
                    FilesystemOperation::move_path(
                        path(from, index),
                        path(to, index),
                        &MoveOptions::default(),
                    )
                });
                commit(moves.collect())
                    .await
                    .expect("rename the directories in the fork");
            }
        }
    }

    /// Commits `planned` as the merge route does, and plans again with
    /// `replan` after a commit that `main` changed under.
    async fn commit<'a>(
        &'a self,
        request: &MergeRequest,
        planned: PlannedCommit,
        replan: impl FnMut(MergeRequest, MergeReservation) -> BoxFuture<'a, Result<PlannedMerge>>,
    ) -> Result<Commit> {
        let (MergeOutcome::Found(commit) | MergeOutcome::Committed { commit, .. }) = self
            .main
            .commit_merge(request, PlannedMerge::Candidate(Box::new(planned)), replan)
            .await?;
        Ok(commit)
    }
}

/// Checks that a runtime whose merge allowance is zero answers a merge, a
/// rebase, and the next-range query with `not_supported` for
/// `filesystem.merges`, and that the rebase creates no new fork.
#[tokio::test]
async fn a_zero_allowance_answers_every_merge_route_with_not_supported() {
    let family = family().await;
    let store = Arc::new(LocalFsStore::new(family._directory.path()).expect("store"));
    let runtime = LoonFs::builder_with_store(store)
        .writer_id(writer_id("merges-off"))
        .execution_budget(ExecutionBudget::builder().max_merge_bytes(0).build())
        .build()
        .await
        .expect("runtime");
    let main = runtime.open_namespace(family.main.id()).expect("open main");
    let new_fork_id = namespace_id("main-rebase");
    let refused = [
        main.merge_fork(family.fork.id(), &actor())
            .await
            .map(drop)
            .expect_err("a merge"),
        runtime
            .fork_namespace_for_rebase(family.main.id(), family.fork.id(), &new_fork_id, &actor())
            .await
            .map(drop)
            .expect_err("a rebase"),
        main.next_merge_range(family.fork.id())
            .await
            .map(drop)
            .expect_err("the next-range query"),
    ];
    for error in refused {
        let response = error.to_error_response();
        assert_eq!(error.code(), ErrorCode::NotSupported, "{error:?}");
        assert_eq!(response.feature.as_deref(), Some(FEATURE_MERGES));
    }
    let missing = runtime
        .namespace(&new_fork_id)
        .metadata()
        .await
        .expect_err("no new fork");
    assert_eq!(missing.code(), ErrorCode::NamespaceNotFound);
}

/// Checks that a merge planned before another merge of the same fork
/// committed, and submitted after that merge settled, reaches the writer's
/// range check, which refuses it with `merge_range_conflict` because the
/// first merge already applied its range.
#[tokio::test]
async fn a_merge_planned_before_another_commits_fails_the_range_check() {
    let family = family().await;
    let late = family.plan(&family.request("late")).await;
    drop(late.reservation);
    family
        .main
        .merge_fork_with_options(
            family.fork.id(),
            &actor(),
            &MergeForkOptions {
                commit_id: Some(CommitId::parse("first").expect("commit id")),
                ..MergeForkOptions::default()
            },
        )
        .await
        .expect("the first merge");

    let reservation = family
        .main
        .session()
        .reserve_merge()
        .expect("a reservation");
    let error = family
        .main
        .submit_merge(late.candidate, &reservation)
        .await
        .expect_err("the late merge repeats the first merge's range");
    assert_eq!(error.code(), ErrorCode::MergeRangeConflict);
}

/// Checks that a merge reserves its share of the merge allowance before it
/// plans and keeps the reservation until its commit settles. While the other
/// reservations fill the allowance, a merge gets `server_busy`, commits
/// nothing, and leaves the allowance as it was, and the same merge commits
/// once the allowance has room.
#[tokio::test]
async fn a_merge_keeps_its_reservation_from_its_plan() {
    let family = family().await;
    let budget = family.runtime.execution_budget().clone();
    let floor = 8 * 1024 * 1024;
    let planned = family.plan(&family.request("first")).await;
    assert_eq!(budget.stats().admitted_merges, 1);
    assert_eq!(budget.stats().admitted_merge_bytes, floor);
    family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect("the first merge");
    drop(planned.reservation);
    assert_eq!(budget.stats().admitted_merge_bytes, 0);

    family
        .fork
        .put_file("/c.txt", b"c", &actor())
        .await
        .expect("write in the fork");
    let others = (0..budget.max_merge_bytes() / floor)
        .map(|_| {
            family
                .main
                .session()
                .reserve_merge()
                .expect("a reservation")
        })
        .collect::<Vec<_>>();
    let full = budget.stats();
    let head = family.main.metadata().await.expect("metadata").head_seq;
    let error = family
        .main
        .merge_fork(family.fork.id(), &actor())
        .await
        .expect_err("the allowance is full");
    assert_eq!(error.code(), ErrorCode::ServerBusy);
    assert_eq!(budget.stats(), full);
    let metadata = family.main.metadata().await.expect("metadata");
    assert_eq!(metadata.head_seq, head, "the refused merge commits nothing");
    drop(others);
    family
        .main
        .merge_fork(family.fork.id(), &actor())
        .await
        .expect("the merge once the allowance has room");
}

/// Checks that a merge whose net change is empty reserves the memory that
/// its plan uses. The plan's path caches grow the reservation above the
/// floor (see [`Family::undo_renames_in_the_fork`]), the merge commits a
/// merge record with no changes, and the reservation returns to the
/// allowance once the merge settles.
#[tokio::test]
async fn a_merge_of_an_undone_change_reserves_the_memory_that_its_plan_uses() {
    let family = family().await;
    family.undo_renames_in_the_fork().await;
    let budget = family.runtime.execution_budget().clone();
    let planned = family.plan(&family.request("undone")).await;
    let reserved = budget.stats().admitted_merge_bytes;
    assert!(reserved > 8 * 1024 * 1024, "{reserved} bytes reserved");
    let commit = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect("the merge");
    assert!(commit.merge.is_some());
    assert_eq!(commit.events, Vec::new());
    drop(planned.reservation);
    assert_eq!(budget.stats().admitted_merge_bytes, 0);
}

/// Checks that a merge whose plan uses more memory than the other merges'
/// reservations leave room for gets `server_busy`, commits nothing, and
/// leaves the allowance as it was. The other reservations leave one floor
/// of 8 MiB, which the plan's path caches outgrow (see
/// [`Family::undo_renames_in_the_fork`]).
#[tokio::test]
async fn a_merge_whose_plan_memory_doesnt_fit_beside_other_reservations_is_busy() {
    let family = family().await;
    family.undo_renames_in_the_fork().await;
    let budget = family.runtime.execution_budget().clone();
    let floor = 8 * 1024 * 1024;
    let others = (1..budget.max_merge_bytes() / floor)
        .map(|_| {
            family
                .main
                .session()
                .reserve_merge()
                .expect("a reservation")
        })
        .collect::<Vec<_>>();
    let full = budget.stats();
    let head = family.main.metadata().await.expect("metadata").head_seq;
    let error = family
        .main
        .merge_fork(family.fork.id(), &actor())
        .await
        .expect_err("the plan's memory doesn't fit");
    assert_eq!(error.code(), ErrorCode::ServerBusy);
    assert_eq!(budget.stats(), full);
    let metadata = family.main.metadata().await.expect("metadata");
    assert_eq!(metadata.head_seq, head, "the refused merge commits nothing");
    drop(others);
}

/// Checks that a merge after the caller dropped the fork's handle, right
/// after a commit, merges that commit. The fork's session ends on the fork's
/// first manifest, which folded none of the fork's commits, so the runtime
/// keeps the fork's head on that manifest, and the merge has to read the
/// manifest that its fold of the fork publishes.
#[tokio::test]
async fn a_merge_after_the_forks_handle_is_dropped_merges_its_last_commit() {
    let Family {
        _directory: _store,
        runtime,
        main,
        fork,
    } = family().await;
    let fork_id = fork.id().clone();
    let last = fork
        .put_file("/c.txt", b"c", &actor())
        .await
        .expect("write in the fork");
    drop(fork);
    runtime.drain().await.expect("end the fork's session");

    let merged = main
        .merge_fork(&fork_id, &actor())
        .await
        .expect("the merge");
    let record = merged.merge.expect("a merge record");
    assert_eq!(record.source_through_seq, last.committed_seq);
    let file = main
        .read_file("/c.txt")
        .await
        .expect("the fork's last file");
    assert_eq!(file.bytes, b"c");
}

/// Checks that a merge reads the fork at the manifest that its fold
/// publishes, not at an older manifest that folded some of the fork's
/// commits. A fold of the fork covers `/b.txt`, the fork then commits
/// `/c.txt`, and its handle is dropped, so the runtime keeps the fork's
/// head on the older manifest. The merge applies both files, through the
/// fork's head.
#[tokio::test]
async fn a_merge_reads_the_fork_at_its_folds_manifest() {
    let Family {
        _directory: _store,
        runtime,
        main,
        fork,
    } = family().await;
    let fork_id = fork.id().clone();
    main.maintenance()
        .fold_wal(&fork_id)
        .await
        .expect("fold the fork's first commit");
    let last = fork
        .put_file("/c.txt", b"c", &actor())
        .await
        .expect("write in the fork");
    drop(fork);
    runtime.drain().await.expect("end the fork's session");

    let merged = main
        .merge_fork(&fork_id, &actor())
        .await
        .expect("the merge");
    let record = merged.merge.expect("a merge record");
    assert_eq!(record.source_through_seq, last.committed_seq);
    for (path, bytes) in [("/b.txt", &b"b"[..]), ("/c.txt", &b"c"[..])] {
        let file = main.read_file(path).await.expect("the fork's file");
        assert_eq!(file.bytes, bytes, "{path}");
    }
}

/// Checks that a merge's plan pins the fork's folded manifest while the
/// merge runs, and that the merge deletes the pin once it commits.
#[tokio::test]
async fn a_merge_releases_the_pin_its_plan_took_on_the_fork() {
    let family = family().await;
    let request = family.request("merge-pin");
    let planned = family.plan(&request).await;
    let pinned = family
        .fork
        .list_snapshots()
        .page()
        .await
        .expect("the fork's snapshots while the merge is planned");
    assert_eq!(
        pinned
            .snapshots
            .iter()
            .map(|snapshot| snapshot.snapshot_id.clone())
            .collect::<Vec<_>>(),
        vec![planned.fork_pin.clone()],
        "the plan pins the fork"
    );
    let mut replans = 0;
    family
        .commit(&request, planned, |request, reservation| {
            replans += 1;
            let main = &family.main;
            Box::pin(async move { main.plan_merge(&request, Some(reservation)).await })
        })
        .await
        .expect("the merge");
    assert_eq!(replans, 0, "the plan commits");
    let released = family
        .fork
        .list_snapshots()
        .page()
        .await
        .expect("the fork's snapshots after the merge");
    assert!(
        released.snapshots.is_empty(),
        "the merge releases its pin: {:?}",
        released.snapshots
    );
}

/// Checks that a retry of a merge that's still planned plans and commits the
/// merge, and that the first plan's commit, submitted after the retry
/// committed, returns the retry's commit from the writer's receipt check, so
/// the merge is applied once.
#[tokio::test]
async fn a_retry_of_a_merge_in_flight_commits_the_merge_once() {
    let family = family().await;
    let request = family.request("merge");
    let PlannedCommit {
        candidate,
        reservation,
        ..
    } = family.plan(&request).await;
    let options = MergeForkOptions {
        commit_id: Some(request.commit_id.clone()),
        ..MergeForkOptions::default()
    };
    let retried = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("the retry");
    let head = family.main.metadata().await.expect("metadata").head_seq;
    let merged = family
        .main
        .submit_merge(candidate, &reservation)
        .await
        .expect("the first plan's commit");
    assert_eq!(merged, retried);
    let metadata = family.main.metadata().await.expect("metadata");
    assert_eq!(metadata.head_seq, head, "the merge is applied once");
}

/// Checks that a retry of a merge that committed returns the merge and folds
/// neither namespace. `main` and the fork each have a later commit in their
/// WAL tails when the retry runs, and neither publishes a manifest.
#[tokio::test]
async fn a_retry_of_a_committed_merge_folds_nothing() {
    let family = family().await;
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("merge").expect("commit id")),
        ..MergeForkOptions::default()
    };
    let merged = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("the merge");
    family.write_main("/later.txt").await;
    family
        .fork
        .put_file("/c.txt", b"c", &actor())
        .await
        .expect("a later commit in the fork");
    let before = family.manifest_nos().await;
    let retried = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("the retry");
    assert_eq!(retried, merged);
    assert_eq!(family.manifest_nos().await, before);
}

/// Checks that a merge's delete of a file expects the file's revision. `main`
/// writes `/a.txt`, which the fork deleted, between the plan and the commit,
/// the commit fails with `stale_revision`, and the next plan reports the
/// change as a `content` conflict.
#[tokio::test]
async fn a_merge_that_deletes_a_file_plans_again_after_a_write() {
    let family = family().await;
    family
        .fork
        .delete_path("/a.txt", &actor())
        .await
        .expect("delete in the fork");
    let request = family.request("delete-a");
    let planned = family.plan(&request).await;
    family
        .main
        .put_file_with_options(
            "/a.txt",
            b"theirs",
            &actor(),
            &crate::PutFileOptions {
                behavior: loonfs_types::DestinationBehavior::Replace,
                ..crate::PutFileOptions::default()
            },
        )
        .await
        .expect("edit in main");

    let error = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect_err("the file changed");
    assert_eq!(error.code(), ErrorCode::StaleRevision);
    let Err(error) = family
        .main
        .plan_merge(&request, Some(planned.reservation))
        .await
    else {
        panic!("the second plan finds the conflict");
    };
    assert_matches!(
        error,
        crate::Error::Core(CoreError::MergeConflict { ref conflict, .. })
            if conflict.kind == MergeConflictKind::Content
    );
}

/// Checks that a merge that deletes a directory expects the head that it was
/// planned at. A file created below the directory between planning and
/// publication fails the commit with `stale_head`, and the next plan reports
/// the directory as a `subtree` conflict.
#[tokio::test]
async fn a_merge_that_deletes_a_directory_plans_again_after_the_head_moves() {
    let family = family().await;
    family.delete_d_in_the_fork().await;
    let request = family.request("delete-d");
    let planned = family.plan(&request).await;
    family.write_main("/d/new.txt").await;

    let error = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect_err("the head moved");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    let Err(error) = family
        .main
        .plan_merge(&request, Some(planned.reservation))
        .await
    else {
        panic!("the second plan finds the conflict");
    };
    assert_matches!(
        error,
        crate::Error::Core(CoreError::MergeConflict { ref conflict, .. })
            if conflict.kind == MergeConflictKind::Subtree
    );
}

/// Checks that a merge that moves a directory expects the head that it was
/// planned at, so an unrelated commit between planning and publication
/// fails it with `stale_head`.
#[tokio::test]
async fn a_merge_that_moves_a_directory_expects_its_head() {
    let family = family().await;
    family
        .fork
        .move_path("/d", "/e", &actor())
        .await
        .expect("move the directory in the fork");
    let planned = family.plan(&family.request("move-d")).await;
    family.write_main("/other.txt").await;
    let error = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect_err("the head moved");
    assert_eq!(error.code(), ErrorCode::StaleHead);
}

/// Checks that a merge that moves a file through a temporary name expects
/// the head that it was planned at. The fork swaps the names of `/d/x.txt`
/// and `/d/y.txt`, so one of them moves through a temporary name, and `main`
/// renames `/d` to `/e` between planning and publication. The commit fails
/// with `stale_head`, and the next plan commits the swap under `/e`.
#[tokio::test]
async fn a_merge_through_a_temporary_name_plans_again_after_a_rename() {
    let family = family().await;
    let x = family.main.stat("/d/x.txt").await.expect("x").inode_id;
    let y = family.main.stat("/d/y.txt").await.expect("y").inode_id;
    for (from, to) in [
        ("/d/x.txt", "/d/t.txt"),
        ("/d/y.txt", "/d/x.txt"),
        ("/d/t.txt", "/d/y.txt"),
    ] {
        family
            .fork
            .move_path(from, to, &actor())
            .await
            .expect("swap the names in the fork");
    }
    let request = family.request("swap");
    let planned = family.plan(&request).await;
    family
        .main
        .move_path("/d", "/e", &actor())
        .await
        .expect("rename the directory in main");

    let error = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect_err("the head moved");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    let PlannedMerge::Candidate(planned) = family
        .main
        .plan_merge(&request, Some(planned.reservation))
        .await
        .expect("the second plan")
    else {
        panic!("expected a plan");
    };
    family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect("the second plan commits");
    let swapped = family.main.stat("/e/x.txt").await.expect("x.txt under /e");
    assert_eq!(swapped.inode_id, y);
    let swapped = family.main.stat("/e/y.txt").await.expect("y.txt under /e");
    assert_eq!(swapped.inode_id, x);
}

/// Checks that a merge with no directory change doesn't expect its head, so
/// it commits after an unrelated commit between planning and publication.
#[tokio::test]
async fn a_merge_without_a_directory_change_commits_after_an_unrelated_commit() {
    let family = family().await;
    let planned = family.plan(&family.request("files")).await;
    family.write_main("/other.txt").await;
    let commit = family
        .main
        .submit_merge(planned.candidate, &planned.reservation)
        .await
        .expect("the merge");
    assert!(commit.merge.is_some());
}

/// Checks that a merge whose head moved commits on its second plan.
#[tokio::test]
async fn a_merge_commits_on_its_second_plan() {
    let family = family().await;
    family.delete_d_in_the_fork().await;
    let request = family.request("replan");
    let planned = family.plan(&request).await;
    family.write_main("/other.txt").await;
    let mut plans = 1;
    let commit = family
        .commit(&request, planned, |request, reservation| {
            plans += 1;
            let main = &family.main;
            Box::pin(async move { main.plan_merge(&request, Some(reservation)).await })
        })
        .await
        .expect("the second plan commits");
    assert_eq!(plans, 2);
    assert!(commit.merge.is_some());
}

/// Checks that a merge plans again after any failed precondition, and not
/// only `stale_head`. `main` changes a file that the fork edited between the
/// plan and the commit, the commit fails with `stale_revision`, and the next
/// plan reports the change as a `content` conflict.
#[tokio::test]
async fn a_merge_plans_again_after_a_stale_revision() {
    let family = family().await;
    family
        .fork
        .put_file_with_options(
            "/a.txt",
            b"ours",
            &actor(),
            &crate::PutFileOptions {
                behavior: loonfs_types::DestinationBehavior::Replace,
                ..crate::PutFileOptions::default()
            },
        )
        .await
        .expect("edit in the fork");
    let request = family.request("edit");
    let planned = family.plan(&request).await;
    family
        .main
        .put_file_with_options(
            "/a.txt",
            b"theirs",
            &actor(),
            &crate::PutFileOptions {
                behavior: loonfs_types::DestinationBehavior::Replace,
                ..crate::PutFileOptions::default()
            },
        )
        .await
        .expect("edit in main");
    let error = family
        .commit(&request, planned, |request, reservation| {
            let main = &family.main;
            Box::pin(async move { main.plan_merge(&request, Some(reservation)).await })
        })
        .await
        .expect_err("the second plan finds the conflict");
    assert_matches!(
        error,
        crate::Error::Core(CoreError::MergeConflict { ref conflict, .. })
            if conflict.kind == MergeConflictKind::Content
    );
}

/// Checks that a merge plans again after another writer takes a name that
/// the merge's create needs. `main` creates `/b.txt`, which the fork created,
/// between the plan and the commit, the commit fails with `path_conflict`,
/// and the next plan reports the taken name as a `name` conflict.
#[tokio::test]
async fn a_merge_plans_again_after_another_writer_takes_its_name() {
    let family = family().await;
    let request = family.request("create");
    let planned = family.plan(&request).await;
    family
        .main
        .put_file("/b.txt", b"main", &actor())
        .await
        .expect("create the fork's name in main");
    let error = family
        .commit(&request, planned, |request, reservation| {
            let main = &family.main;
            Box::pin(async move { main.plan_merge(&request, Some(reservation)).await })
        })
        .await
        .expect_err("the second plan finds the conflict");
    assert_matches!(
        error,
        crate::Error::Core(CoreError::MergeConflict { ref conflict, .. })
            if conflict.kind == MergeConflictKind::Name
    );
}

/// Checks that a merge whose head moves before each of its commits returns
/// `stale_head` after four plans.
#[tokio::test]
async fn a_merge_returns_stale_head_after_four_plans() {
    let family = family().await;
    family.delete_d_in_the_fork().await;
    let request = family.request("stale");
    let planned = family.plan(&request).await;
    family.write_main("/other-1.txt").await;
    let mut plans = 1;
    let error = family
        .commit(&request, planned, |request, reservation| {
            plans += 1;
            let path = format!("/other-{plans}.txt");
            let family = &family;
            Box::pin(async move {
                let planned = family.main.plan_merge(&request, Some(reservation)).await;
                family.write_main(&path).await;
                planned
            })
        })
        .await
        .expect_err("every plan is stale");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert_eq!(plans, 4);
}

/// Checks that a plan after `stale_head` returns the merge when a retry
/// under the same commit ID committed it in between.
#[tokio::test]
async fn a_second_plan_returns_the_merge_that_a_retry_committed() {
    let family = family().await;
    family.delete_d_in_the_fork().await;
    let request = family.request("retried");
    let planned = family.plan(&request).await;
    family.write_main("/other.txt").await;
    let retried = Arc::new(std::sync::Mutex::new(None));
    let commit = family
        .commit(&request, planned, |request, reservation| {
            let retried = Arc::clone(&retried);
            let family = &family;
            Box::pin(async move {
                drop(reservation);
                let options = MergeForkOptions {
                    commit_id: Some(request.commit_id.clone()),
                    ..MergeForkOptions::default()
                };
                let commit = family
                    .main
                    .merge_fork_with_options(family.fork.id(), &actor(), &options)
                    .await
                    .expect("the retry commits");
                *retried.lock().expect("lock") = Some(commit);
                family.main.plan_merge(&request, None).await
            })
        })
        .await
        .expect("the second plan finds the receipt");
    assert_eq!(Some(commit), retried.lock().expect("lock").take());
}

/// Checks that a plan after `stale_head` keeps the first plan's range, so
/// another merge of the fork that committed in between returns
/// `merge_range_conflict`.
#[tokio::test]
async fn a_second_plan_after_another_merge_is_a_range_conflict() {
    let family = family().await;
    family.delete_d_in_the_fork().await;
    let request = family.request("mine");
    let planned = family.plan(&request).await;
    family.write_main("/other.txt").await;
    let error = family
        .commit(&request, planned, |request, reservation| {
            let family = &family;
            Box::pin(async move {
                drop(reservation);
                family
                    .main
                    .merge_fork(family.fork.id(), &actor())
                    .await
                    .expect("another merge of the fork");
                family.main.plan_merge(&request, None).await
            })
        })
        .await
        .expect_err("the range moved");
    assert_eq!(error.code(), ErrorCode::MergeRangeConflict);
}

/// Checks that a source that doesn't exist, and a fork that was deleted,
/// return `invalid_request` with `param` set to the request's field, and that
/// the merge leaves the session of the namespace it merges into open. A
/// retry of a merge that committed before the fork was deleted still
/// returns the merge.
#[tokio::test]
async fn a_source_that_isnt_a_live_fork_is_an_invalid_request() {
    let family = family().await;
    let missing = namespace_id("missing");
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("before").expect("commit id")),
        ..MergeForkOptions::default()
    };
    let merged = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("a merge before the delete");
    family.fork.delete().await.expect("delete the fork");
    let retried = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("a retry after the delete");
    assert_eq!(retried, merged);
    for source in [&missing, family.fork.id()] {
        let error = family
            .main
            .merge_fork(source, &actor())
            .await
            .expect_err("not a live fork");
        assert_eq!(error.code(), ErrorCode::InvalidRequest, "{source}");
        assert_eq!(
            error.invalid_request_param().as_deref(),
            Some("source_namespace_id"),
            "{source}"
        );
    }
    assert_eq!(family.main.session_status(), NamespaceSessionStatus::Open);
    family
        .main
        .put_file("/after.txt", b"after", &actor())
        .await
        .expect("main still takes writes");
}

/// Checks that only an error about the fork becomes `invalid_request`. The
/// same error about the namespace that the merge commits into stays as it
/// is, so the server's handling of a deleted namespace still applies to it.
#[test]
fn only_the_forks_absence_is_an_invalid_request() {
    let fork = namespace_id("fork");
    let main = namespace_id("main");
    for namespace_id in [fork.clone(), main] {
        for error in [
            CoreError::NamespaceNotFound {
                namespace_id: namespace_id.clone(),
            },
            CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ] {
            let code = error.code();
            let mapped = missing_fork(&fork, error.into());
            let expected = if namespace_id == fork {
                ErrorCode::InvalidRequest
            } else {
                code
            };
            assert_eq!(mapped.code(), expected, "{namespace_id}");
        }
    }
}
