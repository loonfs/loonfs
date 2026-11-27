//! Merges that race another writer of the namespace they merge into, merges
//! that keep their reservation, merges from a source that isn't a live fork,
//! and merges on a runtime that has merges turned off.

use super::{MergeOutcome, PlannedCommit, PlannedMerge};
use crate::publish::{CommitIntent, FilesystemOperation};
use crate::publisher::MergeReservation;
use crate::{
    ActorId, Commit, CommitId, CoreError, DeleteOptions, ErrorCode, LoonFs, MergeForkOptions,
    MoveOptions, Namespace, NamespaceSessionStatus, Result, Writable,
};
use futures::future::BoxFuture;
use loonfs_core::limits::MAX_TAIL_COMMIT_AGE_MS;
use loonfs_core::MergeRequest;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{request_lines, KeyPredicate, RecordingStore};
use loonfs_types::{
    AbsolutePath, CommitSeq, ContentId, ContentLayout, DeleteDirectoryBehavior, ExtentObject,
    ManifestNo, MergeConflictKind,
};
use std::assert_matches;
use std::sync::Arc;
use tempfile::TempDir;

struct Family {
    _directory: TempDir,
    store: crate::SharedObjectStore,
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
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    family_on(directory, store, None).await
}

/// Creates the family of [`family`] in a runtime on `store`, which stores
/// its objects in `directory`, with `wall_clock` as the runtime's wall clock
/// when it's set.
async fn family_on(
    directory: TempDir,
    store: crate::SharedObjectStore,
    wall_clock: Option<Arc<ManualClock>>,
) -> Family {
    let main_id = namespace_id("main");
    let builder = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(writer_id("merge-writer"));
    let builder = match wall_clock {
        Some(clock) => builder.wall_clock(clock),
        None => builder,
    };
    let runtime = builder.build().await.expect("runtime");
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
        store,
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
            PlannedMerge::Committed { commit, .. } => panic!("expected a plan, got {commit:?}"),
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
        let maintenance = self.runtime.maintenance();
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

    /// Returns the content ID of `path` in `namespace`, and the layout that
    /// `namespace`'s current view records for that chain.
    async fn layout(
        &self,
        namespace: &Namespace<Writable>,
        path: &str,
    ) -> (ContentId, ContentLayout) {
        let entry = namespace.stat(path).await.expect("stat the file");
        let content_id = entry
            .content_ref()
            .expect("a file's content reference")
            .content_id
            .clone();
        let layout = loonfs_core::test_support::content_layout(
            self.store.as_ref(),
            namespace.id(),
            &content_id,
        )
        .await
        .expect("a layout of the chain");
        (content_id, layout)
    }

    /// Reads `path` in `namespace` as a reader with nothing cached.
    async fn fresh_read(&self, namespace: &Namespace<Writable>, path: &str) -> Vec<u8> {
        crate::test_support::fresh_reader(self.store.clone())
            .await
            .namespace(namespace.id())
            .read_file(path)
            .await
            .expect("a fresh read")
            .bytes
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
        let (MergeOutcome::Found { commit, .. } | MergeOutcome::Committed { commit, .. }) = self
            .main
            .commit_merge(
                &mut request.clone(),
                PlannedMerge::Candidate(Box::new(planned)),
                replan,
            )
            .await?;
        Ok(commit)
    }
}

/// Checks that a merge candidate committed through `commit_candidate`
/// commits as the next commit of this namespace, with the merge record of
/// its plan, under a reservation of the merge
/// allowance that it releases when the commit settles.
#[tokio::test]
async fn a_merge_candidate_commits_through_commit_candidate() {
    let family = family().await;
    let planned = family.plan(&family.request("merge")).await;
    drop(planned.reservation);
    let budget = family.runtime.execution_budget().clone();
    assert_eq!(budget.stats().admitted_merge_bytes, 0);
    let head_seq = family.main.metadata().await.expect("metadata").head_seq;
    let commit = family
        .main
        .commit_candidate(planned.candidate)
        .await
        .expect("the merge commits");
    assert_eq!(commit.committed_seq, CommitSeq(head_seq.0 + 1));
    let merge = commit.merge.expect("a merge commit");
    assert_eq!(&merge.source_namespace_id, family.fork.id());
    assert_eq!(
        (merge.source_after_seq, merge.source_through_seq),
        (
            planned.record.source_after_seq,
            planned.record.source_through_seq
        )
    );
    assert_eq!(budget.stats().admitted_merge_bytes, 0);
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
    let others = (0..budget.max_merge_bytes().get() / floor)
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
    let others = (1..budget.max_merge_bytes().get() / floor)
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
/// commit from the fork's WAL tail.
#[tokio::test]
async fn a_merge_after_the_forks_handle_is_dropped_merges_its_last_commit() {
    let Family {
        _directory: _store,
        runtime,
        main,
        fork,
        ..
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

/// Checks that a merge reads the fork's WAL tail above a manifest that
/// folded some of the fork's commits. A fold of the fork covers `/b.txt`,
/// the fork then commits `/c.txt`, and its handle is dropped, so the runtime
/// keeps the fork's head on that manifest. The merge applies both files,
/// through the fork's head.
#[tokio::test]
async fn a_merge_reads_the_forks_tail_above_a_folded_manifest() {
    let Family {
        _directory: _store,
        runtime,
        main,
        fork,
        ..
    } = family().await;
    let fork_id = fork.id().clone();
    fork.fold_wal().await.expect("fold the fork's first commit");
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

/// Checks that a merge's plan takes a snapshot of the fork's current
/// manifest, without folding the fork, and that the snapshot stays in the
/// fork's snapshot listing after the merge commits, until it expires. The
/// fork's `/b.txt` is in its WAL tail, so the snapshot's head is below the
/// fork's head. The request's commit ID is the longest that the commit ID
/// grammar accepts.
#[tokio::test]
async fn a_merge_leaves_the_snapshot_its_plan_took_on_the_fork_to_expire() {
    let family = family().await;
    let request = family.request(&"m".repeat(128));
    let manifests = family.manifest_nos().await;
    let fork_head = family.fork.metadata().await.expect("metadata").head_seq;
    let planned = family.plan(&request).await;
    assert_eq!(
        family.manifest_nos().await,
        manifests,
        "the plan folds nothing"
    );
    let page = family
        .fork
        .list_snapshots()
        .page()
        .await
        .expect("the fork's snapshots while the merge is planned");
    let [snapshot] = &page.snapshots[..] else {
        panic!(
            "the plan takes one snapshot of the fork: {:?}",
            page.snapshots
        );
    };
    assert_eq!(snapshot.name, None, "the plan's snapshot has no name");
    assert_eq!(Some(snapshot.snapshot_id.manifest_no()), manifests[1]);
    assert!(snapshot.captured_seq < fork_head, "{snapshot:?}");
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
    let after = family
        .fork
        .list_snapshots()
        .page()
        .await
        .expect("the fork's snapshots after the merge");
    assert_eq!(
        after.snapshots, page.snapshots,
        "the snapshot stays until it expires"
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

/// Checks that a retry through one runtime of a merge that another runtime
/// committed returns the original merge after the fork was deleted. Runtime
/// A's writer session on `main` has a position from before runtime B's
/// merge. The retry's receipt lookup confirms the position first, so the
/// lookup reads B's merge and the retry never reads the deleted fork.
#[tokio::test]
async fn a_retry_of_a_merge_that_another_runtime_committed_finds_the_receipt_after_the_fork_is_deleted(
) {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: crate::SharedObjectStore =
        Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let family = family_on(directory, store.clone(), None).await;
    family.write_main("/later.txt").await;
    let other = LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(writer_id("other-merge-writer"))
        .build()
        .await
        .expect("the other runtime");
    let options = MergeForkOptions {
        commit_id: Some(CommitId::parse("merge").expect("commit id")),
        ..MergeForkOptions::default()
    };
    let other_main = other.open_namespace(family.main.id()).expect("open main");
    let merged = other_main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("the merge in the other runtime");
    other
        .open_namespace(family.fork.id())
        .expect("open the fork")
        .delete()
        .await
        .expect("delete the fork");
    let retried = family
        .main
        .merge_fork_with_options(family.fork.id(), &actor(), &options)
        .await
        .expect("the retry");
    assert_eq!(retried.committed_seq, merged.committed_seq);
    assert_eq!(retried.commit_id, merged.commit_id);
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
/// `store_contention`, with the cause `lost_race`, after four plans,
/// although each commit failed on the head precondition that the merge set.
#[tokio::test]
async fn a_merge_returns_store_contention_after_four_plans() {
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
    assert_eq!(error.code(), ErrorCode::StoreContention);
    assert_eq!(
        error.details().and_then(|details| details.cause),
        Some(loonfs_types::ErrorCause::LostRace)
    );
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

/// Checks that a source that doesn't exist returns `namespace_not_found`, and
/// a fork that was deleted returns `namespace_deleted`, each with
/// `namespace_id` set to the source, and that the merge leaves the session of
/// the namespace it merges into open. A retry of a merge that committed
/// before the fork was deleted still returns the merge.
#[tokio::test]
async fn a_source_that_isnt_a_live_fork_returns_the_sources_error() {
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
    for (source, code) in [
        (&missing, ErrorCode::NamespaceNotFound),
        (family.fork.id(), ErrorCode::NamespaceDeleted),
    ] {
        let error = family
            .main
            .merge_fork(source, &actor())
            .await
            .expect_err("not a live fork");
        assert_eq!(error.code(), code, "{source}");
        assert_eq!(
            error
                .details()
                .and_then(|details| details.namespace_id)
                .as_ref(),
            Some(source),
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

/// Returns the family of [`family`] on a store that records every request,
/// with the log cleared. The runtime's writer sessions on `main` and the fork
/// have positions that their own commits checked moments ago.
async fn recorded_family() -> (Family, Arc<RecordingStore<LocalFsStore>>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let family = family_on(directory, store.clone(), None).await;
    store.reset();
    (family, store)
}

/// Checks the exact requests of a merge in a writer runtime with fresh
/// positions on both namespaces. The fork's `/b.txt` is a piece in the
/// fork's WAL tail, and the merge folds neither namespace.
#[tokio::test]
async fn a_merge_with_fresh_positions_sends_these_requests_in_order() {
    let (family, store) = recorded_family().await;
    family
        .main
        .merge_fork(family.fork.id(), &actor())
        .await
        .expect("the merge");
    let expected = [
        // The receipt lookup is a `strong` read of `main`, so it checks
        // `main`'s position with a successor HEAD and a boundary read. The
        // checks before the plan, and the plan's read of the fork, read the
        // fork's position as it is and send no request.
        "head main MetadataManifest 00000000000000000004",
        "get main Boundary",
        // The snapshot of the fork's current manifest 4, without a fold: the
        // record write, then the verification that manifest 4 is still
        // current. (The fork folded `main`'s tail into its own manifest 2,
        // so `main` is still at manifest 3.)
        "put main-fork Snapshot",
        "head main-fork MetadataManifest 00000000000000000005",
        "get main-fork Boundary",
        // The plan writes `/b.txt`'s piece from the fork's tail as one
        // extent under the pool's prefix, so the commit's reference has a
        // layout.
        "assemble pool ContentBlob",
        // The commit is one WAL put.
        "put main WalObject 00000000000000000005",
    ];
    assert_eq!(request_lines(&store.take()), expected);
}

/// Checks that a `strong` next-range query in a writer runtime with fresh
/// positions checks each position once, with a successor HEAD and a
/// boundary read, as a `strong` read of a position does.
#[tokio::test]
async fn the_next_merge_range_with_fresh_positions_checks_each_position() {
    let (family, store) = recorded_family().await;
    family
        .main
        .strong()
        .next_merge_range(family.fork.id())
        .await
        .expect("the next range");
    assert_eq!(
        request_lines(&store.take()),
        [
            "head main MetadataManifest 00000000000000000004",
            "get main Boundary",
            "head main-fork MetadataManifest 00000000000000000005",
            "get main-fork Boundary",
        ]
    );
}

/// Returns, for each extent of `layout`, its chain, the length of its span
/// object, and the range of the object it reads. A span object's ID is
/// random, so a test compares a layout's extents through this list, and
/// compares the extents themselves to check that two layouts list the same
/// object.
fn span_extents(layout: &ContentLayout) -> Vec<(ContentId, u64, u64, u64)> {
    layout
        .extents
        .iter()
        .map(|extent| {
            let ExtentObject::Span { .. } = extent.object else {
                panic!("the extent should be a span object: {extent:?}");
            };
            (
                extent.content_id.clone(),
                extent.object_bytes,
                extent.offset,
                extent.length,
            )
        })
        .collect()
}

/// Checks that a merge of files whose bytes are pieces in the fork's WAL
/// tail writes each chain's pieces as one extent and publishes no manifest
/// of the fork. `main`'s view lists that extent under the fork as the
/// chain's creator, and a reader with nothing cached reads the bytes in
/// `main`.
#[tokio::test]
async fn a_merge_writes_the_pieces_in_the_forks_tail_without_folding_the_fork() {
    let family = family().await;
    family
        .fork
        .put_file("/c.txt", b"cc", &actor())
        .await
        .expect("write in the fork");
    let before = family.manifest_nos().await;
    family
        .main
        .merge_fork(family.fork.id(), &actor())
        .await
        .expect("the merge");
    assert_eq!(
        family.manifest_nos().await[1],
        before[1],
        "the fork publishes no manifest"
    );
    for (path, bytes) in [("/b.txt", &b"b"[..]), ("/c.txt", &b"cc"[..])] {
        let (content_id, layout) = family.layout(&family.main, path).await;
        let length = u64::try_from(bytes.len()).expect("a small file");
        assert_eq!(layout.creator_namespace_id, *family.fork.id(), "{path}");
        assert_eq!(
            span_extents(&layout),
            [(content_id, length, 0, length)],
            "{path}"
        );
        assert_eq!(family.fresh_read(&family.main, path).await, bytes, "{path}");
    }
}

/// Checks that the fork's own fold after a merge writes a span object of its
/// own, with the bytes of the object that the merge wrote, and that a later
/// merge of an append in the fork's tail writes only the appended pieces.
/// `main`'s layout then lists the fold's extent and the second merge's
/// extent, and both namespaces read the same bytes.
#[tokio::test]
async fn a_merge_after_the_forks_fold_writes_only_the_appended_pieces() {
    let family = family().await;
    let merger = actor();
    let merge = || family.main.merge_fork(family.fork.id(), &merger);
    merge().await.expect("the first merge");
    family.fork.fold_wal().await.expect("fold the fork");
    let (content_id, folded) = family.layout(&family.fork, "/b.txt").await;
    let (merged_id, merged) = family.layout(&family.main, "/b.txt").await;
    assert_eq!(merged_id, content_id);
    assert_eq!(merged.creator_namespace_id, folded.creator_namespace_id);
    assert_eq!(
        span_extents(&merged),
        span_extents(&folded),
        "the fork's fold writes an object of the merge's length"
    );
    assert_ne!(
        merged.extents, folded.extents,
        "the fork's fold writes its own span object"
    );
    for namespace in [&family.main, &family.fork] {
        assert_eq!(family.fresh_read(namespace, "/b.txt").await, b"b");
    }
    family
        .fork
        .append_file("/b.txt", b"++", &actor())
        .await
        .expect("append in the fork");
    merge().await.expect("the second merge");
    let (appended, layout) = family.layout(&family.main, "/b.txt").await;
    assert_eq!(appended, content_id, "the fork extends its own chain");
    assert_eq!(layout.creator_namespace_id, *family.fork.id());
    assert_eq!(
        span_extents(&layout),
        [(content_id.clone(), 1, 0, 1), (content_id.clone(), 2, 0, 2)]
    );
    assert_eq!(
        layout.extents[0], folded.extents[0],
        "the second merge lists the fold's object"
    );
    for namespace in [&family.main, &family.fork] {
        assert_eq!(family.fresh_read(namespace, "/b.txt").await, b"b++");
    }
}

/// Checks that a merge plans from the fork's WAL tail while the tail's
/// oldest commit is at most [`MAX_TAIL_COMMIT_AGE_MS`] old, and folds the
/// fork first once that commit is older. The runtime's wall
/// clock is a manual clock, and the fork's `/b.txt` stays in the fork's tail
/// through the first merge, so it's the tail's oldest commit at the second.
#[tokio::test]
async fn a_merge_folds_a_fork_whose_tail_has_a_commit_older_than_the_max_age() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = Arc::new(ManualClock::at_system_time());
    let family = family_on(directory, store, Some(clock.clone())).await;
    let merger = actor();
    let merge = || family.main.merge_fork(family.fork.id(), &merger);
    let before = family.manifest_nos().await;
    clock.advance_ms(MAX_TAIL_COMMIT_AGE_MS);
    merge().await.expect("the first merge");
    assert_eq!(
        family.manifest_nos().await[1],
        before[1],
        "a tail whose commits are at most the max age old isn't folded"
    );
    family
        .fork
        .put_file("/c.txt", b"c", &actor())
        .await
        .expect("write in the fork");
    clock.advance_ms(1);
    merge().await.expect("the second merge");
    let after = family.manifest_nos().await;
    assert!(after[1] > before[1], "the merge folds the fork: {after:?}");
    assert_eq!(family.fresh_read(&family.main, "/c.txt").await, b"c");
}

/// Checks that `main`'s fold keeps a file within the extent cap across fork,
/// append, and merge cycles. `main` folds a 4 KiB file, and in each cycle a
/// new fork appends 1 KiB, `main` merges it, appends 64 bytes, and folds. The
/// merge writes the fork's bytes as one extent without combining, and the
/// doubling rule can't merge it into the 64 bytes, so each cycle adds two
/// extents. The cap fires at cycle 16, when no extent is as long as every
/// extent after it, so the fold copies the file into one 21,504-byte extent.
/// It fires again at cycle 32 and keeps that extent, which is longer than
/// the 17,408 bytes after it.
#[tokio::test]
async fn fork_append_merge_cycles_keep_mains_layout_within_the_extent_cap() {
    let family = family().await;
    let main = &family.main;
    let mut expected = vec![b'a'; 4096];
    main.put_file("/log", &expected, &actor())
        .await
        .expect("the file");
    let fold_main = || main.fold_wal();
    fold_main().await.expect("fold main");
    let mut extents = 1;
    for cycle in 1..=32 {
        let fork_id = namespace_id(&format!("cycle-{cycle}"));
        family
            .runtime
            .fork_namespace(main.id(), &fork_id, &actor())
            .await
            .expect("fork");
        let fork = family.runtime.open_namespace(&fork_id).expect("open fork");
        fork.append_file("/log", &[b'f'; 1024], &actor())
            .await
            .expect("append in the fork");
        main.merge_fork(&fork_id, &actor()).await.expect("merge");
        let (_, merged) = family.layout(main, "/log").await;
        assert_eq!(merged.extents.len(), extents + 1, "cycle {cycle}");
        assert!(merged.extents.len() <= 33, "cycle {cycle}");
        main.append_file("/log", &[b'm'; 64], &actor())
            .await
            .expect("append in main");
        fold_main().await.expect("fold main");
        expected.extend([b'f'; 1024]);
        expected.extend([b'm'; 64]);
        let (_, folded) = family.layout(main, "/log").await;
        extents = folded.extents.len();
        let lengths: Vec<_> = folded.extents.iter().map(|extent| extent.length).collect();
        match cycle {
            16 => assert_eq!(lengths, [21_504]),
            32 => assert_eq!(lengths, [21_504, 17_408]),
            // The extent cap is 32.
            _ => assert!(extents <= 32, "cycle {cycle}: {lengths:?}"),
        }
    }
    assert_eq!(family.fresh_read(main, "/log").await, expected);
}
