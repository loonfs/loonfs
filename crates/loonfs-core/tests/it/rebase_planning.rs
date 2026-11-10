//! Rebase planning: the replay commit that replays a fork's unmerged commits
//! into a new fork of its source, classified against the new fork's head
//! with the source's history read through the new fork's pin, which skips
//! the changes that conflict and reports them.

use crate::common::{default_writer, namespace_engine, read_context};
use loonfs_core::test_support::{namespace_store, NamespaceFixture, WriterAt};
use loonfs_core::{CoreError, MergeAllowance, MergeRequest, RuntimeReadContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::options::StatOptions;
use loonfs_types::{
    ChangeSeq, Commit, CommitId, InodeId, Merge, MergeConflict, MergeConflictKind,
    MergeConflictRevision, MergeConflictVersion, NamespaceId, NamespaceRebasedFrom,
    NextMergeRangeOutcome, NextMergeRangeResponse, PathEntry, PathEntryKind,
};
use tempfile::TempDir;

/// `main`, a fork of it, and the name of the new fork that a rebase of the
/// fork creates, over one local store.
struct Family {
    _directory: TempDir,
    store: LocalFsStore,
    main: NamespaceId,
    fork: NamespaceId,
    rebase: NamespaceId,
    writer: WriterAt,
}

/// What one replay returned: its commit, and its conflict report, which a
/// retry of a committed replay commit doesn't have.
struct Replay {
    commit: Commit,
    conflicts: Option<Vec<MergeConflict>>,
}

impl Family {
    /// Creates `main` with `/docs/a.txt` and `/docs/b.txt`, each holding its
    /// own path, and forks it.
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let family = Self {
            store: LocalFsStore::new(directory.path()).expect("store"),
            _directory: directory,
            main: namespace_id("main"),
            fork: namespace_id("main-fork"),
            rebase: namespace_id("main-rebase"),
            writer: default_writer(),
        };
        family.fixture(&family.main).create().await.expect("main");
        for path in ["/docs/a.txt", "/docs/b.txt"] {
            family
                .fixture(&family.main)
                .write_file_bytes(path, path.as_bytes(), None)
                .await
                .expect("seed");
        }
        namespace_engine(&family.store, &family.main, &family.writer)
            .fork_namespace(
                &family.fork,
                &loonfs_test_support::test_actor(),
                &loonfs_types::CreationId::generate(),
                loonfs_core::test_support::folded_manifest(&namespace_store(
                    &family.store,
                    &family.main,
                ))
                .await
                .expect("fold the WAL tail"),
            )
            .await
            .expect("fork main");
        family
    }

    fn fixture(&self, namespace_id: &NamespaceId) -> NamespaceFixture<&LocalFsStore> {
        NamespaceFixture::new(&self.store, namespace_id, &self.writer)
    }

    async fn write(&self, namespace_id: &NamespaceId, path: &str, bytes: &[u8]) -> Commit {
        self.fixture(namespace_id)
            .write_file_bytes(path, bytes, None)
            .await
            .expect("write")
    }

    /// Creates the rebase's new fork of `main`'s head, as the route does.
    async fn create_rebase(&self) -> loonfs_types::NamespaceMetadata {
        let main = namespace_engine(&self.store, &self.main, &self.writer);
        let fork = namespace_engine(&self.store, &self.fork, &self.writer);
        let fork_point_seq = main
            .fork_point_for_rebase(
                &read_context(&self.store, &self.main).await,
                &fork,
                &read_context(&self.store, &self.fork).await,
            )
            .await
            .expect("the fork point");
        main.fork_namespace_for_rebase(
            &self.rebase,
            &loonfs_test_support::test_actor(),
            &loonfs_types::CreationId::generate(),
            &self.fork,
            fork_point_seq,
            loonfs_core::test_support::folded_manifest(
                &loonfs_core::test_support::namespace_store(&self.store, &self.main),
            )
            .await
            .expect("fold the WAL tail"),
        )
        .await
        .expect("the rebase's new fork")
    }

    fn request(&self, commit_id: &str, through_seq: Option<ChangeSeq>) -> MergeRequest {
        MergeRequest {
            source_namespace_id: self.fork.clone(),
            through_seq,
            after_seq: None,
            commit_id: CommitId::parse(commit_id).expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            message: None,
        }
    }

    /// Reads `main` through the new fork's pin.
    async fn pinned_context(&self) -> RuntimeReadContext {
        let pin_id = read_context(&self.store, &self.rebase)
            .await
            .read_basis
            .head
            .fork_basis
            .expect("the new fork's basis")
            .source_pin_id;
        let live = read_context(&self.store, &self.main).await;
        RuntimeReadContext {
            read_basis: namespace_store(&self.store, &self.main)
                .load_snapshot_read_basis(&live.read_basis.head, &pin_id)
                .await
                .expect("main through the pin"),
            tail: None,
            visible_seq: None,
        }
    }

    /// Replays the fork into the new fork as the route does: the receipt
    /// lookup, the checks, the fork's fold, the plan, and the commit.
    async fn replay(
        &self,
        commit_id: &str,
        through_seq: Option<ChangeSeq>,
    ) -> Result<Replay, CoreError> {
        let request = self.request(commit_id, through_seq);
        let rebase = namespace_engine(&self.store, &self.rebase, &self.writer);
        let fork = namespace_engine(&self.store, &self.fork, &self.writer);
        let main = namespace_engine(&self.store, &self.main, &self.writer);
        let rebase_context = read_context(&self.store, &self.rebase).await;
        if let Some(commit) = rebase.find_merge(&rebase_context, &request).await? {
            return Ok(Replay {
                commit,
                conflicts: None,
            });
        }
        rebase
            .check_replay(
                &rebase_context,
                &fork,
                &read_context(&self.store, &self.fork).await,
                &request,
            )
            .await?;
        fork.fold_wal().await?;
        let plan = rebase
            .plan_replay(
                &rebase_context,
                &fork,
                &read_context(&self.store, &self.fork).await,
                &main,
                &self.pinned_context().await,
                &request,
                self.writer.now_ms,
                MergeAllowance::unshared(usize::MAX, 3),
            )
            .await?;
        let conflicts = plan.conflicts().to_vec();
        let commit = self
            .fixture(&self.rebase)
            .publish_candidates(vec![plan.into_candidate()])
            .await
            .pop()
            .expect("one result")?;
        Ok(Replay {
            commit,
            conflicts: Some(conflicts),
        })
    }

    /// Returns where the fork's next replay commit into the new fork starts.
    async fn next_range(&self) -> NextMergeRangeResponse {
        namespace_engine(&self.store, &self.rebase, &self.writer)
            .next_merge_range(
                &read_context(&self.store, &self.rebase).await,
                &namespace_engine(&self.store, &self.fork, &self.writer),
                &read_context(&self.store, &self.fork).await,
            )
            .await
            .expect("the next range")
    }

    /// Merges `source`, a fork of `main`, into `main` under `commit_id`.
    async fn merge_into_main(&self, source: &NamespaceId, commit_id: &str) -> Commit {
        let main = namespace_engine(&self.store, &self.main, &self.writer);
        let source_engine = namespace_engine(&self.store, source, &self.writer);
        source_engine
            .fold_wal()
            .await
            .expect("fold the merged fork");
        let request = MergeRequest {
            source_namespace_id: source.clone(),
            ..self.request(commit_id, None)
        };
        let candidate = main
            .plan_merge(
                &read_context(&self.store, &self.main).await,
                &source_engine,
                &read_context(&self.store, source).await,
                &request,
                self.writer.now_ms,
                MergeAllowance::unshared(usize::MAX, 3),
            )
            .await
            .expect("plan the merge")
            .into_candidate()
            .expect("the merge has no conflict");
        self.fixture(&self.main)
            .publish_candidates(vec![candidate])
            .await
            .pop()
            .expect("one result")
            .expect("merge")
    }

    /// Advances the new fork's floor to its fork point, and compacts it, so
    /// that it keeps none of the commit rows that it inherited from below
    /// that point.
    async fn compact_rebase(&self) {
        let rebase = namespace_engine(&self.store, &self.rebase, &self.writer);
        rebase.fold_wal().await.expect("fold the new fork");
        let (_, published) =
            loonfs_core::test_support::compact_every_group(&self.store, &self.rebase).await;
        assert!(
            published > 0,
            "the new fork's own commit gives it a run to compact"
        );
    }

    async fn entry(&self, namespace_id: &NamespaceId, path: &str) -> Option<PathEntry> {
        namespace_engine(&self.store, namespace_id, &self.writer)
            .resolve_path(
                path,
                StatOptions::default(),
                &read_context(&self.store, namespace_id).await,
            )
            .await
            .ok()
    }

    /// Returns `path`'s entry in `namespace_id` as a version of a conflict.
    async fn version(&self, namespace_id: &NamespaceId, path: &str) -> MergeConflictVersion {
        let entry = self
            .entry(namespace_id, path)
            .await
            .unwrap_or_else(|| panic!("{path} in {namespace_id}"));
        let revision = match &entry.kind {
            PathEntryKind::File {
                revision_no,
                content_ref: Some(content_ref),
                ..
            } => Some(MergeConflictRevision {
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
            }),
            _ => None,
        };
        MergeConflictVersion {
            path: entry.path,
            revision,
        }
    }

    async fn inode(&self, namespace_id: &NamespaceId, path: &str) -> InodeId {
        self.entry(namespace_id, path)
            .await
            .unwrap_or_else(|| panic!("{path} in {namespace_id}"))
            .inode_id
    }

    async fn bytes(&self, namespace_id: &NamespaceId, path: &str) -> Option<Vec<u8>> {
        crate::common::read_file_bytes(&self.store, namespace_id, path)
            .await
            .ok()
            .map(|file| file.bytes)
    }
}

/// Checks that a rebase whose changes don't conflict replays all of them
/// into the new fork, with the fork's inode IDs and beside main's later
/// change, and that the new fork then merges into main as its first merge.
#[tokio::test]
async fn a_rebase_whose_changes_all_apply_replays_them_into_the_new_fork() {
    let family = Family::new().await;
    family.write(&family.fork, "/new/c.txt", b"c").await;
    family.write(&family.fork, "/docs/a.txt", b"ours").await;
    family.write(&family.main, "/other.txt", b"theirs").await;
    let created = family.create_rebase().await;
    // The fork was made at main's sequence 2, after main's two seeding
    // commits, and the rebase forks main's head after `/other.txt`.
    let basis = created.fork_basis.expect("the new fork's basis");
    assert_eq!(basis.source_head_seq, ChangeSeq(3));
    assert_eq!(
        basis.rebased_from,
        Some(NamespaceRebasedFrom {
            source_namespace_id: family.fork.clone(),
            resume_after_seq: ChangeSeq(2),
        })
    );

    let replay = family.replay("replay-1", None).await.expect("replay");
    assert_eq!(replay.conflicts, Some(Vec::new()));
    let merge = replay.commit.merge.expect("a replay commit is a merge");
    assert_eq!(merge.source_namespace_id, family.fork);
    assert_eq!(merge.source_after_seq, ChangeSeq(2));
    for path in ["/new", "/new/c.txt"] {
        assert_eq!(
            family
                .entry(&family.rebase, path)
                .await
                .map(|entry| entry.inode_id),
            family
                .entry(&family.fork, path)
                .await
                .map(|entry| entry.inode_id),
            "{path} keeps its ID"
        );
    }
    for (path, bytes) in [
        ("/docs/a.txt", &b"ours"[..]),
        ("/other.txt", b"theirs"),
        ("/new/c.txt", b"c"),
    ] {
        assert_eq!(
            family.bytes(&family.rebase, path).await.as_deref(),
            Some(bytes),
            "{path}"
        );
    }

    family.merge_into_main(&family.rebase, "merge-1").await;
    assert_eq!(
        family.bytes(&family.main, "/docs/a.txt").await.as_deref(),
        Some(&b"ours"[..])
    );
    assert_eq!(
        family.bytes(&family.main, "/new/c.txt").await.as_deref(),
        Some(&b"c"[..])
    );
}

/// Checks that a change that conflicts with a row main committed after the
/// fork point is skipped and reported with the base through the pin, the
/// fork's version, and the new fork's version, while the fork's other
/// change applies.
#[tokio::test]
async fn a_content_conflict_is_skipped_and_reported_with_base_ours_and_theirs() {
    let family = Family::new().await;
    let base = family.version(&family.main, "/docs/a.txt").await;
    family.write(&family.main, "/docs/a.txt", b"theirs").await;
    family.write(&family.fork, "/docs/a.txt", b"ours").await;
    family.write(&family.fork, "/docs/b.txt", b"ours").await;
    family.create_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    let inode_id = family.inode(&family.main, "/docs/a.txt").await;
    assert_eq!(
        replay.conflicts,
        Some(vec![MergeConflict {
            inode_id,
            kind: MergeConflictKind::Content,
            base: Some(base),
            ours: Some(family.version(&family.fork, "/docs/a.txt").await),
            theirs: Some(family.version(&family.rebase, "/docs/a.txt").await),
        }])
    );
    assert_eq!(
        family.bytes(&family.rebase, "/docs/a.txt").await.as_deref(),
        Some(&b"theirs"[..]),
        "the new fork keeps main's version of the conflicting file"
    );
    assert_eq!(
        family.bytes(&family.rebase, "/docs/b.txt").await.as_deref(),
        Some(&b"ours"[..])
    );
}

/// Checks that a directory create that conflicts on its name is skipped,
/// and that the creates below it get `dependency` entries after it, in
/// parent-before-child order, each with the skipped directory's version in
/// the fork as `theirs`.
#[tokio::test]
async fn a_skipped_directory_create_skips_its_children_with_dependency_entries() {
    let family = Family::new().await;
    family.write(&family.main, "/new", b"a file").await;
    family.write(&family.fork, "/new/sub/c.txt", b"c").await;
    family.create_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    let conflicts = replay.conflicts.expect("a report");
    let entries: Vec<_> = conflicts
        .iter()
        .map(|conflict| (conflict.kind, conflict.inode_id, conflict.theirs.clone()))
        .collect();
    assert_eq!(
        entries,
        [
            (
                MergeConflictKind::Name,
                family.inode(&family.fork, "/new").await,
                Some(family.version(&family.main, "/new").await),
            ),
            (
                MergeConflictKind::Dependency,
                family.inode(&family.fork, "/new/sub").await,
                Some(family.version(&family.fork, "/new").await),
            ),
            (
                MergeConflictKind::Dependency,
                family.inode(&family.fork, "/new/sub/c.txt").await,
                Some(family.version(&family.fork, "/new/sub").await),
            ),
        ]
    );
    assert!(
        replay.commit.events.is_empty(),
        "a replay commit whose every change conflicted has no operations"
    );
    assert!(replay.commit.merge.is_some());
}

/// Checks that a skipped move keeps its old directory's delete from
/// applying. The fork moves `/docs/a.txt` out of `/docs` to a name that main
/// took, and then deletes `/docs`. The move is a `name` conflict, and the
/// delete of `/docs`, which would delete the file in the new fork, is a
/// `dependency` entry whose `theirs` is the file's version in the fork, so
/// the new fork still has `/docs` with the file in it.
#[tokio::test]
async fn a_skipped_move_keeps_its_old_directorys_delete_from_applying() {
    let family = Family::new().await;
    family.write(&family.main, "/taken.txt", b"theirs").await;
    family
        .fixture(&family.fork)
        .move_path("/docs/a.txt", "/taken.txt", None)
        .await
        .expect("move out of /docs");
    family
        .fixture(&family.fork)
        .delete_path("/docs", None)
        .await
        .expect("delete /docs");
    family.create_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    let moved = family.version(&family.fork, "/taken.txt").await;
    let entries: Vec<_> = replay
        .conflicts
        .expect("a report")
        .into_iter()
        .map(|conflict| (conflict.kind, conflict.inode_id, conflict.theirs))
        .collect();
    assert_eq!(
        entries,
        [
            (
                MergeConflictKind::Name,
                family.inode(&family.fork, "/taken.txt").await,
                Some(family.version(&family.main, "/taken.txt").await),
            ),
            (
                MergeConflictKind::Dependency,
                family.inode(&family.main, "/docs").await,
                Some(moved),
            ),
        ]
    );
    assert_eq!(
        family.bytes(&family.rebase, "/docs/a.txt").await.as_deref(),
        Some(&b"/docs/a.txt"[..]),
        "the new fork keeps the directory and the file that the fork moved out of it"
    );
}

/// Checks that a rebase in two pieces continues into the same new fork,
/// and that the second piece plans against the rows that the first wrote.
/// The next-range query on the new fork reports a start at the fork's
/// `resume_after_seq` before the first piece, and at the first piece's end
/// after it.
#[tokio::test]
async fn a_rebase_in_two_pieces_continues_into_the_same_new_fork() {
    let family = Family::new().await;
    let first = family.write(&family.fork, "/docs/a.txt", b"one").await;
    family.write(&family.fork, "/docs/a.txt", b"two").await;
    let last = family.write(&family.fork, "/d.txt", b"d").await;
    family.create_rebase().await;
    // The fork was made at main's sequence 2, which is its
    // `resume_after_seq`, because it has never merged.
    assert_eq!(
        family.next_range().await,
        NextMergeRangeResponse {
            fork_point_seq: ChangeSeq(2),
            source_head_seq: last.committed_seq,
            outcome: NextMergeRangeOutcome::Open {
                merge: None,
                next_after_seq: ChangeSeq(2),
            },
        }
    );

    let piece = family
        .replay("piece-1", Some(first.committed_seq))
        .await
        .expect("the first piece");
    assert_eq!(piece.conflicts, Some(Vec::new()));
    assert_eq!(
        family.next_range().await.outcome,
        NextMergeRangeOutcome::Open {
            merge: Some(Merge {
                source_namespace_id: family.fork.clone(),
                source_after_seq: ChangeSeq(2),
                source_through_seq: first.committed_seq,
            }),
            next_after_seq: first.committed_seq,
        }
    );
    assert_eq!(
        family.bytes(&family.rebase, "/docs/a.txt").await.as_deref(),
        Some(&b"one"[..])
    );
    let rest = family
        .replay("piece-2", None)
        .await
        .expect("the second piece");
    assert_eq!(rest.conflicts, Some(Vec::new()));
    let (first, second) = (
        piece.commit.merge.expect("a merge"),
        rest.commit.merge.expect("a merge"),
    );
    assert_eq!(second.source_after_seq, first.source_through_seq);
    for (path, bytes) in [("/docs/a.txt", &b"two"[..]), ("/d.txt", b"d")] {
        assert_eq!(
            family.bytes(&family.rebase, path).await.as_deref(),
            Some(bytes),
            "{path}"
        );
    }
}

/// Checks that a retry with a committed replay commit's commit ID returns
/// that commit without a conflict report, and commits nothing.
#[tokio::test]
async fn a_retry_of_a_replay_commit_returns_no_report() {
    let family = Family::new().await;
    family.write(&family.main, "/docs/a.txt", b"theirs").await;
    family.write(&family.fork, "/docs/a.txt", b"ours").await;
    family.create_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    assert_eq!(replay.conflicts.map(|conflicts| conflicts.len()), Some(1));
    let retry = family.replay("replay-1", None).await.expect("retry");
    assert_eq!(retry.conflicts, None);
    assert_eq!(retry.commit.committed_seq, replay.commit.committed_seq);
    assert_eq!(retry.commit.merge, replay.commit.merge);
}

/// Checks that a commit in the new fork between two pieces, such as a
/// resolver's, makes its row foreign for the next piece, so that the next
/// piece reports the fork's later change to that file instead of
/// overwriting the resolution.
#[tokio::test]
async fn a_resolvers_commit_between_pieces_makes_its_row_foreign_for_the_next_piece() {
    let family = Family::new().await;
    let first = family.write(&family.fork, "/docs/a.txt", b"one").await;
    family.create_rebase().await;
    family
        .replay("piece-1", Some(first.committed_seq))
        .await
        .expect("the first piece");
    family
        .write(&family.rebase, "/docs/a.txt", b"resolved")
        .await;
    family.write(&family.fork, "/docs/a.txt", b"two").await;

    let rest = family
        .replay("piece-2", None)
        .await
        .expect("the second piece");
    let conflicts = rest.conflicts.expect("a report");
    assert_eq!(
        conflicts
            .iter()
            .map(|conflict| (conflict.kind, conflict.theirs.clone()))
            .collect::<Vec<_>>(),
        [(
            MergeConflictKind::Content,
            Some(family.version(&family.rebase, "/docs/a.txt").await),
        )]
    );
    assert_eq!(
        family.bytes(&family.rebase, "/docs/a.txt").await.as_deref(),
        Some(&b"resolved"[..])
    );
}

/// Checks that a replay reads main's history at or below the new fork's fork
/// point through the pin. Main's earlier merge of the fork changed
/// `/docs/a.txt` there, and the fork then deletes `/docs`. After a
/// compaction of the new fork removes main's commit rows from the new fork,
/// the subtree check of `/docs` still finds that change in the source's part
/// of the history through the pin, and the pin still shows that the row is
/// the fork's own, so the delete applies with no conflict.
#[tokio::test]
async fn a_replay_reads_main_through_the_pin_after_the_new_fork_compacts() {
    let family = Family::new().await;
    family.write(&family.fork, "/docs/a.txt", b"merged").await;
    let merged = family.merge_into_main(&family.fork, "merge-1").await;
    family.write(&family.main, "/other.txt", b"theirs").await;
    family
        .fixture(&family.fork)
        .delete_path("/docs", None)
        .await
        .expect("delete /docs");
    let created = family.create_rebase().await;
    assert!(
        merged.committed_seq < created.head_seq,
        "main's merge is below the new fork's fork point"
    );
    family.write(&family.rebase, "/own.txt", b"own").await;
    family.compact_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    assert_eq!(replay.conflicts, Some(Vec::new()));
    assert!(family.entry(&family.rebase, "/docs").await.is_none());
    assert_eq!(
        family.bytes(&family.rebase, "/other.txt").await.as_deref(),
        Some(&b"theirs"[..])
    );
}

/// Checks that a change that main made below a directory after the fork
/// point, and before the rebase, is a `subtree` conflict for the fork's
/// delete of the directory, which the source's part of the history through
/// the pin finds, because the new fork's own part starts after it.
#[tokio::test]
async fn a_subtree_change_on_main_before_the_rebase_is_found_through_the_pin() {
    let family = Family::new().await;
    family.write(&family.main, "/docs/new.txt", b"theirs").await;
    family
        .fixture(&family.fork)
        .delete_path("/docs", None)
        .await
        .expect("delete /docs");
    family.create_rebase().await;

    let replay = family.replay("replay-1", None).await.expect("replay");
    let entries: Vec<_> = replay
        .conflicts
        .expect("a report")
        .into_iter()
        .map(|conflict| (conflict.kind, conflict.inode_id, conflict.theirs))
        .collect();
    assert_eq!(
        entries,
        [(
            MergeConflictKind::Subtree,
            family.inode(&family.main, "/docs").await,
            Some(family.version(&family.rebase, "/docs").await),
        )]
    );
    assert_eq!(
        family
            .bytes(&family.rebase, "/docs/new.txt")
            .await
            .as_deref(),
        Some(&b"theirs"[..])
    );
}
