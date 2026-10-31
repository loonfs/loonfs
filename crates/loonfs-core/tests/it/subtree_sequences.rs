//! Subtree sequences: the `advance_subtree` deltas that end a commit, and
//! the subtree sequence that a stat reports for a directory and the root.

use crate::common::*;
use loonfs_core::publish::{CommitIntent, FilesystemOperation};
use loonfs_core::test_support::NamespaceFixture;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::format::wal::{decode_wal_object_envelope_zstd, WalCommitPayload, WalDelta};
use loonfs_types::options::StatOptions;
use loonfs_types::{
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, InodeId, NamespaceId,
};
use tempfile::tempdir;

/// Returns the newest commit in the namespace's WAL.
async fn newest_commit(store: &LocalFsStore, namespace_id: &NamespaceId) -> WalCommitPayload {
    let keys = store
        .list_prefix(&loonfs_objectstore::keys::wal_prefix(namespace_id))
        .await
        .expect("list the WAL");
    let key = keys.last().expect("the namespace has a WAL object");
    let bytes = store
        .get(key, None)
        .await
        .expect("read the WAL object")
        .expect("the WAL object exists");
    decode_wal_object_envelope_zstd(&bytes)
        .expect("decode the WAL object")
        .into_payload()
        .records
        .pop()
        .expect("the WAL object has a commit")
}

/// Returns the directories whose subtree sequence `commit` advances, in
/// delta order, after checking that the `advance_subtree` deltas end the
/// commit under the operation index after the last operation's.
fn advanced_directories(commit: &WalCommitPayload) -> Vec<InodeId> {
    let first_advance = commit
        .deltas
        .iter()
        .position(|delta| matches!(delta.delta, WalDelta::AdvanceSubtree { .. }))
        .unwrap_or(commit.deltas.len());
    let (operations, advances) = commit.deltas.split_at(first_advance);
    let advance_index = operations
        .last()
        .map(|delta| delta.semantic_operation_index + 1);
    advances
        .iter()
        .map(|delta| {
            assert_eq!(Some(delta.semantic_operation_index), advance_index);
            match &delta.delta {
                WalDelta::AdvanceSubtree { child_inode_id, .. } => *child_inode_id,
                other => panic!("expected only advance_subtree deltas at the end, got {other:?}"),
            }
        })
        .collect()
}

async fn inode_at(store: &LocalFsStore, namespace_id: &NamespaceId, path: &str) -> InodeId {
    resolve_path(store, namespace_id, path)
        .await
        .expect("stat the path")
        .inode_id
}

async fn subtree_seq_at(store: &LocalFsStore, namespace_id: &NamespaceId, path: &str) -> ChangeSeq {
    resolve_path(store, namespace_id, path)
        .await
        .expect("stat the directory")
        .subtree_seq()
        .expect("a directory entry reports its subtree sequence")
}

/// Checks that a commit advances each ancestor directory of the inodes that
/// it changes once, from the parent up to the root's child, parents first,
/// and that a write directly under the root advances nothing.
#[tokio::test]
async fn a_commit_advances_each_ancestor_below_the_root_once_parents_first() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("subtree-advances");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &default_writer());
    fixture.create().await.expect("bootstrap");

    // The put binds `/a` through `/a/b/c/d` in its own commit, so their bind
    // rows already store its sequence.
    fixture
        .write_file_bytes("/a/b/c/d/one.txt", b"1", None)
        .await
        .expect("write at depth five");
    assert!(advanced_directories(&newest_commit(&store, &namespace_id).await).is_empty());
    let mut ancestors = Vec::new();
    for path in ["/a", "/a/b", "/a/b/c", "/a/b/c/d"] {
        ancestors.push(inode_at(&store, &namespace_id, path).await);
    }

    fixture
        .write_file_bytes("/a/b/c/d/one.txt", b"2", None)
        .await
        .expect("replace at depth five");
    let commit = newest_commit(&store, &namespace_id).await;
    assert_eq!(advanced_directories(&commit), ancestors);
    assert_eq!(commit.deltas.len(), 1 + ancestors.len());

    // Three writes at three depths advance each ancestor once.
    fixture
        .write_files_bytes(
            &[
                "/a/b/c/d/two.txt".to_owned(),
                "/a/b/c/three.txt".to_owned(),
                "/a/b/four.txt".to_owned(),
            ],
            b"x",
        )
        .await
        .expect("write three files");
    let commit = newest_commit(&store, &namespace_id).await;
    assert_eq!(advanced_directories(&commit), ancestors);
    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(commit.committed_seq.0 - 1))
        .await
        .expect("read the change feed");
    assert_eq!(changes.changes[0].events.len(), 3, "advances aren't events");

    fixture
        .write_file_bytes("/top.txt", b"t", None)
        .await
        .expect("write under the root");
    assert!(advanced_directories(&newest_commit(&store, &namespace_id).await).is_empty());
}

/// Checks that a commit writes no delta for a directory whose edge it binds
/// or unbinds, and still advances the directories above that edge.
#[tokio::test]
async fn a_directory_that_the_commit_binds_or_unbinds_gets_no_delta() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("subtree-binds");
    let context = default_writer();
    let fixture = NamespaceFixture::new(&store, &namespace_id, &context);
    fixture.create().await.expect("bootstrap");
    fixture
        .write_file_bytes("/a/b/seed.txt", b"s", None)
        .await
        .expect("seed");
    let a = inode_at(&store, &namespace_id, "/a").await;

    fixture
        .write_file_bytes("/a/new/file.txt", b"n", None)
        .await
        .expect("write below a new directory");
    assert_eq!(
        advanced_directories(&newest_commit(&store, &namespace_id).await),
        [a]
    );

    // The commit changes `/a/b` and then deletes it.
    let path = |path: &str| AbsolutePath::parse(path).expect("path");
    submit_commit(
        &store,
        &namespace_id,
        CommitIntent {
            commit_id: CommitId::generate(),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations: vec![
                FilesystemOperation::CreateDirectory {
                    path: path("/a/b/inner"),
                    parents: false,
                },
                FilesystemOperation::DeletePath {
                    path: path("/a/b"),
                    behavior: DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                },
            ],
            preconditions: Vec::new(),
        },
        &context,
    )
    .await
    .expect("change and delete /a/b");
    assert_eq!(
        advanced_directories(&newest_commit(&store, &namespace_id).await),
        [a]
    );
}

/// Checks that a stat of a directory, by path or by inode, reports the
/// sequence of the newest commit that changed something below it, and that
/// a stat of the root reports the head.
#[tokio::test]
async fn stat_reports_each_directory_subtree_sequence_and_the_head_for_the_root() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("subtree-stats");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &default_writer());
    fixture.create().await.expect("bootstrap");
    let created_x = fixture
        .write_file_bytes("/x/seed.txt", b"x", None)
        .await
        .expect("create /x");
    fixture
        .write_file_bytes("/a/b/c/seed.txt", b"s", None)
        .await
        .expect("create /a/b/c");
    let written = fixture
        .write_file_bytes("/a/b/c/new.txt", b"n", None)
        .await
        .expect("write below /a/b/c");
    let head = fixture
        .write_file_bytes("/top.txt", b"t", None)
        .await
        .expect("write under the root");

    let engine = namespace_engine(&store, &namespace_id, &default_writer());
    let context = read_context(&store, &namespace_id).await;
    for path in ["/a", "/a/b", "/a/b/c"] {
        assert_eq!(
            subtree_seq_at(&store, &namespace_id, path).await,
            written.committed_seq,
            "{path}"
        );
        let by_inode = engine
            .stat_inode(
                inode_at(&store, &namespace_id, path).await,
                StatOptions::default(),
                &context,
            )
            .await
            .expect("stat by inode");
        assert_eq!(
            by_inode.subtree_seq(),
            Some(written.committed_seq),
            "{path}"
        );
    }
    assert_eq!(
        subtree_seq_at(&store, &namespace_id, "/x").await,
        created_x.committed_seq
    );
    let root = resolve_path(&store, &namespace_id, "/")
        .await
        .expect("stat the root");
    assert_eq!(root.subtree_seq(), Some(head.committed_seq));
    assert_eq!(root.head_seq, head.committed_seq);
}

/// Checks that a move advances the ancestors of the edge that it removes and
/// of the edge that it adds.
#[tokio::test]
async fn a_move_advances_the_ancestors_of_both_edges() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("subtree-moves");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &default_writer());
    fixture.create().await.expect("bootstrap");
    fixture
        .write_file_bytes("/a/x", b"x", None)
        .await
        .expect("write /a/x");
    fixture
        .write_file_bytes("/b/seed.txt", b"s", None)
        .await
        .expect("create /b");
    let a = inode_at(&store, &namespace_id, "/a").await;
    let b = inode_at(&store, &namespace_id, "/b").await;

    let moved = fixture
        .move_path("/a/x", "/b/x", None)
        .await
        .expect("move /a/x to /b/x");
    assert_eq!(
        advanced_directories(&newest_commit(&store, &namespace_id).await),
        [a, b]
    );
    for path in ["/a", "/b"] {
        assert_eq!(
            subtree_seq_at(&store, &namespace_id, path).await,
            moved.committed_seq,
            "{path}"
        );
    }
}

/// The entries of one page of the listing of `path`.
async fn listed(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    path: &str,
) -> Vec<loonfs_types::PathEntry> {
    namespace_engine(store, namespace_id, &default_writer())
        .list_path_page(
            path,
            loonfs_types::PageRequest {
                limit: loonfs_test_support::ids::page_limit(100),
                cursor: None,
            },
            loonfs_types::options::ListOptions::default(),
            &read_context(store, namespace_id).await,
        )
        .await
        .expect("list the directory")
        .items
}

/// Returns the subtree sequence of the root and of every entry listed under
/// `/` and `/a`, after checking that each listed entry reports what a stat
/// of it reports.
async fn subtree_seqs_under_a(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> Vec<(String, Option<ChangeSeq>)> {
    let mut subtree_seqs = vec![(
        "/".to_owned(),
        Some(subtree_seq_at(store, namespace_id, "/").await),
    )];
    for directory in ["/", "/a"] {
        for entry in listed(store, namespace_id, directory).await {
            let stat = resolve_path(store, namespace_id, entry.path.as_str())
                .await
                .expect("stat the listed entry");
            assert_eq!(entry.subtree_seq(), stat.subtree_seq(), "{}", entry.path);
            subtree_seqs.push((entry.path.to_string(), entry.subtree_seq()));
        }
    }
    subtree_seqs
}

/// Checks that a fold leaves every subtree sequence at the head unchanged.
#[tokio::test]
async fn a_fold_leaves_the_subtree_sequences_at_the_head_unchanged() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("subtree-folds");
    let context = default_writer();
    let fixture = NamespaceFixture::new(&store, &namespace_id, &context);
    fixture.create().await.expect("bootstrap");
    for path in [
        "/a/b/one.txt",
        "/a/c/two.txt",
        "/a/b/three.txt",
        "/x/four.txt",
        "/a/b/one.txt",
    ] {
        fixture
            .write_file_bytes(path, path.as_bytes(), None)
            .await
            .expect("write");
    }
    fixture
        .move_path("/a/c/two.txt", "/x/two.txt", None)
        .await
        .expect("move");

    let unfolded = subtree_seqs_under_a(&store, &namespace_id).await;
    assert!(unfolded
        .iter()
        .any(|(path, subtree_seq)| path == "/a/b" && subtree_seq.is_some()));
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the tail");
    assert_eq!(subtree_seqs_under_a(&store, &namespace_id).await, unfolded);
}
