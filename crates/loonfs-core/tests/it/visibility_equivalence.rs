#![allow(clippy::panic)]
//! Differential coverage for tombstone visibility versus root reachability:
//! after every mutation shape, each tracked inode's reported visibility, its
//! derived current path, and the forward resolution of that path must agree
//! with one hand-written expectation.

use crate::common::read_context;
use loonfs_api::v0::CommitResponse;
use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
    NamespaceId, PageRequest, RevisionNo,
};
use loonfs_core::content::{prepare_stored_content, store_bytes_as_content};
use loonfs_core::publish::{CommitCandidate, CommitRequest, FilesystemOperation};
use loonfs_core::{
    BootstrapOptions, Error as CoreError, ErrorCode, MetadataReorganizeOutcome, NamespaceEngine,
    RuntimeReadContext,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

struct VisibilityHarness {
    _temp_dir: TempDir,
    store: Arc<LocalFsStore>,
    engine: NamespaceEngine<Arc<LocalFsStore>>,
}

impl VisibilityHarness {
    async fn new(namespace: &str) -> Self {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
        let engine = NamespaceEngine::builder(Arc::clone(&store))
            .namespace_id(namespace_id)
            .writer_id("visibility-equivalence")
            .build()
            .expect("build namespace engine");
        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap namespace");
        // Publish the namespace's first manifest up front, so each flush a
        // scenario performs adds exactly one L0 run to it.
        engine.flush_wal().await.expect("publish first manifest");
        Self {
            _temp_dir: temp_dir,
            store,
            engine,
        }
    }

    fn namespace_id(&self) -> &NamespaceId {
        self.engine.namespace_id()
    }

    async fn publish(&self, candidate: CommitCandidate) -> Result<CommitResponse, CoreError> {
        let mut results = self
            .engine
            .publish_namespace_commits_batch(vec![candidate])
            .await;
        assert_eq!(results.len(), 1, "one candidate must produce one result");
        results.pop().expect("one publish result")
    }

    /// Publishes one operation as its own single-operation request, the shape
    /// every scenario below drives except where it deliberately batches.
    async fn publish_operation(
        &self,
        operation: FilesystemOperation,
    ) -> Result<CommitResponse, CoreError> {
        self.publish(CommitCandidate::new(CommitRequest::single(
            CommitId::generate(),
            None,
            operation,
        )))
        .await
    }

    async fn create_directory(&self, path: &str) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("valid path"),
            parents: false,
        })
        .await
    }

    async fn put_file(
        &self,
        path: &str,
        bytes: &[u8],
        behavior: DestinationBehavior,
    ) -> Result<CommitResponse, CoreError> {
        let stored = store_bytes_as_content(&self.store, self.namespace_id(), bytes)
            .await
            .expect("stage content");
        let content_ref = stored.content_ref.clone();
        let catalog =
            loonfs_core::control::load_namespace_catalog_entry(&self.store, self.namespace_id())
                .await
                .expect("load namespace catalog");
        let prepared = prepare_stored_content(&catalog, stored).expect("prepare stored content");
        self.publish(CommitCandidate::prepared(
            CommitRequest::single(
                CommitId::generate(),
                None,
                FilesystemOperation::PutFile {
                    path: AbsolutePath::parse(path).expect("valid path"),
                    content_ref,
                    behavior,
                    expected_revision_no: None,
                },
            ),
            vec![prepared],
        ))
        .await
    }

    async fn delete(
        &self,
        path: &str,
        behavior: DeleteDirectoryBehavior,
    ) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(path).expect("valid path"),
            behavior,
            expected_inode_id: None,
        })
        .await
    }

    async fn move_path(&self, from_path: &str, to_path: &str) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse(from_path).expect("valid source path"),
            to_path: AbsolutePath::parse(to_path).expect("valid destination path"),
            behavior: DestinationBehavior::NoReplace,
        })
        .await
    }

    async fn copy_file(&self, from_path: &str, to_path: &str) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse(from_path).expect("valid source path"),
            to_path: AbsolutePath::parse(to_path).expect("valid destination path"),
            behavior: DestinationBehavior::NoReplace,
        })
        .await
    }

    async fn restore_revision(
        &self,
        path: &str,
        source_revision_no: RevisionNo,
    ) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::RestoreRevision {
            path: AbsolutePath::parse(path).expect("valid path"),
            source_revision_no,
        })
        .await
    }

    async fn undelete(
        &self,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        path: &str,
    ) -> Result<CommitResponse, CoreError> {
        self.publish_operation(FilesystemOperation::Undelete {
            inode_id,
            deleted_at_seq,
            path: Some(AbsolutePath::parse(path).expect("valid path")),
        })
        .await
    }

    /// Publishes several operations as one commit, so later operations in the
    /// batch resolve against what the earlier ones would persist.
    async fn batched_commit(
        &self,
        operations: Vec<FilesystemOperation>,
    ) -> Result<CommitResponse, CoreError> {
        self.publish(CommitCandidate::new(CommitRequest {
            commit_id: CommitId::generate(),
            message: None,
            operations,
        }))
        .await
    }

    async fn read_context(&self) -> RuntimeReadContext {
        read_context(&self.store, self.namespace_id()).await
    }

    async fn inode_id(&self, path: &str) -> InodeId {
        let context = self.read_context().await;
        self.engine
            .resolve_path(path, &context)
            .await
            .expect("resolve tracked path")
            .inode_id
    }

    async fn assert_path_missing(&self, path: &str) {
        let context = self.read_context().await;
        let error = self
            .engine
            .resolve_path(path, &context)
            .await
            .expect_err("path should be hidden");
        assert_eq!(error.code(), ErrorCode::PathNotFound);
    }

    async fn assert_equivalence(
        &self,
        context: &RuntimeReadContext,
        checkpoint: &str,
        expected: &[ExpectedInode<'_>],
    ) {
        let inode_ids: Vec<InodeId> = expected
            .iter()
            .map(|expectation| expectation.inode_id)
            .collect();
        let states = self
            .engine
            .resolve_current_files(&inode_ids, context)
            .await
            .expect("resolve current state for the tracked inodes");
        assert_eq!(
            states.len(),
            expected.len(),
            "{checkpoint}: every requested inode must be answered"
        );

        for (expectation, state) in expected.iter().zip(states) {
            assert_eq!(
                state.inode_id, expectation.inode_id,
                "{checkpoint}: answers must come back in request order"
            );
            // The two oracles meet here: an inode is reported visible
            // exactly when the tombstone rules admit it and its bindings
            // still reach the root, and the expectations below are written
            // by hand, so a disagreement between the two shows up as a
            // visible inode with no expected path or the reverse.
            assert_eq!(
                state.visible,
                expectation.path.is_some(),
                "{checkpoint}: inode {} visibility disagrees with its expected path",
                expectation.inode_id
            );
            assert_eq!(
                state.current_path.as_ref().map(AbsolutePath::as_str),
                expectation.path,
                "{checkpoint}: inode {} derived an unexpected root path",
                expectation.inode_id
            );

            if let Some(path) = state.current_path {
                let resolved = self
                    .engine
                    .resolve_path(&path, context)
                    .await
                    .expect("forward path resolution must confirm a root-reaching chain");
                assert_eq!(
                    resolved.inode_id, expectation.inode_id,
                    "{checkpoint}: derived path resolved to a different inode"
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
struct ExpectedInode<'a> {
    inode_id: InodeId,
    path: Option<&'a str>,
}

impl<'a> ExpectedInode<'a> {
    fn visible(inode_id: InodeId, path: &'a str) -> Self {
        Self {
            inode_id,
            path: Some(path),
        }
    }

    fn hidden(inode_id: InodeId) -> Self {
        Self {
            inode_id,
            path: None,
        }
    }
}

#[tokio::test]
async fn ordinary_mutations_preserve_visibility_equivalence() {
    let harness = VisibilityHarness::new("visibility-ordinary").await;
    harness
        .create_directory("/docs")
        .await
        .expect("create docs");
    harness
        .create_directory("/empty")
        .await
        .expect("create empty directory");
    harness
        .put_file(
            "/docs/original.txt",
            b"revision one",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create original file");
    harness
        .put_file(
            "/docs/original.txt",
            b"revision two",
            DestinationBehavior::Replace,
        )
        .await
        .expect("replace original file");
    harness
        .restore_revision("/docs/original.txt", RevisionNo(1))
        .await
        .expect("restore original revision");
    harness
        .copy_file("/docs/original.txt", "/docs/copy.txt")
        .await
        .expect("copy original file");

    let docs = harness.inode_id("/docs").await;
    let empty = harness.inode_id("/empty").await;
    let original = harness.inode_id("/docs/original.txt").await;
    let copy = harness.inode_id("/docs/copy.txt").await;
    let before_delete = harness.read_context().await;
    harness
        .assert_equivalence(
            &before_delete,
            "before non-recursive delete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(docs, "/docs"),
                ExpectedInode::visible(empty, "/empty"),
                ExpectedInode::visible(original, "/docs/original.txt"),
                ExpectedInode::visible(copy, "/docs/copy.txt"),
            ],
        )
        .await;

    harness
        .delete("/empty", DeleteDirectoryBehavior::NonRecursive)
        .await
        .expect("delete empty directory non-recursively");
    let after_delete = harness.read_context().await;
    harness
        .assert_equivalence(
            &after_delete,
            "after non-recursive delete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(docs, "/docs"),
                ExpectedInode::hidden(empty),
                ExpectedInode::visible(original, "/docs/original.txt"),
                ExpectedInode::visible(copy, "/docs/copy.txt"),
            ],
        )
        .await;
}

#[tokio::test]
async fn recursive_delete_covers_descendants_without_enumerating_them() {
    let harness = VisibilityHarness::new("visibility-recursive-delete").await;
    harness
        .put_file(
            "/tree/a/b/leaf.txt",
            b"leaf",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create deep leaf");
    harness
        .put_file(
            "/tree/sibling.txt",
            b"sibling",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create sibling");

    let tree = harness.inode_id("/tree").await;
    let a = harness.inode_id("/tree/a").await;
    let b = harness.inode_id("/tree/a/b").await;
    let leaf = harness.inode_id("/tree/a/b/leaf.txt").await;
    let sibling = harness.inode_id("/tree/sibling.txt").await;
    let before_delete = harness.read_context().await;

    harness
        .delete("/tree", DeleteDirectoryBehavior::Recursive)
        .await
        .expect("recursive delete");
    let after_delete = harness.read_context().await;
    let before_expected = [
        ExpectedInode::visible(InodeId(1), "/"),
        ExpectedInode::visible(tree, "/tree"),
        ExpectedInode::visible(a, "/tree/a"),
        ExpectedInode::visible(b, "/tree/a/b"),
        ExpectedInode::visible(leaf, "/tree/a/b/leaf.txt"),
        ExpectedInode::visible(sibling, "/tree/sibling.txt"),
    ];
    harness
        .assert_equivalence(
            &before_delete,
            "historical snapshot before recursive delete",
            &before_expected,
        )
        .await;
    harness
        .assert_equivalence(
            &after_delete,
            "snapshot after recursive delete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(tree),
                ExpectedInode::hidden(a),
                ExpectedInode::hidden(b),
                ExpectedInode::hidden(leaf),
                ExpectedInode::hidden(sibling),
            ],
        )
        .await;
}

#[tokio::test]
async fn reused_path_keeps_the_old_subtree_hidden() {
    let harness = VisibilityHarness::new("visibility-path-reuse").await;
    harness
        .put_file("/slot/deep/old.txt", b"old", DestinationBehavior::NoReplace)
        .await
        .expect("create old subtree");
    let old_slot = harness.inode_id("/slot").await;
    let old_deep = harness.inode_id("/slot/deep").await;
    let old_file = harness.inode_id("/slot/deep/old.txt").await;

    harness
        .delete("/slot", DeleteDirectoryBehavior::Recursive)
        .await
        .expect("delete old subtree");
    let deleted_snapshot = harness.read_context().await;
    harness
        .put_file("/slot/new.txt", b"new", DestinationBehavior::NoReplace)
        .await
        .expect("reuse deleted path");
    let new_slot = harness.inode_id("/slot").await;
    let new_file = harness.inode_id("/slot/new.txt").await;
    assert_ne!(old_slot, new_slot, "path reuse must allocate a fresh root");

    harness
        .assert_equivalence(
            &deleted_snapshot,
            "after delete and before path reuse",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(old_slot),
                ExpectedInode::hidden(old_deep),
                ExpectedInode::hidden(old_file),
            ],
        )
        .await;
    let reused_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &reused_snapshot,
            "after path reuse",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(old_slot),
                ExpectedInode::hidden(old_deep),
                ExpectedInode::hidden(old_file),
                ExpectedInode::visible(new_slot, "/slot"),
                ExpectedInode::visible(new_file, "/slot/new.txt"),
            ],
        )
        .await;
}

#[tokio::test]
async fn move_across_a_delete_boundary_preserves_visibility_equivalence() {
    let harness = VisibilityHarness::new("visibility-move-delete").await;
    harness
        .put_file(
            "/source/branch/file.txt",
            b"escaped",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create source subtree");
    harness
        .put_file(
            "/reverse/branch/file.txt",
            b"must stay",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create reverse-order subtree");
    harness
        .create_directory("/safe")
        .await
        .expect("create safe directory");

    let source = harness.inode_id("/source").await;
    let branch = harness.inode_id("/source/branch").await;
    let file = harness.inode_id("/source/branch/file.txt").await;
    let reverse = harness.inode_id("/reverse").await;
    let reverse_branch = harness.inode_id("/reverse/branch").await;
    let reverse_file = harness.inode_id("/reverse/branch/file.txt").await;
    let safe = harness.inode_id("/safe").await;

    // Operations inside one commit resolve in order, so the move sees what
    // the delete before it would persist: the whole subtree is gone, and
    // there is no longer a source path to move out. The failure names the
    // move as the operation that stopped the commit.
    let rejected = harness
        .batched_commit(vec![
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/reverse").expect("valid path"),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
            FilesystemOperation::MovePath {
                from_path: AbsolutePath::parse("/reverse/branch").expect("valid source path"),
                to_path: AbsolutePath::parse("/safe/reverse-branch")
                    .expect("valid destination path"),
                behavior: DestinationBehavior::NoReplace,
            },
        ])
        .await
        .expect_err("moving out after the ancestor delete must be rejected");
    assert_eq!(rejected.code(), ErrorCode::PathNotFound);
    assert_eq!(
        rejected
            .details()
            .expect("a rejected multi-operation commit names its operation")
            .operation_index,
        Some(1)
    );

    let deletion = harness
        .batched_commit(vec![
            FilesystemOperation::MovePath {
                from_path: AbsolutePath::parse("/source/branch").expect("valid source path"),
                to_path: AbsolutePath::parse("/safe/branch").expect("valid destination path"),
                behavior: DestinationBehavior::NoReplace,
            },
            FilesystemOperation::DeletePath {
                path: AbsolutePath::parse("/source").expect("valid path"),
                behavior: DeleteDirectoryBehavior::Recursive,
                expected_inode_id: None,
            },
        ])
        .await
        .expect("moving out before the ancestor delete should commit");

    let deleted_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &deleted_snapshot,
            "after ordered rename and delete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(source),
                ExpectedInode::visible(safe, "/safe"),
                ExpectedInode::visible(branch, "/safe/branch"),
                ExpectedInode::visible(file, "/safe/branch/file.txt"),
                ExpectedInode::visible(reverse, "/reverse"),
                ExpectedInode::visible(reverse_branch, "/reverse/branch"),
                ExpectedInode::visible(reverse_file, "/reverse/branch/file.txt"),
            ],
        )
        .await;

    harness
        .undelete(source, deletion.committed_seq, "/restored-source")
        .await
        .expect("undelete source root");
    harness
        .move_path("/safe/branch", "/restored-source/branch")
        .await
        .expect("move branch into restored subtree");
    let restored_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &restored_snapshot,
            "after crossing the revived boundary",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(source, "/restored-source"),
                ExpectedInode::visible(safe, "/safe"),
                ExpectedInode::visible(branch, "/restored-source/branch"),
                ExpectedInode::visible(file, "/restored-source/branch/file.txt"),
                ExpectedInode::visible(reverse, "/reverse"),
                ExpectedInode::visible(reverse_branch, "/reverse/branch"),
                ExpectedInode::visible(reverse_file, "/reverse/branch/file.txt"),
            ],
        )
        .await;
}

#[tokio::test]
async fn undelete_preserves_later_interior_mutations_and_nested_deletions() {
    let harness = VisibilityHarness::new("visibility-undelete-interior").await;
    harness
        .put_file(
            "/tree/branch/kept.txt",
            b"one",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create kept file");
    harness
        .put_file(
            "/tree/branch/hidden.txt",
            b"hidden",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create independently deleted file");
    let tree = harness.inode_id("/tree").await;
    let branch = harness.inode_id("/tree/branch").await;
    let kept = harness.inode_id("/tree/branch/kept.txt").await;
    let hidden = harness.inode_id("/tree/branch/hidden.txt").await;

    harness
        .delete(
            "/tree/branch/hidden.txt",
            DeleteDirectoryBehavior::NonRecursive,
        )
        .await
        .expect("delete child independently");
    harness
        .put_file(
            "/tree/branch/kept.txt",
            b"two",
            DestinationBehavior::Replace,
        )
        .await
        .expect("mutate retained child");
    harness
        .restore_revision("/tree/branch/kept.txt", RevisionNo(1))
        .await
        .expect("restore retained child revision");
    harness
        .move_path("/tree/branch/kept.txt", "/tree/branch/moved.txt")
        .await
        .expect("rename retained child");
    harness
        .copy_file("/tree/branch/moved.txt", "/tree/branch/copied.txt")
        .await
        .expect("copy retained child");
    let copied = harness.inode_id("/tree/branch/copied.txt").await;

    let first_deletion = harness
        .delete("/tree", DeleteDirectoryBehavior::Recursive)
        .await
        .expect("delete mutated subtree");
    let first_deleted_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &first_deleted_snapshot,
            "after deleting a mutated subtree",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(tree),
                ExpectedInode::hidden(branch),
                ExpectedInode::hidden(kept),
                ExpectedInode::hidden(hidden),
                ExpectedInode::hidden(copied),
            ],
        )
        .await;

    harness
        .undelete(tree, first_deletion.committed_seq, "/restored")
        .await
        .expect("undelete mutated subtree");
    let restored_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &restored_snapshot,
            "after first undelete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(tree, "/restored"),
                ExpectedInode::visible(branch, "/restored/branch"),
                ExpectedInode::visible(kept, "/restored/branch/moved.txt"),
                ExpectedInode::hidden(hidden),
                ExpectedInode::visible(copied, "/restored/branch/copied.txt"),
            ],
        )
        .await;

    harness
        .put_file(
            "/restored/branch/moved.txt",
            b"after undelete",
            DestinationBehavior::Replace,
        )
        .await
        .expect("mutate interior after undelete");
    let second_deletion = harness
        .delete("/restored", DeleteDirectoryBehavior::Recursive)
        .await
        .expect("delete subtree again");
    harness
        .undelete(tree, second_deletion.committed_seq, "/again")
        .await
        .expect("undelete newer generation");
    let second_restored_snapshot = harness.read_context().await;
    harness
        .assert_equivalence(
            &second_restored_snapshot,
            "after mutation and second undelete",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(tree, "/again"),
                ExpectedInode::visible(branch, "/again/branch"),
                ExpectedInode::visible(kept, "/again/branch/moved.txt"),
                ExpectedInode::hidden(hidden),
                ExpectedInode::visible(copied, "/again/branch/copied.txt"),
            ],
        )
        .await;
}

#[derive(Clone, Copy)]
enum MaintenanceOrder {
    ReorganizeThenRetain,
    RetainThenReorganize,
}

#[tokio::test]
async fn retention_and_reorganization_in_both_orders_preserve_ancestor_bindings() {
    run_maintenance_order(
        "visibility-maintenance-a",
        MaintenanceOrder::ReorganizeThenRetain,
    )
    .await;
    run_maintenance_order(
        "visibility-maintenance-b",
        MaintenanceOrder::RetainThenReorganize,
    )
    .await;
}

async fn run_maintenance_order(namespace: &str, order: MaintenanceOrder) {
    let harness = VisibilityHarness::new(namespace).await;

    harness
        .put_file(
            "/old/branch/live.txt",
            b"live one",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create movable subtree");
    harness.engine.flush_wal().await.expect("flush first run");
    let old = harness.inode_id("/old").await;
    let branch = harness.inode_id("/old/branch").await;
    let live = harness.inode_id("/old/branch/live.txt").await;

    harness
        .put_file(
            "/grave/deep/dead.txt",
            b"retained revision",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create subtree retained under a tombstone");
    harness.engine.flush_wal().await.expect("flush second run");
    let grave = harness.inode_id("/grave").await;
    let deep = harness.inode_id("/grave/deep").await;
    let dead = harness.inode_id("/grave/deep/dead.txt").await;

    harness
        .move_path("/old/branch", "/moved")
        .await
        .expect("move branch out of old parent");
    harness.engine.flush_wal().await.expect("flush third run");
    harness
        .delete("/old", DeleteDirectoryBehavior::NonRecursive)
        .await
        .expect("delete empty old parent");
    harness.engine.flush_wal().await.expect("flush fourth run");
    harness
        .put_file(
            "/old/reused.txt",
            b"new root",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("reuse old path");
    harness.engine.flush_wal().await.expect("flush fifth run");
    let reused_old = harness.inode_id("/old").await;
    let reused_file = harness.inode_id("/old/reused.txt").await;
    harness
        .delete("/grave", DeleteDirectoryBehavior::Recursive)
        .await
        .expect("delete retained descendant subtree");
    harness.engine.flush_wal().await.expect("flush sixth run");
    for (index, bytes) in [b"live two".as_slice(), b"live three".as_slice()]
        .into_iter()
        .enumerate()
    {
        harness
            .put_file("/moved/live.txt", bytes, DestinationBehavior::Replace)
            .await
            .unwrap_or_else(|error| panic!("replace for run {} failed: {error}", index + 7));
        harness
            .engine
            .flush_wal()
            .await
            .unwrap_or_else(|error| panic!("flush run {} failed: {error}", index + 7));
    }

    let expected = [
        ExpectedInode::visible(InodeId(1), "/"),
        ExpectedInode::hidden(old),
        ExpectedInode::visible(branch, "/moved"),
        ExpectedInode::visible(live, "/moved/live.txt"),
        ExpectedInode::hidden(grave),
        ExpectedInode::hidden(deep),
        ExpectedInode::hidden(dead),
        ExpectedInode::visible(reused_old, "/old"),
        ExpectedInode::visible(reused_file, "/old/reused.txt"),
    ];
    let after_folds = harness.read_context().await;
    harness
        .assert_equivalence(&after_folds, "after eight WAL folds", &expected)
        .await;

    match order {
        MaintenanceOrder::ReorganizeThenRetain => {
            drain_reorganization(&harness, &expected, "before retention").await;
            harness
                .engine
                .advance_retention_floor()
                .await
                .expect("advance retention after reorganization");
        }
        MaintenanceOrder::RetainThenReorganize => {
            harness
                .engine
                .advance_retention_floor()
                .await
                .expect("advance retention before reorganization");
            drain_reorganization(&harness, &expected, "after retention").await;
        }
    }

    let after_maintenance = harness.read_context().await;
    harness
        .assert_equivalence(
            &after_maintenance,
            "after retention and reorganization",
            &expected,
        )
        .await;
}

async fn drain_reorganization(
    harness: &VisibilityHarness,
    expected: &[ExpectedInode<'_>],
    checkpoint: &str,
) {
    let mut published_units = 0usize;
    for _ in 0..16 {
        let report = harness
            .engine
            .reorganize_metadata()
            .await
            .expect("reorganize metadata");
        match report.outcome {
            MetadataReorganizeOutcome::NotNeeded { .. } => {
                assert!(published_units > 0, "scenario must force reorganization");
                return;
            }
            MetadataReorganizeOutcome::UnitPublished { .. } => {
                published_units += 1;
                let context = harness.read_context().await;
                harness
                    .assert_equivalence(&context, checkpoint, expected)
                    .await;
            }
            MetadataReorganizeOutcome::Superseded => {
                panic!("single-writer test must not supersede reorganization")
            }
            MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("default budget must admit the visibility scenario")
            }
        }
    }
    panic!("reorganization did not drain within the family-group bound");
}

#[tokio::test]
async fn directory_move_sequences_cannot_create_parent_cycles() {
    let harness = VisibilityHarness::new("visibility-cycles").await;
    harness
        .put_file(
            "/a/b/c/file.txt",
            b"cycle guard",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create nested tree");
    let a = harness.inode_id("/a").await;
    let b = harness.inode_id("/a/b").await;
    let c = harness.inode_id("/a/b/c").await;
    let file = harness.inode_id("/a/b/c/file.txt").await;

    harness
        .move_path("/a/b", "/b")
        .await
        .expect("move descendant out first");
    harness
        .move_path("/a", "/b/a")
        .await
        .expect("move former ancestor below the detached subtree");
    let error = harness
        .move_path("/b", "/b/a/b")
        .await
        .expect_err("moving a directory below its descendant must fail");
    assert_eq!(error.code(), ErrorCode::WouldCycle);

    let context = harness.read_context().await;
    harness
        .assert_equivalence(
            &context,
            "after legal moves and rejected cycle",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::visible(b, "/b"),
                ExpectedInode::visible(a, "/b/a"),
                ExpectedInode::visible(c, "/b/c"),
                ExpectedInode::visible(file, "/b/c/file.txt"),
            ],
        )
        .await;
}

#[tokio::test]
async fn inode_revision_reads_remain_identity_addressed_after_visibility_is_lost() {
    let harness = VisibilityHarness::new("visibility-inode-history").await;
    harness
        .put_file(
            "/history.txt",
            b"revision one",
            DestinationBehavior::NoReplace,
        )
        .await
        .expect("create history file");
    harness
        .put_file(
            "/history.txt",
            b"revision two",
            DestinationBehavior::Replace,
        )
        .await
        .expect("replace history file");
    let inode_id = harness.inode_id("/history.txt").await;
    harness
        .delete("/history.txt", DeleteDirectoryBehavior::NonRecursive)
        .await
        .expect("delete history file");
    let context = harness.read_context().await;

    harness
        .assert_equivalence(
            &context,
            "after file deletion",
            &[
                ExpectedInode::visible(InodeId(1), "/"),
                ExpectedInode::hidden(inode_id),
            ],
        )
        .await;
    harness.assert_path_missing("/history.txt").await;

    let revisions = harness
        .engine
        .list_file_revisions_for_inode_page(
            inode_id,
            PageRequest {
                limit: loonfs_test_support::ids::page_limit(32),
                cursor: None,
            },
            &context,
        )
        .await
        .expect("inode history remains readable by stable identity");
    assert_eq!(
        revisions
            .items
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(2), RevisionNo(1)]
    );
    let bytes = harness
        .engine
        .read_file_revision_for_inode(inode_id, RevisionNo(1), &context, None)
        .await
        .expect("retained revision bytes remain readable by inode");
    assert_eq!(bytes, b"revision one");
}
