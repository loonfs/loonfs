use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use loonfs_api::wire::manifest::ManifestStats;
use loonfs_api::{AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::control::{
    load_checkpoint_statistics, load_namespace_current_manifest, load_namespace_statistics,
};
use loonfs_core::publish::{
    CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use tempfile::tempdir;

#[tokio::test]
async fn cached_and_replayed_folds_count_commits_once_and_reads_use_only_manifests() {
    for cached in [false, true] {
        let dir = tempdir().expect("tempdir");
        let store = RecordingStore::new(
            LocalFsStore::new(dir.path()).expect("store"),
            KeyPredicate::any(),
        );
        let ns = namespace_id("stats");
        let context = mutation_context();
        bootstrap_namespace(&store, &ns, &context)
            .await
            .expect("bootstrap");
        let initial = load_namespace_statistics(&store, &ns).await.expect("stats");
        assert_eq!(initial.stats, ManifestStats::default());
        assert_eq!(
            (initial.inode_record_count, initial.metadata_stored_bytes),
            (0, 0)
        );
        let content = store_bytes_as_content(&store, &ns, b"hello")
            .await
            .expect("content");
        let empty = store_bytes_as_content(&store, &ns, b"")
            .await
            .expect("empty content");
        let operations = [
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/a/b").expect("path"),
                parents: true,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/file").expect("path"),
                content_ref: Some(content.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/file").expect("path"),
                content_ref: Some(content.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/empty").expect("path"),
                content_ref: Some(empty.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ];
        let mut engine = NamespaceCommitEngine::new(ns.clone());
        for (index, operation) in operations.into_iter().enumerate() {
            let request = CommitRequest::single(
                CommitId::parse(format!("stats-{index}")).expect("id"),
                loonfs_test_support::test_actor(),
                None,
                operation,
            );
            let candidate = prepared_candidate(&store, &ns, request).await;
            let first = engine
                .publish_batch(
                    &store,
                    vec![candidate.clone()],
                    &context,
                    &PublishTailOptions::default(),
                )
                .await
                .results
                .remove(0)
                .expect("commit");
            let retry = engine
                .publish_batch(
                    &store,
                    vec![candidate],
                    &context,
                    &PublishTailOptions::default(),
                )
                .await
                .results
                .remove(0)
                .expect("retry");
            assert_eq!(first, retry);
        }
        // Publishing commits and staging content do not advance folded statistics.
        assert_eq!(
            load_namespace_statistics(&store, &ns)
                .await
                .expect("unfolded")
                .stats,
            initial.stats
        );
        let input = cached.then(|| engine.wal_fold_input().expect("cached projection"));
        loonfs_core::fold_wal_tail(
            &store,
            None,
            &ns,
            input.clone(),
            &StdMonotonicTimer::default(),
        )
        .await
        .expect("fold");
        let expected = ManifestStats {
            committed_content_bytes_total: 10,
            committed_file_revisions_total: 3,
            committed_mutations_total: 5,
        };
        let folded = load_namespace_statistics(&store, &ns)
            .await
            .expect("folded stats");
        assert_eq!(folded.stats, expected);
        assert_eq!(folded.manifest.manifest_head_seq, ChangeSeq(4));
        assert_eq!(folded.inode_record_count, 5); // Root, two directories, two files.
                                                  // A stale cached fold and a fresh engine both see already-covered activity.
        loonfs_core::fold_wal_tail(&store, None, &ns, input, &StdMonotonicTimer::default())
            .await
            .expect("repeat fold");
        namespace_engine(&store, &ns, &context)
            .flush_wal()
            .await
            .expect("restart fold");
        store.reset();
        let current = load_namespace_current_manifest(&store, &ns)
            .await
            .expect("manifest");
        let observed = current.statistics().expect("stats");
        assert_eq!(observed.stats, expected);
        let manifest_prefix = loonfs_objectstore::keys::metadata_manifest_prefix(&ns);
        assert!(store.take().iter().all(|op| {
            matches!(op,
                RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. }
                if key.ends_with("hint.json") || key.starts_with(&manifest_prefix)
            )
        }));
        store.reset();
        assert_eq!(current.statistics().expect("loaded stats"), observed);
        assert!(store.take().is_empty());
        let mut stored_bytes = 0;
        for segment in current
            .envelope
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
        {
            let key = loonfs_objectstore::keys::metadata_segment_object_key(segment);
            stored_bytes += store
                .get(&key, None)
                .await
                .expect("segment")
                .expect("exists")
                .len() as u64;
        }
        assert_eq!(observed.metadata_stored_bytes, stored_bytes);
    }
}

#[tokio::test]
async fn forks_use_the_selected_checkpoint_and_only_the_immediate_baseline() {
    let dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let parent = namespace_id("parent");
    let child = namespace_id("child");
    let grandchild = namespace_id("grandchild");
    let context = mutation_context();
    bootstrap_namespace(&store, &parent, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &parent, "/file", b"old", &context, Some("old"))
        .await
        .expect("write");
    let engine = namespace_engine(&store, &parent, &context);
    let snapshot = engine
        .create_snapshot("baseline".to_owned(), u64::MAX / 2)
        .await
        .expect("snapshot");
    let baseline = load_checkpoint_statistics(&store, &parent, &snapshot.checkpoint_id)
        .await
        .expect("pin stats");
    write_file_bytes(&store, &parent, "/file", b"newer", &context, Some("newer"))
        .await
        .expect("write");
    engine.flush_wal().await.expect("fold parent");
    engine
        .fork_namespace(
            &child,
            &loonfs_test_support::test_actor(),
            Some(&snapshot.checkpoint_id),
        )
        .await
        .expect("fork old snapshot");
    let inherited = load_namespace_statistics(&store, &child)
        .await
        .expect("child stats");
    assert_eq!(inherited.stats, baseline.stats);
    assert_eq!(inherited.inode_record_count, baseline.inode_record_count);
    assert_eq!(
        inherited.metadata_stored_bytes,
        baseline.metadata_stored_bytes
    );
    assert_eq!(
        inherited
            .activity_since_creation(&store)
            .await
            .expect("baseline"),
        ManifestStats::default()
    );
    write_file_bytes(
        &store,
        &child,
        "/file",
        b"child",
        &context,
        Some("child-write"),
    )
    .await
    .expect("child write");
    let child_engine = namespace_engine(&store, &child, &context);
    child_engine.flush_wal().await.expect("fold child");
    let observed = load_namespace_statistics(&store, &child)
        .await
        .expect("first metered observation");
    assert_eq!(
        observed
            .activity_since_creation(&store)
            .await
            .expect("child activity"),
        ManifestStats {
            committed_content_bytes_total: 5,
            committed_file_revisions_total: 1,
            committed_mutations_total: 1
        }
    );
    child_engine
        .fork_namespace(&grandchild, &loonfs_test_support::test_actor(), None)
        .await
        .expect("nested fork");
    let nested = load_namespace_statistics(&store, &grandchild)
        .await
        .expect("nested stats");
    assert_eq!(nested.stats, observed.stats);
    store.reset();
    assert_eq!(
        nested
            .activity_since_creation(&store)
            .await
            .expect("immediate baseline"),
        ManifestStats::default()
    );
    let reads = store.take_get_keys();
    assert_eq!(reads.len(), 1);
    assert!(reads[0].starts_with("namespaces/child/"));
    assert_eq!(
        load_checkpoint_statistics(&store, &parent, &snapshot.checkpoint_id)
            .await
            .expect("pin unchanged"),
        baseline
    );
}

#[tokio::test]
async fn deleting_and_undeleting_a_subtree_preserves_retained_inodes() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let ns = namespace_id("stats");
    let context = mutation_context();
    bootstrap_namespace(&store, &ns, &context)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &ns, "/dir", &context, None)
        .await
        .expect("directory");
    for path in ["/dir/a", "/dir/b"] {
        write_file_bytes(&store, &ns, path, b"data", &context, None)
            .await
            .expect("file");
    }
    let inode = resolve_path(&store, &ns, "/dir")
        .await
        .expect("directory")
        .inode_id;
    let engine = namespace_engine(&store, &ns, &context);
    engine.flush_wal().await.expect("fold");
    let before = load_namespace_statistics(&store, &ns).await.expect("stats");
    let deleted = submit_operation(
        &store,
        &ns,
        CommitId::parse("delete").expect("id"),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/dir").expect("path"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
        &context,
    )
    .await
    .expect("delete subtree");
    engine.flush_wal().await.expect("fold delete");
    let during = load_namespace_statistics(&store, &ns).await.expect("stats");
    submit_operation(
        &store,
        &ns,
        CommitId::parse("undelete").expect("id"),
        FilesystemOperation::Undelete {
            inode_id: inode,
            deletion_seq: deleted.committed_seq,
            destination_path: None,
        },
        &context,
    )
    .await
    .expect("undelete");
    engine.flush_wal().await.expect("fold undelete");
    let after = load_namespace_statistics(&store, &ns).await.expect("stats");
    assert_eq!(
        (
            before.inode_record_count,
            during.inode_record_count,
            after.inode_record_count
        ),
        (4, 4, 4)
    );
    assert_eq!(
        after.stats.checked_sub(before.stats).expect("delta"),
        ManifestStats {
            committed_mutations_total: 2,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn namespace_deletion_requires_a_successful_final_fold() {
    let dir = tempdir().expect("tempdir");
    let store = FailStore::new(
        LocalFsStore::new(dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationClass::Put,
        InjectedError::PermissionDenied("fold unavailable".to_owned()),
    );
    let ns = namespace_id("stats");
    let context = mutation_context();
    bootstrap_namespace(&store, &ns, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &ns, "/file", b"final", &context, None)
        .await
        .expect("file");
    let engine = namespace_engine(&store, &ns, &context);
    store.fail_all();
    engine
        .delete_namespace(Default::default())
        .await
        .expect_err("cannot delete with an unaccounted tail");
    assert!(!load_namespace_current_manifest(&store, &ns)
        .await
        .expect("manifest")
        .envelope
        .payload()
        .status
        .is_deleted());
    assert_eq!(
        load_namespace_statistics(&store, &ns)
            .await
            .expect("stats")
            .stats,
        ManifestStats::default()
    );
    store.clear();
    engine
        .delete_namespace(Default::default())
        .await
        .expect("retry deletion");
    let final_manifest = load_namespace_current_manifest(&store, &ns)
        .await
        .expect("deleted manifest");
    assert!(final_manifest.envelope.payload().status.is_deleted());
    assert_eq!(
        final_manifest.statistics().expect("final stats").stats,
        ManifestStats {
            committed_content_bytes_total: 5,
            committed_file_revisions_total: 1,
            committed_mutations_total: 1
        }
    );
    assert_eq!(final_manifest.envelope.payload().head_seq, ChangeSeq(1));
    assert!(!final_manifest.envelope.payload().runs.is_empty());
}

#[tokio::test]
async fn failed_and_unacknowledged_fold_publications_do_not_double_count() {
    for applied in [false, true] {
        let dir = tempdir().expect("tempdir");
        let ns = namespace_id("stats");
        let context = mutation_context();
        let inner = LocalFsStore::new(dir.path()).expect("store");
        bootstrap_namespace(&inner, &ns, &context)
            .await
            .expect("bootstrap");
        write_file_bytes(&inner, &ns, "/file", b"data", &context, None)
            .await
            .expect("write");
        let store = FailStore::new(
            inner,
            KeyPredicate::manifest(&ns),
            OperationClass::Put,
            if applied {
                InjectedError::Transport("acknowledgment lost".to_owned())
            } else {
                InjectedError::PermissionDenied("publication failed".to_owned())
            },
        );
        let store = if applied {
            store.apply_then_fail()
        } else {
            store
        };
        store.fail_next(1);
        let result = namespace_engine(&store, &ns, &context).flush_wal().await;
        assert_eq!(result.is_ok(), applied);
        let observed = load_namespace_statistics(&store, &ns)
            .await
            .expect("statistics");
        assert_eq!(
            observed.stats.committed_content_bytes_total,
            if applied { 4 } else { 0 }
        );
        store.clear();
        for _ in 0..2 {
            namespace_engine(&store, &ns, &context)
                .flush_wal()
                .await
                .expect("restart and retry");
        }
        assert_eq!(
            load_namespace_statistics(&store, &ns)
                .await
                .expect("statistics")
                .stats,
            ManifestStats {
                committed_content_bytes_total: 4,
                committed_file_revisions_total: 1,
                committed_mutations_total: 1,
            }
        );
    }
}
