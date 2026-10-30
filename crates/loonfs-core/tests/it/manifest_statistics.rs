//! Manifest activity and referenced storage observations.

use crate::common::*;
use loonfs_core::publish::{CommitIntent, FilesystemOperation, NamespaceCommitEngine};
use loonfs_core::test_support::{namespace_store, NamespaceFixture};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{commit_id, namespace_id};
use loonfs_test_support::stores::{
    replace_object, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::manifest::{ActivityCounter, ManifestActivity, MetadataRowFamily};
use loonfs_types::StdMonotonicTimer;
use loonfs_types::{
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior,
};
use std::collections::BTreeMap;
use tempfile::tempdir;

#[tokio::test]
async fn cached_and_replayed_folds_count_commits_once_and_reads_use_only_manifests() {
    for cached in [false, true] {
        let dir = tempdir().expect("tempdir");
        let store = RecordingStore::new(
            LocalFsStore::new(dir.path()).expect("store"),
            KeyPredicate::any(),
        );
        let ns = namespace_id("activity");
        let context = default_writer();
        NamespaceFixture::new(&store, &ns, &context)
            .create()
            .await
            .expect("bootstrap");
        let initial = namespace_store(&store, &ns)
            .load_statistics()
            .await
            .expect("activity");
        assert_eq!(initial.activity, ManifestActivity::default());
        assert_eq!(initial.segments_by_family.len(), 12);
        assert!(initial.segments_by_family.values().all(|count| *count == 0));
        assert_eq!(
            (initial.inode_record_count, initial.metadata_stored_bytes),
            (0, 0)
        );
        let content = namespace_store(&store, &ns)
            .store_bytes_as_content(b"hello")
            .await
            .expect("content");
        let empty = namespace_store(&store, &ns)
            .store_bytes_as_content(b"")
            .await
            .expect("empty content");
        let operations = [
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/a/b").expect("path"),
                parents: true,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/file").expect("path"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/file").expect("path"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/a/b/empty").expect("path"),
                content: loonfs_types::FileContent::Ref(empty.content_ref().clone()),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ];
        let mut engine =
            NamespaceCommitEngine::new(namespace_store(&store, &ns), context.writer_id.clone());
        for (index, operation) in operations.into_iter().enumerate() {
            let request = CommitIntent::single(
                CommitId::parse(format!("activity-{index}")).expect("id"),
                loonfs_test_support::test_actor(),
                None,
                operation,
            );
            let candidate = prepared_candidate(&store, &ns, request).await;
            let first = engine
                .publish_batch(vec![candidate.clone()], &context.clock())
                .await
                .results
                .remove(0)
                .expect("commit");
            let retry = engine
                .publish_batch(vec![candidate], &context.clock())
                .await
                .results
                .remove(0)
                .expect("retry");
            assert_eq!(first, retry);
        }
        // Publishing commits and staging content do not advance folded activity.
        assert_eq!(
            namespace_store(&store, &ns)
                .load_statistics()
                .await
                .expect("unfolded")
                .activity,
            initial.activity
        );
        let input = cached.then(|| engine.writer_position().expect("the position's tail"));
        namespace_store(&store, &ns)
            .fold_wal_tail(
                input.clone(),
                &loonfs_core::time::OperationClock::start(
                    0,
                    std::sync::Arc::new(StdMonotonicTimer::default()),
                ),
            )
            .await
            .expect("fold");
        let expected = ManifestActivity {
            content_bytes: ActivityCounter::parse(10).expect("activity"),
            file_revisions: ActivityCounter::parse(3).expect("activity"),
            mutations: ActivityCounter::parse(5).expect("activity"),
        };
        let folded = namespace_store(&store, &ns)
            .load_statistics()
            .await
            .expect("folded activity");
        assert_eq!(folded.activity, expected);
        assert_eq!(folded.manifest.head_seq, ChangeSeq(4));
        assert_eq!(folded.inode_record_count, 5); // Root, two directories, two files.
                                                  // A stale cached fold and a fresh engine both see already-covered activity.
        namespace_store(&store, &ns)
            .fold_wal_tail(
                input,
                &loonfs_core::time::OperationClock::start(
                    0,
                    std::sync::Arc::new(StdMonotonicTimer::default()),
                ),
            )
            .await
            .expect("repeat fold");
        namespace_engine(&store, &ns, &context)
            .fold_wal()
            .await
            .expect("restart fold");
        store.reset();
        let current = namespace_store(&store, &ns)
            .load_current_manifest()
            .await
            .expect("manifest");
        let observed = current.statistics().expect("activity");
        assert_eq!(observed.activity, expected);
        let manifest_prefix = loonfs_objectstore::keys::metadata_manifest_prefix(&ns);
        // Discovery reads the hint and the manifests, and probes manifest
        // numbers with HEAD requests.
        assert!(store.take().iter().all(|op| {
            matches!(op,
                RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. }
                if key.ends_with("hint.json") || key.starts_with(&manifest_prefix)
            ) || matches!(op, RecordedOperation::Head { key } if key.starts_with(&manifest_prefix))
        }));
        store.reset();
        assert_eq!(current.statistics().expect("loaded activity"), observed);
        assert!(store.take().is_empty());
        let mut stored_bytes = 0;
        for segment in current.payload().runs.iter().flat_map(|run| &run.segments) {
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
async fn forks_start_activity_at_zero_and_inherit_the_selected_checkpoint_footprint() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let parent = namespace_id("parent");
    let child = namespace_id("child");
    let grandchild = namespace_id("grandchild");
    let context = default_writer();
    NamespaceFixture::new(&store, &parent, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &parent, &context)
        .write_file_bytes("/file", b"old", Some(&commit_id("old")))
        .await
        .expect("write");
    let engine = namespace_engine(&store, &parent, &context);
    let snapshot = engine
        .create_snapshot("baseline".to_owned(), Some(u64::MAX / 2))
        .await
        .expect("snapshot")
        .response;
    let baseline = namespace_store(&store, &parent)
        .load_checkpoint_statistics(&snapshot.snapshot_id)
        .await
        .expect("pin activity");
    NamespaceFixture::new(&store, &parent, &context)
        .write_file_bytes("/file", b"newer", Some(&commit_id("newer")))
        .await
        .expect("write");
    let unavailable_wal = FailStore::new(
        LocalFsStore::new(dir.path()).expect("statistics store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(&parent)),
        OperationSelector::Read,
        InjectedError::PermissionDenied("WAL unavailable".to_owned()),
    );
    unavailable_wal.fail_all();
    assert_eq!(
        namespace_store(&unavailable_wal, &parent)
            .load_checkpoint_statistics(&snapshot.snapshot_id)
            .await
            .expect("checkpoint statistics exclude newer WAL"),
        baseline
    );
    engine.fold_wal().await.expect("fold parent");
    let layout_snapshot = engine
        .create_snapshot("layout".to_owned(), Some(u64::MAX / 2))
        .await
        .expect("layout snapshot")
        .response;
    let current = namespace_store(&store, &parent)
        .load_current_manifest()
        .await
        .expect("manifest");
    assert_eq!(current.payload().runs.len(), 2);
    let written = store
        .get(&current.object_key, None)
        .await
        .expect("manifest bytes")
        .expect("manifest exists");
    // The same document at another compression level is a valid manifest of
    // another stored length, so the statistics report the bytes as read.
    let document = zstd::stream::decode_all(written.as_ref()).expect("decompress manifest");
    let manifest_bytes = zstd::stream::encode_all(document.as_slice(), 19).expect("compress");
    assert_ne!(manifest_bytes.len(), written.len());
    assert!(manifest_bytes.len() < document.len());
    let stored_length = manifest_bytes.len() as u64;
    replace_object(&store, &current.object_key, manifest_bytes.into())
        .await
        .expect("manifest at another compression level");
    let layout = namespace_store(&store, &parent)
        .load_statistics()
        .await
        .expect("layout statistics");
    assert_eq!(layout.manifest_bytes, stored_length);
    assert_eq!(
        layout.segments_by_family,
        BTreeMap::from([
            (MetadataRowFamily::Inodes, 1),
            (MetadataRowFamily::DirentryBinds, 1),
            (MetadataRowFamily::DirentryChildBinds, 1),
            (MetadataRowFamily::DirentryRevisions, 2),
            (MetadataRowFamily::Revisions, 2),
            (MetadataRowFamily::Tombstones, 0),
            (MetadataRowFamily::ActiveDeletions, 0),
            (MetadataRowFamily::CommitReceipts, 2),
            (MetadataRowFamily::Commits, 2),
            (MetadataRowFamily::ContentLayouts, 2),
            (MetadataRowFamily::Attributes, 0),
            (MetadataRowFamily::Access, 0),
        ])
    );
    assert_eq!(
        namespace_store(&store, &parent)
            .load_checkpoint_statistics(&layout_snapshot.snapshot_id)
            .await
            .expect("checkpoint layout statistics"),
        layout
    );
    engine
        .fork_namespace(
            &child,
            &loonfs_test_support::test_actor(),
            Some(&snapshot.snapshot_id),
        )
        .await
        .expect("fork old snapshot");
    let inherited = namespace_store(&store, &child)
        .load_statistics()
        .await
        .expect("child activity");
    assert_eq!(baseline.activity.content_bytes.get(), 3);
    assert_eq!(inherited.activity, ManifestActivity::default());
    assert_eq!(
        inherited.fork_basis.as_ref().expect("fork basis").manifest,
        baseline.manifest
    );
    assert_eq!(inherited.inode_record_count, baseline.inode_record_count);
    assert_eq!(
        inherited.metadata_stored_bytes,
        baseline.metadata_stored_bytes
    );
    NamespaceFixture::new(&store, &child, &context)
        .write_file_bytes("/file", b"child", Some(&commit_id("child-write")))
        .await
        .expect("child write");
    let child_engine = namespace_engine(&store, &child, &context);
    child_engine.fold_wal().await.expect("fold child");
    let observed = namespace_store(&store, &child)
        .load_statistics()
        .await
        .expect("first metered observation");
    assert_eq!(
        observed.activity,
        ManifestActivity {
            content_bytes: ActivityCounter::parse(5).expect("activity"),
            file_revisions: ActivityCounter::parse(1).expect("activity"),
            mutations: ActivityCounter::parse(1).expect("activity")
        }
    );
    child_engine
        .fork_namespace(&grandchild, &loonfs_test_support::test_actor(), None)
        .await
        .expect("nested fork");
    let nested = namespace_store(&store, &grandchild)
        .load_statistics()
        .await
        .expect("nested activity");
    assert_eq!(nested.activity, ManifestActivity::default());
    assert_eq!(nested.inode_record_count, observed.inode_record_count);
    assert_eq!(nested.metadata_stored_bytes, observed.metadata_stored_bytes);
    assert_eq!(
        nested.fork_basis.as_ref().expect("fork basis").manifest,
        observed.manifest
    );
    assert_eq!(
        namespace_store(&store, &parent)
            .load_checkpoint_statistics(&snapshot.snapshot_id)
            .await
            .expect("pin unchanged"),
        baseline
    );
}

#[tokio::test]
async fn deleting_and_undeleting_a_subtree_preserves_retained_inodes() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let ns = namespace_id("activity");
    let context = default_writer();
    NamespaceFixture::new(&store, &ns, &context)
        .create()
        .await
        .expect("bootstrap");
    create_directory_path(&store, &ns, "/dir", &context, None)
        .await
        .expect("directory");
    for path in ["/dir/a", "/dir/b"] {
        NamespaceFixture::new(&store, &ns, &context)
            .write_file_bytes(path, b"data", None)
            .await
            .expect("file");
    }
    let inode = resolve_path(&store, &ns, "/dir")
        .await
        .expect("directory")
        .inode_id;
    let engine = namespace_engine(&store, &ns, &context);
    engine.fold_wal().await.expect("fold");
    let before = namespace_store(&store, &ns)
        .load_statistics()
        .await
        .expect("activity");
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
    engine.fold_wal().await.expect("fold delete");
    let during = namespace_store(&store, &ns)
        .load_statistics()
        .await
        .expect("activity");
    submit_operation(
        &store,
        &ns,
        CommitId::parse("undelete").expect("id"),
        FilesystemOperation::Undelete {
            inode_id: inode,
            deletion_seq: deleted.committed_seq,
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
        },
        &context,
    )
    .await
    .expect("undelete");
    engine.fold_wal().await.expect("fold undelete");
    let after = namespace_store(&store, &ns)
        .load_statistics()
        .await
        .expect("activity");
    assert_eq!(
        (
            before.inode_record_count,
            during.inode_record_count,
            after.inode_record_count
        ),
        (4, 4, 4)
    );
    assert_eq!(
        after.activity,
        ManifestActivity {
            mutations: before.activity.mutations.checked_add(2).expect("mutations"),
            ..before.activity
        }
    );
}

#[tokio::test]
async fn namespace_deletion_requires_a_successful_final_fold() {
    let dir = tempdir().expect("tempdir");
    let store = FailStore::new(
        LocalFsStore::new(dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Put,
        InjectedError::PermissionDenied("fold unavailable".to_owned()),
    );
    let ns = namespace_id("activity");
    let context = default_writer();
    NamespaceFixture::new(&store, &ns, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &ns, &context)
        .write_file_bytes("/file", b"final", None)
        .await
        .expect("file");
    let engine = namespace_engine(&store, &ns, &context);
    store.fail_all();
    engine
        .delete_namespace(Default::default())
        .await
        .expect_err("cannot delete with an unaccounted tail");
    assert!(!namespace_store(&store, &ns)
        .load_current_manifest()
        .await
        .expect("manifest")
        .payload()
        .status
        .is_deleted());
    assert_eq!(
        namespace_store(&store, &ns)
            .load_statistics()
            .await
            .expect("activity")
            .activity,
        ManifestActivity::default()
    );
    store.clear();
    engine
        .delete_namespace(Default::default())
        .await
        .expect("retry deletion");
    let final_manifest = namespace_store(&store, &ns)
        .load_current_manifest()
        .await
        .expect("deleted manifest");
    assert!(final_manifest.payload().status.is_deleted());
    assert_eq!(
        final_manifest
            .statistics()
            .expect("final activity")
            .activity,
        ManifestActivity {
            content_bytes: ActivityCounter::parse(5).expect("activity"),
            file_revisions: ActivityCounter::parse(1).expect("activity"),
            mutations: ActivityCounter::parse(1).expect("activity")
        }
    );
    assert_eq!(final_manifest.payload().head_seq, ChangeSeq(1));
    assert!(!final_manifest.payload().runs.is_empty());
}

#[tokio::test]
async fn failed_and_unacknowledged_fold_publications_do_not_double_count() {
    for applied in [false, true] {
        let dir = tempdir().expect("tempdir");
        let ns = namespace_id("activity");
        let context = default_writer();
        let inner = LocalFsStore::new(dir.path()).expect("store");
        NamespaceFixture::new(&inner, &ns, &context)
            .create()
            .await
            .expect("bootstrap");
        NamespaceFixture::new(&inner, &ns, &context)
            .write_file_bytes("/file", b"data", None)
            .await
            .expect("write");
        let store = FailStore::new(
            inner,
            KeyPredicate::manifest(&ns),
            OperationSelector::Put,
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
        let result = namespace_engine(&store, &ns, &context).fold_wal().await;
        assert_eq!(result.is_ok(), applied);
        let observed = namespace_store(&store, &ns)
            .load_statistics()
            .await
            .expect("statistics");
        assert_eq!(
            observed.activity.content_bytes.get(),
            if applied { 4 } else { 0 }
        );
        store.clear();
        for _ in 0..2 {
            namespace_engine(&store, &ns, &context)
                .fold_wal()
                .await
                .expect("restart and retry");
        }
        assert_eq!(
            namespace_store(&store, &ns)
                .load_statistics()
                .await
                .expect("statistics")
                .activity,
            ManifestActivity {
                content_bytes: ActivityCounter::parse(4).expect("activity"),
                file_revisions: ActivityCounter::parse(1).expect("activity"),
                mutations: ActivityCounter::parse(1).expect("activity"),
            }
        );
    }
}
