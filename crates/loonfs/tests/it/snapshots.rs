//! User-visible snapshots.

use crate::common::*;
use loonfs::{
    CheckpointOwner, CreateSnapshotOptions, ErrorCode, ForkNamespaceOptions, ListSnapshotsResponse,
    LoonFs, NamespaceId, PageRequest, PaginationPolicy, ReadOnly, SharedObjectStore,
};
use loonfs_objectstore::keys::pin_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationSelector};
use std::sync::Arc;
use tempfile::tempdir;

async fn list_snapshots(
    reader: &LoonFs<ReadOnly>,
    namespace_id: &NamespaceId,
) -> loonfs::Result<ListSnapshotsResponse> {
    let namespace = reader.namespace(namespace_id);
    let mut pager = namespace.list_snapshots();
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.snapshots.extend(page.snapshots);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

/// Checks that a created snapshot is listed with its name, expiry, and
/// captured sequence.
#[test]
fn a_created_snapshot_is_listed_with_its_expiry() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "snapshot-create-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");

    let snapshot = block_on(namespace.create_snapshot(
        "report-run",
        &CreateSnapshotOptions {
            ttl_ms: Some(60_000),
        },
    ))
    .expect("create snapshot");
    assert_eq!(snapshot.name, "report-run");
    let expires_at_ms = snapshot
        .expires_at_ms
        .expect("a snapshot with a TTL expires");
    assert!(expires_at_ms >= snapshot.created_at_ms + 60_000);

    let listed = block_on(list_snapshots(&fs.reader, &namespace_id)).expect("list snapshots");
    let listed_snapshot = listed
        .snapshots
        .iter()
        .find(|listed| listed.snapshot_id == snapshot.snapshot_id.clone())
        .expect("the snapshot is in the snapshot listing");
    assert_eq!(listed_snapshot.name, "report-run");
    assert_eq!(listed_snapshot.expires_at_ms, Some(expires_at_ms));
    assert_eq!(listed_snapshot.captured_seq, snapshot.captured_seq);
}

#[tokio::test]
async fn snapshot_create_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-write");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(pin_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost checkpoint write acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-write").await;
    fs.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    store.fail_next(1);

    let snapshot = namespace
        .create_snapshot("report-run", &CreateSnapshotOptions::default())
        .await
        .expect("reconcile the durable snapshot record");

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(
        listed.snapshots[0].snapshot_id,
        snapshot.snapshot_id.clone()
    );
}

#[tokio::test]
async fn snapshot_extension_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-extension");
    let pin_key_prefix = pin_prefix(&namespace_id);
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(pin_key_prefix),
            OperationSelector::CompareAndSwap,
            InjectedError::Transport("lost snapshot extension acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-extension").await;
    fs.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let snapshot = namespace
        .create_snapshot(
            "report-run",
            &CreateSnapshotOptions {
                ttl_ms: Some(60_000),
            },
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    let extended = namespace
        .extend_snapshot(&snapshot.snapshot_id, 3_600_000)
        .await
        .expect("reconcile the durable snapshot extension");

    assert!(extended.expires_at_ms > snapshot.expires_at_ms);
    assert_eq!(store.attempts(), 1);
}

/// Checks that a delete whose acknowledgement is lost reports the error, and
/// that the retry succeeds without recreating the pin.
#[tokio::test]
async fn a_retried_snapshot_delete_succeeds_after_an_uncertain_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-delete");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(pin_prefix(&namespace_id)),
            OperationSelector::Delete,
            InjectedError::Transport("lost snapshot delete acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-delete").await;
    fs.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let snapshot = namespace
        .create_snapshot("report-run", &CreateSnapshotOptions::default())
        .await
        .expect("create snapshot");
    store.fail_next(1);

    assert_core_error_kind(
        namespace.delete_snapshot(&snapshot.snapshot_id).await,
        ErrorCode::ServerError,
    );
    let retried = namespace
        .delete_snapshot(&snapshot.snapshot_id)
        .await
        .expect("a retried delete of a deleted snapshot succeeds");
    assert_eq!(retried.snapshot_id, snapshot.snapshot_id);

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert!(listed.snapshots.is_empty());
}

/// Checks that after a namespace is deleted, the checkpoint inventory still
/// lists its snapshot and fork pin, the snapshot can still be deleted, and a
/// fork pin's ID returns `snapshot_not_found`.
#[tokio::test]
async fn a_deleted_namespace_keeps_its_checkpoint_inventory_and_snapshot_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime_async(store.clone(), "checkpoint-tombstone-setup").await;
    let source = namespace_id("source");
    let target = namespace_id("target");

    fs.create_namespace(&source, &loonfs_test_support::test_actor())
        .await
        .expect("create source namespace");
    fs.put_file(
        &source,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .await
    .expect("put source file");
    fs.writer
        .fork_namespace(&source, &target, &loonfs_test_support::test_actor())
        .await
        .expect("fork source namespace");
    let snapshot = fs
        .create_snapshot(&source)
        .await
        .expect("create a snapshot without an expiry");

    let before_delete = collect_checkpoints(&fs.maintenance, &source)
        .await
        .expect("list checkpoints before deletion");
    let fork_checkpoint = before_delete
        .checkpoints
        .iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.owner,
                CheckpointOwner::Fork {
                    target_namespace_id
                } if target_namespace_id == &target
            )
        })
        .expect("fork-owned checkpoint")
        .checkpoint_id
        .clone();

    let deleter = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "checkpoint-tombstone-deleter",
        ))
        .build()
        .await
        .expect("build deleting writer");
    let namespace = deleter.open_namespace(&source).expect("open namespace");
    namespace.delete().await.expect("delete source namespace");

    let observer = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id(
            "checkpoint-tombstone-observer",
        ))
        .build()
        .await
        .expect("build post-delete runtime");
    let maintenance = observer.maintenance(loonfs_test_support::ids::writer_id(
        "checkpoint-tombstone-observer",
    ));
    let listed = collect_checkpoints(&maintenance, &source)
        .await
        .expect("list checkpoints on deleted namespace");
    assert_eq!(listed.checkpoints.len(), 2);
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == snapshot.snapshot_id));
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == fork_checkpoint));

    let namespace = observer.open_namespace(&source).expect("open namespace");
    let deleted = namespace
        .delete_snapshot(&snapshot.snapshot_id)
        .await
        .expect("delete a snapshot on a deleted namespace");
    assert_eq!(deleted.snapshot_id, snapshot.snapshot_id);
    assert_core_error_kind(
        namespace.delete_snapshot(&fork_checkpoint).await,
        ErrorCode::SnapshotNotFound,
    );
    assert_core_error_kind(
        maintenance.diagnostics(&source).await,
        ErrorCode::NamespaceDeleted,
    );
    assert_core_error_kind(
        namespace
            .create_snapshot("after-delete", &CreateSnapshotOptions::default())
            .await,
        ErrorCode::NamespaceDeleted,
    );
}

/// Checks that an expired snapshot can't be read or forked, and that it
/// stays in the snapshot listing and the checkpoint inventory with its past
/// expiry.
#[tokio::test]
async fn an_expired_snapshot_is_not_found_for_reads_and_forks_but_stays_listed() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(
        1_750_000_000_000,
    ));
    let writer = LoonFs::builder_with_store(store(temp_dir.path()))
        .writer_id(loonfs_test_support::ids::writer_id("snapshot-expiry"))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build writer");
    let source = namespace_id("expiring");
    writer
        .create_namespace(&source, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer.open_namespace(&source).expect("open namespace");
    namespace
        .put_file("/file", b"captured", &loonfs_test_support::test_actor())
        .await
        .expect("put file");
    let snapshot = namespace
        .create_snapshot(
            "short",
            &CreateSnapshotOptions {
                ttl_ms: Some(1_000),
            },
        )
        .await
        .expect("create snapshot");
    let expires_at_ms = snapshot.expires_at_ms.expect("an expiry");
    clock.set_ms(expires_at_ms);

    let reader = writer.read_only().namespace(&source);
    assert_core_error_kind(
        reader.read_view_at_snapshot(&snapshot.snapshot_id).await,
        ErrorCode::SnapshotNotFound,
    );
    assert_core_error_kind(
        reader
            .stat_with_options(
                "/file",
                &loonfs::StatOptions {
                    at: loonfs::ReadAt::Snapshot(snapshot.snapshot_id.clone()),
                    ..Default::default()
                },
            )
            .await,
        ErrorCode::SnapshotNotFound,
    );
    assert_core_error_kind(
        writer
            .fork_namespace_with_options(
                &source,
                &namespace_id("expired-fork"),
                &loonfs_test_support::test_actor(),
                &ForkNamespaceOptions {
                    snapshot_id: Some(snapshot.snapshot_id.clone()),
                },
            )
            .await,
        ErrorCode::SnapshotNotFound,
    );
    assert_core_error_kind(
        namespace
            .extend_snapshot(&snapshot.snapshot_id, 60_000)
            .await,
        ErrorCode::SnapshotNotFound,
    );

    let listed = list_snapshots(&writer.read_only(), &source)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots, vec![snapshot.clone()]);
    assert_eq!(
        reader
            .get_snapshot(&snapshot.snapshot_id)
            .await
            .expect("retrieve the expired snapshot"),
        snapshot
    );
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("snapshot-expiry"));
    let checkpoints = collect_checkpoints(&maintenance, &source)
        .await
        .expect("list checkpoints");
    assert_eq!(checkpoints.checkpoints.len(), 1);
    assert_eq!(
        checkpoints.checkpoints[0].expires_at_ms,
        Some(expires_at_ms)
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shared_read_options_select_the_snapshot_for_paths_and_inodes() {
    let directory = tempdir().expect("directory");
    let writer = LoonFs::builder_with_store(Arc::new(
        LocalFsStore::new(directory.path()).expect("store"),
    ))
    .writer_id(loonfs_test_support::ids::writer_id("snapshot-options"))
    .build()
    .await
    .expect("writer");
    let namespace = namespace_id("options");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file("/file", b"before", &loonfs_test_support::test_actor())
        .await
        .expect("file");
    let reader = writer.read_only();
    let namespace_reader = reader.namespace(&namespace);
    let root = namespace_reader.stat("/").await.expect("root");
    let before = namespace_reader.stat("/file").await.expect("file");
    let snapshot = namespace_writer
        .create_snapshot("options", &CreateSnapshotOptions::default())
        .await
        .expect("snapshot");
    namespace_writer
        .delete_path("/file", &loonfs_test_support::test_actor())
        .await
        .expect("delete");
    let options = loonfs::StatOptions {
        at: loonfs::ReadAt::Snapshot(snapshot.snapshot_id.clone()),
        ..Default::default()
    };
    assert_eq!(
        namespace_reader
            .stat_with_options("/file", &options)
            .await
            .expect("snapshot path"),
        before
    );
    assert_eq!(
        namespace_reader
            .stat_by_inode_with_options(before.inode_id, &options)
            .await
            .expect("snapshot inode"),
        before
    );
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .effective_limit(None)
            .expect("limit"),
        cursor: None,
    };
    let paths = namespace_reader
        .list_with_options(
            "/",
            &loonfs::ListOptions {
                at: loonfs::ReadAt::Snapshot(snapshot.snapshot_id.clone()),
                ..Default::default()
            },
        )
        .page(request.clone())
        .await
        .expect("snapshot paths");
    let inodes = namespace_reader
        .list_by_inode_with_options(
            root.inode_id,
            &loonfs::ListOptions {
                at: loonfs::ReadAt::Snapshot(snapshot.snapshot_id),
                ..Default::default()
            },
        )
        .page(request)
        .await
        .expect("snapshot children");
    assert_eq!(paths.entries.len(), 1);
    assert_eq!(paths.entries, inodes.entries);
    assert_eq!(paths.head_seq, snapshot.captured_seq);
}
