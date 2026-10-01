//! User-visible snapshots.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CheckpointOwnerSummary, CreateCheckpointOptions, CreateNamespaceOptions, CreateSnapshotOptions,
    DeleteNamespaceOptions, ErrorCode, ListSnapshotsResponse, LoonFs, NamespaceId, PageRequest,
    PaginationPolicy, PutFileOptions, ReadOnly, SharedObjectStore, SnapshotPolicy,
};
use loonfs_objectstore::keys::pin_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, OperationKind,
    RecordingStore,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
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

#[test]
fn a_created_snapshot_is_listed_with_its_snapshot_owner() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "snapshot-create-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let expires_at_ms = 4_102_444_800_000;
    let snapshot = block_on(namespace.create_snapshot(
        CreateSnapshotOptions {
            name: "report-run".to_owned(),
            expires_at_ms,
        },
        &SnapshotPolicy::default(),
    ))
    .expect("create snapshot");
    assert_eq!(
        snapshot.owner,
        CheckpointOwnerSummary::Snapshot {
            name: "report-run".to_owned(),
        }
    );

    let listed = block_on(list_snapshots(&fs.reader, &namespace_id)).expect("list snapshots");
    let listed_snapshot = listed
        .snapshots
        .iter()
        .find(|listed| listed.snapshot_id == snapshot.checkpoint_id.clone())
        .expect("the snapshot is in the snapshot listing");
    assert_eq!(listed_snapshot.name, "report-run");
    assert_eq!(listed_snapshot.expires_at_ms, expires_at_ms);
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
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost checkpoint write acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-write").await;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    store.fail_next(1);

    let snapshot = namespace
        .create_snapshot(
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
            &SnapshotPolicy::default(),
        )
        .await
        .expect("reconcile the durable snapshot record");

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(
        listed.snapshots[0].snapshot_id,
        snapshot.checkpoint_id.clone()
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
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost snapshot extension acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-extension").await;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let snapshot = namespace
        .create_snapshot(
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX - 1,
            },
            &SnapshotPolicy::default(),
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    let extended = namespace
        .extend_snapshot(
            &snapshot.checkpoint_id,
            u64::MAX,
            &SnapshotPolicy {
                max_lifetime_ms: u64::MAX,
                ..SnapshotPolicy::default()
            },
        )
        .await
        .expect("reconcile the durable snapshot extension");

    assert_eq!(extended.expires_at_ms, u64::MAX);
    assert_eq!(store.attempts(), 1);
}

#[tokio::test]
async fn snapshot_delete_reports_an_uncertain_delete_without_recreating_the_pin() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-delete");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(pin_prefix(&namespace_id)),
            OperationClass::Delete,
            InjectedError::Transport("lost snapshot delete acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-delete").await;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let snapshot = namespace
        .create_snapshot(
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
            &SnapshotPolicy::default(),
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    assert_core_error_kind(
        namespace.delete_snapshot(&snapshot.checkpoint_id).await,
        ErrorCode::ServerError,
    );
    assert_core_error_kind(
        namespace.delete_snapshot(&snapshot.checkpoint_id).await,
        ErrorCode::SnapshotNotFound,
    );

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert!(listed.snapshots.is_empty());
}

#[tokio::test]
async fn a_namespace_at_its_snapshot_limit_refuses_a_create_without_writing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-quota-full");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::prefix(pin_prefix(&namespace_id)),
    ));
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-quota-full").await;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let options = |name: &str| CreateSnapshotOptions {
        name: name.to_owned(),
        expires_at_ms: u64::MAX,
    };
    let policy = SnapshotPolicy {
        max_live_per_namespace: 1,
        ..SnapshotPolicy::default()
    };
    namespace
        .create_snapshot(options("kept"), &policy)
        .await
        .expect("create the snapshot that fills the limit");
    fs.put_file_bytes(
        &namespace_id,
        "/after.txt",
        b"after",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("leave a WAL tail for a create to fold");
    let manifest_no = fs
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics before the refused create")
        .current_manifest_no;
    store.reset();

    assert_core_error_kind(
        namespace.create_snapshot(options("refused"), &policy).await,
        ErrorCode::SnapshotQuotaExceeded,
    );

    let counts = store.counts();
    assert_eq!((counts.puts, counts.deletes), (0, 0));
    assert_eq!(
        fs.maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("diagnostics after the refused create")
            .current_manifest_no,
        manifest_no
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_snapshot_creates_cannot_both_claim_the_last_quota_slot() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-quota-race");
    let pin_key_prefix = pin_prefix(&namespace_id);
    let checkpoint_writes = Arc::new(AtomicUsize::new(0));
    let checkpoint_writes_seen = checkpoint_writes.clone();
    let checkpoint_write_gate = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        move |operation| {
            let matches = operation.key().starts_with(&pin_key_prefix)
                && matches!(
                    operation.kind(),
                    OperationKind::Put { .. } | OperationKind::PutStreamed { .. }
                );
            if matches {
                checkpoint_writes_seen.fetch_add(1, Ordering::SeqCst);
            }
            matches
        },
    ));
    let checkpoint_list_prefix = pin_prefix(&namespace_id);
    let checkpoint_lists = Arc::new(AtomicUsize::new(0));
    let checkpoint_lists_seen = checkpoint_lists.clone();
    let checkpoint_list_gate = Arc::new(BlockingStore::matching(
        checkpoint_write_gate.clone(),
        move |operation| {
            let matches = operation.key() == checkpoint_list_prefix
                && matches!(operation.kind(), OperationKind::List);
            if matches {
                checkpoint_lists_seen.fetch_add(1, Ordering::SeqCst);
            }
            matches
        },
    ));
    // Neither create may delete its tentative record until both have listed;
    // otherwise the later listing sees one live snapshot and that create
    // succeeds.
    let checkpoint_delete_prefix = pin_prefix(&namespace_id);
    let checkpoint_deletes = Arc::new(AtomicUsize::new(0));
    let checkpoint_deletes_seen = checkpoint_deletes.clone();
    let checkpoint_delete_gate = Arc::new(BlockingStore::matching(
        checkpoint_list_gate.clone(),
        move |operation| {
            let matches = operation.key().starts_with(&checkpoint_delete_prefix)
                && matches!(operation.kind(), OperationKind::Delete);
            if matches {
                checkpoint_deletes_seen.fetch_add(1, Ordering::SeqCst);
            }
            matches
        },
    ));
    let object_store: SharedObjectStore = checkpoint_delete_gate.clone();
    let fs = open_runtime_async(object_store, "snapshot-quota-race").await;
    fs.create_namespace(
        &namespace_id,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    checkpoint_write_gate.arm();
    checkpoint_delete_gate.arm();
    let policy = SnapshotPolicy {
        max_live_per_namespace: 1,
        ..SnapshotPolicy::default()
    };
    let first_writer = namespace.clone();
    let first = tokio::spawn(async move {
        first_writer
            .create_snapshot(
                CreateSnapshotOptions {
                    name: "first".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                &policy,
            )
            .await
    });
    let second_writer = namespace.clone();
    let second = tokio::spawn(async move {
        second_writer
            .create_snapshot(
                CreateSnapshotOptions {
                    name: "second".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                &policy,
            )
            .await
    });

    wait_for_operations(&checkpoint_writes, 2).await;
    checkpoint_list_gate.arm();
    checkpoint_write_gate.release();
    wait_for_operations(&checkpoint_lists, 4).await;
    checkpoint_list_gate.release();
    wait_for_operations(&checkpoint_deletes, 2).await;
    checkpoint_delete_gate.release();

    let first = first.await.expect("first create task");
    let second = second.await.expect("second create task");
    assert_core_error_kind(first, ErrorCode::SnapshotQuotaExceeded);
    assert_core_error_kind(second, ErrorCode::SnapshotQuotaExceeded);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots after raced creates");
    assert!(listed.snapshots.is_empty());
}

async fn wait_for_operations(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected} operations"));
}

#[test]
fn tombstoned_namespace_keeps_checkpoint_inventory_and_user_delete_available() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime(store.clone(), "checkpoint-tombstone-setup");
    let source = namespace_id("source");
    let target = namespace_id("target");

    fs.create_namespace_blocking(
        &source,
        CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create source namespace");
    fs.put_file_bytes_blocking(
        &source,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put source file");
    fs.fork_namespace_blocking(&source, &target)
        .expect("fork source namespace");
    let user_checkpoint = fs
        .create_checkpoint_blocking(&source)
        .expect("create user checkpoint");

    let before_delete = block_on(collect_checkpoints(&fs.maintenance, &source))
        .expect("list checkpoints before deletion");
    let fork_checkpoint = before_delete
        .checkpoints
        .iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.owner,
                CheckpointOwnerSummary::Fork {
                    target_namespace_id
                } if target_namespace_id == &target
            )
        })
        .expect("fork-owned checkpoint")
        .checkpoint_id
        .clone();

    let deleter = block_on(
        LoonFs::builder_with_store(store.clone())
            .writer_id("checkpoint-tombstone-deleter")
            .build(),
    )
    .expect("build deleting writer");
    let namespace = deleter.open_namespace(&source).expect("open namespace");
    block_on(namespace.delete(DeleteNamespaceOptions::default())).expect("delete source namespace");

    let maintenance = block_on(
        LoonFs::builder_with_store(store)
            .writer_id("checkpoint-tombstone-observer")
            .build(),
    )
    .expect("build post-delete maintenance")
    .maintenance(loonfs_test_support::ids::writer_id(
        "checkpoint-tombstone-observer",
    ));
    let listed = block_on(collect_checkpoints(&maintenance, &source))
        .expect("list checkpoints on deleted namespace");
    assert_eq!(listed.checkpoints.len(), 2);
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == user_checkpoint.checkpoint_id));
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == fork_checkpoint));

    let deleted = block_on(maintenance.delete_checkpoint(&source, &user_checkpoint.checkpoint_id))
        .expect("delete user checkpoint on deleted namespace");
    assert_eq!(deleted.checkpoint_id, user_checkpoint.checkpoint_id);
    assert_core_error_kind(
        block_on(maintenance.delete_checkpoint(&source, &fork_checkpoint)),
        ErrorCode::CheckpointNotFound,
    );
    assert_core_error_kind(
        block_on(maintenance.diagnostics(&source)),
        ErrorCode::NamespaceDeleted,
    );
    assert_core_error_kind(
        block_on(maintenance.create_checkpoint(
            &source,
            CreateCheckpointOptions {
                name: "after-delete".to_owned(),
                ttl_ms: None,
            },
        )),
        ErrorCode::NamespaceDeleted,
    );
}

#[tokio::test]
async fn shared_read_options_select_the_snapshot_for_paths_and_inodes() {
    let directory = tempdir().expect("directory");
    let writer = LoonFs::builder_with_store(Arc::new(
        LocalFsStore::new(directory.path()).expect("store"),
    ))
    .writer_id("snapshot-options")
    .build()
    .await
    .expect("writer");
    let namespace = namespace_id("options");
    writer
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file_bytes(
            "/file",
            b"before",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("file");
    let reader = writer.read_only();
    let namespace_reader = reader.namespace(&namespace);
    let root = namespace_reader.stat("/").await.expect("root");
    let before = namespace_reader.stat("/file").await.expect("file");
    let snapshot = namespace_writer
        .create_snapshot(
            CreateSnapshotOptions {
                name: "options".to_owned(),
                expires_at_ms: u64::MAX,
            },
            &SnapshotPolicy::default(),
        )
        .await
        .expect("snapshot");
    namespace_writer
        .delete_path(
            "/file",
            loonfs::DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete");
    let options = loonfs::StatOptions {
        snapshot_id: Some(snapshot.checkpoint_id.clone()),
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
            .resolve_limit(None)
            .expect("limit"),
        cursor: None,
    };
    let paths = namespace_reader
        .list_with_options(
            "/",
            &loonfs::ListOptions {
                snapshot_id: Some(snapshot.checkpoint_id.clone()),
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
                snapshot_id: Some(snapshot.checkpoint_id),
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
