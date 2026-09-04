//! User-visible snapshots.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CheckpointOwnerSummary, CreateCheckpointOptions, CreateNamespaceOptions, CreateSnapshotOptions,
    DeleteNamespaceOptions, ErrorCode, FsMaintenance, FsReader, FsWriter, ListSnapshotsResponse,
    NamespaceId, PageRequest, PaginationPolicy, PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::keys::checkpoint_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, OperationKind,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

async fn list_snapshots(
    reader: &FsReader,
    namespace_id: &NamespaceId,
) -> loonfs::Result<ListSnapshotsResponse> {
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = reader.list_snapshots_pager(namespace_id, request);
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let expires_at_ms = 4_102_444_800_000;
    let snapshot = block_on(fs.writer.create_snapshot(
        &namespace_id,
        CreateSnapshotOptions {
            name: "report-run".to_owned(),
            expires_at_ms,
        },
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
        .find(|listed| listed.snapshot_id == snapshot.checkpoint_id)
        .expect("the snapshot is in the snapshot listing");
    assert_eq!(listed_snapshot.name, "report-run");
    assert_eq!(listed_snapshot.expires_at_ms, expires_at_ms);
    assert_eq!(listed_snapshot.head_seq, snapshot.checkpoint_seq);
}

#[tokio::test]
async fn snapshot_create_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-write");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost checkpoint write acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-write").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    store.fail_next(1);

    let snapshot = fs
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
        )
        .await
        .expect("reconcile the durable snapshot record");

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert_eq!(listed.snapshots.len(), 1);
    assert_eq!(listed.snapshots[0].snapshot_id, snapshot.checkpoint_id);
}

#[tokio::test]
async fn snapshot_extension_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-extension");
    let checkpoint_key_prefix = checkpoint_prefix(&namespace_id);
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_key_prefix),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost snapshot extension acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-extension").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let snapshot = fs
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX - 1,
            },
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    let extended = fs
        .writer
        .extend_snapshot(&namespace_id, &snapshot.checkpoint_id, u64::MAX, u64::MAX)
        .await
        .expect("reconcile the durable snapshot extension");

    assert_eq!(extended.expires_at_ms, u64::MAX);
    assert_eq!(store.attempts(), 1);
}

#[tokio::test]
async fn snapshot_release_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-release");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost snapshot release acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-release").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let snapshot = fs
        .writer
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    fs.writer
        .release_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("reconcile the durable snapshot release");

    assert_eq!(store.attempts(), 1);
    let listed = list_snapshots(&fs.reader, &namespace_id)
        .await
        .expect("list snapshots");
    assert!(listed.snapshots.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_snapshot_creates_cannot_both_claim_the_last_quota_slot() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-quota-race");
    let checkpoint_key_prefix = checkpoint_prefix(&namespace_id);
    let checkpoint_writes = Arc::new(AtomicUsize::new(0));
    let checkpoint_writes_seen = checkpoint_writes.clone();
    let checkpoint_write_gate = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        move |operation| {
            let matches = operation.key().starts_with(&checkpoint_key_prefix)
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
    let checkpoint_list_prefix = checkpoint_prefix(&namespace_id);
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
    let object_store: SharedObjectStore = checkpoint_list_gate.clone();
    let fs = open_runtime_async(object_store, "snapshot-quota-race").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    checkpoint_list_gate.arm();
    checkpoint_write_gate.arm();
    let first_writer = fs.writer.clone();
    let first_namespace = namespace_id.clone();
    let first = tokio::spawn(async move {
        first_writer
            .create_snapshot_with_quota(
                &first_namespace,
                CreateSnapshotOptions {
                    name: "first".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                0,
                1,
            )
            .await
    });
    let second_writer = fs.writer.clone();
    let second_namespace = namespace_id.clone();
    let second = tokio::spawn(async move {
        second_writer
            .create_snapshot_with_quota(
                &second_namespace,
                CreateSnapshotOptions {
                    name: "second".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                0,
                1,
            )
            .await
    });

    wait_for_operations(&checkpoint_writes, 2).await;
    checkpoint_write_gate.release();
    wait_for_operations(&checkpoint_lists, 2).await;
    checkpoint_list_gate.release();

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
fn tombstoned_namespace_keeps_checkpoint_inventory_and_user_release_available() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime(store.clone(), "checkpoint-tombstone-setup");
    let source = namespace_id("source");
    let target = namespace_id("target");

    fs.create_namespace_blocking(&source, CreateNamespaceOptions::default())
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
        FsWriter::builder_with_store(store.clone())
            .writer_id("checkpoint-tombstone-deleter")
            .build(),
    )
    .expect("build deleting writer");
    block_on(deleter.delete_namespace(&source, DeleteNamespaceOptions::default()))
        .expect("delete source namespace");

    let maintenance = block_on(
        FsMaintenance::builder_with_store(store)
            .actor_id("checkpoint-tombstone-observer")
            .build(),
    )
    .expect("build post-delete maintenance");
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

    let released =
        block_on(maintenance.release_checkpoint(&source, &user_checkpoint.checkpoint_id))
            .expect("release user checkpoint on deleted namespace");
    assert_eq!(released.checkpoint_id, user_checkpoint.checkpoint_id);
    assert_core_error_kind(
        block_on(maintenance.release_checkpoint(&source, &fork_checkpoint)),
        ErrorCode::InvalidRequest,
    );
    assert_core_error_kind(
        block_on(maintenance.get_namespace_diagnostics(&source)),
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
