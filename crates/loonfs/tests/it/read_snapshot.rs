//! Multi-call namespace read snapshots.

use crate::common::{assert_core_error_kind, open_runtime_async, store};
use loonfs::{
    CreateNamespaceOptions, CreateSnapshotOptions, DestinationBehavior, ErrorCode, NamespaceId,
    PageRequest, PaginationPolicy, PutFileOptions,
};
use tempfile::tempdir;

#[tokio::test]
async fn pinned_namespace_reads_keep_one_head_across_later_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-reader-test").await;
    let namespace_id = NamespaceId::parse("snapshot-reads").expect("namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let created = runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"before",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");

    let snapshot = runtime
        .reader
        .pin_namespace(&namespace_id)
        .await
        .expect("pin namespace");
    assert_eq!(snapshot.head_seq(), created.committed_seq);
    let before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve initial file");

    runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"after",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file after pin");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/later.txt",
            b"later",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file after pin");

    let pinned_before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve pinned file");
    assert_eq!(pinned_before.revision_no(), before.revision_no());
    assert_eq!(
        snapshot
            .read_content_ref(
                pinned_before.content_ref().expect("file content reference"),
                64,
            )
            .await
            .expect("read pinned content"),
        b"before"
    );
    let later_error = snapshot
        .get_path_entry("/later.txt", Default::default())
        .await
        .expect_err("later file is absent from the pinned view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let page = snapshot
        .list_path_entries_page(
            "/",
            PageRequest {
                limit: PaginationPolicy::default()
                    .resolve_limit(None)
                    .expect("default page limit"),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("list pinned root");
    assert_eq!(page.head_seq, snapshot.head_seq());
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        ["/before.txt"]
    );
    let states = snapshot
        .resolve_current_files(&[before.inode_id])
        .await
        .expect("resolve pinned inode");
    assert_eq!(states[0].current_revision_no, before.revision_no());

    let latest = runtime
        .reader
        .get_file_bytes(&namespace_id, "/before.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"after");
    runtime
        .reader
        .get_path_entry(&namespace_id, "/later.txt", Default::default())
        .await
        .expect("latest view sees later file");
}

#[tokio::test]
async fn pinned_checkpoint_reads_answer_the_state_the_checkpoint_captured() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-checkpoint-test").await;
    let namespace_id = NamespaceId::parse("snapshot-checkpoint-reads").expect("namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"pinned",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");
    let checkpoint = runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"replaced",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file after the checkpoint");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/later.txt",
            b"later",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file after the checkpoint");

    let snapshot = runtime
        .reader
        .pin_namespace_at_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("pin namespace at checkpoint");
    let pinned = snapshot
        .get_path_entry("/pinned.txt", Default::default())
        .await
        .expect("resolve pinned file");
    assert_eq!(
        snapshot
            .read_content_ref(pinned.content_ref().expect("file content reference"), 64)
            .await
            .expect("read pinned content"),
        b"pinned"
    );
    let later_error = snapshot
        .get_path_entry("/later.txt", Default::default())
        .await
        .expect_err("later file is absent from the checkpointed view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let latest = runtime
        .reader
        .get_file_bytes(&namespace_id, "/pinned.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"replaced");
    runtime
        .reader
        .get_path_entry(&namespace_id, "/later.txt", Default::default())
        .await
        .expect("latest view sees later file");
}

#[tokio::test]
async fn a_released_checkpoint_refuses_a_pin_instead_of_reading_current_state() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime =
        open_runtime_async(store(temp_dir.path()), "snapshot-checkpoint-release-test").await;
    let namespace_id = NamespaceId::parse("snapshot-released-checkpoint").expect("namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"pinned",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");
    let checkpoint = runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    runtime
        .admin
        .release_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("release checkpoint");

    assert_core_error_kind(
        runtime
            .reader
            .pin_namespace_at_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
            .await,
        ErrorCode::CheckpointUnavailable,
    );
}

#[tokio::test]
async fn snapshot_pins_serve_captured_state_and_enforce_release() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-lease-read-test").await;
    let namespace_id = NamespaceId::parse("snapshot-lease-reads").expect("namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"captured",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create captured file");
    let captured = runtime
        .reader
        .get_path_entry(&namespace_id, "/pinned.txt", Default::default())
        .await
        .expect("resolve captured file");
    let now_ms = loonfs::current_time_ms().expect("current time");
    let snapshot = runtime
        .admin
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "reader".to_owned(),
                expires_at_ms: now_ms + 60_000,
            },
        )
        .await
        .expect("create snapshot");
    let snapshot_options = loonfs::StatPathOptions {
        snapshot_id: Some(snapshot.checkpoint_id.clone()),
        ..Default::default()
    };
    assert_core_error_kind(
        runtime
            .reader
            .get_path_entry(&namespace_id, "/pinned.txt", snapshot_options.clone())
            .await,
        ErrorCode::InvalidRequest,
    );

    runtime
        .put_file_bytes(
            &namespace_id,
            "/pinned.txt",
            b"current",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace captured file");
    let pinned = runtime
        .reader
        .pin_namespace_at_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("pin live snapshot");
    assert_core_error_kind(
        pinned.get_path_entry("/pinned.txt", snapshot_options).await,
        ErrorCode::InvalidRequest,
    );
    assert_eq!(pinned.head_seq(), snapshot.checkpoint_seq);
    assert_eq!(
        pinned
            .get_file_bytes("/pinned.txt")
            .await
            .expect("read captured bytes")
            .bytes,
        b"captured"
    );
    let download = pinned
        .create_download("/pinned.txt")
        .await
        .expect("resolve captured download");
    assert_eq!(
        download.revision_no,
        captured.revision_no().expect("file revision")
    );
    assert_eq!(
        &download.content_ref,
        captured.content_ref().expect("content reference")
    );

    runtime
        .admin
        .release_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("release snapshot");
    assert_core_error_kind(
        runtime
            .reader
            .pin_namespace_at_snapshot(&namespace_id, &snapshot.checkpoint_id)
            .await,
        ErrorCode::SnapshotGone,
    );
}
