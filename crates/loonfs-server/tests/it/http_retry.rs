//! HTTP idempotency, replay, and writer fencing behavior.

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::{CommitId, DestinationBehavior};
use loonfs_client::{ClientError, DeleteOptions, MutationOptions, NamespacePath, PutFileOptions};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_operation_rejects_same_commit_id_with_different_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current-conflict",
        "http-current-conflict",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let commit_id = CommitId::parse("req-phase-2a-conflict").expect("valid commit id");
    harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/first.txt").expect("first target"),
            b"first payload\n",
            &PutFileOptions {
                commit_id: Some(commit_id.clone()),
                message: Some("first commit".to_owned()),
                ..PutFileOptions::default()
            },
        )
        .await
        .expect("first put");

    match harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/second.txt").expect("second target"),
            b"second payload\n",
            &PutFileOptions {
                commit_id: Some(commit_id.clone()),
                message: Some("second commit".to_owned()),
                ..PutFileOptions::default()
            },
        )
        .await
    {
        Err(ClientError::Api {
            code,
            request_id,
            details,
            ..
        }) => {
            assert_eq!(code, "commit_id_reuse_conflict");
            // The error carries the caller's reconciliation identity as
            // structured fields, not prose (API spec, "Standard error
            // contract").
            let details = details.expect("structured details");
            assert_eq!(details.commit_id, Some(commit_id));
            let request_id = request_id.expect("request id");
            assert!(request_id.starts_with("req_"), "got `{request_id}`");
        }
        other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_commit_id_is_idempotent_and_conflicts_on_different_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-put",
        "http-put",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("demo", "/docs/retry.txt").expect("target");
    let commit_id = CommitId::parse("req-v1-put").expect("valid commit id");

    let first = harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit_id: Some(commit_id.clone()),
                message: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("first put");
    assert!(first.committed_seq.0 >= 1);

    let repeated = harness
        .client
        .put_file_bytes(
            &target,
            b"stable bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit_id: Some(commit_id.clone()),
                message: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("repeat put");
    assert_eq!(repeated, first);

    let entry = harness.client.stat_path(&target).await.expect("stat path");
    assert_eq!(entry.head_seq, first.committed_seq);
    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(bytes, b"stable bytes\n");

    match harness
        .client
        .put_file_bytes(
            &target,
            b"different bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit_id: Some(commit_id),
                message: None,
                expected_revision_no: None,
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => {
            assert_eq!(code, "commit_id_reuse_conflict")
        }
        other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_move_and_copy_commit_ids_are_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-ops",
        "http-ops",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let source = NamespacePath::parse("demo", "/docs/source.txt").expect("source");
    harness
        .client
        .put_file_bytes(&source, b"source bytes\n", &replace_file_options())
        .await
        .expect("seed source");

    let copied = NamespacePath::parse("demo", "/docs/copied.txt").expect("copied");
    let copy_first = harness
        .client
        .copy_path(
            &source,
            &copied,
            DestinationBehavior::NoReplace,
            &MutationOptions {
                commit_id: Some(CommitId::parse("req-v1-copy").expect("valid commit id")),
                message: None,
            },
        )
        .await
        .expect("copy first");
    let copy_repeated = harness
        .client
        .copy_path(
            &source,
            &copied,
            DestinationBehavior::NoReplace,
            &MutationOptions {
                commit_id: Some(CommitId::parse("req-v1-copy").expect("valid commit id")),
                message: None,
            },
        )
        .await
        .expect("copy repeat");
    assert_eq!(copy_repeated, copy_first);
    let source_entry = harness
        .client
        .stat_path(&source)
        .await
        .expect("source stat");
    let copied_entry = harness
        .client
        .stat_path(&copied)
        .await
        .expect("copied stat");
    assert_ne!(source_entry.inode_id, copied_entry.inode_id);
    assert_eq!(source_entry.content_ref, copied_entry.content_ref);

    let moved = NamespacePath::parse("demo", "/docs/moved.txt").expect("moved");
    let move_first = harness
        .client
        .move_path(
            &copied,
            &moved,
            DestinationBehavior::NoReplace,
            &MutationOptions {
                commit_id: Some(CommitId::parse("req-v1-move").expect("valid commit id")),
                message: None,
            },
        )
        .await
        .expect("move first");
    let move_repeated = harness
        .client
        .move_path(
            &copied,
            &moved,
            DestinationBehavior::NoReplace,
            &MutationOptions {
                commit_id: Some(CommitId::parse("req-v1-move").expect("valid commit id")),
                message: None,
            },
        )
        .await
        .expect("move repeat");
    assert_eq!(move_repeated, move_first);
    match harness.client.stat_path(&copied).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => unreachable!("expected path_not_found for moved-from path, got {other:?}"),
    }
    let moved_entry = harness.client.stat_path(&moved).await.expect("moved stat");
    assert_eq!(moved_entry.inode_id, copied_entry.inode_id);

    let delete_first = harness
        .client
        .delete_path(
            &moved,
            &DeleteOptions {
                commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                message: None,
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete first");
    let delete_repeated = harness
        .client
        .delete_path(
            &moved,
            &DeleteOptions {
                commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                message: None,
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete repeat");
    assert_eq!(delete_repeated, delete_first);
    match harness.client.stat_path(&moved).await {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => unreachable!("expected path_not_found for deleted path, got {other:?}"),
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_share_one_store_with_last_writer_wins_fencing() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let server_a = start_server(test_config(
        store_root.clone(),
        "loonfs-server-a",
        "two-server-smoke",
    ))
    .await;
    let server_b = start_server(test_config(
        store_root,
        "loonfs-server-b",
        "two-server-smoke",
    ))
    .await;
    let client_a = server_a.client.clone();
    let client_b = server_b.client.clone();

    client_a
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let host_a_target = NamespacePath::parse("demo", "/docs/host-a.txt").expect("host a target");
    client_a
        .put_file_bytes(&host_a_target, b"host a\n", &replace_file_options())
        .await
        .expect("host a write");

    // Server B's first semantic write acquires the epoch immediately:
    // there is no lease to wait out, only last-writer-wins fencing.
    let host_b_target = NamespacePath::parse("demo", "/docs/host-b.txt").expect("host b target");
    let moved = client_b
        .move_path(
            &host_a_target,
            &host_b_target,
            DestinationBehavior::NoReplace,
            &MutationOptions::default(),
        )
        .await
        .expect("host b takes over on first write");
    assert!(
        moved.committed_seq.0 >= 2,
        "expected later commit seq, got {}",
        moved.committed_seq.0
    );

    // Server A's session is fenced terminally: its writes fail with
    // `writer_fenced` and keep failing, with no silent reacquisition.
    let host_c_target = NamespacePath::parse("demo", "/docs/host-c.txt").expect("host c target");
    for attempt in 0..2 {
        match client_a
            .put_file_bytes(&host_c_target, b"host a again\n", &replace_file_options())
            .await
        {
            Err(ClientError::Api {
                code,
                message,
                details,
                ..
            }) => {
                assert_eq!(code, "writer_fenced", "attempt {attempt}");
                // The details name the winner and when it acquired. Writer
                // ids are process labels, so two runs on one machine can
                // share one; the stamp is what separates them, and carrying
                // it structurally means a caller never parses the message.
                let details = details.expect("a fenced error carries structured details");
                assert_eq!(
                    details.active_writer.as_deref(),
                    Some("loonfs-server-b"),
                    "attempt {attempt}"
                );
                let acquired_at_ms = details
                    .active_acquired_at_ms
                    .expect("fenced details carry the winner's acquisition stamp");
                assert!(
                    message.contains(&format!(
                        "(writer `loonfs-server-b`, acquired at {acquired_at_ms} ms)"
                    )),
                    "the structured stamp should be the one the message renders: {message}"
                );
            }
            other => unreachable!("expected writer_fenced on attempt {attempt}, got {other:?}"),
        }
    }

    // Fencing gates writes only; server A still reads the moved file.
    let host_b_entry = client_a
        .stat_path(&host_b_target)
        .await
        .expect("stat host b file");
    assert_eq!(host_b_entry.head_seq.0, moved.committed_seq.0);
    let host_b_bytes = client_a
        .get_file_bytes(&host_b_target)
        .await
        .expect("read host b file");
    assert_eq!(host_b_bytes, b"host a\n");

    server_a.server.abort();
    server_b.server.abort();
}
