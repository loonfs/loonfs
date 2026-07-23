//! HTTP idempotency, replay, and writer fencing behavior.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{
    v0::{CommitOp, CommitRequest as ApiCommitRequest, CommitSubmissionRequest},
    CommitId, DestinationBehavior, InodeId,
};
use loonfs_client::{ClientError, DeleteOptions, MutationOptions, NamespacePath, PutFileOptions};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_rejects_same_commit_id_with_different_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current-conflict",
        "http-current-conflict",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");

        let first = stage_uploaded_content(&harness.client, &namespace, b"first payload\n");
        let first_request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("req-phase-2a-conflict").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("first.txt")
                        .expect("valid display name"),
                    content_ref: first.content_ref.clone(),
                }],
                message: Some("first commit".to_owned()),
            },
            content_tokens: vec![validated_content_token(&first)],
        };
        harness
            .client
            .commit_operations(&namespace, &first_request)
            .expect("first commit");

        let second = stage_uploaded_content(&harness.client, &namespace, b"second payload\n");
        let conflicting_request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: first_request.commit.commit_id.clone(),
                preconditions: first_request.commit.preconditions.clone(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("second.txt")
                        .expect("valid display name"),
                    content_ref: second.content_ref.clone(),
                }],
                message: Some("second commit".to_owned()),
            },
            content_tokens: vec![validated_content_token(&second)],
        };

        match harness
            .client
            .commit_operations(&namespace, &conflicting_request)
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
                assert_eq!(
                    details.commit_id,
                    Some(first_request.commit.commit_id.clone())
                );
                let request_id = request_id.expect("request id");
                assert!(request_id.starts_with("req_"), "got `{request_id}`");
            }
            other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
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
                },
            )
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
                },
            )
            .expect("repeat put");
        assert_eq!(repeated, first);

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.head_seq, first.committed_seq);
        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"stable bytes\n");

        match harness.client.put_file_bytes(
            &target,
            b"different bytes\n",
            &PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                commit_id: Some(commit_id),
            },
        ) {
            Err(ClientError::Api { code, .. }) => {
                assert_eq!(code, "commit_id_reuse_conflict")
            }
            other => unreachable!("expected commit_id_reuse_conflict, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let source = NamespacePath::parse("demo", "/docs/source.txt").expect("source");
        harness
            .client
            .put_file_bytes(&source, b"source bytes\n", &replace_file_options())
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
                },
            )
            .expect("copy first");
        let copy_repeated = harness
            .client
            .copy_path(
                &source,
                &copied,
                DestinationBehavior::NoReplace,
                &MutationOptions {
                    commit_id: Some(CommitId::parse("req-v1-copy").expect("valid commit id")),
                },
            )
            .expect("copy repeat");
        assert_eq!(copy_repeated, copy_first);
        let source_entry = harness.client.stat_path(&source).expect("source stat");
        let copied_entry = harness.client.stat_path(&copied).expect("copied stat");
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
                },
            )
            .expect("move first");
        let move_repeated = harness
            .client
            .move_path(
                &copied,
                &moved,
                DestinationBehavior::NoReplace,
                &MutationOptions {
                    commit_id: Some(CommitId::parse("req-v1-move").expect("valid commit id")),
                },
            )
            .expect("move repeat");
        assert_eq!(move_repeated, move_first);
        match harness.client.stat_path(&copied) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => unreachable!("expected path_not_found for moved-from path, got {other:?}"),
        }
        let moved_entry = harness.client.stat_path(&moved).expect("moved stat");
        assert_eq!(moved_entry.inode_id, copied_entry.inode_id);

        let delete_first = harness
            .client
            .delete_path(
                &moved,
                &DeleteOptions {
                    commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                    ..DeleteOptions::default()
                },
            )
            .expect("delete first");
        let delete_repeated = harness
            .client
            .delete_path(
                &moved,
                &DeleteOptions {
                    commit_id: Some(CommitId::parse("req-v1-delete").expect("valid commit id")),
                    ..DeleteOptions::default()
                },
            )
            .expect("delete repeat");
        assert_eq!(delete_repeated, delete_first);
        match harness.client.stat_path(&moved) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => unreachable!("expected path_not_found for deleted path, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

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

    tokio::task::spawn_blocking(move || {
        client_a
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let host_a_target =
            NamespacePath::parse("demo", "/docs/host-a.txt").expect("host a target");
        client_a
            .put_file_bytes(&host_a_target, b"host a\n", &replace_file_options())
            .expect("host a write");

        // Server B's first semantic write acquires the epoch immediately:
        // there is no lease to wait out, only last-writer-wins fencing.
        let host_b_target =
            NamespacePath::parse("demo", "/docs/host-b.txt").expect("host b target");
        let moved = client_b
            .move_path(
                &host_a_target,
                &host_b_target,
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            )
            .expect("host b takes over on first write");
        assert!(
            moved.committed_seq.0 >= 2,
            "expected later commit seq, got {}",
            moved.committed_seq.0
        );

        // Server A's session is fenced terminally: its writes fail with
        // `writer_fenced` and keep failing, with no silent reacquisition.
        let host_c_target =
            NamespacePath::parse("demo", "/docs/host-c.txt").expect("host c target");
        for attempt in 0..2 {
            match client_a.put_file_bytes(
                &host_c_target,
                b"host a again\n",
                &replace_file_options(),
            ) {
                Err(ClientError::Api { code, .. }) => {
                    assert_eq!(code, "writer_fenced", "attempt {attempt}")
                }
                other => unreachable!("expected writer_fenced on attempt {attempt}, got {other:?}"),
            }
        }

        // Fencing gates writes only; server A still reads the moved file.
        let host_b_entry = client_a
            .stat_path(&host_b_target)
            .expect("stat host b file");
        assert_eq!(host_b_entry.head_seq.0, moved.committed_seq.0);
        let host_b_bytes = client_a
            .read_file_bytes(&host_b_target)
            .expect("read host b file");
        assert_eq!(host_b_bytes, b"host a\n");
    })
    .await
    .expect("join blocking task");

    server_a.server.abort();
    server_b.server.abort();
}
