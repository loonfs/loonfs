//! HTTP path mutation semantics and readable conflicts.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{
    v0::{CommitOp, CommitRequest as ApiCommitRequest, CommitSubmissionRequest},
    CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId,
};
use loonfs_client::{ClientError, DeleteOptions, MutationOptions, NamespacePath, PutFileOptions};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_stat_omits_the_root_name_and_carries_named_child_names() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-root-name",
        "http-root-name",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        harness
            .client
            .create_directory(
                &NamespacePath::parse("demo", "/docs").expect("directory path"),
                &loonfs_client::CreateDirectoryOptions::default(),
            )
            .expect("create directory");

        let stat_json = |encoded_path: &str| {
            let response = raw_agent()
                .get(&format!(
                    "{}/v0/namespaces/demo/filesystem/stat?path={encoded_path}",
                    harness.server_url
                ))
                .set("authorization", "Bearer test-token")
                .call()
                .expect("stat request");
            serde_json::from_reader::<_, serde_json::Value>(response.into_reader())
                .expect("stat JSON")
        };

        let root = stat_json("%2F");
        assert!(root.get("display_name").is_none());
        let child = stat_json("%2Fdocs");
        assert_eq!(child["display_name"], "docs");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_no_replace_and_copy_preserve_cli_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-copy-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let source = NamespacePath::parse("demo", "/docs/hello.txt").expect("source");
        harness
            .client
            .put_file_bytes(&source, b"hello over http\n", &PutFileOptions::default())
            .expect("initial create");

        match harness
            .client
            .put_file_bytes(&source, b"conflict\n", &PutFileOptions::default())
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_conflict"),
            other => unreachable!("expected path_conflict, got {other:?}"),
        }

        harness
            .client
            .put_file_bytes(&source, b"forced overwrite\n", &replace_file_options())
            .expect("forced overwrite");

        let destination = NamespacePath::parse("demo", "/docs/copy.txt").expect("destination");
        harness
            .client
            .copy_path(
                &source,
                &destination,
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            )
            .expect("copy path");

        let source_entry = harness.client.stat_path(&source).expect("source stat");
        let dest_entry = harness.client.stat_path(&destination).expect("dest stat");
        assert_ne!(source_entry.inode_id, dest_entry.inode_id);
        assert_eq!(source_entry.content_ref, dest_entry.content_ref);
        let dest_bytes = harness
            .client
            .get_file_bytes(&destination)
            .expect("read copied file");
        assert_eq!(dest_bytes, b"forced overwrite\n");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_name_collision_reports_readable_error_message() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-collision-message",
        "http-collision-message",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");

        let completed = stage_uploaded_content(&harness.client, &namespace, b"taken bytes\n");
        let content_ref = completed.content_ref.clone();
        harness
            .client
            .commit_operations(
                &namespace,
                &CommitSubmissionRequest {
                    commit: ApiCommitRequest {
                        commit_id: CommitId::parse("req-collision-create")
                            .expect("valid commit id"),
                        preconditions: Vec::new(),
                        ops: vec![CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: loonfs_api::DisplayName::parse("taken.txt")
                                .expect("valid display name"),
                            content_ref: content_ref.clone(),
                        }],
                        message: None,
                    },
                    content_tokens: vec![validated_content_token(&completed)],
                },
            )
            .expect("create file");

        match harness.client.commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-collision-repeat").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("taken.txt")
                            .expect("valid display name"),
                        content_ref,
                    }],
                    message: None,
                },
                content_tokens: vec![validated_content_token(&completed)],
            },
        ) {
            Err(ClientError::Api {
                status,
                code,
                message,
                ..
            }) => {
                assert_eq!(status, 409);
                assert_eq!(code, "path_conflict");
                // The error body carries the human-readable Display message,
                // not Rust Debug syntax.
                assert!(
                    message.contains("collides with existing name `taken.txt`"),
                    "expected readable collision message, got {message:?}"
                );
                assert!(
                    !message.contains("InodeId(") && !message.contains('{'),
                    "expected no Debug formatting in message, got {message:?}"
                );
            }
            other => unreachable!("expected path_conflict, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_delete_path_behavior_controls_recursive_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-delete-behavior",
        "http-delete-behavior",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let child = NamespacePath::parse("demo", "/docs/child.txt").expect("child path");
        harness
            .client
            .put_file_bytes(&child, b"child", &replace_file_options())
            .expect("write child");

        let dir = NamespacePath::parse("demo", "/docs").expect("dir path");
        let non_recursive = harness
            .client
            .delete_path(&dir, &DeleteOptions::default())
            .expect_err("non-recursive delete rejects non-empty dir");
        match non_recursive {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 409);
                assert_eq!(code, "directory_not_empty");
            }
            other => unreachable!("expected directory_not_empty, got {other:?}"),
        }

        harness
            .client
            .delete_path(
                &dir,
                &DeleteOptions {
                    behavior: DeleteDirectoryBehavior::Recursive,
                    ..DeleteOptions::default()
                },
            )
            .expect("recursive delete succeeds");
        match harness.client.stat_path(&child) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => unreachable!("expected path_not_found after recursive delete, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}
