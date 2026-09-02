//! HTTP path mutation semantics and readable conflicts.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::{DeleteDirectoryBehavior, DestinationBehavior};
use loonfs_client::{ClientError, CopyOptions, DeleteOptions, NamespacePath, PutFileOptions};
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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    harness
        .client
        .create_directory(
            &NamespacePath::parse("demo", "/docs").expect("directory path"),
            &loonfs_client::CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create directory");

    let stat_json = |encoded_path: &str| {
        let response = raw_agent()
            .get(&format!(
                "{}/v0/namespaces/demo/filesystem/entry?path={encoded_path}",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call()
            .expect("stat request");
        serde_json::from_reader::<_, serde_json::Value>(response.into_reader()).expect("stat JSON")
    };

    let root = stat_json("%2F");
    assert!(root.get("display_name").is_none());
    let child = stat_json("%2Fdocs");
    assert_eq!(child["display_name"], "docs");

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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let source = NamespacePath::parse("demo", "/docs/hello.txt").expect("source");
    harness
        .client
        .put_file_bytes(
            &source,
            b"hello over http\n",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("initial create");

    match harness
        .client
        .put_file_bytes(
            &source,
            b"conflict\n",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_conflict"),
        other => panic!("expected path_conflict, got {other:?}"),
    }

    harness
        .client
        .put_file_bytes(&source, b"forced overwrite\n", &replace_file_options())
        .await
        .expect("forced overwrite");

    let destination = NamespacePath::parse("demo", "/docs/copy.txt").expect("destination");
    harness
        .client
        .copy_path(
            &source,
            &destination,
            &CopyOptions {
                behavior: DestinationBehavior::NoReplace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
                expected_destination_inode_id: None,
                expected_destination_revision_no: None,
            },
        )
        .await
        .expect("copy path");

    let source_entry = harness
        .client
        .get_path_entry(&source, &Default::default())
        .await
        .expect("source stat");
    let dest_entry = harness
        .client
        .get_path_entry(&destination, &Default::default())
        .await
        .expect("dest stat");
    assert_ne!(source_entry.inode_id, dest_entry.inode_id);
    assert_eq!(source_entry.content_ref(), dest_entry.content_ref());
    let dest_bytes = harness
        .client
        .get_file_bytes(&destination, &Default::default())
        .await
        .expect("read copied file");
    assert_eq!(dest_bytes, b"forced overwrite\n");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_name_collision_reports_readable_error_message() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-collision-message",
        "http-collision-message",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/taken.txt").expect("target"),
            b"taken bytes\n",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file");

    // A case-folded sibling collides with the stored name: the conflict
    // must name both spellings so the caller can see why.
    match harness
        .client
        .put_file_bytes(
            &NamespacePath::parse("demo", "/TAKEN.txt").expect("colliding target"),
            b"taken bytes\n",
            &PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
    {
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
                message.contains("destination already exists at `/TAKEN.txt`")
                    && message.contains("stored as `taken.txt`"),
                "expected readable collision message, got {message:?}"
            );
            assert!(
                !message.contains("InodeId(") && !message.contains('{'),
                "expected no Debug formatting in message, got {message:?}"
            );
        }
        other => panic!("expected path_conflict, got {other:?}"),
    }

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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let child = NamespacePath::parse("demo", "/docs/child.txt").expect("child path");
    harness
        .client
        .put_file_bytes(&child, b"child", &replace_file_options())
        .await
        .expect("write child");

    let dir = NamespacePath::parse("demo", "/docs").expect("dir path");
    let non_recursive = harness
        .client
        .delete_path(&dir, &DeleteOptions::new(loonfs_test_support::test_actor()))
        .await
        .expect_err("non-recursive delete rejects non-empty dir");
    match non_recursive {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 409);
            assert_eq!(code, "directory_not_empty");
        }
        other => panic!("expected directory_not_empty, got {other:?}"),
    }

    harness
        .client
        .delete_path(
            &dir,
            &DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("recursive delete succeeds");
    match harness
        .client
        .get_path_entry(&child, &Default::default())
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
        other => panic!("expected path_not_found after recursive delete, got {other:?}"),
    }

    harness.server.abort();
}
