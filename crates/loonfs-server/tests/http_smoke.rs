//! Basic end-to-end HTTP namespace and file flows.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{
    ChangeSeq, CommitId, DestinationBehavior, InodeKind, DEFAULT_MAX_PAGE_LIMIT,
    DEFAULT_PAGE_LIMIT, LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX,
};
use loonfs_client::{ClientError, CreateDirectoryOptions, NamespacePath, PutFileOptions};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_namespace_is_terminal_and_retires_the_id() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("doomed");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("doomed", "/note.txt").expect("parse path");
        harness
            .client
            .put_file_bytes(&target, b"last words", &replace_file_options())
            .expect("write file");

        // A stale precondition refuses the delete and deletes nothing.
        let stale = harness
            .client
            .delete_namespace(&namespace, Some(ChangeSeq(0)))
            .expect_err("stale precondition");
        match stale {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 409);
                assert_eq!(code, "stale_head");
            }
            other => unreachable!("expected stale_head, got {other:?}"),
        }
        harness
            .client
            .namespace_status(&namespace)
            .expect("still alive after failed precondition");

        let response = harness
            .client
            .delete_namespace(&namespace, None)
            .expect("delete namespace");
        assert_eq!(response.namespace_id.as_str(), "doomed");
        assert_eq!(response.head_seq, ChangeSeq(1));

        // Terminal: status is 410, reads fail, repeat deletes report
        // deleted, and the id is retired.
        let status = harness
            .client
            .namespace_status(&namespace)
            .expect_err("deleted namespace has no status");
        match status {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 410);
                assert_eq!(code, "namespace_deleted");
            }
            other => unreachable!("expected namespace_deleted, got {other:?}"),
        }
        let read = harness
            .client
            .get_file_bytes(&target)
            .expect_err("reads observe the deleted namespace");
        match read {
            ClientError::Api { code, .. } => assert_eq!(code, "namespace_deleted"),
            other => unreachable!("expected namespace_deleted, got {other:?}"),
        }
        let again = harness
            .client
            .delete_namespace(&namespace, None)
            .expect_err("repeat delete");
        match again {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 410);
                assert_eq!(code, "namespace_deleted");
            }
            other => unreachable!("expected namespace_deleted, got {other:?}"),
        }
        let recreate = harness
            .client
            .create_namespace(&namespace)
            .expect_err("the id is retired");
        match recreate {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 410);
                assert_eq!(code, "namespace_deleted");
            }
            other => unreachable!("expected namespace_deleted, got {other:?}"),
        }
    })
    .await
    .expect("blocking task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capabilities_endpoint_advertises_capabilities() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let capabilities = harness.client.capabilities().expect("fetch capabilities");
        assert_eq!(capabilities.protocol_version, "v0");
        assert!(capabilities.has_profile("core/v0"));
        assert!(capabilities.has_profile("admin/v0"));
        assert!(!capabilities.supports("core.namespaces.list"));
        assert!(capabilities.supports("core.namespaces.create"));
        assert!(capabilities.supports("core.namespaces.fork"));
        assert!(capabilities.supports("core.namespaces.delete"));
        assert!(!capabilities.supports("core.uploads.direct_put"));
        assert_eq!(
            capabilities.limits.get(LIMIT_PAGINATION_DEFAULT),
            Some(&u64::from(DEFAULT_PAGE_LIMIT))
        );
        assert_eq!(
            capabilities.limits.get(LIMIT_PAGINATION_MAX),
            Some(&u64::from(DEFAULT_MAX_PAGE_LIMIT))
        );

        let cached = harness.client.capabilities().expect("cached capabilities");
        assert_eq!(cached, capabilities);
    })
    .await
    .expect("blocking task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_round_trip_supports_namespace_create_and_file_read_write() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let directory = NamespacePath::parse("demo", "/notes").expect("parse directory path");
        harness
            .client
            .create_directory(&directory, &CreateDirectoryOptions::default())
            .expect("create directory");
        let directory_entry = harness
            .client
            .stat_path(&directory)
            .expect("stat directory");
        assert_eq!(directory_entry.inode_kind, InodeKind::Directory);

        let target =
            NamespacePath::parse("demo", "/notes/hello.txt").expect("parse namespace path");
        let written = harness
            .client
            .put_file_bytes(
                &target,
                b"hello over http\n",
                &PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    commit_id: Some(CommitId::parse("smoke-write-1").expect("valid commit id")),
                },
            )
            .expect("write bytes");
        // Path operations echo the commit id they committed under.
        assert_eq!(written.commit_id.as_str(), "smoke-write-1");
        assert_eq!(written.committed_seq, ChangeSeq(2));

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.size_bytes, Some(16));

        let bytes = harness.client.get_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello over http\n");

        let status = harness
            .client
            .namespace_status(&namespace_id("demo"))
            .expect("namespace status");
        assert_eq!(status.namespace_id.as_str(), "demo");
        assert_eq!(status.head_seq, ChangeSeq(2));

        let missing = harness
            .client
            .namespace_status(&namespace_id("absent"))
            .expect_err("missing namespace status");
        match missing {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 404);
                assert_eq!(code, "namespace_not_found");
            }
            other => unreachable!("expected API error for missing namespace, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_namespace_listing_route_is_not_exposed() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-no-namespace-list",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create demo");

        let response = raw_agent()
            .get(&format!("{}/v0/namespaces", harness.server_url))
            .set("authorization", "Bearer test-token")
            .call();
        match response {
            Err(ureq::Error::Status(status, _)) => {
                assert!(
                    status == 404 || status == 405,
                    "GET /v0/namespaces should be missing or method-not-allowed, got {status}"
                );
            }
            Ok(_) => unreachable!("GET /v0/namespaces must not return a namespace list"),
            Err(error) => unreachable!("unexpected transport error: {error}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_namespace_fork_shares_content_and_diverges() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-fork",
        "http-fork-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let source_path = NamespacePath::parse("demo", "/docs/shared.txt").expect("source path");
        let clone_path = NamespacePath::parse("clone", "/docs/shared.txt").expect("clone path");
        harness
            .client
            .put_file_bytes(&source_path, b"base\n", &replace_file_options())
            .expect("write source");

        let forked = harness
            .client
            .fork_namespace(&namespace_id("demo"), &namespace_id("clone"))
            .expect("fork namespace");
        assert_eq!(forked.namespace_id.as_str(), "clone");

        let source_entry = harness.client.stat_path(&source_path).expect("source stat");
        let clone_entry = harness.client.stat_path(&clone_path).expect("clone stat");
        assert_eq!(source_entry.content_ref, clone_entry.content_ref);
        assert_eq!(
            harness
                .client
                .get_file_bytes(&clone_path)
                .expect("read clone"),
            b"base\n"
        );

        harness
            .client
            .put_file_bytes(
                &source_path,
                b"source-after-fork\n",
                &replace_file_options(),
            )
            .expect("replace source");
        assert_eq!(
            harness
                .client
                .get_file_bytes(&clone_path)
                .expect("read clone after source write"),
            b"base\n"
        );

        let clone_write = harness
            .client
            .put_file_bytes(&clone_path, b"clone-after-fork\n", &replace_file_options())
            .expect("replace clone");
        assert_eq!(clone_write.committed_seq, ChangeSeq(2));
        assert_eq!(
            harness
                .client
                .get_file_bytes(&source_path)
                .expect("read source"),
            b"source-after-fork\n"
        );
        assert_eq!(
            harness
                .client
                .get_file_bytes(&clone_path)
                .expect("read clone"),
            b"clone-after-fork\n"
        );

        match harness
            .client
            .list_changes(&namespace_id("clone"), ChangeSeq(0), None)
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
            other => unreachable!("expected rebootstrap_required, got {other:?}"),
        }
        let clone_changes = harness
            .client
            .list_changes(&namespace_id("clone"), ChangeSeq(1), None)
            .expect("clone changes");
        assert_eq!(clone_changes.changes.len(), 1);
        assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}
