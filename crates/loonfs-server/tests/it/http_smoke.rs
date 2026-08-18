//! Basic end-to-end HTTP namespace and file flows.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs::publish::{
    MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_MESSAGE_BYTES,
    MAX_COMMIT_OPERATIONS,
};
use loonfs_api::{
    ChangeSeq, CommitId, DestinationBehavior, InodeKind, DEFAULT_MAX_PAGE_LIMIT,
    DEFAULT_PAGE_LIMIT, LIMIT_COMMIT_MAX_CONTENT_TOKENS, LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
    LIMIT_COMMIT_MAX_MESSAGE_BYTES, LIMIT_COMMIT_MAX_OPERATIONS, LIMIT_DOWNLOAD_MAX_CONCURRENT,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_PAGINATION_DEFAULT, LIMIT_PAGINATION_MAX,
    LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES, LIMIT_UPLOAD_MAX_CONCURRENT,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES,
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

    let namespace = namespace_id("doomed");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = NamespacePath::parse("doomed", "/note.txt").expect("parse path");
    harness
        .client
        .put_file_bytes(&target, b"last words", &replace_file_options())
        .await
        .expect("write file");

    // A stale precondition refuses the delete and deletes nothing.
    let stale = harness
        .client
        .delete_namespace(&namespace, Some(ChangeSeq(0)))
        .await
        .expect_err("stale precondition");
    match stale {
        ClientError::Api {
            status,
            code,
            message,
            details,
            ..
        } => {
            assert_eq!(status, 409);
            assert_eq!(code, "stale_head");
            // The rejection reports both sequences, so a caller that still
            // means to delete knows what to retry against without parsing
            // anything: in words for a person, typed for a program.
            assert_eq!(message, "expected head sequence 0, found 1");
            let details = details.expect("a stale precondition carries its sequences");
            assert_eq!(details.expected_head_seq, Some(ChangeSeq(0)));
            assert_eq!(details.actual_head_seq, Some(ChangeSeq(1)));
        }
        other => panic!("expected stale_head, got {other:?}"),
    }
    harness
        .client
        .namespace_status(&namespace)
        .await
        .expect("still alive after failed precondition");

    let response = harness
        .client
        .delete_namespace(&namespace, None)
        .await
        .expect("delete namespace");
    assert_eq!(response.namespace_id.as_str(), "doomed");
    assert_eq!(response.head_seq, ChangeSeq(1));

    // Terminal: status is 410, reads fail, repeat deletes report
    // deleted, and the id is retired.
    let status = harness
        .client
        .namespace_status(&namespace)
        .await
        .expect_err("deleted namespace has no status");
    match status {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 410);
            assert_eq!(code, "namespace_deleted");
        }
        other => panic!("expected namespace_deleted, got {other:?}"),
    }
    let read = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect_err("reads observe the deleted namespace");
    match read {
        ClientError::Api { code, .. } => assert_eq!(code, "namespace_deleted"),
        other => panic!("expected namespace_deleted, got {other:?}"),
    }
    let again = harness
        .client
        .delete_namespace(&namespace, None)
        .await
        .expect_err("repeat delete");
    match again {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 410);
            assert_eq!(code, "namespace_deleted");
        }
        other => panic!("expected namespace_deleted, got {other:?}"),
    }
    let recreate = harness
        .client
        .create_namespace(&namespace)
        .await
        .expect_err("the id is retired");
    match recreate {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 410);
            assert_eq!(code, "namespace_deleted");
        }
        other => panic!("expected namespace_deleted, got {other:?}"),
    }
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

    let capabilities = harness
        .client
        .capabilities()
        .await
        .expect("fetch capabilities");
    assert_eq!(capabilities.protocol_version, "v0");
    assert!(capabilities.has_profile("core/v0"));
    assert!(capabilities.has_profile("admin/v0"));
    assert!(!capabilities.supports("core.namespaces.list"));
    assert!(capabilities.supports("core.namespaces.create"));
    assert!(capabilities.supports("core.namespaces.fork"));
    assert!(capabilities.supports("core.namespaces.delete"));
    // A local-filesystem deployment presigns nothing, so none of the three
    // transfer capabilities is advertised — and because it presigns no
    // uploads either, no file it holds can be larger than it will proxy.
    assert!(!capabilities.supports("core.uploads.direct_put"));
    assert!(!capabilities.supports("core.uploads.direct_multipart"));
    assert!(!capabilities.supports("core.downloads.direct_get"));
    assert_eq!(
        capabilities.limits.get(LIMIT_PAGINATION_DEFAULT),
        Some(&u64::from(DEFAULT_PAGE_LIMIT))
    );
    assert_eq!(
        capabilities.limits.get(LIMIT_PAGINATION_MAX),
        Some(&u64::from(DEFAULT_MAX_PAGE_LIMIT))
    );
    // The two halves a client pre-validates a write against: what this
    // deployment will carry over the wire, and what the runtime will accept
    // as one commit. Both are advertised, so neither has to be discovered by
    // being rejected.
    let config = test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-smoke",
    );
    for (limit, expected) in [
        (LIMIT_UPLOAD_MAX_CONTENT_BYTES, config.max_upload_bytes),
        (LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES, 8 * 1024 * 1024),
        (LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, config.max_download_bytes),
        (
            LIMIT_UPLOAD_MAX_CONCURRENT,
            config.max_concurrent_uploads as u64,
        ),
        (
            LIMIT_DOWNLOAD_MAX_CONCURRENT,
            config.max_concurrent_downloads as u64,
        ),
        (LIMIT_COMMIT_MAX_OPERATIONS, MAX_COMMIT_OPERATIONS as u64),
        (
            LIMIT_COMMIT_MAX_CONTENT_TOKENS,
            MAX_COMMIT_CONTENT_TOKENS as u64,
        ),
        (
            LIMIT_COMMIT_MAX_EXTERNAL_CONTENT_REFS,
            MAX_COMMIT_EXTERNAL_CONTENT_REFS as u64,
        ),
        (
            LIMIT_COMMIT_MAX_MESSAGE_BYTES,
            MAX_COMMIT_MESSAGE_BYTES as u64,
        ),
    ] {
        assert_eq!(
            capabilities.limits.get(limit),
            Some(&expected),
            "`{limit}` is not advertised with the value this deployment enforces"
        );
    }

    let cached = harness
        .client
        .capabilities()
        .await
        .expect("cached capabilities");
    assert_eq!(cached, capabilities);
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

    let created = harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    assert_eq!(created.namespace_id.as_str(), "demo");
    assert_eq!(created.head_seq, ChangeSeq(0));
    assert_eq!(created.retention_floor_seq, ChangeSeq(0));
    let directory = NamespacePath::parse("demo", "/notes").expect("parse directory path");
    harness
        .client
        .create_directory(
            &directory,
            &CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create directory");
    let directory_entry = harness
        .client
        .stat_path(&directory, &Default::default())
        .await
        .expect("stat directory");
    assert_eq!(directory_entry.inode_kind(), InodeKind::Directory);

    let target = NamespacePath::parse("demo", "/notes/hello.txt").expect("parse namespace path");
    let written = harness
        .client
        .put_file_bytes(
            &target,
            b"hello over http\n",
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: Some(CommitId::parse("smoke-write-1").expect("valid commit id")),
                    message: None,
                },
                expected_revision_no: None,
            },
        )
        .await
        .expect("write bytes");
    // Path operations echo the commit id they committed under.
    assert_eq!(written.commit_id.as_str(), "smoke-write-1");
    assert_eq!(written.committed_seq, ChangeSeq(2));

    let entry = harness
        .client
        .stat_path(&target, &Default::default())
        .await
        .expect("stat path");
    assert_eq!(entry.size_bytes(), Some(16));

    let bytes = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read file");
    assert_eq!(bytes, b"hello over http\n");

    let status = harness
        .client
        .namespace_status(&namespace_id("demo"))
        .await
        .expect("namespace status");
    assert_eq!(status.namespace_id.as_str(), "demo");
    assert_eq!(status.head_seq, ChangeSeq(2));

    let missing = harness
        .client
        .namespace_status(&namespace_id("absent"))
        .await
        .expect_err("missing namespace status");
    match missing {
        ClientError::Api { status, code, .. } => {
            assert_eq!(status, 404);
            assert_eq!(code, "namespace_not_found");
        }
        other => panic!("expected API error for missing namespace, got {other:?}"),
    }

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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
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
        Ok(_) => panic!("GET /v0/namespaces must not return a namespace list"),
        Err(error) => panic!("unexpected transport error: {error}"),
    }

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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let source_path = NamespacePath::parse("demo", "/docs/shared.txt").expect("source path");
    let clone_path = NamespacePath::parse("clone", "/docs/shared.txt").expect("clone path");
    harness
        .client
        .put_file_bytes(&source_path, b"base\n", &replace_file_options())
        .await
        .expect("write source");

    let forked = harness
        .client
        .fork_namespace(&namespace_id("demo"), &namespace_id("clone"))
        .await
        .expect("fork namespace");
    assert_eq!(forked.namespace_id.as_str(), "clone");
    assert_eq!(forked.head_seq, ChangeSeq(1));
    assert_eq!(forked.retention_floor_seq, ChangeSeq(1));

    let source_entry = harness
        .client
        .stat_path(&source_path, &Default::default())
        .await
        .expect("source stat");
    let clone_entry = harness
        .client
        .stat_path(&clone_path, &Default::default())
        .await
        .expect("clone stat");
    assert_eq!(source_entry.content_ref(), clone_entry.content_ref());
    assert_eq!(
        harness
            .client
            .get_file_bytes(&clone_path)
            .await
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
        .await
        .expect("replace source");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&clone_path)
            .await
            .expect("read clone after source write"),
        b"base\n"
    );

    let clone_write = harness
        .client
        .put_file_bytes(&clone_path, b"clone-after-fork\n", &replace_file_options())
        .await
        .expect("replace clone");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert_eq!(
        harness
            .client
            .get_file_bytes(&source_path)
            .await
            .expect("read source"),
        b"source-after-fork\n"
    );
    assert_eq!(
        harness
            .client
            .get_file_bytes(&clone_path)
            .await
            .expect("read clone"),
        b"clone-after-fork\n"
    );

    match harness
        .client
        .list_changes(&namespace_id("clone"), ChangeSeq(0), None)
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
        other => panic!("expected rebootstrap_required, got {other:?}"),
    }
    let clone_changes = harness
        .client
        .list_changes(&namespace_id("clone"), ChangeSeq(1), None)
        .await
        .expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].committed_seq, ChangeSeq(2));

    harness.server.abort();
}
