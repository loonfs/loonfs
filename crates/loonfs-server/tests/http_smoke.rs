use bytes::Bytes;
use loonfs::content_tokens::mint_content_token;
use loonfs::publish::MAX_COMMIT_CONTENT_TOKENS;
use loonfs_api::{
    v0::{
        BeginUploadRequest, CommitDelta, CommitOp, CommitRequest as ApiCommitRequest,
        CommitSubmissionRequest, CompleteUploadRequest, CompleteUploadResponse,
        ValidatedContentToken,
    },
    AdvanceRetentionResponse, ApiError, ChangeSeq, CheckpointId, CommitId, CommitResponse,
    ContentRef, CreateCheckpointResponse, DestinationBehavior, ErrorCode, FilesystemOperation,
    FilesystemOperationRequest, InodeId, InodeKind, ListPathEntriesResponse, ManifestId,
    NamespaceId, RevisionNo, DEFAULT_MAX_PAGE_LIMIT, DEFAULT_PAGE_LIMIT, LIMIT_PAGINATION_DEFAULT,
    LIMIT_PAGINATION_MAX,
};
use loonfs_client::{Client, ClientConfig, ClientError, MutationOptions, NamespacePath};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::{ConfiguredObjectStore, ObjectStore};
use loonfs_server::{app, RuntimeCacheConfigOverrides, ServerConfig, StoreConfig};
use serde_json::json;
use std::future::Future;
use std::io::Read as _;
use std::path::PathBuf;
use tempfile::tempdir;

const TEST_CONTENT_TOKEN_SECRET: &str = "test-content-token-secret";

/// Raw-request agent with socket inactivity timeouts. A starved or wedged
/// server must produce a readable transport error, not an indefinite hang
/// or a panic inside the HTTP client's response path — the failure mode a
/// loaded CI runner once hit through the default timeout-less agent.
fn raw_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_read(std::time::Duration::from_secs(30))
        .timeout_write(std::time::Duration::from_secs(30))
        .build()
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

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
            .write_file_bytes(&target, b"last words", &MutationOptions::default())
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
            .read_file_bytes(&target)
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
async fn config_endpoint_advertises_capabilities() {
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
async fn http_paginates_directory_listing_and_rejects_cursor_path_mismatch() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-directory-pagination",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
        let other = NamespacePath::parse("demo", "/other").expect("other path");
        harness
            .client
            .create_directory(&docs, false, &MutationOptions::default())
            .expect("create docs dir");
        harness
            .client
            .create_directory(&other, false, &MutationOptions::default())
            .expect("create other dir");
        for name in ["a.txt", "b.txt", "c.txt"] {
            let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
            harness
                .client
                .write_file_bytes(&path, name.as_bytes(), &MutationOptions::default())
                .expect("write file");
        }

        let first_page = harness
            .client
            .list_path_page(&docs, Some(2), None)
            .expect("first directory page");
        assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
        let cursor = first_page.next_cursor.clone().expect("directory cursor");

        let second_page = harness
            .client
            .list_path_page(&docs, Some(2), Some(&cursor))
            .expect("second directory page");
        assert_eq!(entry_names(&second_page), vec!["c.txt"]);
        assert_eq!(second_page.next_cursor, None);

        let full_listing = harness
            .client
            .list_path_all(&docs)
            .expect("full directory list");
        assert_eq!(entry_names(&full_listing), vec!["a.txt", "b.txt", "c.txt"]);
        assert_eq!(full_listing.next_cursor, None);

        let mismatch = harness
            .client
            .list_path_page(&other, Some(2), Some(&cursor))
            .expect_err("directory cursor must match listed path");
        match mismatch {
            ClientError::Api { status, code, .. } => {
                assert_eq!(status, 400);
                assert_eq!(code, "invalid_request");
            }
            other => unreachable!("expected cursor rejection, got {other:?}"),
        }

        let raw_first_page: ListPathEntriesResponse = get_json(
            &format!(
                "{}/v0/namespaces/demo/filesystem/list?path=/docs&limit=1",
                harness.server_url
            ),
            "test-token",
        )
        .expect("raw first directory page");
        assert_eq!(raw_first_page.entries.len(), 1);
        assert!(raw_first_page.next_cursor.is_some());

        let nonnumeric_limit: Result<ListPathEntriesResponse, ApiError> = get_json(
            &format!(
                "{}/v0/namespaces/demo/filesystem/list?path=/docs&limit=not-a-number",
                harness.server_url
            ),
            "test-token",
        );
        let error = nonnumeric_limit.expect_err("nonnumeric limit rejected");
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains("invalid limit"));
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

/// The client aggregates pages without re-ordering: mixed-case names must come
/// back in the server's canonical casefolded name-key order, not in raw
/// display-name byte order (`B.txt` sorts after `a.txt`, not before).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_client_listing_preserves_canonical_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-listing-order",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");
        let docs = NamespacePath::parse("demo", "/docs").expect("docs path");
        harness
            .client
            .create_directory(&docs, false, &MutationOptions::default())
            .expect("create docs dir");
        for name in ["B.txt", "a.txt", "c.txt"] {
            let path = NamespacePath::parse("demo", &format!("/docs/{name}")).expect("file path");
            harness
                .client
                .write_file_bytes(&path, name.as_bytes(), &MutationOptions::default())
                .expect("write file");
        }

        let listing = harness.client.list_path_all(&docs).expect("directory list");
        assert_eq!(entry_names(&listing), vec!["a.txt", "B.txt", "c.txt"]);
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
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
            .create_directory(&directory, false, &MutationOptions::default())
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
            .write_file_bytes(
                &target,
                b"hello over http\n",
                &MutationOptions::with_commit_id(
                    CommitId::parse("smoke-write-1").expect("valid commit id"),
                ),
            )
            .expect("write bytes");
        // Path operations echo the commit id they committed under.
        assert_eq!(written.commit_id.as_str(), "smoke-write-1");
        assert_eq!(written.committed_seq, ChangeSeq(2));

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.size_bytes, Some(16));

        let bytes = harness.client.read_file_bytes(&target).expect("read file");
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
async fn http_rejects_invalid_namespace_ids_in_body_and_path() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-invalid-ns",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        assert_invalid_namespace_response(
            raw_agent()
                .post(&format!("{}/v0/namespaces", harness.server_url))
                .set("authorization", "Bearer test-token")
                .send_json(json!({ "namespace_id": "bad/name" })),
        );

        assert_invalid_namespace_response(
            raw_agent()
                .get(&format!(
                    "{}/v0/namespaces/bad%25/filesystem/list?path=/",
                    harness.server_url
                ))
                .set("authorization", "Bearer test-token")
                .call(),
        );
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_content_rejects_invalid_upload_id() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-invalid-upload-id",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace(&namespace_id("demo"))
            .expect("create namespace");

        let invalid_upload_id = ["upl", "123"].join("-");
        match harness
            .client
            .upload_content(&namespace_id("demo"), &invalid_upload_id, b"hello")
        {
            Err(ClientError::Api { status, code, .. }) => {
                assert_eq!(status, 400);
                assert_eq!(code, "invalid_request");
            }
            other => unreachable!("expected upload id rejection, got {other:?}"),
        }
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
            .put_file_bytes(
                &source,
                b"hello over http\n",
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            )
            .expect("initial create");

        match harness.client.put_file_bytes(
            &source,
            b"conflict\n",
            DestinationBehavior::NoReplace,
            &MutationOptions::default(),
        ) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_conflict"),
            other => unreachable!("expected path_conflict, got {other:?}"),
        }

        harness
            .client
            .put_file_bytes(
                &source,
                b"forced overwrite\n",
                DestinationBehavior::Replace,
                &MutationOptions::default(),
            )
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
            .read_file_bytes(&destination)
            .expect("read copied file");
        assert_eq!(dest_bytes, b"forced overwrite\n");
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
            .write_file_bytes(&source_path, b"base\n", &MutationOptions::default())
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
                .read_file_bytes(&clone_path)
                .expect("read clone"),
            b"base\n"
        );

        harness
            .client
            .write_file_bytes(
                &source_path,
                b"source-after-fork\n",
                &MutationOptions::default(),
            )
            .expect("replace source");
        assert_eq!(
            harness
                .client
                .read_file_bytes(&clone_path)
                .expect("read clone after source write"),
            b"base\n"
        );

        let clone_write = harness
            .client
            .write_file_bytes(
                &clone_path,
                b"clone-after-fork\n",
                &MutationOptions::default(),
            )
            .expect("replace clone");
        assert_eq!(clone_write.committed_seq, ChangeSeq(2));
        assert_eq!(
            harness
                .client
                .read_file_bytes(&source_path)
                .expect("read source"),
            b"source-after-fork\n"
        );
        assert_eq!(
            harness
                .client
                .read_file_bytes(&clone_path)
                .expect("read clone"),
            b"clone-after-fork\n"
        );

        match harness
            .client
            .list_changes_page(&namespace_id("clone"), ChangeSeq(0), None)
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
            other => unreachable!("expected rebootstrap_required, got {other:?}"),
        }
        let clone_changes = harness
            .client
            .list_changes_page(&namespace_id("clone"), ChangeSeq(1), None)
            .expect("clone changes");
        assert_eq!(clone_changes.changes.len(), 1);
        assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_commit_and_change_feed_are_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-current-smoke",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        let file_bytes = b"phase-2a over http\n";
        let target = NamespacePath::parse("demo", "/uploaded.txt").expect("target");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");

        let begin = harness
            .client
            .begin_upload(&namespace, &BeginUploadRequest::default())
            .expect("begin upload");
        let first_content = harness
            .client
            .upload_content(&namespace, begin.upload_id.as_str(), file_bytes)
            .expect("upload content");
        let repeated_content = harness
            .client
            .upload_content(&namespace, begin.upload_id.as_str(), file_bytes)
            .expect("repeat upload content");
        assert_eq!(first_content, repeated_content);
        match harness.client.upload_content(
            &namespace,
            begin.upload_id.as_str(),
            b"different bytes",
        ) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_content_conflict"),
            other => unreachable!("expected upload_content_conflict, got {other:?}"),
        }

        let mismatch_upload = harness
            .client
            .begin_upload(&namespace, &BeginUploadRequest::default())
            .expect("begin mismatch upload");
        let staged = harness
            .client
            .upload_content(&namespace, mismatch_upload.upload_id.as_str(), file_bytes)
            .expect("stage mismatch upload content");
        assert_ne!(
            staged.content_ref,
            ContentRef::whole_file_v0(b"other bytes")
        );
        match harness.client.complete_upload(
            &namespace,
            mismatch_upload.upload_id.as_str(),
            &CompleteUploadRequest {
                content_ref: ContentRef::whole_file_v0(b"other bytes"),
            },
        ) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "invalid_request"),
            other => unreachable!("expected upload content rejection, got {other:?}"),
        }

        let completed = stage_uploaded_content(&harness.client, &namespace, file_bytes);
        let content_ref = completed.content_ref.clone();

        let commit_request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("req-phase-2a-create-file").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("uploaded.txt")
                        .expect("valid display name"),
                    content_ref: content_ref.clone(),
                }],
                message: Some("upload over http".to_owned()),
            },
            content_tokens: vec![validated_content_token(&completed)],
        };
        let commit = harness
            .client
            .commit_operations(&namespace, &commit_request)
            .expect("commit uploaded file");
        assert_eq!(
            commit.commit_id,
            CommitId::parse("req-phase-2a-create-file").expect("valid commit id")
        );
        assert_eq!(commit.committed_seq, ChangeSeq(1));

        let repeated_commit = harness
            .client
            .commit_operations(&namespace, &commit_request)
            .expect("repeat commit");
        assert_eq!(repeated_commit, commit);

        let stat = harness
            .client
            .stat_path(&target)
            .expect("stat committed file");
        assert_eq!(stat.inode_id, InodeId(2));
        assert_eq!(stat.content_ref.as_ref(), Some(&content_ref));
        let read_back = harness
            .client
            .read_file_bytes(&target)
            .expect("read committed file");
        assert_eq!(read_back, file_bytes);

        let changes = harness
            .client
            .list_changes_page(&namespace, ChangeSeq(0), None)
            .expect("list changes");
        assert_eq!(changes.namespace_id, namespace);
        assert_eq!(changes.after_seq, ChangeSeq(0));
        assert_eq!(changes.through_seq, commit.committed_seq);
        assert_eq!(changes.changes.len(), 1);
        let change = &changes.changes[0];
        assert_eq!(change.seq, commit.committed_seq);
        assert_eq!(change.commit_id, commit.commit_id);
        assert_eq!(change.commit_id, commit_request.commit.commit_id);
        assert_eq!(change.message.as_deref(), Some("upload over http"));
        assert_eq!(change.deltas.len(), 3);
        assert!(matches!(
            &change.deltas[1],
            CommitDelta::BindDirentry {
                semantic_op_index: 0,
                delta_index: 1,
                name_key,
                display_name,
                ..
            } if name_key.as_str() == "uploaded.txt" && display_name.as_str() == "uploaded.txt"
        ));

        let empty = harness
            .client
            .list_changes_page(&namespace, commit.committed_seq, None)
            .expect("list changes after head");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn path_put_with_bad_content_token_fails_content_not_prepared() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-bad-content-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let completed = stage_uploaded_content(&harness.client, &namespace, b"token rejected");

        let request = FilesystemOperationRequest {
            commit_id: CommitId::parse("bad-token-put").expect("valid commit id"),
            content_tokens: vec![ValidatedContentToken {
                content_ref: completed.content_ref.clone(),
                token: "not.a.valid.token".to_owned(),
            }],
            operation: FilesystemOperation::PutFile {
                path: "/bad-token.txt".to_owned(),
                content_ref: completed.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        };
        assert_content_not_prepared_response(
            send_filesystem_operation(&harness.server_url, &namespace, &request),
            &request,
            "content token was rejected: content token is malformed",
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn path_put_without_content_token_fails_content_not_prepared() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-missing-content-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let completed = stage_uploaded_content(&harness.client, &namespace, b"token missing");
        let request = FilesystemOperationRequest {
            commit_id: CommitId::parse("missing-token-put").expect("valid commit id"),
            content_tokens: Vec::new(),
            operation: FilesystemOperation::PutFile {
                path: "/missing-token.txt".to_owned(),
                content_ref: completed.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        };

        assert_content_not_prepared_response(
            send_filesystem_operation(&harness.server_url, &namespace, &request),
            &request,
            &missing_content_proof_message(&request),
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn path_put_with_valid_content_token_succeeds() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-valid-content-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let bytes = b"valid token";
        let completed = stage_uploaded_content(&harness.client, &namespace, bytes);
        let request = FilesystemOperationRequest {
            commit_id: CommitId::parse("valid-token-put").expect("valid commit id"),
            content_tokens: vec![ValidatedContentToken {
                content_ref: completed.content_ref.clone(),
                token: completed
                    .validated_content_token
                    .expect("completed upload carries token"),
            }],
            operation: FilesystemOperation::PutFile {
                path: "/valid-token.txt".to_owned(),
                content_ref: completed.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        };
        let response = send_filesystem_operation(&harness.server_url, &namespace, &request)
            .expect("valid token put");
        let response: CommitResponse =
            serde_json::from_reader(response.into_reader()).expect("decode operation response");
        assert_eq!(response.committed_seq, ChangeSeq(1));

        let target = NamespacePath::parse("demo", "/valid-token.txt").expect("target");
        assert_eq!(
            harness.client.read_file_bytes(&target).expect("read file"),
            bytes
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn landed_path_put_replays_after_content_token_is_absent_expired_or_garbage() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-content-token-replay",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let completed = stage_uploaded_content(&harness.client, &namespace, b"token replay");
        let content_ref = completed.content_ref.clone();
        let mut request = FilesystemOperationRequest {
            commit_id: CommitId::parse("token-replay-put").expect("valid commit id"),
            content_tokens: vec![ValidatedContentToken {
                content_ref: completed.content_ref.clone(),
                token: completed
                    .validated_content_token
                    .expect("completed upload carries token"),
            }],
            operation: FilesystemOperation::PutFile {
                path: "/token-replay.txt".to_owned(),
                content_ref: completed.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        };
        let send = |request: &FilesystemOperationRequest| {
            let response = send_filesystem_operation(&harness.server_url, &namespace, request)
                .expect("landed put should replay");
            serde_json::from_reader::<_, CommitResponse>(response.into_reader())
                .expect("decode operation response")
        };
        let original = send(&request);

        request.content_tokens.clear();
        assert_eq!(send(&request), original);

        request.content_tokens = vec![ValidatedContentToken {
            content_ref: content_ref.clone(),
            token: mint_content_token(TEST_CONTENT_TOKEN_SECRET, &namespace, &content_ref, 0)
                .expect("mint expired content token"),
        }];
        assert_eq!(send(&request), original);

        request.content_tokens = vec![ValidatedContentToken {
            content_ref,
            token: "not.a.valid.token".to_owned(),
        }];
        assert_eq!(send(&request), original);
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn path_put_with_only_an_irrelevant_token_reports_the_missing_put_proof() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-irrelevant-content-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = stage_uploaded_content(&harness.client, &namespace, b"target content");
        let irrelevant = stage_uploaded_content(&harness.client, &namespace, b"irrelevant content");
        let request = FilesystemOperationRequest {
            commit_id: CommitId::parse("irrelevant-token-put").expect("valid commit id"),
            content_tokens: vec![ValidatedContentToken {
                content_ref: irrelevant.content_ref,
                token: irrelevant
                    .validated_content_token
                    .expect("completed upload carries token"),
            }],
            operation: FilesystemOperation::PutFile {
                path: "/irrelevant-token.txt".to_owned(),
                content_ref: target.content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
        };

        assert_content_not_prepared_response(
            send_filesystem_operation(&harness.server_url, &namespace, &request),
            &request,
            &missing_content_proof_message(&request),
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_with_valid_tokens_for_all_distinct_refs_succeeds() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-tokens",
        "http-commit-valid-tokens",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let first = stage_uploaded_content(&harness.client, &namespace, b"first");
        let second = stage_uploaded_content(&harness.client, &namespace, b"second");
        let third = stage_uploaded_content(&harness.client, &namespace, b"third");
        let request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-all-proofs").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("first.txt")
                            .expect("valid display name"),
                        content_ref: first.content_ref.clone(),
                    },
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("first-copy.txt")
                            .expect("valid display name"),
                        content_ref: first.content_ref.clone(),
                    },
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("second.txt")
                            .expect("valid display name"),
                        content_ref: second.content_ref.clone(),
                    },
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("third.txt")
                            .expect("valid display name"),
                        content_ref: third.content_ref.clone(),
                    },
                ],
                message: None,
            },
            content_tokens: vec![
                validated_content_token(&first),
                validated_content_token(&second),
                validated_content_token(&third),
                ValidatedContentToken {
                    content_ref: ContentRef::whole_file_v0(b"irrelevant"),
                    token: "irrelevant.garbage".to_owned(),
                },
            ],
        };

        let response = harness
            .client
            .commit_operations(&namespace, &request)
            .expect("all distinct refs are prepared");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        assert_eq!(
            harness
                .client
                .stat_path(&NamespacePath::parse("demo", "/first-copy.txt").expect("path"))
                .expect("repeated ref file")
                .content_ref,
            Some(first.content_ref)
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_with_one_missing_proof_reports_first_uncovered_digest() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-missing-token",
        "http-commit-missing-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let covered = stage_uploaded_content(&harness.client, &namespace, b"covered");
        let uncovered = stage_uploaded_content(&harness.client, &namespace, b"uncovered");
        let request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-missing-proof").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("covered.txt")
                            .expect("valid display name"),
                        content_ref: covered.content_ref.clone(),
                    },
                    CommitOp::CreateFile {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse("uncovered.txt")
                            .expect("valid display name"),
                        content_ref: uncovered.content_ref.clone(),
                    },
                ],
                message: None,
            },
            content_tokens: vec![validated_content_token(&covered)],
        };

        assert_commit_content_not_prepared_response(
            send_commit_submission(&harness.server_url, &namespace, &request),
            &request.commit.commit_id,
            &format!(
                "content ref `{}` is not prepared for publication",
                uncovered.content_ref.digest
            ),
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_with_invalid_relevant_token_reports_token_failure() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-invalid-token",
        "http-commit-invalid-token",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let completed = stage_uploaded_content(&harness.client, &namespace, b"invalid token");
        let request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-invalid-token").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("invalid.txt")
                        .expect("valid display name"),
                    content_ref: completed.content_ref.clone(),
                }],
                message: None,
            },
            content_tokens: vec![ValidatedContentToken {
                content_ref: completed.content_ref,
                token: "not.a.valid.token".to_owned(),
            }],
        };

        assert_commit_content_not_prepared_response(
            send_commit_submission(&harness.server_url, &namespace, &request),
            &request.commit.commit_id,
            "content token was rejected: content token is malformed",
        );
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn landed_commit_replays_byte_for_byte_with_over_limit_absent_expired_or_garbage_tokens() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-token-replay",
        "http-commit-token-replay",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let completed = stage_uploaded_content(&harness.client, &namespace, b"commit replay");
        let content_ref = completed.content_ref.clone();
        let valid_token = validated_content_token(&completed);
        let mut request = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-token-replay").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("replay.txt")
                        .expect("valid display name"),
                    content_ref: content_ref.clone(),
                }],
                message: None,
            },
            content_tokens: vec![valid_token.clone()],
        };
        let send = |request: &CommitSubmissionRequest| {
            response_bytes(
                send_commit_submission(&harness.server_url, &namespace, request)
                    .expect("landed commit should replay"),
            )
        };
        let original = send(&request);

        request.content_tokens = vec![valid_token; MAX_COMMIT_CONTENT_TOKENS + 1];
        assert_eq!(send(&request), original);

        request.content_tokens.clear();
        assert_eq!(send(&request), original);

        request.content_tokens = vec![ValidatedContentToken {
            content_ref: content_ref.clone(),
            token: mint_content_token(TEST_CONTENT_TOKEN_SECRET, &namespace, &content_ref, 0)
                .expect("mint expired token"),
        }];
        assert_eq!(send(&request), original);

        request.content_tokens = vec![ValidatedContentToken {
            content_ref,
            token: "not.a.valid.token".to_owned(),
        }];
        assert_eq!(send(&request), original);
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_commit_body_without_content_tokens_still_parses_and_commits_mkdir() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-bare-commit",
        "http-bare-commit",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let body = json!({
            "commit_id": "bare-commit-mkdir",
            "preconditions": [],
            "ops": [{
                "kind": "create_directory",
                "parent_inode_id": 1,
                "display_name": "docs"
            }],
            "message": null
        });

        let response =
            send_commit_json(&harness.server_url, &namespace, &body).expect("bare commit body");
        let response: CommitResponse =
            serde_json::from_reader(response.into_reader()).expect("decode response");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        harness
            .client
            .stat_path(&NamespacePath::parse("demo", "/docs").expect("path"))
            .expect("mkdir committed");
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_operation_and_prepared_proof_limits_reject_new_requests() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-limits",
        "http-commit-limits",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let oversized_ops = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-too-many-ops").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: (0..4097)
                    .map(|index| CommitOp::CreateDirectory {
                        parent_inode_id: InodeId(1),
                        display_name: loonfs_api::DisplayName::parse(format!("dir-{index}"))
                            .expect("valid display name"),
                    })
                    .collect(),
                message: None,
            },
            content_tokens: Vec::new(),
        };
        assert_invalid_commit_limit_response(
            send_commit_submission(&harness.server_url, &namespace, &oversized_ops),
            "4097 operations",
        );

        let completed = stage_uploaded_content(&harness.client, &namespace, b"proof limit");
        let prepared_proof = validated_content_token(&completed);
        let oversized_proofs = CommitSubmissionRequest {
            commit: ApiCommitRequest {
                commit_id: CommitId::parse("commit-too-many-proofs").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("never-committed.txt")
                        .expect("valid display name"),
                    content_ref: completed.content_ref,
                }],
                message: None,
            },
            content_tokens: vec![prepared_proof; MAX_COMMIT_CONTENT_TOKENS + 1],
        };
        assert_invalid_commit_limit_response(
            send_commit_submission(&harness.server_url, &namespace, &oversized_proofs),
            "4097 prepared content proofs",
        );

        let changes = harness
            .client
            .list_changes_page(&namespace, ChangeSeq(0), None)
            .expect("list changes");
        assert!(changes.changes.is_empty());
    })
    .await
    .expect("blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_restore_revision_appends_new_head_and_reports_change() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-restore",
        "http-restore",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        let target = NamespacePath::parse("demo", "/restore.txt").expect("target");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");

        let first = stage_uploaded_content(&harness.client, &namespace, b"first bytes\n");
        let first_content_ref = first.content_ref.clone();
        harness
            .client
            .commit_operations(
                &namespace,
                &CommitSubmissionRequest {
                    commit: ApiCommitRequest {
                        commit_id: CommitId::parse("req-restore-create").expect("valid commit id"),
                        preconditions: Vec::new(),
                        ops: vec![CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: loonfs_api::DisplayName::parse("restore.txt")
                                .expect("valid display name"),
                            content_ref: first_content_ref.clone(),
                        }],
                        message: None,
                    },
                    content_tokens: vec![validated_content_token(&first)],
                },
            )
            .expect("create file");
        let inode_id = harness
            .client
            .stat_path(&target)
            .expect("stat created file")
            .inode_id;

        let second = stage_uploaded_content(&harness.client, &namespace, b"second bytes\n");
        let second_content_ref = second.content_ref.clone();
        let replace = harness
            .client
            .commit_operations(
                &namespace,
                &CommitSubmissionRequest {
                    commit: ApiCommitRequest {
                        commit_id: CommitId::parse("req-restore-replace").expect("valid commit id"),
                        preconditions: Vec::new(),
                        ops: vec![CommitOp::ReplaceFile {
                            inode_id,
                            base_revision_no: RevisionNo(1),
                            content_ref: second_content_ref.clone(),
                        }],
                        message: None,
                    },
                    content_tokens: vec![validated_content_token(&second)],
                },
            )
            .expect("replace file");
        assert_eq!(replace.committed_seq, ChangeSeq(2));

        let restore = harness
            .client
            .commit_operations(
                &namespace,
                &CommitSubmissionRequest {
                    commit: ApiCommitRequest {
                        commit_id: CommitId::parse("req-restore-restore").expect("valid commit id"),
                        preconditions: Vec::new(),
                        ops: vec![CommitOp::RestoreRevision {
                            inode_id,
                            source_revision_no: RevisionNo(1),
                            base_revision_no: RevisionNo(2),
                        }],
                        message: Some("restore revision".to_owned()),
                    },
                    content_tokens: Vec::new(),
                },
            )
            .expect("restore revision");
        assert_eq!(restore.committed_seq, ChangeSeq(3));

        let entry = harness
            .client
            .stat_path(&target)
            .expect("stat restored file");
        assert_eq!(entry.inode_id, inode_id);
        assert_eq!(entry.content_ref.as_ref(), Some(&first_content_ref));
        let bytes = harness
            .client
            .read_file_bytes(&target)
            .expect("read restored file");
        assert_eq!(bytes, b"first bytes\n");

        let changes = harness
            .client
            .list_changes_page(&namespace, ChangeSeq(0), None)
            .expect("list changes");
        assert_eq!(changes.changes.len(), 3);
        assert_eq!(
            changes.changes[2].commit_id,
            CommitId::parse("req-restore-restore").expect("valid commit id")
        );
        assert!(matches!(
            &changes.changes[2].deltas[0],
            CommitDelta::AppendFileRevision {
                semantic_op_index: 0,
                inode_id: delta_inode,
                revision_no,
                content_ref,
                ..
            } if *delta_inode == inode_id
                && *revision_no == RevisionNo(3)
                && content_ref == &first_content_ref
        ));

        let first_page = harness
            .client
            .list_changes_page(&namespace, ChangeSeq(0), Some(2))
            .expect("list first changes page");
        assert_eq!(first_page.after_seq, ChangeSeq(0));
        assert_eq!(first_page.through_seq, ChangeSeq(2));
        assert_eq!(first_page.next_after_seq, Some(ChangeSeq(2)));
        assert_eq!(first_page.changes.len(), 2);

        let second_page = harness
            .client
            .list_changes_page(
                &namespace,
                first_page.next_after_seq.expect("next page"),
                Some(2),
            )
            .expect("list second changes page");
        assert_eq!(second_page.after_seq, ChangeSeq(2));
        assert_eq!(second_page.through_seq, ChangeSeq(3));
        assert_eq!(second_page.next_after_seq, None);
        assert_eq!(
            second_page
                .changes
                .iter()
                .map(|change| change.seq)
                .collect::<Vec<_>>(),
            vec![ChangeSeq(3)]
        );
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_revision_routes_list_read_and_restore_by_path_and_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-revisions",
        "http-revisions",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("demo", "/docs/rev.txt").expect("target");
        harness
            .client
            .put_file_bytes(
                &target,
                b"one",
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            )
            .expect("create file");
        harness
            .client
            .put_file_bytes(
                &target,
                b"two",
                DestinationBehavior::Replace,
                &MutationOptions::default(),
            )
            .expect("replace file");

        let entry = harness.client.stat_path(&target).expect("stat file");
        let revisions = harness
            .client
            .list_file_revisions_page(&target, None, None)
            .expect("path revisions");
        assert_eq!(revisions.inode_id, entry.inode_id);
        assert_eq!(revisions.revisions.len(), 2);
        assert_eq!(
            harness
                .client
                .read_file_revision_bytes(&target, RevisionNo(1))
                .expect("read path revision"),
            b"one"
        );

        let moved = NamespacePath::parse("demo", "/docs/moved.txt").expect("moved");
        harness
            .client
            .move_path(
                &target,
                &moved,
                DestinationBehavior::NoReplace,
                &MutationOptions::default(),
            )
            .expect("move path");
        assert!(matches!(
            harness.client.list_file_revisions_page(&target, None, None),
            Err(ClientError::Api { code, .. }) if code == "path_not_found"
        ));
        let inode_revisions = harness
            .client
            .list_file_revisions_for_inode_page(&namespace, entry.inode_id, None, None)
            .expect("inode revisions");
        assert_eq!(inode_revisions.revisions.len(), 2);
        assert_eq!(
            harness
                .client
                .read_file_revision_bytes_for_inode(&namespace, entry.inode_id, RevisionNo(2))
                .expect("read inode revision"),
            b"two"
        );

        harness
            .client
            .restore_file_revision(&moved, RevisionNo(1), &MutationOptions::default())
            .expect("path restore");
        assert_eq!(
            harness
                .client
                .read_file_bytes(&moved)
                .expect("read restored file"),
            b"one"
        );
        harness
            .client
            .restore_file_revision_for_inode(
                &namespace,
                entry.inode_id,
                RevisionNo(2),
                RevisionNo(3),
                &CommitId::parse("c_restore_inode_revision_0001").expect("valid commit id"),
            )
            .expect("inode restore");
        assert_eq!(
            harness
                .client
                .read_file_bytes(&moved)
                .expect("read inode-restored file"),
            b"two"
        );
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_restore_revision_missing_source_returns_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-restore-missing-source",
        "http-restore-missing-source",
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        harness
            .client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("demo", "/restore.txt").expect("target");

        let first = stage_uploaded_content(&harness.client, &namespace, b"first bytes\n");
        harness
            .client
            .commit_operations(
                &namespace,
                &CommitSubmissionRequest {
                    commit: ApiCommitRequest {
                        commit_id: CommitId::parse("req-restore-missing-source-create")
                            .expect("valid commit id"),
                        preconditions: Vec::new(),
                        ops: vec![CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: loonfs_api::DisplayName::parse("restore.txt")
                                .expect("valid display name"),
                            content_ref: first.content_ref.clone(),
                        }],
                        message: None,
                    },
                    content_tokens: vec![validated_content_token(&first)],
                },
            )
            .expect("create file");
        let inode_id = harness
            .client
            .stat_path(&target)
            .expect("stat created file")
            .inode_id;

        match harness.client.commit_operations(
            &namespace,
            &CommitSubmissionRequest {
                commit: ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-missing-source-restore")
                        .expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::RestoreRevision {
                        inode_id,
                        source_revision_no: RevisionNo(99),
                        base_revision_no: RevisionNo(1),
                    }],
                    message: None,
                },
                content_tokens: Vec::new(),
            },
        ) {
            Err(ClientError::Api { status, code, .. }) => {
                assert_eq!(status, 404);
                assert_eq!(code, "revision_not_found");
            }
            other => unreachable!("expected revision_not_found, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

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
                DestinationBehavior::NoReplace,
                &MutationOptions::with_commit_id(commit_id.clone()),
            )
            .expect("first put");
        assert!(first.committed_seq.0 >= 1);

        let repeated = harness
            .client
            .put_file_bytes(
                &target,
                b"stable bytes\n",
                DestinationBehavior::NoReplace,
                &MutationOptions::with_commit_id(commit_id.clone()),
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
            DestinationBehavior::NoReplace,
            &MutationOptions::with_commit_id(commit_id),
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
            .write_file_bytes(&source, b"source bytes\n", &MutationOptions::default())
            .expect("seed source");

        let copied = NamespacePath::parse("demo", "/docs/copied.txt").expect("copied");
        let copy_first = harness
            .client
            .copy_path(
                &source,
                &copied,
                DestinationBehavior::NoReplace,
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-copy").expect("valid commit id"),
                ),
            )
            .expect("copy first");
        let copy_repeated = harness
            .client
            .copy_path(
                &source,
                &copied,
                DestinationBehavior::NoReplace,
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-copy").expect("valid commit id"),
                ),
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
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-move").expect("valid commit id"),
                ),
            )
            .expect("move first");
        let move_repeated = harness
            .client
            .move_path(
                &copied,
                &moved,
                DestinationBehavior::NoReplace,
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-move").expect("valid commit id"),
                ),
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
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-delete").expect("valid commit id"),
                ),
            )
            .expect("delete first");
        let delete_repeated = harness
            .client
            .delete_path(
                &moved,
                &MutationOptions::with_commit_id(
                    CommitId::parse("req-v1-delete").expect("valid commit id"),
                ),
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
            .write_file_bytes(&child, b"child", &MutationOptions::default())
            .expect("write child");

        let dir = NamespacePath::parse("demo", "/docs").expect("dir path");
        let non_recursive = harness
            .client
            .delete_path(&dir, &MutationOptions::default())
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
            .delete_path_recursive(&dir, &MutationOptions::default())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_malformed_bodies_fail_inside_the_error_envelope() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-move-behavior",
        "http-move-behavior",
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
            .write_file_bytes(&source, b"source", &MutationOptions::default())
            .expect("seed source");

        // Unknown behaviors are not wire variants; they fail request
        // validation inside the error envelope as invalid_request.
        for (commit_id, behavior) in [("move-replace", "replace"), ("move-exchange", "exchange")] {
            let request = serde_json::json!({
                "commit_id": commit_id,
                "operation": {
                    "op": "move_path",
                    "from_path": "/docs/source.txt",
                    "to_path": "/docs/target.txt",
                    "behavior": behavior,
                },
            });
            match raw_agent()
                .post(&format!(
                    "{}/v0/namespaces/demo/filesystem/operations",
                    harness.server_url
                ))
                .set("authorization", "Bearer test-token")
                .send_json(request)
            {
                Err(ureq::Error::Status(status, response)) => {
                    assert_eq!(status, 400);
                    let error: ApiError =
                        serde_json::from_reader(response.into_reader()).expect("decode api error");
                    assert_eq!(error.code, "invalid_request");
                }
                other => unreachable!("expected rejected move behavior, got {other:?}"),
            }
        }

        // Malformed upload bodies must also stay inside the envelope — an
        // Option-typed body must reject garbage, not default it to a session.
        match raw_agent()
            .post(&format!(
                "{}/v0/namespaces/demo/uploads",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/json")
            .send_string("{\"mode\": \"direkt_put\"}")
        {
            Err(ureq::Error::Status(status, response)) => {
                assert_eq!(status, 400);
                let error: ApiError =
                    serde_json::from_reader(response.into_reader()).expect("decode api error");
                assert_eq!(error.code, "invalid_request");
            }
            other => unreachable!("expected rejected upload body, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_checkpoint_and_retention_are_idempotent_and_soft() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-admin",
        "http-admin",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
        client
            .create_namespace(&namespace)
            .expect("create namespace");
        client
            .write_file_bytes(&target, b"hello admin\n", &MutationOptions::default())
            .expect("write file");

        let first = post_checkpoint(&server_url, namespace.as_str()).expect("first checkpoint");
        assert!(CheckpointId::parse(first.checkpoint_id.as_str()).is_ok());
        assert_eq!(first.checkpoint_seq, ChangeSeq(1));
        assert_eq!(first.manifest_id, ManifestId(1));
        assert_eq!(first.current_manifest_id, Some(first.manifest_id));

        let repeated = post_checkpoint(&server_url, namespace.as_str()).expect("repeat checkpoint");
        assert_eq!(repeated, first);

        // Release is idempotent the same way: the first call flips the
        // record, the repeat observes the settled end state.
        let released = post_checkpoint_release(
            &server_url,
            namespace.as_str(),
            first.checkpoint_id.as_str(),
        )
        .expect("release checkpoint");
        assert!(released.was_active);
        let released_again = post_checkpoint_release(
            &server_url,
            namespace.as_str(),
            first.checkpoint_id.as_str(),
        )
        .expect("repeat release");
        assert!(!released_again.was_active);
        let bogus_release =
            post_checkpoint_release(&server_url, namespace.as_str(), "not-a-checkpoint-id")
                .expect_err("malformed checkpoint id");
        assert_eq!(bogus_release.code, "invalid_request");

        // The GC grace window's derived safety floor is enforced at the API:
        // a sub-minimum override is rejected, not honored.
        let unsafe_gc: Result<loonfs_api::GcResponse, ApiError> = post_admin_json_body(
            &format!("{server_url}/v0/admin/namespaces/{namespace}/gc"),
            "test-token",
            serde_json::json!({ "grace_window_ms": 1 }),
        );
        let unsafe_gc = unsafe_gc.expect_err("sub-minimum grace window is rejected");
        assert_eq!(unsafe_gc.code, "invalid_request");
        assert!(unsafe_gc.message.contains("derived safety minimum"));

        let advanced =
            post_retention_advance(&server_url, namespace.as_str()).expect("advance retention");
        assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));

        let repeated_advance =
            post_retention_advance(&server_url, namespace.as_str()).expect("repeat retention");
        assert_eq!(repeated_advance, advanced);

        let bytes = client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello admin\n");

        match client.list_changes_page(&namespace, ChangeSeq(0), None) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
            other => unreachable!("expected rebootstrap_required, got {other:?}"),
        }

        let empty = client
            .list_changes_page(&namespace, ChangeSeq(1), None)
            .expect("changes after floor");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_gc_is_explicit_and_retains_young_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-gc",
        "http-admin-gc",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
        client
            .write_file_bytes(&target, b"hello gc\n", &MutationOptions::default())
            .expect("write file");
        post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");

        // A freshly written namespace sits entirely inside the grace
        // window: the pass runs, deletes nothing, and reads keep working.
        let report = post_gc(&server_url, namespace.as_str()).expect("gc pass");
        assert_eq!(report.deleted_wal_segments, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
        assert_eq!(report.deleted_manifests, 0);
        assert_eq!(report.deleted_checkpoint_records, 0);
        assert!(!report.degraded_retention);

        let bytes = client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello gc\n");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_maintenance_tick_reports_outcomes_not_errors() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-tick",
        "http-admin-tick",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        client
            .create_namespace(&namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
        client
            .write_file_bytes(&target, b"hello tick\n", &MutationOptions::default())
            .expect("write file");

        // One WAL segment sits far below the default threshold.
        let idle = post_maintenance_tick(&server_url, namespace.as_str()).expect("idle tick");
        assert_eq!(idle.namespace_id, namespace);
        assert_eq!(idle.status_before.wal_tail_segments, 1);
        assert_eq!(idle.outcome, loonfs_api::MaintenanceTickOutcome::NotNeeded);
        assert!(idle.gc.is_none());

        // Forcing the threshold to one segment flushes the WAL tail
        // and runs the opted-in GC pass.
        let forced: loonfs_api::MaintenanceTickResponse = client
            .maintenance_tick(
                &namespace,
                &loonfs_api::MaintenanceTickRequest {
                    max_wal_tail_segments: Some(1),
                    gc: Some(loonfs_api::GcRequest::default()),
                },
            )
            .expect("forced tick");
        assert_eq!(
            forced.outcome,
            loonfs_api::MaintenanceTickOutcome::WalFlushed {
                manifest_head_seq: ChangeSeq(1),
            }
        );
        let gc = forced.gc.expect("gc report present when opted in");
        assert_eq!(gc.deleted_wal_segments, 0);
        assert!(!gc.degraded_retention);

        let bytes = client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello tick\n");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_retention_advance_uses_initial_manifest_after_create() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-admin-missing-checkpoint",
        "http-admin-missing-checkpoint",
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        client
            .create_namespace(&namespace)
            .expect("create namespace");

        let advanced =
            post_retention_advance(&server_url, namespace.as_str()).expect("advance retention");
        assert_eq!(advanced.retention_floor_seq, ChangeSeq(0));
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_checkpoint_manifest_consumption_is_strict_when_manifest_is_corrupted() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let harness = start_server(test_config(
        store_root.clone(),
        "loonfs-server-admin-corrupt",
        "http-admin-corrupt",
    ))
    .await;
    // A second server on the same store reads cold: it must consume the
    // manifest and notice the corruption. The first server's reads are
    // pinned to the head-plus-manifest pair its own publish seeded, which
    // stays valid without touching the corrupted object.
    let cold = start_server(test_config(
        store_root,
        "loonfs-server-cold-reader",
        "http-admin-corrupt",
    ))
    .await;
    let client = harness.client.clone();
    let cold_client = cold.client.clone();
    let server_url = harness.server_url.clone();
    let store_root = harness.store_root.clone();
    let store_key_prefix = harness.store_key_prefix.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = namespace_id("demo");
        let target = NamespacePath::parse("demo", "/docs/hello.txt").expect("target");
        client
            .create_namespace(&namespace)
            .expect("create namespace");
        client
            .write_file_bytes(&target, b"hello\n", &MutationOptions::default())
            .expect("write file");
        post_checkpoint(&server_url, namespace.as_str()).expect("checkpoint");

        let store = ConfiguredObjectStore::local_fs(&store_root, store_key_prefix.as_deref())
            .expect("construct store");
        let root = block_on(loonfs_core::control::load_namespace_metadata_root_control(
            &store, &namespace,
        ))
        .expect("metadata root");
        block_on(store.put_overwrite(
            &metadata_manifest_object(namespace.as_str(), &root.state.manifest_object_id),
            Bytes::from_static(br#"{"bad":"json"}"#),
        ))
        .expect("corrupt manifest");

        match cold_client.stat_path(&target) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
            other => unreachable!("expected namespace_corrupt, got {other:?}"),
        }
        // The warm server keeps serving its pinned pair; the corruption is
        // surfaced by whoever actually consumes the manifest.
        client
            .stat_path(&target)
            .expect("warm server reads from its pinned head-plus-manifest pair");
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
    cold.server.abort();
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
            .write_file_bytes(&host_a_target, b"host a\n", &MutationOptions::default())
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
            match client_a.write_file_bytes(
                &host_c_target,
                b"host a again\n",
                &MutationOptions::default(),
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

struct TestServer {
    client: Client,
    server_url: String,
    store_root: PathBuf,
    store_key_prefix: Option<String>,
    server: tokio::task::JoinHandle<()>,
}

async fn start_server(config: ServerConfig) -> TestServer {
    let (store_root, store_key_prefix) = match &config.store {
        StoreConfig::LocalFs { root, key_prefix } => (PathBuf::from(root), key_prefix.clone()),
        other => unreachable!("test harness requires local fs store, got {other:?}"),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    // The lifecycle handle is dropped: these tests abort the server task
    // instead of shutting it down gracefully.
    let (router, _lifecycle) = app(config).await.expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    TestServer {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".to_owned()),
            request_timeout_ms: None,
            disable_transient_retry: false,
        })
        .expect("valid client config"),
        server_url: format!("http://{}", addr),
        store_root,
        store_key_prefix,
        server,
    }
}

fn test_config(store_root: std::path::PathBuf, writer_id: &str, key_prefix: &str) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: TEST_CONTENT_TOKEN_SECRET.into(),
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        grep: loonfs_server::GrepConfig::default(),
        background_maintenance: true,
        min_publish_interval_ms: 0,
        max_upload_bytes: 256 * 1024 * 1024,
        max_download_bytes: 256 * 1024 * 1024,
        max_commit_body_bytes: 8 * 1024 * 1024,
        max_concurrent_uploads: 8,
        max_concurrent_downloads: 16,
        max_concurrent_maintenance: loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
        allow_unauthenticated_remote: false,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some(key_prefix.to_owned()),
        },
    }
}

fn entry_names(response: &ListPathEntriesResponse) -> Vec<&str> {
    response
        .entries
        .iter()
        .map(|entry| entry.display_name.as_str())
        .collect()
}

fn get_json<T: serde::de::DeserializeOwned>(url: &str, auth_token: &str) -> Result<T, ApiError> {
    let request = raw_agent()
        .get(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    match request.call() {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        }),
        Err(ureq::Error::Status(_, response)) => Err(serde_json::from_reader::<_, ApiError>(
            response.into_reader(),
        )
        .unwrap_or_else(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        })),
        Err(ureq::Error::Transport(error)) => Err(ApiError {
            code: "transport".to_owned(),
            feature: None,
            message: error.to_string(),
            request_id: None,
            details: None,
        }),
    }
}

fn post_checkpoint(
    server_url: &str,
    namespace: &str,
) -> Result<CreateCheckpointResponse, ApiError> {
    post_admin_json_body(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/checkpoints"),
        "test-token",
        serde_json::json!({ "name": "nightly" }),
    )
}

fn post_checkpoint_release(
    server_url: &str,
    namespace: &str,
    checkpoint_id: &str,
) -> Result<loonfs_api::ReleaseCheckpointResponse, ApiError> {
    post_admin_json(
        &format!(
            "{server_url}/v0/admin/namespaces/{namespace}/checkpoints/{checkpoint_id}/release"
        ),
        "test-token",
    )
}

fn post_gc(server_url: &str, namespace: &str) -> Result<loonfs_api::GcResponse, ApiError> {
    post_admin_json(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/gc"),
        "test-token",
    )
}

fn post_maintenance_tick(
    server_url: &str,
    namespace: &str,
) -> Result<loonfs_api::MaintenanceTickResponse, ApiError> {
    post_admin_json(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/maintenance/tick"),
        "test-token",
    )
}

fn post_retention_advance(
    server_url: &str,
    namespace: &str,
) -> Result<AdvanceRetentionResponse, ApiError> {
    post_admin_json(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/retention/advance"),
        "test-token",
    )
}

fn post_admin_json<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
) -> Result<T, ApiError> {
    let request = raw_agent()
        .post(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    decode_admin_response(request.call())
}

fn post_admin_json_body<T: serde::de::DeserializeOwned>(
    url: &str,
    auth_token: &str,
    body: serde_json::Value,
) -> Result<T, ApiError> {
    let request = raw_agent()
        .post(url)
        .set("authorization", &format!("Bearer {auth_token}"));
    decode_admin_response(request.send_json(body))
}

fn decode_admin_response<T: serde::de::DeserializeOwned>(
    result: Result<ureq::Response, ureq::Error>,
) -> Result<T, ApiError> {
    match result {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        }),
        Err(ureq::Error::Status(_, response)) => Err(serde_json::from_reader::<_, ApiError>(
            response.into_reader(),
        )
        .unwrap_or_else(|err| ApiError {
            code: "invalid_json".to_owned(),
            feature: None,
            message: err.to_string(),
            request_id: None,
            details: None,
        })),
        Err(ureq::Error::Transport(error)) => Err(ApiError {
            code: "transport".to_owned(),
            feature: None,
            message: error.to_string(),
            request_id: None,
            details: None,
        }),
    }
}

fn assert_invalid_namespace_response(result: Result<ureq::Response, ureq::Error>) {
    match result {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_request");
            assert!(error.message.contains("invalid namespace_id"));
        }
        other => unreachable!("expected invalid_namespace_id response, got {other:?}"),
    }
}

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}

fn send_filesystem_operation(
    server_url: &str,
    namespace_id: &NamespaceId,
    request: &FilesystemOperationRequest,
) -> Result<ureq::Response, Box<ureq::Error>> {
    raw_agent()
        .post(&format!(
            "{server_url}/v0/namespaces/{namespace_id}/filesystem/operations"
        ))
        .set("authorization", "Bearer test-token")
        .send_json(request)
        .map_err(Box::new)
}

fn send_commit_submission(
    server_url: &str,
    namespace_id: &NamespaceId,
    request: &CommitSubmissionRequest,
) -> Result<ureq::Response, Box<ureq::Error>> {
    send_commit_json(server_url, namespace_id, request)
}

fn send_commit_json(
    server_url: &str,
    namespace_id: &NamespaceId,
    request: &impl serde::Serialize,
) -> Result<ureq::Response, Box<ureq::Error>> {
    raw_agent()
        .post(&format!(
            "{server_url}/v0/namespaces/{namespace_id}/commits"
        ))
        .set("authorization", "Bearer test-token")
        .send_json(request)
        .map_err(Box::new)
}

fn response_bytes(response: ureq::Response) -> Vec<u8> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .expect("read response bytes");
    bytes
}

fn assert_commit_content_not_prepared_response(
    result: Result<ureq::Response, Box<ureq::Error>>,
    commit_id: &CommitId,
    expected_message: &str,
) {
    match result {
        Err(error) if matches!(error.as_ref(), ureq::Error::Status(_, _)) => {
            let ureq::Error::Status(status, response) = *error else {
                unreachable!("guard requires an HTTP status error");
            };
            assert_eq!(status, 409);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, ErrorCode::ContentNotPrepared.as_str());
            assert_eq!(error.message, expected_message);
            assert_eq!(
                error.details.and_then(|details| details.commit_id),
                Some(commit_id.clone())
            );
        }
        other => unreachable!("expected content_not_prepared response, got {other:?}"),
    }
}

fn assert_invalid_commit_limit_response(
    result: Result<ureq::Response, Box<ureq::Error>>,
    expected_message_fragment: &str,
) {
    match result {
        Err(error) if matches!(error.as_ref(), ureq::Error::Status(_, _)) => {
            let ureq::Error::Status(status, response) = *error else {
                unreachable!("guard requires an HTTP status error");
            };
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
            assert!(
                error.message.contains(expected_message_fragment),
                "expected `{expected_message_fragment}` in `{}`",
                error.message
            );
        }
        other => unreachable!("expected invalid_request response, got {other:?}"),
    }
}

fn assert_content_not_prepared_response(
    result: Result<ureq::Response, Box<ureq::Error>>,
    request: &FilesystemOperationRequest,
    expected_message: &str,
) {
    match result {
        Err(error) if matches!(error.as_ref(), ureq::Error::Status(_, _)) => {
            let ureq::Error::Status(status, response) = *error else {
                unreachable!("guard requires an HTTP status error");
            };
            assert_eq!(status, 409);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, ErrorCode::ContentNotPrepared.as_str());
            assert_eq!(error.message, expected_message);
            assert_eq!(
                error.details.and_then(|details| details.commit_id),
                Some(request.commit_id.clone())
            );
        }
        other => unreachable!("expected content_not_prepared response, got {other:?}"),
    }
}

fn missing_content_proof_message(request: &FilesystemOperationRequest) -> String {
    let FilesystemOperation::PutFile { content_ref, .. } = &request.operation else {
        unreachable!("content preparation assertion requires a put request");
    };
    format!(
        "content ref `{}` is not prepared for publication",
        content_ref.digest
    )
}

fn stage_uploaded_content(
    client: &Client,
    namespace_id: &NamespaceId,
    file_bytes: &[u8],
) -> CompleteUploadResponse {
    let begin = client
        .begin_upload(namespace_id, &BeginUploadRequest::default())
        .expect("begin upload");
    let staged = client
        .upload_content(namespace_id, begin.upload_id.as_str(), file_bytes)
        .expect("upload content");
    let complete_request = CompleteUploadRequest {
        content_ref: staged.content_ref,
    };
    let complete = client
        .complete_upload(namespace_id, begin.upload_id.as_str(), &complete_request)
        .expect("complete upload");
    let repeated = client
        .complete_upload(namespace_id, begin.upload_id.as_str(), &complete_request)
        .expect("repeat complete upload");
    assert_eq!(repeated.namespace_id, complete.namespace_id);
    assert_eq!(repeated.upload_id, complete.upload_id);
    assert_eq!(repeated.content_ref, complete.content_ref);
    assert!(complete.validated_content_token.is_some());
    assert!(repeated.validated_content_token.is_some());
    complete
}

fn validated_content_token(completed: &CompleteUploadResponse) -> ValidatedContentToken {
    ValidatedContentToken {
        content_ref: completed.content_ref.clone(),
        token: completed
            .validated_content_token
            .clone()
            .expect("completed upload carries token"),
    }
}
