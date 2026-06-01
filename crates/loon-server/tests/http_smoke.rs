use loon_api::{
    v0::{
        CommitAnnotations, CommitDelta, CommitOp, CommitOpResult,
        CommitRequest as ApiCommitRequest, CompleteUploadRequest,
    },
    wire::control::{ControlObjectKind, LeaseStateEnvelope},
    AdvanceRetentionResponse, ApiError, ChangeSeq, CommitId, ContentRef, CreateCheckpointResponse,
    InodeId, InodeKind, RevisionNo,
};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_objectstore::keys::{checkpoint_manifest, namespace_lease};
use loon_objectstore::{ConfiguredObjectStore, ObjectStore};
use loon_server::{app, ServerConfig, StoreConfig};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_round_trip_supports_namespace_create_and_file_read_write() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loon-server-test",
        "http-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let directory = NamespacePath::parse("demo:/notes").expect("parse directory path");
        harness
            .client
            .create_dir(&directory)
            .expect("create directory");
        let directory_entry = harness
            .client
            .stat_path(&directory)
            .expect("stat directory");
        assert_eq!(directory_entry.inode_kind, InodeKind::Dir);

        let target = NamespacePath::parse("demo:/notes/hello.txt").expect("parse namespace path");
        harness
            .client
            .write_file_bytes(&target, b"hello over http\n")
            .expect("write bytes");

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.size_bytes, Some(16));

        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello over http\n");
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
        "loon-server-test",
        "http-invalid-ns",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        assert_invalid_namespace_response(
            ureq::post(&format!("{}/v0/namespaces", harness.server_url))
                .set("authorization", "Bearer test-token")
                .send_json(json!({ "namespace_id": "bad/name" })),
        );

        assert_invalid_namespace_response(
            ureq::get(&format!(
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
        "loon-server-test",
        "http-invalid-upload-id",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");

        let invalid_upload_id = ["upl", "123"].join("-");
        match harness
            .client
            .upload_content("demo", &invalid_upload_id, b"hello")
        {
            Err(ClientError::Api { status, code, .. }) => {
                assert_eq!(status, 400);
                assert_eq!(code, "invalid_upload_id");
            }
            other => panic!("expected invalid_upload_id, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_put_create_only_and_copy_preserve_cli_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loon-server-test",
        "http-copy-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let source = NamespacePath::parse("demo:/docs/hello.txt").expect("source");
        harness
            .client
            .put_file_bytes(&source, b"hello over http\n", false)
            .expect("initial create");

        match harness.client.put_file_bytes(&source, b"conflict\n", false) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_conflict"),
            other => panic!("expected path_conflict, got {other:?}"),
        }

        harness
            .client
            .put_file_bytes(&source, b"forced overwrite\n", true)
            .expect("forced overwrite");

        let destination = NamespacePath::parse("demo:/docs/copy.txt").expect("destination");
        harness
            .client
            .copy_path(&source, &destination)
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
        "loon-server-fork",
        "http-fork-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let source_path = NamespacePath::parse("demo:/docs/shared.txt").expect("source path");
        let clone_path = NamespacePath::parse("clone:/docs/shared.txt").expect("clone path");
        harness
            .client
            .write_file_bytes(&source_path, b"base\n")
            .expect("write source");

        let forked = harness
            .client
            .fork_namespace("demo", "clone")
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
            .write_file_bytes(&source_path, b"source-after-fork\n")
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
            .write_file_bytes(&clone_path, b"clone-after-fork\n")
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

        match harness.client.list_changes("clone", ChangeSeq(0)) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
            other => panic!("expected rebootstrap_required, got {other:?}"),
        }
        let clone_changes = harness
            .client
            .list_changes("clone", ChangeSeq(1))
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
        "loon-server-current",
        "http-current-smoke",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        let file_bytes = b"phase-2a over http\n";
        let target = NamespacePath::parse("demo:/uploaded.txt").expect("target");
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let begin = harness
            .client
            .begin_upload(namespace)
            .expect("begin upload");
        let first_content = harness
            .client
            .upload_content(namespace, &begin.upload_id, file_bytes)
            .expect("upload content");
        let repeated_content = harness
            .client
            .upload_content(namespace, &begin.upload_id, file_bytes)
            .expect("repeat upload content");
        assert_eq!(first_content, repeated_content);
        match harness
            .client
            .upload_content(namespace, &begin.upload_id, b"different bytes")
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_content_conflict"),
            other => panic!("expected upload_content_conflict, got {other:?}"),
        }

        let mismatch_upload = harness
            .client
            .begin_upload(namespace)
            .expect("begin mismatch upload");
        let staged = harness
            .client
            .upload_content(namespace, &mismatch_upload.upload_id, file_bytes)
            .expect("stage mismatch upload content");
        assert_ne!(
            staged.content_ref,
            ContentRef::whole_file_v0(b"other bytes")
        );
        match harness.client.complete_upload(
            namespace,
            &mismatch_upload.upload_id,
            &CompleteUploadRequest {
                content_ref: ContentRef::whole_file_v0(b"other bytes"),
            },
        ) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "invalid_upload_content"),
            other => panic!("expected invalid_upload_content, got {other:?}"),
        }

        let content_ref = stage_uploaded_content_ref(&harness.client, namespace, file_bytes);

        let mut annotations = CommitAnnotations::new();
        annotations.insert("source".to_owned(), json!("http-smoke"));
        annotations.insert("kind".to_owned(), json!("service-proxied"));
        let commit_request = ApiCommitRequest {
            commit_id: CommitId::parse("req-phase-2a-create-file").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "uploaded.txt".to_owned(),
                content_ref: content_ref.clone(),
            }],
            message: Some("upload over http".to_owned()),
            annotations: Some(annotations.clone()),
        };
        let commit = harness
            .client
            .commit_operations(namespace, &commit_request)
            .expect("commit uploaded file");
        assert_eq!(
            commit.commit_id,
            CommitId::parse("req-phase-2a-create-file").expect("valid commit id")
        );
        assert_eq!(commit.committed_seq, ChangeSeq(1));
        assert_eq!(
            commit.results,
            vec![CommitOpResult::CreateFile {
                op_index: 0,
                inode_id: InodeId(2),
                revision_no: loon_api::RevisionNo(1),
                content_ref: content_ref.clone(),
            }]
        );

        let repeated_commit = harness
            .client
            .commit_operations(namespace, &commit_request)
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
            .list_changes(namespace, ChangeSeq(0))
            .expect("list changes");
        assert_eq!(changes.namespace_id.as_str(), namespace);
        assert_eq!(changes.from_exclusive_seq, ChangeSeq(0));
        assert_eq!(changes.through_seq, commit.committed_seq);
        assert_eq!(changes.changes.len(), 1);
        let change = &changes.changes[0];
        assert_eq!(change.seq, commit.committed_seq);
        assert_eq!(change.commit_id, commit.commit_id);
        assert_eq!(change.commit_id, commit_request.commit_id);
        assert_eq!(change.message.as_deref(), Some("upload over http"));
        assert_eq!(change.annotations, Some(annotations));
        assert_eq!(change.ops, commit.results);
        assert_eq!(change.deltas.len(), 3);
        assert!(matches!(
            &change.deltas[1],
            CommitDelta::BindDirentry {
                semantic_op_index: 0,
                delta_index: 1,
                name_key,
                display_name,
                ..
            } if name_key.as_str() == "uploaded.txt" && display_name == "uploaded.txt"
        ));

        let empty = harness
            .client
            .list_changes(namespace, commit.committed_seq)
            .expect("list changes after head");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_commit_restore_revision_appends_new_head_and_reports_change() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loon-server-restore",
        "http-restore",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        let target = NamespacePath::parse("demo:/restore.txt").expect("target");
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let first_content_ref =
            stage_uploaded_content_ref(&harness.client, namespace, b"first bytes\n");
        let create = harness
            .client
            .commit_operations(
                namespace,
                &ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-create").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::CreateFile {
                        parent_inode: InodeId(1),
                        display_name: "restore.txt".to_owned(),
                        content_ref: first_content_ref.clone(),
                    }],
                    message: None,
                    annotations: None,
                },
            )
            .expect("create file");
        let inode_id = match &create.results[0] {
            CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
            other => panic!("unexpected create result: {other:?}"),
        };

        let second_content_ref =
            stage_uploaded_content_ref(&harness.client, namespace, b"second bytes\n");
        let replace = harness
            .client
            .commit_operations(
                namespace,
                &ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-replace").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::ReplaceFile {
                        inode_id,
                        base_revision_no: RevisionNo(1),
                        content_ref: second_content_ref.clone(),
                    }],
                    message: None,
                    annotations: None,
                },
            )
            .expect("replace file");
        assert_eq!(replace.committed_seq, ChangeSeq(2));

        let restore = harness
            .client
            .commit_operations(
                namespace,
                &ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-restore").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::RestoreRevision {
                        inode_id,
                        source_revision_no: RevisionNo(1),
                        base_revision_no: RevisionNo(2),
                    }],
                    message: Some("restore revision".to_owned()),
                    annotations: None,
                },
            )
            .expect("restore revision");
        assert_eq!(restore.committed_seq, ChangeSeq(3));
        assert_eq!(
            restore.results,
            vec![CommitOpResult::RestoreRevision {
                op_index: 0,
                inode_id,
                source_revision_no: RevisionNo(1),
                revision_no: RevisionNo(3),
                content_ref: first_content_ref.clone(),
            }]
        );

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
            .list_changes(namespace, ChangeSeq(0))
            .expect("list changes");
        assert_eq!(changes.changes.len(), 3);
        assert_eq!(
            changes.changes[2].commit_id,
            CommitId::parse("req-restore-restore").expect("valid commit id")
        );
        assert_eq!(changes.changes[2].ops, restore.results);
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
        "loon-server-revisions",
        "http-revisions",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");
        let target = NamespacePath::parse("demo:/docs/rev.txt").expect("target");
        harness
            .client
            .put_file_bytes(&target, b"one", false)
            .expect("create file");
        harness
            .client
            .put_file_bytes(&target, b"two", true)
            .expect("replace file");

        let entry = harness.client.stat_path(&target).expect("stat file");
        let revisions = harness
            .client
            .list_file_revisions(&target)
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

        let moved = NamespacePath::parse("demo:/docs/moved.txt").expect("moved");
        harness
            .client
            .move_path(&target, &moved)
            .expect("move path");
        assert!(matches!(
            harness.client.list_file_revisions(&target),
            Err(ClientError::Api { code, .. }) if code == "path_not_found"
        ));
        let inode_revisions = harness
            .client
            .list_file_revisions_for_inode(namespace, entry.inode_id)
            .expect("inode revisions");
        assert_eq!(inode_revisions.revisions.len(), 2);
        assert_eq!(
            harness
                .client
                .read_file_revision_bytes_for_inode(namespace, entry.inode_id, RevisionNo(2))
                .expect("read inode revision"),
            b"two"
        );

        harness
            .client
            .restore_file_revision(&moved, RevisionNo(1))
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
                namespace,
                entry.inode_id,
                RevisionNo(2),
                RevisionNo(3),
                "c_restore_inode_revision_0001",
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
        "loon-server-restore-missing-source",
        "http-restore-missing-source",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let first_content_ref =
            stage_uploaded_content_ref(&harness.client, namespace, b"first bytes\n");
        let create = harness
            .client
            .commit_operations(
                namespace,
                &ApiCommitRequest {
                    commit_id: CommitId::parse("req-restore-missing-source-create")
                        .expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![CommitOp::CreateFile {
                        parent_inode: InodeId(1),
                        display_name: "restore.txt".to_owned(),
                        content_ref: first_content_ref,
                    }],
                    message: None,
                    annotations: None,
                },
            )
            .expect("create file");
        let inode_id = match &create.results[0] {
            CommitOpResult::CreateFile { inode_id, .. } => *inode_id,
            other => panic!("unexpected create result: {other:?}"),
        };

        match harness.client.commit_operations(
            namespace,
            &ApiCommitRequest {
                commit_id: CommitId::parse("req-restore-missing-source-restore")
                    .expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::RestoreRevision {
                    inode_id,
                    source_revision_no: RevisionNo(99),
                    base_revision_no: RevisionNo(1),
                }],
                message: None,
                annotations: None,
            },
        ) {
            Err(ClientError::Api { status, code, .. }) => {
                assert_eq!(status, 409);
                assert_eq!(code, "revision_not_found");
            }
            other => panic!("expected revision_not_found, got {other:?}"),
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
        "loon-server-current-conflict",
        "http-current-conflict",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        harness
            .client
            .create_namespace(namespace)
            .expect("create namespace");

        let first_content_ref =
            stage_uploaded_content_ref(&harness.client, namespace, b"first payload\n");
        let first_request = ApiCommitRequest {
            commit_id: CommitId::parse("req-phase-2a-conflict").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "first.txt".to_owned(),
                content_ref: first_content_ref,
            }],
            message: Some("first commit".to_owned()),
            annotations: None,
        };
        harness
            .client
            .commit_operations(namespace, &first_request)
            .expect("first commit");

        let second_content_ref =
            stage_uploaded_content_ref(&harness.client, namespace, b"second payload\n");
        let mut changed_annotations = BTreeMap::new();
        changed_annotations.insert("source".to_owned(), json!("changed"));
        let conflicting_request = ApiCommitRequest {
            commit_id: first_request.commit_id.clone(),
            preconditions: first_request.preconditions.clone(),
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(1),
                display_name: "second.txt".to_owned(),
                content_ref: second_content_ref,
            }],
            message: Some("second commit".to_owned()),
            annotations: Some(changed_annotations),
        };

        match harness
            .client
            .commit_operations(namespace, &conflicting_request)
        {
            Err(ClientError::Api { code, .. }) => {
                assert_eq!(code, "commit_id_reuse_conflict")
            }
            other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
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
        "loon-server-put",
        "http-put",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let target = NamespacePath::parse("demo:/docs/retry.txt").expect("target");
        let commit_id = "req-v1-put";

        let first = harness
            .client
            .put_file_bytes_with_commit_id(&target, b"stable bytes\n", false, commit_id)
            .expect("first put");
        assert!(first.committed_seq.0 >= 1);

        let repeated = harness
            .client
            .put_file_bytes_with_commit_id(&target, b"stable bytes\n", false, commit_id)
            .expect("repeat put");
        assert_eq!(repeated, first);

        let entry = harness.client.stat_path(&target).expect("stat path");
        assert_eq!(entry.authoritative_head_seq, first.committed_seq);
        let bytes = harness.client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"stable bytes\n");

        match harness.client.put_file_bytes_with_commit_id(
            &target,
            b"different bytes\n",
            false,
            commit_id,
        ) {
            Err(ClientError::Api { code, .. }) => {
                assert_eq!(code, "commit_id_reuse_conflict")
            }
            other => panic!("expected commit_id_reuse_conflict, got {other:?}"),
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
        "loon-server-ops",
        "http-ops",
        60_000,
    ))
    .await;

    tokio::task::spawn_blocking(move || {
        harness
            .client
            .create_namespace("demo")
            .expect("create namespace");
        let source = NamespacePath::parse("demo:/docs/source.txt").expect("source");
        harness
            .client
            .write_file_bytes(&source, b"source bytes\n")
            .expect("seed source");

        let copied = NamespacePath::parse("demo:/docs/copied.txt").expect("copied");
        let copy_first = harness
            .client
            .copy_path_with_commit_id(&source, &copied, "req-v1-copy")
            .expect("copy first");
        let copy_repeated = harness
            .client
            .copy_path_with_commit_id(&source, &copied, "req-v1-copy")
            .expect("copy repeat");
        assert_eq!(copy_repeated, copy_first);
        let source_entry = harness.client.stat_path(&source).expect("source stat");
        let copied_entry = harness.client.stat_path(&copied).expect("copied stat");
        assert_ne!(source_entry.inode_id, copied_entry.inode_id);
        assert_eq!(source_entry.content_ref, copied_entry.content_ref);

        let moved = NamespacePath::parse("demo:/docs/moved.txt").expect("moved");
        let move_first = harness
            .client
            .move_path_with_commit_id(&copied, &moved, "req-v1-move")
            .expect("move first");
        let move_repeated = harness
            .client
            .move_path_with_commit_id(&copied, &moved, "req-v1-move")
            .expect("move repeat");
        assert_eq!(move_repeated, move_first);
        match harness.client.stat_path(&copied) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => panic!("expected path_not_found for moved-from path, got {other:?}"),
        }
        let moved_entry = harness.client.stat_path(&moved).expect("moved stat");
        assert_eq!(moved_entry.inode_id, copied_entry.inode_id);

        let delete_first = harness
            .client
            .delete_path_with_commit_id(&moved, "req-v1-delete")
            .expect("delete first");
        let delete_repeated = harness
            .client
            .delete_path_with_commit_id(&moved, "req-v1-delete")
            .expect("delete repeat");
        assert_eq!(delete_repeated, delete_first);
        match harness.client.stat_path(&moved) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "path_not_found"),
            other => panic!("expected path_not_found for deleted path, got {other:?}"),
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
        "loon-server-admin",
        "http-admin",
        60_000,
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        let target = NamespacePath::parse("demo:/docs/hello.txt").expect("target");
        client
            .create_namespace(namespace)
            .expect("create namespace");
        client
            .write_file_bytes(&target, b"hello admin\n")
            .expect("write file");

        let first = post_checkpoint(&server_url, namespace).expect("first checkpoint");
        assert_eq!(first.checkpoint_seq, ChangeSeq(1));
        assert_eq!(first.checkpoint_hint_seq, Some(ChangeSeq(1)));
        assert!(first.checkpoint_hint_points_at_checkpoint);

        let repeated = post_checkpoint(&server_url, namespace).expect("repeat checkpoint");
        assert_eq!(repeated, first);

        let advanced = post_retention_advance(&server_url, namespace).expect("advance retention");
        assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));

        let repeated_advance =
            post_retention_advance(&server_url, namespace).expect("repeat retention");
        assert_eq!(repeated_advance, advanced);

        let bytes = client.read_file_bytes(&target).expect("read file");
        assert_eq!(bytes, b"hello admin\n");

        match client.list_changes(namespace, ChangeSeq(0)) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "rebootstrap_required"),
            other => panic!("expected rebootstrap_required, got {other:?}"),
        }

        let empty = client
            .list_changes(namespace, ChangeSeq(1))
            .expect("changes after floor");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admin_retention_advance_preserves_checkpoint_unavailable_error() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loon-server-admin-missing-checkpoint",
        "http-admin-missing-checkpoint",
        60_000,
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        client
            .create_namespace(namespace)
            .expect("create namespace");

        match post_retention_advance(&server_url, namespace) {
            Err(ApiError { code, .. }) => assert_eq!(code, "checkpoint_unavailable"),
            other => panic!("expected checkpoint_unavailable, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_checkpoint_consumption_is_strict_when_manifest_is_corrupted() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loon-server-admin-corrupt",
        "http-admin-corrupt",
        60_000,
    ))
    .await;
    let client = harness.client.clone();
    let server_url = harness.server_url.clone();
    let store_root = harness.store_root.clone();
    let store_key_prefix = harness.store_key_prefix.clone();

    tokio::task::spawn_blocking(move || {
        let namespace = "demo";
        let target = NamespacePath::parse("demo:/docs/hello.txt").expect("target");
        client
            .create_namespace(namespace)
            .expect("create namespace");
        client
            .write_file_bytes(&target, b"hello\n")
            .expect("write file");
        post_checkpoint(&server_url, namespace).expect("checkpoint");

        let store = ConfiguredObjectStore::local_fs(&store_root, store_key_prefix.as_deref())
            .expect("construct store");
        store
            .put_overwrite(&checkpoint_manifest(namespace, 1), br#"{"bad":"json"}"#)
            .expect("corrupt manifest");

        match client.stat_path(&target) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "namespace_corrupt"),
            other => panic!("expected namespace_corrupt, got {other:?}"),
        }
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_share_one_store_and_handoff_the_lease() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let server_a = start_server(test_config(
        store_root.clone(),
        "loon-server-a",
        "two-server-smoke",
        60_000,
    ))
    .await;
    let server_b = start_server(test_config(
        store_root,
        "loon-server-b",
        "two-server-smoke",
        60_000,
    ))
    .await;
    let client_a = server_a.client.clone();
    let client_b = server_b.client.clone();
    let store_root = server_a.store_root.clone();
    let store_key_prefix = server_a.store_key_prefix.clone();

    tokio::task::spawn_blocking(move || {
        client_a.create_namespace("demo").expect("create namespace");
        let host_a_target = NamespacePath::parse("demo:/docs/host-a.txt").expect("host a target");
        client_a
            .write_file_bytes(&host_a_target, b"host a\n")
            .expect("host a write");

        let host_b_target = NamespacePath::parse("demo:/docs/host-b.txt").expect("host b target");
        match client_b.move_path(&host_a_target, &host_b_target) {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "lease_conflict"),
            other => panic!("expected lease_conflict, got {other:?}"),
        }
        force_expire_namespace_lease(&store_root, store_key_prefix.as_deref(), "demo");

        let committed_seq = retry_until_lease_handoff(&client_b, &host_a_target, &host_b_target);
        assert!(
            committed_seq >= 2,
            "expected later commit seq, got {committed_seq}"
        );

        let host_b_entry = client_a
            .stat_path(&host_b_target)
            .expect("stat host b file");
        assert_eq!(host_b_entry.authoritative_head_seq.0, committed_seq);
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
        other => panic!("test harness requires local fs store, got {other:?}"),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = app(config).expect("build app");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });

    TestServer {
        client: Client::new(ClientConfig {
            server_url: format!("http://{}", addr),
            auth_token: Some("test-token".to_owned()),
        }),
        server_url: format!("http://{}", addr),
        store_root,
        store_key_prefix,
        server,
    }
}

fn test_config(
    store_root: std::path::PathBuf,
    writer_id: &str,
    key_prefix: &str,
    lease_duration_ms: u64,
) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".to_owned()),
        writer_id: writer_id.to_owned(),
        writer_version: format!("{writer_id}/0.1.0"),
        lease_duration_ms,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: Some(key_prefix.to_owned()),
        },
    }
}

fn post_checkpoint(
    server_url: &str,
    namespace: &str,
) -> Result<CreateCheckpointResponse, ApiError> {
    post_admin_json(
        &format!("{server_url}/v0/admin/namespaces/{namespace}/checkpoint"),
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
    let request = ureq::post(url).set("authorization", &format!("Bearer {auth_token}"));
    match request.call() {
        Ok(response) => serde_json::from_reader(response.into_reader()).map_err(|err| ApiError {
            code: "invalid_json".to_owned(),
            message: err.to_string(),
        }),
        Err(ureq::Error::Status(_, response)) => Err(serde_json::from_reader::<_, ApiError>(
            response.into_reader(),
        )
        .unwrap_or_else(|err| ApiError {
            code: "invalid_json".to_owned(),
            message: err.to_string(),
        })),
        Err(ureq::Error::Transport(error)) => Err(ApiError {
            code: "transport".to_owned(),
            message: error.to_string(),
        }),
    }
}

fn assert_invalid_namespace_response(result: Result<ureq::Response, ureq::Error>) {
    match result {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_namespace_id");
            assert!(error.message.contains("invalid namespace_id"));
        }
        other => panic!("expected invalid_namespace_id response, got {other:?}"),
    }
}

fn retry_until_lease_handoff(client: &Client, from: &NamespacePath, to: &NamespacePath) -> u64 {
    for _attempt in 0..20 {
        match client.move_path(from, to) {
            Ok(result) => return result.committed_seq.0,
            Err(ClientError::Api { code, .. }) if code == "lease_conflict" => {
                thread::sleep(Duration::from_millis(50));
            }
            other => panic!("expected success or lease_conflict while waiting, got {other:?}"),
        }
    }

    panic!("timed out waiting for lease handoff");
}

fn force_expire_namespace_lease(
    store_root: &std::path::Path,
    key_prefix: Option<&str>,
    namespace: &str,
) {
    let store =
        ConfiguredObjectStore::local_fs(store_root, key_prefix).expect("construct test store");
    let lease_key = namespace_lease(namespace);
    let bytes = store
        .get(&lease_key, None)
        .expect("read namespace lease")
        .expect("namespace lease exists");
    let mut envelope: LeaseStateEnvelope =
        serde_json::from_slice(&bytes).expect("decode namespace lease");
    envelope.state.lease_expires_at_ms = 0;
    let writer_version = envelope.writer_version;
    let envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        writer_version,
        envelope.state,
    )
    .expect("encode expired namespace lease");
    let bytes = serde_json::to_vec(&envelope).expect("serialize expired namespace lease");
    store
        .put_overwrite(&lease_key, &bytes)
        .expect("write expired namespace lease");
}

fn stage_uploaded_content_ref(client: &Client, namespace: &str, file_bytes: &[u8]) -> ContentRef {
    let begin = client.begin_upload(namespace).expect("begin upload");
    let staged = client
        .upload_content(namespace, &begin.upload_id, file_bytes)
        .expect("upload content");
    let complete_request = CompleteUploadRequest {
        content_ref: staged.content_ref,
    };
    let complete = client
        .complete_upload(namespace, &begin.upload_id, &complete_request)
        .expect("complete upload");
    let repeated = client
        .complete_upload(namespace, &begin.upload_id, &complete_request)
        .expect("repeat complete upload");
    assert_eq!(repeated, complete);
    complete.content_ref
}
