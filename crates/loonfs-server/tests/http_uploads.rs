//! HTTP upload session and upload-backed commit flows.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{
    v0::{
        BeginUploadRequest, CommitDelta, CommitOp, CommitRequest as ApiCommitRequest,
        CommitSubmissionRequest, CompleteUploadRequest,
    },
    ApiError, ChangeSeq, CommitId, ContentRef, InodeId,
};
use loonfs_client::{ClientError, NamespacePath};
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

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
        let result = raw_agent()
            .put(&format!(
                "{}/v0/namespaces/demo/uploads/{invalid_upload_id}/content",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/octet-stream")
            .send_bytes(b"hello");
        let ureq::Error::Status(status, response) =
            result.expect_err("invalid upload id should fail")
        else {
            unreachable!("invalid upload id should return an HTTP status");
        };
        assert_eq!(status, 400);
        let error: ApiError =
            serde_json::from_reader(response.into_reader()).expect("API error envelope");
        assert_eq!(error.code, "invalid_request");
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
            .upload_content(&namespace, &begin.upload_id, file_bytes)
            .expect("upload content");
        let repeated_content = harness
            .client
            .upload_content(&namespace, &begin.upload_id, file_bytes)
            .expect("repeat upload content");
        assert_eq!(first_content, repeated_content);
        match harness
            .client
            .upload_content(&namespace, &begin.upload_id, b"different bytes")
        {
            Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_content_conflict"),
            other => unreachable!("expected upload_content_conflict, got {other:?}"),
        }

        let mismatch_upload = harness
            .client
            .begin_upload(&namespace, &BeginUploadRequest::default())
            .expect("begin mismatch upload");
        let staged = harness
            .client
            .upload_content(&namespace, &mismatch_upload.upload_id, file_bytes)
            .expect("stage mismatch upload content");
        assert_ne!(
            staged.content_ref,
            ContentRef::whole_file_v0(b"other bytes")
        );
        match harness.client.complete_upload(
            &namespace,
            &mismatch_upload.upload_id,
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
            .get_file_bytes(&target)
            .expect("read committed file");
        assert_eq!(read_back, file_bytes);

        let changes = harness
            .client
            .list_changes(&namespace, ChangeSeq(0), None)
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
            .list_changes(&namespace, commit.committed_seq, None)
            .expect("list changes after head");
        assert_eq!(empty.changes, Vec::new());
    })
    .await
    .expect("join blocking task");

    harness.server.abort();
}
