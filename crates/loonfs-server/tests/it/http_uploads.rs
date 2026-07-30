//! HTTP upload session and upload-backed commit flows.

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::ContentId;
use loonfs_api::{
    v0::{BeginUploadRequest, CompleteUploadRequest, FilesystemChange},
    AbsolutePath, ApiError, ChangeSeq, CommitId, CommitRequest, CommitResponse, ContentRef,
    DestinationBehavior, FilesystemOperation, InodeId, InodeKind, RevisionNo,
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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
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
    let ureq::Error::Status(status, response) = result.expect_err("invalid upload id should fail")
    else {
        unreachable!("invalid upload id should return an HTTP status");
    };
    assert_eq!(status, 400);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, "invalid_request");

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

    let namespace = namespace_id("demo");
    let file_bytes = b"phase-2a over http\n";
    let target = NamespacePath::parse("demo", "/uploaded.txt").expect("target");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let begin = harness
        .client
        .begin_upload(&namespace, &BeginUploadRequest::default())
        .await
        .expect("begin upload");
    let first_content = harness
        .client
        .upload_content(&namespace, &begin.upload_id, file_bytes)
        .await
        .expect("upload content");
    let repeated_content = harness
        .client
        .upload_content(&namespace, &begin.upload_id, file_bytes)
        .await
        .expect("repeat upload content");
    assert_eq!(first_content, repeated_content);
    match harness
        .client
        .upload_content(&namespace, &begin.upload_id, b"different bytes")
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_content_conflict"),
        other => unreachable!("expected upload_content_conflict, got {other:?}"),
    }

    let mismatch_upload = harness
        .client
        .begin_upload(&namespace, &BeginUploadRequest::default())
        .await
        .expect("begin mismatch upload");
    let staged = harness
        .client
        .upload_content(&namespace, &mismatch_upload.upload_id, file_bytes)
        .await
        .expect("stage mismatch upload content");
    assert_ne!(
        staged.content_ref,
        ContentRef::blob_v1(ContentId::generate(), b"other bytes")
    );
    match harness
        .client
        .complete_upload(
            &namespace,
            &mismatch_upload.upload_id,
            &CompleteUploadRequest {
                content_ref: ContentRef::blob_v1(ContentId::generate(), b"other bytes"),
            },
        )
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "invalid_request"),
        other => unreachable!("expected upload content rejection, got {other:?}"),
    }

    let completed = stage_uploaded_content(&harness.client, &namespace, file_bytes).await;
    let content_ref = completed.content_ref.clone();

    // The staged upload publishes through a put that references the
    // uploaded ref with its validated content token.
    let put_request = CommitRequest {
        commit_id: CommitId::parse("req-phase-2a-create-file").expect("valid commit id"),
        message: Some("upload over http".to_owned()),
        content_tokens: vec![validated_content_token(&completed)],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/uploaded.txt").expect("path"),
            content_ref: content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    let send_put = |request: &CommitRequest| {
        let response =
            send_commit(&harness.server_url, &namespace, request).expect("commit uploaded file");
        serde_json::from_reader::<_, CommitResponse>(response.into_reader())
            .expect("decode operation response")
    };
    let commit = send_put(&put_request);
    assert_eq!(
        commit.commit_id,
        CommitId::parse("req-phase-2a-create-file").expect("valid commit id")
    );
    assert_eq!(commit.committed_seq, ChangeSeq(1));

    let repeated_commit = send_put(&put_request);
    assert_eq!(repeated_commit, commit);

    let stat = harness
        .client
        .stat_path(&target)
        .await
        .expect("stat committed file");
    assert_eq!(stat.inode_id, InodeId(2));
    assert_eq!(stat.content_ref.as_ref(), Some(&content_ref));
    let read_back = harness
        .client
        .get_file_bytes(&target)
        .await
        .expect("read committed file");
    assert_eq!(read_back, file_bytes);

    let changes = harness
        .client
        .list_changes(&namespace, ChangeSeq(0), None)
        .await
        .expect("list changes");
    assert_eq!(changes.namespace_id, namespace);
    assert_eq!(changes.after_seq, ChangeSeq(0));
    assert_eq!(changes.through_seq, commit.committed_seq);
    assert_eq!(changes.changes.len(), 1);
    let change = &changes.changes[0];
    assert_eq!(change.seq, commit.committed_seq);
    assert_eq!(change.commit_id, commit.commit_id);
    assert_eq!(change.commit_id, put_request.commit_id);
    assert_eq!(change.message.as_deref(), Some("upload over http"));
    // One semantic event per request operation: the file creation with its
    // binding and first revision.
    assert_eq!(change.events.len(), 1);
    assert!(matches!(
        &change.events[0],
        FilesystemChange::Created {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(1),
            name,
            revision_no: Some(RevisionNo(1)),
            content_ref: Some(created_ref),
        } if name.as_str() == "uploaded.txt" && *created_ref == content_ref
    ));

    let empty = harness
        .client
        .list_changes(&namespace, commit.committed_seq, None)
        .await
        .expect("list changes after head");
    assert_eq!(empty.changes, Vec::new());

    harness.server.abort();
}
