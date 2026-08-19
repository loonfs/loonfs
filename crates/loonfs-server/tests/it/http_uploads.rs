//! HTTP upload session and upload-backed commit flows.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::{
    v0::{
        BeginUploadRequest, FilesystemChange, UploadMode, UploadSessionResponse,
        UploadSessionStatus,
    },
    AbsolutePath, ApiError, ChangeSeq, CommitId, CommitRequest, CommitResponse, ContentRef,
    DestinationBehavior, ErrorCode, FilesystemOperation, InodeId, RevisionNo,
    LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES,
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
        panic!("invalid upload id should return an HTTP status");
    };
    assert_eq!(status, 400);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, "invalid_request");

    harness.server.abort();
}

/// A begin body that mixes two transports, or names none, is refused where
/// it is read. These used to reach a handler that compared the fields back
/// against the mode; the tagged request means there is nothing to compare,
/// and the standard 400 envelope is what the client sees either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_begin_upload_rejects_a_body_that_mixes_transports() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-begin-upload-shape",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");

    for body in [
        // A proxied begin has no geometry to ask for.
        r#"{"mode":"service_proxied","multipart":{"part_size_bytes":8388608}}"#,
        // A direct put with nothing to sign is not a direct put.
        r#"{"mode":"direct_put"}"#,
        // A multipart begin promises nothing about its payload.
        r#"{"mode":"direct_multipart","content":{"size_bytes":5,"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}"#,
        // And a request that does not say how it moves its bytes.
        "{}",
    ] {
        let result = raw_agent()
            .post(&format!(
                "{}/v0/namespaces/demo/uploads",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/json")
            .send_string(body);
        let ureq::Error::Status(status, response) =
            result.expect_err("a mixed begin body should fail")
        else {
            panic!("a rejected begin body returns an HTTP status");
        };
        assert_eq!(status, 400, "body: {body}");
        let error: ApiError =
            serde_json::from_reader(response.into_reader()).expect("API error envelope");
        assert_eq!(error.code, "invalid_request", "body: {body}");
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_proxied_mode_rejects_multipart_and_retired_completion_fields_precisely() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-completion-shape",
    ))
    .await;
    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let begin = harness
        .client
        .begin_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");

    let completion_url = format!(
        "{}/v0/namespaces/{namespace}/uploads/{}/complete",
        harness.server_url,
        begin.upload_id()
    );
    for (body, wrong) in [
        (
            r#"{"content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}},"parts":[]}"#,
            "unknown field `content`",
        ),
        (
            r#"{"completion":"content_ref"}"#,
            "unknown field `completion`",
        ),
        (r#"{"content_ref":{}}"#, "unknown field `content_ref`"),
    ] {
        let result = raw_agent()
            .post(&completion_url)
            .set("authorization", "Bearer test-token")
            .set("content-type", "application/json")
            .send_string(body);
        let ureq::Error::Status(status, response) =
            result.expect_err("wrong completion shape should fail")
        else {
            panic!("a rejected completion body returns an HTTP status");
        };
        assert_eq!(status, 400);
        let error: ApiError =
            serde_json::from_reader(response.into_reader()).expect("API error envelope");
        assert_eq!(error.code, "invalid_request");
        assert!(
            error.message.contains(wrong),
            "completion error should name `{wrong}` verbatim: {}",
            error.message
        );
        assert!(error.message.contains("service_proxied completion"));
    }

    harness
        .client
        .upload_content(&namespace, begin.upload_id(), b"hello")
        .await
        .expect("stage content");
    harness
        .client
        .complete_upload(&namespace, begin.upload_id())
        .await
        .expect("empty completion succeeds for proxied mode");
    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_body_one_under_reaches_session_validation_and_one_over_answers_413() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-completion-body-cap",
    ))
    .await;
    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let begin = harness
        .client
        .begin_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let limit = harness
        .client
        .capabilities()
        .await
        .expect("fetch capabilities")
        .limits
        .get(LIMIT_UPLOAD_COMPLETION_MAX_BODY_BYTES)
        .copied()
        .and_then(|limit| usize::try_from(limit).ok())
        .expect("completion body limit is advertised and fits usize");
    let completion_url = format!(
        "{}/v0/namespaces/{namespace}/uploads/{}/complete",
        harness.server_url,
        begin.upload_id()
    );

    let mut just_under = vec![b' '; limit - 1];
    just_under[..2].copy_from_slice(b"{}");
    let result = raw_agent()
        .post(&completion_url)
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_bytes(&just_under);
    let ureq::Error::Status(status, response) =
        result.expect_err("unstaged content should fail session validation")
    else {
        panic!("an unstaged completion returns an HTTP status");
    };
    assert_eq!(status, 400);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    assert!(
        error.message.contains("upload content has not been staged"),
        "body below the cap should decode and reach session validation: {}",
        error.message
    );
    drop(just_under);

    let one_over = vec![b' '; limit + 1];
    let result = raw_agent()
        .post(&completion_url)
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_bytes(&one_over);
    let ureq::Error::Status(status, response) =
        result.expect_err("body above the completion cap should fail")
    else {
        panic!("an oversized completion returns an HTTP status");
    };
    assert_eq!(status, 413);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, ErrorCode::ContentTooLarge.as_str());
    assert!(
        error.message.contains(&format!("{limit} bytes")),
        "oversize error should name the enforced limit: {}",
        error.message
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_content_token_passes_unchanged_into_http_commit() {
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
        .begin_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let first_content = harness
        .client
        .upload_content(&namespace, begin.upload_id(), file_bytes)
        .await
        .expect("upload content");
    let repeated_content = harness
        .client
        .upload_content(&namespace, begin.upload_id(), file_bytes)
        .await
        .expect("repeat upload content");
    assert_eq!(first_content, repeated_content);
    match harness
        .client
        .upload_content(&namespace, begin.upload_id(), b"different bytes")
        .await
    {
        Err(ClientError::Api { code, .. }) => assert_eq!(code, "upload_content_conflict"),
        other => panic!("expected upload_content_conflict, got {other:?}"),
    }

    let completed = stage_uploaded_content(&harness.client, &namespace, file_bytes).await;
    let content_ref = completed.content_ref.clone();

    // Copy the completion token directly into the commit request.
    let put_request = CommitRequest {
        commit_id: CommitId::parse("req-phase-2a-create-file").expect("valid commit id"),
        actor: loonfs_test_support::test_actor(),
        message: Some("upload over http".to_owned()),
        content_tokens: vec![content_token(&completed)],
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
        .stat_path(&target, &Default::default())
        .await
        .expect("stat committed file");
    assert_eq!(stat.inode_id, InodeId(2));
    assert_eq!(stat.content_ref(), Some(&content_ref));
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
    assert_eq!(change.committed_seq, commit.committed_seq);
    assert_eq!(change.commit_id, commit.commit_id);
    assert_eq!(change.commit_id, put_request.commit_id);
    assert_eq!(change.message.as_deref(), Some("upload over http"));
    // One semantic event per request operation: the file creation with its
    // binding and first revision.
    assert_eq!(change.events.len(), 1);
    assert!(matches!(
        &change.events[0],
        FilesystemChange::FileCreated {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name,
            revision_no: RevisionNo(1),
            content_ref: created_ref,
        } if display_name.as_str() == "uploaded.txt" && *created_ref == content_ref
    ));

    let empty = harness
        .client
        .list_changes(&namespace, commit.committed_seq, None)
        .await
        .expect("list changes after head");
    assert_eq!(empty.changes, Vec::new());

    harness.server.abort();
}

/// The two new surfaces over HTTP: reading a session, and ending one
/// without content. The status read is what re-mints, so a client that lost
/// its commit response gets a usable token back without sending a byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upload_status_re_mints_and_abort_is_terminal() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-upload-status-and-abort",
    ))
    .await;
    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    // An open session reports itself and mints nothing.
    let open = harness
        .client
        .begin_upload(&namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let status = get_upload_status(&harness.server_url, open.upload_id());
    assert_eq!(status.namespace_id, namespace);
    assert_eq!(&status.upload_id, open.upload_id());
    assert_eq!(status.mode, UploadMode::ServiceProxied);
    assert!(matches!(status.status, UploadSessionStatus::Open { .. }));

    // Aborting it is terminal, repeatable, and observable.
    let aborted = abort_upload(&harness.server_url, open.upload_id()).expect("abort");
    let repeated = abort_upload(&harness.server_url, open.upload_id()).expect("repeated abort");
    assert_eq!(repeated, aborted);
    assert_eq!(aborted.namespace_id, namespace);
    assert_eq!(&aborted.upload_id, open.upload_id());
    assert_eq!(aborted.mode, UploadMode::ServiceProxied);
    let UploadSessionStatus::Aborted {
        aborted_at_ms: response_aborted_at_ms,
    } = aborted.status
    else {
        panic!("abort reports an aborted session")
    };
    let status = get_upload_status(&harness.server_url, open.upload_id());
    assert_eq!(status.mode, UploadMode::ServiceProxied);
    let UploadSessionStatus::Aborted { aborted_at_ms } = status.status else {
        panic!("an aborted session reports itself aborted");
    };
    assert_eq!(aborted_at_ms, response_aborted_at_ms);

    // Completing an aborted session reports the same absence its eventual
    // deletion will.
    let completion = harness
        .client
        .complete_upload(&namespace, open.upload_id())
        .await
        .expect_err("an aborted session cannot complete");
    assert_eq!(completion.code(), Some(ErrorCode::UploadNotFound));

    // A completed session re-mints on every read, and refuses to be aborted.
    // The client here is one that lost its commit response and threw the
    // completion's receipt away: it reads the session and commits with what
    // the read hands back, without sending a byte of content again.
    let (upload_id, content_ref, completed_at_ms) =
        complete_upload_session(&harness, &namespace, b"status re-mint").await;
    let status = get_upload_status(&harness.server_url, &upload_id);
    assert_eq!(status.mode, UploadMode::ServiceProxied);
    let UploadSessionStatus::Completed {
        completed_at_ms: reported_completed_at_ms,
        content_ref: reported_ref,
        content_token,
    } = status.status
    else {
        panic!("a completed session reports itself completed");
    };
    assert_eq!(reported_completed_at_ms, completed_at_ms);
    assert_eq!(reported_ref, content_ref);
    let re_minted = content_token.expect("a completed session re-mints");
    let commit = send_commit(
        &harness.server_url,
        &namespace,
        &CommitRequest {
            commit_id: CommitId::parse("re-minted-receipt-put").expect("valid commit id"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            content_tokens: vec![re_minted],
            operations: vec![FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/re-minted.txt").expect("path"),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            }],
        },
    )
    .expect("a re-minted receipt admits its content");
    let commit: CommitResponse =
        serde_json::from_reader(commit.into_reader()).expect("decode commit response");
    assert_eq!(commit.committed_seq, ChangeSeq(1));

    let ureq::Error::Status(status_code, response) =
        *abort_upload(&harness.server_url, &upload_id).expect_err("a completed session is final")
    else {
        panic!("aborting a completed session should return an HTTP status");
    };
    assert_eq!(status_code, 409);
    let error: ApiError =
        serde_json::from_reader(response.into_reader()).expect("API error envelope");
    assert_eq!(error.code, "upload_already_completed");

    harness.server.abort();
}

/// Cross-process recovery through the Rust client alone: the upload id is
/// the only thing that has to survive. Reading the session back names the
/// exact content that landed and hands over a token that admits it, so the
/// retry commits without re-uploading anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_reads_a_completed_upload_back_and_commits_what_it_names() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-client-upload-readback",
    ))
    .await;
    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    let file_bytes = b"read the session back";
    let (upload_id, content_ref, completed_at_ms) =
        complete_upload_session(&harness, &namespace, file_bytes).await;

    let status = harness
        .client
        .get_upload_status(&namespace, &upload_id)
        .await
        .expect("read the upload session back");
    assert_eq!(status.namespace_id, namespace);
    assert_eq!(status.upload_id, upload_id);
    assert_eq!(status.mode, UploadMode::ServiceProxied);
    let UploadSessionStatus::Completed {
        completed_at_ms: reported_completed_at_ms,
        content_ref: reported_ref,
        content_token,
    } = status.status
    else {
        panic!("a completed session reports itself completed");
    };
    assert_eq!(reported_completed_at_ms, completed_at_ms);
    assert_eq!(reported_ref, content_ref);

    let commit = harness
        .client
        .commit(
            &namespace,
            &CommitRequest {
                commit_id: CommitId::parse("client-read-back-put").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
                message: None,
                content_tokens: vec![content_token.expect("a completed session re-mints")],
                operations: vec![FilesystemOperation::PutFile {
                    path: AbsolutePath::parse("/read-back.txt").expect("path"),
                    content_ref,
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                }],
            },
        )
        .await
        .expect("the token the read handed back admits its content");
    assert_eq!(commit.committed_seq, ChangeSeq(1));

    let read_back = harness
        .client
        .get_file_bytes(&NamespacePath::parse("demo", "/read-back.txt").expect("path"))
        .await
        .expect("read the committed file");
    assert_eq!(read_back, file_bytes);

    harness.server.abort();
}

/// Stages content and hands back the session id, which the shared helper
/// does not expose.
async fn complete_upload_session(
    harness: &crate::common::TestServer,
    namespace: &loonfs_api::NamespaceId,
    bytes: &[u8],
) -> (loonfs_api::UploadId, ContentRef, u64) {
    let begin = harness
        .client
        .begin_upload(namespace, &BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    harness
        .client
        .upload_content(namespace, begin.upload_id(), bytes)
        .await
        .expect("upload content");
    let completed = harness
        .client
        .complete_upload(namespace, begin.upload_id())
        .await
        .expect("complete upload");
    assert_eq!(completed.mode, UploadMode::ServiceProxied);
    let UploadSessionStatus::Completed {
        completed_at_ms,
        content_ref,
        ..
    } = completed.status
    else {
        panic!("completion reports a completed session")
    };
    (begin.upload_id().clone(), content_ref, completed_at_ms)
}

fn get_upload_status(server_url: &str, upload_id: &loonfs_api::UploadId) -> UploadSessionResponse {
    let response = raw_agent()
        .get(&format!(
            "{server_url}/v0/namespaces/demo/uploads/{upload_id}"
        ))
        .set("authorization", "Bearer test-token")
        .call()
        .expect("read upload status");
    serde_json::from_reader(response.into_reader()).expect("decode upload status")
}

fn abort_upload(
    server_url: &str,
    upload_id: &loonfs_api::UploadId,
) -> Result<UploadSessionResponse, Box<ureq::Error>> {
    let response = raw_agent()
        .post(&format!(
            "{server_url}/v0/namespaces/demo/uploads/{upload_id}/abort"
        ))
        .set("authorization", "Bearer test-token")
        .set("content-type", "application/json")
        .send_string("{}")
        .map_err(Box::new)?;
    Ok(serde_json::from_reader(response.into_reader()).expect("decode abort response"))
}
