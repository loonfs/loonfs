//! HTTP content-token admission and replay behavior.

use crate::common::http_split_support::*;
use crate::common::start_server;
use loonfs_api::ContentId;
use loonfs_api::{
    v0::ValidatedContentToken, AbsolutePath, ApiError, ChangeSeq, CommitId, CommitRequest,
    CommitResponse, ContentRef, DestinationBehavior, ErrorCode, FilesystemOperation,
};
use loonfs_client::NamespacePath;
use loonfs_test_support::ids::namespace_id;
use serde_json::json;
use std::io::Read as _;
use tempfile::tempdir;

fn response_bytes(response: ureq::Response) -> Vec<u8> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .expect("read response bytes");
    bytes
}

fn assert_content_not_prepared_response(
    result: Result<ureq::Response, Box<ureq::Error>>,
    request: &CommitRequest,
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

fn missing_content_proof_message(request: &CommitRequest) -> String {
    let [FilesystemOperation::PutFile { content_ref, .. }] = &request.operations[..] else {
        unreachable!("content preparation assertion requires a one-put request");
    };
    format!(
        "content object `{}` is not prepared for publication",
        content_ref.content_id
    )
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

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let completed = stage_uploaded_content(&harness.client, &namespace, b"token rejected").await;

    let request = CommitRequest {
        commit_id: CommitId::parse("bad-token-put").expect("valid commit id"),
        message: None,
        content_tokens: vec![ValidatedContentToken {
            content_ref: completed.content_ref.clone(),
            token: "not.a.valid.token".to_owned(),
        }],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/bad-token.txt").expect("path"),
            content_ref: completed.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    assert_content_not_prepared_response(
        send_commit(&harness.server_url, &namespace, &request),
        &request,
        "content token was rejected: content token is malformed",
    );

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

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let completed = stage_uploaded_content(&harness.client, &namespace, b"token missing").await;
    let request = CommitRequest {
        commit_id: CommitId::parse("missing-token-put").expect("valid commit id"),
        message: None,
        content_tokens: Vec::new(),
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/missing-token.txt").expect("path"),
            content_ref: completed.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };

    assert_content_not_prepared_response(
        send_commit(&harness.server_url, &namespace, &request),
        &request,
        &missing_content_proof_message(&request),
    );

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

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let bytes = b"valid token";
    let completed = stage_uploaded_content(&harness.client, &namespace, bytes).await;
    let request = CommitRequest {
        commit_id: CommitId::parse("valid-token-put").expect("valid commit id"),
        message: None,
        content_tokens: vec![ValidatedContentToken {
            content_ref: completed.content_ref.clone(),
            token: completed
                .validated_content_token
                .expect("completed upload carries token"),
        }],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/valid-token.txt").expect("path"),
            content_ref: completed.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    let response = send_commit(&harness.server_url, &namespace, &request).expect("valid token put");
    let response: CommitResponse =
        serde_json::from_reader(response.into_reader()).expect("decode operation response");
    assert_eq!(response.committed_seq, ChangeSeq(1));

    let target = NamespacePath::parse("demo", "/valid-token.txt").expect("target");
    assert_eq!(
        harness
            .client
            .get_file_bytes(&target)
            .await
            .expect("read file"),
        bytes
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn landed_path_put_replays_after_content_token_is_absent_rejected_or_garbage() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-current",
        "http-content-token-replay",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let completed = stage_uploaded_content(&harness.client, &namespace, b"token replay").await;
    let content_ref = completed.content_ref.clone();
    let mut request = CommitRequest {
        commit_id: CommitId::parse("token-replay-put").expect("valid commit id"),
        message: None,
        content_tokens: vec![ValidatedContentToken {
            content_ref: completed.content_ref.clone(),
            token: completed
                .validated_content_token
                .expect("completed upload carries token"),
        }],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/token-replay.txt").expect("path"),
            content_ref: completed.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    // Replays must be byte-for-byte: the durable receipt answers, not a
    // fresh evaluation that could phrase the same outcome differently.
    let send = |request: &CommitRequest| {
        response_bytes(
            send_commit(&harness.server_url, &namespace, request)
                .expect("landed put should replay"),
        )
    };
    let original = send(&request);
    serde_json::from_slice::<CommitResponse>(&original).expect("decode operation response");

    request.content_tokens.clear();
    assert_eq!(send(&request), original);

    // A real receipt, but for other bytes: well formed, correctly signed,
    // and rejected. Only a completed session mints one, so this is the
    // closest a test can get to a token the publisher will turn down for a
    // reason other than syntax.
    let other = stage_uploaded_content(&harness.client, &namespace, b"other bytes").await;
    request.content_tokens = vec![ValidatedContentToken {
        content_ref: content_ref.clone(),
        token: other
            .validated_content_token
            .expect("completed upload carries token"),
    }];
    assert_eq!(send(&request), original);

    request.content_tokens = vec![ValidatedContentToken {
        content_ref,
        token: "not.a.valid.token".to_owned(),
    }];
    assert_eq!(send(&request), original);

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

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let target = stage_uploaded_content(&harness.client, &namespace, b"target content").await;
    let irrelevant =
        stage_uploaded_content(&harness.client, &namespace, b"irrelevant content").await;
    let request = CommitRequest {
        commit_id: CommitId::parse("irrelevant-token-put").expect("valid commit id"),
        message: None,
        content_tokens: vec![ValidatedContentToken {
            content_ref: irrelevant.content_ref,
            token: irrelevant
                .validated_content_token
                .expect("completed upload carries token"),
        }],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/irrelevant-token.txt").expect("path"),
            content_ref: target.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };

    assert_content_not_prepared_response(
        send_commit(&harness.server_url, &namespace, &request),
        &request,
        &missing_content_proof_message(&request),
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn puts_with_a_valid_token_reuse_the_ref_and_ignore_irrelevant_tokens() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-commit-tokens",
        "http-commit-valid-tokens",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let first = stage_uploaded_content(&harness.client, &namespace, b"first").await;
    // An irrelevant garbage token rides along with the valid proof; only
    // the token covering the operation's ref decides admission.
    let request = CommitRequest {
        commit_id: CommitId::parse("put-all-proofs").expect("valid commit id"),
        message: None,
        content_tokens: vec![
            validated_content_token(&first),
            ValidatedContentToken {
                content_ref: ContentRef::blob_v1(ContentId::generate(), b"irrelevant"),
                token: "irrelevant.garbage".to_owned(),
            },
        ],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/first.txt").expect("path"),
            content_ref: first.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    let response =
        send_commit(&harness.server_url, &namespace, &request).expect("covered ref is prepared");
    let response: CommitResponse =
        serde_json::from_reader(response.into_reader()).expect("decode operation response");
    assert_eq!(response.committed_seq, ChangeSeq(1));

    // The same staged ref and token admit a second put: preparation
    // belongs to the content, not to one operation.
    let repeat_ref = CommitRequest {
        commit_id: CommitId::parse("put-repeated-ref").expect("valid commit id"),
        message: None,
        content_tokens: vec![validated_content_token(&first)],
        operations: vec![FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/first-copy.txt").expect("path"),
            content_ref: first.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        }],
    };
    send_commit(&harness.server_url, &namespace, &repeat_ref)
        .expect("repeated ref is still prepared");
    assert_eq!(
        harness
            .client
            .stat_path(&NamespacePath::parse("demo", "/first-copy.txt").expect("path"))
            .await
            .expect("repeated ref file")
            .content_ref,
        Some(first.content_ref)
    );

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_operation_body_without_content_tokens_still_parses_and_commits_mkdir() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-bare-commit",
        "http-bare-commit",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");
    let body = json!({
        "commit_id": "bare-commit-mkdir",
        "operations": [{
            "kind": "create_directory",
            "path": "/docs"
        }],
        "message": null
    });

    let response =
        send_commit_json(&harness.server_url, &namespace, &body).expect("bare operation body");
    let response: CommitResponse =
        serde_json::from_reader(response.into_reader()).expect("decode response");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    harness
        .client
        .stat_path(&NamespacePath::parse("demo", "/docs").expect("path"))
        .await
        .expect("mkdir committed");

    harness.server.abort();
}
