//! HTTP content-token admission and replay behavior.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs::content_tokens::mint_content_token;
use loonfs::publish::MAX_COMMIT_CONTENT_TOKENS;
use loonfs_api::{
    v0::{
        CommitOp, CommitRequest as ApiCommitRequest, CommitSubmissionRequest, ValidatedContentToken,
    },
    AbsolutePath, ApiError, ChangeSeq, CommitId, CommitResponse, ContentRef, DestinationBehavior,
    ErrorCode, FilesystemOperation, FilesystemOperationRequest, InodeId, NamespaceId,
};
use loonfs_client::NamespacePath;
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use serde_json::json;
use std::io::Read as _;
use tempfile::tempdir;

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
                path: AbsolutePath::parse("/bad-token.txt").expect("path"),
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
                path: AbsolutePath::parse("/missing-token.txt").expect("path"),
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
                path: AbsolutePath::parse("/valid-token.txt").expect("path"),
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
            harness.client.get_file_bytes(&target).expect("read file"),
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
                path: AbsolutePath::parse("/token-replay.txt").expect("path"),
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
                path: AbsolutePath::parse("/irrelevant-token.txt").expect("path"),
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
