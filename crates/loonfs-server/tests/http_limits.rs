//! HTTP request validation and configured limit enforcement.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs::publish::MAX_COMMIT_CONTENT_TOKENS;
use loonfs_api::{
    v0::{CommitOp, CommitRequest as ApiCommitRequest, CommitSubmissionRequest},
    ApiError, ChangeSeq, CommitId, ErrorCode, InodeId,
};
use loonfs_client::NamespacePath;
use loonfs_test_support::http::raw_agent;
use loonfs_test_support::ids::namespace_id;
use serde_json::json;
use tempfile::tempdir;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_rejects_invalid_namespace_ids_in_body_and_path() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-test",
        "http-invalid-ns",
    ))
    .await;

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

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
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

    let completed = stage_uploaded_content(&harness.client, &namespace, b"proof limit").await;
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
        .list_changes(&namespace, ChangeSeq(0), None)
        .await
        .expect("list changes");
    assert!(changes.changes.is_empty());

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

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");
    let source = NamespacePath::parse("demo", "/docs/source.txt").expect("source");
    harness
        .client
        .put_file_bytes(&source, b"source", &replace_file_options())
        .await
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

    harness.server.abort();
}
