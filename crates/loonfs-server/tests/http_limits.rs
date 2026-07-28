//! HTTP request validation and configured limit enforcement.

mod common;

use common::http_split_support::*;
use common::start_server;
use loonfs_api::{ApiError, CommitResponse};
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

    // `exchange` is not a DestinationBehavior variant, so it fails request
    // validation inside the error envelope. `replace` is one, and commits:
    // it is the control that proves the rejection comes from the behavior
    // value rather than from an operation body the server never decoded.
    let move_request = |commit_id: &str, behavior: &str| {
        json!({
            "commit_id": commit_id,
            "operation": {
                "kind": "move_path",
                "from_path": "/docs/source.txt",
                "to_path": "/docs/target.txt",
                "behavior": behavior,
            },
        })
    };
    let operations_url = format!(
        "{}/v0/namespaces/demo/filesystem/operations",
        harness.server_url
    );

    match raw_agent()
        .post(&operations_url)
        .set("authorization", "Bearer test-token")
        .send_json(move_request("move-exchange", "exchange"))
    {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_request");
        }
        other => unreachable!("expected rejected move behavior, got {other:?}"),
    }

    let accepted = raw_agent()
        .post(&operations_url)
        .set("authorization", "Bearer test-token")
        .send_json(move_request("move-replace", "replace"))
        .expect("replace is a valid move behavior");
    assert_eq!(accepted.status(), 200);
    let committed: CommitResponse =
        serde_json::from_reader(accepted.into_reader()).expect("decode commit response");
    assert_eq!(committed.commit_id.as_str(), "move-replace");

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
