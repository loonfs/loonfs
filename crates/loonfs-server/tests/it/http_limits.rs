//! HTTP request validation and configured limit enforcement.

#![allow(clippy::panic)]

use crate::common::http_split_support::*;
use crate::common::start_server;
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
        other => panic!("expected invalid_namespace_id response, got {other:?}"),
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
                "{}/v0/namespaces/bad%25/filesystem/entries?path=/",
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
            "actor": loonfs_test_support::test_actor(),
            "operations": [{
                "kind": "move_path",
                "from_path": "/docs/source.txt",
                "to_path": "/docs/target.txt",
                "behavior": behavior,
            }],
        })
    };
    let commits_url = format!("{}/v0/namespaces/demo/commits", harness.server_url);

    match raw_agent()
        .post(&commits_url)
        .set("authorization", "Bearer test-token")
        .send_json(move_request("move-exchange", "exchange"))
    {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_request");
        }
        other => panic!("expected rejected move behavior, got {other:?}"),
    }

    let accepted = raw_agent()
        .post(&commits_url)
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
        other => panic!("expected rejected upload body, got {other:?}"),
    }

    harness.server.abort();
}

/// Asserts that an unknown query parameter returns the expected API error.
fn assert_unknown_query_parameter(
    result: Result<ureq::Response, ureq::Error>,
    expected_param: &str,
) -> ApiError {
    match result {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_request");
            assert_eq!(error.param.as_deref(), Some(expected_param));
            error
        }
        other => panic!("expected an unknown query parameter response, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_misspelled_delete_guard_is_rejected_and_the_namespace_survives() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-query-guard",
        "http-query-guard",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    // Reject the misspelled guard instead of deleting without a guard.
    let error = assert_unknown_query_parameter(
        raw_agent()
            .delete(&format!(
                "{}/v0/namespaces/demo?expected_head_sq=418",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call(),
        "expected_head_sq",
    );
    assert!(
        error.message.contains("expected_head_seq"),
        "the message names the parameter the caller meant: {}",
        error.message
    );

    harness
        .client
        .get_namespace(&namespace)
        .await
        .expect("the namespace outlives the rejected delete");

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_query_parameters_are_rejected_on_every_operation() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-query-strictness",
        "http-query-strictness",
    ))
    .await;

    harness
        .client
        .create_namespace(&namespace_id("demo"))
        .await
        .expect("create namespace");

    // Reject a misspelled parameter on a route that accepts query parameters.
    assert_unknown_query_parameter(
        raw_agent()
            .get(&format!(
                "{}/v0/namespaces/demo/filesystem/entry?path=%2F&include_atributes=false",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call(),
        "include_atributes",
    );

    // Routes with no query parameters reject every parameter.
    assert_unknown_query_parameter(
        raw_agent()
            .get(&format!(
                "{}/v0/namespaces/demo?include_attributes=true",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call(),
        "include_attributes",
    );

    // The same rule applies to routes without path parameters.
    assert_unknown_query_parameter(
        raw_agent()
            .get(&format!(
                "{}/v0/capabilities?verbose=true",
                harness.server_url
            ))
            .set("authorization", "Bearer test-token")
            .call(),
        "verbose",
    );

    // Reject repeated parameters instead of choosing one value. The error
    // message names the parameter, while `param` remains unset.
    match raw_agent()
        .get(&format!(
            "{}/v0/namespaces/demo/filesystem/entry?path=%2F&path=%2Fdocs",
            harness.server_url
        ))
        .set("authorization", "Bearer test-token")
        .call()
    {
        Err(ureq::Error::Status(status, response)) => {
            assert_eq!(status, 400);
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "invalid_request");
            assert_eq!(error.param, None);
            assert!(
                error.message.contains("duplicate field `path`"),
                "the message names the repeated parameter: {}",
                error.message
            );
        }
        other => panic!("expected a repeated parameter rejection, got {other:?}"),
    }

    // An empty query string is valid.
    let empty_query = raw_agent()
        .get(&format!("{}/v0/capabilities?", harness.server_url))
        .set("authorization", "Bearer test-token")
        .call()
        .expect("an empty query string names no parameter");
    assert_eq!(empty_query.status(), 200);

    // Operational routes outside `/v0` ignore query parameters used by
    // probes and scrapers.
    for path in ["health", "readiness", "metrics"] {
        let response = raw_agent()
            .get(&format!("{}/{path}?cache_buster=1", harness.server_url))
            .set("authorization", "Bearer test-token")
            .call()
            .unwrap_or_else(|error| panic!("`/{path}` ignores query strings, got {error:?}"));
        assert_eq!(response.status(), 200);
    }

    harness.server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_query_parameter_without_credentials_answers_unauthorized() {
    let temp_dir = tempdir().expect("tempdir");
    let harness = start_server(test_config(
        temp_dir.path().join("store"),
        "loonfs-server-query-auth",
        "http-query-auth",
    ))
    .await;

    let namespace = namespace_id("demo");
    harness
        .client
        .create_namespace(&namespace)
        .await
        .expect("create namespace");

    // Authorization errors take precedence over query errors.
    for url in [
        format!(
            "{}/v0/namespaces/demo/filesystem/entry?path=%2F&include_atributes=false",
            harness.server_url
        ),
        format!(
            "{}/v0/namespaces/demo?expected_head_sq=418",
            harness.server_url
        ),
    ] {
        match raw_agent().get(&url).call() {
            Err(ureq::Error::Status(401, response)) => {
                let error: ApiError =
                    serde_json::from_reader(response.into_reader()).expect("decode api error");
                assert_eq!(error.code, "unauthorized");
                assert_eq!(error.param, None);
            }
            other => panic!("expected 401 for `{url}`, got {other:?}"),
        }
    }

    // The same ordering applies when the body extractor handles authorization.
    match raw_agent()
        .post(&format!(
            "{}/v0/namespaces/demo/commits?dry_run=true",
            harness.server_url
        ))
        .send_json(serde_json::json!({}))
    {
        Err(ureq::Error::Status(401, response)) => {
            let error: ApiError =
                serde_json::from_reader(response.into_reader()).expect("decode api error");
            assert_eq!(error.code, "unauthorized");
        }
        other => panic!("expected 401 for an unauthorized commit, got {other:?}"),
    }

    harness
        .client
        .get_namespace(&namespace)
        .await
        .expect("the namespace outlives the unauthorized requests");

    harness.server.abort();
}
