#![cfg(feature = "openapi")]
#![allow(clippy::panic)]

use serde_json::Value;
use std::collections::BTreeSet;

const OPENAPI_JSON_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/openapi.json");

#[test]
fn openapi_static_file_is_current() {
    let mut generated = loonfs_server::openapi_json_pretty().expect("generate openapi json");
    generated.push('\n');
    let committed = std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json");

    assert_eq!(
        generated, committed,
        "docs/specs/openapi.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- docs/specs/openapi.json`"
    );
}

#[test]
fn openapi_documents_current_server_paths() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");

    for (path, method) in [
        ("/health", "get"),
        ("/v0/config", "get"),
        ("/v0/namespaces", "post"),
        ("/v0/namespaces", "get"),
        ("/v0/namespaces/{namespace}", "get"),
        ("/v0/namespaces/{namespace}", "delete"),
        ("/v0/namespaces/{namespace}/forks", "post"),
        ("/v0/namespaces/{namespace}/filesystem/list", "get"),
        ("/v0/namespaces/{namespace}/filesystem/stat", "get"),
        ("/v0/namespaces/{namespace}/filesystem/content", "get"),
        ("/v0/namespaces/{namespace}/filesystem/revisions", "get"),
        ("/v0/namespaces/{namespace}/filesystem/operations", "post"),
        (
            "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions",
            "get",
        ),
        (
            "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
        ),
        (
            "/v0/namespaces/{namespace}/inodes/{inode_id}/revisions/{source_revision_no}/restore",
            "post",
        ),
        ("/v0/namespaces/{namespace}/uploads", "post"),
        (
            "/v0/namespaces/{namespace}/uploads/{upload_id}/content",
            "put",
        ),
        (
            "/v0/namespaces/{namespace}/uploads/{upload_id}/complete",
            "post",
        ),
        ("/v0/namespaces/{namespace}/commits", "post"),
        ("/v0/namespaces/{namespace}/changes", "get"),
        ("/v0/admin/namespaces/{namespace}/checkpoint", "post"),
        ("/v0/admin/namespaces/{namespace}/retention/advance", "post"),
    ] {
        assert_path_method(paths, path, method);
    }

    assert!(!paths.contains_key("/openapi.json"));
    assert_query_params(paths, "/v0/namespaces", "get", &["limit", "cursor"]);
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace}/filesystem/list",
        "get",
        &["path", "limit", "cursor"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace}/changes",
        "get",
        &["after_seq", "limit"],
    );
}

fn assert_path_method(paths: &serde_json::Map<String, Value>, path: &str, method: &str) {
    let path_item = paths
        .get(path)
        .unwrap_or_else(|| panic!("missing OpenAPI path `{path}`"));
    assert!(
        path_item.get(method).is_some(),
        "missing OpenAPI method `{method}` for `{path}`"
    );
}

fn assert_query_params(
    paths: &serde_json::Map<String, Value>,
    path: &str,
    method: &str,
    expected: &[&str],
) {
    let operation = paths
        .get(path)
        .and_then(|path_item| path_item.get(method))
        .unwrap_or_else(|| panic!("missing OpenAPI operation `{method} {path}`"));
    let params = operation
        .get("parameters")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing parameters for `{method} {path}`"));
    let query_names = params
        .iter()
        .filter(|param| param.get("in").and_then(Value::as_str) == Some("query"))
        .filter_map(|param| param.get("name").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();

    for name in expected {
        assert!(
            query_names.contains(name),
            "missing query parameter `{name}` for `{method} {path}`"
        );
    }
}
