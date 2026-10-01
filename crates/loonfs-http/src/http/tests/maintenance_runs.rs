//! Maintenance runs against namespaces that do not exist or are deleted.

use super::*;
use tower::ServiceExt;

async fn run(
    router: &axum::Router,
    namespace_id: &NamespaceId,
    body: &str,
) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v0/maintenance/namespaces/{namespace_id}/runs"))
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_owned()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    let code = body["code"].as_str().map_or_else(
        || body["kind"].as_str().expect("job kind").to_owned(),
        str::to_owned,
    );
    (status, code)
}

#[tokio::test]
async fn runtime_jobs_answer_a_missing_and_a_deleted_namespace_with_fixed_codes() {
    let directory = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(directory.path(), "maintenance-runs"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let missing = namespace_id("missing");
    let deleted = namespace_id("deleted");
    state
        .runtime
        .create_namespace(
            &deleted,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    state
        .runtime
        .open_namespace(&deleted)
        .expect("open namespace")
        .delete(Default::default())
        .await
        .expect("delete namespace");

    let mut answers = Vec::new();
    for kind in ["metadata", "metadata_compaction", "gc", "retention"] {
        for namespace_id in [&missing, &deleted] {
            let body = format!(r#"{{"kind":"{kind}"}}"#);
            let (status, code) = run(&router, namespace_id, &body).await;
            answers.push(format!("{kind} {namespace_id}: {} {code}", status.as_u16()));
        }
    }
    assert_eq!(
        answers,
        [
            "metadata missing: 404 namespace_not_found",
            "metadata deleted: 410 namespace_deleted",
            "metadata_compaction missing: 404 namespace_not_found",
            "metadata_compaction deleted: 410 namespace_deleted",
            "gc missing: 404 namespace_not_found",
            "gc deleted: 200 gc",
            "retention missing: 404 namespace_not_found",
            "retention deleted: 410 namespace_deleted",
        ]
    );
    state.runtime.shutdown().await.expect("shutdown writer");
}
