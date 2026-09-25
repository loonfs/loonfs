//! Actor headers on read requests.

use super::fixtures::{test_app, test_options, TestAppOptions};
use tempfile::tempdir;

#[tokio::test]
async fn a_read_ignores_the_actor_header() {
    use tower::ServiceExt as _;
    let temp_dir = tempdir().expect("tempdir");
    let (router, _state) = test_app(
        test_options(&temp_dir.path().join("store"), "actor-read"),
        TestAppOptions::default(),
    )
    .await
    .expect("build app");
    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/v0/capabilities")
                .header("authorization", "Bearer test-token")
                .header("Loonfs-Actor", "ignored invalid actor")
                .body(axum::body::Body::empty())
                .expect("read request"),
        )
        .await
        .expect("read response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
