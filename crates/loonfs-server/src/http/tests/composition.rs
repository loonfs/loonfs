//! Contract routes composed with and without server operational routes.

use super::*;
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt;

#[tokio::test]
async fn compositions_preserve_operational_routes_capabilities_and_filesystem_behavior() {
    for serves_maintenance in [false, true] {
        let directory = tempdir().expect("temporary store");
        let mut config = test_config(directory.path(), "composed-writer");
        config.maintenance = if serves_maintenance {
            crate::MaintenanceMode::ServeAndMaintain
        } else {
            crate::MaintenanceMode::MaintainOnly
        };
        let (router, state) = app(config, AppOptions::default())
            .await
            .expect("server app");
        let router = if serves_maintenance {
            router
        } else {
            loonfs_http::router(state.binding.clone())
        };
        assert_eq!(state.binding.options.serves_maintenance, serves_maintenance);
        assert!(state.runner.is_some(), "background maintenance survives");

        for path in ["/health", "/readiness", "/metrics"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            if serves_maintenance {
                assert_eq!(
                    response.status(),
                    if path == "/metrics" {
                        StatusCode::UNAUTHORIZED
                    } else {
                        StatusCode::OK
                    },
                    "{path}"
                );
            } else {
                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
                let body = axum::body::to_bytes(response.into_body(), 4096)
                    .await
                    .expect("body");
                let error: serde_json::Value =
                    serde_json::from_slice(&body).expect("error envelope");
                assert_eq!(error["code"], "route_not_found", "{path}");
            }
        }

        let denied = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v0/capabilities")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let capabilities = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v0/capabilities")
                    .header("authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(capabilities.status(), StatusCode::OK);
        let body = axum::body::to_bytes(capabilities.into_body(), 65536)
            .await
            .expect("body");
        let capabilities: CapabilityDocument = serde_json::from_slice(&body).expect("capabilities");
        capabilities.validate().expect("valid capabilities");
        assert_eq!(
            capabilities
                .api_groups
                .iter()
                .any(|group| group == loonfs_api::API_GROUP_MAINTENANCE_V0),
            serves_maintenance
        );

        let created = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/namespaces")
                    .header("authorization", "Bearer test-token")
                    .header("loonfs-actor", "composition-test")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"namespace_id":"example"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(created.status().is_success(), "{}", created.status());
        let fetched = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v0/namespaces/example")
                    .header("authorization", "Bearer test-token")
                    .header("loonfs-actor", "composition-test")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(fetched.status(), StatusCode::OK);
        let diagnostics = router
            .oneshot(
                Request::builder()
                    .uri("/v0/maintenance/namespaces/example/diagnostics")
                    .header("authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            diagnostics.status(),
            if serves_maintenance {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        );
        if let Some(runner) = &state.runner {
            runner.shutdown().await.expect("maintenance shutdown");
        }
        state.writer.shutdown().await.expect("writer shutdown");
    }
}
