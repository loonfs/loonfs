//! Request refusal, probe access, and store traffic at the host limit.

use super::*;
use axum::body::{Body, Bytes};
use axum::http::Request;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use tower::ServiceExt;

#[tokio::test]
async fn full_request_admission_refuses_an_unsent_body_and_keeps_probes_available() {
    let directory = tempdir().expect("temporary store");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = test_config(directory.path(), "admission-writer");
    config.max_in_flight_requests = 1;
    let (router, state) = app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let limit = state.binding.request_limit.as_ref().expect("request limit");
    let before = store.counts();
    let first_polls = Arc::new(AtomicUsize::new(0));
    let mut first = Box::pin(
        router
            .clone()
            .oneshot(create_namespace_request(unsent_body(&first_polls))),
    );
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(first_polls.load(Ordering::SeqCst), 1);
    assert_eq!(limit.in_flight(), 1);

    let refused_polls = Arc::new(AtomicUsize::new(0));
    let refused = router
        .clone()
        .oneshot(create_namespace_request(unsent_body(&refused_polls)));
    let refused = tokio::time::timeout(std::time::Duration::from_secs(1), refused)
        .await
        .expect("refusal must not wait for a body")
        .expect("response");
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.headers()["retry-after"], "1");
    let request_id = refused.headers()["x-request-id"]
        .to_str()
        .expect("request id")
        .to_owned();
    let body = axum::body::to_bytes(refused.into_body(), 4096)
        .await
        .expect("error body");
    let error: loonfs_types::ApiError = serde_json::from_slice(&body).expect("error envelope");
    assert_eq!(error.code, "server_busy");
    assert_eq!(error.request_id.as_deref(), Some(request_id.as_str()));
    assert_eq!(refused_polls.load(Ordering::SeqCst), 0);

    for path in ["/health", "/readiness?probe=1"] {
        let response = router.clone().oneshot(request(path)).await.expect("probe");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(limit.in_flight(), 1);
    }
    let rendered =
        super::super::metrics::render(&state.binding.metrics, None, None, 8, 16, limit.in_flight());
    assert!(rendered.contains("loonfs_server_in_flight_requests 1\n"));
    assert!(rendered.contains("loonfs_server_busy_rejections_total{kind=\"request\"} 1\n"));
    assert!(rendered.contains("method=\"POST\",route=\"/v0/namespaces\",status_class=\"5xx\"} 1\n"));
    assert_eq!(store.counts(), before);
    drop(first);
    assert_eq!(limit.in_flight(), 0);

    let response = router
        .clone()
        .oneshot(request("/v0/namespaces/missing"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(limit.in_flight(), 1);
    let admitted_counts = store.counts();
    assert_ne!(admitted_counts, before);
    axum::body::to_bytes(response.into_body(), 4096)
        .await
        .expect("body");
    assert_eq!(limit.in_flight(), 0);

    store.take();
    let mut uncapped = state.binding.clone();
    uncapped.request_limit = None;
    let response = loonfs_http::router(uncapped)
        .oneshot(request("/v0/namespaces/missing"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(store.counts(), admitted_counts);
    drop(response);
    let response = router
        .oneshot(request("/v0/capabilities"))
        .await
        .expect("response");
    assert_eq!(limit.in_flight(), 1);
    drop(response);
    assert_eq!(limit.in_flight(), 0);
    state.runtime.shutdown().await.expect("shutdown");
}

fn request(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .header("authorization", "Bearer test-token")
        .body(Body::empty())
        .expect("request")
}

fn create_namespace_request(body: Body) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v0/namespaces")
        .header("authorization", "Bearer test-token")
        .header("content-type", "application/json")
        .header("content-length", "128")
        .header("loonfs-actor", "test-actor")
        .body(body)
        .expect("request")
}

fn unsent_body(polls: &Arc<AtomicUsize>) -> Body {
    let polls = Arc::clone(polls);
    Body::from_stream(futures::stream::poll_fn(move |_| {
        polls.fetch_add(1, Ordering::SeqCst);
        Poll::<Option<Result<Bytes, std::io::Error>>>::Pending
    }))
}

#[tokio::test]
async fn a_full_server_answers_busy_before_the_client_sends_any_body_byte() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let directory = tempdir().expect("temporary store");
    let store = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        |_| true,
    ));
    let mut config = test_config(directory.path(), "unsent-body-writer");
    config.max_in_flight_requests = 1;
    let (router, state) = app(config, options_with_store(store.clone()))
        .await
        .expect("app");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    store.arm();
    let mut first = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    first.write_all(b"GET /v0/namespaces/missing HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-token\r\nConnection: close\r\n\r\n").await.expect("send headers");
    store.wait_until_blocked().await;

    let mut refused = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    refused.write_all(b"POST /v0/namespaces HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-token\r\nContent-Type: application/json\r\nContent-Length: 128\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").await.expect("send headers only");
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        refused.read_to_end(&mut response),
    )
    .await
    .expect("response before sending body")
    .expect("read response");
    let response = String::from_utf8(response).expect("response text");
    assert!(
        response.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
        "{response}"
    );
    assert!(response.contains("retry-after: 1\r\n"), "{response}");
    assert!(response.contains("\"code\":\"server_busy\""), "{response}");

    let mut probe = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect probe");
    probe
        .write_all(b"GET /readiness HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("probe headers");
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        probe.read_to_end(&mut response),
    )
    .await
    .expect("probe at request cap")
    .expect("probe response");
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    store.release();
    first
        .read_to_end(&mut Vec::new())
        .await
        .expect("first response");
    server.abort();
    state.runtime.shutdown().await.expect("shutdown");
}
