//! Request admission while a slow client drains a buffered response.

use super::{test_app, test_options, TestAppOptions};
use crate::{observe_routes, RequestLimit};
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use std::num::NonZeroUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

#[tokio::test]
async fn a_slow_client_keeps_the_request_slot_until_only_transport_buffering_remains() {
    const RESPONSE_BYTES: usize = 1024 * 1024;
    const RETAINED_BYTES: usize = 8192 + 4096 * 100 + 64 * 1024;

    let directory = tempfile::tempdir().expect("temporary store");
    let (_, mut state) = test_app(
        test_options(directory.path(), "slow-response-writer"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let limit = RequestLimit::new(NonZeroUsize::new(1).expect("positive cap"));
    state.request_limit = Some(limit.clone());
    let router = observe_routes(
        Router::new().route(
            "/",
            get(|| async { Bytes::from(vec![b'x'; RESPONSE_BYTES]) }),
        ),
        &state,
    );
    let (server, mut client) = tokio::io::duplex(1024);
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .expect("request headers");
    let mut connection = Box::pin(http1::Builder::new().serve_connection(
        TokioIo::new(server),
        TowerToHyperService::new(router.clone()),
    ));
    assert!(futures::poll!(connection.as_mut()).is_pending());
    assert_eq!(limit.in_flight(), 1);

    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        client
            .read_exact(&mut byte)
            .await
            .expect("response headers");
        headers.push(byte[0]);
    }
    let headers = String::from_utf8(headers).expect("header text");
    assert!(headers.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(headers.contains("content-length: 1048576\r\n"));

    let mut read = 0;
    let mut released = false;
    while read < RESPONSE_BYTES {
        assert!(futures::poll!(connection.as_mut()).is_pending());
        let in_flight = limit.in_flight();
        if RESPONSE_BYTES - read > RETAINED_BYTES {
            assert_eq!(in_flight, 1, "client has read {read} response bytes");
        }
        if !released {
            let response = router
                .clone()
                .oneshot(Request::new(Body::empty()))
                .await
                .expect("second response");
            if in_flight == 1 {
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(response.headers()["retry-after"], "1");
            } else {
                assert!(RESPONSE_BYTES - read <= RETAINED_BYTES);
                assert_eq!(response.status(), StatusCode::OK);
                released = true;
            }
            drop(response);
            assert_eq!(limit.in_flight(), in_flight);
        }
        let mut buffer = [0; 1024];
        let count = tokio::select! {
            result = connection.as_mut() => panic!("connection ended before the response was read: {result:?}"),
            result = client.read(&mut buffer) => result.expect("response bytes"),
        };
        assert!(count > 0);
        assert!(buffer[..count].iter().all(|byte| *byte == b'x'));
        read += count;
    }
    assert!(released);
    assert_eq!(limit.in_flight(), 0);
    drop(connection);
    state.runtime.shutdown().await.expect("shutdown");
}
