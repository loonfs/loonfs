//! Transport retry, identity, streaming, and timeout contracts.

use super::*;
use crate::{scripted_transport, ClientConfig};
use futures::FutureExt as _;
use loonfs_test_support::clock::ManualClock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct Layered {
    message: &'static str,
    cause: Option<Box<Layered>>,
}

impl std::fmt::Display for Layered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for Layered {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

/// Test clock that reports an expired deadline after its first read.
#[derive(Debug, Default)]
struct ExpiredAfterFirstRead {
    reads: AtomicU64,
}

impl MonotonicTimer for ExpiredAfterFirstRead {
    fn monotonic_now_ms(&self) -> u64 {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            0
        } else {
            100
        }
    }
}

fn deadline_client(transport: &crate::scripted_transport::ScriptedTransport) -> Client {
    let mut client = Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        },
        transport.clone(),
    )
    .expect("valid client config");
    client.transport_retry = TransportRetryPolicy {
        max_retries: 1,
        initial_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
        operation_deadline: Duration::from_millis(100),
    };
    client.timer = Arc::new(ExpiredAfterFirstRead::default());
    client
}

#[test]
fn send_errors_surface_the_root_cause_and_the_url() {
    let error = Layered {
        message: "error sending request",
        cause: Some(Box::new(Layered {
            message: "client error (Connect)",
            cause: Some(Box::new(Layered {
                message: "tcp connect error: Connection refused (os error 61)",
                cause: None,
            })),
        })),
    };

    let connect = render_send_error("http://127.0.0.1:9/v0/namespaces", &error, true, false);
    assert!(
        connect.contains("http://127.0.0.1:9/v0/namespaces"),
        "{connect}"
    );
    assert!(connect.contains("Connection refused"), "{connect}");
    assert!(connect.contains("`server_url`"), "{connect}");

    let timeout = render_send_error("http://h/v0", &error, false, true);
    assert!(timeout.contains("timed out"), "{timeout}");

    // A layer that restates its child is not repeated.
    let repeated = Layered {
        message: "outer: inner detail",
        cause: Some(Box::new(Layered {
            message: "inner detail",
            cause: None,
        })),
    };
    let rendered = render_send_error("http://h/v0", &repeated, false, false);
    assert!(rendered.contains("http://h/v0"), "{rendered}");
    assert!(rendered.contains("outer: inner detail"), "{rendered}");
    assert_eq!(rendered.matches("inner detail").count(), 1, "{rendered}");
}

#[tokio::test]
async fn pin_deletes_send_once_after_transport_failure() {
    let namespace_id = loonfs_api::NamespaceId::parse("demo").expect("namespace id");
    let pin_id =
        loonfs_api::PinId::parse("pin_00000000000000000001-0000000000000001").expect("pin id");
    for snapshot in [true, false] {
        let transport = scripted_transport::failures(2);
        let mut client = deadline_client(&transport);
        client.timer = Arc::new(StdMonotonicTimer::default());
        if snapshot {
            client
                .delete_snapshot(&namespace_id, &pin_id)
                .await
                .expect_err("transport failure");
        } else {
            client
                .delete_checkpoint(&namespace_id, &pin_id)
                .await
                .expect_err("transport failure");
        }
        assert_eq!(transport.attempts(), 1);
    }
}

#[tokio::test]
async fn transport_retries_stop_when_the_operation_deadline_is_spent() {
    let transport = scripted_transport::failures(1);
    let client = deadline_client(&transport);

    client
        .call(
            &client.get("http://example.invalid/control"),
            None,
            SendPolicy::Retry,
        )
        .await
        .expect_err("the spent deadline must preserve the first failure");

    assert_eq!(transport.attempts(), 1);
}

#[tokio::test]
async fn content_transfer_retries_are_exempt_from_the_operation_deadline() {
    let transport = scripted_transport::failure_then_success(b"content".to_vec());
    let client = deadline_client(&transport);

    let response = client
        .call(
            &client.get("http://example.invalid/content"),
            None,
            SendPolicy::RetryUnbounded,
        )
        .await
        .expect("the count-bounded content retry succeeds");

    assert_eq!(response.bytes, b"content");
    assert!(transport.attempts() >= 2, "{}", transport.attempts());
}

fn service_client<S>(service: S, request_timeout_ms: Option<u64>) -> Client
where
    S: tower::Service<Request<Body>, Response = Response<Body>, Error = TransportError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    Client::with_transport(
        ClientConfig {
            server_url: "http://127.0.0.1".to_owned(),
            auth_token: Some("test-token".into()),
            request_timeout_ms,
            disable_transient_retry: true,
            ca_cert_path: None,
        },
        service,
    )
    .expect("valid config")
}

#[tokio::test]
async fn services_receive_identical_body_and_identity_on_retries_but_not_provider_requests() {
    use loonfs_api::{PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId};
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let service = tower::service_fn(move |request: Request<Body>| {
        let recorded = recorded.clone();
        async move {
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await?.to_bytes();
            let mut recorded = recorded.lock().expect("requests lock");
            recorded.push((parts.method, parts.uri, parts.headers, bytes));
            if recorded.len() == 1 {
                let body = Body::from_stream(futures::stream::iter([
                    Ok(Bytes::from_static(b"partial")),
                    Err(std::io::Error::other("connection reset")),
                ]));
                Ok(Response::new(body))
            } else {
                Ok(Response::new(Body::from(Bytes::from_static(b"{}"))))
            }
        }
    });
    let mut client = service_client(service, None).with_subject(Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse("usr_demo").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse("usr_demo").expect("principal")
        ]))
        .expect("principals"),
    });
    client.transport_retry_enabled = true;
    client.transport_retry.initial_backoff = Duration::ZERO;
    client.transport_retry.max_backoff = Duration::ZERO;
    let request = client
        .post("http://127.0.0.1/commits")
        .header("Loonfs-Actor", "actor");
    let _: serde_json::Value = client
        .request_json(
            request,
            Some(&serde_json::json!({"key": "value"})),
            SendPolicy::Retry,
        )
        .await
        .expect("body failure retried");
    client
        .call(
            &WireRequest::presigned(
                Method::GET,
                "https://provider.invalid/object?signature=opaque",
            ),
            None,
            SendPolicy::Once,
        )
        .await
        .expect("provider request");

    let requests = requests.lock().expect("requests lock");
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[0].0, Method::POST);
    assert_eq!(requests[0].3, br#"{"key":"value"}"#.as_slice());
    let headers: Vec<_> = requests[0]
        .2
        .iter()
        .map(|(name, value)| (name.as_str(), value.to_str().expect("text")))
        .collect();
    assert_eq!(
        headers,
        vec![
            ("authorization", "Bearer test-token"),
            ("loonfs-principal-scope", "org_demo"),
            ("loonfs-subject", "usr_demo"),
            ("loonfs-principals", "usr_demo"),
            ("loonfs-actor", "actor"),
            ("content-type", "application/json"),
        ]
    );
    assert!(requests[0].2["authorization"].is_sensitive());
    assert!(requests[2].2.is_empty());
    assert_eq!(
        requests[2].1.to_string(),
        "https://provider.invalid/object?signature=opaque"
    );
}

fn timed_client<S>(service: S, timeout: Option<u64>) -> (Client, Arc<ManualClock>)
where
    S: tower::Service<Request<Body>, Response = Response<Body>, Error = TransportError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let clock = Arc::new(ManualClock::new(0));
    let mut client = service_client(service, timeout);
    client.timer = clock.clone();
    (client, clock)
}

async fn advance_time(clock: &ManualClock, elapsed_ms: u64) {
    clock.advance_ms(elapsed_ms);
    tokio::time::advance(Duration::from_millis(elapsed_ms)).await;
}

fn pending_chunks(count: usize) -> (Vec<tokio::sync::oneshot::Sender<Bytes>>, PayloadStream) {
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..count).map(|_| tokio::sync::oneshot::channel()).unzip();
    let chunks = futures::stream::iter(receivers)
        .then(|receiver| async { receiver.await.map_err(std::io::Error::other) })
        .boxed();
    (senders, chunks)
}

#[tokio::test(start_paused = true)]
async fn service_timeouts_cover_readiness_headers_and_streamed_bodies() {
    for (timeout, elapsed_ms) in [(None, 60_001), (Some(25), 25)] {
        let transport =
            scripted_transport::script([scripted_transport::Outcome::Success(Vec::new())]);
        let (client, clock) = timed_client(transport.clone(), timeout);
        let request = client.put("http://127.0.0.1/content");
        let upload = client.call_streamed_once(&request, futures::stream::pending().boxed(), None);
        tokio::pin!(upload);
        assert!(futures::poll!(&mut upload).is_pending());
        advance_time(&clock, elapsed_ms).await;
        let error = upload
            .now_or_never()
            .expect("request should finish")
            .expect_err("upload timeout");
        assert!(matches!(error, ClientError::Http(message) if message.contains("timed out")));
        assert_eq!(transport.attempts(), 1);
        assert_eq!(transport.sent()[0].body_bytes(), 0);

        let service = tower::service_fn(|_: Request<Body>| {
            std::future::pending::<std::result::Result<Response<Body>, TransportError>>()
        });
        let (client, clock) = timed_client(service, timeout);
        let headers = client.get_health();
        tokio::pin!(headers);
        assert!(futures::poll!(&mut headers).is_pending());
        advance_time(&clock, elapsed_ms).await;
        let error = headers
            .now_or_never()
            .expect("request should finish")
            .expect_err("header timeout");
        assert!(matches!(error, ClientError::Http(message) if message.contains("timed out")));

        let service = tower::service_fn(|_: Request<Body>| async {
            Ok(Response::new(Body::from_stream(
                futures::stream::pending::<std::io::Result<Bytes>>(),
            )))
        });
        let (client, clock) = timed_client(service, timeout);
        let mut stream = client
            .call_for_response_stream(&client.get("http://127.0.0.1/content"))
            .await
            .expect("response headers");
        let chunk = stream.next();
        tokio::pin!(chunk);
        assert!(futures::poll!(&mut chunk).is_pending());
        advance_time(&clock, elapsed_ms).await;
        let error = chunk
            .now_or_never()
            .expect("request should finish")
            .expect("body result")
            .expect_err("body timeout");
        assert!(error.to_string().contains("timed out"));
    }
    #[derive(Clone)]
    struct PendingService;
    impl tower::Service<Request<Body>> for PendingService {
        type Response = Response<Body>;
        type Error = TransportError;
        type Future = std::future::Pending<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn call(&mut self, _: Request<Body>) -> Self::Future {
            std::future::pending()
        }
    }
    for (timeout, elapsed_ms) in [(None, 60_001), (Some(25), 25)] {
        let (client, clock) = timed_client(PendingService, timeout);
        let request = client.get_health();
        tokio::pin!(request);
        assert!(futures::poll!(&mut request).is_pending());
        advance_time(&clock, elapsed_ms).await;
        let error = request
            .now_or_never()
            .expect("request should finish")
            .expect_err("readiness timeout");
        assert!(matches!(error, ClientError::Http(message) if message.contains("timed out")));
    }
}

#[tokio::test(start_paused = true)]
async fn upload_progress_resets_inactivity_without_resetting_the_request_deadline() {
    for timeout in [None, Some(90_000)] {
        let transport = scripted_transport::script([scripted_transport::Outcome::Success(
            b"uploaded".to_vec(),
        )]);
        let (client, clock) = timed_client(transport.clone(), timeout);
        let (senders, chunks) = pending_chunks(5);
        let request = client.put("http://127.0.0.1/content");
        let upload = client.call_streamed_once(&request, chunks, Some(15));
        tokio::pin!(upload);
        for (index, sender) in senders.into_iter().enumerate() {
            assert!(futures::poll!(&mut upload).is_pending());
            if timeout.is_some() && index == 2 {
                advance_time(&clock, 30_000).await;
                let error = upload
                    .as_mut()
                    .now_or_never()
                    .expect("request should finish at its deadline")
                    .expect_err("total deadline");
                assert!(
                    matches!(error, ClientError::Http(message) if message.contains("timed out"))
                );
                assert_eq!(transport.sent()[0].body_chunks, vec![3; 2]);
                break;
            }
            advance_time(&clock, if index == 0 { 20_000 } else { 40_000 }).await;
            sender
                .send(Bytes::from_static(b"one"))
                .expect("body reader");
        }
        if timeout.is_none() {
            assert_eq!(
                upload
                    .now_or_never()
                    .expect("upload should finish")
                    .expect("progressing upload"),
                b"uploaded"
            );
            assert_eq!(clock.now_ms(), 180_000);
            assert_eq!(transport.sent()[0].body_chunks, vec![3; 5]);
        }
        assert_eq!(transport.attempts(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn response_progress_resets_inactivity_without_resetting_total_deadlines() {
    for (configured, attempt) in [(None, None), (Some(90_000), None), (None, Some(90_000))] {
        let (senders, chunks) = pending_chunks(3);
        let (headers_ready, headers_wait) = tokio::sync::oneshot::channel::<()>();
        let response = Arc::new(std::sync::Mutex::new(Some((headers_wait, chunks))));
        let service = tower::service_fn(move |_: Request<Body>| {
            let (headers_wait, chunks) = response
                .lock()
                .expect("response lock")
                .take()
                .expect("one response");
            async move {
                headers_wait.await.expect("response headers");
                Ok(Response::new(Body::from_stream(chunks)))
            }
        });
        let (client, clock) = timed_client(service, configured);
        let request = client
            .build(&client.get("http://127.0.0.1/content"), Body::empty())
            .expect("request");
        let response = client.send(request, attempt.map(Duration::from_millis));
        tokio::pin!(response);
        assert!(futures::poll!(&mut response).is_pending());
        advance_time(&clock, 40_000).await;
        headers_ready.send(()).expect("header wait");
        let response = response
            .now_or_never()
            .expect("headers should arrive")
            .map_err(|failure| failure.error)
            .expect("response");
        let mut body = response.body;
        for (index, sender) in senders.into_iter().enumerate() {
            let chunk = body.frame();
            tokio::pin!(chunk);
            assert!(futures::poll!(&mut chunk).is_pending());
            if (configured.is_some() || attempt.is_some()) && index == 1 {
                advance_time(&clock, 10_000).await;
                let error = chunk
                    .now_or_never()
                    .expect("request should finish at its deadline")
                    .expect("body result")
                    .expect_err("total deadline");
                assert!(error.is_timeout());
                break;
            }
            advance_time(&clock, 40_000).await;
            sender
                .send(Bytes::from_static(b"one"))
                .expect("body reader");
            let frame = chunk
                .now_or_never()
                .expect("chunk should arrive")
                .expect("body result")
                .expect("progressing response");
            assert_eq!(frame.into_data().expect("data frame"), b"one".as_slice());
        }
        if configured.is_none() && attempt.is_none() {
            assert!(body
                .frame()
                .now_or_never()
                .expect("body should finish")
                .is_none());
            assert_eq!(clock.now_ms(), 160_000);
        }
    }
}
