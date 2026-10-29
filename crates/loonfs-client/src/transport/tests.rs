//! Transport retry, identity, streaming, and timeout contracts.

use super::*;
use crate::namespace_path::NamespacePath;
use crate::operations::Operation;
use crate::{scripted_transport, ClientConfig};
use crate::{CommitOptions, CreateDirectoryOptions, DeleteNamespaceOptions};
use futures::FutureExt as _;
use loonfs_test_support::clock::ManualClock;
use loonfs_types::ChangeSeq;
use loonfs_types::Commit;
use loonfs_types::CommitId;
use loonfs_types::CompleteUploadBody;
use loonfs_types::ContentId;
use loonfs_types::ContentRef;
use loonfs_types::CreateUploadBody;
use loonfs_types::DeleteSnapshotResponse;
use loonfs_types::NamespaceDiagnostics;
use loonfs_types::NamespaceMetadata;
use loonfs_types::ObjectTransferAccess;
use loonfs_types::PinId;
use loonfs_types::UploadId;
use loonfs_types::UploadSession;
use loonfs_types::UploadSessionStatus;
use std::assert_matches;
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
        initial_backoff_ms: 0,
        max_backoff_ms: 0,
        operation_deadline_ms: 100,
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

/// Checks that the client retries the codes that clear on their own, and sends a request that
/// fails with `store_contention` once, because the server already retried its compare-and-swap.
#[tokio::test]
async fn store_contention_is_not_retried_by_the_client() {
    let namespace_id = loonfs_types::NamespaceId::parse("demo").expect("namespace id");
    let upload_id =
        loonfs_types::UploadId::parse("upl_00000000000000000000000001").expect("upload id");
    for (code, attempts) in [
        (ErrorCode::CommitQueueFull, 4),
        (ErrorCode::ServerBusy, 4),
        (ErrorCode::ShuttingDown, 4),
        (ErrorCode::StoreContention, 1),
    ] {
        let transport = scripted_transport::refusals(code, 4);
        let mut client = deadline_client(&transport);
        client.transport_retry = TransportRetryPolicy {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            ..DEFAULT
        };
        client.timer = Arc::new(StdMonotonicTimer::default());
        let error = client
            .abort_upload(&namespace_id, &upload_id)
            .await
            .expect_err("every attempt is refused");
        assert_eq!(error.code(), Some(code));
        assert_eq!(transport.attempts(), attempts, "{code}");
    }
}

#[tokio::test]
async fn transport_retries_stop_when_the_operation_deadline_is_spent() {
    let transport = scripted_transport::failures(1);
    let client = deadline_client(&transport);

    client
        .call(&client.request(
            Operation::retry(Method::GET),
            "http://example.invalid/control",
        ))
        .await
        .expect_err("the spent deadline must preserve the first failure");

    assert_eq!(transport.attempts(), 1);
}

/// Checks that a content read retries without the operation deadline, both
/// buffered and streamed, and that a streamed read resends until its response
/// head arrives.
#[tokio::test]
async fn content_transfer_retries_are_exempt_from_the_operation_deadline() {
    let transport = scripted_transport::failure_then_success(b"content".to_vec());
    let client = deadline_client(&transport);
    let content = client.request(
        Operation::retry_without_deadline(Method::GET),
        "http://example.invalid/content",
    );

    let response = client
        .call(&content)
        .await
        .expect("the count-bounded content retry succeeds");
    assert_eq!(response.bytes, b"content");
    assert_eq!(transport.attempts(), 2);

    let transport = scripted_transport::failure_then_success(b"content".to_vec());
    let client = deadline_client(&transport);
    let mut stream = client
        .call_for_response_stream(&content)
        .await
        .expect("the streamed read resends until a response head arrives");
    let mut streamed = Vec::new();
    while let Some(chunk) = stream.next().await {
        streamed.extend_from_slice(&chunk.expect("a body chunk"));
    }
    assert_eq!(streamed, b"content");
    assert_eq!(transport.attempts(), 2);
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
            request_timeout_ms: request_timeout_ms.and_then(std::num::NonZeroU64::new),
            disable_transient_retry: true,
            ca_cert_path: None,
        },
        service,
    )
    .expect("valid config")
}

#[tokio::test]
async fn services_receive_identical_body_and_identity_on_retries_but_not_provider_requests() {
    use loonfs_types::{PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId};
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
    client.transport_retry.initial_backoff_ms = 0;
    client.transport_retry.max_backoff_ms = 0;
    let request = client
        .request(Operation::retry(Method::POST), "http://127.0.0.1/commits")
        .header("Loonfs-Actor", "actor")
        .json(&serde_json::json!({"key": "value"}))
        .expect("serialize the body");
    let _: serde_json::Value = client
        .send_json(request)
        .await
        .expect("body failure retried");
    client
        .call(&WireRequest::presigned(
            Method::GET,
            "https://provider.invalid/object?signature=opaque",
            SendPolicy::Once,
        ))
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
        let request = client.request(
            Operation::retry_without_deadline(Method::PUT),
            "http://127.0.0.1/content",
        );
        let upload = client.call_streamed_once(&request, futures::stream::pending().boxed(), None);
        tokio::pin!(upload);
        assert!(futures::poll!(&mut upload).is_pending());
        advance_time(&clock, elapsed_ms).await;
        let error = upload
            .now_or_never()
            .expect("request should finish")
            .expect_err("upload timeout");
        assert_matches!(error, ClientError::Http(message) if message.contains("timed out"));
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
        assert_matches!(error, ClientError::Http(message) if message.contains("timed out"));

        let service = tower::service_fn(|_: Request<Body>| async {
            Ok(Response::new(Body::from_stream(
                futures::stream::pending::<std::io::Result<Bytes>>(),
            )))
        });
        let (client, clock) = timed_client(service, timeout);
        let mut stream = client
            .call_for_response_stream(&client.request(
                Operation::retry_without_deadline(Method::GET),
                "http://127.0.0.1/content",
            ))
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
        assert_matches!(error, ClientError::Http(message) if message.contains("timed out"));
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
        let request = client.request(
            Operation::retry_without_deadline(Method::PUT),
            "http://127.0.0.1/content",
        );
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
                assert_matches!(
                    error,
                    ClientError::Http(message) if message.contains("timed out")
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
            .build(
                &client.request(Operation::retry(Method::GET), "http://127.0.0.1/content"),
                Body::empty(),
            )
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

fn test_content_ref(bytes: &[u8]) -> ContentRef {
    ContentRef::blob_v1(
        loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        bytes,
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    )
}

#[test]
fn retryable_transport_failure_covers_transport_and_unavailability_only() {
    let api = |code: &str| ClientError::Api {
        status: 503,
        response: Box::new(ErrorResponse {
            code: code.to_owned(),
            feature: None,
            message: String::new(),
            param: None,
            request_id: None,
            details: None,
        }),
    };
    assert!(retryable_transport_failure(
        FailureSource::Network,
        &ClientError::Http("reset".to_owned())
    ));
    assert!(retryable_transport_failure(
        FailureSource::Response,
        &api("server_busy")
    ));
    assert!(retryable_transport_failure(
        FailureSource::Response,
        &api("commit_queue_full")
    ));
    assert!(retryable_transport_failure(
        FailureSource::Response,
        &api("shutting_down")
    ));
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &api("server_error")
    ));
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &api("checkpoint_unavailable")
    ));
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &api("maintenance_required")
    ));
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &api("index_lagging")
    ));
    // A server deadline uses 503 but is not safe to retry automatically.
    // The caller must first determine whether a mutation completed.
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &api("deadline_exceeded")
    ));
    assert!(!retryable_transport_failure(
        FailureSource::Response,
        &ClientError::Http("http status 502 with a non-envelope body".to_owned())
    ));
}

#[tokio::test]
async fn transport_failures_resend_up_to_the_attempt_cap() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let max_attempts = DEFAULT.max_retries as usize + 1;
    let transport = crate::scripted_transport::failures(max_attempts);
    let retrying = Client::with_transport(
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
    let error = retrying
        .get_namespace(&namespace_id)
        .await
        .expect_err("dropped connections must fail");
    assert_matches!(error, ClientError::Http(_));
    assert_eq!(transport.attempts(), max_attempts);

    let transport = crate::scripted_transport::failures(1);
    let single_shot = Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
            ca_cert_path: None,
        },
        transport.clone(),
    )
    .expect("valid client config");
    single_shot
        .get_namespace(&namespace_id)
        .await
        .expect_err("dropped connection must fail without retry");
    assert_eq!(transport.attempts(), 1);
}

pub(crate) fn retry_policy_client(
    transport: &crate::scripted_transport::ScriptedTransport,
) -> Client {
    Client::with_transport(
        ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        },
        transport.clone(),
    )
    .expect("valid client config")
}

fn single_attempt_probe() -> (crate::scripted_transport::ScriptedTransport, Client) {
    let transport = crate::scripted_transport::failure_then_success(b"{}".to_vec());
    let client = retry_policy_client(&transport);
    (transport, client)
}

fn assert_single_attempt<T>(
    result: Result<T>,
    transport: &crate::scripted_transport::ScriptedTransport,
) {
    assert!(
        matches!(result, Err(ClientError::Http(_))),
        "expected the first transport failure to surface"
    );
    assert_eq!(transport.attempts(), 1);
}

#[tokio::test]
async fn retry_policy_lifecycle_mutations_are_single_attempt() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let fork_id = NamespaceId::parse("fork").expect("valid id");

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await,
        &transport,
    );

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .fork_namespace(&namespace_id, &fork_id, &loonfs_test_support::test_actor())
            .await,
        &transport,
    );

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .delete_namespace_with_options(
                &namespace_id,
                &DeleteNamespaceOptions {
                    expected_head_seq: Some(ChangeSeq(7)),
                },
            )
            .await,
        &transport,
    );
}

#[tokio::test]
async fn retry_policy_matches_snapshot_and_maintenance_operation_classes() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .create_snapshot_with_options(
                &namespace_id,
                "backup",
                &crate::CreateSnapshotOptions {
                    ttl_ms: std::num::NonZeroU64::new(10_000),
                },
            )
            .await,
        &transport,
    );

    let snapshot_id =
        PinId::parse("pin_00000000000000000001-0000000000000001").expect("valid pin id");
    let deleted = DeleteSnapshotResponse {
        namespace_id: namespace_id.clone(),
        snapshot_id: snapshot_id.clone(),
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&deleted).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);
    let actual = client
        .delete_snapshot(&namespace_id, &snapshot_id)
        .await
        .expect("an idempotent snapshot delete should retry");
    assert_eq!(actual, deleted);
    assert_eq!(transport.attempts(), 2);

    let response = NamespaceDiagnostics {
        created_at_ms: 1_000,
        created_by: loonfs_test_support::test_actor(),
        fork_basis: None,
        namespace_id: namespace_id.clone(),
        head_seq: ChangeSeq(3),
        retention_floor_seq: ChangeSeq(1),
        acknowledged_floor_seq: ChangeSeq(1),
        current_manifest_no: None,
        wal_tail_objects: 2,
        live_snapshots: 3,
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&response).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);

    let actual = client
        .get_namespace_diagnostics(&namespace_id)
        .await
        .expect("safe maintenance read should retry");
    assert_eq!(actual, response);
    assert_eq!(transport.attempts(), 2);
}

#[tokio::test]
async fn retry_policy_commit_id_filesystem_mutation_retries() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let commit_id = CommitId::parse("c_00000000000000000000000000000001").expect("valid commit id");
    let response = Commit {
        namespace_id: namespace_id.clone(),
        commit_id: commit_id.clone(),
        committed_seq: ChangeSeq(1),
        committed_by: loonfs_test_support::test_actor(),
        committed_at_ms: 1_752_624_000_000,
        message: None,
        events: Vec::new(),
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&response).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);
    let spec = NamespacePath::parse("demo", "/docs").expect("valid namespace path");

    let actual = client
        .create_directory_with_options(
            &spec,
            &loonfs_test_support::test_actor(),
            &CreateDirectoryOptions {
                commit: CommitOptions {
                    commit_id: Some(commit_id),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("commit-id mutation should retry");
    assert_eq!(actual, response);
    assert_eq!(transport.attempts(), 2);
}

#[tokio::test]
async fn retry_policy_read_retries() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let response = NamespaceMetadata {
        access: loonfs_types::NamespaceAccessMode::Unrestricted {},
        created_at_ms: 1_000,
        created_by: loonfs_test_support::test_actor(),
        fork_basis: None,
        namespace_id: namespace_id.clone(),
        head_seq: ChangeSeq(0),
        retention_floor_seq: ChangeSeq(0),
        retention_window_ms: None,
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&response).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);

    let actual = client
        .get_namespace(&namespace_id)
        .await
        .expect("read should retry");
    assert_eq!(actual, response);
    assert_eq!(transport.attempts(), 2);
}

#[tokio::test]
async fn retry_policy_upload_begins_are_single_attempt() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .create_upload(&namespace_id, &CreateUploadBody::ServiceProxied {})
            .await,
        &transport,
    );

    let (transport, client) = single_attempt_probe();
    assert_single_attempt(
        client
            .create_direct_put_upload_target(&namespace_id, Some(b"direct".len() as u64))
            .await,
        &transport,
    );
}

#[tokio::test]
async fn retry_policy_presigned_upload_is_single_attempt() {
    let transport = crate::scripted_transport::failure_then_success(Vec::new());
    let client = retry_policy_client(&transport);
    let access = ObjectTransferAccess::PresignedUrl {
        method: "PUT".to_owned(),
        url: "http://example.invalid/upload".to_owned(),
        headers: std::collections::BTreeMap::new(),
        expires_at_ms: 1,
    };

    let result = client.upload_via_presigned_url(&access, b"direct").await;

    assert_matches!(result, Err(ClientError::Http(_)));
    assert_eq!(transport.attempts(), 1);
}

#[tokio::test]
async fn retry_policy_proxied_upload_content_retries() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let upload_id = UploadId::parse("upl_00000000000000000000000001").expect("valid upload id");
    let response = UploadSession {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: loonfs_types::UploadMode::ServiceProxied,
        status: UploadSessionStatus::Open {
            expires_at_ms: 1000,
            checksum_algorithm: loonfs_types::ChecksumAlgorithm::Crc64nvme,
            part_size_bytes: None,
            access: None,
            content_ref: Some(test_content_ref(b"content")),
        },
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&response).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);

    let actual = client
        .put_upload_content(&namespace_id, &upload_id, b"content")
        .await
        .expect("identical content staging should retry");
    assert_eq!(actual, response);
    assert_eq!(transport.attempts(), 2);
}

#[tokio::test]
async fn retry_policy_upload_completion_retries() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let upload_id = UploadId::parse("upl_00000000000000000000000001").expect("valid upload id");
    let content_ref = test_content_ref(b"content");
    let response = UploadSession {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: loonfs_types::UploadMode::ServiceProxied,
        status: UploadSessionStatus::Completed {
            completed_at_ms: 1,
            content_ref: content_ref.clone(),
            content_token: None,
        },
    };
    let transport = crate::scripted_transport::failure_then_success(
        serde_json::to_vec(&response).expect("serialize response"),
    );
    let client = retry_policy_client(&transport);

    let actual = client
        .complete_upload(
            &namespace_id,
            &upload_id,
            &CompleteUploadBody::ServiceProxied {},
        )
        .await
        .expect("completed-session replay should retry");
    assert_eq!(actual, response);
    assert_eq!(transport.attempts(), 2);
}
