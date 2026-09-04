//! Request-outcome logging at the HTTP boundary.
//!
//! This is a separate binary because its global subscriber would leak across
//! the shared `it` harness and capture unrelated tests.

#[path = "it/common/mod.rs"]
mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request};
use axum::Router;
use bytes::Bytes;
use common::http_split_support::test_config;
use loonfs::{CreateNamespaceOptions, FsWriter, SharedObjectStore, TraceMode, TraceStoreKind};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_server::{app, AppOptions, MaintenanceMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationClass};
use std::convert::Infallible;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use tower::ServiceExt as _;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};

const COMPLETION_TARGET: &str = "loonfs_server::http::request";
const COMPLETION_MESSAGE: &str = "request completed";

#[derive(Clone, Debug)]
struct CapturedEvent {
    level: Level,
    target: String,
    message: Option<String>,
    request_id: Option<String>,
    method: Option<String>,
    route: Option<String>,
    status: Option<u64>,
    elapsed_ms: Option<u64>,
}

#[derive(Clone)]
struct Capture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
    test_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Capture {
    fn clear(&self) {
        self.events.lock().expect("capture lock").clear();
    }

    fn snapshot(&self) -> Vec<CapturedEvent> {
        self.events.lock().expect("capture lock").clone()
    }
}

struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = CapturedFields::default();
        event.record(&mut fields);
        self.events
            .lock()
            .expect("capture lock")
            .push(CapturedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                message: fields.message,
                request_id: fields.request_id,
                method: fields.method,
                route: fields.route,
                status: fields.status,
                elapsed_ms: fields.elapsed_ms,
            });
    }
}

#[derive(Default)]
struct CapturedFields {
    message: Option<String>,
    request_id: Option<String>,
    method: Option<String>,
    route: Option<String>,
    status: Option<u64>,
    elapsed_ms: Option<u64>,
}

impl CapturedFields {
    fn record(&mut self, field: &Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "request_id" => self.request_id = Some(value),
            "method" => self.method = Some(value),
            "route" => self.route = Some(value),
            _ => {}
        }
    }
}

impl Visit for CapturedFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{value:?}").trim_matches('"').to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "status" => self.status = Some(value),
            "elapsed_ms" => self.elapsed_ms = Some(value),
            _ => {}
        }
    }
}

fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            events: events.clone(),
        });
        tracing::subscriber::set_global_default(subscriber).expect("install capture subscriber");
        Capture {
            events,
            test_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    })
}

async fn request_error(router: &Router, uri: &str) -> (u16, loonfs_api::ApiError, String) {
    request_error_with(
        router,
        Method::GET,
        uri,
        Some("Bearer test-token"),
        Body::empty(),
    )
    .await
}

async fn request_error_with(
    router: &Router,
    method: Method,
    uri: &str,
    authorization: Option<&str>,
    body: Body,
) -> (u16, loonfs_api::ApiError, String) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization);
    }
    let response = router
        .clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("route request");
    let status = response.status().as_u16();
    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("request-id response header")
        .to_str()
        .expect("request-id response header value")
        .to_owned();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read API error body");
    let body = serde_json::from_slice::<loonfs_api::ApiError>(&bytes).expect("API error body");
    assert_eq!(body.request_id.as_deref(), Some(request_id.as_str()));
    (status, body, request_id)
}

fn assert_completion_fields(
    event: &CapturedEvent,
    request_id: &str,
    method: &str,
    route: &str,
    status: u64,
) {
    assert_eq!(event.request_id.as_deref(), Some(request_id));
    assert_eq!(event.method.as_deref(), Some(method));
    assert_eq!(event.route.as_deref(), Some(route));
    assert_eq!(event.status, Some(status));
    assert!(event.elapsed_ms.is_some(), "missing elapsed_ms: {event:#?}");
}

fn completion_events<'a>(events: &'a [CapturedEvent], request_id: &str) -> Vec<&'a CapturedEvent> {
    events
        .iter()
        .filter(|event| {
            event.target == COMPLETION_TARGET
                && event.message.as_deref() == Some(COMPLETION_MESSAGE)
                && event.request_id.as_deref() == Some(request_id)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_path_has_one_debug_completion_and_no_errors() {
    let capture = capture();
    let _test_guard = capture.test_lock.lock().await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "logging-missing-path",
        "logging-missing-path",
    );
    config.maintenance = MaintenanceMode::ServeOnly;
    let (router, state) = app(config, AppOptions::default()).await.expect("build app");
    let writer = state.writer;
    writer
        .create_namespace(&namespace_id("demo"), CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    capture.clear();

    let (status, body, request_id) = request_error(
        &router,
        "/v0/namespaces/demo/filesystem/entry?path=%2Fmissing.txt",
    )
    .await;
    assert_eq!(status, 404);
    assert_eq!(body.code, "path_not_found");

    let events = capture.snapshot();
    assert!(
        events.iter().all(|event| event.level != Level::ERROR),
        "missing path emitted ERROR events: {events:#?}"
    );
    let completions = completion_events(&events, &request_id);
    assert_eq!(
        completions.len(),
        1,
        "expected one completion event: {events:#?}"
    );
    assert_eq!(completions[0].level, Level::DEBUG);

    writer.shutdown().await.expect("shutdown writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expected_typed_errors_use_debug_or_warn_and_keep_completion_fields() {
    let capture = capture();
    let _test_guard = capture.test_lock.lock().await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut config = test_config(
        temp_dir.path().join("store"),
        "logging-expected-errors",
        "logging-expected-errors",
    );
    config.maintenance = MaintenanceMode::ServeOnly;
    config.max_concurrent_uploads = 1;
    let (router, state) = app(config, AppOptions::default()).await.expect("build app");
    let writer = state.writer;
    let namespace = namespace_id("demo");
    writer
        .create_namespace(&namespace, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let upload = writer
        .create_upload(&namespace)
        .await
        .expect("begin upload");

    capture.clear();
    let (status, body, request_id) =
        request_error(&router, "/v0/namespaces/demo/grep?pattern=x").await;
    assert_eq!(status, 501);
    assert_eq!(body.code, "not_supported");
    let events = capture.snapshot();
    assert!(
        events.iter().all(|event| event.level != Level::ERROR),
        "not_supported emitted ERROR events: {events:#?}"
    );
    let completions = completion_events(&events, &request_id);
    assert_eq!(completions.len(), 1, "completion events: {events:#?}");
    assert_eq!(completions[0].level, Level::DEBUG);
    assert_completion_fields(
        completions[0],
        &request_id,
        "GET",
        "/v0/namespaces/{namespace_id}/grep",
        501,
    );

    capture.clear();
    let (status, body, request_id) = request_error_with(
        &router,
        Method::GET,
        "/v0/namespaces/demo",
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(status, 401);
    assert_eq!(body.code, "unauthorized");
    let events = capture.snapshot();
    let completions = completion_events(&events, &request_id);
    assert_eq!(completions.len(), 1, "completion events: {events:#?}");
    assert_eq!(completions[0].level, Level::WARN);
    assert_completion_fields(
        completions[0],
        &request_id,
        "GET",
        "/v0/namespaces/{namespace_id}",
        401,
    );

    let (body_polled_tx, body_polled_rx) = tokio::sync::oneshot::channel();
    let stalled_body = Body::from_stream(futures::stream::once(async move {
        let _ = body_polled_tx.send(());
        std::future::pending::<Result<Bytes, Infallible>>().await
    }));
    let upload_uri = format!("/v0/namespaces/demo/uploads/{}/content", upload.upload_id());
    let stalled_request = Request::builder()
        .method(Method::PUT)
        .uri(&upload_uri)
        .header("authorization", "Bearer test-token")
        .body(stalled_body)
        .expect("stalled upload request");
    let stalled_upload = tokio::spawn(router.clone().oneshot(stalled_request));
    tokio::time::timeout(std::time::Duration::from_secs(5), body_polled_rx)
        .await
        .expect("stalled body should be polled after taking the upload permit")
        .expect("body poll notification");

    capture.clear();
    let (status, body, request_id) = request_error_with(
        &router,
        Method::PUT,
        &upload_uri,
        Some("Bearer test-token"),
        Body::empty(),
    )
    .await;
    assert_eq!(status, 503);
    assert_eq!(body.code, "server_busy");
    let events = capture.snapshot();
    let completions = completion_events(&events, &request_id);
    assert_eq!(completions.len(), 1, "completion events: {events:#?}");
    assert_eq!(completions[0].level, Level::WARN);
    assert_completion_fields(
        completions[0],
        &request_id,
        "PUT",
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
        503,
    );

    stalled_upload.abort();
    let _ = stalled_upload.await;
    writer.shutdown().await.expect("shutdown writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_fault_has_one_error_from_the_boundary() {
    let capture = capture();
    let _test_guard = capture.test_lock.lock().await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let key_prefix = "logging-store-fault";
    let mut config = test_config(store_root.clone(), "logging-store-fault", key_prefix);
    config.maintenance = MaintenanceMode::ServeOnly;

    let failing = Arc::new(FailStore::new(
        LocalFsStore::with_key_prefix(&store_root, Some(key_prefix)).expect("build local store"),
        KeyPredicate::wal_head(
            &loonfs_api::NamespaceId::parse("faulty").expect("valid namespace id"),
        ),
        OperationClass::Read,
        InjectedError::Transport("injected WAL-head read failure".to_owned()),
    ));
    let store: SharedObjectStore = failing.clone();
    let bootstrap = FsWriter::builder_with_store(store.clone())
        .writer_id("logging-store-fault-bootstrap")
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build bootstrap writer");
    bootstrap
        .create_namespace(&namespace_id("faulty"), CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    bootstrap
        .shutdown()
        .await
        .expect("shutdown bootstrap writer");

    failing.fail_all();
    let router = app(
        config,
        AppOptions {
            store: Some(store),
            direct_transfers: None,
        },
    )
    .await
    .expect("build app")
    .0;
    capture.clear();
    let (status, body, request_id) = request_error(&router, "/v0/namespaces/faulty").await;
    assert_eq!(status, 500);
    assert_eq!(body.code, "server_error");

    let events = capture.snapshot();
    let errors: Vec<_> = events
        .iter()
        .filter(|event| event.level == Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "expected one ERROR event: {events:#?}");
    assert_eq!(errors[0].target, COMPLETION_TARGET);
    assert_eq!(errors[0].message.as_deref(), Some(COMPLETION_MESSAGE));
    assert_eq!(errors[0].request_id.as_deref(), Some(request_id.as_str()));
    assert_eq!(completion_events(&events, &request_id).len(), 1);
    assert_completion_fields(
        errors[0],
        &request_id,
        "GET",
        "/v0/namespaces/{namespace_id}",
        500,
    );

    drop(router);
}
