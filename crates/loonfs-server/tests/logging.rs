//! Request-outcome logging at the HTTP boundary.
//!
//! This is a separate binary because its global subscriber would leak across
//! the shared `it` harness and capture unrelated tests.

#[path = "it/common/mod.rs"]
mod common;

use axum::body::{to_bytes, Body};
use axum::http::Request;
use axum::Router;
use common::http_split_support::test_config;
use loonfs::{CreateNamespaceOptions, FsWriter, SharedObjectStore, TraceMode, TraceStoreKind};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_server::{app, app_with_store, MaintenanceMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationClass};
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
            });
    }
}

#[derive(Default)]
struct CapturedFields {
    message: Option<String>,
    request_id: Option<String>,
}

impl CapturedFields {
    fn record(&mut self, field: &Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "request_id" => self.request_id = Some(value),
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
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .expect("request"),
        )
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
    config.maintenance = MaintenanceMode::Manual;
    let (router, writer, _local_cache) = app(config).await.expect("build app");
    writer
        .create_namespace(&namespace_id("demo"), CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    capture.clear();

    let (status, body, request_id) = request_error(
        &router,
        "/v0/namespaces/demo/filesystem/stat?path=%2Fmissing.txt",
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
async fn store_fault_has_one_error_from_the_boundary() {
    let capture = capture();
    let _test_guard = capture.test_lock.lock().await;
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let key_prefix = "logging-store-fault";
    let mut config = test_config(store_root.clone(), "logging-store-fault", key_prefix);
    config.maintenance = MaintenanceMode::Manual;

    let failing = Arc::new(FailStore::new(
        LocalFsStore::with_key_prefix(&store_root, Some(key_prefix)).expect("build local store"),
        KeyPredicate::wal_head("faulty"),
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
    let router = app_with_store(config, store).await.expect("build app");
    capture.clear();
    let (status, _body, request_id) = request_error(&router, "/v0/namespaces/faulty").await;
    assert_eq!(status, 500);

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

    drop(router);
}
