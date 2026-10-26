//! The usage report that the binding records once for each request to a
//! namespace route.

use super::direct_download::LoopbackIssuer;
use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::{BindingState, JobMode, UsageRecorder, UsageReport};
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use futures::StreamExt as _;
use http_body_util::BodyExt as _;
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{public_inode_id, NamespaceId};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tower::ServiceExt as _;

/// The size of `/file.txt`, larger than one response frame, so that a client
/// can read part of the content.
const CONTENT_BYTES: usize = 200 * 1024;

/// A usage recorder that keeps every report in memory.
#[derive(Default)]
struct RecordingUsage {
    reports: Mutex<Vec<UsageReport>>,
}

impl RecordingUsage {
    /// Removes and returns the reports recorded so far.
    fn take(&self) -> Vec<UsageReport> {
        std::mem::take(
            &mut *self
                .reports
                .lock()
                .expect("the usage report lock should not be poisoned"),
        )
    }
}

impl UsageRecorder for RecordingUsage {
    fn record(&self, report: UsageReport) {
        self.reports
            .lock()
            .expect("the usage report lock should not be poisoned")
            .push(report);
    }
}

struct Fixture {
    /// Keeps the store's directory until the test ends.
    _directory: TempDir,
    router: axum::Router,
    state: BindingState,
    usage: Arc<RecordingUsage>,
    namespace_id: NamespaceId,
    /// The inode ID of `/file.txt`, in its public form.
    file_inode: String,
}

/// Creates a namespace `demo` with `/file.txt`, behind a binding that records
/// usage and presigns object reads.
async fn fixture(name: &str) -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let usage = Arc::new(RecordingUsage::default());
    let mut options = test_options(directory.path(), name);
    options.grep = JobMode::Disabled;
    let (router, state) = test_app(
        options,
        TestAppOptions {
            direct_transfers: Some(DirectTransferIssuers {
                get: LoopbackIssuer::at("http://objects"),
                put: None,
                multipart: None,
            }),
            usage_recorder: Some(usage.clone()),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");
    let namespace_id = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    state
        .runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = state
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open namespace");
    namespace
        .put_file("/file.txt", &vec![b'x'; CONTENT_BYTES], &actor)
        .await
        .expect("put file");
    let file_inode = namespace.stat("/file.txt").await.expect("stat").inode_id;
    Fixture {
        _directory: directory,
        router,
        state,
        usage,
        namespace_id,
        file_inode: public_inode_id::encode(file_inode),
    }
}

impl Fixture {
    /// Sends `request` and reads the response body to the end.
    async fn send(&self, request: Request<Body>) -> (StatusCode, Bytes) {
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, body)
    }

    /// Returns the report of a request to `operation` with the given status and bytes.
    fn report(
        &self,
        operation: &'static str,
        status: StatusCode,
        request: &str,
        response: &[u8],
        grant_bytes: u64,
    ) -> UsageReport {
        UsageReport {
            operation,
            namespace_id: self.namespace_id.clone(),
            status: Some(status),
            request_bytes: request.len() as u64,
            response_bytes: response.len() as u64,
            grant_bytes,
        }
    }

    async fn shutdown(self) {
        self.state.runtime.shutdown().await.expect("shutdown");
    }
}

/// Builds an authorized request with a JSON body.
fn request(method: &str, uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token")
        .header("loonfs-actor", "usage-test")
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .expect("request")
}

#[tokio::test]
async fn namespace_requests_report_their_operation_status_and_bytes() {
    let fixture = fixture("usage-requests").await;
    let commit = json!({
        "commit_id": "usage",
        "operations": [{"kind": "put_file", "path": "/inline.txt", "inline_content": "c2FtZQ=="}],
    })
    .to_string();
    let cases = [
        ("GET", "/v0/namespaces/demo", String::new(), "get_namespace"),
        (
            "HEAD",
            "/v0/namespaces/demo",
            String::new(),
            "get_namespace",
        ),
        (
            "POST",
            "/v0/namespaces/demo/commits",
            commit,
            "create_commit",
        ),
        // The router decodes `%64emo` to `demo` before the handler parses
        // the namespace ID.
        (
            "GET",
            "/v0/namespaces/%64emo",
            String::new(),
            "get_namespace",
        ),
    ];
    for (method, uri, body, operation) in cases {
        let (status, response) = fixture.send(request(method, uri, &body)).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {response:?}");
        assert_eq!(
            fixture.usage.take(),
            [fixture.report(operation, status, &body, &response, 0)],
            "{uri}"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn content_reads_report_the_content_length() {
    let fixture = fixture("usage-content").await;
    let file = fixture.file_inode.clone();
    for (uri, operation) in [
        (
            "/v0/namespaces/demo/filesystem/content?path=/file.txt".to_owned(),
            "get_file_content",
        ),
        (
            format!("/v0/namespaces/demo/inodes/{file}/content"),
            "get_file_content_by_inode",
        ),
        (
            format!("/v0/namespaces/demo/inodes/{file}/revisions/1/content"),
            "get_file_revision_content_by_inode",
        ),
    ] {
        let (status, response) = fixture.send(request("GET", &uri, "")).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(response.len(), CONTENT_BYTES, "{uri}");
        assert_eq!(
            fixture.usage.take(),
            [fixture.report(operation, status, "", &response, 0)],
            "{uri}"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn download_grants_report_the_bytes_of_their_ranges() {
    let fixture = fixture("usage-downloads").await;
    let file = fixture.file_inode.clone();
    for (uri, body, operation) in [
        (
            "/v0/namespaces/demo/filesystem/downloads".to_owned(),
            json!({"path": "/file.txt"}),
            "create_download",
        ),
        (
            format!("/v0/namespaces/demo/inodes/{file}/downloads"),
            json!({}),
            "create_download_by_inode",
        ),
        (
            format!("/v0/namespaces/demo/inodes/{file}/revisions/1/downloads"),
            json!({"start_offset": 1000}),
            "create_revision_download_by_inode",
        ),
    ] {
        let body = body.to_string();
        let (status, response) = fixture.send(request("POST", &uri, &body)).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {response:?}");
        let grant: Value = serde_json::from_slice(&response).expect("grant");
        let grant_bytes = grant["ranges"]
            .as_array()
            .expect("ranges")
            .iter()
            .map(|range| range["length"].as_u64().expect("range length"))
            .sum();
        let start_offset = grant["ranges"][0]["start_offset"]
            .as_u64()
            .expect("start offset");
        assert_eq!(grant_bytes, CONTENT_BYTES as u64 - start_offset, "{uri}");
        assert_eq!(
            fixture.usage.take(),
            [fixture.report(operation, status, &body, &response, grant_bytes)],
            "{uri}"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn requests_outside_namespace_routes_report_nothing() {
    let fixture = fixture("usage-outside").await;
    for (method, uri, body, expected) in [
        ("GET", "/v0/capabilities", "", StatusCode::OK),
        (
            "POST",
            "/v0/namespaces",
            r#"{"namespace_id":"other"}"#,
            StatusCode::OK,
        ),
        ("GET", "/v0/unknown", "", StatusCode::NOT_FOUND),
        ("GET", "/v0/namespaces/Demo", "", StatusCode::BAD_REQUEST),
        (
            "DELETE",
            "/v0/namespaces/demo/changes",
            "",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ] {
        let (status, response) = fixture.send(request(method, uri, body)).await;
        assert_eq!(status, expected, "{method} {uri}: {response:?}");
        assert_eq!(fixture.usage.take(), [], "{method} {uri}");
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_client_that_goes_away_still_produces_a_report() {
    let fixture = fixture("usage-abandoned").await;
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            "/v0/namespaces/demo/filesystem/content?path=/file.txt",
            "",
        ))
        .await
        .expect("response");
    let status = response.status();
    assert_eq!(status, StatusCode::OK);
    let mut body = response.into_body();
    let first = body
        .frame()
        .await
        .expect("frame")
        .expect("data")
        .into_data()
        .expect("data frame");
    assert_eq!(fixture.usage.take(), []);
    drop(body);
    assert!(first.len() < CONTENT_BYTES);
    assert_eq!(
        fixture.usage.take(),
        [fixture.report("get_file_content", status, "", &first, 0)]
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_request_canceled_before_its_response_still_produces_a_report() {
    let fixture = fixture("usage-canceled").await;
    let (status, response) = fixture
        .send(request(
            "POST",
            "/v0/namespaces/demo/uploads",
            r#"{"mode":"service_proxied"}"#,
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    let upload: Value = serde_json::from_slice(&response).expect("upload session");
    assert_eq!(fixture.usage.take().len(), 1);
    let chunk = Bytes::from_static(b"the first chunk of a longer upload");
    let (wants_more, handler_wants_more) = tokio::sync::oneshot::channel();
    // The handler polls for the second item only after the counting body
    // returned the first chunk, so the signal means the chunk is counted.
    // The second item never arrives, so the request stays in the handler.
    let body = futures::stream::once(std::future::ready(Ok::<_, std::io::Error>(chunk.clone())))
        .chain(futures::stream::once(async move {
            wants_more.send(()).expect("the test waits for the signal");
            std::future::pending::<Result<Bytes, std::io::Error>>().await
        }));
    let upload_request = Request::builder()
        .method("PUT")
        .uri(format!(
            "/v0/namespaces/demo/uploads/{}/content",
            upload["upload_id"].as_str().expect("upload id")
        ))
        .header("authorization", "Bearer test-token")
        .body(Body::from_stream(body))
        .expect("upload request");
    let task = tokio::spawn(fixture.router.clone().oneshot(upload_request));
    handler_wants_more
        .await
        .expect("the handler reads past the first chunk");
    assert_eq!(fixture.usage.take(), []);
    task.abort();
    assert!(task
        .await
        .expect_err("the request is canceled")
        .is_cancelled());
    assert_eq!(
        fixture.usage.take(),
        [UsageReport {
            operation: "put_upload_content",
            namespace_id: fixture.namespace_id.clone(),
            status: None,
            request_bytes: chunk.len() as u64,
            response_bytes: 0,
            grant_bytes: 0,
        }]
    );
    fixture.shutdown().await;
}

#[cfg(feature = "openapi")]
#[test]
fn usage_operations_match_the_openapi_document() {
    use super::super::usage::NAMESPACE_OPERATIONS;
    use std::collections::BTreeSet;

    let document =
        serde_json::to_value(crate::openapi_document()).expect("serialize the OpenAPI document");
    let documented: BTreeSet<(String, String, String)> = document["paths"]
        .as_object()
        .expect("OpenAPI paths object")
        .iter()
        .filter(|(path, _)| path.contains("{namespace_id}"))
        .flat_map(|(path, item)| {
            item.as_object()
                .expect("OpenAPI path item")
                .iter()
                .filter_map(move |(method, operation)| {
                    let operation = operation.get("operationId")?.as_str()?;
                    Some((method.to_uppercase(), path.clone(), operation.to_owned()))
                })
        })
        .collect();
    let table: BTreeSet<(String, String, String)> = NAMESPACE_OPERATIONS
        .iter()
        .map(|(method, route, operation)| {
            (
                method.as_str().to_owned(),
                (*route).to_owned(),
                (*operation).to_owned(),
            )
        })
        .collect();
    assert_eq!(table.len(), NAMESPACE_OPERATIONS.len());
    assert_eq!(table, documented);
}
