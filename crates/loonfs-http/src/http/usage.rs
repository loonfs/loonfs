//! The usage report that the binding records once for each request to a
//! namespace route, and the recorder trait that embedders implement.

use crate::BindingState;
use axum::body::{Body, Bytes};
use axum::extract::{MatchedPath, RawPathParams, Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::RequestExt as _;
use http_body::{Frame, SizeHint};
use loonfs_types::NamespaceId;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// What one request used, and the namespace that the request ran against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageReport {
    /// The operation ID of the matched route, as the OpenAPI document spells it.
    pub operation: &'static str,
    /// The namespace that the request ran against.
    pub namespace_id: NamespaceId,
    /// The status of the response, or `None` when the request ended before the
    /// handler produced a response.
    pub status: Option<StatusCode>,
    /// The request body bytes that the handler read.
    pub request_bytes: u64,
    /// The response body bytes that the server wrote before the body ended or
    /// the client went away.
    pub response_bytes: u64,
    /// The bytes that the download grants in the response signed URLs for,
    /// zero for every other operation.
    pub grant_bytes: u64,
}

/// Embedders adapt this trait to their own usage accounting.
///
/// The binding calls it once for each request to a route with a `{namespace_id}`
/// parameter whose namespace ID parses: after the response body ended or the
/// client went away, from the task that drove the body, or when the request
/// ended before the handler produced a response, from the task that dropped
/// the request. An implementation returns quickly and never blocks; a host
/// that aggregates or ships reports does that on its own task.
pub trait UsageRecorder: Send + Sync + 'static {
    /// Records what one request used.
    fn record(&self, report: UsageReport);
}

/// The bytes that the signed URLs of a download grant response cover.
#[derive(Clone, Copy, Debug)]
pub(super) struct GrantedBytes(pub(super) u64);

/// The operation ID of each route with a `{namespace_id}` parameter, by
/// method and route template.
pub(super) const NAMESPACE_OPERATIONS: &[(Method, &str, &str)] = &[
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}",
        "get_namespace",
    ),
    (
        Method::DELETE,
        "/v0/namespaces/{namespace_id}",
        "delete_namespace",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/forks",
        "fork_namespace",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/snapshots",
        "create_snapshot",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/snapshots",
        "list_snapshots",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
        "extend_snapshot",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        "get_snapshot",
    ),
    (
        Method::DELETE,
        "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        "delete_snapshot",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/filesystem/entries",
        "list_path_entries",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/filesystem/entry",
        "get_path_entry",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/filesystem/content",
        "get_file_content",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/filesystem/downloads",
        "create_download",
    ),
    (Method::GET, "/v0/namespaces/{namespace_id}/grep", "grep"),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/filesystem/revisions",
        "list_file_revisions",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
        "get_inode",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        "list_inode_children",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        "get_file_content_by_inode",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
        "create_download_by_inode",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
        "list_file_revisions_by_inode",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        "get_file_revision_content_by_inode",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
        "create_revision_download_by_inode",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/filesystem/trash",
        "list_trash",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/commits",
        "create_commit",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/uploads",
        "create_upload",
    ),
    (
        Method::PUT,
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
        "put_upload_content",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
        "sign_upload_parts",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
        "complete_upload",
    ),
    (
        Method::POST,
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
        "abort_upload",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/uploads/{upload_id}",
        "get_upload",
    ),
    (
        Method::GET,
        "/v0/namespaces/{namespace_id}/changes",
        "list_changes",
    ),
    (
        Method::GET,
        "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
        "get_namespace_diagnostics",
    ),
    (
        Method::GET,
        "/v0/maintenance/namespaces/{namespace_id}/grep/index",
        "get_grep_index",
    ),
    (
        Method::POST,
        "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
        "enable_grep_index",
    ),
    (
        Method::POST,
        "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
        "disable_grep_index",
    ),
    (
        Method::GET,
        "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
        "list_checkpoints",
    ),
    (
        Method::GET,
        "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
        "get_checkpoint",
    ),
    (
        Method::POST,
        "/v0/maintenance/namespaces/{namespace_id}/runs",
        "run_maintenance",
    ),
];

/// Records one usage report for each request to a namespace route, once the
/// response body ends, the client goes away, or the request ends before its
/// response.
pub(super) async fn report_usage(
    State(state): State<BindingState>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(recorder) = state.usage_recorder else {
        return next.run(request).await;
    };
    let Some((operation, namespace_id)) = find_namespace_operation(&mut request).await else {
        return next.run(request).await;
    };
    let mut pending = PendingUsage {
        recorder,
        operation,
        namespace_id,
        status: None,
        request_bytes: Arc::new(AtomicU64::new(0)),
        response_bytes: 0,
        grant_bytes: 0,
    };
    let request = request.map(|body| {
        Body::new(CountedRequestBody {
            body,
            bytes: Arc::clone(&pending.request_bytes),
        })
    });
    let response = next.run(request).await;
    pending.status = Some(response.status());
    pending.grant_bytes = response
        .extensions()
        .get::<GrantedBytes>()
        .map_or(0, |GrantedBytes(bytes)| *bytes);
    response.map(|body| {
        Body::new(ReportedResponseBody {
            body,
            pending: Some(pending),
        })
    })
}

/// Finds the operation ID of the request's route and the namespace ID in its
/// path, for a namespace route whose namespace ID parses.
async fn find_namespace_operation(request: &mut Request) -> Option<(&'static str, NamespaceId)> {
    let route = request.extensions().get::<MatchedPath>()?.as_str();
    // axum serves a HEAD request through the route's GET handler.
    let method = match *request.method() {
        Method::HEAD => &Method::GET,
        ref method => method,
    };
    let (_, _, operation) = NAMESPACE_OPERATIONS
        .iter()
        .find(|(table_method, template, _)| table_method == method && *template == route)?;
    // The handler parses the percent-decoded path parameter, so the report
    // parses the same value. The raw path segment of an encoded namespace ID
    // doesn't parse, and the request would go unreported.
    let parameters = request.extract_parts::<RawPathParams>().await.ok()?;
    let (_, segment) = parameters
        .iter()
        .find(|(name, _)| *name == "namespace_id")?;
    Some((*operation, NamespaceId::parse(segment).ok()?))
}

/// A request body that adds the length of each data frame that the handler
/// reads to a shared count.
struct CountedRequestBody {
    body: Body,
    bytes: Arc<AtomicU64>,
}

impl http_body::Body for CountedRequestBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = ready!(Pin::new(&mut this.body).poll_frame(cx));
        if let Some(Ok(frame)) = &frame {
            if let Some(data) = frame.data_ref() {
                this.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
            }
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// A guard over one request's usage report that records the report when the
/// guard drops.
///
/// The guard exists from before the handler runs, so a request that ends
/// before its response, such as a stream reset during a proxied upload, still
/// records the bytes the handler read.
struct PendingUsage {
    recorder: Arc<dyn UsageRecorder>,
    operation: &'static str,
    namespace_id: NamespaceId,
    /// The status of the response, or `None` until the handler returns one.
    status: Option<StatusCode>,
    request_bytes: Arc<AtomicU64>,
    response_bytes: u64,
    grant_bytes: u64,
}

impl Drop for PendingUsage {
    fn drop(&mut self) {
        self.recorder.record(UsageReport {
            operation: self.operation,
            namespace_id: self.namespace_id.clone(),
            status: self.status,
            request_bytes: self.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes,
            grant_bytes: self.grant_bytes,
        });
    }
}

/// A response body that counts the data bytes it yields, and records the
/// request's usage report when the body ends, fails, or drops.
struct ReportedResponseBody {
    body: Body,
    /// The request's usage report, or `None` once the body ended and recorded
    /// the report.
    pending: Option<PendingUsage>,
}

impl http_body::Body for ReportedResponseBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = ready!(Pin::new(&mut this.body).poll_frame(cx));
        match &frame {
            Some(Ok(frame)) => {
                if let (Some(data), Some(pending)) = (frame.data_ref(), &mut this.pending) {
                    pending.response_bytes += data.len() as u64;
                }
            }
            // Dropping the guard records the report.
            Some(Err(_)) | None => this.pending = None,
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
