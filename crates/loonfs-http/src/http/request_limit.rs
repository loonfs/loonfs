//! Request admission before authentication and body extraction.

use super::error::ApiResponseError;
use super::extractors::server_busy_error;
use crate::BindingState;
use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http_body::Body as _;
use loonfs::ErrorCode;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The count of admitted requests, each holding one permit until its
/// response's last frame is sent, and the drain that waits for them.
#[derive(Clone)]
pub struct RequestLimit {
    capacity: u32,
    permits: Arc<Semaphore>,
    draining: Arc<AtomicBool>,
}

impl RequestLimit {
    pub fn new(capacity: NonZeroUsize) -> Self {
        // `settle` acquires every permit in one call, which counts in `u32`.
        let capacity =
            u32::try_from(capacity.get().min(Semaphore::MAX_PERMITS)).unwrap_or(u32::MAX);
        Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity as usize)),
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.capacity as usize - self.permits.available_permits()
    }

    /// Refuses every later request with `shutting_down`, then waits until
    /// the admitted ones have sent their last frame.
    pub async fn settle(&self) {
        self.draining.store(true, Ordering::Release);
        // The semaphore is never closed, so the acquire cannot fail.
        let _ = Arc::clone(&self.permits)
            .acquire_many_owned(self.capacity)
            .await;
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

pub(super) async fn admit_request(
    State(state): State<Arc<BindingState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(limit) = state.request_limit() else {
        return next.run(request).await;
    };
    if matches!(*request.method(), Method::GET | Method::HEAD)
        && request
            .extensions()
            .get::<MatchedPath>()
            .is_some_and(|path| matches!(path.as_str(), "/health" | "/readiness"))
    {
        return next.run(request).await;
    }
    // The permit is taken before the flag is read, so a request that sees
    // no drain holds a permit the drain waits for.
    let permit = Arc::clone(&limit.permits).try_acquire_owned();
    if limit.is_draining() {
        return super::response_frames::split_response(
            ApiResponseError::new(
                ErrorCode::ShuttingDown,
                "the server is shutting down and no longer admits requests",
            )
            .into_response(),
        );
    }
    let Ok(permit) = permit else {
        state.metrics.request_rejected_as_busy();
        return super::response_frames::split_response(
            server_busy_error("in-flight requests").into_response(),
        );
    };
    let response = next.run(request).await;
    response.map(|body| {
        let permit = (!body.is_end_stream()).then_some(permit);
        Body::new(AdmittedBody { body, permit })
    })
}

struct AdmittedBody {
    body: Body,
    permit: Option<OwnedSemaphorePermit>,
}

impl http_body::Body for AdmittedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.body).poll_frame(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) || this.body.is_end_stream() {
            this.permit.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
