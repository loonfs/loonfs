//! Request admission before authentication and body extraction.

use super::extractors::server_busy_error;
use crate::BindingState;
use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http_body::Body as _;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct RequestLimit {
    capacity: usize,
    permits: Arc<Semaphore>,
}

impl RequestLimit {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let capacity = capacity.get().min(Semaphore::MAX_PERMITS);
        Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.capacity - self.permits.available_permits()
    }
}

pub(super) async fn admit_request(
    State(state): State<BindingState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(limit) = &state.request_limit else {
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
    let Ok(permit) = Arc::clone(&limit.permits).try_acquire_owned() else {
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
