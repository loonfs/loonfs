//! Buffered download response bodies and their admission permits.

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::Stream;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::OwnedSemaphorePermit;

/// One materialized download plus the permit accounting for its memory.
///
/// The stream owns the permit even after yielding its only chunk, so the
/// response body releases admission only when it is fully consumed or
/// abandoned and dropped.
struct DownloadBodyStream {
    bytes: Option<bytes::Bytes>,
    _permit: OwnedSemaphorePermit,
}

pub(super) fn buffered_download_response(bytes: Vec<u8>, permit: OwnedSemaphorePermit) -> Response {
    let body = Body::from_stream(DownloadBodyStream {
        bytes: Some(bytes.into()),
        _permit: permit,
    });
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

impl Stream for DownloadBodyStream {
    type Item = Result<bytes::Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.bytes.take().map(Ok))
    }
}
