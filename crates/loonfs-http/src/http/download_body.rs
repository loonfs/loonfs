//! Streaming download response bodies, the `Range` header that selects part
//! of one, and their admission permits.

use super::error::ApiResponseError;
use axum::body::Body;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use loonfs::{ErrorCode, FileContentStream, ReadRange, SharedObjectStore, StreamRange};
use loonfs_types::NamespaceId;
use tokio::sync::OwnedSemaphorePermit;

/// The byte range that a content read's `Range` header asks for, or `None`
/// when the request has no `Range` header (api.md, "Ranged reads").
pub(super) struct RangeHeader(pub(super) Option<ReadRange>);

impl RangeHeader {
    /// Returns the part of the file that the read streams.
    pub(super) fn stream_range(&self) -> StreamRange {
        self.0.map_or_else(StreamRange::default, StreamRange::Range)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RangeHeader {
    type Rejection = ApiResponseError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let mut values = parts.headers.get_all(header::RANGE).iter();
        let Some(value) = values.next() else {
            return Ok(Self(None));
        };
        let range = match values.next() {
            Some(_) => None,
            None => value.to_str().ok().and_then(ReadRange::from_range_header),
        };
        range.map(|range| Self(Some(range))).ok_or_else(|| {
            ApiResponseError::new(
                ErrorCode::InvalidRequest,
                &format!(
                    "`Range` {:?} must ask for one range of bytes, as `bytes=start-end` or \
                     `bytes=start-`",
                    String::from_utf8_lossy(value.as_bytes())
                ),
            )
            .with_param("Range")
        })
    }
}

/// Keeps admission until the stream finishes or is abandoned.
///
/// The response to a read without a range has no Content-Length, because
/// successful HTTP completion follows checksum verification, not merely
/// delivery of the expected number of bytes. A ranged read isn't verified, so
/// its response includes Content-Length and Content-Range. A late failure
/// aborts the body.
pub(super) fn streamed_download_response(
    stream: FileContentStream<SharedObjectStore>,
    range: Option<ReadRange>,
    permit: OwnedSemaphorePermit,
    max_content_bytes: Option<u64>,
    namespace_id: &NamespaceId,
) -> Result<Response, ApiResponseError> {
    let offsets = stream.offsets();
    let length = offsets.end - offsets.start;
    if let Some(max_bytes) = max_content_bytes {
        match range {
            None if stream.size_bytes() > max_bytes => {
                return Err(ApiResponseError::runtime_for_namespace(
                    namespace_id,
                    loonfs::Error::Core(loonfs::CoreError::ContentTooLarge {
                        size_bytes: stream.size_bytes(),
                        max_bytes,
                    }),
                ));
            }
            Some(_) if length > max_bytes => {
                return Err(ApiResponseError::new(
                    ErrorCode::ContentTooLarge,
                    &format!(
                        "the requested range is {length} bytes, over the {max_bytes}-byte \
                         limit for one proxied read"
                    ),
                ));
            }
            _ => {}
        }
    }
    let size_bytes = stream.size_bytes();
    let body = futures::stream::try_unfold((stream, permit), |(mut stream, permit)| async move {
        match stream.next_chunk().await {
            Ok(Some(bytes)) => Ok(Some((bytes, (stream, permit)))),
            Ok(None) => Ok(None),
            Err(error) => {
                tracing::warn!(code = %error.code(), error = %error, "download body failed verification or transfer");
                Err(std::io::Error::other(error))
            }
        }
    });
    let mut response = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::ACCEPT_RANGES, "bytes"),
        ],
        Body::from_stream(body),
    )
        .into_response();
    if range.is_some() {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{size_bytes}",
                offsets.start,
                offsets.end - 1
            ))
            .expect("a content range of digits is a valid header value"),
        );
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    }
    Ok(response)
}
