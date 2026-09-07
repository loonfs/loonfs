//! Streaming download response bodies and their admission permits.

use super::error::ApiResponseError;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use loonfs::{FileContentStream, SharedObjectStore};
use loonfs_api::NamespaceId;
use tokio::sync::OwnedSemaphorePermit;

/// Keeps admission until the stream finishes or is abandoned. No Content-Length
/// is sent: successful HTTP completion follows checksum verification, not merely
/// delivery of the expected number of bytes. A late failure aborts the body.
pub(super) fn streamed_download_response(
    stream: FileContentStream<SharedObjectStore>,
    permit: OwnedSemaphorePermit,
    max_content_bytes: u64,
    namespace_id: &NamespaceId,
) -> Result<Response, ApiResponseError> {
    if stream.size_bytes() > max_content_bytes {
        return Err(ApiResponseError::runtime_for_namespace(
            namespace_id,
            loonfs::RuntimeError::Core(loonfs::CoreError::ContentTooLarge {
                size_bytes: stream.size_bytes(),
                max_bytes: max_content_bytes,
            }),
        ));
    }
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
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(body),
    )
        .into_response())
}
