//! Requests this crate signs for itself when a provider client cannot
//! express them, and the provider-independent reading of their responses.

use crate::object_store::Result;
use crate::presign::PresignedUrl;
use crate::{ObjectStoreError, StoredObjectChecksum};
use loonfs_api::Checksum;
use object_store::client::{HttpClient, HttpRequestBody};

/// One internally signed response, read whole: the bodies are small control
/// documents, and the S3 family can report failure inside a 200 body.
pub(crate) struct SignedResponse {
    pub(crate) status: http::StatusCode,
    pub(crate) headers: http::HeaderMap,
    pub(crate) body: bytes::Bytes,
}

pub(crate) async fn execute_signed(
    client: &HttpClient,
    key: &str,
    signed: PresignedUrl,
    body: HttpRequestBody,
) -> Result<SignedResponse> {
    let mut builder = http::Request::builder()
        .method(signed.method.as_str())
        .uri(&signed.url);
    for (name, value) in &signed.headers {
        builder = builder.header(name, value);
    }
    let request = builder
        .body(body)
        .map_err(|err| ObjectStoreError::transport(key, err.to_string()))?;
    let response = client
        .execute(request)
        .await
        .map_err(|err| ObjectStoreError::retryable_transport(key, err.to_string()))?;

    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .bytes()
        .await
        .map_err(|err| ObjectStoreError::retryable_transport(key, err.to_string()))?;
    Ok(SignedResponse {
        status,
        headers,
        body,
    })
}

/// Reads a signed checksum head, taking the provider's header reader. An
/// object with no usable checksum is an error, never a size-only answer:
/// completion has nothing to compare.
pub(crate) fn stored_checksum_from_signed_head(
    key: &str,
    response: &SignedResponse,
    stored_checksum: impl FnOnce(&http::HeaderMap) -> Option<Checksum>,
) -> Result<Option<StoredObjectChecksum>> {
    let status = response.status;
    if status == http::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if status == http::StatusCode::FORBIDDEN || status == http::StatusCode::UNAUTHORIZED {
        return Err(ObjectStoreError::PermissionDenied {
            object_key: key.to_owned(),
            message: format!("provider refused the checksum head with {status}"),
        });
    }
    if !status.is_success() {
        return Err(transport_for_status(
            key,
            status,
            format!("checksum head failed with {status}"),
        ));
    }

    let Some(checksum) = stored_checksum(&response.headers) else {
        return Err(ObjectStoreError::StoredChecksumMissing {
            object_key: key.to_owned(),
        });
    };
    let size_bytes = response
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            ObjectStoreError::transport(key, "checksum head reported no content length")
        })?;

    Ok(Some(StoredObjectChecksum {
        size_bytes,
        checksum,
    }))
}

fn transport_for_status(
    key: &str,
    status: http::StatusCode,
    message: impl Into<String>,
) -> ObjectStoreError {
    if status == http::StatusCode::REQUEST_TIMEOUT
        || status == http::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        ObjectStoreError::retryable_transport(key, message)
    } else {
        ObjectStoreError::transport(key, message)
    }
}
