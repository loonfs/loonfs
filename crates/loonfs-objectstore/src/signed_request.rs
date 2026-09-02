//! Helpers for HTTP requests signed by LoonFS.

use crate::object_store::Result;
use crate::presign::PresignedUrl;
use crate::{ObjectStoreError, StoredObjectChecksum};
use loonfs_api::Checksum;
use object_store::client::{HttpClient, HttpRequestBody};

const RETRYABLE_SIGNED_ERROR_CODES: &[&str] = &[
    "InternalError",
    "RequestTimeout",
    "ServiceUnavailable",
    "SlowDown",
];

pub(crate) struct SignedResponse {
    pub(crate) status: http::StatusCode,
    pub(crate) headers: http::HeaderMap,
    pub(crate) body: bytes::Bytes,
}

pub(crate) async fn send_signed(
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

/// Reads an object's size and checksum from a signed `HEAD` response.
pub(crate) fn stored_checksum_from_signed_head(
    key: &str,
    response: &SignedResponse,
    stored_checksum: impl FnOnce(&http::HeaderMap) -> Option<Checksum>,
) -> Result<Option<StoredObjectChecksum>> {
    if let Some(error) = classify_signed_response(key, response.status, None) {
        return match error {
            ObjectStoreError::NotFound { .. } => Ok(None),
            error => Err(error),
        };
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

pub(crate) fn classify_signed_response(
    key: &str,
    status: http::StatusCode,
    code: Option<&str>,
) -> Option<ObjectStoreError> {
    if status.is_success() && code.is_none() {
        return None;
    }

    let message = match code {
        Some(code) => format!("provider request failed: {code}"),
        None => format!("provider request failed with {status}"),
    };
    let error = match code {
        Some("AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch") => {
            ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message,
            }
        }
        Some("NoSuchBucket" | "NoSuchKey") => ObjectStoreError::NotFound {
            object_key: key.to_owned(),
        },
        Some("ConditionalRequestConflict" | "PreconditionFailed") => {
            ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            }
        }
        Some(code) if RETRYABLE_SIGNED_ERROR_CODES.contains(&code) => {
            ObjectStoreError::retryable_transport(key, message)
        }
        _ if status == http::StatusCode::NOT_FOUND => ObjectStoreError::NotFound {
            object_key: key.to_owned(),
        },
        _ if status == http::StatusCode::FORBIDDEN || status == http::StatusCode::UNAUTHORIZED => {
            ObjectStoreError::PermissionDenied {
                object_key: key.to_owned(),
                message,
            }
        }
        _ if status == http::StatusCode::PRECONDITION_FAILED
            || status == http::StatusCode::CONFLICT =>
        {
            ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            }
        }
        _ if status == http::StatusCode::REQUEST_TIMEOUT
            || status == http::StatusCode::TOO_MANY_REQUESTS
            || status.is_server_error() =>
        {
            ObjectStoreError::retryable_transport(key, message)
        }
        _ => ObjectStoreError::transport(key, message),
    };
    Some(error)
}
