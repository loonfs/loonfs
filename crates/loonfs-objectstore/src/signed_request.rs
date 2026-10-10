//! Helpers for HTTP requests signed by LoonFS.

use crate::object_store::Result;
use crate::presign::PresignedUrl;
use crate::{ObjectStoreError, StoredObjectChecksum};
use loonfs_types::Checksum;
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
    send(client, key, builder, body).await
}

/// Sends one request and collects its whole response.
pub(crate) async fn send(
    client: &HttpClient,
    key: &str,
    request: http::request::Builder,
    body: HttpRequestBody,
) -> Result<SignedResponse> {
    let request = request
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

/// The object length and ETag a signed `HEAD` reports, or `None` when the
/// object is absent.
pub(crate) fn object_length_from_signed_head(
    key: &str,
    response: &SignedResponse,
) -> Result<Option<(u64, String)>> {
    if let Some(error) = classify_signed_response(key, response.status, None) {
        return match error {
            ObjectStoreError::NotFound { .. } => Ok(None),
            error => Err(error),
        };
    }
    let header = |name: http::header::HeaderName| {
        response
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let size_bytes = header(http::header::CONTENT_LENGTH)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| ObjectStoreError::transport(key, "head reported no content length"))?;
    let etag = header(http::header::ETAG)
        .ok_or_else(|| ObjectStoreError::transport(key, "head reported no etag"))?;
    Ok(Some((size_bytes, etag)))
}

pub(crate) fn metadata_from_signed_head(
    key: &str,
    response: &SignedResponse,
    sha256_header: &str,
    version_header: &str,
    stored_checksum: impl FnOnce(&http::HeaderMap) -> Option<Checksum>,
) -> Result<Option<crate::ObjectMetadata>> {
    let Some((size_bytes, etag)) = object_length_from_signed_head(key, response)? else {
        return Ok(None);
    };
    let header = |name: &str| {
        response
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
    };
    let attestation = header(sha256_header)
        .and_then(crate::provider_object_store::attested_sha256)
        .or_else(|| stored_checksum(&response.headers));
    let last_modified_ms = header("last-modified")
        .and_then(|value| httpdate::parse_http_date(value).ok())
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|value| u64::try_from(value.as_millis()).ok());
    Ok(Some(crate::ObjectMetadata {
        size_bytes,
        etag: Some(etag),
        version: header(version_header).map(str::to_owned),
        last_modified_ms,
        attestation,
    }))
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
        Some("BadDigest") => ObjectStoreError::ChecksumMismatch {
            object_key: key.to_owned(),
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn head(status: u16, headers: &[(&str, &str)]) -> SignedResponse {
        let mut map = http::HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                http::header::HeaderName::from_bytes(name.as_bytes()).expect("header"),
                value.parse().expect("value"),
            );
        }
        SignedResponse {
            status: http::StatusCode::from_u16(status).expect("status"),
            headers: map,
            body: bytes::Bytes::new(),
        }
    }

    #[test]
    fn a_head_prefers_writer_sha256_then_stored_crc_and_preserves_object_age() {
        let sha256 = Checksum::sha256(b"bytes");
        let crc = Checksum::crc64nvme(b"bytes");
        let headers = [
            ("content-length", "5"),
            ("etag", "version"),
            ("last-modified", "Thu, 01 Jan 1970 00:00:01 GMT"),
        ];
        let mut response = head(200, &headers);
        for expected in [Some(crc.clone()), None] {
            let metadata = metadata_from_signed_head(
                "key",
                &response,
                "x-amz-meta-sha256",
                "x-amz-version-id",
                |_| expected.clone(),
            )
            .expect("head")
            .expect("object");
            assert_eq!(metadata.attestation, expected);
            assert_eq!(metadata.last_modified_ms, Some(1000));
        }
        response
            .headers
            .insert("x-amz-meta-sha256", sha256.value.parse().expect("header"));
        let metadata = metadata_from_signed_head(
            "key",
            &response,
            "x-amz-meta-sha256",
            "x-amz-version-id",
            |_| Some(crc),
        )
        .expect("head")
        .expect("object");
        assert_eq!(metadata.attestation, Some(sha256));
    }

    #[test]
    fn a_signed_head_reports_the_length_and_etag_or_absence() {
        let key = "namespaces/demo/content/con_1";
        assert_eq!(
            object_length_from_signed_head(
                key,
                &head(200, &[("content-length", "12"), ("etag", "\"v1\"")]),
            )
            .expect("present"),
            Some((12, "\"v1\"".to_owned()))
        );
        assert_eq!(
            object_length_from_signed_head(key, &head(404, &[])).expect("absent"),
            None
        );
        assert!(matches!(
            object_length_from_signed_head(key, &head(200, &[("etag", "\"v1\"")])),
            Err(ObjectStoreError::Transport { .. })
        ));
        assert!(matches!(
            object_length_from_signed_head(key, &head(403, &[])),
            Err(ObjectStoreError::PermissionDenied { .. })
        ));
    }
}
