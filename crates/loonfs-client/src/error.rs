//! Defines [`ClientError`], returned by asynchronous client operations.

use loonfs_types::config::ConfigFieldError;
use loonfs_types::{ErrorCause, ErrorCode, ErrorResponse};
use thiserror::Error;

/// Error returned by the asynchronous HTTP client.
///
/// Foreign causes (I/O, JSON, and HTTP transport) are captured as message
/// strings rather than `#[source]` chains.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The client configuration could not be read.
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    /// The client configuration could not be decoded.
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    /// A client configuration field is missing or invalid.
    #[error(transparent)]
    ConfigField(#[from] ConfigFieldError),
    /// A path is invalid or cannot be used with the requested namespace.
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    /// Sending an HTTP request or reading its response failed.
    #[error("http error: {0}")]
    Http(String),
    /// A response had an error status and a body that is not the API's error
    /// envelope, such as a storage provider's refusal of a presigned URL or
    /// a load balancer's HTML 502.
    #[error("http status {status} with a non-envelope body: {detail}")]
    HttpStatus {
        /// HTTP status code of the response.
        status: u16,
        /// The `<Code>` of a storage provider's XML error body, when the
        /// body is one.
        provider_code: Option<String>,
        /// The provider's error code, or why the body is not the error
        /// envelope.
        detail: String,
    },
    /// The server returned a structured API error response.
    #[error("server returned {status} {}: {}", response.code, response.message)]
    Api {
        /// HTTP status code returned by the server.
        status: u16,
        /// The error body that the server returned. Boxed to keep the enum
        /// small.
        response: Box<ErrorResponse>,
    },
    /// None of the server's upload methods can accept the payload.
    ///
    /// Returned before any data is uploaded.
    #[error("payload of {size_bytes} bytes exceeds every upload transport this deployment offers: {reason}")]
    UploadTooLarge {
        /// Total payload size in bytes.
        size_bytes: u64,
        /// Description of the upload limits the payload exceeds.
        reason: String,
    },
    /// Reading or writing streamed data failed.
    #[error("i/o error: {0}")]
    Io(String),
    /// Serializing a request or decoding a JSON response failed.
    #[error("json error: {0}")]
    Json(String),
    /// A decoded response violates the API contract.
    ///
    /// [`ClientError::Json`] means the response could not be decoded. This
    /// variant means decoding succeeded, but the decoded value is not valid
    /// according to the API specification.
    #[error("server response violates the API contract: {0}")]
    Protocol(String),
}

impl ClientError {
    /// Converts a decoded API error into a client error.
    pub fn from_error_response(status: u16, response: ErrorResponse) -> Self {
        Self::Api {
            status,
            response: Box::new(response),
        }
    }

    /// Returns the typed code for [`ClientError::Api`].
    ///
    /// Returns `None` for other error variants and for API codes this client
    /// version does not recognize.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Api { response, .. } => ErrorCode::parse(&response.code),
            _ => None,
        }
    }

    /// Returns the cause in the details of [`ClientError::Api`], for a code
    /// that two causes share because the remedy is the same for both. Branch
    /// on [`Self::code`], and use the cause to report or count what happened.
    ///
    /// Returns `None` for other error variants and for errors without a cause.
    pub fn cause(&self) -> Option<ErrorCause> {
        match self {
            ClientError::Api { response, .. } => response.details.as_ref()?.cause,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn status_errors_keep_the_status_and_the_provider_code_when_the_body_is_not_the_envelope() {
        let error = crate::transport::map_status_error(502, b"<html>upstream error</html>");

        assert_matches!(
            error,
            ClientError::HttpStatus {
                status: 502,
                provider_code: None,
                ..
            }
        );
        assert!(error.to_string().contains("non-envelope body"), "{error}");

        let error = crate::transport::map_status_error(
            400,
            b"<?xml version=\"1.0\"?><Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>",
        );
        assert_matches!(
            &error,
            ClientError::HttpStatus { status: 400, provider_code: Some(code), .. } if code == "ExpiredToken"
        );
        assert!(error.to_string().ends_with("ExpiredToken"), "{error}");
    }

    fn api_error(status: u16, code: &str) -> ClientError {
        ClientError::Api {
            status,
            response: Box::new(ErrorResponse {
                code: code.to_owned(),
                feature: None,
                message: "test".to_owned(),
                param: None,
                request_id: None,
                details: None,
            }),
        }
    }

    #[test]
    fn api_errors_expose_the_cause_that_shares_a_code() {
        for (body, code, cause) in [
            (
                r#"{"code":"outcome_unknown","message":"m","details":{"cause":"deadline"}}"#,
                ErrorCode::OutcomeUnknown,
                Some(ErrorCause::Deadline),
            ),
            (
                r#"{"code":"outcome_unknown","message":"m","details":{"cause":"unconfirmed"}}"#,
                ErrorCode::OutcomeUnknown,
                Some(ErrorCause::Unconfirmed),
            ),
            (
                r#"{"code":"snapshot_not_found","message":"m","details":{"cause":"manifest_missing"}}"#,
                ErrorCode::SnapshotNotFound,
                Some(ErrorCause::ManifestMissing),
            ),
            (
                r#"{"code":"store_contention","message":"m","details":{"cause":"publication_budget"}}"#,
                ErrorCode::StoreContention,
                Some(ErrorCause::PublicationBudget),
            ),
            (
                r#"{"code":"store_contention","message":"m","details":{"cause":"from_a_newer_server"}}"#,
                ErrorCode::StoreContention,
                Some(ErrorCause::Unknown),
            ),
            (
                r#"{"code":"snapshot_not_found","message":"m"}"#,
                ErrorCode::SnapshotNotFound,
                None,
            ),
        ] {
            let response: ErrorResponse = serde_json::from_str(body).expect("error envelope");
            let error = ClientError::from_error_response(503, response);
            assert_eq!(error.code(), Some(code));
            assert_eq!(error.cause(), cause);
        }
    }

    #[test]
    fn api_errors_tolerate_unknown_registry_codes() {
        assert_eq!(api_error(503, "code_from_a_newer_server").code(), None);
        assert_eq!(
            ClientError::Http("connection refused".to_owned()).code(),
            None
        );
    }
}
