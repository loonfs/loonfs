//! Defines [`ClientError`], returned by asynchronous client operations.

use loonfs_types::config::ConfigFieldError;
use loonfs_types::{ErrorCode, ErrorResponse};
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_errors_keep_the_status_when_the_body_is_not_the_envelope() {
        let error = crate::transport::map_status_error(502, b"<html>upstream error</html>");

        let ClientError::Http(message) = error else {
            panic!("expected Http error, got {error:?}");
        };
        assert!(message.contains("502"), "{message}");
        assert!(message.contains("non-envelope body"), "{message}");
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
    fn api_errors_expose_known_registry_codes() {
        for (wire, expected) in [
            ("stale_revision", ErrorCode::StaleRevision),
            ("content_not_prepared", ErrorCode::ContentNotPrepared),
            ("namespace_deleted", ErrorCode::NamespaceDeleted),
            ("commit_outcome_unknown", ErrorCode::CommitOutcomeUnknown),
            ("index_corrupt", ErrorCode::IndexCorrupt),
        ] {
            assert_eq!(api_error(500, wire).code(), Some(expected));
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
