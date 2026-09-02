//! Defines [`ClientError`], returned by asynchronous client operations.

use loonfs_api::{ApiError, ErrorCode, ErrorDetails};
use thiserror::Error;

/// Error returned by the asynchronous HTTP client.
///
/// Foreign causes (I/O, JSON, and HTTP transport) are captured as message
/// strings rather than `#[source]` chains.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The client configuration could not be read.
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    /// The client configuration could not be decoded.
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    /// A required client configuration field is missing.
    #[error("missing `{field}`")]
    MissingConfigField {
        /// Name of the missing field.
        field: &'static str,
    },
    /// A client configuration field contains an invalid value.
    #[error("invalid `{field}`: {reason}")]
    ConfigValidation {
        /// Name of the field containing the invalid value.
        field: &'static str,
        /// Why the value is invalid.
        reason: String,
    },
    /// A path is invalid or cannot be used with the requested namespace.
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    /// Sending an HTTP request or reading its response failed.
    #[error("http error: {0}")]
    Http(String),
    /// The server returned a structured API error response.
    #[error("server returned {status} {code}: {message}")]
    Api {
        /// HTTP status code returned by the server.
        status: u16,
        /// Machine-readable API error code returned by the server.
        code: String,
        /// Capability feature key accompanying `not_supported` errors.
        feature: Option<String>,
        /// Error message returned by the server.
        message: String,
        /// Identifies the invalid input. Body fields use JSON Pointer paths;
        /// query and path parameters use their names; CLI errors use the flag
        /// or argument as written.
        param: Option<String>,
        /// Correlation ID assigned to the failed request.
        request_id: Option<String>,
        /// Additional structured error details, when provided by the server.
        /// Boxed to keep the enum small.
        details: Option<Box<ErrorDetails>>,
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
    pub fn from_api_error(status: u16, body: ApiError) -> Self {
        Self::Api {
            status,
            code: body.code,
            feature: body.feature,
            message: body.message,
            param: body.param,
            request_id: body.request_id,
            details: body.details,
        }
    }

    /// Returns the typed code for [`ClientError::Api`].
    ///
    /// Returns `None` for other error variants and for API codes this client
    /// version does not recognize.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Api { code, .. } => ErrorCode::parse(code),
            _ => None,
        }
    }
}
