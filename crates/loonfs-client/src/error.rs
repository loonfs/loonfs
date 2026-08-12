//! [`ClientError`]: every failure the async client surfaces.

use loonfs_api::{ErrorCode, ErrorDetails};
use thiserror::Error;

/// Error returned by the async HTTP client.
///
/// Foreign causes (I/O, JSON, and HTTP transport) are captured as message
/// strings rather than `#[source]` chains.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Reading the client configuration failed.
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    /// Decoding the client configuration failed.
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    /// The client configuration omitted a required field.
    #[error("missing `{field}`")]
    MissingConfigField {
        /// Name of the required field.
        field: &'static str,
    },
    /// A client configuration field failed validation.
    #[error("invalid `{field}`: {reason}")]
    ConfigValidation {
        /// Name of the invalid field.
        field: &'static str,
        /// Plain-English reason the value is invalid.
        reason: String,
    },
    /// A namespace-qualified path failed validation.
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    /// A commit id failed validation.
    #[error("invalid commit_id `{0}`")]
    InvalidCommitId(String),
    /// A checkpoint id failed validation.
    #[error("invalid checkpoint_id `{0}`")]
    InvalidCheckpointId(String),
    /// An HTTP transport operation failed.
    #[error("http error: {0}")]
    Http(String),
    /// The server returned a structured API error response.
    #[error("server returned {status} {code}: {message}")]
    Api {
        /// HTTP response status.
        status: u16,
        /// Stable machine-readable API error code.
        code: String,
        /// Capability feature key accompanying `not_supported` errors.
        feature: Option<String>,
        /// Human-readable error message.
        message: String,
        /// Correlation id the server assigned to the failed request.
        request_id: Option<String>,
        /// Structured context for the code, when the server sent any. Boxed
        /// so the rare detailed error does not widen every client result.
        details: Option<Box<ErrorDetails>>,
    },
    /// No upload transport this deployment offers can carry the payload.
    ///
    /// Raised before any byte moves, because the alternative is sending an
    /// oversized payload into the capped proxy to be refused there.
    #[error("payload of {size_bytes} bytes exceeds every upload transport this deployment offers: {reason}")]
    UploadTooLarge {
        /// Complete length of the payload that could not be carried.
        size_bytes: u64,
        /// The caps it exceeded, named so a caller can act on them.
        reason: String,
    },
    /// Reading or writing a streamed payload failed.
    #[error("i/o error: {0}")]
    Io(String),
    /// Encoding or decoding JSON failed.
    #[error("json error: {0}")]
    Json(String),
    /// A well-formed response that breaks a shape rule the API spec states.
    ///
    /// Distinct from [`ClientError::Json`], which is a body that did not
    /// decode: this one decoded and then said something the contract forbids,
    /// so the client refuses it rather than passing a wrong answer along.
    #[error("server response violates the API contract: {0}")]
    Protocol(String),
}

impl ClientError {
    /// Returns the typed code for [`ClientError::Api`] errors, or `None` for
    /// non-API errors and for codes this build does not know (clients must
    /// tolerate unknown codes).
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Api { code, .. } => ErrorCode::parse(code),
            _ => None,
        }
    }
}
