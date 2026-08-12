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
    #[error("failed to read config: {0}")]
    ConfigIo(String),
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    #[error("missing `{field}`")]
    MissingConfigField { field: &'static str },
    #[error("invalid `{field}`: {reason}")]
    ConfigValidation { field: &'static str, reason: String },
    #[error("invalid namespace path `{0}`")]
    InvalidNamespacePath(String),
    #[error("invalid commit_id `{0}`")]
    InvalidCommitId(String),
    #[error("invalid checkpoint_id `{0}`")]
    InvalidCheckpointId(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("server returned {status} {code}: {message}")]
    Api {
        status: u16,
        code: String,
        /// Capability feature key accompanying `not_supported` errors.
        feature: Option<String>,
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
    #[error("i/o error: {0}")]
    Io(String),
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
