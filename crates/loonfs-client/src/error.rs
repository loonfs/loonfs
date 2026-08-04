//! [`ClientError`]: every failure the blocking client surfaces.

use loonfs_api::{ErrorCode, ErrorDetails, ErrorKind};
use thiserror::Error;

/// Error returned by the blocking HTTP client.
///
/// Foreign causes (io, json, ureq) are captured as message strings rather
/// than `#[source]` chains.
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

    /// Returns the caller-action category for [`ClientError::Api`] errors.
    ///
    /// Known codes classify through [`ErrorCode::kind`]. Unknown codes (a
    /// newer server) fall back to the HTTP status class, so retry decisions
    /// still work: 503 is [`ErrorKind::Unavailable`], other 5xx are
    /// [`ErrorKind::Internal`], and 4xx are [`ErrorKind::InvalidRequest`].
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            ClientError::Api { status, code, .. } => match ErrorCode::parse(code) {
                Some(code) => Some(code.kind()),
                None => kind_for_status_class(*status),
            },
            _ => None,
        }
    }
}

/// Coarse status-class fallback for error codes this build does not know.
pub(crate) fn kind_for_status_class(status: u16) -> Option<ErrorKind> {
    match status {
        // 503 stays retryable even when the code is unknown.
        503 => Some(ErrorKind::Unavailable),
        400..=499 => Some(ErrorKind::InvalidRequest),
        500..=599 => Some(ErrorKind::Internal),
        _ => None,
    }
}
