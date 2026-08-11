//! The HTTP error envelope served by every v0 endpoint, and the mapping
//! from error kinds to HTTP statuses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use loonfs::{CoreError, RuntimeError};
use loonfs_api::{
    ApiError, CommitId, ErrorCode, ErrorDetails, ErrorKind, NamespaceId, NamespaceIdValidationError,
};

pub(super) struct ApiResponseError {
    status: StatusCode,
    body: ApiError,
    /// Emitted as a `Retry-After` header, for retryable capacity errors.
    retry_after_seconds: Option<u32>,
}

impl ApiResponseError {
    pub(super) fn new(status: StatusCode, code: ErrorCode, message: &str) -> Self {
        Self {
            status,
            body: ApiError {
                code: code.as_str().to_owned(),
                feature: None,
                message: message.to_owned(),
                request_id: None,
                details: None,
            },
            retry_after_seconds: None,
        }
    }

    pub(super) fn not_supported(feature: &str, message: &str) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            body: ApiError {
                code: ErrorCode::NotSupported.as_str().to_owned(),
                feature: Some(feature.to_owned()),
                message: message.to_owned(),
                request_id: None,
                details: None,
            },
            retry_after_seconds: None,
        }
    }

    /// Stamps a `Retry-After` hint onto the response, HTTP's native shape
    /// for "come back shortly" so generic clients and proxies pace
    /// themselves too.
    pub(super) fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }

    /// Stamps the mutation's idempotency key into the error details, so a
    /// failed or uncertain outcome carries the caller's reconciliation
    /// handle (API spec, "Commit responses and safe retry"). Details the
    /// error already carries win over the stamp.
    pub(super) fn with_commit_id(mut self, commit_id: &CommitId) -> Self {
        self.body
            .details
            .get_or_insert_with(Box::<ErrorDetails>::default)
            .commit_id
            .get_or_insert_with(|| commit_id.clone());
        self
    }

    pub(super) fn invalid_namespace_id(error: NamespaceIdValidationError) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &error.to_string(),
        )
    }

    pub(super) fn runtime(error: RuntimeError) -> Self {
        let code = error.code();
        let rendered = error.to_string();
        let message = match &error {
            RuntimeError::Config(message) => message.as_str(),
            _ => rendered.as_str(),
        };
        let mut response = Self::new(status_for_core_error_code(code), code, message);
        if let RuntimeError::Core(error) = error {
            response.body.details = error.details().map(Box::new);
        }
        response
    }

    fn core(error: CoreError) -> Self {
        let status = status_for_core_error_code(error.code());
        let mut response = Self::new(status, error.code(), &error.to_string());
        response.body.details = error.details().map(Box::new);
        response
    }

    pub(super) fn core_for_namespace(namespace_id: &NamespaceId, error: CoreError) -> Self {
        if matches!(error.code(), ErrorCode::NamespaceNotFound) {
            return Self::new(
                StatusCode::NOT_FOUND,
                ErrorCode::NamespaceNotFound,
                &format!("namespace `{}` does not exist", namespace_id.as_str()),
            );
        }

        Self::core(error)
    }

    pub(super) fn runtime_for_namespace(namespace_id: &NamespaceId, error: RuntimeError) -> Self {
        if error.code() == ErrorCode::NamespaceNotFound {
            return Self::new(
                StatusCode::NOT_FOUND,
                ErrorCode::NamespaceNotFound,
                &format!("namespace `{}` does not exist", namespace_id.as_str()),
            );
        }

        Self::runtime(error)
    }
}

pub(super) fn status_for_core_error_code(code: ErrorCode) -> StatusCode {
    status_for_error_kind(code.kind())
}

/// Maps a caller-action [`ErrorKind`] to the HTTP status this server serves:
/// the api.md error table is the source of truth, and the spec-table sync
/// test in `super::tests` enforces that this mapping composed with
/// [`ErrorCode::kind`] reproduces it exactly.
fn status_for_error_kind(kind: ErrorKind) -> StatusCode {
    match kind {
        ErrorKind::InvalidRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorKind::ContentTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::Gone => StatusCode::GONE,
        ErrorKind::AlreadyExists | ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        ErrorKind::NotSupported => StatusCode::NOT_IMPLEMENTED,
        ErrorKind::Unavailable | ErrorKind::OutcomeUnknown => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::DataCorruption | ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        // A kind without an explicit arm serves as 500 until someone decides
        // its real status. The spec-table test in `super::tests` fails on any
        // code whose served status disagrees with the api.md registry, so new
        // kinds cannot ship on this default silently.
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiResponseError {
    fn into_response(mut self) -> Response {
        // The correlation id is scoped by the request-id middleware; a body
        // rendered outside a request scope (tests constructing errors
        // directly) simply omits it.
        self.body.request_id = super::REQUEST_ID.try_with(|id| id.clone()).ok();
        let mut response = (self.status, Json(self.body)).into_response();
        if let Some(seconds) = self.retry_after_seconds {
            if let Ok(value) = axum::http::HeaderValue::from_str(&seconds.to_string()) {
                response
                    .headers_mut()
                    .insert(axum::http::header::RETRY_AFTER, value);
            }
        }
        response
    }
}
