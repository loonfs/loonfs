//! The HTTP error envelope served by every v0 endpoint, and the mapping
//! from error kinds to HTTP statuses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use loonfs::RuntimeError;
use loonfs_api::{
    ApiError, CommitId, ErrorCode, ErrorDetails, ErrorKind, NamespaceId, NamespaceIdValidationError,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ServedErrorCode(pub(super) ErrorCode);

pub(super) struct ApiResponseError {
    status: StatusCode,
    code: ErrorCode,
    body: Box<ApiError>,
    /// Emitted as a `Retry-After` header, for retryable capacity errors.
    retry_after_seconds: Option<u32>,
}

impl ApiResponseError {
    pub(super) fn new(status: StatusCode, code: ErrorCode, message: &str) -> Self {
        let response = Self {
            status,
            code,
            body: Box::new(ApiError {
                code: code.as_str().to_owned(),
                feature: None,
                message: message.to_owned(),
                param: None,
                request_id: None,
                details: None,
            }),
            retry_after_seconds: None,
        };
        if code.retryable_without_operator_action() {
            response.with_retry_after(1)
        } else {
            response
        }
    }

    pub(super) fn not_supported(feature: &str, message: &str) -> Self {
        let mut response = Self::new(
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::NotSupported,
            message,
        );
        response.body.feature = Some(feature.to_owned());
        response
    }

    /// Stamps a `Retry-After` hint onto the response, HTTP's native shape
    /// for "come back shortly" so generic clients and proxies pace
    /// themselves too.
    pub(super) fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }

    /// Identifies the request input that caused the error.
    pub(super) fn with_param(mut self, param: impl Into<String>) -> Self {
        self.body.param = Some(param.into());
        self
    }

    /// Sets `param` only for an `invalid_request` error.
    pub(super) fn with_invalid_request_param(self, param: impl Into<String>) -> Self {
        if self.body.code == ErrorCode::InvalidRequest.as_str() {
            self.with_param(param)
        } else {
            self
        }
    }

    #[cfg(test)]
    pub(super) fn param(&self) -> Option<&str> {
        self.body.param.as_deref()
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
        .with_param("namespace_id")
    }

    pub(super) fn runtime(error: RuntimeError) -> Self {
        let code = error.code();
        let details = error.details();
        let message = error.public_message();
        let mut response = Self::new(status_for_core_error_code(code), code, &message);
        response.body.details = details.map(Box::new);
        response
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
        ErrorKind::NotSupported => StatusCode::NOT_IMPLEMENTED,
        // HTTP 408 means the client did not finish sending its request. A
        // server-side deadline uses 503 instead. The error code remains
        // non-retryable because a cancelled mutation may still complete.
        ErrorKind::DeadlineExceeded | ErrorKind::Unavailable | ErrorKind::OutcomeUnknown => {
            StatusCode::SERVICE_UNAVAILABLE
        }
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
        response.extensions_mut().insert(ServedErrorCode(self.code));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_immediately_retryable_code_answers_with_retry_after() {
        for code in ErrorCode::ALL {
            let response =
                ApiResponseError::new(status_for_core_error_code(code), code, "test response")
                    .into_response();
            assert_eq!(
                response.extensions().get::<ServedErrorCode>(),
                Some(&ServedErrorCode(code)),
                "response extension lost {code}"
            );
            let retry_after = response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .map(|value| value.to_str().expect("Retry-After is ASCII"));
            let expected = code.retryable_without_operator_action().then_some("1");
            assert_eq!(retry_after, expected, "unexpected Retry-After for {code}");
        }
    }

    #[test]
    fn not_supported_response_keeps_its_typed_code_extension() {
        let response =
            ApiResponseError::not_supported("test.feature", "not available").into_response();
        assert_eq!(
            response.extensions().get::<ServedErrorCode>(),
            Some(&ServedErrorCode(ErrorCode::NotSupported))
        );
    }
}
