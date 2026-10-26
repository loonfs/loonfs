//! The simulation's error type, which has the fields of the wire's error envelope.

use http::StatusCode;
use loonfs_types::{CommitId, ErrorCode, ErrorDetails, ErrorKind, ErrorResponse};

/// An expected error that the simulated server returns, with the fields of the wire's error
/// envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("simulated server error `{code}`: {message}")]
pub struct MountSimError {
    code: ErrorCode,
    message: String,
    param: Option<String>,
    feature: Option<String>,
    details: Box<ErrorDetails>,
}

/// The result type of the simulation's fallible operations.
pub type Result<T> = std::result::Result<T, MountSimError>;

impl MountSimError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            param: None,
            feature: None,
            details: Box::default(),
        }
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, message)
    }

    /// Returns `not_supported` for an operation or route that the simulation doesn't serve.
    pub(crate) fn not_simulated(what: &str) -> Self {
        Self::new(
            ErrorCode::NotSupported,
            format!("the simulated server doesn't serve {what}"),
        )
    }

    pub(crate) fn with_param(mut self, param: impl Into<String>) -> Self {
        self.param = Some(param.into());
        self
    }

    pub(crate) fn with_feature(mut self, feature: &str) -> Self {
        self.feature = Some(feature.to_owned());
        self
    }

    pub(crate) fn with_details(mut self, fill: impl FnOnce(&mut ErrorDetails)) -> Self {
        fill(&mut self.details);
        self
    }

    /// Records the position of the operation that stopped a commit, and points `param` at the
    /// operation when the error identified a field of it.
    pub(crate) fn at_operation(mut self, index: usize) -> Self {
        let index = u32::try_from(index).expect("a commit has at most 4,096 operations");
        if let Some(field) = self.param.take() {
            self.param = Some(match field.strip_prefix('/') {
                Some(_) => field,
                None => format!("/operations/{index}/{field}"),
            });
        }
        self.details.operation_index = Some(index);
        self
    }

    pub(crate) fn at_precondition(mut self, index: usize) -> Self {
        let index = u32::try_from(index).expect("a commit has at most 1,024 preconditions");
        if let Some(field) = self.param.take() {
            self.param = Some(format!("/preconditions/{index}/{field}"));
        }
        self.details.precondition_index = Some(index);
        self
    }

    pub(crate) fn for_commit(self, commit_id: &CommitId) -> Self {
        self.with_details(|details| details.commit_id = Some(commit_id.clone()))
    }

    /// Returns the wire error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the human-readable message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the invalid input, as a JSON pointer, a header, or a parameter name.
    pub fn param(&self) -> Option<&str> {
        self.param.as_deref()
    }

    /// Returns the machine-readable details.
    pub fn details(&self) -> &ErrorDetails {
        &self.details
    }

    pub(crate) fn status(&self) -> StatusCode {
        status_for(self.code)
    }

    pub(crate) fn to_response(&self, request_id: &str) -> ErrorResponse {
        ErrorResponse {
            code: self.code.as_str().to_owned(),
            feature: self.feature.clone(),
            message: self.message.clone(),
            param: self.param.clone(),
            request_id: Some(request_id.to_owned()),
            details: (*self.details != ErrorDetails::default()).then(|| self.details.clone()),
        }
    }
}

/// Returns the HTTP status that the API specification's error table assigns to `code`.
///
/// `loonfs-http` keeps the same mapping in a private function, so the simulation has its own copy.
fn status_for(code: ErrorCode) -> StatusCode {
    match code.kind() {
        ErrorKind::InvalidRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        ErrorKind::ContentTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorKind::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::Gone => StatusCode::GONE,
        ErrorKind::AlreadyExists | ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::NotSupported => StatusCode::NOT_IMPLEMENTED,
        ErrorKind::StoragePermissionDenied
        | ErrorKind::DeadlineExceeded
        | ErrorKind::Unavailable
        | ErrorKind::OutcomeUnknown => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
