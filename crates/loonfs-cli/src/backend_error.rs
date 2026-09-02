//! Shared backend errors and runtime error conversion.

use loonfs::RuntimeError;
use loonfs_api::{ErrorCode, ErrorDetails, NamespaceId};
use loonfs_client::ClientError;
use loonfs_grep::GrepError;
use thiserror::Error;

/// Error returned by either CLI backend.
///
/// `code` is either a shared [`loonfs_api::ErrorCode`] or a backend-local code.
/// Shared codes come from the registry so embedded and remote profiles report
/// the same code for the same failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub(crate) struct BackendError {
    /// Registry or backend-local error code.
    pub code: String,
    /// Feature key for `not_supported` errors.
    pub feature: Option<String>,
    /// Human-readable description of the failure.
    pub message: String,
    /// Identifies the invalid input. Body fields use JSON Pointer paths;
    /// query and path parameters use their names; CLI errors use the flag or
    /// argument as written.
    pub param: Option<String>,
    /// Correlation id the server assigned to the failed request. Always
    /// `None` for embedded and local failures, which have no server hop.
    pub request_id: Option<String>,
    /// Structured context for the code, when the transport carried any.
    pub details: Option<Box<ErrorDetails>>,
}

impl BackendError {
    /// Builds an error carrying a registry code verbatim.
    pub(crate) fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            feature: None,
            message: message.into(),
            param: None,
            request_id: None,
            details: None,
        }
    }

    /// A backend configuration that could not be loaded or used.
    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::new("invalid_config", message)
    }

    /// Caller input rejected before it reached a backend.
    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest.as_str(), message)
    }

    /// Transport failure between a client and a remote server.
    pub(crate) fn client_error(message: impl Into<String>) -> Self {
        Self::new("client_error", message)
    }

    /// Local i/o failure while moving bytes for a backend call.
    pub(crate) fn io_error(message: impl Into<String>) -> Self {
        Self::new("io_error", message)
    }

    /// Embedded-runtime failure without a registry code.
    pub(crate) fn runtime_error(message: impl Into<String>) -> Self {
        Self::new("runtime_error", message)
    }

    pub(crate) fn with_param(mut self, param: impl Into<String>) -> Self {
        self.param = Some(param.into());
        self
    }

    pub(crate) fn with_invalid_request_param(self, param: impl Into<String>) -> Self {
        if self.code == ErrorCode::InvalidRequest.as_str() {
            self.with_param(param)
        } else {
            self
        }
    }
}

impl From<ClientError> for BackendError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::ConfigIo(message) | ClientError::ConfigDecode(message) => {
                Self::invalid_config(message)
            }
            ClientError::MissingConfigField { field } => {
                Self::invalid_config(format!("missing `{field}`"))
            }
            ClientError::ConfigValidation { field, reason } => {
                Self::invalid_config(format!("invalid `{field}`: {reason}"))
            }
            ClientError::InvalidNamespacePath(message) => Self::invalid_request(message),
            ClientError::InvalidCommitId(message) | ClientError::InvalidCheckpointId(message) => {
                // The registry code core and server report for a malformed
                // id, so pre-flight client validation matches backend
                // behavior.
                Self::new(ErrorCode::InvalidRequest.as_str(), message)
            }
            ClientError::Http(message)
            | ClientError::Json(message)
            | ClientError::Protocol(message) => Self::client_error(message),
            ClientError::Api {
                status: _,
                code,
                feature,
                message,
                param,
                request_id,
                details,
            } => Self {
                code,
                feature,
                message,
                param,
                request_id,
                details,
            },
            ClientError::UploadTooLarge { size_bytes, reason } => Self::new(
                ErrorCode::ContentTooLarge.as_str(),
                format!(
                    "payload of {size_bytes} bytes exceeds every upload transport this deployment offers: {reason}"
                ),
            ),
            ClientError::Io(message) => Self::io_error(format!("i/o error: {message}")),
            // `ClientError` is non-exhaustive across crate boundaries, so future
            // variants map to a generic transport failure. Add an explicit arm when a
            // new variant needs a more specific CLI code.
            other => Self::client_error(other.to_string()),
        }
    }
}

pub(crate) fn map_runtime_error(error: RuntimeError) -> BackendError {
    let public_message = error.public_message().into_owned();
    match error {
        RuntimeError::Config(_) => BackendError::invalid_config(public_message),
        RuntimeError::RuntimeTask(_) => BackendError::runtime_error(public_message),
        // The embedded surface reports the same structured details a server
        // puts in its error envelope for the same condition, so `--json`
        // consumers read one contract from both backends.
        error => BackendError {
            code: error.code().as_str().to_owned(),
            feature: None,
            message: public_message,
            param: None,
            request_id: None,
            details: error.details().map(Box::new),
        },
    }
}

pub(crate) fn map_namespace_scoped_runtime_error(
    namespace_id: &NamespaceId,
    error: RuntimeError,
) -> BackendError {
    if error.code() == ErrorCode::NamespaceNotFound {
        return BackendError::new(
            ErrorCode::NamespaceNotFound.as_str(),
            format!("namespace `{namespace_id}` does not exist"),
        );
    }

    map_runtime_error(error)
}

/// Grep's own failures carry registry codes of their own; everything it
/// surfaces from the filesystem handles is shaped like any other runtime
/// error, so embedded and remote report one code per condition.
pub(crate) fn map_namespace_scoped_grep_error(
    namespace_id: &NamespaceId,
    error: GrepError,
) -> BackendError {
    match error {
        GrepError::Runtime(error) => {
            let cursor_is_invalid = matches!(
                &error,
                RuntimeError::Core(loonfs::CoreError::InvalidCursor(_))
            );
            let response = map_namespace_scoped_runtime_error(namespace_id, error);
            if cursor_is_invalid {
                response.with_param("/cursor")
            } else {
                response
            }
        }
        error => {
            let message = error.public_message().into_owned();
            BackendError::new(error.code().as_str(), message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{map_runtime_error, BackendError, ClientError};
    use loonfs::RuntimeError;
    use loonfs_api::ChangeSeq;

    #[test]
    fn embedded_runtime_errors_carry_their_structured_details() {
        let error = map_runtime_error(RuntimeError::Core(
            loonfs::CoreError::StaleHeadPrecondition {
                expected: ChangeSeq(41),
                actual: ChangeSeq(45),
            },
        ));

        assert_eq!(error.code, "stale_head");
        let details = error.details.expect("runtime error includes details");
        assert_eq!(details.expected_head_seq, Some(ChangeSeq(41)));
        assert_eq!(details.actual_head_seq, Some(ChangeSeq(45)));
    }

    #[test]
    fn api_errors_pass_their_code_and_message_through_verbatim() {
        let error = BackendError::from(ClientError::Api {
            status: 404,
            code: "namespace_not_found".to_owned(),
            feature: None,
            message: "namespace `demo` does not exist".to_owned(),
            param: None,
            request_id: None,
            details: None,
        });

        assert_eq!(error.code, "namespace_not_found");
        assert_eq!(error.message, "namespace `demo` does not exist");
    }

    #[test]
    fn config_and_transport_errors_map_to_backend_local_codes() {
        let error = BackendError::from(ClientError::MissingConfigField {
            field: "server_url",
        });
        assert_eq!(error.code, "invalid_config");
        assert_eq!(error.message, "missing `server_url`");

        let error = BackendError::from(ClientError::Http("connection refused".to_owned()));
        assert_eq!(error.code, "client_error");

        let error = BackendError::from(ClientError::Io("read failed".to_owned()));
        assert_eq!(error.code, "io_error");
        assert_eq!(error.message, "i/o error: read failed");

        let error = BackendError::from(ClientError::UploadTooLarge {
            size_bytes: 1024,
            reason: "proxied and direct limits are lower".to_owned(),
        });
        assert_eq!(error.code, loonfs_api::ErrorCode::ContentTooLarge.as_str());
    }

    #[test]
    fn invalid_namespace_paths_map_to_the_registry_request_code() {
        let error = BackendError::from(ClientError::InvalidNamespacePath(
            "path must be absolute".to_owned(),
        ));

        assert_eq!(error.code, loonfs_api::ErrorCode::InvalidRequest.as_str());
    }
}
