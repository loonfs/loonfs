//! Runtime and client error conversion for CLI backends.

use crate::error::CliError;
use loonfs::RuntimeError;
use loonfs_api::{ErrorCode, NamespaceId};
use loonfs_client::ClientError;
use loonfs_grep::GrepError;

pub(crate) trait NamespaceScoped<T> {
    fn scoped(self, namespace_id: &NamespaceId) -> Result<T, CliError>;
}

impl<T> NamespaceScoped<T> for Result<T, RuntimeError> {
    fn scoped(self, namespace_id: &NamespaceId) -> Result<T, CliError> {
        self.map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }
}

impl<T> NamespaceScoped<T> for Result<T, GrepError> {
    fn scoped(self, namespace_id: &NamespaceId) -> Result<T, CliError> {
        self.map_err(|error| map_namespace_scoped_grep_error(namespace_id, error))
    }
}

impl From<ClientError> for CliError {
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

pub(crate) fn map_runtime_error(error: RuntimeError) -> CliError {
    let public_message = error.public_message().into_owned();
    match error {
        RuntimeError::Config(_) => CliError::invalid_config(public_message),
        RuntimeError::RuntimeTask(_) => CliError::runtime_error(public_message),
        // The embedded surface reports the same structured details a server
        // puts in its error envelope for the same condition, so `--json`
        // consumers read one contract from both backends.
        error => CliError {
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
) -> CliError {
    if error.code() == ErrorCode::NamespaceNotFound {
        return CliError::new(
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
) -> CliError {
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
            CliError::new(error.code().as_str(), message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{map_runtime_error, CliError, ClientError};
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
        let error = CliError::from(ClientError::Api {
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
        let error = CliError::from(ClientError::MissingConfigField {
            field: "server_url",
        });
        assert_eq!(error.code, "invalid_config");
        assert_eq!(error.message, "missing `server_url`");

        let error = CliError::from(ClientError::Http("connection refused".to_owned()));
        assert_eq!(error.code, "client_error");

        let error = CliError::from(ClientError::Io("read failed".to_owned()));
        assert_eq!(error.code, "io_error");
        assert_eq!(error.message, "i/o error: read failed");

        let error = CliError::from(ClientError::UploadTooLarge {
            size_bytes: 1024,
            reason: "proxied and direct limits are lower".to_owned(),
        });
        assert_eq!(error.code, loonfs_api::ErrorCode::ContentTooLarge.as_str());
    }

    #[test]
    fn invalid_namespace_paths_map_to_the_registry_request_code() {
        let error = CliError::from(ClientError::InvalidNamespacePath(
            "path must be absolute".to_owned(),
        ));

        assert_eq!(error.code, loonfs_api::ErrorCode::InvalidRequest.as_str());
    }
}
