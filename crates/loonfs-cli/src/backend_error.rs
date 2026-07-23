//! Runtime-error message and registry-code shaping for CLI backends.

use loonfs::RuntimeError;
use loonfs_api::{ErrorCode, NamespaceId};
use loonfs_client::backend::BackendError;

pub(crate) fn map_runtime_error(error: RuntimeError) -> BackendError {
    match error {
        RuntimeError::Config(message) => BackendError::invalid_config(message),
        RuntimeError::RuntimeTask(message) => BackendError::runtime_error(message),
        error => BackendError::new(error.code().as_str(), error.to_string()),
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
