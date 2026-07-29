//! Runtime-error message and registry-code shaping for CLI backends.

use loonfs::RuntimeError;
use loonfs_api::{ErrorCode, NamespaceId};
use loonfs_client::backend::BackendError;
use loonfs_grep::GrepError;

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

/// Grep's own failures carry registry codes of their own; everything it
/// surfaces from the filesystem handles is shaped like any other runtime
/// error, so embedded and remote report one code per condition.
pub(crate) fn map_namespace_scoped_grep_error(
    namespace_id: &NamespaceId,
    error: GrepError,
) -> BackendError {
    match error {
        GrepError::Runtime(error) => map_namespace_scoped_runtime_error(namespace_id, error),
        error => BackendError::new(error.code().as_str(), error.to_string()),
    }
}
