//! Transport-neutral backend API plus the HTTP implementation.
//!
//! Hosts can program against [`Backend`] and choose whether calls go over HTTP
//! or to an embedded runtime.

use crate::{Client, ClientError, NamespacePath};
use loonfs_api::{
    v0::ChangesResponse, AdvanceRetentionResponse, AuthoritativePathEntry, ChangeSeq,
    CreateCheckpointResponse, DeleteNamespaceResponse, ErrorCode, ListFileRevisionsResponse,
    MutationResult, NamespaceStatusResponse, NamespaceSummary, RevisionNo,
};
use thiserror::Error;

/// Failure surfaced by a [`Backend`], as a stable `(code, message)` pair.
///
/// Registry codes pass through verbatim; backend-local codes cover config,
/// input, transport, IO, and embedded-runtime failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub struct BackendError {
    /// Registry or backend-local error code.
    pub code: String,
    /// Human-readable description of the failure.
    pub message: String,
}

impl BackendError {
    /// Builds an error carrying a registry code verbatim.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// A backend configuration that could not be loaded or used.
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::new("invalid_config", message)
    }

    /// Caller input rejected before it reached a backend.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new("invalid_input", message)
    }

    /// Transport failure between a client and a remote server.
    pub fn client_error(message: impl Into<String>) -> Self {
        Self::new("client_error", message)
    }

    /// Local i/o failure while moving bytes for a backend call.
    pub fn io_error(message: impl Into<String>) -> Self {
        Self::new("io_error", message)
    }

    /// Embedded-runtime failure without a registry code.
    pub fn runtime_error(message: impl Into<String>) -> Self {
        Self::new("runtime_error", message)
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
            ClientError::InvalidNamespacePath(message) => Self::invalid_input(message),
            ClientError::InvalidCommitId(message) => {
                // The registry code core and server report for a malformed
                // commit id, so pre-flight client validation matches backend
                // behavior.
                Self::new(ErrorCode::InvalidRequest.as_str(), message)
            }
            ClientError::Http(message) | ClientError::Json(message) => Self::client_error(message),
            ClientError::Api { code, message, .. } => Self::new(code, message),
            ClientError::Io(message) => Self::io_error(format!("i/o error: {message}")),
        }
    }
}

/// One logical LoonFS API over embedded and remote transports.
pub trait Backend {
    /// Creates a new empty namespace.
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError>;
    /// Marks a namespace deleted; `expected_head_seq` guards against deleting
    /// a namespace that moved since the caller last observed it.
    fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, BackendError>;
    /// Creates a new namespace as a fork of the source's durable view.
    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, BackendError>;
    /// Summarizes a namespace's current head state.
    fn namespace_status(&self, namespace_id: &str)
        -> Result<NamespaceStatusResponse, BackendError>;
    /// Lists the entries of a directory.
    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, BackendError>;
    /// Describes a single path entry.
    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, BackendError>;
    /// Reads a file's current content.
    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError>;
    /// Reads a retained file revision's content.
    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError>;
    /// Lists one page of a file's retained revisions.
    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError>;
    /// Writes a file; `force` replaces existing content.
    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, BackendError>;
    /// Creates a directory.
    fn create_directory(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError>;
    /// Deletes a file or empty directory.
    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError>;
    /// Moves a path within a namespace.
    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, BackendError>;
    /// Copies a file within a namespace.
    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, BackendError>;
    /// Restores a file to one of its retained revisions.
    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, BackendError>;

    // --- maintenance/admin plane (`admin/v0`) ---

    /// Creates or reuses a checkpoint pinning the namespace's current view.
    fn create_checkpoint(
        &self,
        namespace_id: &str,
    ) -> Result<CreateCheckpointResponse, BackendError>;
    /// Advances the namespace retention floor. Irreversible: WAL history
    /// before the floor stops being replayable.
    fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError>;
    /// Reads the ordered change feed after the `after_seq` cursor.
    fn list_changes(
        &self,
        namespace_id: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError>;
}

/// [`Backend`] over HTTP: wraps a [`Client`] pointed at a LoonFS server.
#[derive(Debug)]
pub struct RemoteBackend {
    client: Client,
}

impl RemoteBackend {
    /// Wraps a configured HTTP client.
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl Backend for RemoteBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError> {
        self.client
            .create_namespace(namespace_id)
            .map_err(BackendError::from)
    }

    fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        self.client
            .delete_namespace(namespace_id, expected_head_seq.map(ChangeSeq))
            .map_err(BackendError::from)
    }

    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, BackendError> {
        self.client
            .fork_namespace(source, new_namespace_id)
            .map_err(BackendError::from)
    }

    fn namespace_status(
        &self,
        namespace_id: &str,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        self.client
            .namespace_status(namespace_id)
            .map_err(BackendError::from)
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        Ok(self
            .client
            .list_path(spec)
            .map_err(BackendError::from)?
            .entries)
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, BackendError> {
        self.client.stat_path(spec).map_err(BackendError::from)
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
        self.client
            .read_file_bytes(spec)
            .map_err(BackendError::from)
    }

    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        self.client
            .read_file_revision_bytes(spec, revision_no)
            .map_err(BackendError::from)
    }

    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        self.client
            .list_file_revisions_page(spec, limit, cursor)
            .map_err(BackendError::from)
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, BackendError> {
        self.client
            .put_file_bytes(spec, bytes, force)
            .map_err(BackendError::from)
    }

    fn create_directory(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError> {
        self.client
            .create_directory(spec)
            .map_err(BackendError::from)
    }

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError> {
        self.client.delete_path(spec).map_err(BackendError::from)
    }

    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, BackendError> {
        self.client.move_path(from, to).map_err(BackendError::from)
    }

    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, BackendError> {
        self.client.copy_path(from, to).map_err(BackendError::from)
    }

    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, BackendError> {
        self.client
            .restore_file_revision(spec, source_revision_no)
            .map_err(BackendError::from)
    }

    fn create_checkpoint(
        &self,
        namespace_id: &str,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        self.client
            .create_checkpoint(namespace_id)
            .map_err(BackendError::from)
    }

    fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError> {
        self.client
            .advance_retention(namespace_id)
            .map_err(BackendError::from)
    }

    fn list_changes(
        &self,
        namespace_id: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        self.client
            .list_changes_page(namespace_id, after_seq, limit)
            .map_err(BackendError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendError, ClientError};

    #[test]
    fn api_errors_pass_their_code_and_message_through_verbatim() {
        let error = BackendError::from(ClientError::Api {
            status: 404,
            code: "namespace_not_found".to_owned(),
            feature: None,
            message: "namespace `demo` does not exist".to_owned(),
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
    }
}
