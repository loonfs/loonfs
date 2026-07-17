//! The backend seam: one logical LoonFS API over interchangeable transports.
//!
//! [`Backend`] is the host-facing abstraction every LoonFS surface programs
//! against: the `loon` CLI today, and future hosts (SDKs, FUSE adapters,
//! agents) tomorrow. A host writes its features once against this trait and
//! lets the selected profile decide whether calls travel over HTTP or run
//! against an embedded runtime.
//!
//! This crate ships the trait and its HTTP implementation, [`RemoteBackend`].
//! The embedded implementation lives with the host that embeds the `loonfs`
//! runtime (currently `loonfs-cli`), which keeps this crate's dependency
//! surface wire-only (`loonfs-api`).
//!
//! The trait is async so hosts drive every transport from their own runtime.
//! [`Client`] stays a synchronous wire client; [`RemoteBackend`] bridges by
//! running each wire call on the runtime's blocking pool instead of stalling
//! an async worker.

use crate::{Client, ClientError, MutationOptions, NamespacePath};
use async_trait::async_trait;
use loonfs_api::{
    v0::ChangesResponse, AdvanceRetentionResponse, AuthoritativePathEntry, ChangeSeq, CommitId,
    CommitResponse, CopyBehavior, CreateCheckpointRequest, CreateCheckpointResponse,
    DeleteNamespaceResponse, DisableGramsIndexResponse, EnableGramsIndexResponse, ErrorCode,
    ErrorDetails, FlushWalResponse, GcRequest, GcResponse, GrepRequest, GrepResponse, InodeId,
    ListFileRevisionsResponse, MaintenanceTickRequest, MaintenanceTickResponse, MoveBehavior,
    NamespaceStatusResponse, NamespaceSummary, PutBehavior, ReleaseCheckpointResponse, RevisionNo,
};
use thiserror::Error;

/// Failure surfaced by a [`Backend`], as a `(code, message)` pair.
///
/// `code` draws from exactly two namespaces:
///
/// - **Registry codes** ([`loonfs_api::ErrorCode`]) pass through verbatim
///   from whichever transport produced them, so embedded and remote backends
///   surface the same code for the same failure. Never restate a registry
///   code as a string literal; use `ErrorCode::X.as_str()` or an error's
///   `code()`.
/// - **Backend-local codes** cover failures that never produce a registry
///   code. The complete list, each owned by a constructor below, is:
///   `invalid_config` (deliberately the same string as the registry code of
///   the same meaning), `invalid_input`, `client_error`, `io_error`, and
///   `runtime_error`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub struct BackendError {
    /// Registry or backend-local error code.
    pub code: String,
    /// Human-readable description of the failure.
    pub message: String,
    /// Correlation id the server assigned to the failed request. Always
    /// `None` for embedded and local failures, which have no server hop.
    pub request_id: Option<String>,
    /// Structured context for the code, when the transport carried any.
    pub details: Option<Box<ErrorDetails>>,
}

impl BackendError {
    /// Builds an error carrying a registry code verbatim.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            request_id: None,
            details: None,
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
            ClientError::InvalidCommitId(message) | ClientError::InvalidCheckpointId(message) => {
                // The registry code core and server report for a malformed
                // id, so pre-flight client validation matches backend
                // behavior.
                Self::new(ErrorCode::InvalidRequest.as_str(), message)
            }
            ClientError::Http(message) | ClientError::Json(message) => Self::client_error(message),
            ClientError::Api {
                code,
                message,
                request_id,
                details,
                ..
            } => Self {
                code,
                message,
                request_id,
                details,
            },
            ClientError::Io(message) => Self::io_error(format!("i/o error: {message}")),
        }
    }
}

/// One logical LoonFS API over two transports.
///
/// Implementations must report the same registry error code for the same
/// failure, so a host renders identical outcomes regardless of which
/// transport a profile selects (`loonfs-cli`'s two-mode parity tests hold
/// this line).
#[async_trait]
pub trait Backend {
    /// Creates a new empty namespace.
    async fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError>;
    /// Marks a namespace deleted; `expected_head_seq` guards against deleting
    /// a namespace that moved since the caller last observed it.
    async fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, BackendError>;
    /// Creates a new namespace as a fork of the source's durable view.
    async fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, BackendError>;
    /// Summarizes a namespace's current head state.
    async fn namespace_status(
        &self,
        namespace_id: &str,
    ) -> Result<NamespaceStatusResponse, BackendError>;
    /// Lists the entries of a directory.
    async fn list_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError>;
    /// Describes a single path entry.
    async fn stat_path(&self, spec: &NamespacePath)
        -> Result<AuthoritativePathEntry, BackendError>;
    /// Reads a file's current content.
    async fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError>;
    /// Content search over a namespace's gram index.
    async fn grep(
        &self,
        namespace_id: &str,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError>;
    /// Enables the gram index on a namespace (admin plane).
    async fn enable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<EnableGramsIndexResponse, BackendError>;
    /// Disables the gram index on a namespace (admin plane).
    async fn disable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<DisableGramsIndexResponse, BackendError>;
    /// Reads a retained file revision's content.
    async fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError>;
    /// Lists one page of a file's retained revisions.
    async fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError>;
    /// Writes a file; `behavior` selects create-only or replace semantics.
    /// An explicit `commit_id` makes the call retryable by resubmission;
    /// absent, one is generated and returned in the response.
    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        behavior: PutBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Creates a directory; `parents` also creates missing ancestors.
    async fn create_directory(
        &self,
        spec: &NamespacePath,
        parents: bool,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Deletes a file or empty directory.
    async fn delete_path(
        &self,
        spec: &NamespacePath,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Moves a path within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: MoveBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Copies a file within a namespace; `behavior` selects create-only or
    /// replace semantics for the destination.
    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: CopyBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Restores a file to one of its retained revisions.
    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;
    /// Recovers a deleted file or subtree to the spec's path; `inode_id`
    /// and `deleted_at_seq` are the identity and committed sequence the
    /// delete reported.
    async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError>;

    // --- maintenance/admin plane (`admin/v0`) ---

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view.
    async fn create_checkpoint(
        &self,
        namespace_id: &str,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError>;
    /// Releases a user-owned checkpoint pin by id. Idempotent.
    async fn release_checkpoint(
        &self,
        namespace_id: &str,
        checkpoint_id: &str,
    ) -> Result<ReleaseCheckpointResponse, BackendError>;
    /// Flushes the WAL tail and advances the metadata root, creating no
    /// checkpoint record.
    async fn flush_wal(&self, namespace_id: &str) -> Result<FlushWalResponse, BackendError>;
    /// Advances the namespace retention floor. Irreversible: WAL history
    /// before the floor stops being replayable.
    async fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError>;
    /// Runs one bounded maintenance step: a root advancement once the WAL
    /// tail reaches the threshold, optionally followed by a GC pass.
    async fn maintenance_tick(
        &self,
        namespace_id: &str,
        request: MaintenanceTickRequest,
    ) -> Result<MaintenanceTickResponse, BackendError>;
    /// Runs one mark-and-sweep garbage-collection pass. Nothing sweeps
    /// without this explicit call or a maintenance-tick opt-in.
    async fn gc_namespace(
        &self,
        namespace_id: &str,
        request: GcRequest,
    ) -> Result<GcResponse, BackendError>;
    /// Reads the ordered change feed after the `after_seq` cursor.
    async fn list_changes(
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

/// Carries a backend-level commit id into the wire client's options.
fn mutation_options(commit_id: Option<CommitId>) -> MutationOptions {
    MutationOptions {
        commit_id: commit_id.map(|id| id.to_string()),
    }
}

impl RemoteBackend {
    /// Runs one synchronous wire call on the blocking pool, so async hosts
    /// never stall an executor worker on HTTP I/O.
    async fn wire<T, F>(&self, call: F) -> Result<T, BackendError>
    where
        T: Send + 'static,
        F: FnOnce(Client) -> Result<T, ClientError> + Send + 'static,
    {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || call(client))
            .await
            .map_err(|error| BackendError::runtime_error(error.to_string()))?
            .map_err(BackendError::from)
    }
}

#[async_trait]
impl Backend for RemoteBackend {
    async fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.create_namespace(&namespace_id))
            .await
    }

    async fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| {
            client.delete_namespace(&namespace_id, expected_head_seq.map(ChangeSeq))
        })
        .await
    }

    async fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, BackendError> {
        let source = source.to_owned();
        let new_namespace_id = new_namespace_id.to_owned();
        self.wire(move |client| client.fork_namespace(&source, &new_namespace_id))
            .await
    }

    async fn namespace_status(
        &self,
        namespace_id: &str,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.namespace_status(&namespace_id))
            .await
    }

    async fn list_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        let spec = spec.clone();
        Ok(self
            .wire(move |client| client.list_path(&spec))
            .await?
            .entries)
    }

    async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        let spec = spec.clone();
        self.wire(move |client| client.stat_path(&spec)).await
    }

    async fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
        let spec = spec.clone();
        self.wire(move |client| client.read_file_bytes(&spec)).await
    }

    async fn grep(
        &self,
        namespace_id: &str,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        let request = request.clone();
        self.wire(move |client| client.grep(&namespace_id, &request))
            .await
    }

    async fn enable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<EnableGramsIndexResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.enable_grams_index(&namespace_id))
            .await
    }

    async fn disable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<DisableGramsIndexResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.disable_grams_index(&namespace_id))
            .await
    }

    async fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        let spec = spec.clone();
        self.wire(move |client| client.read_file_revision_bytes(&spec, revision_no))
            .await
    }

    async fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        let spec = spec.clone();
        let cursor = cursor.map(ToOwned::to_owned);
        self.wire(move |client| client.list_file_revisions_page(&spec, limit, cursor.as_deref()))
            .await
    }

    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        behavior: PutBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let spec = spec.clone();
        let bytes = bytes.to_vec();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.put_file_bytes(&spec, &bytes, behavior, &options))
            .await
    }

    async fn create_directory(
        &self,
        spec: &NamespacePath,
        parents: bool,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let spec = spec.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.create_directory(&spec, parents, &options))
            .await
    }

    async fn delete_path(
        &self,
        spec: &NamespacePath,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let spec = spec.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.delete_path(&spec, &options))
            .await
    }

    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: MoveBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let from = from.clone();
        let to = to.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.move_path(&from, &to, behavior, &options))
            .await
    }

    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: CopyBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let from = from.clone();
        let to = to.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.copy_path(&from, &to, behavior, &options))
            .await
    }

    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let spec = spec.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.restore_file_revision(&spec, source_revision_no, &options))
            .await
    }

    async fn undelete(
        &self,
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let spec = spec.clone();
        let options = mutation_options(commit_id);
        self.wire(move |client| client.undelete(&spec, inode_id, deleted_at_seq, &options))
            .await
    }

    async fn create_checkpoint(
        &self,
        namespace_id: &str,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.create_checkpoint(&namespace_id, &request))
            .await
    }

    async fn release_checkpoint(
        &self,
        namespace_id: &str,
        checkpoint_id: &str,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        let checkpoint_id = checkpoint_id.to_owned();
        self.wire(move |client| client.release_checkpoint(&namespace_id, &checkpoint_id))
            .await
    }

    async fn flush_wal(&self, namespace_id: &str) -> Result<FlushWalResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.flush_wal(&namespace_id))
            .await
    }

    async fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.advance_retention(&namespace_id))
            .await
    }

    async fn maintenance_tick(
        &self,
        namespace_id: &str,
        request: MaintenanceTickRequest,
    ) -> Result<MaintenanceTickResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.maintenance_tick(&namespace_id, &request))
            .await
    }

    async fn gc_namespace(
        &self,
        namespace_id: &str,
        request: GcRequest,
    ) -> Result<GcResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.gc_namespace(&namespace_id, &request))
            .await
    }

    async fn list_changes(
        &self,
        namespace_id: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        let namespace_id = namespace_id.to_owned();
        self.wire(move |client| client.list_changes_page(&namespace_id, after_seq, limit))
            .await
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
    }
}
