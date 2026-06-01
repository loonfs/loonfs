use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use loon_api::{
    AuthoritativePathEntry, CommitId, ListFileRevisionsResponse, MutationResult, NamespaceId,
    NamespaceSummary, RevisionNo,
};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loonfs::{
    BootstrapNamespaceError, CopyOptions, CoreError, CoreErrorKind, CreateDirOptions,
    CreateNamespaceOptions, DeleteOptions, Fs, FsConfig, MoveOptions, PutFileBehavior,
    PutFileOptions, RestoreRevisionOptions, RuntimeCacheConfig, RuntimeError, SharedObjectStore,
};
use std::sync::Arc;

const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

pub trait Backend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError>;
    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, CliError>;
    fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, CliError>;
    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError>;
    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError>;
    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError>;
    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, CliError>;
    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListFileRevisionsResponse, CliError>;
    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError>;
    fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, CliError>;
    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError>;
    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError>;
    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError>;
    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, CliError>;
}

// --- Remote backend (HTTP via loon-client) ---

pub struct RemoteBackend {
    client: Client,
}

impl Backend for RemoteBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError> {
        self.client
            .create_namespace(namespace_id)
            .map_err(map_client_error)
    }

    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, CliError> {
        self.client
            .fork_namespace(source, new_namespace_id)
            .map_err(map_client_error)
    }

    fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, CliError> {
        self.client.list_namespaces().map_err(map_client_error)
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        self.client.list_path(spec).map_err(map_client_error)
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError> {
        self.client.stat_path(spec).map_err(map_client_error)
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError> {
        self.client.read_file_bytes(spec).map_err(map_client_error)
    }

    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, CliError> {
        self.client
            .read_file_revision_bytes(spec, revision_no)
            .map_err(map_client_error)
    }

    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListFileRevisionsResponse, CliError> {
        self.client
            .list_file_revisions(spec)
            .map_err(map_client_error)
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError> {
        self.client
            .put_file_bytes(spec, bytes, force)
            .map_err(map_client_error)
    }

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        self.client.delete_path(spec).map_err(map_client_error)
    }

    fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        self.client.create_dir(spec).map_err(map_client_error)
    }

    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        self.client.move_path(from, to).map_err(map_client_error)
    }

    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        self.client.copy_path(from, to).map_err(map_client_error)
    }

    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, CliError> {
        self.client
            .restore_file_revision(spec, source_revision_no)
            .map_err(map_client_error)
    }
}

fn map_client_error(error: ClientError) -> CliError {
    match error {
        ClientError::ConfigIo(message) | ClientError::ConfigDecode(message) => {
            CliError::invalid_config(message)
        }
        ClientError::MissingConfigField { field } => {
            CliError::invalid_config(format!("missing `{field}`"))
        }
        ClientError::ConfigValidation { field, reason } => {
            CliError::invalid_config(format!("invalid `{field}`: {reason}"))
        }
        ClientError::InvalidNamespacePath(message) | ClientError::InvalidCommitId(message) => {
            CliError::invalid_input(message)
        }
        ClientError::Http(message) | ClientError::Json(message) => CliError::client_error(message),
        ClientError::Api { code, message, .. } => CliError::new(code, message),
        ClientError::Io(message) => CliError::new("io_error", format!("i/o error: {message}")),
    }
}

// --- Embedded backend (embedded/direct mode uses the shared loonfs runtime) ---

pub struct EmbeddedBackend {
    fs: Fs,
}

impl Backend for EmbeddedBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError> {
        let ns_id = parse_namespace_id(namespace_id)?;
        self.fs
            .create_namespace(&ns_id, CreateNamespaceOptions::default())
            .map_err(map_runtime_error)
    }

    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, CliError> {
        let source_namespace_id = parse_namespace_id(source)?;
        let new_namespace_id = parse_namespace_id(new_namespace_id)?;
        self.fs
            .fork_namespace(&source_namespace_id, &new_namespace_id)
            .map_err(map_runtime_error)
    }

    fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, CliError> {
        self.fs.list_namespaces().map_err(map_runtime_error)
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        self.fs
            .list_path(&ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        self.fs
            .stat_path(&ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let result = self
            .fs
            .read_file_bytes(&ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))?;
        Ok(result.bytes)
    }

    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let result = self
            .fs
            .read_file_revision_bytes(&ns_id, &spec.absolute_path, revision_no)
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))?;
        Ok(result.bytes)
    }

    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListFileRevisionsResponse, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        self.fs
            .list_file_revisions(&ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let behavior = if force {
            PutFileBehavior::ReplaceExisting
        } else {
            PutFileBehavior::CreateOnly
        };
        let commit_id = generated_commit_id();
        self.fs
            .put_file_bytes(
                &ns_id,
                &spec.absolute_path,
                bytes,
                PutFileOptions {
                    behavior,
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.fs
            .delete_path(
                &ns_id,
                &spec.absolute_path,
                DeleteOptions {
                    recursive: false,
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.fs
            .create_dir(
                &ns_id,
                &spec.absolute_path,
                CreateDirOptions {
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&from.namespace)?;
        let commit_id = generated_commit_id();
        self.fs
            .move_path(
                &ns_id,
                &from.absolute_path,
                &to.absolute_path,
                MoveOptions {
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&from.namespace, error))
    }

    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&from.namespace)?;
        let commit_id = generated_commit_id();
        self.fs
            .copy_path(
                &ns_id,
                &from.absolute_path,
                &to.absolute_path,
                CopyOptions {
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&from.namespace, error))
    }

    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, CliError> {
        let ns_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.fs
            .restore_file_revision(
                &ns_id,
                &spec.absolute_path,
                source_revision_no,
                RestoreRevisionOptions {
                    commit_id: Some(commit_id),
                },
            )
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }
}

fn parse_namespace_id(namespace: &str) -> Result<NamespaceId, CliError> {
    NamespaceId::parse(namespace).map_err(|error| CliError::invalid_input(error.to_string()))
}

fn generated_commit_id() -> CommitId {
    CommitId::generate()
}

fn map_runtime_error(error: RuntimeError) -> CliError {
    match error {
        RuntimeError::Core(error) => map_core_error(error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => CliError::invalid_config(message),
    }
}

fn map_namespace_scoped_runtime_error(namespace: &str, error: RuntimeError) -> CliError {
    match error {
        RuntimeError::Core(error) => map_namespace_scoped_core_error(namespace, error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => CliError::invalid_config(message),
    }
}

fn map_core_error(error: CoreError) -> CliError {
    if matches!(
        error.kind(),
        CoreErrorKind::InvalidNamespaceId
            | CoreErrorKind::InvalidCommitId
            | CoreErrorKind::InvalidUploadId
    ) {
        return CliError::invalid_input(error.to_string());
    }

    let code = match error.kind() {
        CoreErrorKind::InvalidPath => "invalid_path",
        CoreErrorKind::InvalidNamespaceId => unreachable!("handled before code mapping"),
        CoreErrorKind::InvalidCommitId => unreachable!("handled before code mapping"),
        CoreErrorKind::InvalidUploadId => unreachable!("handled before code mapping"),
        CoreErrorKind::NamespaceNotFound => "namespace_not_found",
        CoreErrorKind::NamespaceExists => "namespace_exists",
        CoreErrorKind::NamespacePartial => "namespace_partial",
        CoreErrorKind::PathNotFound => "path_not_found",
        CoreErrorKind::RevisionNotFound => "revision_not_found",
        CoreErrorKind::PathConflict => "path_conflict",
        CoreErrorKind::DirectoryNotEmpty => "directory_not_empty",
        CoreErrorKind::StaleHead => "stale_head",
        CoreErrorKind::StaleRevision => "stale_revision",
        CoreErrorKind::TombstoneConflict => "tombstone_conflict",
        CoreErrorKind::LeaseConflict => "lease_conflict",
        CoreErrorKind::WouldCycle => "would_cycle",
        CoreErrorKind::UnsupportedRenameMode => "unsupported_rename_mode",
        CoreErrorKind::CommitIdReuseConflict => "commit_id_reuse_conflict",
        CoreErrorKind::CommitQueueFull => "commit_queue_full",
        CoreErrorKind::CheckpointUnavailable => "checkpoint_unavailable",
        CoreErrorKind::UploadNotFound => "upload_not_found",
        CoreErrorKind::UploadAlreadyCompleted => "upload_already_completed",
        CoreErrorKind::UploadContentConflict => "upload_content_conflict",
        CoreErrorKind::InvalidUploadContent => "invalid_upload_content",
        CoreErrorKind::RebootstrapRequired => "rebootstrap_required",
        CoreErrorKind::NamespaceCorrupt => "namespace_corrupt",
        CoreErrorKind::ServerError => "server_error",
    };
    CliError::new(code, error.to_string())
}

fn map_namespace_scoped_core_error(namespace: &str, error: CoreError) -> CliError {
    if matches!(error.kind(), CoreErrorKind::NamespaceNotFound) {
        return CliError::new(
            "namespace_not_found",
            format!("namespace `{namespace}` does not exist"),
        );
    }

    map_core_error(error)
}

fn map_bootstrap_error(error: BootstrapNamespaceError) -> CliError {
    match &error {
        BootstrapNamespaceError::InvalidNamespaceId(_) => {
            CliError::invalid_input(error.to_string())
        }
        BootstrapNamespaceError::NamespaceAlreadyExists { .. } => {
            CliError::new("namespace_exists", error.to_string())
        }
        BootstrapNamespaceError::NamespacePartiallyInitialized { .. } => {
            CliError::new("namespace_partial", error.to_string())
        }
        BootstrapNamespaceError::EmptyHolderId | BootstrapNamespaceError::EmptyWriterVersion => {
            CliError::invalid_config(error.to_string())
        }
        _ => CliError::new("bootstrap_failed", error.to_string()),
    }
}

fn default_writer_id() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "loon-cli".to_owned())
}

// --- Target resolution ---

pub enum ResolvedTarget {
    Embedded(Box<EmbeddedTarget>),
    Remote(RemoteTarget),
}

pub struct EmbeddedTarget {
    backend: EmbeddedBackend,
}

pub struct RemoteTarget {
    backend: RemoteBackend,
}

impl ResolvedTarget {
    pub fn resolve(profile_name: &str, profile: &ProfileConfig) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Embedded {
                store,
                writer_id,
                writer_version,
                lease_duration_ms,
                ..
            } => Ok(Self::Embedded(Box::new(EmbeddedTarget::new(
                store,
                writer_id.as_deref(),
                writer_version.as_deref(),
                *lease_duration_ms,
            )?))),
            ProfileConfig::Remote {
                server_url,
                auth_token,
                ..
            } => Ok(Self::Remote(RemoteTarget::new(
                profile_name,
                server_url,
                auth_token.as_deref(),
            )?)),
        }
    }

    pub fn mode_str(&self) -> &'static str {
        match self {
            ResolvedTarget::Embedded(_) => "embedded",
            ResolvedTarget::Remote(_) => "remote",
        }
    }

    pub fn backend(&self) -> &dyn Backend {
        match self {
            ResolvedTarget::Embedded(target) => &target.backend,
            ResolvedTarget::Remote(target) => &target.backend,
        }
    }
}

impl EmbeddedTarget {
    fn new(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
        writer_version: Option<&str>,
        lease_duration_ms: Option<u64>,
    ) -> Result<Self, CliError> {
        let store = store_config.object_store()?;
        let store: SharedObjectStore = Arc::new(store);
        let fs = Fs::open(
            store,
            FsConfig {
                writer_id: writer_id
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(default_writer_id),
                writer_version: writer_version
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| format!("loon/{}", env!("CARGO_PKG_VERSION"))),
                lease_duration_ms: lease_duration_ms.unwrap_or(DEFAULT_LEASE_DURATION_MS),
                runtime_cache: RuntimeCacheConfig::default(),
            },
        )
        .map_err(map_runtime_error)?;
        let backend = EmbeddedBackend { fs };
        Ok(Self { backend })
    }
}

impl RemoteTarget {
    fn new(
        _profile_name: &str,
        server_url: &str,
        auth_token: Option<&str>,
    ) -> Result<Self, CliError> {
        let client = Client::new(ClientConfig {
            server_url: server_url.to_owned(),
            auth_token: auth_token.map(ToOwned::to_owned),
        });
        Ok(Self {
            backend: RemoteBackend { client },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{map_core_error, Backend, EmbeddedTarget, DEFAULT_LEASE_DURATION_MS};
    use crate::config::StoreConfig;
    use loon_api::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
    use loon_client::NamespacePath;
    use loon_core::commit::CommitValidationError;
    use loonfs::CoreError;
    use tempfile::tempdir;

    #[test]
    fn map_core_error_uses_revision_not_found_code() {
        let error = map_core_error(CoreError::CommitValidation(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id: InodeId(42),
                source_revision_no: RevisionNo(7),
            },
        ));

        assert_eq!(error.code, "revision_not_found");
    }

    #[test]
    fn embedded_target_uses_five_second_default_lease_when_unset() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        let target = EmbeddedTarget::new(&store, None, None, None).expect("build embedded target");

        assert_eq!(
            target.backend.fs.config().lease_duration_ms,
            DEFAULT_LEASE_DURATION_MS
        );
        assert_eq!(target.backend.fs.config().lease_duration_ms, 5_000);
    }

    #[test]
    fn embedded_target_preserves_explicit_lease_duration() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        let target =
            EmbeddedTarget::new(&store, None, None, Some(12_345)).expect("build embedded target");

        assert_eq!(target.backend.fs.config().lease_duration_ms, 12_345);
    }

    #[test]
    fn embedded_backend_generates_non_empty_commit_id_for_embedded_put() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None, None).expect("build embedded target");
        target
            .backend
            .create_namespace("demo")
            .expect("create namespace");

        target
            .backend
            .put_file_bytes(
                &NamespacePath {
                    namespace: "demo".to_owned(),
                    absolute_path: "/file.txt".to_owned(),
                },
                b"hello",
                false,
            )
            .expect("put file");

        let changes = target
            .backend
            .fs
            .list_changes_after(
                &NamespaceId::parse("demo").expect("valid namespace id"),
                ChangeSeq(0),
            )
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert!(!changes.changes[0].commit_id.as_str().trim().is_empty());
    }
}
