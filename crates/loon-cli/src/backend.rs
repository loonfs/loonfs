use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use loon_api::{
    AuthoritativeFileBytes, AuthoritativePathEntry, InodeId, MutationResult, NamespaceId,
    NamespaceSummary, ReadSession,
};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_core::{
    begin_read_session, bootstrap_namespace, close_read_session, copy_file_path, delete_path,
    delete_path_non_recursive, list_namespaces, list_path, list_read_session_children, move_path,
    put_file_bytes, read_file_bytes, read_read_session_file, resolve_path, BootstrapNamespaceError,
    CoreError, CoreErrorKind, MutationContext, PutFileBehavior,
};
use loon_objectstore::ConfiguredObjectStore;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

pub trait Backend {
    fn create_namespace(&self, name: &str) -> Result<NamespaceSummary, CliError>;
    fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, CliError>;
    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError>;
    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError>;
    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError>;
    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError>;
    fn delete_path(
        &self,
        spec: &NamespacePath,
        recursive: bool,
    ) -> Result<MutationResult, CliError>;
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
    fn begin_read_session(&self, spec: &NamespacePath) -> Result<ReadSession, CliError>;
    fn list_read_session_children(
        &self,
        namespace: &NamespaceId,
        session_id: &str,
        parent_inode_id: InodeId,
    ) -> Result<Vec<AuthoritativePathEntry>, CliError>;
    fn read_read_session_file(
        &self,
        namespace: &NamespaceId,
        session_id: &str,
        inode_id: InodeId,
    ) -> Result<AuthoritativeFileBytes, CliError>;
    fn close_read_session(&self, namespace: &NamespaceId, session_id: &str)
        -> Result<(), CliError>;
}

// --- Remote backend (HTTP via loon-client) ---

pub struct RemoteBackend {
    client: Client,
}

impl Backend for RemoteBackend {
    fn create_namespace(&self, name: &str) -> Result<NamespaceSummary, CliError> {
        self.client.create_namespace(name).map_err(map_client_error)
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

    fn delete_path(
        &self,
        spec: &NamespacePath,
        recursive: bool,
    ) -> Result<MutationResult, CliError> {
        let result = if recursive {
            self.client.delete_path_recursive(spec)
        } else {
            self.client.delete_path(spec)
        };
        result.map_err(map_client_error)
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

    fn begin_read_session(&self, _spec: &NamespacePath) -> Result<ReadSession, CliError> {
        Err(unsupported_recursive_get())
    }

    fn list_read_session_children(
        &self,
        _namespace: &NamespaceId,
        _session_id: &str,
        _parent_inode_id: InodeId,
    ) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        Err(unsupported_recursive_get())
    }

    fn read_read_session_file(
        &self,
        _namespace: &NamespaceId,
        _session_id: &str,
        _inode_id: InodeId,
    ) -> Result<AuthoritativeFileBytes, CliError> {
        Err(unsupported_recursive_get())
    }

    fn close_read_session(
        &self,
        _namespace: &NamespaceId,
        _session_id: &str,
    ) -> Result<(), CliError> {
        Err(unsupported_recursive_get())
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
        ClientError::InvalidNamespacePath(message) => CliError::invalid_input(message),
        ClientError::Http(message) | ClientError::Json(message) => CliError::client_error(message),
        ClientError::Api { code, message, .. } => CliError::new(code, message),
        ClientError::Io(message) => CliError::new("io_error", format!("i/o error: {message}")),
    }
}

// --- Direct backend (calls loon-core directly) ---

pub struct DirectBackend {
    store: ConfiguredObjectStore,
    writer_id: String,
    writer_version: String,
    lease_duration_ms: u64,
}

impl DirectBackend {
    fn mutation_context(&self) -> MutationContext {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        MutationContext {
            writer_id: self.writer_id.clone(),
            writer_version: self.writer_version.clone(),
            now_ms,
            lease_duration_ms: self.lease_duration_ms,
        }
    }
}

impl Backend for DirectBackend {
    fn create_namespace(&self, name: &str) -> Result<NamespaceSummary, CliError> {
        let ns_id = NamespaceId::from(name.to_owned());
        bootstrap_namespace(&self.store, &ns_id, &self.mutation_context(), false)
            .map_err(map_bootstrap_error)
    }

    fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>, CliError> {
        list_namespaces(&self.store).map_err(map_core_error)
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        list_path(&self.store, &ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        resolve_path(&self.store, &ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        let result = read_file_bytes(&self.store, &ns_id, &spec.absolute_path)
            .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))?;
        Ok(result.bytes)
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        let behavior = if force {
            PutFileBehavior::ReplaceExisting
        } else {
            PutFileBehavior::CreateOnly
        };
        put_file_bytes(
            &self.store,
            &ns_id,
            &spec.absolute_path,
            bytes,
            behavior,
            &self.mutation_context(),
            None,
        )
        .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))
    }

    fn delete_path(
        &self,
        spec: &NamespacePath,
        recursive: bool,
    ) -> Result<MutationResult, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        let delete = if recursive {
            delete_path
        } else {
            delete_path_non_recursive
        };
        delete(
            &self.store,
            &ns_id,
            &spec.absolute_path,
            &self.mutation_context(),
            None,
        )
        .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))
    }

    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let ns_id = NamespaceId::from(from.namespace.clone());
        move_path(
            &self.store,
            &ns_id,
            &from.absolute_path,
            &to.absolute_path,
            &self.mutation_context(),
            None,
        )
        .map_err(|error| map_namespace_scoped_core_error(&from.namespace, error))
    }

    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let ns_id = NamespaceId::from(from.namespace.clone());
        copy_file_path(
            &self.store,
            &ns_id,
            &from.absolute_path,
            &to.absolute_path,
            &self.mutation_context(),
            None,
        )
        .map_err(|error| map_namespace_scoped_core_error(&from.namespace, error))
    }

    fn begin_read_session(&self, spec: &NamespacePath) -> Result<ReadSession, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        begin_read_session(
            &self.store,
            &ns_id,
            &spec.absolute_path,
            &self.mutation_context(),
        )
        .map_err(|error| map_namespace_scoped_core_error(&spec.namespace, error))
    }

    fn list_read_session_children(
        &self,
        namespace: &NamespaceId,
        session_id: &str,
        parent_inode_id: InodeId,
    ) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        list_read_session_children(&self.store, namespace, session_id, parent_inode_id)
            .map_err(map_core_error)
    }

    fn read_read_session_file(
        &self,
        namespace: &NamespaceId,
        session_id: &str,
        inode_id: InodeId,
    ) -> Result<AuthoritativeFileBytes, CliError> {
        read_read_session_file(&self.store, namespace, session_id, inode_id).map_err(map_core_error)
    }

    fn close_read_session(
        &self,
        namespace: &NamespaceId,
        session_id: &str,
    ) -> Result<(), CliError> {
        close_read_session(&self.store, namespace, session_id).map_err(map_core_error)
    }
}

fn map_core_error(error: CoreError) -> CliError {
    let code = match error.kind() {
        CoreErrorKind::InvalidPath => "invalid_path",
        CoreErrorKind::NamespaceNotFound => "namespace_not_found",
        CoreErrorKind::PathNotFound => "path_not_found",
        CoreErrorKind::RevisionNotFound => "revision_not_found",
        CoreErrorKind::PathConflict => "path_conflict",
        CoreErrorKind::StaleHead => "stale_head",
        CoreErrorKind::StaleRevision => "stale_revision",
        CoreErrorKind::TombstoneConflict => "tombstone_conflict",
        CoreErrorKind::LeaseConflict => "lease_conflict",
        CoreErrorKind::WouldCycle => "would_cycle",
        CoreErrorKind::RequestIdConflict => "request_id_conflict",
        CoreErrorKind::CheckpointUnavailable => "checkpoint_unavailable",
        CoreErrorKind::UploadNotFound => "upload_not_found",
        CoreErrorKind::UploadAlreadyCompleted => "upload_already_completed",
        CoreErrorKind::UploadBlockConflict => "upload_block_conflict",
        CoreErrorKind::InvalidUploadBlock => "invalid_upload_block",
        CoreErrorKind::ReadSessionNotFound => "read_session_not_found",
        CoreErrorKind::InvalidReadSessionTarget => "invalid_read_session_target",
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

fn unsupported_recursive_get() -> CliError {
    CliError::new(
        "unsupported_operation",
        "recursive `get` is not supported for remote profiles yet",
    )
}

// --- Target resolution ---

pub enum ResolvedTarget {
    Local(Box<LocalTarget>),
    Remote(RemoteTarget),
}

pub struct LocalTarget {
    backend: DirectBackend,
}

pub struct RemoteTarget {
    backend: RemoteBackend,
}

impl ResolvedTarget {
    pub fn resolve(profile_name: &str, profile: &ProfileConfig) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Local {
                store,
                writer_id,
                writer_version,
                lease_duration_ms,
                ..
            } => Ok(Self::Local(Box::new(LocalTarget::new(
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
            ResolvedTarget::Local(_) => "local",
            ResolvedTarget::Remote(_) => "remote",
        }
    }

    pub fn backend(&self) -> &dyn Backend {
        match self {
            ResolvedTarget::Local(target) => &target.backend,
            ResolvedTarget::Remote(target) => &target.backend,
        }
    }
}

impl LocalTarget {
    fn new(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
        writer_version: Option<&str>,
        lease_duration_ms: Option<u64>,
    ) -> Result<Self, CliError> {
        let store = store_config.object_store()?;
        let backend = DirectBackend {
            store,
            writer_id: writer_id
                .map(ToOwned::to_owned)
                .unwrap_or_else(default_writer_id),
            writer_version: writer_version
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("loon/{}", env!("CARGO_PKG_VERSION"))),
            lease_duration_ms: lease_duration_ms.unwrap_or(DEFAULT_LEASE_DURATION_MS),
        };
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
    use super::{map_core_error, LocalTarget, DEFAULT_LEASE_DURATION_MS};
    use crate::config::StoreConfig;
    use loon_api::{InodeId, RevisionNo};
    use loon_core::{commit::CommitValidationError, CoreError};
    use tempfile::tempdir;

    #[test]
    fn map_core_error_uses_revision_not_found_code() {
        let error = map_core_error(CoreError::CommitValidation(
            CommitValidationError::RestoreRevisionSourceRevisionMissing {
                inode_id: InodeId(42),
                source_revision: RevisionNo(7),
            },
        ));

        assert_eq!(error.code, "revision_not_found");
    }

    #[test]
    fn local_target_uses_five_second_default_lease_when_unset() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        let target = LocalTarget::new(&store, None, None, None).expect("build local target");

        assert_eq!(target.backend.lease_duration_ms, DEFAULT_LEASE_DURATION_MS);
        assert_eq!(target.backend.lease_duration_ms, 5_000);
    }

    #[test]
    fn local_target_preserves_explicit_lease_duration() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        let target =
            LocalTarget::new(&store, None, None, Some(12_345)).expect("build local target");

        assert_eq!(target.backend.lease_duration_ms, 12_345);
    }
}
