use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use loon_api::{AuthoritativePathEntry, MutationResult, NamespaceId, NamespaceSummary};
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use loon_core::{
    bootstrap_namespace, copy_file_path, delete_path_non_recursive, list_namespaces, list_path,
    move_path, put_file_bytes, read_file_bytes, resolve_path, BootstrapNamespaceError, CoreError,
    CoreErrorKind, MutationContext, PutFileBehavior,
};
use loon_objectstore::ConfiguredObjectStore;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_LEASE_DURATION_MS: u64 = 30_000;

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

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        self.client.delete_path(spec).map_err(map_client_error)
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

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        let ns_id = NamespaceId::from(spec.namespace.clone());
        delete_path_non_recursive(
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
}

fn map_core_error(error: CoreError) -> CliError {
    let code = match error.kind() {
        CoreErrorKind::InvalidPath => "invalid_path",
        CoreErrorKind::NamespaceNotFound => "namespace_not_found",
        CoreErrorKind::PathNotFound => "path_not_found",
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
            } => Ok(Self::Local(Box::new(LocalTarget::new(
                store,
                writer_id.as_deref(),
                writer_version.as_deref(),
                *lease_duration_ms,
            )?))),
            ProfileConfig::Remote {
                server_url,
                auth_token,
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
