use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use loonfs::{
    BootstrapNamespaceError, CopyOptions, CoreError, CreateDirOptions, CreateNamespaceOptions,
    DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions, ErrorCode, Fs, FsConfig,
    MoveOptions, PutFileOptions, RestoreRevisionOptions, RuntimeCacheConfig, RuntimeError,
    SharedObjectStore, TraceMode, TraceStoreKind,
};
use loonfs_api::{
    AuthoritativePathEntry, ChangeSeq, CommitId, DeleteDirectoryBehavior, EffectiveLimit,
    ListFileRevisionsResponse, MoveBehavior, MutationResult, NamespaceId, NamespaceStatusResponse,
    NamespaceSummary, PaginationPolicy, PutBehavior, RevisionNo,
};
use loonfs_client::{Client, ClientConfig, ClientError, NamespacePath};
use std::future::Future;
use std::sync::Arc;

pub(crate) trait Backend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError>;
    fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, CliError>;
    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, CliError>;
    fn namespace_status(&self, namespace_id: &str) -> Result<NamespaceStatusResponse, CliError>;
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
        limit: Option<u32>,
        cursor: Option<&str>,
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

// --- Remote backend (HTTP via loonfs-client) ---

pub(crate) struct RemoteBackend {
    client: Client,
}

impl Backend for RemoteBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError> {
        self.client
            .create_namespace(namespace_id)
            .map_err(map_client_error)
    }

    fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, CliError> {
        self.client
            .delete_namespace(namespace_id, expected_head_seq.map(ChangeSeq))
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

    fn namespace_status(&self, namespace_id: &str) -> Result<NamespaceStatusResponse, CliError> {
        self.client
            .namespace_status(namespace_id)
            .map_err(map_client_error)
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        Ok(self
            .client
            .list_path(spec)
            .map_err(map_client_error)?
            .entries)
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
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, CliError> {
        self.client
            .list_file_revisions_page(spec, limit, cursor)
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
        other => CliError::client_error(other.to_string()),
    }
}

// --- Embedded backend (embedded/direct mode uses the shared loonfs runtime) ---

pub(crate) struct EmbeddedBackend {
    fs: Fs,
    runtime: tokio::runtime::Runtime,
}

impl EmbeddedBackend {
    fn block_on<T, F>(&self, future: F) -> Result<T, CliError>
    where
        F: Future<Output = loonfs::Result<T>>,
    {
        self.runtime.block_on(future).map_err(map_runtime_error)
    }

    fn block_on_scoped<T, F>(&self, namespace: &str, future: F) -> Result<T, CliError>
    where
        F: Future<Output = loonfs::Result<T>>,
    {
        self.runtime
            .block_on(future)
            .map_err(|error| map_namespace_scoped_runtime_error(namespace, error))
    }
}

impl Backend for EmbeddedBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, CliError> {
        let namespace_id = parse_namespace_id(namespace_id)?;
        self.block_on(
            self.fs
                .create_namespace(&namespace_id, CreateNamespaceOptions::default()),
        )
    }

    fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, CliError> {
        let namespace_id = parse_namespace_id(namespace_id)?;
        let options = DeleteNamespaceOptions {
            expected_head_seq: expected_head_seq.map(ChangeSeq),
        };
        self.block_on(self.fs.delete_namespace(&namespace_id, options))
    }

    fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, CliError> {
        let source_namespace_id = parse_namespace_id(source)?;
        let new_namespace_id = parse_namespace_id(new_namespace_id)?;
        self.block_on(
            self.fs
                .fork_namespace(&source_namespace_id, &new_namespace_id),
        )
    }

    fn namespace_status(&self, namespace_id: &str) -> Result<NamespaceStatusResponse, CliError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let status = self.block_on_scoped(namespace_id, self.fs.namespace_status(&parsed))?;
        Ok(NamespaceStatusResponse {
            namespace_id: status.namespace_id,
            head_seq: status.head_seq,
            current_manifest_id: status.current_manifest_id,
            latest_checkpoint_id: status.latest_checkpoint_id,
            wal_tail_segments: status.wal_tail_segments,
            retention_floor_seq: status.retention_floor_seq,
        })
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.block_on_scoped(
            &spec.namespace,
            self.fs.list_path(&namespace_id, &spec.absolute_path),
        )
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.block_on_scoped(
            &spec.namespace,
            self.fs.stat_path(&namespace_id, &spec.absolute_path),
        )
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let result = self.block_on_scoped(
            &spec.namespace,
            self.fs.read_file_bytes(&namespace_id, &spec.absolute_path),
        )?;
        Ok(result.bytes)
    }

    fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let result = self.block_on_scoped(
            &spec.namespace,
            self.fs
                .read_file_revision_bytes(&namespace_id, &spec.absolute_path, revision_no),
        )?;
        Ok(result.bytes)
    }

    fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.block_on_scoped(
            &spec.namespace,
            self.fs.list_file_revisions_page(
                &namespace_id,
                &spec.absolute_path,
                loonfs_api::PageRequest {
                    limit: resolve_cli_page_limit(limit)?,
                    cursor: cursor
                        .map(loonfs_api::decode_file_revisions_cursor)
                        .transpose()
                        .map_err(|error| CliError::new("invalid_request", error.to_string()))?,
                },
            ),
        )
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let behavior = if force {
            PutBehavior::Replace
        } else {
            PutBehavior::NoReplace
        };
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &spec.namespace,
            self.fs.put_file_bytes(
                &namespace_id,
                &spec.absolute_path,
                bytes,
                PutFileOptions {
                    behavior,
                    commit_id: Some(commit_id),
                },
            ),
        )
    }

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &spec.namespace,
            self.fs.delete_path(
                &namespace_id,
                &spec.absolute_path,
                DeleteOptions {
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    commit_id: Some(commit_id),
                },
            ),
        )
    }

    fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &spec.namespace,
            self.fs.create_dir(
                &namespace_id,
                &spec.absolute_path,
                CreateDirOptions {
                    commit_id: Some(commit_id),
                },
            ),
        )
    }

    fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&from.namespace)?;
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &from.namespace,
            self.fs.move_path(
                &namespace_id,
                &from.absolute_path,
                &to.absolute_path,
                MoveOptions {
                    behavior: MoveBehavior::NoReplace,
                    commit_id: Some(commit_id),
                },
            ),
        )
    }

    fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
    ) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&from.namespace)?;
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &from.namespace,
            self.fs.copy_path(
                &namespace_id,
                &from.absolute_path,
                &to.absolute_path,
                CopyOptions {
                    commit_id: Some(commit_id),
                },
            ),
        )
    }

    fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
    ) -> Result<MutationResult, CliError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let commit_id = generated_commit_id();
        self.block_on_scoped(
            &spec.namespace,
            self.fs.restore_file_revision(
                &namespace_id,
                &spec.absolute_path,
                source_revision_no,
                RestoreRevisionOptions {
                    commit_id: Some(commit_id),
                },
            ),
        )
    }
}

fn parse_namespace_id(namespace: &str) -> Result<NamespaceId, CliError> {
    NamespaceId::parse(namespace).map_err(|error| CliError::invalid_input(error.to_string()))
}

fn resolve_cli_page_limit(limit: Option<u32>) -> Result<EffectiveLimit, CliError> {
    PaginationPolicy::default()
        .resolve_limit(limit)
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

fn generated_commit_id() -> CommitId {
    CommitId::generate()
}

fn map_runtime_error(error: RuntimeError) -> CliError {
    match error {
        RuntimeError::Core(error) => map_core_error(error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => CliError::invalid_config(message),
        RuntimeError::RuntimeTask(message) => CliError::new("runtime_error", message),
        other => CliError::new("runtime_error", other.to_string()),
    }
}

fn map_namespace_scoped_runtime_error(namespace: &str, error: RuntimeError) -> CliError {
    match error {
        RuntimeError::Core(error) => map_namespace_scoped_core_error(namespace, error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => CliError::invalid_config(message),
        RuntimeError::RuntimeTask(message) => CliError::new("runtime_error", message),
        other => CliError::new("runtime_error", other.to_string()),
    }
}

fn map_core_error(error: CoreError) -> CliError {
    if matches!(error.code(), ErrorCode::InvalidRequest) {
        return CliError::invalid_input(error.to_string());
    }

    CliError::new(error.code().as_str(), error.to_string())
}

fn map_namespace_scoped_core_error(namespace: &str, error: CoreError) -> CliError {
    if matches!(error.code(), ErrorCode::NamespaceNotFound) {
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
        BootstrapNamespaceError::NamespaceDeleted { .. } => {
            CliError::new("namespace_deleted", error.to_string())
        }
        BootstrapNamespaceError::EmptyHolderId | BootstrapNamespaceError::EmptyWriterVersion => {
            CliError::invalid_config(error.to_string())
        }
        _ => CliError::new("server_error", error.to_string()),
    }
}

fn default_writer_id() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "loonfs-cli".to_owned())
}

// --- Target resolution ---

pub(crate) enum ResolvedTarget {
    Embedded(Box<EmbeddedTarget>),
    Remote(RemoteTarget),
}

pub(crate) struct EmbeddedTarget {
    backend: EmbeddedBackend,
}

pub(crate) struct RemoteTarget {
    backend: RemoteBackend,
}

impl ResolvedTarget {
    pub(crate) fn resolve(profile_name: &str, profile: &ProfileConfig) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Embedded {
                store,
                writer_id,
                writer_version,
                ..
            } => Ok(Self::Embedded(Box::new(EmbeddedTarget::new(
                store,
                writer_id.as_deref(),
                writer_version.as_deref(),
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

    pub(crate) fn mode_str(&self) -> &'static str {
        match self {
            ResolvedTarget::Embedded(_) => "embedded",
            ResolvedTarget::Remote(_) => "remote",
        }
    }

    pub(crate) fn backend(&self) -> &dyn Backend {
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
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: trace_store_kind(store_config),
            },
        )
        .map_err(map_runtime_error)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| CliError::invalid_config(error.to_string()))?;
        let backend = EmbeddedBackend { fs, runtime };
        Ok(Self { backend })
    }
}

fn trace_store_kind(store: &StoreConfig) -> TraceStoreKind {
    match store {
        StoreConfig::LocalFs { .. } => TraceStoreKind::LocalFs,
        StoreConfig::AwsS3 { .. } => TraceStoreKind::S3,
        StoreConfig::CloudflareR2 { .. } => TraceStoreKind::R2,
        StoreConfig::GcpGcs { .. } => TraceStoreKind::Gcs,
        StoreConfig::AzureAbs { .. } => TraceStoreKind::Abs,
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
    use super::{map_core_error, Backend, EmbeddedTarget};
    use crate::config::StoreConfig;
    use loonfs::CoreError;
    use loonfs_api::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
    use loonfs_client::NamespacePath;
    use loonfs_core::commit::CommitValidationError;
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
    fn embedded_backend_generates_non_empty_commit_id_for_embedded_put() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None).expect("build embedded target");
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

        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let changes = target
            .backend
            .block_on(
                target
                    .backend
                    .fs
                    .list_changes_after(&namespace_id, ChangeSeq(0)),
            )
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert!(!changes.changes[0].commit_id.as_str().trim().is_empty());
    }
}
