//! Profile-mode wiring for the [`Backend`] seam.
//!
//! The trait and its HTTP implementation live in `loonfs_client::backend`;
//! this module keeps the embedded implementation (the CLI is the only host
//! that embeds the `loonfs` runtime today, and `loonfs-client` must stay
//! wire-only) plus the profile-to-backend resolution.

use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use loonfs::{
    BootstrapNamespaceError, ChangesResponse, CopyOptions, CoreError, CreateDirOptions,
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions,
    ErrorCode, Fs, FsConfig, ListChangesOptions, MoveOptions, PutFileOptions,
    RestoreRevisionOptions, RuntimeCacheConfig, RuntimeError, SharedObjectStore, TraceMode,
    TraceStoreKind,
};
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativePathEntry, ChangeSeq, CommitId,
    CreateCheckpointResponse, DeleteDirectoryBehavior, EffectiveLimit, ListFileRevisionsResponse,
    MoveBehavior, MutationResult, NamespaceId, NamespaceStatusResponse, NamespaceSummary,
    PaginationPolicy, PutBehavior, RevisionNo,
};
use loonfs_client::{Client, ClientConfig, NamespacePath};
use std::future::Future;
use std::sync::Arc;

pub(crate) use loonfs_client::backend::{Backend, BackendError, RemoteBackend};

// --- Embedded backend (embedded/direct mode uses the shared loonfs runtime) ---

pub(crate) struct EmbeddedBackend {
    fs: Fs,
    runtime: tokio::runtime::Runtime,
}

impl EmbeddedBackend {
    fn block_on<T, F>(&self, future: F) -> Result<T, BackendError>
    where
        F: Future<Output = loonfs::Result<T>>,
    {
        self.runtime.block_on(future).map_err(map_runtime_error)
    }

    fn block_on_scoped<T, F>(&self, namespace: &str, future: F) -> Result<T, BackendError>
    where
        F: Future<Output = loonfs::Result<T>>,
    {
        self.runtime
            .block_on(future)
            .map_err(|error| map_namespace_scoped_runtime_error(namespace, error))
    }
}

impl Backend for EmbeddedBackend {
    fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError> {
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
    ) -> Result<DeleteNamespaceResponse, BackendError> {
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
    ) -> Result<NamespaceSummary, BackendError> {
        let source_namespace_id = parse_namespace_id(source)?;
        let new_namespace_id = parse_namespace_id(new_namespace_id)?;
        self.block_on(
            self.fs
                .fork_namespace(&source_namespace_id, &new_namespace_id),
        )
    }

    fn namespace_status(
        &self,
        namespace_id: &str,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.block_on_scoped(namespace_id, self.fs.namespace_status(&parsed))
    }

    fn list_path(&self, spec: &NamespacePath) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.block_on_scoped(
            &spec.namespace,
            self.fs.list_path(&namespace_id, &spec.absolute_path),
        )
    }

    fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.block_on_scoped(
            &spec.namespace,
            self.fs.stat_path(&namespace_id, &spec.absolute_path),
        )
    }

    fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
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
    ) -> Result<Vec<u8>, BackendError> {
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
    ) -> Result<ListFileRevisionsResponse, BackendError> {
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
                        .map_err(|error| {
                            BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                        })?,
                },
            ),
        )
    }

    fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        force: bool,
    ) -> Result<MutationResult, BackendError> {
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

    fn delete_path(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError> {
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

    fn create_dir(&self, spec: &NamespacePath) -> Result<MutationResult, BackendError> {
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
    ) -> Result<MutationResult, BackendError> {
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
    ) -> Result<MutationResult, BackendError> {
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
    ) -> Result<MutationResult, BackendError> {
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

    // The admin methods mirror the server handlers' error scoping exactly:
    // checkpoint/retention map runtime errors unscoped, the change feed is
    // namespace-scoped. Parity keeps embedded and remote outputs identical.

    fn create_checkpoint(
        &self,
        namespace_id: &str,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.block_on(self.fs.create_checkpoint(&parsed))
    }

    fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.block_on(self.fs.advance_retention_floor(&parsed))
    }

    fn list_changes(
        &self,
        namespace_id: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let limit = resolve_cli_page_limit(limit)?;
        self.block_on_scoped(
            namespace_id,
            self.fs.list_changes_after(
                &parsed,
                after_seq,
                ListChangesOptions { limit: Some(limit) },
            ),
        )
    }
}

fn parse_namespace_id(namespace: &str) -> Result<NamespaceId, BackendError> {
    NamespaceId::parse(namespace)
        .map_err(|error| BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()))
}

fn resolve_cli_page_limit(limit: Option<u32>) -> Result<EffectiveLimit, BackendError> {
    PaginationPolicy::default()
        .resolve_limit(limit)
        .map_err(|error| BackendError::invalid_input(error.to_string()))
}

fn generated_commit_id() -> CommitId {
    CommitId::generate()
}

fn map_runtime_error(error: RuntimeError) -> BackendError {
    match error {
        RuntimeError::Core(error) => map_core_error(error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => BackendError::invalid_config(message),
        RuntimeError::RuntimeTask(message) => BackendError::runtime_error(message),
        other => BackendError::runtime_error(other.to_string()),
    }
}

fn map_namespace_scoped_runtime_error(namespace: &str, error: RuntimeError) -> BackendError {
    match error {
        RuntimeError::Core(error) => map_namespace_scoped_core_error(namespace, error),
        RuntimeError::Bootstrap(error) => map_bootstrap_error(error),
        RuntimeError::Config(message) => BackendError::invalid_config(message),
        RuntimeError::RuntimeTask(message) => BackendError::runtime_error(message),
        other => BackendError::runtime_error(other.to_string()),
    }
}

// Embedded-mode failures surface the same registry code the server would
// serve for the identical failure, so `loon --json` consumers see one code
// per mistake regardless of profile mode.

fn map_core_error(error: CoreError) -> BackendError {
    BackendError::new(error.code().as_str(), error.to_string())
}

fn map_namespace_scoped_core_error(namespace: &str, error: CoreError) -> BackendError {
    if matches!(error.code(), ErrorCode::NamespaceNotFound) {
        return BackendError::new(
            ErrorCode::NamespaceNotFound.as_str(),
            format!("namespace `{namespace}` does not exist"),
        );
    }

    map_core_error(error)
}

fn map_bootstrap_error(error: BootstrapNamespaceError) -> BackendError {
    BackendError::new(error.code().as_str(), error.to_string())
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
                auth_token.as_ref().map(|token| token.expose()),
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
        let store = store_config
            .configured_object_store()
            .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}")))?;
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
                trace_store_kind: TraceStoreKind::from(store_config.kind()),
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
            backend: RemoteBackend::new(client),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{map_bootstrap_error, map_core_error, Backend, EmbeddedTarget};
    use crate::config::StoreConfig;
    use loonfs::{BootstrapNamespaceError, CoreError, ErrorCode};
    use loonfs_api::{ChangeSeq, InodeId, NamespaceId, RevisionNo};
    use loonfs_client::NamespacePath;
    use tempfile::tempdir;

    #[test]
    fn map_core_error_surfaces_registry_codes_verbatim() {
        let error = map_core_error(CoreError::RevisionNotFound {
            inode_id: InodeId(42),
            revision_no: RevisionNo(7),
        });

        assert_eq!(error.code, ErrorCode::RevisionNotFound.as_str());
    }

    #[test]
    fn map_core_error_does_not_rewrite_invalid_id_codes() {
        // Embedded mode must report the same code the server serves for the
        // identical failure, not a CLI-local `invalid_input` rewrite.
        let invalid_id = NamespaceId::parse("bad/name").expect_err("invalid namespace id");
        let error = map_core_error(CoreError::InvalidNamespaceId(invalid_id));

        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    }

    #[test]
    fn map_bootstrap_error_surfaces_registry_codes_verbatim() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let error =
            map_bootstrap_error(BootstrapNamespaceError::NamespaceAlreadyExists { namespace_id });

        assert_eq!(error.code, ErrorCode::NamespaceExists.as_str());
        assert!(error.message.contains("already exists"));
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
            .block_on(target.backend.fs.list_changes_after(
                &namespace_id,
                ChangeSeq(0),
                loonfs::ListChangesOptions::default(),
            ))
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert!(!changes.changes[0].commit_id.as_str().trim().is_empty());
    }

    #[test]
    fn embedded_admin_methods_surface_registry_codes_for_missing_namespaces() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None).expect("build embedded target");

        let checkpoint = target
            .backend
            .create_checkpoint("missing")
            .expect_err("checkpoint on missing namespace");
        assert_eq!(checkpoint.code, ErrorCode::NamespaceNotFound.as_str());

        let changes = target
            .backend
            .list_changes("missing", ChangeSeq(0), None)
            .expect_err("changes on missing namespace");
        assert_eq!(changes.code, ErrorCode::NamespaceNotFound.as_str());
        assert_eq!(changes.message, "namespace `missing` does not exist");
    }
}
