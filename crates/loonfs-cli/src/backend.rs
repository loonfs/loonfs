//! Profile-mode wiring for the [`Backend`] seam.
//!
//! The trait and its HTTP implementation live in `loonfs_client::backend`;
//! this module keeps the embedded implementation (the CLI is the only host
//! that embeds the `loonfs` runtime today, and `loonfs-client` must stay
//! wire-only) plus the profile-to-backend resolution.

use crate::config::{ProfileConfig, StoreConfig};
use crate::error::CliError;
use async_trait::async_trait;
use loonfs::{
    gc_config_from_request, gc_response_from_report, BootstrapNamespaceError, ChangesResponse,
    CopyOptions, CoreError, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions,
    ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter, ListChangesOptions,
    MaintenanceTickOptions, MaintenanceTickResult, MoveOptions, PutFileOptions,
    RestoreRevisionOptions, RuntimeError, SharedObjectStore, TraceStoreKind,
};
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativePathEntry, ChangeSeq, CheckpointId, CommitId,
    CommitResponse, CopyBehavior, CreateCheckpointRequest, CreateCheckpointResponse,
    DeleteDirectoryBehavior, DisableGramsIndexResponse, EffectiveLimit, EnableGramsIndexResponse,
    FlushWalResponse, GcRequest, GcResponse, GrepRequest, GrepResponse, ListFileRevisionsResponse,
    MaintenanceTickRequest, MaintenanceTickResponse, MoveBehavior, NamespaceId,
    NamespaceStatusResponse, NamespaceSummary, PaginationPolicy, PutBehavior,
    ReleaseCheckpointResponse, RevisionNo,
};
use loonfs_client::{Client, ClientConfig, NamespacePath};
use std::sync::Arc;

pub(crate) use loonfs_client::backend::{Backend, BackendError, RemoteBackend};

// --- Embedded backend (embedded/direct mode uses the shared loonfs runtime) ---

/// Purpose-specific handles over one shared store client: reads go through
/// the reader, mutations through the writer, and maintenance through the
/// admin handle. The embedded writer runs `FsBackgroundWork::Enabled` — the
/// same policy as the reference server — so a publish that crosses the WAL
/// threshold schedules its own maintenance tick, and every mutation settles
/// scheduled work before the one-shot process exits. A publish gated on
/// `maintenance_required` waits for the tick that same gated publish
/// scheduled, then resubmits, so embedded writes recover from WAL debt
/// instead of hard-stopping. `loon admin` commands remain the explicit path
/// for everything else (GC, retention, forced ticks).
pub(crate) struct EmbeddedBackend {
    writer: FsWriter,
    reader: FsReader,
    admin: FsAdmin,
}

/// How many times a gated publish resubmits after settling the maintenance
/// tick it scheduled. One recovery is the normal case; the second covers a
/// tick that raced another writer's debt. Past that the error surfaces.
const MAX_MAINTENANCE_RECOVERIES: usize = 2;

impl EmbeddedBackend {
    /// Waits out writer-scheduled maintenance so a one-shot command never
    /// exits (tearing down the runtime) while a tick is mid-flight. A settle
    /// failure after a committed mutation is reported as a warning on
    /// stderr, never as the mutation's outcome — the commit landed.
    async fn settle_background_work_after<T>(
        &self,
        result: Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        match (result, self.writer.wait_for_background_work().await) {
            (result, Ok(())) => result,
            (Ok(value), Err(error)) => {
                use std::io::Write;
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "warning: background maintenance did not settle cleanly: {error}"
                );
                Ok(value)
            }
            (Err(error), Err(_)) => Err(error),
        }
    }

    /// Runs one mutation with `maintenance_required` recovery: a gated
    /// publish observes the oversized WAL tail and schedules its own
    /// recovery tick (the writer policy is `Enabled`), so settle that tick
    /// and resubmit. A gated attempt commits nothing, so the resubmission
    /// cannot double-apply.
    async fn publish_with_maintenance_recovery<T, F, Fut>(
        &self,
        namespace: &str,
        attempt: F,
    ) -> Result<T, BackendError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RuntimeError>>,
    {
        let mut result = attempt().await;
        for _ in 0..MAX_MAINTENANCE_RECOVERIES {
            let gated = matches!(
                &result,
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired)
            );
            if !gated {
                break;
            }
            self.writer
                .wait_for_background_work()
                .await
                .map_err(map_runtime_error)?;
            result = attempt().await;
        }
        let result = result.map_err(|error| map_namespace_scoped_runtime_error(namespace, error));
        self.settle_background_work_after(result).await
    }
}

#[async_trait]
impl Backend for EmbeddedBackend {
    async fn create_namespace(&self, namespace_id: &str) -> Result<NamespaceSummary, BackendError> {
        let namespace_id = parse_namespace_id(namespace_id)?;
        let result = self
            .writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn delete_namespace(
        &self,
        namespace_id: &str,
        expected_head_seq: Option<u64>,
    ) -> Result<DeleteNamespaceResponse, BackendError> {
        let namespace_id = parse_namespace_id(namespace_id)?;
        let options = DeleteNamespaceOptions {
            expected_head_seq: expected_head_seq.map(ChangeSeq),
        };
        let result = self
            .writer
            .delete_namespace(&namespace_id, options)
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn fork_namespace(
        &self,
        source: &str,
        new_namespace_id: &str,
    ) -> Result<NamespaceSummary, BackendError> {
        let source_namespace_id = parse_namespace_id(source)?;
        let new_namespace_id = parse_namespace_id(new_namespace_id)?;
        let result = self
            .writer
            .fork_namespace(&source_namespace_id, &new_namespace_id)
            .await
            .map_err(map_runtime_error);
        self.settle_background_work_after(result).await
    }

    async fn namespace_status(
        &self,
        namespace_id: &str,
    ) -> Result<NamespaceStatusResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .namespace_status(&parsed)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn list_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<Vec<AuthoritativePathEntry>, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.reader
            .list_path(&namespace_id, &spec.absolute_path)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    async fn stat_path(
        &self,
        spec: &NamespacePath,
    ) -> Result<AuthoritativePathEntry, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.reader
            .stat_path(&namespace_id, &spec.absolute_path)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    async fn read_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let result = self
            .reader
            .read_file_bytes(&namespace_id, &spec.absolute_path)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))?;
        Ok(result.bytes)
    }

    async fn grep(
        &self,
        namespace_id: &str,
        request: &GrepRequest,
    ) -> Result<GrepResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.reader
            .grep(&parsed, request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn enable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<EnableGramsIndexResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .enable_grams_index(&parsed)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn disable_grams_index(
        &self,
        namespace_id: &str,
    ) -> Result<DisableGramsIndexResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .disable_grams_index(&parsed)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
    }

    async fn read_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let result = self
            .reader
            .read_file_revision_bytes(&namespace_id, &spec.absolute_path, revision_no)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))?;
        Ok(result.bytes)
    }

    async fn list_file_revisions(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        let request = loonfs_api::PageRequest {
            limit: resolve_cli_page_limit(limit)?,
            cursor: cursor
                .map(loonfs_api::decode_file_revisions_cursor)
                .transpose()
                .map_err(|error| {
                    BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
                })?,
        };
        self.reader
            .list_file_revisions_page(&namespace_id, &spec.absolute_path, request)
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(&spec.namespace, error))
    }

    async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        behavior: PutBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.publish_with_maintenance_recovery(&spec.namespace, || {
            self.writer.put_file_bytes(
                &namespace_id,
                &spec.absolute_path,
                bytes,
                PutFileOptions {
                    behavior,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn delete_path(
        &self,
        spec: &NamespacePath,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.publish_with_maintenance_recovery(&spec.namespace, || {
            self.writer.delete_path(
                &namespace_id,
                &spec.absolute_path,
                DeleteOptions {
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn create_directory(
        &self,
        spec: &NamespacePath,
        parents: bool,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.publish_with_maintenance_recovery(&spec.namespace, || {
            self.writer.create_directory(
                &namespace_id,
                &spec.absolute_path,
                CreateDirectoryOptions {
                    commit_id: commit_id.clone(),
                    parents,
                },
            )
        })
        .await
    }

    async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: MoveBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&from.namespace)?;
        self.publish_with_maintenance_recovery(&from.namespace, || {
            self.writer.move_path(
                &namespace_id,
                &from.absolute_path,
                &to.absolute_path,
                MoveOptions {
                    behavior,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        behavior: CopyBehavior,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&from.namespace)?;
        self.publish_with_maintenance_recovery(&from.namespace, || {
            self.writer.copy_path(
                &namespace_id,
                &from.absolute_path,
                &to.absolute_path,
                CopyOptions {
                    behavior,
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        commit_id: Option<CommitId>,
    ) -> Result<CommitResponse, BackendError> {
        let namespace_id = parse_namespace_id(&spec.namespace)?;
        self.publish_with_maintenance_recovery(&spec.namespace, || {
            self.writer.restore_file_revision(
                &namespace_id,
                &spec.absolute_path,
                source_revision_no,
                RestoreRevisionOptions {
                    commit_id: commit_id.clone(),
                },
            )
        })
        .await
    }

    // The admin methods mirror the server handlers' error scoping exactly:
    // checkpoint/retention map runtime errors unscoped, the change feed is
    // namespace-scoped. Parity keeps embedded and remote outputs identical.

    async fn create_checkpoint(
        &self,
        namespace_id: &str,
        request: CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .create_checkpoint(&parsed, CreateCheckpointOptions::from_request(request))
            .await
            .map_err(map_runtime_error)
    }

    async fn release_checkpoint(
        &self,
        namespace_id: &str,
        checkpoint_id: &str,
    ) -> Result<ReleaseCheckpointResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let checkpoint_id = CheckpointId::parse(checkpoint_id).map_err(|error| {
            BackendError::new(ErrorCode::InvalidRequest.as_str(), error.to_string())
        })?;
        self.admin
            .release_checkpoint(&parsed, &checkpoint_id)
            .await
            .map_err(map_runtime_error)
    }

    async fn flush_wal(&self, namespace_id: &str) -> Result<FlushWalResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .flush_wal(&parsed)
            .await
            .map_err(map_runtime_error)
    }

    async fn advance_retention(
        &self,
        namespace_id: &str,
    ) -> Result<AdvanceRetentionResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        self.admin
            .advance_retention_floor(&parsed)
            .await
            .map_err(map_runtime_error)
    }

    async fn maintenance_tick(
        &self,
        namespace_id: &str,
        request: MaintenanceTickRequest,
    ) -> Result<MaintenanceTickResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let options = MaintenanceTickOptions::from_request(request);
        self.admin
            .maintenance_tick_namespace(&parsed, options)
            .await
            .map(MaintenanceTickResult::into_response)
            .map_err(map_runtime_error)
    }

    async fn gc_namespace(
        &self,
        namespace_id: &str,
        request: GcRequest,
    ) -> Result<GcResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let config = gc_config_from_request(request);
        self.admin
            .gc_namespace(&parsed, &config)
            .await
            .map(|report| gc_response_from_report(parsed, report))
            .map_err(map_runtime_error)
    }

    async fn list_changes(
        &self,
        namespace_id: &str,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse, BackendError> {
        let parsed = parse_namespace_id(namespace_id)?;
        let limit = resolve_cli_page_limit(limit)?;
        self.reader
            .list_changes_after(
                &parsed,
                after_seq,
                ListChangesOptions { limit: Some(limit) },
            )
            .await
            .map_err(|error| map_namespace_scoped_runtime_error(namespace_id, error))
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
    pub(crate) async fn resolve(
        profile_name: &str,
        profile: &ProfileConfig,
        no_retry: bool,
    ) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Embedded {
                store,
                writer_id,
                writer_version,
                ..
            } => Ok(Self::Embedded(Box::new(
                EmbeddedTarget::new(store, writer_id.as_deref(), writer_version.as_deref()).await?,
            ))),
            ProfileConfig::Remote {
                server_url,
                auth_token,
                ..
            } => Ok(Self::Remote(RemoteTarget::new(
                profile_name,
                server_url,
                auth_token.as_ref().map(|token| token.expose()),
                no_retry,
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
    async fn new(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
        writer_version: Option<&str>,
    ) -> Result<Self, CliError> {
        let writer_id = writer_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_writer_id);
        let writer_version = writer_version
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("loon/{}", env!("CARGO_PKG_VERSION")));
        let trace_store_kind = TraceStoreKind::from(store_config.kind());
        // One command drives all three handles from one runtime, so they
        // deliberately share one provider client.
        let store: SharedObjectStore = Arc::new(
            store_config
                .configured_object_store()
                .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}")))?,
        );
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id(writer_id.clone())
            .writer_version(writer_version.clone())
            // The server's policy: publishes past the WAL threshold schedule
            // their own tick. The backend settles scheduled work after each
            // mutation, so a one-shot command exits with maintenance done
            // rather than stalling at the WAL backpressure cap.
            .background_work(FsBackgroundWork::Enabled)
            // A CLI invocation is one solo mutation: holding the commit
            // window open would only add its full delay to every command.
            .min_publish_interval_ms(0)
            .trace_store_kind(trace_store_kind)
            .build()
            .await
            .map_err(map_runtime_error)?;
        let reader = writer.reader();
        let admin = FsAdmin::builder_with_store(store)
            .actor_id(writer_id)
            .actor_version(writer_version)
            .trace_store_kind(trace_store_kind)
            .build()
            .await
            .map_err(map_runtime_error)?;
        let backend = EmbeddedBackend {
            writer,
            reader,
            admin,
        };
        Ok(Self { backend })
    }
}

impl RemoteTarget {
    fn new(
        _profile_name: &str,
        server_url: &str,
        auth_token: Option<&str>,
        no_retry: bool,
    ) -> Result<Self, CliError> {
        let client = Client::new(ClientConfig {
            server_url: server_url.to_owned(),
            auth_token: auth_token.map(ToOwned::to_owned),
            request_timeout_ms: None,
            disable_transient_retry: no_retry,
        });
        Ok(Self {
            backend: RemoteBackend::new(client),
        })
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{map_bootstrap_error, map_core_error, Backend, EmbeddedTarget};
    use crate::config::StoreConfig;
    use loonfs::{
        BootstrapNamespaceError, CoreError, CreateNamespaceOptions, ErrorCode, FsBackgroundWork,
        FsWriter, PutFileOptions, RuntimeError, SharedObjectStore,
    };
    use loonfs_api::{
        ChangeSeq, CreateCheckpointRequest, InodeId, NamespaceId, PutBehavior, RevisionNo,
    };
    use loonfs_client::NamespacePath;
    use std::sync::Arc;
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

    #[tokio::test]
    async fn embedded_backend_put_returns_the_commit_id_it_committed_under() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace("demo")
            .await
            .expect("create namespace");

        let response = target
            .backend
            .put_file_bytes(
                &NamespacePath {
                    namespace: "demo".to_owned(),
                    absolute_path: "/file.txt".to_owned(),
                },
                b"hello",
                PutBehavior::NoReplace,
                None,
            )
            .await
            .expect("put file");
        assert!(!response.commit_id.as_str().trim().is_empty());

        let changes = target
            .backend
            .list_changes("demo", ChangeSeq(0), None)
            .await
            .expect("list changes");
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].commit_id, response.commit_id);
    }

    #[tokio::test]
    async fn embedded_admin_methods_surface_registry_codes_for_missing_namespaces() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");

        let checkpoint = target
            .backend
            .create_checkpoint(
                "missing",
                CreateCheckpointRequest {
                    name: "nightly".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
            .expect_err("checkpoint on missing namespace");
        assert_eq!(checkpoint.code, ErrorCode::NamespaceNotFound.as_str());

        let changes = target
            .backend
            .list_changes("missing", ChangeSeq(0), None)
            .await
            .expect_err("changes on missing namespace");
        assert_eq!(changes.code, ErrorCode::NamespaceNotFound.as_str());
        assert_eq!(changes.message, "namespace `missing` does not exist");
    }

    #[tokio::test]
    async fn embedded_writes_never_stall_at_the_wal_backpressure_cap() {
        let temp_dir = tempdir().expect("create temp dir");
        let store = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };
        let target = EmbeddedTarget::new(&store, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .create_namespace("demo")
            .await
            .expect("create namespace");

        // More publishes than the WAL backpressure cap: the Enabled policy
        // must keep ticking the tail down so no write ever stalls on
        // `maintenance_required` (each stall used to require a manual
        // `loon admin tick`).
        for index in 0..140 {
            target
                .backend
                .put_file_bytes(
                    &NamespacePath {
                        namespace: "demo".to_owned(),
                        absolute_path: format!("/files/f{index}.txt"),
                    },
                    b"payload",
                    PutBehavior::NoReplace,
                    None,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("put {index} failed: {} {}", error.code, error.message)
                });
        }
    }

    #[tokio::test]
    async fn embedded_writes_recover_from_preexisting_wal_debt() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_config = StoreConfig::LocalFs {
            root: temp_dir.path().display().to_string(),
            key_prefix: None,
        };

        // Accumulate WAL debt the way pre-fix builds did: a ManualOnly
        // writer publishes until the backpressure gate refuses the next
        // publish outright.
        let store: SharedObjectStore = Arc::new(
            store_config
                .configured_object_store()
                .expect("configure store"),
        );
        let writer = FsWriter::builder_with_store(store)
            .writer_id("debt-builder")
            .writer_version("test")
            .background_work(FsBackgroundWork::ManualOnly)
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build debt writer");
        let namespace = NamespaceId::parse("demo").expect("namespace id");
        writer
            .create_namespace(&namespace, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let mut stalled = false;
        for index in 0..200 {
            let result = writer
                .put_file_bytes(
                    &namespace,
                    &format!("/files/f{index}.txt"),
                    b"payload",
                    PutFileOptions {
                        behavior: PutBehavior::NoReplace,
                        commit_id: None,
                    },
                )
                .await;
            match result {
                Ok(_) => {}
                Err(RuntimeError::Core(error))
                    if matches!(error.code(), ErrorCode::MaintenanceRequired) =>
                {
                    stalled = true;
                    break;
                }
                Err(error) => panic!("unexpected stall error: {error}"),
            }
        }
        assert!(stalled, "ManualOnly writer never hit the backpressure cap");

        // The embedded backend digs itself out: the gated publish schedules
        // its own tick, the backend settles it and resubmits.
        let target = EmbeddedTarget::new(&store_config, None, None)
            .await
            .expect("build embedded target");
        target
            .backend
            .put_file_bytes(
                &NamespacePath {
                    namespace: "demo".to_owned(),
                    absolute_path: "/recovered.txt".to_owned(),
                },
                b"payload",
                PutBehavior::NoReplace,
                None,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("recovery put failed: {} {}", error.code, error.message)
            });
    }
}
