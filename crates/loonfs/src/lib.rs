#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitOp, CommitOpResult, CommitPrecondition,
    CommitRequest, CommitResponse, CompleteUploadRequest, CompleteUploadResponse,
    UploadContentResponse,
};
pub use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId,
    ContentRef, CreateCheckpointResponse, FilesystemOperationResponse, InodeId, MutationResult,
    NamespaceId, NamespaceSummary,
};
pub use loon_core::{
    BootstrapNamespaceError, CoreError, CoreErrorKind, NamespaceMutationCandidate,
    PathMutationIntent, PutFileBehavior,
};
use loon_core::{MutationContext, VerifiedNamespaceBasis};
pub use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

pub const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;
pub const DEFAULT_MAX_UNCHECKPOINTED_COMMITS: u64 = 1_000;

pub type SharedObjectStore = Arc<dyn ObjectStore + Send + Sync>;
pub type Result<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Bootstrap(#[from] BootstrapNamespaceError),
    #[error("invalid runtime config: {0}")]
    Config(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsConfig {
    pub writer_id: String,
    pub writer_version: String,
    pub lease_duration_ms: u64,
    pub runtime_cache: RuntimeCacheConfig,
}

impl FsConfig {
    pub fn new(writer_id: impl Into<String>) -> Self {
        Self {
            writer_id: writer_id.into(),
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            runtime_cache: RuntimeCacheConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCacheConfig {
    pub basis_cache_enabled: bool,
    pub max_cached_namespaces: usize,
}

impl RuntimeCacheConfig {
    pub fn disabled() -> Self {
        Self {
            basis_cache_enabled: false,
            max_cached_namespaces: 0,
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            basis_cache_enabled: true,
            max_cached_namespaces: 64,
        }
    }
}

#[derive(Clone)]
pub struct Fs {
    inner: Arc<FsInner>,
}

struct FsInner {
    store: SharedObjectStore,
    config: FsConfig,
    basis_cache: Mutex<BasisCache>,
}

pub struct FsBuilder {
    store: SharedObjectStore,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    runtime_cache: RuntimeCacheConfig,
}

#[derive(Debug, Default)]
struct BasisCache {
    entries: HashMap<NamespaceId, VerifiedNamespaceBasis>,
    order: VecDeque<NamespaceId>,
}

impl BasisCache {
    fn get(&mut self, namespace_id: &NamespaceId) -> Option<VerifiedNamespaceBasis> {
        let basis = self.entries.get(namespace_id).cloned()?;
        self.touch(namespace_id);
        Some(basis)
    }

    fn insert(&mut self, basis: VerifiedNamespaceBasis, max_cached_namespaces: usize) {
        if max_cached_namespaces == 0 {
            return;
        }
        let namespace_id = basis.head.namespace_id.clone();
        self.entries.insert(namespace_id.clone(), basis);
        self.touch(&namespace_id);
        while self.entries.len() > max_cached_namespaces {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn invalidate(&mut self, namespace_id: &NamespaceId) {
        self.entries.remove(namespace_id);
        self.order.retain(|candidate| candidate != namespace_id);
    }

    fn touch(&mut self, namespace_id: &NamespaceId) {
        self.order.retain(|candidate| candidate != namespace_id);
        self.order.push_back(namespace_id.clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStatus {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub checkpoint_hint_seq: Option<ChangeSeq>,
    pub uncheckpointed_commits: u64,
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTickOptions {
    pub max_uncheckpointed_commits: u64,
}

impl Default for MaintenanceTickOptions {
    fn default() -> Self {
        Self {
            max_uncheckpointed_commits: DEFAULT_MAX_UNCHECKPOINTED_COMMITS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceTickOutcome {
    NotNeeded,
    CheckpointPublished {
        checkpoint_seq: ChangeSeq,
    },
    CheckpointSuperseded {
        attempted_seq: ChangeSeq,
        checkpoint_hint_seq: ChangeSeq,
    },
    CheckpointPublishRaceLost {
        observed_head_seq: ChangeSeq,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickResult {
    pub namespace_id: NamespaceId,
    pub status_before: NamespaceStatus,
    pub outcome: MaintenanceTickOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CreateNamespaceOptions {
    pub allow_existing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    pub behavior: PutFileBehavior,
    pub commit_id: Option<CommitId>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: PutFileBehavior::CreateOnly,
            commit_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateDirOptions {
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteOptions {
    pub recursive: bool,
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MoveOptions {
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CopyOptions {
    pub commit_id: Option<CommitId>,
}

impl FsBuilder {
    pub fn new(store: SharedObjectStore) -> Self {
        Self {
            store,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            runtime_cache: RuntimeCacheConfig::default(),
        }
    }

    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.runtime_cache = runtime_cache;
        self
    }

    pub fn build(self) -> Result<Fs> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        Fs::open(
            self.store,
            FsConfig {
                writer_id,
                writer_version: self.writer_version,
                lease_duration_ms: self.lease_duration_ms,
                runtime_cache: self.runtime_cache,
            },
        )
    }
}

impl Fs {
    pub fn open(store: SharedObjectStore, config: FsConfig) -> Result<Self> {
        validate_config(&config)?;
        Ok(Self {
            inner: Arc::new(FsInner {
                store,
                config,
                basis_cache: Mutex::new(BasisCache::default()),
            }),
        })
    }

    pub fn builder(store: SharedObjectStore) -> FsBuilder {
        FsBuilder::new(store)
    }

    pub fn config(&self) -> &FsConfig {
        &self.inner.config
    }

    pub fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let result = loon_core::bootstrap_namespace(
            self.store(),
            namespace_id,
            &self.mutation_context(),
            options.allow_existing,
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result =
            loon_core::fork_namespace(self.store(), source, target, &self.mutation_context())
                .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(source);
        }
        if result.is_ok() {
            self.invalidate_namespace_cache(target);
        }
        result
    }

    pub fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>> {
        Ok(loon_core::list_namespaces(self.store())?)
    }

    pub fn namespace_status(&self, namespace_id: &NamespaceId) -> Result<NamespaceStatus> {
        let summary = loon_core::load_namespace_head_summary(self.store(), namespace_id)?;
        let checkpoint_seq = summary
            .checkpoint_hint_seq
            .map(|seq| seq.0)
            .unwrap_or_default();
        Ok(NamespaceStatus {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            checkpoint_hint_seq: summary.checkpoint_hint_seq,
            uncheckpointed_commits: summary.head_seq.0.saturating_sub(checkpoint_seq),
            retention_floor_seq: summary.retention_floor_seq,
        })
    }

    pub fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        if options.max_uncheckpointed_commits == 0 {
            return Err(RuntimeError::Config(
                "max_uncheckpointed_commits must be greater than zero".to_owned(),
            ));
        }

        let status_before = self.namespace_status(namespace_id)?;
        let observed_head_seq = status_before.head_seq;
        if status_before.uncheckpointed_commits < options.max_uncheckpointed_commits {
            return Ok(MaintenanceTickResult {
                namespace_id: namespace_id.clone(),
                status_before,
                outcome: MaintenanceTickOutcome::NotNeeded,
            });
        }

        let checkpoint = match self.create_checkpoint(namespace_id) {
            Ok(checkpoint) => checkpoint,
            Err(RuntimeError::Core(error)) if error.kind() == CoreErrorKind::StaleHead => {
                return Ok(MaintenanceTickResult {
                    namespace_id: namespace_id.clone(),
                    status_before,
                    outcome: MaintenanceTickOutcome::CheckpointPublishRaceLost {
                        observed_head_seq,
                    },
                });
            }
            Err(error) => return Err(error),
        };

        let outcome = if checkpoint.checkpoint_hint_points_at_checkpoint {
            MaintenanceTickOutcome::CheckpointPublished {
                checkpoint_seq: checkpoint.checkpoint_seq,
            }
        } else {
            let Some(checkpoint_hint_seq) = checkpoint.checkpoint_hint_seq else {
                return Err(RuntimeError::Core(CoreError::Store(
                    "checkpoint hint publication returned no checkpoint hint".to_owned(),
                )));
            };
            MaintenanceTickOutcome::CheckpointSuperseded {
                attempted_seq: checkpoint.checkpoint_seq,
                checkpoint_hint_seq,
            }
        };

        Ok(MaintenanceTickResult {
            namespace_id: namespace_id.clone(),
            status_before,
            outcome,
        })
    }

    pub fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::resolve_path_from_basis(
            self.store(),
            &basis,
            absolute_path,
        )?)
    }

    pub fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::list_path_from_basis(
            self.store(),
            &basis,
            absolute_path,
        )?)
    }

    pub fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::read_file_bytes_from_basis(
            self.store(),
            &basis,
            absolute_path,
        )?)
    }

    pub fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::put_file_bytes(
            self.store(),
            namespace_id,
            absolute_path,
            bytes,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::put_file_content_ref(
            self.store(),
            namespace_id,
            absolute_path,
            content_ref,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn create_dir(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::create_dir_path(
            self.store(),
            namespace_id,
            absolute_path,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        let commit_id = options.commit_id.as_ref().map(CommitId::as_str);
        let result = if options.recursive {
            loon_core::delete_path(
                self.store(),
                namespace_id,
                absolute_path,
                &self.mutation_context(),
                commit_id,
            )
        } else {
            loon_core::delete_path_non_recursive(
                self.store(),
                namespace_id,
                absolute_path,
                &self.mutation_context(),
                commit_id,
            )
        }
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::move_path(
            self.store(),
            namespace_id,
            from_path,
            to_path,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::copy_file_path(
            self.store(),
            namespace_id,
            from_path,
            to_path,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn begin_upload(&self, namespace_id: &NamespaceId) -> Result<BeginUploadResponse> {
        Ok(loon_core::begin_upload(
            self.store(),
            namespace_id,
            &self.mutation_context(),
        )?)
    }

    pub fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(loon_core::upload_content(
            self.store(),
            namespace_id,
            upload_id,
            bytes,
            &self.mutation_context(),
        )?)
    }

    pub fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(loon_core::complete_upload(
            self.store(),
            namespace_id,
            upload_id,
            request,
            &self.mutation_context(),
        )?)
    }

    pub fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        let result = loon_core::commit_operations(
            self.store(),
            namespace_id,
            request,
            &self.mutation_context(),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        let results: Vec<_> = loon_core::commit_operations_batch(
            self.store(),
            namespace_id,
            requests,
            &self.mutation_context(),
        )
        .into_iter()
        .map(|result| result.map_err(RuntimeError::Core))
        .collect();
        self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        results
    }

    pub fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let results: Vec<_> = loon_core::publish_namespace_mutations_batch(
            self.store(),
            namespace_id,
            candidates,
            &self.mutation_context(),
        )
        .into_iter()
        .map(|result| result.map_err(RuntimeError::Core))
        .collect();
        self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        results
    }

    pub fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse> {
        Ok(loon_core::list_changes_after(
            self.store(),
            namespace_id,
            after_seq,
        )?)
    }

    pub fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        let result =
            loon_core::create_checkpoint(self.store(), namespace_id, &self.mutation_context())
                .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = loon_core::advance_retention_floor(
            self.store(),
            namespace_id,
            &self.mutation_context(),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    fn basis_for_read(&self, namespace_id: &NamespaceId) -> Result<VerifiedNamespaceBasis> {
        let cache_config = &self.inner.config.runtime_cache;
        if !cache_config.basis_cache_enabled || cache_config.max_cached_namespaces == 0 {
            return Ok(
                loon_core::load_verified_namespace_basis(self.store(), namespace_id)
                    .map_err(CoreError::from)?,
            );
        }

        let cached = self
            .inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .get(namespace_id);
        if let Some(basis) = cached {
            match loon_core::load_namespace_head_identity(self.store(), namespace_id) {
                Ok(identity)
                    if identity.namespace_id == *namespace_id
                        && identity.head_etag == basis.head_etag =>
                {
                    return Ok(basis);
                }
                Ok(_) | Err(_) => {
                    self.invalidate_namespace_cache(namespace_id);
                }
            }
        }

        let basis = loon_core::load_verified_namespace_basis(self.store(), namespace_id)
            .map_err(CoreError::from)?;
        self.cache_basis(basis.clone());
        Ok(basis)
    }

    fn cache_basis(&self, basis: VerifiedNamespaceBasis) {
        let cache_config = &self.inner.config.runtime_cache;
        if !cache_config.basis_cache_enabled || cache_config.max_cached_namespaces == 0 {
            return;
        }
        self.inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .insert(basis, cache_config.max_cached_namespaces);
    }

    fn invalidate_namespace_cache(&self, namespace_id: &NamespaceId) {
        self.inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .invalidate(namespace_id);
    }

    fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(namespace_id);
        }
        result
    }

    fn invalidate_namespace_cache_after_batch(
        &self,
        namespace_id: &NamespaceId,
        results: &[Result<CommitResponse>],
    ) {
        if results.iter().any(should_invalidate_after_result) {
            self.invalidate_namespace_cache(namespace_id);
        }
    }

    fn store(&self) -> &(dyn ObjectStore + Send + Sync) {
        self.inner.store.as_ref()
    }

    fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.inner.config.writer_id.clone(),
            writer_version: self.inner.config.writer_version.clone(),
            now_ms: current_time_ms(),
            lease_duration_ms: self.inner.config.lease_duration_ms,
        }
    }
}

fn default_writer_version() -> String {
    format!("loonfs/{}", env!("CARGO_PKG_VERSION"))
}

fn validate_config(config: &FsConfig) -> Result<()> {
    if config.writer_id.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_id must not be empty".to_owned(),
        ));
    }
    if config.writer_version.trim().is_empty() {
        return Err(RuntimeError::Config(
            "writer_version must not be empty".to_owned(),
        ));
    }
    if config.lease_duration_ms == 0 {
        return Err(RuntimeError::Config(
            "lease_duration_ms must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn should_invalidate_after_result<T>(result: &Result<T>) -> bool {
    match result {
        Ok(_) => true,
        Err(RuntimeError::Core(error)) if error.kind() == CoreErrorKind::StaleHead => true,
        _ => false,
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
