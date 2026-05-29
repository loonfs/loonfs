#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitOp, CommitOpResult, CommitPrecondition,
    CommitRequest, CommitResponse, CompleteUploadRequest, CompleteUploadResponse,
    UploadContentResponse,
};
use loon_api::HeadState;
pub use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId,
    ContentRef, CreateCheckpointResponse, FilesystemOperationResponse, InodeId, MutationResult,
    NamespaceId, NamespaceSummary,
};
use loon_core::publisher::{NamespaceCommitEnginePublishResult, WarmBasisEvent};
pub use loon_core::{
    BootstrapNamespaceError, CoreError, CoreErrorKind, NamespaceMutationCandidate,
    PathMutationIntent, PutFileBehavior,
};
use loon_core::{
    ContentValidationKey, ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl,
    MutationContext, NamespaceCommitEngine, VerifiedNamespaceBasis,
};
use loon_objectstore::keys::namespace_head;
pub use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

pub const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;
pub const DEFAULT_MAX_WAL_TAIL_SEGMENTS: u64 = 32;

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
    pub control_cache_enabled: bool,
    pub max_cached_namespaces: usize,
    pub max_validated_content_refs: usize,
}

impl RuntimeCacheConfig {
    pub fn disabled() -> Self {
        Self {
            basis_cache_enabled: false,
            control_cache_enabled: false,
            max_cached_namespaces: 0,
            max_validated_content_refs: 0,
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            basis_cache_enabled: true,
            control_cache_enabled: true,
            max_cached_namespaces: 64,
            max_validated_content_refs: 16_384,
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
    commit_engines: Mutex<CommitEngineCache>,
    control_cache: Mutex<RuntimeControlCache>,
    validated_content_cache: Mutex<ValidatedContentCache>,
    cache_stats: RuntimeCacheStatsInner,
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
    entries: HashMap<NamespaceId, Arc<VerifiedNamespaceBasis>>,
    order: VecDeque<NamespaceId>,
}

impl BasisCache {
    fn get(&mut self, namespace_id: &NamespaceId) -> Option<Arc<VerifiedNamespaceBasis>> {
        let basis = self.entries.get(namespace_id).cloned()?;
        self.touch(namespace_id);
        Some(basis)
    }

    fn insert(&mut self, basis: Arc<VerifiedNamespaceBasis>, max_cached_namespaces: usize) {
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

#[derive(Debug, Default)]
struct ValidatedContentCache {
    entries: HashMap<ContentValidationKey, ContentRef>,
    by_content_ref: HashMap<ContentRef, HashSet<ContentValidationKey>>,
    order: VecDeque<ContentValidationKey>,
}

impl ValidatedContentCache {
    fn insert(&mut self, key: ContentValidationKey, content_ref: ContentRef, max_entries: usize) {
        if max_entries == 0 {
            return;
        }
        if let Some(previous_ref) = self.entries.insert(key.clone(), content_ref.clone()) {
            self.remove_from_index(&previous_ref, &key);
        }
        self.by_content_ref
            .entry(content_ref)
            .or_default()
            .insert(key.clone());
        self.touch(&key);
        while self.entries.len() > max_entries {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some(content_ref) = self.entries.remove(&evicted) {
                self.remove_from_index(&content_ref, &evicted);
            }
        }
    }

    fn trusted_keys_for_refs(&mut self, content_refs: &[ContentRef]) -> Vec<ContentValidationKey> {
        if content_refs.is_empty() {
            return Vec::new();
        }
        let mut keys = Vec::new();
        let mut seen = HashSet::new();
        for content_ref in content_refs {
            if let Some(candidates) = self.by_content_ref.get(content_ref) {
                for key in candidates {
                    if seen.insert(key.clone()) {
                        keys.push(key.clone());
                    }
                }
            }
        }
        for key in &keys {
            self.touch(key);
        }
        keys
    }

    fn remove_from_index(&mut self, content_ref: &ContentRef, key: &ContentValidationKey) {
        let Some(indexed) = self.by_content_ref.get_mut(content_ref) else {
            return;
        };
        indexed.remove(key);
        if indexed.is_empty() {
            self.by_content_ref.remove(content_ref);
        }
    }

    fn touch(&mut self, key: &ContentValidationKey) {
        self.order.retain(|existing| existing != key);
        self.order.push_back(key.clone());
    }
}

#[derive(Debug, Default)]
struct CommitEngineCache {
    entries: HashMap<NamespaceId, Arc<Mutex<NamespaceCommitEngine>>>,
    order: VecDeque<NamespaceId>,
}

impl CommitEngineCache {
    fn get_or_insert(
        &mut self,
        namespace_id: &NamespaceId,
        max_cached_namespaces: usize,
    ) -> Arc<Mutex<NamespaceCommitEngine>> {
        if let Some(engine) = self.entries.get(namespace_id).cloned() {
            self.touch(namespace_id);
            return engine;
        }
        let engine = Arc::new(Mutex::new(NamespaceCommitEngine::new(namespace_id.clone())));
        self.entries.insert(namespace_id.clone(), engine.clone());
        self.touch(namespace_id);
        while self.entries.len() > max_cached_namespaces {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
        engine
    }

    fn invalidate(&mut self, namespace_id: &NamespaceId) {
        if let Some(engine) = self.entries.remove(namespace_id) {
            engine
                .lock()
                .expect("commit engine lock poisoned")
                .invalidate();
        }
        self.order.retain(|candidate| candidate != namespace_id);
    }

    fn touch(&mut self, namespace_id: &NamespaceId) {
        self.order.retain(|candidate| candidate != namespace_id);
        self.order.push_back(namespace_id.clone());
    }
}

#[derive(Debug, Default)]
struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, NamespaceControlCacheEntry>,
    namespace_order: VecDeque<NamespaceId>,
}

#[derive(Debug, Default)]
struct NamespaceControlCacheEntry {
    head: Option<CachedControl<HeadState>>,
}

#[derive(Debug, Clone)]
struct CachedControl<T> {
    identity: ControlObjectIdentity,
    _marker: std::marker::PhantomData<T>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    pub publish_warm_basis_hits: usize,
    pub publish_warm_basis_misses: usize,
    pub publish_warm_basis_invalidations: usize,
    pub publish_warm_basis_advances: usize,
}

#[derive(Debug, Default)]
struct RuntimeCacheStatsInner {
    publish_warm_basis_hits: AtomicUsize,
    publish_warm_basis_misses: AtomicUsize,
    publish_warm_basis_invalidations: AtomicUsize,
    publish_warm_basis_advances: AtomicUsize,
}

impl RuntimeCacheStatsInner {
    fn snapshot(&self) -> RuntimeCacheStats {
        RuntimeCacheStats {
            publish_warm_basis_hits: self.publish_warm_basis_hits.load(Ordering::SeqCst),
            publish_warm_basis_misses: self.publish_warm_basis_misses.load(Ordering::SeqCst),
            publish_warm_basis_invalidations: self
                .publish_warm_basis_invalidations
                .load(Ordering::SeqCst),
            publish_warm_basis_advances: self.publish_warm_basis_advances.load(Ordering::SeqCst),
        }
    }

    fn record_publish_result(&self, result: &NamespaceCommitEnginePublishResult) {
        match result.warm_basis_event {
            WarmBasisEvent::Reused => {
                self.publish_warm_basis_hits.fetch_add(1, Ordering::SeqCst);
            }
            WarmBasisEvent::ColdLoaded | WarmBasisEvent::InvalidatedThenColdLoaded => {
                self.publish_warm_basis_misses
                    .fetch_add(1, Ordering::SeqCst);
            }
            WarmBasisEvent::Disabled => {}
        }
        if result.warm_basis_event == WarmBasisEvent::InvalidatedThenColdLoaded
            || result.warm_basis_update.is_invalidated()
        {
            self.publish_warm_basis_invalidations
                .fetch_add(1, Ordering::SeqCst);
        }
        if result.warm_basis_update.is_advanced() {
            self.publish_warm_basis_advances
                .fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl RuntimeControlCache {
    fn namespace_head(&mut self, namespace_id: &NamespaceId) -> Option<CachedControl<HeadState>> {
        let head = self.namespaces.get(namespace_id)?.head.clone()?;
        self.touch_namespace(namespace_id);
        Some(head)
    }

    fn insert_namespace_head(
        &mut self,
        namespace_id: &NamespaceId,
        head: CachedControl<HeadState>,
        max_cached_namespaces: usize,
    ) {
        if max_cached_namespaces == 0 {
            return;
        }
        self.namespace_entry(namespace_id, max_cached_namespaces)
            .head = Some(head);
    }

    fn invalidate_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespaces.remove(namespace_id);
        self.namespace_order
            .retain(|candidate| candidate != namespace_id);
    }

    fn invalidate_namespace_head(&mut self, namespace_id: &NamespaceId) {
        if let Some(entry) = self.namespaces.get_mut(namespace_id) {
            entry.head = None;
        }
    }

    fn namespace_entry(
        &mut self,
        namespace_id: &NamespaceId,
        max_cached_namespaces: usize,
    ) -> &mut NamespaceControlCacheEntry {
        self.namespaces.entry(namespace_id.clone()).or_default();
        self.touch_namespace(namespace_id);
        while self.namespaces.len() > max_cached_namespaces {
            let Some(evicted) = self.namespace_order.pop_front() else {
                break;
            };
            self.namespaces.remove(&evicted);
        }
        self.namespaces
            .get_mut(namespace_id)
            .expect("namespace cache entry should exist")
    }

    fn touch_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespace_order
            .retain(|candidate| candidate != namespace_id);
        self.namespace_order.push_back(namespace_id.clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStatus {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub checkpoint_hint_seq: Option<ChangeSeq>,
    pub wal_tail_segments: u64,
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTickOptions {
    pub max_wal_tail_segments: u64,
}

impl Default for MaintenanceTickOptions {
    fn default() -> Self {
        Self {
            max_wal_tail_segments: DEFAULT_MAX_WAL_TAIL_SEGMENTS,
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
                commit_engines: Mutex::new(CommitEngineCache::default()),
                control_cache: Mutex::new(RuntimeControlCache::default()),
                validated_content_cache: Mutex::new(ValidatedContentCache::default()),
                cache_stats: RuntimeCacheStatsInner::default(),
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
        Ok(NamespaceStatus {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            checkpoint_hint_seq: summary.checkpoint_hint_seq,
            wal_tail_segments: summary.wal_tail_segments,
            retention_floor_seq: summary.retention_floor_seq,
        })
    }

    pub fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        if options.max_wal_tail_segments == 0 {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        }

        let status_before = self.namespace_status(namespace_id)?;
        let observed_head_seq = status_before.head_seq;
        if status_before.wal_tail_segments < options.max_wal_tail_segments {
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
            basis.as_ref(),
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
            basis.as_ref(),
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
            basis.as_ref(),
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
        let (response, validation_key) = loon_core::upload_content_with_validation_key(
            self.store(),
            namespace_id,
            upload_id,
            bytes,
            &self.mutation_context(),
        )?;
        self.cache_validated_content(validation_key, response.content_ref.clone());
        Ok(response)
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
        self.publish_namespace_mutations_batch(
            namespace_id,
            vec![NamespaceMutationCandidate::Commit(request)],
        )
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Store(
                "empty commit batch".to_owned(),
            )))
        })
    }

    pub fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.publish_namespace_mutations_batch(
            namespace_id,
            requests
                .into_iter()
                .map(NamespaceMutationCandidate::Commit)
                .collect(),
        )
    }

    pub fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let content_validation_hints = self.content_validation_hints_for_candidates(&candidates);
        if self.commit_engine_cache_enabled() {
            let engine = self.commit_engine(namespace_id);
            let publish = {
                let mut engine = engine.lock().expect("commit engine lock poisoned");
                engine.publish_batch_with_content_validation_hints(
                    self.store(),
                    candidates,
                    &self.mutation_context(),
                    &content_validation_hints,
                )
            };
            self.inner.cache_stats.record_publish_result(&publish);
            if let Some(basis) = publish.warm_basis_update.basis() {
                self.cache_basis(Arc::new(basis.clone()));
            } else if publish.warm_basis_update.is_invalidated() {
                self.invalidate_namespace_cache(namespace_id);
            }
            return publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
        }

        let results: Vec<_> =
            loon_core::publish_namespace_mutations_batch_with_content_validation_hints(
                self.store(),
                namespace_id,
                candidates,
                &self.mutation_context(),
                &content_validation_hints,
            )
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect();
        self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        results
    }

    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner.cache_stats.snapshot()
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

    fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedControl<HeadState>, ControlObjectLoadError> {
        let cache_config = &self.inner.config.runtime_cache;
        if !self.control_cache_enabled() {
            return loon_core::load_namespace_head_control(self.store(), namespace_id)
                .map(cached_head);
        }

        let cached = self
            .inner
            .control_cache
            .lock()
            .expect("control cache lock poisoned")
            .namespace_head(namespace_id);
        if let Some(head) = cached {
            match self.cached_control_identity_matches(
                &namespace_head(namespace_id.as_str()),
                &head.identity,
            ) {
                Ok(true) => return Ok(head),
                Ok(false) => self
                    .inner
                    .control_cache
                    .lock()
                    .expect("control cache lock poisoned")
                    .invalidate_namespace_head(namespace_id),
                Err(error) => {
                    self.inner
                        .control_cache
                        .lock()
                        .expect("control cache lock poisoned")
                        .invalidate_namespace_head(namespace_id);
                    return Err(error);
                }
            }
        }

        let loaded = match loon_core::load_namespace_head_control(self.store(), namespace_id) {
            Ok(loaded) => loaded,
            Err(error) => {
                self.inner
                    .control_cache
                    .lock()
                    .expect("control cache lock poisoned")
                    .invalidate_namespace_head(namespace_id);
                return Err(error);
            }
        };
        let head = cached_head(loaded);
        self.inner
            .control_cache
            .lock()
            .expect("control cache lock poisoned")
            .insert_namespace_head(
                namespace_id,
                head.clone(),
                cache_config.max_cached_namespaces,
            );
        Ok(head)
    }

    fn cached_control_identity_matches(
        &self,
        object_key: &str,
        identity: &ControlObjectIdentity,
    ) -> std::result::Result<bool, ControlObjectLoadError> {
        let metadata = self
            .store()
            .head(object_key)
            .map_err(|error| ControlObjectLoadError::Store(error.to_string()))?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: object_key.to_owned(),
            })?;
        let Some(etag) = metadata.etag else {
            return Err(ControlObjectLoadError::Store(format!(
                "missing control object etag for `{object_key}`"
            )));
        };
        Ok(etag == identity.etag)
    }

    fn control_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.control_cache_enabled && cache_config.max_cached_namespaces > 0
    }

    fn commit_engine_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.basis_cache_enabled && cache_config.max_cached_namespaces > 0
    }

    fn validated_content_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_validated_content_refs > 0
    }

    fn cache_validated_content(&self, key: ContentValidationKey, content_ref: ContentRef) {
        let cache_config = &self.inner.config.runtime_cache;
        if cache_config.max_validated_content_refs == 0 {
            return;
        }
        self.inner
            .validated_content_cache
            .lock()
            .expect("validated content cache lock poisoned")
            .insert(key, content_ref, cache_config.max_validated_content_refs);
    }

    fn content_validation_hints_for_candidates(
        &self,
        candidates: &[NamespaceMutationCandidate],
    ) -> Vec<ContentValidationKey> {
        if !self.validated_content_cache_enabled() {
            return Vec::new();
        }
        let content_refs = candidate_content_refs(candidates);
        self.inner
            .validated_content_cache
            .lock()
            .expect("validated content cache lock poisoned")
            .trusted_keys_for_refs(&content_refs)
    }

    fn commit_engine(&self, namespace_id: &NamespaceId) -> Arc<Mutex<NamespaceCommitEngine>> {
        let cache_config = &self.inner.config.runtime_cache;
        self.inner
            .commit_engines
            .lock()
            .expect("commit engine cache lock poisoned")
            .get_or_insert(namespace_id, cache_config.max_cached_namespaces)
    }

    fn basis_for_read(&self, namespace_id: &NamespaceId) -> Result<Arc<VerifiedNamespaceBasis>> {
        let cache_config = &self.inner.config.runtime_cache;
        if !cache_config.basis_cache_enabled || cache_config.max_cached_namespaces == 0 {
            return Ok(Arc::new(
                loon_core::load_verified_namespace_basis(self.store(), namespace_id)
                    .map_err(CoreError::from)?,
            ));
        }

        let cached = self
            .inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .get(namespace_id);
        if let Some(basis) = cached {
            if self.control_cache_enabled() {
                match self.load_namespace_head_cached(namespace_id) {
                    Ok(head) if head.identity.etag == basis.head_etag => {
                        return Ok(basis);
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            } else {
                match loon_core::load_namespace_head_identity(self.store(), namespace_id) {
                    Ok(identity) if identity.head_etag == basis.head_etag => {
                        return Ok(basis);
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            }
        }

        let basis = loon_core::load_verified_namespace_basis(self.store(), namespace_id)
            .map_err(CoreError::from)?;
        let basis = Arc::new(basis);
        self.cache_basis(Arc::clone(&basis));
        Ok(basis)
    }

    fn cache_basis(&self, basis: Arc<VerifiedNamespaceBasis>) {
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
        self.inner
            .control_cache
            .lock()
            .expect("control cache lock poisoned")
            .invalidate_namespace(namespace_id);
        self.inner
            .commit_engines
            .lock()
            .expect("commit engine cache lock poisoned")
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

fn candidate_content_refs(candidates: &[NamespaceMutationCandidate]) -> Vec<ContentRef> {
    let mut content_refs = Vec::new();
    for candidate in candidates {
        match candidate {
            NamespaceMutationCandidate::Commit(request) => {
                for op in &request.ops {
                    match op {
                        CommitOp::CreateFile { content_ref, .. }
                        | CommitOp::ReplaceFile { content_ref, .. } => {
                            content_refs.push(content_ref.clone());
                        }
                        _ => {}
                    }
                }
            }
            NamespaceMutationCandidate::Path(PathMutationIntent::PutFile {
                content_ref, ..
            }) => {
                content_refs.push(content_ref.clone());
            }
            NamespaceMutationCandidate::Path(_) => {}
        }
    }
    content_refs
}

fn cached_head(loaded: LoadedHeadControl) -> CachedControl<HeadState> {
    CachedControl {
        identity: loaded.identity,
        _marker: std::marker::PhantomData,
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
