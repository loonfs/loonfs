#![forbid(unsafe_code)]

mod trace;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use loon_api::sha256_digest;
pub use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitAnnotations, CommitDelta, CommitOp, CommitOpResult,
    CommitPrecondition, CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, RenameMode, UploadContentResponse, UploadMode,
};
use loon_api::wire::control::HeadState;
pub use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId,
    ContentRef, ContentRefKind, CreateCheckpointResponse, DisplayName, FileRevision,
    FilesystemOperationResponse, InodeId, InodeKind, ListFileRevisionsResponse, MutationResult,
    NameKey, NamePolicy, NamespaceId, NamespaceSummary, RevisionNo,
};
use loon_core::publisher::{BasisReuseEvent, NamespaceCommitEnginePublishResult};
pub use loon_core::{
    BootstrapNamespaceError, CoreError, CoreErrorKind, MetadataTableCacheConfig,
    NamespaceMutationCandidate, PathMutationIntent, PutFileBehavior,
};
use loon_core::{
    ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl, MutationContext,
    NamespaceCommitEngine, VerifiedNamespaceBasis,
};
use loon_objectstore::keys::namespace_head;
use loon_objectstore::{ByteRange, ObjectMetadata, PutMode};
pub use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;
pub use trace::{TraceMode, TraceStoreKind};

pub const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;
pub const DEFAULT_MAX_WAL_TAIL_SEGMENTS: u64 = 32;
const MAX_UPLOADED_CONTENT_PROOF_ENTRIES: usize = 16_384;
const UPLOADED_CONTENT_PROOF_TTL: Duration = Duration::from_secs(30);

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
    pub trace_mode: TraceMode,
    pub trace_store_kind: TraceStoreKind,
}

impl FsConfig {
    pub fn new(writer_id: impl Into<String>) -> Self {
        Self {
            writer_id: writer_id.into(),
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Embedded,
            trace_store_kind: TraceStoreKind::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCacheConfig {
    pub basis_cache_enabled: bool,
    pub control_cache_enabled: bool,
    pub max_cached_namespaces: usize,
    pub metadata_table_cache: MetadataTableCacheConfig,
}

impl RuntimeCacheConfig {
    pub fn disabled() -> Self {
        Self {
            basis_cache_enabled: false,
            control_cache_enabled: false,
            max_cached_namespaces: 0,
            metadata_table_cache: MetadataTableCacheConfig {
                enabled: false,
                max_blocks: 0,
                max_decoded_bytes: None,
            },
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            basis_cache_enabled: true,
            control_cache_enabled: true,
            max_cached_namespaces: 64,
            metadata_table_cache: MetadataTableCacheConfig::default(),
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
    metadata_table_cache: loon_core::MetadataTableCache,
    uploaded_content_proofs: Mutex<UploadedContentProofCache>,
    cache_stats: RuntimeCacheStatsInner,
}

pub struct FsBuilder {
    store: SharedObjectStore,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    runtime_cache: RuntimeCacheConfig,
    trace_store_kind: TraceStoreKind,
}

#[derive(Debug, Default)]
struct BasisCache {
    entries: HashMap<NamespaceId, CachedVerifiedBasis>,
    order: VecDeque<NamespaceId>,
}

#[derive(Debug, Clone)]
struct CachedVerifiedBasis {
    basis: Arc<VerifiedNamespaceBasis>,
    head_etag_reuse_token: String,
}

impl CachedVerifiedBasis {
    fn new(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            head_etag_reuse_token: basis.head_etag.clone(),
            basis,
        }
    }

    fn basis_arc(&self) -> Arc<VerifiedNamespaceBasis> {
        Arc::clone(&self.basis)
    }

    fn matches_head_etag(&self, head_etag: &str) -> bool {
        self.head_etag_reuse_token == head_etag
    }

    fn matches_head_etag_probe(&self, probe: &loon_core::NamespaceHeadEtagProbe) -> bool {
        self.matches_head_etag(&probe.head_etag)
    }
}

impl BasisCache {
    fn get(&mut self, namespace_id: &NamespaceId) -> Option<CachedVerifiedBasis> {
        let basis = self.entries.get(namespace_id).cloned()?;
        self.touch(namespace_id);
        Some(basis)
    }

    fn insert(&mut self, basis: Arc<VerifiedNamespaceBasis>, max_cached_namespaces: usize) {
        if max_cached_namespaces == 0 {
            return;
        }
        let namespace_id = basis.head.namespace_id.clone();
        self.entries
            .insert(namespace_id.clone(), CachedVerifiedBasis::new(basis));
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UploadedContentProofKey {
    namespace_id: NamespaceId,
    digest: String,
}

#[derive(Debug, Default)]
struct UploadedContentProofCache {
    entries: HashMap<UploadedContentProofKey, UploadedContentProof>,
    order: VecDeque<UploadedContentProofKey>,
}

#[derive(Debug, Clone)]
struct UploadedContentProof {
    content_ref: ContentRef,
    expires_at: SystemTime,
}

impl UploadedContentProofCache {
    fn insert(&mut self, namespace_id: &NamespaceId, content_ref: ContentRef) {
        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: content_ref.digest.clone(),
        };
        let proof = UploadedContentProof {
            content_ref,
            expires_at: SystemTime::now() + UPLOADED_CONTENT_PROOF_TTL,
        };
        self.entries.insert(key.clone(), proof);
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        while self.entries.len() > MAX_UPLOADED_CONTENT_PROOF_ENTRIES {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn get(&mut self, namespace_id: &NamespaceId, digest: &str) -> Option<ContentRef> {
        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: digest.to_owned(),
        };
        let proof = self.entries.get(&key)?;
        if SystemTime::now() > proof.expires_at {
            self.entries.remove(&key);
            self.order.retain(|existing| existing != &key);
            return None;
        }
        Some(proof.content_ref.clone())
    }
}

#[cfg(test)]
mod proof_cache_tests {
    use super::{UploadedContentProofCache, UploadedContentProofKey};
    use loon_api::{ContentRef, NamespaceId};
    use std::time::{Duration, SystemTime};

    #[test]
    fn uploaded_content_proof_expires_without_refresh_on_lookup() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_ref = ContentRef::whole_file_v0(b"bytes");
        let mut cache = UploadedContentProofCache::default();
        cache.insert(&namespace_id, content_ref.clone());

        assert_eq!(
            cache.get(&namespace_id, &content_ref.digest),
            Some(content_ref.clone())
        );

        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: content_ref.digest.clone(),
        };
        cache
            .entries
            .get_mut(&key)
            .expect("proof exists")
            .expires_at = SystemTime::now() - Duration::from_secs(1);

        assert_eq!(cache.get(&namespace_id, &content_ref.digest), None);
        assert!(!cache.entries.contains_key(&key));
    }
}

struct UploadedContentProofStore<'a> {
    inner: &'a (dyn ObjectStore + Send + Sync),
    namespace_id: &'a NamespaceId,
    proofs: &'a Mutex<UploadedContentProofCache>,
}

impl UploadedContentProofStore<'_> {
    fn proof_metadata(&self, key: &str) -> Option<ObjectMetadata> {
        let digest = content_blob_digest_from_key(key)?;
        let content_ref = self
            .proofs
            .lock()
            .expect("uploaded content proof cache lock poisoned")
            .get(self.namespace_id, &digest)?;
        Some(ObjectMetadata {
            etag: None,
            size_bytes: content_ref.size_bytes,
            checksum_sha256: Some(content_ref.digest),
        })
    }

    fn record_write_proof(&self, key: &str, bytes: &[u8]) {
        let Some(digest) = content_blob_digest_from_key(key) else {
            return;
        };
        if sha256_digest(bytes) != digest {
            return;
        }
        self.proofs
            .lock()
            .expect("uploaded content proof cache lock poisoned")
            .insert(
                self.namespace_id,
                ContentRef {
                    kind: loon_api::ContentRefKind::WholeFileV0,
                    digest,
                    size_bytes: bytes.len() as u64,
                },
            );
    }
}

impl ObjectStore for UploadedContentProofStore<'_> {
    fn head(&self, key: &str) -> std::result::Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn head_with_checksum(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectMetadata>, ObjectStoreError> {
        if let Some(metadata) = self.proof_metadata(key) {
            return Ok(Some(metadata));
        }
        self.inner.head_with_checksum(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> std::result::Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> std::result::Result<ObjectMetadata, ObjectStoreError> {
        let metadata = self.inner.put(key, bytes, mode)?;
        self.record_write_proof(key, bytes);
        Ok(metadata)
    }

    fn delete(&self, key: &str) -> std::result::Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> std::result::Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}

fn content_blob_digest_from_key(key: &str) -> Option<String> {
    if !key.contains("/blobs/sha256/") {
        return None;
    }
    let hex = key.rsplit('/').next()?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("sha256:{}", hex.to_ascii_lowercase()))
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
    state: T,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    pub publish_warm_basis_hits: usize,
    pub publish_warm_basis_misses: usize,
    pub publish_warm_basis_invalidations: usize,
    pub publish_warm_basis_advances: usize,
    pub read_materialized_table_hits: usize,
    pub read_full_basis_fallbacks: usize,
    pub metadata_table_cache_hits: usize,
    pub metadata_table_cache_misses: usize,
    pub metadata_table_cache_inserts: usize,
    pub metadata_table_cache_evictions: usize,
}

#[derive(Debug, Default)]
struct RuntimeCacheStatsInner {
    publish_warm_basis_hits: AtomicUsize,
    publish_warm_basis_misses: AtomicUsize,
    publish_warm_basis_invalidations: AtomicUsize,
    publish_warm_basis_advances: AtomicUsize,
    read_materialized_table_hits: AtomicUsize,
    read_full_basis_fallbacks: AtomicUsize,
}

impl RuntimeCacheStatsInner {
    fn snapshot(
        &self,
        metadata_table_cache: loon_core::MetadataTableCacheStats,
    ) -> RuntimeCacheStats {
        RuntimeCacheStats {
            publish_warm_basis_hits: self.publish_warm_basis_hits.load(Ordering::SeqCst),
            publish_warm_basis_misses: self.publish_warm_basis_misses.load(Ordering::SeqCst),
            publish_warm_basis_invalidations: self
                .publish_warm_basis_invalidations
                .load(Ordering::SeqCst),
            publish_warm_basis_advances: self.publish_warm_basis_advances.load(Ordering::SeqCst),
            read_materialized_table_hits: self.read_materialized_table_hits.load(Ordering::SeqCst),
            read_full_basis_fallbacks: self.read_full_basis_fallbacks.load(Ordering::SeqCst),
            metadata_table_cache_hits: metadata_table_cache.hits,
            metadata_table_cache_misses: metadata_table_cache.misses,
            metadata_table_cache_inserts: metadata_table_cache.inserts,
            metadata_table_cache_evictions: metadata_table_cache.evictions,
        }
    }

    fn record_publish_result(&self, result: &NamespaceCommitEnginePublishResult) {
        match result.basis_reuse_event {
            BasisReuseEvent::ReusedAfterHeadEtagMatch => {
                self.publish_warm_basis_hits.fetch_add(1, Ordering::SeqCst);
            }
            BasisReuseEvent::ColdLoaded | BasisReuseEvent::InvalidatedThenColdLoaded => {
                self.publish_warm_basis_misses
                    .fetch_add(1, Ordering::SeqCst);
            }
            BasisReuseEvent::Disabled => {}
        }
        if result.basis_reuse_event == BasisReuseEvent::InvalidatedThenColdLoaded
            || result.verified_basis_cache_update.is_invalidated()
        {
            self.publish_warm_basis_invalidations
                .fetch_add(1, Ordering::SeqCst);
        }
        if result.verified_basis_cache_update.is_advanced() {
            self.publish_warm_basis_advances
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    fn record_metadata_read_source(&self, source: loon_core::MetadataReadSource) {
        match source {
            loon_core::MetadataReadSource::MaterializedTables => {
                self.read_materialized_table_hits
                    .fetch_add(1, Ordering::SeqCst);
            }
            loon_core::MetadataReadSource::FullBasisFallback => {
                self.read_full_basis_fallbacks
                    .fetch_add(1, Ordering::SeqCst);
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RestoreRevisionOptions {
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
            trace_store_kind: TraceStoreKind::Unknown,
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

    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.trace_store_kind = trace_store_kind;
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
                trace_mode: TraceMode::Embedded,
                trace_store_kind: self.trace_store_kind,
            },
        )
    }
}

impl Fs {
    pub fn open(store: SharedObjectStore, config: FsConfig) -> Result<Self> {
        validate_config(&config)?;
        let metadata_table_cache =
            loon_core::MetadataTableCache::new(config.runtime_cache.metadata_table_cache.clone());
        Ok(Self {
            inner: Arc::new(FsInner {
                store,
                config,
                basis_cache: Mutex::new(BasisCache::default()),
                commit_engines: Mutex::new(CommitEngineCache::default()),
                control_cache: Mutex::new(RuntimeControlCache::default()),
                metadata_table_cache,
                uploaded_content_proofs: Mutex::new(UploadedContentProofCache::default()),
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

    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            result = tracing::field::Empty,
        )
    )]
    pub fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        let span = tracing::Span::current();
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
        let result = (|| {
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
        })();
        span.record("result", trace::result_label(&result));
        result
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.stat",
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
            result = tracing::field::Empty,
        )
    )]
    pub fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
        let result = (|| {
            let head = self.head_for_metadata_read(namespace_id)?;
            if head.state.checkpoint_hint_seq.is_some() {
                if let Some(entry) =
                    loon_core::resolve_path_from_materialized_tables_at_head_with_cache(
                        self.store(),
                        namespace_id,
                        &head.state,
                        absolute_path,
                        Some(&self.inner.metadata_table_cache),
                    )?
                {
                    span.record("cache_path", trace::CachePath::MaterializedTables.as_str());
                    self.inner.cache_stats.record_metadata_read_source(
                        loon_core::MetadataReadSource::MaterializedTables,
                    );
                    return Ok(entry);
                }
            }

            let basis = self.basis_for_read_at_head(namespace_id, &head)?;
            let entry = loon_core::resolve_path_from_basis(&basis, absolute_path)?;
            self.inner
                .cache_stats
                .record_metadata_read_source(loon_core::MetadataReadSource::FullBasisFallback);
            Ok(entry)
        })();
        span.record("result", trace::result_label(&result));
        result
    }

    pub fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        let head = self.head_for_metadata_read(namespace_id)?;
        if head.state.checkpoint_hint_seq.is_some() {
            if let Some(entries) = loon_core::list_path_from_materialized_tables_at_head_with_cache(
                self.store(),
                namespace_id,
                &head.state,
                absolute_path,
                Some(&self.inner.metadata_table_cache),
            )? {
                self.inner
                    .cache_stats
                    .record_metadata_read_source(loon_core::MetadataReadSource::MaterializedTables);
                return Ok(entries);
            }
        }

        let basis = self.basis_for_read_at_head(namespace_id, &head)?;
        let entries = loon_core::list_path_from_basis(&basis, absolute_path)?;
        self.inner
            .cache_stats
            .record_metadata_read_source(loon_core::MetadataReadSource::FullBasisFallback);
        Ok(entries)
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

    pub fn list_file_revisions(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::list_file_revisions_from_basis(
            basis.as_ref(),
            absolute_path,
        )?)
    }

    pub fn list_file_revisions_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::list_file_revisions_for_inode_from_basis(
            basis.as_ref(),
            inode_id,
        )?)
    }

    pub fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::read_file_revision_bytes_from_basis(
            self.store(),
            basis.as_ref(),
            absolute_path,
            revision_no,
        )?)
    }

    pub fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(loon_core::read_file_revision_bytes_for_inode_from_basis(
            self.store(),
            basis.as_ref(),
            inode_id,
            revision_no,
        )?)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
            result = tracing::field::Empty,
        )
    )]
    pub fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
        span.record("payload_class", trace::payload_class(bytes.len()));
        let store = self.uploaded_content_proof_store(namespace_id);
        let result = loon_core::put_file_bytes(
            &store,
            namespace_id,
            absolute_path,
            bytes,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        let result = self.finish_namespace_mutation(namespace_id, result);
        span.record("result", trace::result_label(&result));
        result
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
            result = tracing::field::Empty,
        )
    )]
    pub fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
        span.record(
            "payload_class",
            trace::payload_class(usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX)),
        );
        let store = self.uploaded_content_proof_store(namespace_id);
        let result = loon_core::put_file_content_ref(
            &store,
            namespace_id,
            absolute_path,
            content_ref,
            options.behavior,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        let result = self.finish_namespace_mutation(namespace_id, result);
        span.record("result", trace::result_label(&result));
        result
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

    pub fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<MutationResult> {
        let result = loon_core::restore_file_revision(
            self.store(),
            namespace_id,
            absolute_path,
            source_revision_no,
            &self.mutation_context(),
            options.commit_id.as_ref().map(CommitId::as_str),
        )
        .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub fn restore_file_revision_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        let commit_id = options.commit_id.unwrap_or_else(CommitId::generate);
        let request = CommitRequest {
            commit_id,
            preconditions: vec![CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no: base_revision_no,
            }],
            ops: vec![CommitOp::RestoreRevision {
                inode_id,
                source_revision_no,
                base_revision_no,
            }],
            message: None,
            annotations: None,
        };
        self.commit_operations(namespace_id, request)
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
        let store = self.uploaded_content_proof_store(namespace_id);
        Ok(loon_core::upload_content(
            &store,
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
        let store = self.uploaded_content_proof_store(namespace_id);
        if self.commit_engine_cache_enabled() {
            let engine = self.commit_engine(namespace_id);
            let publish = {
                let mut engine = engine.lock().expect("commit engine lock poisoned");
                engine.publish_batch(&store, candidates, &self.mutation_context())
            };
            self.inner.cache_stats.record_publish_result(&publish);
            if let Some(basis) = publish
                .verified_basis_cache_update
                .verified_basis_to_cache()
            {
                self.cache_basis(Arc::new(basis.clone()));
            } else if publish.verified_basis_cache_update.is_invalidated() {
                self.invalidate_namespace_cache(namespace_id);
            }
            return publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
        }

        let results: Vec<_> = loon_core::publish_namespace_mutations_batch(
            &store,
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

    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner
            .cache_stats
            .snapshot(self.inner.metadata_table_cache.stats())
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

    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            result = tracing::field::Empty,
        )
    )]
    pub fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        let span = tracing::Span::current();
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
        let result =
            loon_core::create_checkpoint(self.store(), namespace_id, &self.mutation_context())
                .map_err(RuntimeError::from);
        let result = self.finish_namespace_mutation(namespace_id, result);
        span.record("result", trace::result_label(&result));
        result
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

    fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CachedControl<HeadState>> {
        match self.load_namespace_head_cached(namespace_id) {
            Ok(head) => Ok(head),
            Err(
                ControlObjectLoadError::MissingObject { .. }
                | ControlObjectLoadError::MissingObjectAfterHead { .. },
            ) => {
                let basis = loon_core::load_verified_namespace_basis(self.store(), namespace_id)
                    .map_err(CoreError::from)?;
                let head = CachedControl {
                    identity: ControlObjectIdentity {
                        etag: basis.head_etag.clone(),
                    },
                    state: basis.head.clone(),
                };
                self.cache_basis(Arc::new(basis));
                Ok(head)
            }
            Err(error) => Err(RuntimeError::Core(CoreError::Basis(
                loon_core::BasisLoadError::LoadHead(error),
            ))),
        }
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
                    Ok(head) if basis.matches_head_etag(&head.identity.etag) => {
                        // A matching ETag only proves the durable head object is
                        // unchanged since this basis was reconstructed and
                        // verified; the cache itself is not authoritative.
                        tracing::Span::current()
                            .record("cache_path", trace::CachePath::WarmReuse.as_str());
                        return Ok(basis.basis_arc());
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            } else {
                match loon_core::probe_namespace_head_etag(self.store(), namespace_id) {
                    Ok(probe) if basis.matches_head_etag_probe(&probe) => {
                        // A matching ETag only proves the durable head object is
                        // unchanged since this basis was reconstructed and
                        // verified; the cache itself is not authoritative.
                        tracing::Span::current()
                            .record("cache_path", trace::CachePath::EtagProbe.as_str());
                        return Ok(basis.basis_arc());
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            }
        }

        let basis = loon_core::load_verified_namespace_basis(self.store(), namespace_id)
            .map_err(CoreError::from)?;
        tracing::Span::current().record("cache_path", trace::CachePath::ColdReconstruct.as_str());
        let basis = Arc::new(basis);
        self.cache_basis(Arc::clone(&basis));
        Ok(basis)
    }

    fn basis_for_read_at_head(
        &self,
        namespace_id: &NamespaceId,
        head: &CachedControl<HeadState>,
    ) -> Result<Arc<VerifiedNamespaceBasis>> {
        let cache_config = &self.inner.config.runtime_cache;
        if cache_config.basis_cache_enabled && cache_config.max_cached_namespaces > 0 {
            let cached = self
                .inner
                .basis_cache
                .lock()
                .expect("basis cache lock poisoned")
                .get(namespace_id);
            if let Some(basis) = cached {
                if basis.matches_head_etag(&head.identity.etag) {
                    tracing::Span::current()
                        .record("cache_path", trace::CachePath::WarmReuse.as_str());
                    return Ok(basis.basis_arc());
                }
                self.invalidate_namespace_cache(namespace_id);
            }
        }

        let basis = loon_core::load_verified_namespace_basis_at_head(
            self.store(),
            namespace_id,
            head.state.clone(),
            head.identity.etag.clone(),
        )
        .map_err(CoreError::from)?;
        tracing::Span::current().record("cache_path", trace::CachePath::ColdReconstruct.as_str());
        let basis = Arc::new(basis);
        self.cache_basis(Arc::clone(&basis));
        Ok(basis)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
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

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
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

    fn uploaded_content_proof_store<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
    ) -> UploadedContentProofStore<'a> {
        UploadedContentProofStore {
            inner: self.store(),
            namespace_id,
            proofs: &self.inner.uploaded_content_proofs,
        }
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

fn cached_head(loaded: LoadedHeadControl) -> CachedControl<HeadState> {
    CachedControl {
        identity: loaded.identity,
        state: loaded.state,
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

#[cfg(test)]
mod tests {
    use super::{
        ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, DisplayName, InodeId,
        NameKey, NamePolicy, RenameMode, RevisionNo,
    };

    #[test]
    fn explicit_commit_facade_exports_constructor_types() {
        let display_name = DisplayName::parse("Report.txt").expect("valid display name");
        let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
        let precondition =
            CommitPrecondition::binding_is(InodeId(1), name_key, InodeId(2), ChangeSeq(3), 4);

        let request = CommitRequest {
            commit_id: CommitId::generate(),
            preconditions: vec![precondition],
            ops: vec![
                CommitOp::RestoreRevision {
                    inode_id: InodeId(2),
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                },
                CommitOp::Rename {
                    inode_id: InodeId(2),
                    new_parent_inode: InodeId(1),
                    new_display_name: "report.txt".to_owned(),
                    mode: RenameMode::NoReplace,
                },
            ],
            message: None,
            annotations: None,
        };

        assert_eq!(request.preconditions.len(), 1);
        assert_eq!(request.ops.len(), 2);
    }
}
