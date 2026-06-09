//! Embedded LoonFS runtime.
//!
//! `loonfs` is the ergonomic runtime layer. It wraps `loon-core` with caching,
//! upload helpers, maintenance hooks, and optional object-store metrics. Use it
//! when you want LoonFS in-process, or when building the reference server.

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
    FilesystemOperationResponse, InodeId, InodeKind, ListFileRevisionsResponse, ManifestId,
    MutationResult, NameKey, NamePolicy, NamespaceId, NamespaceSummary, RevisionNo,
};
pub use loon_core::cache::MetadataTableCacheConfig;
use loon_core::cache::{
    load_namespace_head_summary, load_verified_namespace_basis,
    load_verified_namespace_basis_at_head, probe_namespace_head_etag, BasisLoadError,
    MetadataTableCache, MetadataTableCacheStats, NamespaceHeadEtagProbe, VerifiedNamespaceBasis,
    VerifiedNamespaceBasisWeight,
};
use loon_core::control::{
    load_namespace_head_control, ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl,
};
use loon_core::publish::{
    BasisReuseEvent, NamespaceCommitEngine, NamespaceCommitEnginePublishResult,
};
pub use loon_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
pub use loon_core::{
    BootstrapNamespaceError, Error, Error as CoreError, ErrorCode, ErrorKind, PutFileBehavior,
};
use loon_core::{MutationContext, NamespaceEngine};
use loon_objectstore::keys::namespace_head;
use loon_objectstore::metrics::InstrumentedObjectStore;
pub use loon_objectstore::metrics::{
    JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricSample, ObjectStoreMetricsRecorder,
    ObjectStoreOperation, ObjectStoreResultClass, PutModeClass, RangeClass,
};
use loon_objectstore::{ByteRange, ObjectBody, ObjectMetadata, PutMode};
pub use loon_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;
pub use trace::{payload_class, TraceMode, TraceStoreKind};

pub const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;
pub const DEFAULT_MAX_WAL_TAIL_SEGMENTS: u64 = 32;
pub const DEFAULT_MAX_CACHED_NAMESPACES: usize = 64;
pub const DEFAULT_MAX_CACHED_BASIS_ROWS: usize = 1_000_000;
pub const DEFAULT_MAX_CACHED_BASIS_DECODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_UPLOADED_CONTENT_PROOF_ENTRIES: usize = 16_384;
const UPLOADED_CONTENT_PROOF_TTL: Duration = Duration::from_secs(30);

/// Shared object-store handle used by the runtime.
pub type SharedObjectStore = Arc<dyn ObjectStore + Send + Sync>;
/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Error)]
/// Runtime error.
pub enum RuntimeError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Bootstrap(#[from] BootstrapNamespaceError),
    #[error("invalid runtime config: {0}")]
    Config(String),
    #[error("runtime task failed: {0}")]
    RuntimeTask(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Configuration for an embedded runtime instance.
pub struct FsConfig {
    /// Writer id used for namespace leases and commits.
    pub writer_id: String,
    /// Writer version reported in mutation context.
    pub writer_version: String,
    /// Lease duration used by write operations.
    pub lease_duration_ms: u64,
    /// Cache configuration.
    pub runtime_cache: RuntimeCacheConfig,
    /// Tracing mode label.
    pub trace_mode: TraceMode,
    /// Object-store kind label used by tracing.
    pub trace_store_kind: TraceStoreKind,
}

impl FsConfig {
    /// Builds a config with default runtime settings.
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
/// Cache configuration for the embedded runtime.
pub struct RuntimeCacheConfig {
    /// Enables verified-basis caching.
    pub basis_cache_enabled: bool,
    /// Enables namespace control-object caching.
    pub control_cache_enabled: bool,
    /// Maximum namespaces retained in runtime caches.
    pub max_cached_namespaces: usize,
    /// Maximum metadata rows retained across cached bases.
    pub max_cached_basis_rows: usize,
    /// Approximate decoded-byte budget for cached bases.
    pub max_cached_basis_decoded_bytes: Option<usize>,
    /// Cache settings for decoded metadata tables.
    pub metadata_table_cache: MetadataTableCacheConfig,
}

impl RuntimeCacheConfig {
    /// Disables runtime caches.
    pub fn disabled() -> Self {
        Self {
            basis_cache_enabled: false,
            control_cache_enabled: false,
            max_cached_namespaces: 0,
            max_cached_basis_rows: 0,
            max_cached_basis_decoded_bytes: Some(0),
            metadata_table_cache: MetadataTableCacheConfig {
                enabled: false,
                max_blocks: 0,
                max_decoded_bytes: Some(0),
            },
        }
    }
}

impl Default for RuntimeCacheConfig {
    fn default() -> Self {
        Self {
            basis_cache_enabled: true,
            control_cache_enabled: true,
            max_cached_namespaces: DEFAULT_MAX_CACHED_NAMESPACES,
            max_cached_basis_rows: DEFAULT_MAX_CACHED_BASIS_ROWS,
            max_cached_basis_decoded_bytes: Some(DEFAULT_MAX_CACHED_BASIS_DECODED_BYTES),
            metadata_table_cache: MetadataTableCacheConfig::default(),
        }
    }
}

#[derive(Clone)]
/// Embedded filesystem runtime.
///
/// `Fs` is cheap to clone. Clones share caches and the underlying object store.
pub struct Fs {
    inner: Arc<FsInner>,
}

struct FsInner {
    store: SharedObjectStore,
    config: FsConfig,
    basis_cache: Mutex<BasisCache>,
    commit_engines: Mutex<CommitEngineCache>,
    control_cache: Mutex<RuntimeControlCache>,
    metadata_table_cache: Arc<MetadataTableCache>,
    uploaded_content_proofs: Mutex<UploadedContentProofCache>,
    cache_stats: RuntimeCacheStatsInner,
}

/// Builder for [`Fs`].
pub struct FsBuilder {
    store: SharedObjectStore,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    runtime_cache: RuntimeCacheConfig,
    trace_mode: TraceMode,
    trace_store_kind: TraceStoreKind,
    metrics_recorder: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
}

#[derive(Debug, Default)]
struct BasisCache {
    entries: HashMap<NamespaceId, CachedVerifiedBasis>,
    order: VecDeque<NamespaceId>,
    cached_rows: usize,
    cached_decoded_bytes: usize,
}

#[derive(Debug, Clone)]
struct CachedVerifiedBasis {
    basis: Arc<VerifiedNamespaceBasis>,
    head_etag_reuse_token: String,
    weight: VerifiedNamespaceBasisWeight,
}

impl CachedVerifiedBasis {
    fn new(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            head_etag_reuse_token: basis.head_etag.clone(),
            weight: basis.weight(),
            basis,
        }
    }

    fn basis_arc(&self) -> Arc<VerifiedNamespaceBasis> {
        Arc::clone(&self.basis)
    }

    fn matches_head_etag(&self, head_etag: &str) -> bool {
        self.head_etag_reuse_token == head_etag
    }

    fn matches_head_etag_probe(&self, probe: &NamespaceHeadEtagProbe) -> bool {
        self.matches_head_etag(&probe.head_etag)
    }

    fn weight(&self) -> VerifiedNamespaceBasisWeight {
        self.weight
    }
}

#[derive(Debug, Clone, Copy)]
struct BasisCacheLimits {
    max_namespaces: usize,
    max_rows: usize,
    max_decoded_bytes: Option<usize>,
}

impl BasisCacheLimits {
    fn from_config(config: &RuntimeCacheConfig) -> Self {
        Self {
            max_namespaces: config.max_cached_namespaces,
            max_rows: config.max_cached_basis_rows,
            max_decoded_bytes: config.max_cached_basis_decoded_bytes,
        }
    }

    fn basis_cache_enabled(&self) -> bool {
        self.max_namespaces > 0
            && self.max_rows > 0
            && self.max_decoded_bytes.map(|max| max > 0).unwrap_or(true)
    }

    fn can_cache(&self, weight: VerifiedNamespaceBasisWeight) -> bool {
        self.basis_cache_enabled()
            && weight.rows <= self.max_rows
            && self
                .max_decoded_bytes
                .map(|max| weight.decoded_bytes <= max)
                .unwrap_or(true)
    }

    fn is_over_budget(&self, namespace_count: usize, rows: usize, decoded_bytes: usize) -> bool {
        namespace_count > self.max_namespaces
            || rows > self.max_rows
            || self
                .max_decoded_bytes
                .map(|max| decoded_bytes > max)
                .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BasisCacheEviction {
    count: usize,
    rows: usize,
    decoded_bytes: usize,
}

impl BasisCacheEviction {
    fn add_weight(&mut self, weight: VerifiedNamespaceBasisWeight) {
        self.count = self.count.saturating_add(1);
        self.rows = self.rows.saturating_add(weight.rows);
        self.decoded_bytes = self.decoded_bytes.saturating_add(weight.decoded_bytes);
    }

    fn merge(&mut self, other: Self) {
        self.count = self.count.saturating_add(other.count);
        self.rows = self.rows.saturating_add(other.rows);
        self.decoded_bytes = self.decoded_bytes.saturating_add(other.decoded_bytes);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BasisCacheUpdate {
    eviction: BasisCacheEviction,
}

impl BasisCache {
    fn get(&mut self, namespace_id: &NamespaceId) -> Option<CachedVerifiedBasis> {
        let basis = self.entries.get(namespace_id).cloned()?;
        self.touch(namespace_id);
        Some(basis)
    }

    fn insert(
        &mut self,
        basis: Arc<VerifiedNamespaceBasis>,
        limits: BasisCacheLimits,
    ) -> BasisCacheUpdate {
        let namespace_id = basis.head.namespace_id.clone();
        let mut eviction = self.remove_entry(&namespace_id);
        let cached = CachedVerifiedBasis::new(basis);
        debug_assert!(limits.can_cache(cached.weight()));
        self.cached_rows = self.cached_rows.saturating_add(cached.weight().rows);
        self.cached_decoded_bytes = self
            .cached_decoded_bytes
            .saturating_add(cached.weight().decoded_bytes);
        self.entries.insert(namespace_id.clone(), cached);
        self.touch(&namespace_id);
        while limits.is_over_budget(
            self.entries.len(),
            self.cached_rows,
            self.cached_decoded_bytes,
        ) {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            eviction.merge(self.remove_entry(&evicted));
        }
        self.update_with_eviction(eviction)
    }

    fn invalidate(&mut self, namespace_id: &NamespaceId) -> BasisCacheUpdate {
        self.order.retain(|candidate| candidate != namespace_id);
        let eviction = self.remove_entry(namespace_id);
        self.update_with_eviction(eviction)
    }

    fn touch(&mut self, namespace_id: &NamespaceId) {
        self.order.retain(|candidate| candidate != namespace_id);
        self.order.push_back(namespace_id.clone());
    }

    fn remove_entry(&mut self, namespace_id: &NamespaceId) -> BasisCacheEviction {
        let Some(entry) = self.entries.remove(namespace_id) else {
            return BasisCacheEviction::default();
        };
        self.order.retain(|candidate| candidate != namespace_id);
        let weight = entry.weight();
        self.cached_rows = self.cached_rows.saturating_sub(weight.rows);
        self.cached_decoded_bytes = self
            .cached_decoded_bytes
            .saturating_sub(weight.decoded_bytes);
        let mut eviction = BasisCacheEviction::default();
        eviction.add_weight(weight);
        eviction
    }

    fn update_with_eviction(&self, eviction: BasisCacheEviction) -> BasisCacheUpdate {
        BasisCacheUpdate { eviction }
    }

    fn cached_totals(&self) -> (usize, usize, usize) {
        (
            self.entries.len(),
            self.cached_rows,
            self.cached_decoded_bytes,
        )
    }

    fn prune_one_lru(&mut self) -> BasisCacheEviction {
        let Some(evicted) = self.order.pop_front() else {
            return BasisCacheEviction::default();
        };
        self.remove_entry(&evicted)
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
            expires_at: wall_clock_now() + UPLOADED_CONTENT_PROOF_TTL,
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
        if wall_clock_now() > proof.expires_at {
            self.entries.remove(&key);
            self.order.retain(|existing| existing != &key);
            return None;
        }
        Some(proof.content_ref.clone())
    }
}

#[cfg(test)]
mod proof_cache_tests {
    use super::{wall_clock_now, UploadedContentProofCache, UploadedContentProofKey};
    use loon_api::{ContentRef, NamespaceId};
    use std::time::Duration;

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
            .expires_at = wall_clock_now() - Duration::from_secs(1);

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
            version: None,
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

    fn get_with_metadata(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key)
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
            self.remove_entry(&evicted);
        }
        engine
    }

    fn invalidate(&mut self, namespace_id: &NamespaceId) -> BasisCacheEviction {
        let eviction = self.remove_entry(namespace_id);
        self.order.retain(|candidate| candidate != namespace_id);
        eviction
    }

    fn cached_basis_totals(&self) -> (usize, usize, usize) {
        self.entries
            .values()
            .filter_map(|engine| {
                engine
                    .lock()
                    .expect("commit engine lock poisoned")
                    .cached_basis_weight()
            })
            .fold(
                (0_usize, 0_usize, 0_usize),
                |(count, rows, decoded_bytes), weight| {
                    (
                        count.saturating_add(1),
                        rows.saturating_add(weight.rows),
                        decoded_bytes.saturating_add(weight.decoded_bytes),
                    )
                },
            )
    }

    fn prune_one_lru(&mut self) -> BasisCacheEviction {
        while let Some(evicted) = self.order.pop_front() {
            let eviction = self.remove_entry(&evicted);
            if eviction.count > 0 {
                return eviction;
            }
        }
        BasisCacheEviction::default()
    }

    fn remove_entry(&mut self, namespace_id: &NamespaceId) -> BasisCacheEviction {
        let Some(engine) = self.entries.remove(namespace_id) else {
            return BasisCacheEviction::default();
        };
        let mut engine = engine.lock().expect("commit engine lock poisoned");
        let mut eviction = BasisCacheEviction::default();
        if let Some(weight) = engine.cached_basis_weight() {
            eviction.add_weight(weight);
        }
        engine.invalidate();
        eviction
    }

    fn touch(&mut self, namespace_id: &NamespaceId) {
        self.order.retain(|candidate| candidate != namespace_id);
        self.order.push_back(namespace_id.clone());
    }
}

fn combined_cached_basis_totals(
    basis_cache: &BasisCache,
    commit_engines: &CommitEngineCache,
) -> (usize, usize, usize) {
    let (basis_count, basis_rows, basis_decoded_bytes) = basis_cache.cached_totals();
    let (engine_count, engine_rows, engine_decoded_bytes) = commit_engines.cached_basis_totals();
    (
        basis_count.saturating_add(engine_count),
        basis_rows.saturating_add(engine_rows),
        basis_decoded_bytes.saturating_add(engine_decoded_bytes),
    )
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
/// Snapshot of runtime cache counters.
///
/// These counters are diagnostic. They are useful for tuning cache limits and
/// understanding read/write warmup behavior.
pub struct RuntimeCacheStats {
    pub async_engine_calls: usize,
    pub warm_basis_cache_hits: usize,
    pub warm_basis_cache_misses: usize,
    pub warm_basis_evictions: usize,
    pub warm_basis_evicted_rows: usize,
    pub warm_basis_evicted_decoded_bytes: usize,
    pub warm_basis_uncacheable_count: usize,
    pub warm_basis_uncacheable_rows: usize,
    pub warm_basis_uncacheable_decoded_bytes: usize,
    pub warm_basis_cached_rows: usize,
    pub warm_basis_cached_decoded_bytes: usize,
    pub warm_basis_rehydrate_count: usize,
    pub warm_basis_rehydrate_ms: usize,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataReadSource {
    MaterializedTables,
    FullBasisFallback,
}

#[derive(Debug, Default)]
struct RuntimeCacheStatsInner {
    async_engine_calls: AtomicUsize,
    warm_basis_cache_hits: AtomicUsize,
    warm_basis_cache_misses: AtomicUsize,
    warm_basis_evictions: AtomicUsize,
    warm_basis_evicted_rows: AtomicUsize,
    warm_basis_evicted_decoded_bytes: AtomicUsize,
    warm_basis_uncacheable_count: AtomicUsize,
    warm_basis_uncacheable_rows: AtomicUsize,
    warm_basis_uncacheable_decoded_bytes: AtomicUsize,
    warm_basis_cached_rows: AtomicUsize,
    warm_basis_cached_decoded_bytes: AtomicUsize,
    warm_basis_rehydrate_count: AtomicUsize,
    warm_basis_rehydrate_ms: AtomicUsize,
    publish_warm_basis_hits: AtomicUsize,
    publish_warm_basis_misses: AtomicUsize,
    publish_warm_basis_invalidations: AtomicUsize,
    publish_warm_basis_advances: AtomicUsize,
    read_materialized_table_hits: AtomicUsize,
    read_full_basis_fallbacks: AtomicUsize,
}

impl RuntimeCacheStatsInner {
    fn snapshot(&self, metadata_table_cache: MetadataTableCacheStats) -> RuntimeCacheStats {
        RuntimeCacheStats {
            async_engine_calls: self.async_engine_calls.load(Ordering::SeqCst),
            warm_basis_cache_hits: self.warm_basis_cache_hits.load(Ordering::SeqCst),
            warm_basis_cache_misses: self.warm_basis_cache_misses.load(Ordering::SeqCst),
            warm_basis_evictions: self.warm_basis_evictions.load(Ordering::SeqCst),
            warm_basis_evicted_rows: self.warm_basis_evicted_rows.load(Ordering::SeqCst),
            warm_basis_evicted_decoded_bytes: self
                .warm_basis_evicted_decoded_bytes
                .load(Ordering::SeqCst),
            warm_basis_uncacheable_count: self.warm_basis_uncacheable_count.load(Ordering::SeqCst),
            warm_basis_uncacheable_rows: self.warm_basis_uncacheable_rows.load(Ordering::SeqCst),
            warm_basis_uncacheable_decoded_bytes: self
                .warm_basis_uncacheable_decoded_bytes
                .load(Ordering::SeqCst),
            warm_basis_cached_rows: self.warm_basis_cached_rows.load(Ordering::SeqCst),
            warm_basis_cached_decoded_bytes: self
                .warm_basis_cached_decoded_bytes
                .load(Ordering::SeqCst),
            warm_basis_rehydrate_count: self.warm_basis_rehydrate_count.load(Ordering::SeqCst),
            warm_basis_rehydrate_ms: self.warm_basis_rehydrate_ms.load(Ordering::SeqCst),
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

    fn record_async_engine_call(&self) {
        self.async_engine_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn record_publish_result(&self, result: &NamespaceCommitEnginePublishResult) {
        match result.basis_reuse_event {
            BasisReuseEvent::ReusedAfterHeadEtagMatch => {
                self.warm_basis_cache_hits.fetch_add(1, Ordering::SeqCst);
                self.publish_warm_basis_hits.fetch_add(1, Ordering::SeqCst);
            }
            BasisReuseEvent::ColdLoaded | BasisReuseEvent::InvalidatedThenColdLoaded => {
                self.warm_basis_cache_misses.fetch_add(1, Ordering::SeqCst);
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

    fn record_warm_basis_hit(&self) {
        self.warm_basis_cache_hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_warm_basis_miss(&self) {
        self.warm_basis_cache_misses.fetch_add(1, Ordering::SeqCst);
    }

    fn record_warm_basis_rehydrate(&self, elapsed_ms: usize) {
        self.warm_basis_rehydrate_count
            .fetch_add(1, Ordering::SeqCst);
        self.warm_basis_rehydrate_ms
            .fetch_add(elapsed_ms, Ordering::SeqCst);
    }

    fn record_warm_basis_cache_update(&self, update: BasisCacheUpdate) {
        self.record_warm_basis_eviction(update.eviction);
    }

    fn record_warm_basis_uncacheable(&self, weight: VerifiedNamespaceBasisWeight) {
        self.warm_basis_uncacheable_count
            .fetch_add(1, Ordering::SeqCst);
        self.warm_basis_uncacheable_rows
            .fetch_add(weight.rows, Ordering::SeqCst);
        self.warm_basis_uncacheable_decoded_bytes
            .fetch_add(weight.decoded_bytes, Ordering::SeqCst);
    }

    fn set_warm_basis_cached_weight(&self, rows: usize, decoded_bytes: usize) {
        self.warm_basis_cached_rows.store(rows, Ordering::SeqCst);
        self.warm_basis_cached_decoded_bytes
            .store(decoded_bytes, Ordering::SeqCst);
    }

    fn record_warm_basis_eviction(&self, eviction: BasisCacheEviction) {
        if eviction.count == 0 {
            return;
        }
        self.warm_basis_evictions
            .fetch_add(eviction.count, Ordering::SeqCst);
        self.warm_basis_evicted_rows
            .fetch_add(eviction.rows, Ordering::SeqCst);
        self.warm_basis_evicted_decoded_bytes
            .fetch_add(eviction.decoded_bytes, Ordering::SeqCst);
    }

    fn record_metadata_read_source(&self, source: MetadataReadSource) {
        match source {
            MetadataReadSource::MaterializedTables => {
                self.read_materialized_table_hits
                    .fetch_add(1, Ordering::SeqCst);
            }
            MetadataReadSource::FullBasisFallback => {
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
/// Current maintenance-related namespace status.
pub struct NamespaceStatus {
    /// Namespace being inspected.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Current manifest pointer recorded by the head.
    pub current_manifest_id: Option<ManifestId>,
    /// Latest checkpoint recorded by the head.
    pub latest_checkpoint_id: Option<String>,
    /// Number of visible WAL segments after the manifest basis.
    pub wal_tail_segments: u64,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Options for one maintenance tick.
pub struct MaintenanceTickOptions {
    /// Publish a checkpoint when the visible WAL tail reaches this many segments.
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
/// Outcome of one maintenance tick.
pub enum MaintenanceTickOutcome {
    /// No checkpoint was needed.
    NotNeeded,
    /// A checkpoint was published.
    CheckpointPublished { checkpoint_seq: ChangeSeq },
    /// Another checkpoint already covered the attempted sequence.
    CheckpointSuperseded {
        attempted_seq: ChangeSeq,
        current_manifest_id: ManifestId,
    },
    /// A concurrent head update won the race.
    CheckpointPublishRaceLost { observed_head_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of one maintenance tick.
pub struct MaintenanceTickResult {
    pub namespace_id: NamespaceId,
    pub status_before: NamespaceStatus,
    pub outcome: MaintenanceTickOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Options for creating a namespace.
pub struct CreateNamespaceOptions {
    /// Treat an existing namespace as success.
    pub allow_existing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Options for writing a file path.
pub struct PutFileOptions {
    /// Create-only or replace-existing behavior.
    pub behavior: PutFileBehavior,
    /// Optional idempotency key.
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
/// Options for creating a directory.
pub struct CreateDirOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Options for deleting a path.
pub struct DeleteOptions {
    /// Whether directory deletes may remove a subtree.
    pub recursive: bool,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Options for moving a path.
pub struct MoveOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Options for copying a file path.
pub struct CopyOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Options for restoring a file revision by path.
pub struct RestoreRevisionOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
}

impl FsBuilder {
    /// Starts an embedded runtime builder.
    pub fn new(store: SharedObjectStore) -> Self {
        Self {
            store,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Embedded,
            trace_store_kind: TraceStoreKind::Unknown,
            metrics_recorder: None,
        }
    }

    /// Sets the writer id used by namespace mutations.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the writer version used in mutation context.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Sets the lease duration for write operations.
    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.runtime_cache = runtime_cache;
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.trace_store_kind = trace_store_kind;
        self
    }

    /// Install object-store metrics collection for this runtime.
    ///
    /// The runtime wraps the provided object store before opening `Fs`; callers do not need to
    /// manually construct an instrumented store.
    pub fn with_metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the runtime.
    pub fn build(self) -> Result<Fs> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let trace_store_kind = self.trace_store_kind;
        let store = match self.metrics_recorder {
            Some(recorder) => Arc::new(
                InstrumentedObjectStore::new(self.store, recorder)
                    .with_store_kind(trace_store_kind.as_str()),
            ) as SharedObjectStore,
            None => self.store,
        };
        Fs::open(
            store,
            FsConfig {
                writer_id,
                writer_version: self.writer_version,
                lease_duration_ms: self.lease_duration_ms,
                runtime_cache: self.runtime_cache,
                trace_mode: self.trace_mode,
                trace_store_kind,
            },
        )
    }
}

impl Fs {
    /// Opens an embedded runtime from an object store and config.
    pub fn open(store: SharedObjectStore, config: FsConfig) -> Result<Self> {
        validate_config(&config)?;
        let metadata_table_cache = Arc::new(MetadataTableCache::new(
            config.runtime_cache.metadata_table_cache.clone(),
        ));
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

    /// Starts a runtime builder.
    pub fn builder(store: SharedObjectStore) -> FsBuilder {
        FsBuilder::new(store)
    }

    /// Returns this runtime's config.
    pub fn config(&self) -> &FsConfig {
        &self.inner.config
    }

    fn record_trace_context(&self, span: &tracing::Span) {
        span.record("mode", self.inner.config.trace_mode.as_str());
        span.record("store_kind", self.inner.config.trace_store_kind.as_str());
    }

    async fn run_engine_blocking<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Fs) -> Result<T> + Send + 'static,
    {
        self.inner.cache_stats.record_async_engine_call();
        let fs = self.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            operation(fs)
        })
        .await
        .map_err(|error| RuntimeError::RuntimeTask(error.to_string()))?
    }

    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.create_namespace_blocking(&namespace_id, options))
            .await
    }

    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(namespace_id)
            .bootstrap_namespace(loon_core::BootstrapOptions {
                allow_existing: options.allow_existing,
            })
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let source = source.clone();
        let target = target.clone();
        self.run_engine_blocking(move |fs| fs.fork_namespace_blocking(&source, &target))
            .await
    }

    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let result = self
            .namespace_engine(source)
            .fork_namespace(target, loon_core::ForkOptions::default())
            .map_err(RuntimeError::from);
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(source);
        }
        if result.is_ok() {
            self.invalidate_namespace_cache(target);
        }
        result
    }

    pub async fn list_namespaces(&self) -> Result<Vec<NamespaceSummary>> {
        self.run_engine_blocking(|fs| fs.list_namespaces_blocking())
            .await
    }

    fn list_namespaces_blocking(&self) -> Result<Vec<NamespaceSummary>> {
        Ok(loon_core::list_namespaces(self.store())?)
    }

    pub async fn namespace_status(&self, namespace_id: &NamespaceId) -> Result<NamespaceStatus> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.namespace_status_blocking(&namespace_id))
            .await
    }

    fn namespace_status_blocking(&self, namespace_id: &NamespaceId) -> Result<NamespaceStatus> {
        let summary = load_namespace_head_summary(self.store(), namespace_id)?;
        Ok(NamespaceStatus {
            namespace_id: summary.namespace_id,
            head_seq: summary.head_seq,
            current_manifest_id: summary.current_manifest_id,
            latest_checkpoint_id: summary.latest_checkpoint_id,
            wal_tail_segments: summary.wal_tail_segments,
            retention_floor_seq: summary.retention_floor_seq,
        })
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn maintenance_tick_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| {
            fs.maintenance_tick_namespace_blocking(&namespace_id, options)
        })
        .await
    }

    fn maintenance_tick_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> Result<MaintenanceTickResult> {
        if options.max_wal_tail_segments == 0 {
            return Err(RuntimeError::Config(
                "max_wal_tail_segments must be greater than zero".to_owned(),
            ));
        }

        let status_before = self.namespace_status_blocking(namespace_id)?;
        let observed_head_seq = status_before.head_seq;
        if status_before.wal_tail_segments < options.max_wal_tail_segments {
            return Ok(MaintenanceTickResult {
                namespace_id: namespace_id.clone(),
                status_before,
                outcome: MaintenanceTickOutcome::NotNeeded,
            });
        }

        let checkpoint = match self.create_checkpoint_blocking(namespace_id) {
            Ok(checkpoint) => checkpoint,
            Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => {
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

        let outcome = if checkpoint.current_manifest_id == Some(checkpoint.manifest_id) {
            MaintenanceTickOutcome::CheckpointPublished {
                checkpoint_seq: checkpoint.checkpoint_seq,
            }
        } else {
            let Some(current_manifest_id) = checkpoint.current_manifest_id else {
                return Err(RuntimeError::Core(CoreError::Store(
                    "checkpoint publication returned no current manifest id".to_owned(),
                )));
            };
            MaintenanceTickOutcome::CheckpointSuperseded {
                attempted_seq: checkpoint.checkpoint_seq,
                current_manifest_id,
            }
        };

        Ok(MaintenanceTickResult {
            namespace_id: namespace_id.clone(),
            status_before,
            outcome,
        })
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.stat",
        err,
        skip_all,
        fields(
            operation = "stat",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            cache_path = tracing::field::Empty,
        )
    )]
    pub async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| fs.stat_path_blocking(&namespace_id, &absolute_path))
            .await
    }

    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry> {
        let head = self.head_for_metadata_read(namespace_id)?;
        let engine = self.namespace_engine(namespace_id);
        if head.state.current_manifest_id.is_some() {
            let entry = engine.resolve_path(
                absolute_path,
                loon_core::ReadOptions::materialized_tables_at_head(
                    head.state.clone(),
                    Some(Arc::clone(&self.inner.metadata_table_cache)),
                ),
            )?;
            tracing::Span::current()
                .record("cache_path", trace::CachePath::MaterializedTables.as_str());
            self.inner
                .cache_stats
                .record_metadata_read_source(MetadataReadSource::MaterializedTables);
            return Ok(entry);
        }

        let basis = self.basis_for_read_at_head(namespace_id, &head)?;
        let entry = engine.resolve_path(
            absolute_path,
            loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
        )?;
        self.inner
            .cache_stats
            .record_metadata_read_source(MetadataReadSource::FullBasisFallback);
        Ok(entry)
    }

    pub async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| fs.list_path_blocking(&namespace_id, &absolute_path))
            .await
    }

    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<Vec<AuthoritativePathEntry>> {
        let head = self.head_for_metadata_read(namespace_id)?;
        let engine = self.namespace_engine(namespace_id);
        if head.state.current_manifest_id.is_some() {
            let entries = engine.list_path(
                absolute_path,
                loon_core::ReadOptions::materialized_tables_at_head(
                    head.state.clone(),
                    Some(Arc::clone(&self.inner.metadata_table_cache)),
                ),
            )?;
            self.inner
                .cache_stats
                .record_metadata_read_source(MetadataReadSource::MaterializedTables);
            return Ok(entries);
        }

        let basis = self.basis_for_read_at_head(namespace_id, &head)?;
        let entries = engine.list_path(
            absolute_path,
            loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
        )?;
        self.inner
            .cache_stats
            .record_metadata_read_source(MetadataReadSource::FullBasisFallback);
        Ok(entries)
    }

    pub async fn read_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.read_file_bytes_blocking(&namespace_id, &absolute_path)
        })
        .await
    }

    fn read_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(self.namespace_engine(namespace_id).read_file(
            absolute_path,
            loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
        )?)
    }

    pub async fn list_file_revisions(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.list_file_revisions_blocking(&namespace_id, &absolute_path)
        })
        .await
    }

    fn list_file_revisions_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(self.namespace_engine(namespace_id).list_file_revisions(
            absolute_path,
            loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
        )?)
    }

    pub async fn list_file_revisions_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| {
            fs.list_file_revisions_for_inode_blocking(&namespace_id, inode_id)
        })
        .await
    }

    fn list_file_revisions_for_inode_blocking(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<ListFileRevisionsResponse> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(self
            .namespace_engine(namespace_id)
            .list_file_revisions_for_inode(
                inode_id,
                loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )?)
    }

    pub async fn read_file_revision_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.read_file_revision_bytes_blocking(&namespace_id, &absolute_path, revision_no)
        })
        .await
    }

    fn read_file_revision_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        revision_no: RevisionNo,
    ) -> Result<AuthoritativeFileBytes> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(self.namespace_engine(namespace_id).read_file_revision(
            absolute_path,
            revision_no,
            loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
        )?)
    }

    pub async fn read_file_revision_bytes_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| {
            fs.read_file_revision_bytes_for_inode_blocking(&namespace_id, inode_id, revision_no)
        })
        .await
    }

    fn read_file_revision_bytes_for_inode_blocking(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let basis = self.basis_for_read(namespace_id)?;
        Ok(self
            .namespace_engine(namespace_id)
            .read_file_revision_for_inode(
                inode_id,
                revision_no,
                loon_core::ReadOptions::verified_basis(Arc::clone(&basis)),
            )?)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record("payload_class", trace::payload_class(bytes.len()));
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        let bytes = bytes.to_vec();
        self.run_engine_blocking(move |fs| {
            fs.put_file_bytes_blocking(&namespace_id, &absolute_path, &bytes, options)
        })
        .await
    }

    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let store = self.uploaded_content_proof_store(namespace_id);
        let result = self
            .namespace_engine_with_store(namespace_id, store)
            .put_file(
                absolute_path,
                bytes,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    put_file_behavior: options.behavior,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.put",
        err,
        skip_all,
        fields(
            operation = "put",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
            payload_class = tracing::field::Empty,
        )
    )]
    pub async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        span.record(
            "payload_class",
            trace::payload_class(usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX)),
        );
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.put_file_content_ref_blocking(&namespace_id, &absolute_path, content_ref, options)
        })
        .await
    }

    fn put_file_content_ref_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        let store = self.uploaded_content_proof_store(namespace_id);
        let result = self
            .namespace_engine_with_store(namespace_id, store)
            .put_file_content_ref(
                absolute_path,
                content_ref,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    put_file_behavior: options.behavior,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn create_dir(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> Result<MutationResult> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.create_dir_blocking(&namespace_id, &absolute_path, options)
        })
        .await
    }

    fn create_dir_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> Result<MutationResult> {
        let result = self
            .namespace_engine(namespace_id)
            .create_dir(
                absolute_path,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.delete_path_blocking(&namespace_id, &absolute_path, options)
        })
        .await
    }

    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        let result = self
            .namespace_engine(namespace_id)
            .delete_path(
                absolute_path,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    recursive_delete: options.recursive,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        let namespace_id = namespace_id.clone();
        let from_path = from_path.to_owned();
        let to_path = to_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.move_path_blocking(&namespace_id, &from_path, &to_path, options)
        })
        .await
    }

    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        let result = self
            .namespace_engine(namespace_id)
            .move_path(
                from_path,
                to_path,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        let namespace_id = namespace_id.clone();
        let from_path = from_path.to_owned();
        let to_path = to_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.copy_path_blocking(&namespace_id, &from_path, &to_path, options)
        })
        .await
    }

    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        let result = self
            .namespace_engine(namespace_id)
            .copy_path(
                from_path,
                to_path,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<MutationResult> {
        let namespace_id = namespace_id.clone();
        let absolute_path = absolute_path.to_owned();
        self.run_engine_blocking(move |fs| {
            fs.restore_file_revision_blocking(
                &namespace_id,
                &absolute_path,
                source_revision_no,
                options,
            )
        })
        .await
    }

    fn restore_file_revision_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<MutationResult> {
        let result = self
            .namespace_engine(namespace_id)
            .restore_file_revision(
                absolute_path,
                source_revision_no,
                loon_core::WriteOptions {
                    commit_id: options.commit_id,
                    ..loon_core::WriteOptions::default()
                },
            )
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn restore_file_revision_for_inode(
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
        self.commit_operations(namespace_id, request).await
    }

    pub async fn begin_upload(&self, namespace_id: &NamespaceId) -> Result<BeginUploadResponse> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.begin_upload_blocking(&namespace_id))
            .await
    }

    fn begin_upload_blocking(&self, namespace_id: &NamespaceId) -> Result<BeginUploadResponse> {
        Ok(self.namespace_engine(namespace_id).begin_upload()?)
    }

    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        let namespace_id = namespace_id.clone();
        let upload_id = upload_id.to_owned();
        let bytes = bytes.to_vec();
        self.run_engine_blocking(move |fs| {
            fs.upload_content_blocking(&namespace_id, &upload_id, &bytes)
        })
        .await
    }

    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        let store = self.uploaded_content_proof_store(namespace_id);
        Ok(self
            .namespace_engine_with_store(namespace_id, store)
            .upload_content(upload_id, bytes)?)
    }

    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        let namespace_id = namespace_id.clone();
        let upload_id = upload_id.to_owned();
        let request = request.clone();
        self.run_engine_blocking(move |fs| {
            fs.complete_upload_blocking(&namespace_id, &upload_id, &request)
        })
        .await
    }

    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &str,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .complete_upload(upload_id, request)?)
    }

    pub async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.publish_namespace_mutations_batch(
            namespace_id,
            vec![NamespaceMutationCandidate::Commit(request)],
        )
        .await
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Err(RuntimeError::Core(CoreError::Store(
                "empty commit batch".to_owned(),
            )))
        })
    }

    pub async fn commit_operations_batch(
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
        .await
    }

    pub async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let result_count = candidates.len();
        let namespace_id = namespace_id.clone();
        match self
            .run_engine_blocking(move |fs| {
                Ok(fs.publish_namespace_mutations_batch_blocking(&namespace_id, candidates))
            })
            .await
        {
            Ok(results) => results,
            Err(error) => repeat_runtime_error(result_count, error),
        }
    }

    fn publish_namespace_mutations_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        let store = self.uploaded_content_proof_store(namespace_id);
        if self.commit_engine_cache_enabled() {
            let engine = self.commit_engine(namespace_id);
            let publish = {
                let mut engine = engine.lock().expect("commit engine lock poisoned");
                engine.publish_batch(&store, candidates, &self.mutation_context())
            };
            {
                let _span = tracing::info_span!(
                    "publisher.batch_update_cache",
                    phase = "batch_update_cache",
                    mode = self.inner.config.trace_mode.as_str(),
                    store_kind = self.inner.config.trace_store_kind.as_str(),
                    batch_size
                )
                .entered();
                self.inner.cache_stats.record_publish_result(&publish);
                if let Some(basis) = publish
                    .verified_basis_cache_update
                    .verified_basis_to_cache()
                {
                    self.cache_basis(basis);
                } else if publish.verified_basis_cache_update.is_invalidated() {
                    self.invalidate_namespace_cache(namespace_id);
                }
            }
            return publish
                .results
                .into_iter()
                .map(|result| result.map_err(RuntimeError::Core))
                .collect();
        }

        let results: Vec<_> = self
            .namespace_engine_with_store(namespace_id, store)
            .publish_namespace_mutations_batch(candidates)
            .into_iter()
            .map(|result| result.map_err(RuntimeError::Core))
            .collect();
        {
            let _span = tracing::info_span!(
                "publisher.batch_update_cache",
                phase = "batch_update_cache",
                mode = self.inner.config.trace_mode.as_str(),
                store_kind = self.inner.config.trace_store_kind.as_str(),
                batch_size
            )
            .entered();
            self.invalidate_namespace_cache_after_batch(namespace_id, &results);
        }
        results
    }

    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.inner
            .cache_stats
            .snapshot(self.inner.metadata_table_cache.stats())
    }

    pub async fn list_changes_after(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.list_changes_after_blocking(&namespace_id, after_seq))
            .await
    }

    fn list_changes_after_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> Result<ChangesResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .list_changes_after(after_seq)?)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.compaction",
        err,
        skip_all,
        fields(
            operation = "compaction",
            mode = tracing::field::Empty,
            store_kind = tracing::field::Empty,
        )
    )]
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        let span = tracing::Span::current();
        self.record_trace_context(&span);
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.create_checkpoint_blocking(&namespace_id))
            .await
    }

    fn create_checkpoint_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CreateCheckpointResponse> {
        let result = self
            .namespace_engine(namespace_id)
            .create_checkpoint()
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let namespace_id = namespace_id.clone();
        self.run_engine_blocking(move |fs| fs.advance_retention_floor_blocking(&namespace_id))
            .await
    }

    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        let result = self
            .namespace_engine(namespace_id)
            .advance_retention_floor()
            .map_err(RuntimeError::from);
        self.finish_namespace_mutation(namespace_id, result)
    }

    fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedControl<HeadState>, ControlObjectLoadError> {
        let cache_config = &self.inner.config.runtime_cache;
        if !self.control_cache_enabled() {
            return load_namespace_head_control(self.store(), namespace_id).map(cached_head);
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

        let loaded = match load_namespace_head_control(self.store(), namespace_id) {
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
                self.inner.cache_stats.record_warm_basis_miss();
                let started = monotonic_now();
                let basis = load_verified_namespace_basis(self.store(), namespace_id)
                    .map_err(CoreError::from)?;
                self.inner
                    .cache_stats
                    .record_warm_basis_rehydrate(elapsed_ms_usize(started));
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
                BasisLoadError::LoadHead(error),
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
        self.basis_cache_enabled()
    }

    fn basis_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.basis_cache_enabled
            && BasisCacheLimits::from_config(cache_config).basis_cache_enabled()
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
        if !self.basis_cache_enabled() {
            self.inner.cache_stats.record_warm_basis_miss();
            let started = monotonic_now();
            let basis = load_verified_namespace_basis(self.store(), namespace_id)
                .map_err(CoreError::from)?;
            self.inner
                .cache_stats
                .record_warm_basis_rehydrate(elapsed_ms_usize(started));
            return Ok(Arc::new(basis));
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
                        self.inner.cache_stats.record_warm_basis_hit();
                        return Ok(basis.basis_arc());
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            } else {
                match probe_namespace_head_etag(self.store(), namespace_id) {
                    Ok(probe) if basis.matches_head_etag_probe(&probe) => {
                        // A matching ETag only proves the durable head object is
                        // unchanged since this basis was reconstructed and
                        // verified; the cache itself is not authoritative.
                        tracing::Span::current()
                            .record("cache_path", trace::CachePath::EtagProbe.as_str());
                        self.inner.cache_stats.record_warm_basis_hit();
                        return Ok(basis.basis_arc());
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            }
        }

        self.inner.cache_stats.record_warm_basis_miss();
        let started = monotonic_now();
        let basis =
            load_verified_namespace_basis(self.store(), namespace_id).map_err(CoreError::from)?;
        self.inner
            .cache_stats
            .record_warm_basis_rehydrate(elapsed_ms_usize(started));
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
        if self.basis_cache_enabled() {
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
                    self.inner.cache_stats.record_warm_basis_hit();
                    return Ok(basis.basis_arc());
                }
                self.invalidate_namespace_cache(namespace_id);
            }
        }

        self.inner.cache_stats.record_warm_basis_miss();
        let started = monotonic_now();
        let basis = load_verified_namespace_basis_at_head(
            self.store(),
            namespace_id,
            head.state.clone(),
            head.identity.etag.clone(),
        )
        .map_err(CoreError::from)?;
        self.inner
            .cache_stats
            .record_warm_basis_rehydrate(elapsed_ms_usize(started));
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
        if !self.basis_cache_enabled() {
            return;
        }
        let limits = BasisCacheLimits::from_config(cache_config);
        let weight = basis.weight();
        if !limits.can_cache(weight) {
            self.inner.cache_stats.record_warm_basis_uncacheable(weight);
            tracing::debug!(
                namespace_id = %basis.head.namespace_id,
                rows = weight.rows,
                decoded_bytes = weight.decoded_bytes,
                max_rows = limits.max_rows,
                max_decoded_bytes = ?limits.max_decoded_bytes,
                "warm basis exceeds cache limits"
            );
            self.prune_warm_basis_budget();
            return;
        }
        let update = self
            .inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .insert(basis, limits);
        self.inner
            .cache_stats
            .record_warm_basis_cache_update(update);
        self.prune_warm_basis_budget();
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
    fn invalidate_namespace_cache(&self, namespace_id: &NamespaceId) {
        let basis_update = self
            .inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned")
            .invalidate(namespace_id);
        self.inner
            .control_cache
            .lock()
            .expect("control cache lock poisoned")
            .invalidate_namespace(namespace_id);
        let engine_eviction = self
            .inner
            .commit_engines
            .lock()
            .expect("commit engine cache lock poisoned")
            .invalidate(namespace_id);
        self.inner
            .cache_stats
            .record_warm_basis_cache_update(basis_update);
        self.inner
            .cache_stats
            .record_warm_basis_eviction(engine_eviction);
        self.refresh_warm_basis_cached_weight();
    }

    fn prune_warm_basis_budget(&self) {
        if !self.basis_cache_enabled() {
            return;
        }
        let limits = BasisCacheLimits::from_config(&self.inner.config.runtime_cache);
        loop {
            let over_budget = {
                let basis_cache = self
                    .inner
                    .basis_cache
                    .lock()
                    .expect("basis cache lock poisoned");
                let commit_engines = self
                    .inner
                    .commit_engines
                    .lock()
                    .expect("commit engine cache lock poisoned");
                let (count, rows, decoded_bytes) =
                    combined_cached_basis_totals(&basis_cache, &commit_engines);
                limits.is_over_budget(count, rows, decoded_bytes)
            };
            if !over_budget {
                break;
            }

            let basis_eviction = self
                .inner
                .basis_cache
                .lock()
                .expect("basis cache lock poisoned")
                .prune_one_lru();
            if basis_eviction.count > 0 {
                self.inner
                    .cache_stats
                    .record_warm_basis_eviction(basis_eviction);
                continue;
            }

            let engine_eviction = self
                .inner
                .commit_engines
                .lock()
                .expect("commit engine cache lock poisoned")
                .prune_one_lru();
            if engine_eviction.count == 0 {
                break;
            }
            self.inner
                .cache_stats
                .record_warm_basis_eviction(engine_eviction);
        }

        self.refresh_warm_basis_cached_weight();
    }

    fn refresh_warm_basis_cached_weight(&self) {
        let basis_cache = self
            .inner
            .basis_cache
            .lock()
            .expect("basis cache lock poisoned");
        let commit_engines = self
            .inner
            .commit_engines
            .lock()
            .expect("commit engine cache lock poisoned");
        let (_, rows, decoded_bytes) = combined_cached_basis_totals(&basis_cache, &commit_engines);
        self.inner
            .cache_stats
            .set_warm_basis_cached_weight(rows, decoded_bytes);
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

    fn namespace_engine(&self, namespace_id: &NamespaceId) -> NamespaceEngine<SharedObjectStore> {
        self.namespace_engine_with_store(namespace_id, self.inner.store.clone())
    }

    fn namespace_engine_with_store<S: ObjectStore>(
        &self,
        namespace_id: &NamespaceId,
        store: S,
    ) -> NamespaceEngine<S> {
        NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer(self.inner.config.writer_id.clone())
            .writer_version(self.inner.config.writer_version.clone())
            .lease_duration_ms(self.inner.config.lease_duration_ms)
            .build()
            .expect("validated runtime config should build namespace engine")
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
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::StaleHead => true,
        _ => false,
    }
}

fn repeat_runtime_error<T>(count: usize, error: RuntimeError) -> Vec<Result<T>> {
    let message = error.to_string();
    (0..count)
        .map(|_| {
            Err(match &error {
                RuntimeError::Core(error) => RuntimeError::Core(error.clone()),
                RuntimeError::Config(message) => RuntimeError::Config(message.clone()),
                RuntimeError::RuntimeTask(message) => RuntimeError::RuntimeTask(message.clone()),
                RuntimeError::Bootstrap(_) => RuntimeError::RuntimeTask(message.clone()),
            })
        })
        .collect()
}

fn current_time_ms() -> u64 {
    wall_clock_now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[allow(clippy::disallowed_methods)]
fn wall_clock_now() -> SystemTime {
    // Runtime cache TTLs and request timestamps are explicit wall-clock boundaries.
    SystemTime::now()
}

#[allow(clippy::disallowed_methods)]
fn monotonic_now() -> std::time::Instant {
    // Runtime cache statistics intentionally measure elapsed wall time around cache misses.
    std::time::Instant::now()
}

fn elapsed_ms_usize(started: std::time::Instant) -> usize {
    usize::try_from(started.elapsed().as_millis()).unwrap_or(usize::MAX)
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
