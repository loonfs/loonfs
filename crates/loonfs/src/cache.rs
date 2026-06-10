//! Runtime caching: verified bases, per-namespace commit engines, and
//! control-object reads, plus the cache-management half of [`Fs`].
//!
//! Every cache revalidates against durable state before its contents are
//! used; nothing here weakens read-after-write consistency.

use crate::fs::{should_invalidate_after_result, Fs};
use crate::time::{elapsed_ms_usize, monotonic_now};
use crate::{CommitResponse, CoreError, NamespaceId, RuntimeCacheConfig};
use crate::{Result, RuntimeError};
use loon_api::wire::control::HeadState;
use loon_core::cache::{
    load_verified_namespace_basis, load_verified_namespace_basis_at_head,
    probe_namespace_head_etag, BasisLoadError, MetadataTableCacheStats, NamespaceHeadEtagProbe,
    VerifiedNamespaceBasis, VerifiedNamespaceBasisWeight,
};
use loon_core::control::{
    load_namespace_head_control, ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl,
};
use loon_core::publish::{
    BasisReuseEvent, NamespaceCommitEngine, NamespaceCommitEnginePublishResult,
};
use loon_objectstore::keys::namespace_head;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Default)]
pub(crate) struct BasisCache {
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

    pub(crate) fn basis_cache_enabled(&self) -> bool {
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

#[derive(Debug, Default)]
pub(crate) struct CommitEngineCache {
    entries: HashMap<NamespaceId, Arc<AsyncMutex<NamespaceCommitEngine>>>,
    order: VecDeque<NamespaceId>,
}

impl CommitEngineCache {
    fn get_or_insert(
        &mut self,
        namespace_id: &NamespaceId,
        max_cached_namespaces: usize,
    ) -> Arc<AsyncMutex<NamespaceCommitEngine>> {
        if let Some(engine) = self.entries.get(namespace_id).cloned() {
            self.touch(namespace_id);
            return engine;
        }
        let engine = Arc::new(AsyncMutex::new(NamespaceCommitEngine::new(
            namespace_id.clone(),
        )));
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
            .filter_map(|engine| engine.try_lock().ok()?.cached_basis_weight())
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
        let mut eviction = BasisCacheEviction::default();
        if let Ok(mut engine) = engine.try_lock() {
            if let Some(weight) = engine.cached_basis_weight() {
                eviction.add_weight(weight);
            }
            engine.invalidate();
        }
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

pub(crate) struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, NamespaceControlCacheEntry>,
    namespace_order: VecDeque<NamespaceId>,
}

#[derive(Debug, Default)]
struct NamespaceControlCacheEntry {
    head: Option<CachedControl<HeadState>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedControl<T> {
    pub(crate) identity: ControlObjectIdentity,
    pub(crate) state: T,
}

/// Snapshot of runtime cache counters.
///
/// These counters are diagnostic. They are useful for tuning cache limits and
/// understanding read/write warmup behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    /// Reads and publishes that reused a cached verified basis after head revalidation.
    pub warm_basis_cache_hits: usize,
    /// Reads and publishes that could not reuse a cached verified basis.
    pub warm_basis_cache_misses: usize,
    /// Cached bases dropped by budget eviction or invalidation.
    pub warm_basis_evictions: usize,
    /// Metadata rows dropped with evicted bases.
    pub warm_basis_evicted_rows: usize,
    /// Decoded bytes dropped with evicted bases.
    pub warm_basis_evicted_decoded_bytes: usize,
    /// Verified bases too heavy to cache under the configured limits.
    pub warm_basis_uncacheable_count: usize,
    /// Metadata rows in bases that were too heavy to cache.
    pub warm_basis_uncacheable_rows: usize,
    /// Decoded bytes in bases that were too heavy to cache.
    pub warm_basis_uncacheable_decoded_bytes: usize,
    /// Metadata rows currently retained across cached bases.
    pub warm_basis_cached_rows: usize,
    /// Decoded bytes currently retained across cached bases.
    pub warm_basis_cached_decoded_bytes: usize,
    /// Cold loads that reconstructed a verified basis from durable state.
    pub warm_basis_rehydrate_count: usize,
    /// Total milliseconds spent reconstructing verified bases from durable state.
    pub warm_basis_rehydrate_ms: usize,
    /// Publish batches that reused the commit engine's cached basis.
    pub publish_warm_basis_hits: usize,
    /// Publish batches that had to cold-load a basis.
    pub publish_warm_basis_misses: usize,
    /// Publish results that invalidated a cached basis.
    pub publish_warm_basis_invalidations: usize,
    /// Publish results that advanced the cached basis to the committed head.
    pub publish_warm_basis_advances: usize,
    /// Metadata reads served from materialized tables.
    pub read_materialized_table_hits: usize,
    /// Metadata reads that fell back to a full verified basis.
    pub read_full_basis_fallbacks: usize,
    /// Decoded metadata-table cache hits.
    pub metadata_table_cache_hits: usize,
    /// Decoded metadata-table cache misses.
    pub metadata_table_cache_misses: usize,
    /// Blocks inserted into the decoded metadata-table cache.
    pub metadata_table_cache_inserts: usize,
    /// Blocks evicted from the decoded metadata-table cache.
    pub metadata_table_cache_evictions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataReadSource {
    MaterializedTables,
    FullBasisFallback,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeCacheStatsInner {
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
    pub(crate) fn snapshot(
        &self,
        metadata_table_cache: MetadataTableCacheStats,
    ) -> RuntimeCacheStats {
        RuntimeCacheStats {
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

    pub(crate) fn record_publish_result(&self, result: &NamespaceCommitEnginePublishResult) {
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

    pub(crate) fn record_metadata_read_source(&self, source: MetadataReadSource) {
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

impl Fs {
    pub(crate) async fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedControl<HeadState>, ControlObjectLoadError> {
        let cache_config = &self.inner.config.runtime_cache;
        if !self.control_cache_enabled() {
            return load_namespace_head_control(self.store(), namespace_id)
                .await
                .map(cached_head);
        }

        let cached = self.inner.control_cache().namespace_head(namespace_id);
        if let Some(head) = cached {
            match self
                .cached_control_identity_matches(
                    &namespace_head(namespace_id.as_str()),
                    &head.identity,
                )
                .await
            {
                Ok(true) => return Ok(head),
                Ok(false) => self
                    .inner
                    .control_cache()
                    .invalidate_namespace_head(namespace_id),
                Err(error) => {
                    self.inner
                        .control_cache()
                        .invalidate_namespace_head(namespace_id);
                    return Err(error);
                }
            }
        }

        let loaded = match load_namespace_head_control(self.store(), namespace_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.inner
                    .control_cache()
                    .invalidate_namespace_head(namespace_id);
                return Err(error);
            }
        };
        let head = cached_head(loaded);
        self.inner.control_cache().insert_namespace_head(
            namespace_id,
            head.clone(),
            cache_config.max_cached_namespaces,
        );
        Ok(head)
    }

    pub(crate) async fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CachedControl<HeadState>> {
        match self.load_namespace_head_cached(namespace_id).await {
            Ok(head) => Ok(head),
            Err(
                ControlObjectLoadError::MissingObject { .. }
                | ControlObjectLoadError::MissingObjectAfterHead { .. },
            ) => {
                self.inner.cache_stats.record_warm_basis_miss();
                let started = monotonic_now();
                let basis = load_verified_namespace_basis(self.store(), namespace_id)
                    .await
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

    async fn cached_control_identity_matches(
        &self,
        object_key: &str,
        identity: &ControlObjectIdentity,
    ) -> std::result::Result<bool, ControlObjectLoadError> {
        let metadata = self
            .store()
            .head(object_key)
            .await
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

    pub(crate) fn control_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.control_cache_enabled && cache_config.max_cached_namespaces > 0
    }

    pub(crate) fn commit_engine_cache_enabled(&self) -> bool {
        self.basis_cache_enabled()
    }

    fn basis_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.basis_cache_enabled
            && BasisCacheLimits::from_config(cache_config).basis_cache_enabled()
    }

    pub(crate) fn commit_engine(
        &self,
        namespace_id: &NamespaceId,
    ) -> Arc<AsyncMutex<NamespaceCommitEngine>> {
        let cache_config = &self.inner.config.runtime_cache;
        self.inner
            .commit_engines()
            .get_or_insert(namespace_id, cache_config.max_cached_namespaces)
    }

    pub(crate) async fn basis_for_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Arc<VerifiedNamespaceBasis>> {
        if !self.basis_cache_enabled() {
            self.inner.cache_stats.record_warm_basis_miss();
            let started = monotonic_now();
            let basis = load_verified_namespace_basis(self.store(), namespace_id)
                .await
                .map_err(CoreError::from)?;
            self.inner
                .cache_stats
                .record_warm_basis_rehydrate(elapsed_ms_usize(started));
            return Ok(Arc::new(basis));
        }

        let cached = self.inner.basis_cache().get(namespace_id);
        if let Some(basis) = cached {
            if self.control_cache_enabled() {
                match self.load_namespace_head_cached(namespace_id).await {
                    Ok(head) if basis.matches_head_etag(&head.identity.etag) => {
                        // A matching ETag only proves the durable head object is
                        // unchanged since this basis was reconstructed and
                        // verified; the cache itself is not authoritative.
                        tracing::Span::current()
                            .record("cache_path", crate::trace::CachePath::WarmReuse.as_str());
                        self.inner.cache_stats.record_warm_basis_hit();
                        return Ok(basis.basis_arc());
                    }
                    Ok(_) | Err(_) => {
                        self.invalidate_namespace_cache(namespace_id);
                    }
                }
            } else {
                match probe_namespace_head_etag(self.store(), namespace_id).await {
                    Ok(probe) if basis.matches_head_etag_probe(&probe) => {
                        // A matching ETag only proves the durable head object is
                        // unchanged since this basis was reconstructed and
                        // verified; the cache itself is not authoritative.
                        tracing::Span::current()
                            .record("cache_path", crate::trace::CachePath::EtagProbe.as_str());
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
        let basis = load_verified_namespace_basis(self.store(), namespace_id)
            .await
            .map_err(CoreError::from)?;
        self.inner
            .cache_stats
            .record_warm_basis_rehydrate(elapsed_ms_usize(started));
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::ColdReconstruct.as_str(),
        );
        let basis = Arc::new(basis);
        self.cache_basis(Arc::clone(&basis));
        Ok(basis)
    }

    pub(crate) async fn basis_for_read_at_head(
        &self,
        namespace_id: &NamespaceId,
        head: &CachedControl<HeadState>,
    ) -> Result<Arc<VerifiedNamespaceBasis>> {
        if self.basis_cache_enabled() {
            let cached = self.inner.basis_cache().get(namespace_id);
            if let Some(basis) = cached {
                if basis.matches_head_etag(&head.identity.etag) {
                    tracing::Span::current()
                        .record("cache_path", crate::trace::CachePath::WarmReuse.as_str());
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
        .await
        .map_err(CoreError::from)?;
        self.inner
            .cache_stats
            .record_warm_basis_rehydrate(elapsed_ms_usize(started));
        tracing::Span::current().record(
            "cache_path",
            crate::trace::CachePath::ColdReconstruct.as_str(),
        );
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
    pub(crate) fn cache_basis(&self, basis: Arc<VerifiedNamespaceBasis>) {
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
        let update = self.inner.basis_cache().insert(basis, limits);
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
    pub(crate) fn invalidate_namespace_cache(&self, namespace_id: &NamespaceId) {
        let basis_update = self.inner.basis_cache().invalidate(namespace_id);
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        let engine_eviction = self.inner.commit_engines().invalidate(namespace_id);
        self.inner
            .cache_stats
            .record_warm_basis_cache_update(basis_update);
        self.inner
            .cache_stats
            .record_warm_basis_eviction(engine_eviction);
        self.refresh_warm_basis_cached_weight();
    }

    pub(crate) fn prune_warm_basis_budget(&self) {
        if !self.basis_cache_enabled() {
            return;
        }
        let limits = BasisCacheLimits::from_config(&self.inner.config.runtime_cache);
        loop {
            let over_budget = {
                let basis_cache = self.inner.basis_cache();
                let commit_engines = self.inner.commit_engines();
                let (count, rows, decoded_bytes) =
                    combined_cached_basis_totals(&basis_cache, &commit_engines);
                limits.is_over_budget(count, rows, decoded_bytes)
            };
            if !over_budget {
                break;
            }

            let basis_eviction = self.inner.basis_cache().prune_one_lru();
            if basis_eviction.count > 0 {
                self.inner
                    .cache_stats
                    .record_warm_basis_eviction(basis_eviction);
                continue;
            }

            let engine_eviction = self.inner.commit_engines().prune_one_lru();
            if engine_eviction.count == 0 {
                break;
            }
            self.inner
                .cache_stats
                .record_warm_basis_eviction(engine_eviction);
        }

        self.refresh_warm_basis_cached_weight();
    }

    pub(crate) fn refresh_warm_basis_cached_weight(&self) {
        let basis_cache = self.inner.basis_cache();
        let commit_engines = self.inner.commit_engines();
        let (_, rows, decoded_bytes) = combined_cached_basis_totals(&basis_cache, &commit_engines);
        self.inner
            .cache_stats
            .set_warm_basis_cached_weight(rows, decoded_bytes);
    }

    pub(crate) fn finish_namespace_mutation<T>(
        &self,
        namespace_id: &NamespaceId,
        result: Result<T>,
    ) -> Result<T> {
        if should_invalidate_after_result(&result) {
            self.invalidate_namespace_cache(namespace_id);
        }
        result
    }

    pub(crate) fn invalidate_namespace_cache_after_batch(
        &self,
        namespace_id: &NamespaceId,
        results: &[Result<CommitResponse>],
    ) {
        if results.iter().any(should_invalidate_after_result) {
            self.invalidate_namespace_cache(namespace_id);
        }
    }
}

fn cached_head(loaded: LoadedHeadControl) -> CachedControl<HeadState> {
    CachedControl {
        identity: loaded.identity,
        state: loaded.state,
    }
}
