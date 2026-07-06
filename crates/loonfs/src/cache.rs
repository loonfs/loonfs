//! Runtime caching: control-object reads, WAL-tail projections, and
//! per-namespace commit-engine serialization, plus the cache-management half of [`Fs`].
//!
//! Every cache revalidates against durable state before its contents are
//! used; nothing here weakens read-after-write consistency.

use crate::fs::{should_invalidate_after_result, Fs};
use crate::{CommitResponse, CoreError, NamespaceId};
use crate::{Result, RuntimeError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::ManifestId;
use loonfs_core::cache::{MetadataTableCacheStats, WalTailProjectionCacheStats};
use loonfs_core::control::{
    load_namespace_read_anchor, ControlObjectIdentity, ControlObjectLoadError, LoadedHeadControl,
    LoadedMetadataRootControl,
};
use loonfs_core::publish::NamespaceCommitEngine;
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext};
use loonfs_objectstore::keys::{namespace_config, wal_head};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

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

    fn invalidate(&mut self, namespace_id: &NamespaceId) {
        self.remove_entry(namespace_id);
        self.order.retain(|candidate| candidate != namespace_id);
    }

    fn remove_entry(&mut self, namespace_id: &NamespaceId) {
        let Some(engine) = self.entries.remove(namespace_id) else {
            return;
        };
        if let Ok(mut engine) = engine.try_lock() {
            engine.invalidate();
        };
    }

    fn touch(&mut self, namespace_id: &NamespaceId) {
        self.order.retain(|candidate| candidate != namespace_id);
        self.order.push_back(namespace_id.clone());
    }
}

#[derive(Debug, Default)]

pub(crate) struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, NamespaceControlCacheEntry>,
    namespace_order: VecDeque<NamespaceId>,
}

#[derive(Debug, Default)]
struct NamespaceControlCacheEntry {
    head: Option<CachedNamespaceAnchor>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedControl<T> {
    pub(crate) identity: ControlObjectIdentity,
    pub(crate) state: T,
}

/// Head snapshot pinned together with the manifest the metadata root
/// referenced when it was taken. The pair stays consistent even when
/// compaction moves the live root past this head; reads at the pin replay a
/// little more WAL until the cache refreshes.
#[derive(Debug, Clone)]
pub(crate) struct CachedNamespaceAnchor {
    pub(crate) head: CachedControl<HeadState>,
    pub(crate) manifest_id: ManifestId,
}

/// Snapshot of runtime cache counters.
///
/// These counters are diagnostic. They are useful for tuning cache limits and
/// understanding read/write warmup behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    /// Latest metadata reads served through the metadata-view path.
    pub latest_metadata_view_reads: usize,
    /// WAL-tail projection cache hits.
    pub wal_tail_projection_cache_hits: usize,
    /// WAL-tail projection cache misses.
    pub wal_tail_projection_cache_misses: usize,
    /// WAL-tail projections inserted.
    pub wal_tail_projection_cache_inserts: usize,
    /// WAL-tail projections evicted or invalidated.
    pub wal_tail_projection_cache_evictions: usize,
    /// Metadata rows dropped with evicted WAL-tail projections.
    pub wal_tail_projection_cache_evicted_rows: usize,
    /// Decoded bytes dropped with evicted WAL-tail projections.
    pub wal_tail_projection_cache_evicted_decoded_bytes: usize,
    /// WAL-tail projections too heavy to cache under configured limits.
    pub wal_tail_projection_cache_uncacheable_count: usize,
    /// Metadata rows in WAL-tail projections too heavy to cache.
    pub wal_tail_projection_cache_uncacheable_rows: usize,
    /// Decoded bytes in WAL-tail projections too heavy to cache.
    pub wal_tail_projection_cache_uncacheable_decoded_bytes: usize,
    /// Metadata rows currently retained across cached WAL-tail projections.
    pub wal_tail_projection_cache_cached_rows: usize,
    /// Decoded bytes currently retained across cached WAL-tail projections.
    pub wal_tail_projection_cache_cached_decoded_bytes: usize,
    /// Decoded metadata-table cache hits.
    pub metadata_table_cache_hits: usize,
    /// Decoded metadata-table cache misses.
    pub metadata_table_cache_misses: usize,
    /// Blocks inserted into the decoded metadata-table cache.
    pub metadata_table_cache_inserts: usize,
    /// Blocks evicted from the decoded metadata-table cache.
    pub metadata_table_cache_evictions: usize,
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeCacheStatsInner {
    latest_metadata_view_reads: AtomicUsize,
}

impl RuntimeCacheStatsInner {
    pub(crate) fn snapshot(
        &self,
        metadata_table_cache: MetadataTableCacheStats,
        wal_tail_projection_cache: WalTailProjectionCacheStats,
    ) -> RuntimeCacheStats {
        RuntimeCacheStats {
            latest_metadata_view_reads: self.latest_metadata_view_reads.load(Ordering::SeqCst),
            wal_tail_projection_cache_hits: wal_tail_projection_cache.hits,
            wal_tail_projection_cache_misses: wal_tail_projection_cache.misses,
            wal_tail_projection_cache_inserts: wal_tail_projection_cache.inserts,
            wal_tail_projection_cache_evictions: wal_tail_projection_cache.evictions,
            wal_tail_projection_cache_evicted_rows: wal_tail_projection_cache.evicted_rows,
            wal_tail_projection_cache_evicted_decoded_bytes: wal_tail_projection_cache
                .evicted_decoded_bytes,
            wal_tail_projection_cache_uncacheable_count: wal_tail_projection_cache
                .uncacheable_count,
            wal_tail_projection_cache_uncacheable_rows: wal_tail_projection_cache.uncacheable_rows,
            wal_tail_projection_cache_uncacheable_decoded_bytes: wal_tail_projection_cache
                .uncacheable_decoded_bytes,
            wal_tail_projection_cache_cached_rows: wal_tail_projection_cache.cached_rows,
            wal_tail_projection_cache_cached_decoded_bytes: wal_tail_projection_cache
                .cached_decoded_bytes,
            metadata_table_cache_hits: metadata_table_cache.hits,
            metadata_table_cache_misses: metadata_table_cache.misses,
            metadata_table_cache_inserts: metadata_table_cache.inserts,
            metadata_table_cache_evictions: metadata_table_cache.evictions,
        }
    }

    pub(crate) fn record_latest_metadata_view_read(&self) {
        self.latest_metadata_view_reads
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl RuntimeControlCache {
    fn wal_head(&mut self, namespace_id: &NamespaceId) -> Option<CachedNamespaceAnchor> {
        let head = self.namespaces.get(namespace_id)?.head.clone()?;
        self.touch_namespace(namespace_id);
        Some(head)
    }

    fn insert_namespace_head(
        &mut self,
        namespace_id: &NamespaceId,
        head: CachedNamespaceAnchor,
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
    ) -> std::result::Result<CachedNamespaceAnchor, ControlObjectLoadError> {
        let cache_config = &self.inner.config.runtime_cache;
        if !self.control_cache_enabled() {
            return load_namespace_read_anchor(self.store(), namespace_id)
                .await
                .map(cached_anchor);
        }

        let cached = self.inner.control_cache().wal_head(namespace_id);
        if let Some(head) = cached {
            match self
                .cached_control_identity_matches(
                    &wal_head(namespace_id.as_str()),
                    &head.head.identity,
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

        let loaded = match load_namespace_read_anchor(self.store(), namespace_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                self.inner
                    .control_cache()
                    .invalidate_namespace_head(namespace_id);
                return Err(error);
            }
        };
        let head = cached_anchor(loaded);
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
    ) -> Result<CachedNamespaceAnchor> {
        match self.load_namespace_head_cached(namespace_id).await {
            Ok(head) => Ok(head),
            Err(ControlObjectLoadError::MissingObject { object_key }) => {
                Err(self.missing_head_read_error(namespace_id, object_key).await)
            }
            Err(error) => Err(RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(error),
            ))),
        }
    }

    async fn missing_head_read_error(
        &self,
        namespace_id: &NamespaceId,
        head_key: String,
    ) -> RuntimeError {
        let descriptor_key = namespace_config(namespace_id.as_str());
        let descriptor_exists = match self.store().head(&descriptor_key).await {
            Ok(value) => value.is_some(),
            Err(error) => {
                return RuntimeError::Core(CoreError::Store {
                    object_key: descriptor_key,
                    message: error.message(),
                })
            }
        };
        if descriptor_exists {
            RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadHead(ControlObjectLoadError::MissingObject {
                    object_key: head_key,
                }),
            ))
        } else {
            RuntimeError::Core(CoreError::MetadataProjection(
                MetadataProjectionLoadError::LoadNamespaceDescriptor(
                    ControlObjectLoadError::MissingObject {
                        object_key: descriptor_key,
                    },
                ),
            ))
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
            .map_err(|error| ControlObjectLoadError::Store {
                object_key: object_key.to_owned(),
                message: error.message(),
            })?
            .ok_or_else(|| ControlObjectLoadError::MissingObject {
                object_key: object_key.to_owned(),
            })?;
        let Some(etag) = metadata.etag else {
            return Err(ControlObjectLoadError::Store {
                object_key: object_key.to_owned(),
                message: "missing control object etag".to_owned(),
            });
        };
        Ok(etag == identity.etag)
    }

    pub(crate) fn control_cache_enabled(&self) -> bool {
        let cache_config = &self.inner.config.runtime_cache;
        cache_config.control_cache_enabled && cache_config.max_cached_namespaces > 0
    }

    pub(crate) fn commit_engine_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_cached_namespaces > 0
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

    pub(crate) fn runtime_read_context(
        &self,
        anchor: &CachedNamespaceAnchor,
    ) -> RuntimeReadContext {
        let cache_config = &self.inner.config.runtime_cache;
        let tail_cache = cache_config
            .wal_tail_projection_cache_enabled
            .then(|| Arc::clone(&self.inner.wal_tail_projection_cache));
        RuntimeReadContext::pinned_head(
            anchor.head.state.clone(),
            anchor.head.identity.etag.clone(),
            anchor.manifest_id,
            Some(Arc::clone(&self.inner.metadata_table_cache)),
            tail_cache,
        )
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        skip_all,
        fields(phase = "update_cache")
    )]
    pub(crate) fn invalidate_namespace_cache(&self, namespace_id: &NamespaceId) {
        self.invalidate_namespace_read_cache(namespace_id);
        self.inner.commit_engines().invalidate(namespace_id);
    }

    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
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
            self.invalidate_namespace_read_cache(namespace_id);
        }
    }
}

fn cached_anchor(
    (head, root): (LoadedHeadControl, LoadedMetadataRootControl),
) -> CachedNamespaceAnchor {
    CachedNamespaceAnchor {
        head: CachedControl {
            identity: head.identity,
            state: head.state,
        },
        manifest_id: root.state.manifest_id,
    }
}
