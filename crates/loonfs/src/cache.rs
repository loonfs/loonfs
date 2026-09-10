//! Runtime caches for control-object reads and WAL-tail projections.
//! WAL probes observe commits; interval checks observe manifest changes.

use crate::fs::{should_invalidate_after_result, ReadCore};
use crate::metrics::RuntimeInstruments;
use crate::trace::phase_span;
use crate::{CheckpointId, CommitResponse, CoreError, NamespaceId, Recency, RuntimeCacheConfig};
use crate::{Result, RuntimeError};
use loonfs_core::cache::{MetadataSegmentCacheStats, WalTailProjectionCacheStats};
use loonfs_core::control::NamespaceReadState;
use loonfs_core::control::{
    load_checkpoint_read_basis, load_namespace_read_anchor, load_snapshot_read_basis,
    CheckpointReadBasis, ControlObjectLoadError, MetadataBasis, VerifiedNamespaceCatalogEntry,
};
use loonfs_core::{MetadataProjectionLoadError, RuntimeReadContext, StoreFailureClass};
use loonfs_objectstore::keys::metadata_manifest_object;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct RuntimeControlCache {
    namespaces: HashMap<NamespaceId, (CachedNamespaceAnchor, u64)>,
    namespace_order: Recency<NamespaceId>,
}

/// A head snapshot and the metadata basis it authorized at that point.
/// Keeping them together ensures reads use a consistent pair. If compaction
/// advances the live root, reads may replay additional WAL entries until the
/// cache refreshes.
#[derive(Debug, Clone)]
pub(crate) struct CachedNamespaceAnchor {
    pub(crate) head: NamespaceReadState,
    pub(crate) basis: MetadataBasis,
    locally_published: bool,
    last_control_check_ms: u64,
    validation: Arc<tokio::sync::Mutex<()>>,
}

/// Snapshot of runtime cache counters.
///
/// These counters are diagnostic. They are useful for tuning cache limits and
/// understanding read/write warmup behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCacheStats {
    /// Latest metadata reads served through the metadata-view path.
    pub latest_metadata_view_reads: usize,
    /// Snapshot-backed metadata views created by the runtime.
    pub snapshot_view_reads: usize,
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
    /// Decoded metadata-segment cache hits.
    pub metadata_segment_cache_hits: usize,
    /// Decoded metadata-segment cache misses.
    pub metadata_segment_cache_misses: usize,
    /// Blocks inserted into the decoded metadata-segment cache.
    pub metadata_segment_cache_inserts: usize,
    /// Blocks evicted from the decoded metadata-segment cache.
    pub metadata_segment_cache_evictions: usize,
    /// Segments skipped by their bloom filter before any index or data read.
    pub metadata_segment_cache_filter_skips: usize,
    /// Segments whose filter admitted a lookup that matched no rows.
    pub metadata_segment_cache_filter_false_positives: usize,
}

pub(crate) struct RuntimeCacheStatsInner {
    latest_metadata_view_reads: AtomicUsize,
    snapshot_view_reads: AtomicUsize,
    instruments: Arc<RuntimeInstruments>,
}

impl RuntimeCacheStatsInner {
    pub(crate) fn new(instruments: Arc<RuntimeInstruments>) -> Self {
        Self {
            latest_metadata_view_reads: AtomicUsize::new(0),
            snapshot_view_reads: AtomicUsize::new(0),
            instruments,
        }
    }

    pub(crate) fn snapshot(
        &self,
        metadata_segment_cache: MetadataSegmentCacheStats,
        wal_tail_projection_cache: WalTailProjectionCacheStats,
    ) -> RuntimeCacheStats {
        RuntimeCacheStats {
            latest_metadata_view_reads: self.latest_metadata_view_reads.load(Ordering::SeqCst),
            snapshot_view_reads: self.snapshot_view_reads.load(Ordering::SeqCst),
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
            metadata_segment_cache_hits: metadata_segment_cache.hits,
            metadata_segment_cache_misses: metadata_segment_cache.misses,
            metadata_segment_cache_inserts: metadata_segment_cache.inserts,
            metadata_segment_cache_evictions: metadata_segment_cache.evictions,
            metadata_segment_cache_filter_skips: metadata_segment_cache.filter_skips,
            metadata_segment_cache_filter_false_positives: metadata_segment_cache
                .filter_false_positives,
        }
    }

    pub(crate) fn record_latest_metadata_view_read(&self) {
        self.latest_metadata_view_reads
            .fetch_add(1, Ordering::SeqCst);
        self.instruments.latest_metadata_view_read();
    }

    pub(crate) fn record_snapshot_view_read(&self) {
        self.snapshot_view_reads.fetch_add(1, Ordering::SeqCst);
        self.instruments.snapshot_view_read();
    }
}

impl RuntimeControlCache {
    fn cached_namespace_head(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Option<CachedNamespaceAnchor> {
        let cached = &mut self.namespaces.get_mut(namespace_id)?.0;
        let head = cached.clone();
        cached.locally_published = false;
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
        let last_touch = self.namespace_order.touch(namespace_id);
        self.namespaces
            .insert(namespace_id.clone(), (head, last_touch));
        let Self {
            namespaces,
            namespace_order,
        } = self;
        while namespaces.len() > max_cached_namespaces {
            let Some(evicted) = namespace_order
                .pop_oldest(|key, stamp| namespace_slot_is_live(namespaces, key, stamp))
            else {
                break;
            };
            namespaces.remove(&evicted);
        }
        namespace_order.compact(namespaces.len(), |key, stamp| {
            namespace_slot_is_live(namespaces, key, stamp)
        });
    }

    fn invalidate_namespace(&mut self, namespace_id: &NamespaceId) {
        self.namespaces.remove(namespace_id);
    }

    fn touch_namespace(&mut self, namespace_id: &NamespaceId) {
        let stamp = self.namespace_order.touch(namespace_id);
        if let Some((_, last_touch)) = self.namespaces.get_mut(namespace_id) {
            *last_touch = stamp;
        }
        let namespaces = &self.namespaces;
        self.namespace_order
            .compact(namespaces.len(), |key, stamp| {
                namespace_slot_is_live(namespaces, key, stamp)
            });
    }
}

fn namespace_slot_is_live(
    namespaces: &HashMap<NamespaceId, (CachedNamespaceAnchor, u64)>,
    namespace_id: &NamespaceId,
    stamp: u64,
) -> bool {
    namespaces
        .get(namespace_id)
        .is_some_and(|(_, last_touch)| *last_touch == stamp)
}

impl ReadCore {
    pub(crate) async fn load_namespace_head_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedNamespaceAnchor, ControlObjectLoadError> {
        let validation = self
            .inner
            .control_cache()
            .namespaces
            .get(namespace_id)
            .map(|(head, _)| Arc::clone(&head.validation))
            .unwrap_or_default();
        // Concurrent readers share the interval check and the resulting head.
        let _validation = validation.lock().await;
        let result = self
            .refresh_namespace_head(namespace_id)
            .await
            .map(|mut head| {
                head.validation = Arc::clone(&validation);
                head
            });
        match &result {
            Ok(head) => self.inner.control_cache().insert_namespace_head(
                namespace_id,
                head.clone(),
                self.runtime_cache_config().max_cached_namespaces,
            ),
            Err(_) => self
                .inner
                .control_cache()
                .invalidate_namespace(namespace_id),
        }
        result
    }

    async fn refresh_namespace_head(
        &self,
        namespace_id: &NamespaceId,
    ) -> std::result::Result<CachedNamespaceAnchor, ControlObjectLoadError> {
        let now_ms = self.inner.timer.monotonic_now_ms();
        let cached = self
            .inner
            .control_cache()
            .cached_namespace_head(namespace_id);
        if let Some(mut head) = cached {
            if head.locally_published {
                head.locally_published = false;
                return Ok(head);
            }
            let check_due = now_ms.saturating_sub(head.last_control_check_ms)
                >= self
                    .runtime_cache_config()
                    .manifest_revalidation_interval_ms;
            let matches = !check_due || self.manifest_is_current(namespace_id, &head.basis).await?;
            if matches {
                if check_due {
                    head.last_control_check_ms = now_ms;
                }
                let mut context = self.runtime_read_context(&head);
                if loonfs_core::control::probe_namespace_wal(self.store(), &mut context).await? {
                    head.head = context.head;
                    return Ok(head);
                }
            }
        }
        load_namespace_read_anchor(self.store(), namespace_id)
            .await
            .map(|loaded| cached_anchor(loaded, now_ms))
    }

    /// Loads the read anchor, mapping an absent head to the one answer it
    /// can mean: the namespace does not exist.
    pub(crate) async fn head_for_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<CachedNamespaceAnchor> {
        self.load_namespace_head_cached(namespace_id)
            .await
            .map_err(|error| {
                RuntimeError::Core(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::LoadHead(error),
                ))
            })
    }

    /// The interval check. A deletion, a floor advance, a flush, or a
    /// compaction each publish a successor to the cached manifest, so its
    /// absence is what makes the cached anchor still current. Commits are
    /// observed through the WAL probe instead; the hint is never consulted.
    async fn manifest_is_current(
        &self,
        namespace_id: &NamespaceId,
        basis: &MetadataBasis,
    ) -> std::result::Result<bool, ControlObjectLoadError> {
        let Ok(next) = basis.manifest_no().successor() else {
            return Ok(true);
        };
        let object_key = metadata_manifest_object(namespace_id, &next);
        let successor = self.store().head(&object_key).await.map_err(|error| {
            ControlObjectLoadError::Store {
                object_key: object_key.clone(),
                message: error.public_message().into_owned(),
                class: StoreFailureClass::of(&error),
            }
        })?;
        Ok(successor.is_none())
    }

    pub(crate) fn control_cache_enabled(&self) -> bool {
        self.inner.config.runtime_cache.max_cached_namespaces > 0
    }

    /// The budgets every runtime cache sizes itself from, including the
    /// publish side's retained tail projections.
    pub(crate) fn runtime_cache_config(&self) -> &RuntimeCacheConfig {
        &self.inner.config.runtime_cache
    }

    pub(crate) fn runtime_read_context(
        &self,
        anchor: &CachedNamespaceAnchor,
    ) -> RuntimeReadContext {
        RuntimeReadContext {
            head: anchor.head.clone(),
            basis: anchor.basis.clone(),
            segment_cache: Arc::clone(&self.inner.metadata_segment_cache),
            tail_cache: Arc::clone(&self.inner.wal_tail_projection_cache),
        }
    }

    /// The shared preamble of every pinned read: revalidate or load the head
    /// anchor and pin the read context to it. The context's `head` is the
    /// anchor the read is pinned to, and the namespace's immutable identity
    /// travels inside it, so a read resolves everything it needs from one
    /// object.
    pub(crate) async fn pinned_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        let read_context = self.runtime_read_context(&anchor);
        Ok((self.reader_engine(namespace_id), read_context))
    }

    /// Pins the metadata view captured by a checkpoint.
    pub(crate) async fn pinned_read_at_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let live = self.head_for_metadata_read(namespace_id).await?;
        let pinned = load_checkpoint_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            checkpoint_id,
        )
        .await?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned))
    }

    /// Pins a snapshot-owned checkpoint while enforcing its live lease.
    pub(crate) async fn pinned_read_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        now_ms: u64,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let live = self.head_for_metadata_read(namespace_id).await?;
        let pinned = load_snapshot_read_basis(
            self.store(),
            Some(self.inner.metadata_segment_cache.as_ref()),
            &live.head,
            snapshot_id,
            now_ms,
        )
        .await?;
        Ok(self.pinned_read_at_basis(namespace_id, pinned))
    }

    fn pinned_read_at_basis(
        &self,
        namespace_id: &NamespaceId,
        pinned: CheckpointReadBasis,
    ) -> (
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    ) {
        let read_context = self.runtime_read_context(&CachedNamespaceAnchor {
            head: pinned.head,
            basis: pinned.basis,
            locally_published: false,
            last_control_check_ms: 0,
            validation: Arc::default(),
        });
        (self.reader_engine(namespace_id), read_context)
    }

    /// Pins the latest metadata view and records the read in cache metrics.
    pub(crate) async fn pinned_metadata_read(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(
        loonfs_core::NamespaceReaderEngine<crate::SharedObjectStore>,
        RuntimeReadContext,
    )> {
        let pinned = self.pinned_read(namespace_id).await?;
        self.inner.cache_stats.record_latest_metadata_view_read();
        Ok(pinned)
    }

    /// Returns the namespace's immutable identity, read off the cached head
    /// anchor.
    pub(crate) async fn load_namespace_catalog_cached(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let anchor = self.head_for_metadata_read(namespace_id).await?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&anchor.head))
    }

    /// Namespace-terminal invalidation: the whole entry is removed, because
    /// the namespace itself is gone. The publisher that ran the delete is
    /// evicted with its engine and session by the publication service.
    pub(crate) fn invalidate_namespace_cache_for_delete(&self, namespace_id: &NamespaceId) {
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
    }

    pub(crate) fn seed_namespace_read_cache(
        &self,
        namespace_id: &NamespaceId,
        state: loonfs_core::publish::ResultingReadState,
    ) {
        if !self.control_cache_enabled() {
            return;
        }
        let max_cached_namespaces = self.inner.config.runtime_cache.max_cached_namespaces;
        let head_seq = state.head.seq;
        let manifest_no = state.basis.manifest_no();
        let mut cache = self.inner.control_cache();
        let (last_control_check_ms, validation) = cache
            .namespaces
            .get(namespace_id)
            .map(|(head, _)| (head.last_control_check_ms, Arc::clone(&head.validation)))
            .unwrap_or_default();
        cache.insert_namespace_head(
            namespace_id,
            CachedNamespaceAnchor {
                head: state.head,
                basis: state.basis,
                locally_published: true,
                last_control_check_ms,
                validation,
            },
            max_cached_namespaces,
        );
        drop(cache);
        self.inner.wal_tail_projection_cache.insert(
            loonfs_core::cache::WalTailProjectionCacheKey {
                namespace_id: namespace_id.clone(),
                manifest_no,
                manifest_head_seq: state.manifest_head_seq,
                head_seq,
            },
            state.tail_rows,
        );
    }

    /// Drops the namespace's read caches. The publish-side view of the same
    /// state — a namespace publisher's WAL tail projection — is stale for
    /// exactly the same reasons, so a caller that owns a publication service
    /// drops that too; see `FsWriter::invalidate_namespace`.
    pub(crate) fn invalidate_namespace_read_cache(&self, namespace_id: &NamespaceId) {
        let _span = phase_span!(self, "update_cache", namespace_id).entered();
        self.inner
            .control_cache()
            .invalidate_namespace(namespace_id);
        self.inner
            .wal_tail_projection_cache
            .invalidate_namespace(namespace_id);
    }

    /// A successful publication or a WAL number conflict can leave read caches
    /// stale. The publisher revalidates its own view before its next batch.
    pub(crate) fn invalidate_read_cache_after_batch(
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
    (head, basis): (NamespaceReadState, MetadataBasis),
    last_control_check_ms: u64,
) -> CachedNamespaceAnchor {
    CachedNamespaceAnchor {
        head,
        basis,
        locally_published: false,
        last_control_check_ms,
        validation: Arc::default(),
    }
}
