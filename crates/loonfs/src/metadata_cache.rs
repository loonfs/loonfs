//! The decoded metadata cache that one or more runtimes read through.

use crate::{ObjectStore, SharedObjectStore};
use loonfs_core::cache::{
    CacheScope, CompletedUploadSessionCache, HeadStateCache, MetadataSegmentCache, ObjectCache,
    ObjectStoreScope, ReadWorkingMemory, SharedCompletedUploadSessions, SharedHeadState,
    SharedObjects, SharedSegmentBlocks, StoredMetadataBlockCache,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[cfg(test)]
mod tests;

/// Default limit for decoded segment blocks and manifests: 256 MiB.
pub const DEFAULT_MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
/// Default limit for namespace head anchors: 16 MiB (enough for about 18,000
/// namespaces).
pub const DEFAULT_MAX_HEAD_BYTES: usize = 16 * 1024 * 1024;
/// Default limit for WAL-tail projections: 64 MiB.
pub const DEFAULT_MAX_HEAD_STATE_BYTES: usize = 64 * 1024 * 1024;
/// Default limit for completed upload sessions, 32 MiB (enough for about
/// 110,000 sessions).
pub const DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES: usize = 32 * 1024 * 1024;
/// Default limit for decoded WAL objects and small content objects: 128 MiB.
pub const DEFAULT_MAX_OBJECT_BYTES: usize = 128 * 1024 * 1024;
/// Default size of the largest content object the cache keeps: 1 MiB.
pub const DEFAULT_MAX_CACHED_OBJECT_BYTES: u64 = 1024 * 1024;

/// Whether a cache lookup found its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheLookup {
    /// The cache held the entry.
    Hit,
    /// The lookup went to the store.
    Miss,
}

/// Decoded metadata that one or more runtimes read through.
///
/// The cache keeps segment blocks, manifests, namespace head anchors,
/// WAL-tail projections, WAL objects, small content objects, and the
/// completed upload sessions that garbage collection reuses between passes.
///
/// Runtimes built with the cache over clones of one [`SharedObjectStore`]
/// share the decoded segment blocks, manifests, WAL objects, and content
/// objects in the cache. Runtimes over different object stores never read
/// each other's entries, even when two object stores contain objects under
/// the same keys. Two runtimes built from the same
/// [`StoreConfig`](crate::StoreConfig) each create their own object-store
/// client, so they don't share entries. Each
/// [`LoonFs`](crate::LoonFs) keeps its own head anchors, WAL-tail
/// projections, and completed upload sessions. The limits bound the whole
/// cache, and the least recently used entry is evicted first, whichever
/// runtime it belongs to. Clones share the cache. A runtime built without
/// one creates a private cache with the default limits.
#[derive(Clone)]
pub struct MetadataCache {
    inner: Arc<MetadataCacheInner>,
}

struct MetadataCacheInner {
    segment_blocks: Arc<SharedSegmentBlocks>,
    head_state: Arc<SharedHeadState>,
    completed_upload_sessions: Arc<SharedCompletedUploadSessions>,
    objects: Arc<SharedObjects>,
    next_runtime_scope: AtomicU64,
    object_store_scopes: Mutex<ObjectStoreScopes>,
    head_anchor_hits: AtomicUsize,
    head_anchor_misses: AtomicUsize,
}

impl fmt::Debug for MetadataCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataCache")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl MetadataCache {
    /// Starts a cache builder with the default limits.
    pub fn builder() -> MetadataCacheBuilder {
        MetadataCacheBuilder {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_head_bytes: DEFAULT_MAX_HEAD_BYTES,
            max_head_state_bytes: DEFAULT_MAX_HEAD_STATE_BYTES,
            max_completed_upload_session_bytes: DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            max_cached_object_bytes: DEFAULT_MAX_CACHED_OBJECT_BYTES,
        }
    }

    /// Snapshots the cache's counters and occupancy, summed over every
    /// runtime that reads through it.
    pub fn stats(&self) -> MetadataCacheStats {
        let segments = self.inner.segment_blocks.stats();
        let heads = self.inner.head_state.head_stats();
        let tails = self.inner.head_state.tail_stats();
        let objects = self.inner.objects.stats();
        let completed_sessions = self.inner.completed_upload_sessions.stats();
        MetadataCacheStats {
            segment_hits: segments.hits,
            segment_misses: segments.misses,
            segment_inserts: segments.inserts,
            segment_evictions: segments.evictions,
            segment_filter_skips: segments.filter_skips,
            segment_filter_false_positives: segments.filter_false_positives,
            segment_bytes: segments.cached_decoded_bytes,
            head_anchor_hits: self.inner.head_anchor_hits.load(Ordering::Relaxed),
            head_anchor_misses: self.inner.head_anchor_misses.load(Ordering::Relaxed),
            head_evictions: heads.evictions,
            head_evicted_bytes: heads.evicted_decoded_bytes,
            head_rejections: heads.rejections,
            head_rejected_bytes: heads.rejected_decoded_bytes,
            head_bytes: heads.cached_decoded_bytes,
            wal_tail_hits: tails.hits,
            wal_tail_misses: tails.misses,
            wal_tail_inserts: tails.inserts,
            head_state_evictions: tails.evictions,
            head_state_evicted_bytes: tails.evicted_decoded_bytes,
            head_state_rejections: tails.rejections,
            head_state_rejected_bytes: tails.rejected_decoded_bytes,
            head_state_bytes: tails.cached_decoded_bytes,
            wal_object_hits: objects.wal_hits,
            wal_object_misses: objects.wal_misses,
            wal_object_inserts: objects.wal_inserts,
            content_object_hits: objects.content_hits,
            content_object_misses: objects.content_misses,
            content_object_inserts: objects.content_inserts,
            object_evictions: objects.evictions,
            object_bytes: objects.cached_decoded_bytes,
            completed_session_hits: completed_sessions.hits,
            completed_session_misses: completed_sessions.misses,
            completed_session_evictions: completed_sessions.evictions,
            completed_session_bytes: completed_sessions.cached_decoded_bytes,
        }
    }

    /// Returns one runtime's views of the cache, with segment blocks, WAL
    /// objects, and content objects under the scope of `store`, and head
    /// state and completed upload sessions under a new scope for the runtime.
    pub(crate) fn bind(
        &self,
        store: &SharedObjectStore,
        read_working_memory: Arc<ReadWorkingMemory>,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    ) -> MetadataCacheBinding {
        let object_store_scope = self
            .inner
            .object_store_scopes
            .lock()
            .expect("object store scopes lock should not be poisoned")
            .scope_of(store);
        let runtime_scope =
            CacheScope::new(self.inner.next_runtime_scope.fetch_add(1, Ordering::SeqCst));
        MetadataCacheBinding {
            cache: self.clone(),
            segment_blocks: MetadataSegmentCache::new(
                Arc::clone(&self.inner.segment_blocks),
                object_store_scope,
                read_working_memory,
                stored_block_cache,
            ),
            head_state: HeadStateCache::new(Arc::clone(&self.inner.head_state), runtime_scope),
            completed_upload_sessions: CompletedUploadSessionCache::new(
                Arc::clone(&self.inner.completed_upload_sessions),
                runtime_scope,
            ),
            objects: ObjectCache::new(Arc::clone(&self.inner.objects), object_store_scope),
        }
    }

    /// Records one head anchor lookup that a read used to start from the
    /// cache or that sent it to the store.
    pub(crate) fn record_head_anchor_lookup(&self, lookup: CacheLookup) {
        let counter = match lookup {
            CacheLookup::Hit => &self.inner.head_anchor_hits,
            CacheLookup::Miss => &self.inner.head_anchor_misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// One runtime's views of a [`MetadataCache`].
pub(crate) struct MetadataCacheBinding {
    pub(crate) cache: MetadataCache,
    pub(crate) segment_blocks: MetadataSegmentCache,
    pub(crate) head_state: HeadStateCache,
    pub(crate) completed_upload_sessions: CompletedUploadSessionCache,
    pub(crate) objects: ObjectCache,
}

/// The object stores of the runtimes that read through the cache, each with
/// the scope of its segment blocks, manifests, WAL objects, and content
/// objects.
///
/// An object store is identified by the address of its allocation. Each
/// entry keeps a weak reference to the object store, so the allocation can't
/// be freed and reused by another object store while the entry exists. Each
/// call to `bind` removes the entries of dropped object stores, so the list
/// has one entry for each live object store that a runtime was built over,
/// plus one for each object store dropped since the last call.
#[derive(Default)]
struct ObjectStoreScopes {
    next_scope: u64,
    stores: Vec<(Weak<dyn ObjectStore>, ObjectStoreScope)>,
}

impl ObjectStoreScopes {
    /// Returns the scope of `store`. The first call for `store` mints a new
    /// scope.
    fn scope_of(&mut self, store: &SharedObjectStore) -> ObjectStoreScope {
        self.stores.retain(|(known, _)| known.strong_count() > 0);
        let known_scope = self
            .stores
            .iter()
            .find(|(known, _)| std::ptr::addr_eq(known.as_ptr(), Arc::as_ptr(store)))
            .map(|(_, scope)| *scope);
        known_scope.unwrap_or_else(|| {
            let scope = ObjectStoreScope::new(self.next_scope);
            self.next_scope += 1;
            self.stores.push((Arc::downgrade(store), scope));
            scope
        })
    }
}

/// Builder for a [`MetadataCache`].
#[must_use]
pub struct MetadataCacheBuilder {
    max_segment_bytes: usize,
    max_head_bytes: usize,
    max_head_state_bytes: usize,
    max_completed_upload_session_bytes: usize,
    max_object_bytes: usize,
    max_cached_object_bytes: u64,
}

impl MetadataCacheBuilder {
    /// Sets the decoded bytes of segment blocks and manifests the cache
    /// holds. Defaults to [`DEFAULT_MAX_SEGMENT_BYTES`]; zero keeps none.
    pub fn max_segment_bytes(mut self, max_segment_bytes: usize) -> Self {
        self.max_segment_bytes = max_segment_bytes;
        self
    }

    /// Sets the decoded bytes of namespace head anchors that the cache keeps
    /// for reads. Defaults to [`DEFAULT_MAX_HEAD_BYTES`]. Zero keeps none, so
    /// every read that doesn't start from a writer session's position
    /// confirms or loads its head. A head heavier than the whole limit isn't
    /// kept.
    ///
    /// Heads have a limit of their own, so a large WAL tail can't evict them.
    /// A head is charged about 0.6 to 0.9 KB, or about 116 KB with the
    /// largest access control list.
    pub fn max_head_bytes(mut self, max_head_bytes: usize) -> Self {
        self.max_head_bytes = max_head_bytes;
        self
    }

    /// Sets the decoded bytes of WAL-tail projections that the cache keeps
    /// for reads. Defaults to [`DEFAULT_MAX_HEAD_STATE_BYTES`]. Zero keeps
    /// none, so reads cache no tails. (A writer session keeps its own WAL
    /// tail whatever the limit is, and namespace heads have their own limit,
    /// [`Self::max_head_bytes`].) A tail heavier than the whole limit isn't
    /// kept.
    pub fn max_head_state_bytes(mut self, max_head_state_bytes: usize) -> Self {
        self.max_head_state_bytes = max_head_state_bytes;
        self
    }

    /// Sets the bytes of completed upload sessions that the cache keeps
    /// between garbage collection passes.
    ///
    /// A pass doesn't read the record of a completed session that the cache
    /// keeps. Defaults to [`DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES`]. Zero
    /// keeps none, so every pass reads every session's record. The least
    /// recently used session is evicted first.
    pub fn max_completed_upload_session_bytes(
        mut self,
        max_completed_upload_session_bytes: usize,
    ) -> Self {
        self.max_completed_upload_session_bytes = max_completed_upload_session_bytes;
        self
    }

    /// Sets the decoded bytes of WAL objects and small content objects the
    /// cache keeps. Reads look for both kinds of object here before the
    /// store, and a read that loads a WAL object or verifies a small content
    /// object from the store adds it. Defaults to
    /// [`DEFAULT_MAX_OBJECT_BYTES`]; zero keeps none. An object heavier than
    /// the whole limit is not kept.
    pub fn max_object_bytes(mut self, max_object_bytes: usize) -> Self {
        self.max_object_bytes = max_object_bytes;
        self
    }

    /// Sets the size of the largest content object the cache keeps. A read
    /// of a larger object goes to the store. Defaults to
    /// [`DEFAULT_MAX_CACHED_OBJECT_BYTES`].
    pub fn max_cached_object_bytes(mut self, max_cached_object_bytes: u64) -> Self {
        self.max_cached_object_bytes = max_cached_object_bytes;
        self
    }

    /// Builds the cache.
    pub fn build(self) -> MetadataCache {
        MetadataCache {
            inner: Arc::new(MetadataCacheInner {
                segment_blocks: Arc::new(SharedSegmentBlocks::new(self.max_segment_bytes)),
                head_state: Arc::new(SharedHeadState::new(
                    self.max_head_bytes,
                    self.max_head_state_bytes,
                )),
                completed_upload_sessions: Arc::new(SharedCompletedUploadSessions::new(
                    self.max_completed_upload_session_bytes,
                )),
                objects: Arc::new(SharedObjects::new(
                    self.max_object_bytes,
                    self.max_cached_object_bytes,
                )),
                next_runtime_scope: AtomicU64::new(0),
                object_store_scopes: Mutex::default(),
                head_anchor_hits: AtomicUsize::new(0),
                head_anchor_misses: AtomicUsize::new(0),
            }),
        }
    }
}

/// Counters and occupancy of a [`MetadataCache`], summed over every runtime
/// that reads through it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataCacheStats {
    /// Segment block and manifest lookups the cache answered.
    pub segment_hits: usize,
    /// Segment block and manifest lookups the cache could not answer.
    pub segment_misses: usize,
    /// Segment blocks and manifests inserted.
    pub segment_inserts: usize,
    /// Segment blocks and manifests evicted to stay within the segment limit.
    pub segment_evictions: usize,
    /// Segments skipped because their bloom filter ruled out a lookup before
    /// any index or data read.
    pub segment_filter_skips: usize,
    /// Segments whose filter admitted a lookup that matched no rows.
    pub segment_filter_false_positives: usize,
    /// Bytes charged to segment blocks and manifests now, including their
    /// keys and the cache's own allocation for each entry.
    pub segment_bytes: usize,
    /// Head anchor lookups a read could start from.
    pub head_anchor_hits: usize,
    /// Head anchor lookups that sent a read to the store.
    pub head_anchor_misses: usize,
    /// Head anchors evicted to stay within the head limit.
    pub head_evictions: usize,
    /// Bytes charged to the evicted head anchors, including their keys.
    pub head_evicted_bytes: usize,
    /// Head anchors heavier than the whole head limit, which are never kept.
    pub head_rejections: usize,
    /// Bytes that the rejected head anchors would have been charged,
    /// including their keys and the cache's own allocation for each entry.
    pub head_rejected_bytes: usize,
    /// Bytes charged to head anchors now, including their keys and the
    /// cache's own allocation for each entry.
    pub head_bytes: usize,
    /// WAL-tail projection lookups the cache answered.
    pub wal_tail_hits: usize,
    /// WAL-tail projection lookups the cache could not answer.
    pub wal_tail_misses: usize,
    /// WAL-tail projections inserted.
    pub wal_tail_inserts: usize,
    /// WAL-tail projections evicted to stay within the head-state limit.
    pub head_state_evictions: usize,
    /// Bytes charged to the evicted WAL-tail projections, including their
    /// keys.
    pub head_state_evicted_bytes: usize,
    /// WAL-tail projections heavier than the whole head-state limit, which
    /// are never kept.
    pub head_state_rejections: usize,
    /// Bytes that the rejected WAL-tail projections would have been charged,
    /// including their keys and the cache's own allocation for each entry.
    pub head_state_rejected_bytes: usize,
    /// Bytes charged to WAL-tail projections now, including their keys and
    /// the cache's own allocation for each entry.
    pub head_state_bytes: usize,
    /// WAL object lookups that found the object in the cache.
    pub wal_object_hits: usize,
    /// WAL object lookups that went to the store.
    pub wal_object_misses: usize,
    /// WAL objects inserted.
    pub wal_object_inserts: usize,
    /// Content object lookups that found the requested bytes in the cache.
    pub content_object_hits: usize,
    /// Content object lookups that went to the store.
    pub content_object_misses: usize,
    /// Content objects inserted.
    pub content_object_inserts: usize,
    /// WAL objects and content objects evicted to stay within the object
    /// limit.
    pub object_evictions: usize,
    /// Bytes charged to WAL objects and content objects now, including their
    /// keys and the cache's own allocation for each entry.
    pub object_bytes: usize,
    /// Upload session lookups that found a completed session in the cache.
    pub completed_session_hits: usize,
    /// Upload session lookups that found no session in the cache, so the
    /// pass read the session's record.
    pub completed_session_misses: usize,
    /// Completed upload sessions evicted to stay within the completed
    /// upload session limit.
    pub completed_session_evictions: usize,
    /// Bytes charged to completed upload sessions now, including their keys
    /// and the cache's own allocation for each entry.
    pub completed_session_bytes: usize,
}
