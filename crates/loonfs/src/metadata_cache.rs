//! The decoded metadata cache that one or more runtimes read through.

use crate::{ObjectStore, SharedObjectStore, READAHEAD_WINDOW_BYTES};
use loonfs_core::cache::{
    CacheScope, CompletedUploadSessionCache, DecodedBlockCacheStats, HeadAndWalTailCache,
    MemorySegmentBlocks, MetadataSegmentCache, ObjectCache, ObjectStoreScope, ReadWorkingMemory,
    SharedCompletedUploadSessions, SharedHeadsAndWalTails, SharedObjects, SharedSegmentBlocks,
};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tiered_segment_blocks::TieredSegmentBlocks;

mod ssd_segment_blocks;
#[cfg(test)]
mod tests;
mod tiered_segment_blocks;

pub use ssd_segment_blocks::{
    SsdBlockWriter, SsdSegmentBlocks, SsdSegmentBlocksStats, ASSUMED_STORED_BLOCK_BYTES,
    SSD_INDEX_ENTRY_BYTES,
};

/// Default limit for decoded segment blocks: 256 MiB.
pub const DEFAULT_MAX_SEGMENT_BYTES: usize = 256 * 1024 * 1024;
/// Default limit for decoded manifests: 32 MiB (enough for about 600
/// namespaces of 10,000 files).
pub const DEFAULT_MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
/// Default limit for namespace heads: 16 MiB (enough for about 18,000
/// namespaces).
pub const DEFAULT_MAX_HEAD_BYTES: usize = 16 * 1024 * 1024;
/// Default limit for WAL-tail projections: 64 MiB.
pub const DEFAULT_MAX_WAL_TAIL_BYTES: usize = 64 * 1024 * 1024;
/// Default limit for completed upload sessions, 32 MiB (enough for about
/// 125,000 sessions).
pub const DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES: usize = 32 * 1024 * 1024;
/// Default limit for small content objects: 96 MiB.
pub const DEFAULT_MAX_CONTENT_OBJECT_BYTES: usize = 96 * 1024 * 1024;
/// Default size of the largest content object the cache keeps: 1 MiB.
pub const DEFAULT_MAX_CACHED_OBJECT_BYTES: u64 = 1024 * 1024;

/// Whether a cache lookup found its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheLookup {
    /// The cache had the entry.
    Hit,
    /// The lookup went to the store.
    Miss,
}

/// Decoded metadata that one or more runtimes read through.
///
/// The cache keeps segment blocks, manifests, namespace heads, WAL-tail
/// projections, small content objects, and the completed upload sessions
/// that garbage collection reuses between passes.
/// A cache built with an SSD tier (see
/// [`MetadataCacheBuilder::ssd_segment_blocks`]) also keeps every segment
/// block that it inserts on local disk.
///
/// Runtimes built with the cache over clones of one [`SharedObjectStore`]
/// share the decoded segment blocks, manifests, and content objects in the
/// cache. Runtimes over different object stores never read each other's
/// entries, even when two object stores contain objects under the same
/// keys. Two runtimes built from the same
/// [`StoreConfig`](crate::StoreConfig) each create their own object-store
/// client, so they don't share entries. Each
/// [`LoonFs`](crate::LoonFs) keeps its own namespace heads, WAL-tail
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
    /// The SSD tier of the segment blocks, if the cache has one.
    ssd_segment_blocks: Option<Arc<SsdSegmentBlocks>>,
    heads_and_wal_tails: Arc<SharedHeadsAndWalTails>,
    completed_upload_sessions: Arc<SharedCompletedUploadSessions>,
    objects: Arc<SharedObjects>,
    next_runtime_scope: AtomicU64,
    object_store_scopes: Mutex<ObjectStoreScopes>,
    read_ahead_window_bytes: usize,
    head_hits: AtomicUsize,
    head_misses: AtomicUsize,
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
            max_manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
            max_head_bytes: DEFAULT_MAX_HEAD_BYTES,
            max_wal_tail_bytes: DEFAULT_MAX_WAL_TAIL_BYTES,
            max_completed_upload_session_bytes: DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES,
            max_content_object_bytes: DEFAULT_MAX_CONTENT_OBJECT_BYTES,
            max_cached_object_bytes: DEFAULT_MAX_CACHED_OBJECT_BYTES,
            read_ahead_window_bytes: READAHEAD_WINDOW_BYTES,
            ssd_segment_blocks: None,
        }
    }

    /// Snapshots the cache's counters and occupancy, summed over every
    /// runtime that reads through it.
    pub fn stats(&self) -> MetadataCacheStats {
        let segments = self.inner.segment_blocks.stats();
        MetadataCacheStats {
            segment_blocks: DecodedBlockCacheStats {
                hits: segments.hits,
                misses: segments.misses,
                inserts: segments.inserts,
                evictions: segments.evictions,
                evicted_decoded_bytes: segments.evicted_decoded_bytes,
                rejections: segments.rejections,
                rejected_decoded_bytes: segments.rejected_decoded_bytes,
                cached_decoded_bytes: segments.cached_decoded_bytes,
            },
            segment_filter: SegmentFilterStats {
                skips: segments.filter_skips,
                false_positives: segments.filter_false_positives,
            },
            segment_ssd: self
                .inner
                .ssd_segment_blocks
                .as_ref()
                .map(|ssd| ssd.stats()),
            manifests: self.inner.segment_blocks.manifest_stats(),
            heads: DecodedBlockCacheStats {
                hits: self.inner.head_hits.load(Ordering::Relaxed),
                misses: self.inner.head_misses.load(Ordering::Relaxed),
                ..self.inner.heads_and_wal_tails.head_stats()
            },
            wal_tails: self.inner.heads_and_wal_tails.tail_stats(),
            content_objects: self.inner.objects.content_object_stats(),
            completed_upload_sessions: self.inner.completed_upload_sessions.stats(),
        }
    }

    /// Returns one runtime's views of the cache, with segment blocks and
    /// content objects under the scope of `store`, and namespace heads,
    /// WAL-tail projections, and completed upload sessions under a new scope
    /// for the runtime.
    pub(crate) fn bind(
        &self,
        store: &SharedObjectStore,
        read_working_memory: Arc<ReadWorkingMemory>,
    ) -> MetadataCacheBinding {
        let object_store_scope = self
            .inner
            .object_store_scopes
            .lock()
            .expect("object store scopes lock should not be poisoned")
            .scope_of(store);
        let runtime_scope =
            CacheScope::new(self.inner.next_runtime_scope.fetch_add(1, Ordering::SeqCst));
        // A store without an identity, such as a store in memory, has no
        // objects outside this process, so its blocks stay in memory.
        let shared_segment_blocks = Arc::clone(&self.inner.segment_blocks);
        let segment_blocks = match (&self.inner.ssd_segment_blocks, store.identity()) {
            (Some(ssd), Some(identity)) => {
                let memory = MemorySegmentBlocks::new(
                    Arc::clone(&shared_segment_blocks),
                    object_store_scope,
                );
                MetadataSegmentCache::with_blocks(
                    Arc::new(TieredSegmentBlocks::new(memory, Arc::clone(ssd), identity)),
                    shared_segment_blocks,
                    object_store_scope,
                    read_working_memory,
                    self.inner.read_ahead_window_bytes,
                )
            }
            _ => MetadataSegmentCache::new(
                shared_segment_blocks,
                object_store_scope,
                read_working_memory,
                self.inner.read_ahead_window_bytes,
            ),
        };
        MetadataCacheBinding {
            cache: self.clone(),
            segment_blocks,
            heads_and_wal_tails: HeadAndWalTailCache::new(
                Arc::clone(&self.inner.heads_and_wal_tails),
                runtime_scope,
            ),
            completed_upload_sessions: CompletedUploadSessionCache::new(
                Arc::clone(&self.inner.completed_upload_sessions),
                runtime_scope,
            ),
            objects: ObjectCache::new(Arc::clone(&self.inner.objects), object_store_scope),
        }
    }

    /// Stops the SSD tier from taking writes, and waits for it to write the
    /// blocks that it queued. A cache without an SSD tier has nothing to
    /// wait for.
    ///
    /// A host calls this when it shuts down, once no runtime reads through
    /// the cache, so the blocks that reads loaded last are on disk for the
    /// next start (see [`SsdSegmentBlocks::close`]).
    pub async fn close(&self) {
        if let Some(ssd) = &self.inner.ssd_segment_blocks {
            ssd.close().await;
        }
    }

    /// Records one namespace head lookup that a read used to start from the
    /// cache or that sent it to the store.
    pub(crate) fn record_head_lookup(&self, lookup: CacheLookup) {
        let counter = match lookup {
            CacheLookup::Hit => &self.inner.head_hits,
            CacheLookup::Miss => &self.inner.head_misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// One runtime's views of a [`MetadataCache`].
pub(crate) struct MetadataCacheBinding {
    pub(crate) cache: MetadataCache,
    pub(crate) segment_blocks: MetadataSegmentCache,
    pub(crate) heads_and_wal_tails: HeadAndWalTailCache,
    pub(crate) completed_upload_sessions: CompletedUploadSessionCache,
    pub(crate) objects: ObjectCache,
}

/// The object stores of the runtimes that read through the cache, each with
/// the scope of its segment blocks, manifests, and content objects.
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
    max_manifest_bytes: usize,
    max_head_bytes: usize,
    max_wal_tail_bytes: usize,
    max_completed_upload_session_bytes: usize,
    max_content_object_bytes: usize,
    max_cached_object_bytes: u64,
    read_ahead_window_bytes: usize,
    ssd_segment_blocks: Option<SsdSegmentBlocks>,
}

impl MetadataCacheBuilder {
    /// Sets the decoded bytes of segment blocks (data, index, and filter
    /// blocks) that the cache keeps in memory. Defaults to
    /// [`DEFAULT_MAX_SEGMENT_BYTES`]. Zero keeps none.
    pub fn max_segment_bytes(mut self, max_segment_bytes: usize) -> Self {
        self.max_segment_bytes = max_segment_bytes;
        self
    }

    /// Sets the decoded bytes of manifests that the cache keeps. Defaults to
    /// [`DEFAULT_MAX_MANIFEST_BYTES`]. Zero keeps none, so every read loads
    /// its manifest from the store. A manifest heavier than the whole limit
    /// isn't kept.
    ///
    /// Manifests have a limit of their own, so reads that load many segment
    /// blocks can't evict them. A manifest is charged about 10 to 60 KB for
    /// a namespace of 10,000 files, and about 240 KB for a namespace of
    /// 1,000,000 files.
    pub fn max_manifest_bytes(mut self, max_manifest_bytes: usize) -> Self {
        self.max_manifest_bytes = max_manifest_bytes;
        self
    }

    /// Sets the decoded bytes of namespace heads that the cache keeps for
    /// reads. Defaults to [`DEFAULT_MAX_HEAD_BYTES`]. Zero keeps none, so
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
    /// for reads. Defaults to [`DEFAULT_MAX_WAL_TAIL_BYTES`]. Zero keeps
    /// none, so reads cache no tails. (A writer session keeps its own WAL
    /// tail whatever the limit is, and namespace heads have their own limit,
    /// [`Self::max_head_bytes`].) A tail heavier than the whole limit isn't
    /// kept.
    pub fn max_wal_tail_bytes(mut self, max_wal_tail_bytes: usize) -> Self {
        self.max_wal_tail_bytes = max_wal_tail_bytes;
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

    /// Sets the bytes of small content objects that the cache keeps for
    /// reads. Reads look for a content object here before the store, and a
    /// read that verifies a content object of up to
    /// [`Self::max_cached_object_bytes`] from the store adds it. Defaults to
    /// [`DEFAULT_MAX_CONTENT_OBJECT_BYTES`]. Zero keeps none.
    pub fn max_content_object_bytes(mut self, max_content_object_bytes: usize) -> Self {
        self.max_content_object_bytes = max_content_object_bytes;
        self
    }

    /// Sets the size of the largest content object the cache keeps. A read
    /// of a larger object goes to the store. Defaults to
    /// [`DEFAULT_MAX_CACHED_OBJECT_BYTES`].
    pub fn max_cached_object_bytes(mut self, max_cached_object_bytes: u64) -> Self {
        self.max_cached_object_bytes = max_cached_object_bytes;
        self
    }

    /// Sets the stored bytes of segment blocks that a metadata read fetches
    /// ahead when it misses a data block. Defaults to
    /// [`READAHEAD_WINDOW_BYTES`], which says why the default is 1 MiB.
    ///
    /// A read that misses a data block fetches that block and the blocks
    /// after it in the segment, as long as their stored bytes stay within
    /// this size, in one ranged GET. Every runtime that reads through the
    /// cache uses this size. A size above 4 MiB (the most that one ranged GET
    /// of blocks reads) acts as 4 MiB, and zero reads no blocks ahead. A
    /// window also stops at the read working memory that's free (see
    /// [`ExecutionBudgetBuilder::max_read_working_bytes`]), because the
    /// read's memo is the only place that keeps the window's other blocks.
    ///
    /// [`ExecutionBudgetBuilder::max_read_working_bytes`]: crate::ExecutionBudgetBuilder::max_read_working_bytes
    pub fn read_ahead_window_bytes(mut self, read_ahead_window_bytes: usize) -> Self {
        self.read_ahead_window_bytes = read_ahead_window_bytes;
        self
    }

    /// Sets the SSD tier of the segment blocks, which keeps every block that
    /// the cache inserts on local disk as well as in memory. Defaults to
    /// none, so blocks stay in memory alone.
    ///
    /// The tier keeps the blocks of every runtime whose object store has an
    /// identity (see [`ObjectStore::identity`]), under the tier's own byte
    /// budget. Manifests and the cache's other entries stay in memory.
    pub fn ssd_segment_blocks(mut self, ssd_segment_blocks: SsdSegmentBlocks) -> Self {
        self.ssd_segment_blocks = Some(ssd_segment_blocks);
        self
    }

    /// Builds the cache.
    pub fn build(self) -> MetadataCache {
        MetadataCache {
            inner: Arc::new(MetadataCacheInner {
                segment_blocks: Arc::new(SharedSegmentBlocks::new(
                    self.max_segment_bytes,
                    self.max_manifest_bytes,
                )),
                ssd_segment_blocks: self.ssd_segment_blocks.map(Arc::new),
                heads_and_wal_tails: Arc::new(SharedHeadsAndWalTails::new(
                    self.max_head_bytes,
                    self.max_wal_tail_bytes,
                )),
                completed_upload_sessions: Arc::new(SharedCompletedUploadSessions::new(
                    self.max_completed_upload_session_bytes,
                )),
                objects: Arc::new(SharedObjects::new(
                    self.max_content_object_bytes,
                    self.max_cached_object_bytes,
                )),
                next_runtime_scope: AtomicU64::new(0),
                object_store_scopes: Mutex::default(),
                read_ahead_window_bytes: self.read_ahead_window_bytes,
                head_hits: AtomicUsize::new(0),
                head_misses: AtomicUsize::new(0),
            }),
        }
    }
}

/// Counters and occupancy of a [`MetadataCache`], summed over every runtime
/// that reads through it, with one set of cache counters for each kind of
/// entry.
///
/// The byte fields of each set report what the cache charges its entries,
/// including their keys and the cache's own allocation for each entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataCacheStats {
    /// The decoded data, index, and filter blocks of segments in memory.
    pub segment_blocks: DecodedBlockCacheStats,
    /// The lookups that the segments' bloom filters settled.
    pub segment_filter: SegmentFilterStats,
    /// The counters and occupancy of the SSD tier of the segment blocks, or
    /// `None` if the cache has no SSD tier.
    pub segment_ssd: Option<SsdSegmentBlocksStats>,
    /// The decoded manifests.
    pub manifests: DecodedBlockCacheStats,
    /// The namespace heads.
    ///
    /// `hits` counts the reads that started from a cached head, whether
    /// they took the head as it was or refreshed it, and `misses` the reads
    /// that loaded the head from the store because the cache had none. A
    /// read can look its head up in the cache more than once while it
    /// confirms the head, so these count each read once, where the cache's
    /// own lookup counts would count each lookup.
    pub heads: DecodedBlockCacheStats,
    /// The WAL-tail projections.
    pub wal_tails: DecodedBlockCacheStats,
    /// The small content objects.
    pub content_objects: DecodedBlockCacheStats,
    /// The completed upload sessions that garbage collection keeps between
    /// passes. A lookup misses when the pass reads the session's record.
    pub completed_upload_sessions: DecodedBlockCacheStats,
}

/// The lookups that the bloom filters of segments settled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentFilterStats {
    /// Segments skipped because their bloom filter ruled out a lookup before
    /// any index or data read.
    pub skips: usize,
    /// Segments whose filter admitted a lookup that matched no rows.
    pub false_positives: usize,
}
