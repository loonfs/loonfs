//! Caches for decoded manifest state: SST blocks keyed by owner and segment
//! id, validated manifests, and the head state reads start from.
//!
//! One or more runtimes may read through one cache, each through its own
//! views. The segment view adds the [`ObjectStoreScope`] of the runtime's
//! object store to every key, so runtimes over the same object store share
//! decoded blocks and manifests, and runtimes over different object stores
//! never read each other's blocks. The head-state view adds the runtime's
//! [`CacheScope`] to every key, so no runtime reads the namespace heads or
//! WAL-tail projections of another runtime. Eviction runs across every scope.
//!
//! The segment view also carries the handle to the optional node-local
//! cache of the same blocks in their encoded form; see
//! [`stored_block_cache`](super::stored_block_cache).

use super::runs::MetadataRunManifest;
use super::stored_block_cache::StoredMetadataBlockCache;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
    DecodedBlockKey, DecodedSegmentBlock, SegmentCacheKey,
};
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::read_working_memory::ReadWorkingMemory;
use crate::time::Observation;
use crate::wal::ProjectedWalTail;
use loonfs_types::format::manifest::MetadataRow;
use loonfs_types::format::manifest::NamespaceManifestEnvelope;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// The scope under which a shared cache keeps one runtime's head state.
///
/// The cache mints one scope per runtime, so no runtime reads the head state
/// of another runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheScope(u64);

impl CacheScope {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

impl DecodedBlockKey for CacheScope {
    fn heap_bytes(&self) -> usize {
        0
    }
}

/// The scope under which a shared cache keeps the segment blocks, manifests,
/// WAL objects, and content objects read from one object store.
///
/// The cache mints one scope per object store, and every runtime over that
/// object store reads under that scope. Runtimes over one object store can
/// share these entries because all of these objects are immutable, and
/// each object key identifies one object within one object store. Two object
/// stores can contain different objects under the same key (for example, the
/// first manifest of a namespace with the same ID), so the cache keys every
/// entry by the object store's scope as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectStoreScope(u64);

impl ObjectStoreScope {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

impl DecodedBlockKey for ObjectStoreScope {
    fn heap_bytes(&self) -> usize {
        0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataSegmentCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    /// Segments a scan skipped because their bloom filter ruled the lookup
    /// key out before any index or data fetch.
    pub filter_skips: usize,
    /// Segments whose filter admitted a lookup that then matched no rows.
    /// Approximate: a lookup narrower than the filter key (an exact unbind,
    /// a single revision) can count a true admission here.
    pub filter_false_positives: usize,
    pub cached_decoded_bytes: usize,
}

pub(super) type MetadataSegmentCacheKey = SegmentCacheKey;
pub(super) type DecodedMetadataSegmentBlock = DecodedSegmentBlock<
    MetadataRow,
    (
        Arc<NamespaceManifestEnvelope>,
        Arc<Vec<MetadataRunManifest>>,
        u64,
    ),
>;

pub(crate) fn read_working_memory(
    segment_cache: Option<&MetadataSegmentCache>,
) -> Arc<ReadWorkingMemory> {
    segment_cache.map_or_else(Arc::default, |cache| Arc::clone(&cache.read_working_memory))
}

/// Decoded segment blocks and manifests for every object store, under one
/// byte budget.
pub struct SharedSegmentBlocks {
    blocks:
        DecodedBlockCache<(ObjectStoreScope, MetadataSegmentCacheKey), DecodedMetadataSegmentBlock>,
    filter_stats: MetadataSegmentFilterStatsInner,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
}

impl std::fmt::Debug for SharedSegmentBlocks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedSegmentBlocks")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct MetadataSegmentFilterStatsInner {
    filter_skips: AtomicUsize,
    filter_false_positives: AtomicUsize,
}

impl SharedSegmentBlocks {
    pub fn new(
        max_decoded_bytes: usize,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            blocks: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes,
                observer: observer.clone(),
            }),
            filter_stats: MetadataSegmentFilterStatsInner::default(),
            observer,
        }
    }

    pub fn stats(&self) -> MetadataSegmentCacheStats {
        let blocks = self.blocks.stats();
        MetadataSegmentCacheStats {
            hits: blocks.hits,
            misses: blocks.misses,
            inserts: blocks.inserts,
            evictions: blocks.evictions,
            filter_skips: self.filter_stats.filter_skips.load(Ordering::Relaxed),
            filter_false_positives: self
                .filter_stats
                .filter_false_positives
                .load(Ordering::Relaxed),
            cached_decoded_bytes: blocks.cached_decoded_bytes,
        }
    }
}

/// One runtime's view of shared segment blocks, under the scope of the
/// runtime's object store, with the runtime's shared read working memory and
/// node-local encoded tier.
pub struct MetadataSegmentCache {
    blocks: Arc<SharedSegmentBlocks>,
    scope: ObjectStoreScope,
    read_working_memory: Arc<ReadWorkingMemory>,
    /// Optional node-local cache for encoded blocks. Keeping it with the
    /// decoded cache ensures callers use both cache tiers or neither tier.
    stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
}

impl std::fmt::Debug for MetadataSegmentCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetadataSegmentCache")
            .field("scope", &self.scope)
            .field("read_working_memory", &self.read_working_memory)
            .field("stored_block_cache", &self.stored_block_cache)
            .finish_non_exhaustive()
    }
}

impl MetadataSegmentCache {
    pub fn new(
        blocks: Arc<SharedSegmentBlocks>,
        scope: ObjectStoreScope,
        read_working_memory: Arc<ReadWorkingMemory>,
        stored_block_cache: Option<Arc<dyn StoredMetadataBlockCache>>,
    ) -> Self {
        Self {
            blocks,
            scope,
            read_working_memory,
            stored_block_cache,
        }
    }

    /// A view over blocks no other view reads, with the default block memo
    /// budget and no encoded tier.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedSegmentBlocks::new(max_decoded_bytes, None)),
            ObjectStoreScope::new(0),
            Arc::default(),
            None,
        )
    }

    /// Returns the node-local encoded-block cache, if one was configured.
    pub fn stored_block_cache(&self) -> Option<&Arc<dyn StoredMetadataBlockCache>> {
        self.stored_block_cache.as_ref()
    }

    /// Resolves one block access through a single-flight cell.
    pub(super) async fn get_or_load<E, F, Fut>(
        &self,
        cache_key: &MetadataSegmentCacheKey,
        load: F,
    ) -> Result<DecodedMetadataSegmentBlock, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DecodedMetadataSegmentBlock, E>>,
    {
        self.blocks
            .blocks
            .get_or_load(&(self.scope, cache_key.clone()), load)
            .await
    }

    /// Counters for the whole shared cache, across every scope.
    pub fn stats(&self) -> MetadataSegmentCacheStats {
        self.blocks.stats()
    }

    pub(super) fn record_filter_skip(&self) {
        self.blocks
            .filter_stats
            .filter_skips
            .fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.blocks.observer {
            observer.filter_skip();
        }
    }

    pub(super) fn record_filter_false_positive(&self) {
        self.blocks
            .filter_stats
            .filter_false_positives
            .fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.blocks.observer {
            observer.filter_false_positive();
        }
    }

    pub(super) fn get(&self, key: &MetadataSegmentCacheKey) -> Option<DecodedMetadataSegmentBlock> {
        self.blocks.blocks.get(&(self.scope, key.clone()))
    }

    pub(super) fn insert(&self, key: MetadataSegmentCacheKey, block: DecodedMetadataSegmentBlock) {
        self.blocks.blocks.insert((self.scope, key), block);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeadStateCacheStats {
    pub tail_hits: usize,
    pub tail_misses: usize,
    pub tail_inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    /// Entries heavier than the whole budget, which are never kept.
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    pub cached_decoded_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailProjectionCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub head_seq: ChangeSeq,
}

/// A namespace head and the metadata basis it was read against, with the
/// checks that confirmed them.
///
/// Keeping them together ensures reads use a consistent pair. If compaction
/// advances the current manifest, reads may replay additional WAL entries until
/// the head refreshes.
#[derive(Debug, Clone)]
pub struct CachedHead {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    /// The successor check that last confirmed `basis`. A read trusts the
    /// basis only while this check is younger than the revalidation bound,
    /// so the check is kept when the head is marked unconfirmed.
    pub basis_checked: Option<Observation>,
    /// When the WAL probe last confirmed `head`, or when the head was loaded,
    /// or `None` when the head is unconfirmed. The observation is taken
    /// before the probe or the load is sent, so `head` includes every commit
    /// acknowledged before it. An eventual read within its maximum staleness
    /// reads at `head` without a request, and a strong read of an unconfirmed
    /// head checks for a successor manifest whatever the interval.
    pub head_confirmed: Option<Observation>,
}

impl CachedHead {
    /// The order that [`NamespaceHead::replace`] compares heads in.
    fn order(&self) -> (ChangeSeq, ManifestNo) {
        (self.head.seq, self.basis.manifest_no())
    }

    fn heap_bytes(&self) -> usize {
        self.head.heap_bytes() + self.basis.0.heap_bytes()
    }
}

/// A namespace's head in one runtime, from its first load until nothing uses
/// it and the cache evicts it.
///
/// The head exists once it's loaded and is replaced only with a newer one
/// (see [`Self::replace`]). A change that the runtime learns of marks the
/// head unconfirmed (see [`Self::unconfirm`]) instead of removing it, so a
/// read that started before the change can't put back an older head.
#[derive(Debug, Default)]
pub struct NamespaceHead {
    /// The lock that refreshes of the head take, so they run one at a time.
    pub refresh: tokio::sync::Mutex<()>,
    /// The number of the last refresh that started.
    pub started: AtomicU64,
    /// The number of the last refresh that completed, which a read that
    /// waited for it compares with the number it observed when it arrived.
    pub completed_validation_no: AtomicU64,
    current: std::sync::Mutex<HeadSlot>,
}

/// The state behind a [`NamespaceHead`]'s lock.
#[derive(Debug, Default)]
struct HeadSlot {
    head: Option<CachedHead>,
    /// When the head was last marked unconfirmed. A confirmation taken before
    /// this observation predates the change, so it doesn't confirm the head.
    unconfirmed_at: Option<Observation>,
}

impl NamespaceHead {
    fn lock(&self) -> std::sync::MutexGuard<'_, HeadSlot> {
        // A critical section under this lock only compares and clones a head,
        // so it can't panic and poison the lock.
        self.current
            .lock()
            .expect("the namespace head lock should not be poisoned")
    }

    /// Returns the namespace's head, or `None` before its first load.
    pub fn current(&self) -> Option<CachedHead> {
        self.lock().head.clone()
    }

    /// Installs `candidate` if it's newer than the current head, and returns
    /// the head that's current afterwards, which the read starts from.
    ///
    /// A candidate is newer when its head sequence is higher, or when its
    /// sequence is the same and its basis is a later manifest. A candidate
    /// at the same sequence and manifest keeps the current head, and each
    /// confirmation becomes the newer of the two, because confirmations
    /// only move forward. An older candidate changes nothing. So the head
    /// that reads start from never moves backwards, whatever order the
    /// reads that produced the candidates finish in.
    ///
    /// A candidate's head confirmation is kept only if it's newer than the
    /// last time the head was marked unconfirmed, so a read that started
    /// before a change can't confirm the head again after it.
    pub fn replace(&self, mut candidate: CachedHead) -> CachedHead {
        let mut slot = self.lock();
        if let Some(unconfirmed_at) = &slot.unconfirmed_at {
            candidate.head_confirmed = candidate
                .head_confirmed
                .filter(|confirmed| confirmed.is_after(unconfirmed_at));
        }
        let next = match slot.head.take() {
            None => candidate,
            Some(cached) => match candidate.order().cmp(&cached.order()) {
                std::cmp::Ordering::Greater => candidate,
                std::cmp::Ordering::Less => cached,
                std::cmp::Ordering::Equal => CachedHead {
                    basis_checked: newer(cached.basis_checked, candidate.basis_checked),
                    head_confirmed: newer(cached.head_confirmed, candidate.head_confirmed),
                    ..cached
                },
            },
        };
        slot.head = Some(next.clone());
        next
    }

    /// Marks the head unconfirmed at `now`, after the runtime learns of a
    /// change to the namespace, such as a manifest that it published itself
    /// or found. The head and its basis stay, so the next strong read
    /// confirms them from the known tip with a WAL probe and a successor
    /// check, and an eventual read doesn't read at the head until then.
    pub fn unconfirm(&self, now: Observation) {
        let mut slot = self.lock();
        if let Some(cached) = slot.head.as_mut() {
            cached.head_confirmed = None;
        }
        slot.unconfirmed_at = Some(match slot.unconfirmed_at.take() {
            Some(previous) => previous.newer(now),
            None => now,
        });
    }
}

/// Returns the newer of two observations of one timer.
fn newer(first: Option<Observation>, second: Option<Observation>) -> Option<Observation> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.newer(second)),
        (first, second) => first.or(second),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HeadStateKey {
    Head(NamespaceId),
    Tail(WalTailProjectionCacheKey),
}

impl DecodedBlockKey for HeadStateKey {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Head(namespace_id)
            | Self::Tail(WalTailProjectionCacheKey { namespace_id, .. }) => {
                HeapBytes::heap_bytes(namespace_id)
            }
        }
    }
}

#[derive(Debug, Clone)]
enum HeadStateEntry {
    Head(Arc<NamespaceHead>),
    Tail(Arc<ProjectedWalTail>),
}

impl HeadStateEntry {
    fn into_head(self) -> Option<Arc<NamespaceHead>> {
        match self {
            Self::Head(head) => Some(head),
            Self::Tail(_) => None,
        }
    }

    fn into_tail(self) -> Option<Arc<ProjectedWalTail>> {
        match self {
            Self::Tail(tail) => Some(tail),
            Self::Head(_) => None,
        }
    }
}

impl DecodedBlock for HeadStateEntry {
    fn weight(&self) -> usize {
        match self {
            Self::Head(head) => {
                arc_bytes::<NamespaceHead>()
                    + head.current().as_ref().map_or(0, CachedHead::heap_bytes)
            }
            Self::Tail(tail) => tail.decoded_bytes(),
        }
    }

    /// A head that a read is using stays cached, so the read's refresh and the
    /// reads that wait for it share one head.
    fn pinned(&self) -> bool {
        match self {
            Self::Head(head) => Arc::strong_count(head) > 1,
            Self::Tail(_) => false,
        }
    }
}

/// Namespace heads and WAL-tail projections for every scope, under one
/// byte budget and one recency order.
pub struct SharedHeadState {
    entries: DecodedBlockCache<(CacheScope, HeadStateKey), HeadStateEntry>,
    max_decoded_bytes: usize,
    tail_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    counters: HeadStateCounters,
}

impl std::fmt::Debug for SharedHeadState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedHeadState")
            .field("max_decoded_bytes", &self.max_decoded_bytes)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct HeadStateCounters {
    tail_hits: AtomicUsize,
    tail_misses: AtomicUsize,
    tail_inserts: AtomicUsize,
}

impl SharedHeadState {
    /// `observer` sees evictions, rejections, and retained bytes across both
    /// kinds of entry; `tail_observer` sees WAL-tail lookups and inserts.
    pub fn new(
        max_decoded_bytes: usize,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
        tail_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            entries: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes,
                observer,
            }),
            max_decoded_bytes,
            tail_observer,
            counters: HeadStateCounters::default(),
        }
    }

    pub fn stats(&self) -> HeadStateCacheStats {
        let entries = self.entries.stats();
        let counters = &self.counters;
        HeadStateCacheStats {
            tail_hits: counters.tail_hits.load(Ordering::Relaxed),
            tail_misses: counters.tail_misses.load(Ordering::Relaxed),
            tail_inserts: counters.tail_inserts.load(Ordering::Relaxed),
            evictions: entries.evictions,
            evicted_decoded_bytes: entries.evicted_decoded_bytes,
            rejections: entries.rejections,
            rejected_decoded_bytes: entries.rejected_decoded_bytes,
            cached_decoded_bytes: entries.cached_decoded_bytes,
        }
    }
}

/// One runtime's view of shared head state.
#[derive(Debug)]
pub struct HeadStateCache {
    shared: Arc<SharedHeadState>,
    scope: CacheScope,
}

impl HeadStateCache {
    pub fn new(shared: Arc<SharedHeadState>, scope: CacheScope) -> Self {
        Self { shared, scope }
    }

    /// A view over head state no other view reads.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedHeadState::new(max_decoded_bytes, None, None)),
            CacheScope::new(0),
        )
    }

    /// Counters for the whole shared cache, across every scope.
    pub fn stats(&self) -> HeadStateCacheStats {
        self.shared.stats()
    }

    fn head_key(&self, namespace_id: &NamespaceId) -> (CacheScope, HeadStateKey) {
        (self.scope, HeadStateKey::Head(namespace_id.clone()))
    }

    /// Returns the namespace's head, creating it when the cache has none,
    /// and marks it recently used.
    ///
    /// The head is created empty and stays cached while a read uses it. A
    /// cache with no room for it returns a head that isn't cached, so each
    /// read loads on its own.
    pub fn namespace_head(&self, namespace_id: &NamespaceId) -> Arc<NamespaceHead> {
        let key = self.head_key(namespace_id);
        if let Some(head) = self
            .shared
            .entries
            .get(&key)
            .and_then(HeadStateEntry::into_head)
        {
            return head;
        }
        let created = Arc::new(NamespaceHead::default());
        let mut found = None;
        self.shared
            .entries
            .insert_if(
                key,
                HeadStateEntry::Head(Arc::clone(&created)),
                |current| match current {
                    Some(HeadStateEntry::Head(head)) => {
                        found = Some(Arc::clone(head));
                        false
                    }
                    _ => true,
                },
            );
        found.unwrap_or(created)
    }

    /// Returns the namespace's head without creating it, and marks it
    /// recently used.
    pub fn get_head(&self, namespace_id: &NamespaceId) -> Option<Arc<NamespaceHead>> {
        self.shared
            .entries
            .get(&self.head_key(namespace_id))
            .and_then(HeadStateEntry::into_head)
    }

    /// Returns the namespace's head without creating it or marking it used.
    pub fn peek_head(&self, namespace_id: &NamespaceId) -> Option<Arc<NamespaceHead>> {
        self.shared
            .entries
            .peek(&self.head_key(namespace_id))
            .and_then(HeadStateEntry::into_head)
    }

    /// Charges the cache for `head` again after its head changed, if the
    /// cache still has this head for the namespace.
    pub fn recharge_head(&self, namespace_id: &NamespaceId, head: &Arc<NamespaceHead>) {
        self.shared.entries.insert_if(
            self.head_key(namespace_id),
            HeadStateEntry::Head(Arc::clone(head)),
            |current| matches!(current, Some(HeadStateEntry::Head(cached)) if Arc::ptr_eq(cached, head)),
        );
    }

    pub fn get_tail(&self, key: &WalTailProjectionCacheKey) -> Option<Arc<ProjectedWalTail>> {
        let shared = &self.shared;
        if shared.max_decoded_bytes == 0 {
            return None;
        }
        let tail = shared
            .entries
            .get(&(self.scope, HeadStateKey::Tail(key.clone())))
            .and_then(HeadStateEntry::into_tail);
        let counter = match &tail {
            Some(_) => &shared.counters.tail_hits,
            None => &shared.counters.tail_misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &shared.tail_observer {
            match &tail {
                Some(_) => observer.hit(),
                None => observer.miss(),
            }
        }
        tail
    }

    pub fn insert_tail(&self, key: WalTailProjectionCacheKey, tail: Arc<ProjectedWalTail>) {
        let shared = &self.shared;
        if shared.entries.insert(
            (self.scope, HeadStateKey::Tail(key)),
            HeadStateEntry::Tail(tail),
        ) {
            shared.counters.tail_inserts.fetch_add(1, Ordering::Relaxed);
            if let Some(observer) = &shared.tail_observer {
                observer.insert();
            }
        }
    }

    pub fn invalidate_tail(&self, key: &WalTailProjectionCacheKey) {
        self.shared
            .entries
            .invalidate(&(self.scope, HeadStateKey::Tail(key.clone())));
    }

    /// Returns the budget needed to cache this WAL-tail projection on its own.
    #[cfg(any(test, feature = "test-support"))]
    pub fn tail_charged_bytes(
        &self,
        key: &WalTailProjectionCacheKey,
        tail: &Arc<ProjectedWalTail>,
    ) -> usize {
        crate::block_cache::charged_bytes(
            &(self.scope, HeadStateKey::Tail(key.clone())),
            &HeadStateEntry::Tail(Arc::clone(tail)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheScope, CachedHead, DecodedMetadataSegmentBlock, HeadStateCache, HeadStateEntry,
        HeadStateKey, MetadataBasis, MetadataSegmentCache, MetadataSegmentCacheKey, NamespaceHead,
        NamespaceReadState, ObjectStoreScope, Observation, SharedSegmentBlocks,
        WalTailProjectionCacheKey,
    };
    use crate::block_cache::{charged_bytes, DecodedBlock, SegmentBlockKind};
    use crate::metadata::{InodeRecord, MetadataState};
    use crate::wal::ProjectedWalTail;
    use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
    use loonfs_types::format::segment_blocks::DecodedDataBlock;
    use loonfs_types::{ActorId, ChangeSeq, InodeId, InodeKind, ManifestNo, NamespaceId};
    use loonfs_types::{WalNo, WriterEpoch};
    use std::sync::Arc;

    fn block(decoded_bytes: usize) -> DecodedMetadataSegmentBlock {
        DecodedMetadataSegmentBlock::Data {
            block: Arc::new(DecodedDataBlock {
                row_keys: Vec::new(),
                rows: Vec::new(),
            }),
            decoded_bytes,
        }
    }

    fn key(identity: &str) -> MetadataSegmentCacheKey {
        MetadataSegmentCacheKey {
            identity: identity.to_owned(),
            block_kind: SegmentBlockKind::Data,
            block_offset: 0,
        }
    }

    #[test]
    fn row_attribution_and_timestamps_never_enter_projection_cache_keys() {
        let cache = HeadStateCache::unshared(16 * 1024);
        let key = WalTailProjectionCacheKey {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            manifest_no: ManifestNo(7),
            head_seq: ChangeSeq(12),
        };
        let actors = [
            ActorId::parse("auth0|x").expect("actor id"),
            ActorId::parse("x".repeat(256)).expect("256-byte actor id"),
            ActorId::parse("external|actor").expect("external actor id"),
        ];

        for (offset, actor) in actors.into_iter().enumerate() {
            let rows = MetadataState::from_rows(
                vec![InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    committed_seq: ChangeSeq(3),
                    commit_id: loonfs_types::CommitId::parse("c_cache_row").expect("commit id"),
                    committed_by: actor.clone(),
                    committed_at_ms: 3_000 + offset as u64,
                }],
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            let tail = Arc::new(ProjectedWalTail::from_rows(rows));
            cache.insert_tail(key.clone(), Arc::clone(&tail));
            assert_eq!(
                cache
                    .get_tail(&key)
                    .expect("same projection cache key should hit")
                    .chunks()[0]
                    .rows
                    .inodes()[0]
                    .committed_by,
                actor
            );
        }
    }

    /// Checks that views over two object stores read their own blocks under
    /// the same key, and that two views over one object store read the same
    /// blocks.
    #[tokio::test]
    async fn views_read_only_the_blocks_of_their_own_object_store() {
        let shared = Arc::new(SharedSegmentBlocks::new(usize::MAX, None));
        let view = |scope| {
            MetadataSegmentCache::new(
                Arc::clone(&shared),
                ObjectStoreScope::new(scope),
                Arc::default(),
                None,
            )
        };
        let (first, second, first_again) = (view(1), view(2), view(1));
        first.insert(key("a"), block(100));
        second.insert(key("a"), block(200));
        for (view, decoded_bytes) in [(&first, 100), (&second, 200), (&first_again, 100)] {
            let hit = view
                .get(&key("a"))
                .expect("each view reads the block of its own object store");
            assert_eq!(hit.weight(), decoded_bytes);
        }

        // Each load waits for the other to start. One in-flight cell shared
        // across scopes would run a single load, and this would time out.
        let (started, shared_key) = (tokio::sync::Barrier::new(2), key("b"));
        let first_load = first.get_or_load(&shared_key, || async {
            started.wait().await;
            Ok::<_, String>(block(300))
        });
        let second_load = second.get_or_load(&shared_key, || async {
            started.wait().await;
            Ok::<_, String>(block(400))
        });
        let (from_first, from_second) =
            loonfs_test_support::clock::bounded(std::time::Duration::from_secs(10), async {
                tokio::join!(first_load, second_load)
            })
            .await
            .expect("loads in two scopes run side by side");
        assert_eq!(from_first.expect("first load").weight(), 300);
        assert_eq!(from_second.expect("second load").weight(), 400);
        for (view, decoded_bytes) in [(&first, 300), (&second, 400)] {
            let hit = view.get(&key("b")).expect("each view caches its own load");
            assert_eq!(hit.weight(), decoded_bytes);
        }
    }

    #[test]
    fn byte_budget_evicts_the_oldest_block() {
        let budget = charged_bytes(&(ObjectStoreScope::new(0), key("a")), &block(600));
        let cache = MetadataSegmentCache::unshared(budget);
        cache.insert(key("a"), block(600));
        cache.insert(key("b"), block(600));
        assert!(
            cache.get(&key("a")).is_none(),
            "oldest block should evict once the byte budget is exceeded"
        );
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn replacing_a_block_reaccounts_its_decoded_bytes() {
        let charge = |identity, decoded_bytes| {
            charged_bytes(
                &(ObjectStoreScope::new(0), key(identity)),
                &block(decoded_bytes),
            )
        };
        let cache = MetadataSegmentCache::unshared(charge("a", 100) + charge("b", 600));
        cache.insert(key("a"), block(600));
        cache.insert(key("a"), block(100));
        // 600 was released on replace: another 600 fits without eviction.
        cache.insert(key("b"), block(600));
        assert!(cache.get(&key("a")).is_some());
        assert!(cache.get(&key("b")).is_some());
        assert_eq!(cache.stats().evictions, 0);
    }

    #[test]
    fn cache_hits_share_the_decoded_row_allocation() {
        let cache = MetadataSegmentCache::unshared(usize::MAX);
        let inserted = block(64);
        let rows = match &inserted {
            DecodedMetadataSegmentBlock::Data { block: rows, .. } => Arc::clone(rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => {
                panic!("fixture builds a data block")
            }
        };
        cache.insert(key("a"), inserted);
        let hit = cache.get(&key("a")).expect("inserted block should hit");
        let shares_allocation = match &hit {
            DecodedMetadataSegmentBlock::Data {
                block: hit_rows, ..
            } => Arc::ptr_eq(hit_rows, &rows),
            DecodedMetadataSegmentBlock::Index { .. }
            | DecodedMetadataSegmentBlock::Filter { .. }
            | DecodedMetadataSegmentBlock::Manifest { .. } => false,
        };
        assert!(
            shares_allocation,
            "a cache hit should share the decoded rows, not clone them"
        );
    }

    fn namespace(name: &str) -> NamespaceId {
        NamespaceId::parse(name).expect("namespace id")
    }

    /// Creates a head for namespace `demo` at head sequence `seq` against
    /// manifest `manifest_no`, with both checks at `checked`.
    fn head_at(seq: u64, manifest_no: u64, checked: &Observation) -> CachedHead {
        let namespace_id = namespace("demo");
        CachedHead {
            head: NamespaceReadState {
                namespace_id: namespace_id.clone(),
                created_at_ms: 1_000,
                created_by: loonfs_test_support::test_actor(),
                access: loonfs_types::NamespaceAccess::Unrestricted {},
                naming: loonfs_types::NamespaceNaming::CaseInsensitive,
                fork_basis: None,
                status: NamespaceStatus::Active {},
                writer_epoch: WriterEpoch(1),
                writer: None,
                seq: ChangeSeq(seq),
                next_inode_id: InodeId(2),
                wal_no: WalNo(seq),
                folded_wal_no: WalNo(0),
            },
            basis: MetadataBasis(ManifestRef {
                owner_namespace_id: namespace_id,
                manifest_no: ManifestNo(manifest_no),
                head_seq: ChangeSeq(0),
                payload_checksum: "checksum".to_owned(),
            }),
            basis_checked: Some(checked.clone()),
            head_confirmed: Some(checked.clone()),
        }
    }

    fn order(head: &CachedHead) -> (u64, u64) {
        (head.head.seq.0, head.basis.manifest_no().0)
    }

    /// Checks that a candidate at a lower head sequence, or at the same
    /// sequence against an earlier manifest, doesn't replace the current
    /// head.
    #[test]
    fn a_lower_head_does_not_replace_the_current_head() {
        let now = crate::test_support::observed_now();
        let head = NamespaceHead::default();
        head.replace(head_at(5, 2, &now));
        assert_eq!(order(&head.replace(head_at(4, 3, &now))), (5, 2));
        assert_eq!(order(&head.replace(head_at(5, 1, &now))), (5, 2));
        assert_eq!(order(&head.current().expect("a head")), (5, 2));
    }

    /// Checks that a candidate at a higher head sequence, or at the same
    /// sequence against a later manifest, replaces the current head.
    #[test]
    fn a_higher_head_or_a_later_manifest_replaces_the_current_head() {
        let now = crate::test_support::observed_now();
        let head = NamespaceHead::default();
        assert_eq!(order(&head.replace(head_at(5, 1, &now))), (5, 1));
        assert_eq!(order(&head.replace(head_at(5, 2, &now))), (5, 2));
        assert_eq!(order(&head.replace(head_at(6, 1, &now))), (6, 1));
        assert_eq!(order(&head.current().expect("a head")), (6, 1));
    }

    /// Checks that a candidate at the same head and manifest keeps the
    /// current head and takes the newer of each confirmation, and that it
    /// confirms an unconfirmed head.
    #[test]
    fn an_equal_head_keeps_the_newer_confirmations() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let early = Observation::now(clock.clone());
        clock.advance_ms(10);
        let late = Observation::now(clock.clone());
        let head = NamespaceHead::default();
        head.replace(CachedHead {
            head_confirmed: Some(late.clone()),
            ..head_at(5, 1, &early)
        });
        let merged = head.replace(CachedHead {
            basis_checked: Some(late.clone()),
            ..head_at(5, 1, &early)
        });
        let age = |check: &Option<Observation>| check.as_ref().map(Observation::age_ms);
        assert_eq!(age(&merged.basis_checked), Some(0));
        assert_eq!(age(&merged.head_confirmed), Some(0));

        head.unconfirm(early.clone());
        clock.advance_ms(10);
        let later = Observation::now(clock.clone());
        let confirmed = head.replace(head_at(5, 1, &later));
        assert_eq!(age(&confirmed.head_confirmed), Some(0));
        assert_eq!(age(&confirmed.basis_checked), Some(0));
    }

    /// Checks that a confirmation taken before the head was marked
    /// unconfirmed doesn't confirm it again, whether it comes with an equal
    /// head or a newer one, and that a later confirmation does, also within
    /// the same millisecond.
    #[test]
    fn a_confirmation_from_before_a_change_does_not_confirm_the_head() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let before = Observation::now(clock.clone());
        let head = NamespaceHead::default();
        head.replace(head_at(5, 1, &before));
        head.unconfirm(Observation::now(clock.clone()));
        assert!(head
            .replace(head_at(5, 1, &before))
            .head_confirmed
            .is_none());
        assert!(head
            .replace(head_at(6, 1, &before))
            .head_confirmed
            .is_none());
        let after = Observation::now(clock.clone());
        assert!(head.replace(head_at(6, 1, &after)).head_confirmed.is_some());
    }

    /// Checks that unconfirming a head keeps the head, its basis, and its
    /// successor check, and clears the head's confirmation.
    #[test]
    fn unconfirming_keeps_the_head_and_clears_its_confirmation() {
        let now = crate::test_support::observed_now();
        let head = NamespaceHead::default();
        head.unconfirm(now.renew());
        assert!(head.current().is_none());
        head.replace(head_at(5, 2, &now));
        head.unconfirm(now.renew());
        let current = head.current().expect("the head stays");
        assert_eq!(order(&current), (5, 2));
        assert!(current.basis_checked.is_some());
        assert!(current.head_confirmed.is_none());
    }

    /// Checks that eviction skips a namespace head that a task is using, and
    /// evicts one that nothing uses.
    #[test]
    fn a_held_head_survives_eviction_and_a_released_one_does_not() {
        let one_head = charged_bytes(
            &(CacheScope::new(0), HeadStateKey::Head(namespace("aaaa"))),
            &HeadStateEntry::Head(Arc::new(NamespaceHead::default())),
        );
        let cache = HeadStateCache::unshared(one_head);
        let held = cache.namespace_head(&namespace("aaaa"));
        let released = cache.namespace_head(&namespace("bbbb"));
        drop(released);
        let third = cache.namespace_head(&namespace("cccc"));
        let cached = cache.peek_head(&namespace("aaaa")).expect("the held head");
        assert!(Arc::ptr_eq(&cached, &held));
        assert!(cache.peek_head(&namespace("bbbb")).is_none());
        assert!(cache.stats().evictions > 0);
        drop((held, third));
    }

    #[tokio::test]
    async fn get_or_load_retries_a_failed_load_and_then_stops_loading() {
        let cache = MetadataSegmentCache::unshared(usize::MAX);
        let failed: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Err("transport".to_owned()) })
            .await;
        assert_eq!(
            failed.expect_err("the failed load should return its error"),
            "transport"
        );
        let recovered: Result<_, String> = cache
            .get_or_load(&key("a"), || async { Ok(block(1)) })
            .await;
        assert!(
            recovered.is_ok(),
            "a failed fetch should leave nothing behind for the next caller"
        );

        let cached: Result<_, String> = cache
            .get_or_load(&key("a"), || async {
                Err("a populated key must not re-fetch".to_owned())
            })
            .await;
        assert!(cached.is_ok(), "the cached block should answer the access");
    }
}
