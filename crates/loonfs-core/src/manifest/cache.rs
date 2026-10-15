//! Caches for decoded manifest state: SST blocks keyed by owner and segment
//! id, validated manifests, and the head state reads start from.
//!
//! One or more runtimes may read through one cache, each through its own
//! views. The segment view adds the [`ObjectStoreScope`] of the runtime's
//! object store to every key, so runtimes over the same object store share
//! decoded blocks and manifests, and runtimes over different object stores
//! never read each other's blocks. The head-state view adds the runtime's
//! [`CacheScope`] to every key, so no runtime reads the head anchors or
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
use std::hash::{DefaultHasher, Hash, Hasher};
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

/// A namespace head and the metadata basis it was read against.
///
/// Keeping them together ensures reads use a consistent pair. If compaction
/// advances the current manifest, reads may replay additional WAL entries until
/// the anchor refreshes.
#[derive(Debug, Clone)]
pub struct CachedReadAnchor {
    pub head: NamespaceReadState,
    pub basis: MetadataBasis,
    /// The successor check that last confirmed `basis`.
    pub basis_checked: Option<Observation>,
    /// When the WAL probe last confirmed `head`, or when the anchor was
    /// loaded. The observation is taken before the probe or the load is
    /// sent, so `head` includes every commit acknowledged before it. A
    /// eventual read within its maximum staleness reads at `head` without a request.
    pub head_confirmed: Observation,
    pub validation: Arc<NamespaceValidation>,
    /// The number of the validation that produced this anchor.
    pub completed_validation_no: u64,
}

/// What a namespace's anchor slot had when a refresh took this ticket (see
/// [`HeadStateCache::anchor_ticket`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorTicket {
    /// The slot had the anchor that the cache installed with this version.
    Version(u64),
    /// The slot was empty, and its absence generation had this value.
    Absent { generation: u64 },
}

/// An install that [`HeadStateCache::replace_anchor`] refused, because the
/// namespace's anchor slot changed after the refresh took its ticket.
#[derive(Debug)]
pub struct AnchorFenced {
    /// The slot's anchor when the install was refused, or `None` when the
    /// slot is empty.
    pub current: Option<Arc<CachedReadAnchor>>,
}

/// Lets concurrent reads of one namespace share one revalidation.
#[derive(Debug, Default)]
pub struct NamespaceValidation {
    pub lock: tokio::sync::Mutex<()>,
    pub started: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HeadStateKey {
    Anchor(NamespaceId),
    Tail(WalTailProjectionCacheKey),
}

impl DecodedBlockKey for HeadStateKey {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Anchor(namespace_id)
            | Self::Tail(WalTailProjectionCacheKey { namespace_id, .. }) => {
                HeapBytes::heap_bytes(namespace_id)
            }
        }
    }
}

#[derive(Debug, Clone)]
enum HeadStateEntry {
    /// An anchor, with the version that the cache gave it when it was
    /// installed.
    Anchor {
        version: u64,
        anchor: Arc<CachedReadAnchor>,
    },
    Tail(Arc<ProjectedWalTail>),
}

impl HeadStateEntry {
    fn into_anchor(self) -> Option<Arc<CachedReadAnchor>> {
        match self {
            Self::Anchor { anchor, .. } => Some(anchor),
            Self::Tail(_) => None,
        }
    }

    fn versioned_anchor(&self) -> Option<(u64, &Arc<CachedReadAnchor>)> {
        match self {
            Self::Anchor { version, anchor } => Some((*version, anchor)),
            Self::Tail(_) => None,
        }
    }

    fn into_tail(self) -> Option<Arc<ProjectedWalTail>> {
        match self {
            Self::Tail(tail) => Some(tail),
            Self::Anchor { .. } => None,
        }
    }
}

impl DecodedBlock for HeadStateEntry {
    fn weight(&self) -> usize {
        match self {
            Self::Anchor { anchor, .. } => {
                arc_bytes::<CachedReadAnchor>()
                    + arc_bytes::<NamespaceValidation>()
                    + anchor.head.heap_bytes()
                    + anchor.basis.0.heap_bytes()
            }
            Self::Tail(tail) => tail.decoded_bytes(),
        }
    }
}

/// The number of absence generations in a [`SharedHeadState`].
const ABSENCE_GENERATIONS: usize = 1024;

/// Namespace head anchors and WAL-tail projections for every scope, under one
/// byte budget and one recency order.
pub struct SharedHeadState {
    entries: DecodedBlockCache<(CacheScope, HeadStateKey), HeadStateEntry>,
    max_decoded_bytes: usize,
    tail_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    counters: HeadStateCounters,
    /// The version that the last installed anchor got. Versions are unique
    /// across the shared cache, so an anchor that was dropped and loaded
    /// again never has the version of a ticket taken before the drop.
    last_anchor_version: AtomicU64,
    /// Counters that version an empty anchor slot. Each anchor slot (a scope
    /// and a namespace) maps to one counter by hash, and dropping or
    /// installing an anchor in the slot advances its counter.
    ///
    /// An empty slot has no entry to remove, so without the counters a drop
    /// of an empty slot couldn't fence a load that started before the drop,
    /// and that load could install a head older than the reads that the
    /// writer's position returned in between. Eviction can remove an entry,
    /// but it can't erase a counter. Two slots that share a counter can fence
    /// each other's loads by mistake, which only means that one load's
    /// result isn't cached and the next read loads again.
    absence_generations: Box<[AtomicU64]>,
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
            last_anchor_version: AtomicU64::new(0),
            absence_generations: (0..ABSENCE_GENERATIONS)
                .map(|_| AtomicU64::new(0))
                .collect(),
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

    fn anchor_key(&self, namespace_id: &NamespaceId) -> (CacheScope, HeadStateKey) {
        (self.scope, HeadStateKey::Anchor(namespace_id.clone()))
    }

    /// Returns the absence generation of the namespace's anchor slot.
    fn absence_generation(&self, namespace_id: &NamespaceId) -> &AtomicU64 {
        let generations = &self.shared.absence_generations;
        let mut hasher = DefaultHasher::new();
        (self.scope, namespace_id).hash(&mut hasher);
        let index = hasher.finish() % generations.len() as u64;
        &generations
            [usize::try_from(index).expect("an index below the table's length fits in usize")]
    }

    /// Returns the namespace's anchor and marks it recently used.
    pub fn get_anchor(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        self.shared
            .entries
            .get(&self.anchor_key(namespace_id))
            .and_then(HeadStateEntry::into_anchor)
    }

    /// Returns the namespace's anchor without marking it used.
    pub fn peek_anchor(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedReadAnchor>> {
        self.shared
            .entries
            .peek(&self.anchor_key(namespace_id))
            .and_then(HeadStateEntry::into_anchor)
    }

    /// Returns what the namespace's anchor slot has now, for a refresh to
    /// take before it sends any request and to pass to
    /// [`Self::replace_anchor`].
    pub fn anchor_ticket(&self, namespace_id: &NamespaceId) -> AnchorTicket {
        // Read the generation before the slot, so a drop between the two reads
        // fences the ticket instead of being missed.
        let generation = self.absence_generation(namespace_id).load(Ordering::SeqCst);
        match self.shared.entries.peek(&self.anchor_key(namespace_id)) {
            Some(HeadStateEntry::Anchor { version, .. }) => AnchorTicket::Version(version),
            _ => AnchorTicket::Absent { generation },
        }
    }

    /// Installs `anchor` as the namespace's anchor, if the slot hasn't
    /// changed since the refresh that produced `anchor` took `ticket`, and
    /// returns `anchor`.
    ///
    /// The install succeeds when the slot still has the ticket's anchor and
    /// `anchor` isn't at a lower head sequence, or when the slot is still
    /// empty and its absence generation hasn't moved. An anchor at a higher
    /// head sequence than the cached one also replaces it, whatever the
    /// ticket. A cached anchor was loaded after the slot's last drop, so its
    /// head is at least as new as every head that reads returned before that
    /// drop, and an anchor at a higher head can replace it without moving the
    /// cached head backwards. A successful install assigns `anchor` the next
    /// version and advances the slot's absence generation.
    ///
    /// Otherwise nothing changes, and [`AnchorFenced`] includes the slot's
    /// anchor, which is at a head sequence at least as high as `anchor`'s. A
    /// drop of the slot (see [`Self::invalidate_anchor`]) fences every
    /// ticket taken before it, so a refresh that started before the drop
    /// can't put its result back into the emptied slot. A cache with no room
    /// for anchors keeps nothing and returns `anchor`.
    pub fn replace_anchor(
        &self,
        ticket: AnchorTicket,
        anchor: Arc<CachedReadAnchor>,
    ) -> Result<Arc<CachedReadAnchor>, AnchorFenced> {
        let namespace_id = &anchor.head.namespace_id;
        let key = self.anchor_key(namespace_id);
        let generation = self.absence_generation(namespace_id);
        let version = self
            .shared
            .last_anchor_version
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let entry = HeadStateEntry::Anchor {
            version,
            anchor: Arc::clone(&anchor),
        };
        let mut fenced = None;
        self.shared.entries.insert_if(key, entry, |current| {
            let current = current.and_then(HeadStateEntry::versioned_anchor);
            let admitted = match current {
                Some((current_version, cached)) => {
                    anchor.head.seq > cached.head.seq
                        || (ticket == AnchorTicket::Version(current_version)
                            && anchor.head.seq >= cached.head.seq)
                }
                None => {
                    ticket
                        == AnchorTicket::Absent {
                            generation: generation.load(Ordering::SeqCst),
                        }
                }
            };
            if admitted {
                generation.fetch_add(1, Ordering::SeqCst);
            } else {
                fenced = Some(AnchorFenced {
                    current: current.map(|(_, cached)| Arc::clone(cached)),
                });
            }
            admitted
        });
        match fenced {
            Some(fenced) => Err(fenced),
            None => Ok(anchor),
        }
    }

    /// Drops the namespace's anchor and advances the slot's absence
    /// generation, also when the slot is empty, so every ticket taken before
    /// the drop is fenced.
    pub fn invalidate_anchor(&self, namespace_id: &NamespaceId) {
        let generation = self.absence_generation(namespace_id);
        self.shared
            .entries
            .invalidate_with(&self.anchor_key(namespace_id), || {
                generation.fetch_add(1, Ordering::SeqCst);
            });
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
        AnchorTicket, CachedReadAnchor, DecodedMetadataSegmentBlock, HeadStateCache, MetadataBasis,
        MetadataSegmentCache, MetadataSegmentCacheKey, NamespaceReadState, ObjectStoreScope,
        SharedSegmentBlocks, WalTailProjectionCacheKey,
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

    /// Creates an anchor for namespace `demo` at head sequence `seq`.
    fn anchor_at(seq: u64) -> Arc<CachedReadAnchor> {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        Arc::new(CachedReadAnchor {
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
                manifest_no: ManifestNo(1),
                head_seq: ChangeSeq(0),
                payload_checksum: "checksum".to_owned(),
            }),
            basis_checked: Some(crate::test_support::observed_now()),
            head_confirmed: crate::test_support::observed_now(),
            validation: Arc::default(),
            completed_validation_no: 0,
        })
    }

    fn demo() -> NamespaceId {
        NamespaceId::parse("demo").expect("namespace id")
    }

    /// Installs `anchor` and asserts that the install succeeded.
    fn install(cache: &HeadStateCache, ticket: AnchorTicket, anchor: &Arc<CachedReadAnchor>) {
        let installed = cache
            .replace_anchor(ticket, Arc::clone(anchor))
            .expect("the install succeeds");
        assert!(Arc::ptr_eq(&installed, anchor));
    }

    /// Returns the anchor that a refused install reports as the slot's.
    fn fenced(
        cache: &HeadStateCache,
        ticket: AnchorTicket,
        anchor: Arc<CachedReadAnchor>,
    ) -> Option<Arc<CachedReadAnchor>> {
        cache
            .replace_anchor(ticket, anchor)
            .expect_err("the install is fenced")
            .current
    }

    /// Checks that of two cold loads that both found the slot empty, the
    /// first to finish installs its anchor and the second is fenced when its
    /// head is older, so the newer head stays cached whatever order the
    /// loads finish in.
    #[test]
    fn an_older_cold_load_finishing_last_leaves_the_newer_anchor() {
        let cache = HeadStateCache::unshared(1 << 20);
        let older = cache.anchor_ticket(&demo());
        let newer = cache.anchor_ticket(&demo());
        let newer_anchor = anchor_at(6);
        install(&cache, newer, &newer_anchor);
        let current = fenced(&cache, older, anchor_at(5)).expect("the slot's anchor");
        assert!(Arc::ptr_eq(&current, &newer_anchor));
        let cached = cache.get_anchor(&demo()).expect("an anchor");
        assert!(Arc::ptr_eq(&cached, &newer_anchor));
    }

    /// Checks that when the older of two cold loads finishes first, the
    /// newer load's anchor replaces it, because a higher head replaces any
    /// cached anchor.
    #[test]
    fn a_newer_cold_load_finishing_last_replaces_the_older_anchor() {
        let cache = HeadStateCache::unshared(1 << 20);
        let older = cache.anchor_ticket(&demo());
        let newer = cache.anchor_ticket(&demo());
        install(&cache, older, &anchor_at(5));
        let newer_anchor = anchor_at(6);
        install(&cache, newer, &newer_anchor);
        let cached = cache.get_anchor(&demo()).expect("an anchor");
        assert!(Arc::ptr_eq(&cached, &newer_anchor));
    }

    /// Checks that dropping the anchor fences a ticket taken before the drop,
    /// whether the ticket refers to the dropped anchor's version or to an
    /// empty slot.
    #[test]
    fn an_invalidation_fences_every_ticket_taken_before_it() {
        let cache = HeadStateCache::unshared(1 << 20);
        let absent = cache.anchor_ticket(&demo());
        cache.invalidate_anchor(&demo());
        assert!(fenced(&cache, absent, anchor_at(5)).is_none());
        install(&cache, cache.anchor_ticket(&demo()), &anchor_at(5));
        let version = cache.anchor_ticket(&demo());
        assert!(matches!(version, AnchorTicket::Version(_)));
        cache.invalidate_anchor(&demo());
        assert!(fenced(&cache, version, anchor_at(5)).is_none());
        assert!(cache.get_anchor(&demo()).is_none());
    }

    /// Checks that an install at the ticket's version but at a lower head
    /// sequence is fenced, and that the cached anchor stays.
    #[test]
    fn an_anchor_at_a_lower_head_does_not_replace_the_cached_anchor() {
        let cache = HeadStateCache::unshared(1 << 20);
        let cached = anchor_at(5);
        install(&cache, cache.anchor_ticket(&demo()), &cached);
        let ticket = cache.anchor_ticket(&demo());
        let current = fenced(&cache, ticket, anchor_at(4)).expect("the slot's anchor");
        assert!(Arc::ptr_eq(&current, &cached));
        assert_eq!(cache.anchor_ticket(&demo()), ticket);
    }

    /// Checks that an install at the ticket's version and at the same or a
    /// higher head sequence succeeds and assigns the slot a new version.
    #[test]
    fn an_install_at_the_tickets_version_advances_the_version() {
        let cache = HeadStateCache::unshared(1 << 20);
        install(&cache, cache.anchor_ticket(&demo()), &anchor_at(5));
        let first = cache.anchor_ticket(&demo());
        install(&cache, first, &anchor_at(5));
        let second = cache.anchor_ticket(&demo());
        assert_ne!(second, first);
        let higher = anchor_at(6);
        install(&cache, second, &higher);
        assert_ne!(cache.anchor_ticket(&demo()), second);
        let cached = cache.get_anchor(&demo()).expect("an anchor");
        assert!(Arc::ptr_eq(&cached, &higher));
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
