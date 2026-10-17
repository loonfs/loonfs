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
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockKey, DecodedSegmentBlock,
    SegmentCacheKey,
};
use crate::error::CoreError;
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::read_working_memory::ReadWorkingMemory;
use crate::shared_check::SharedCheck;
use crate::time::Observation;
use crate::wal::ProjectedWalTail;
use loonfs_types::format::manifest::MetadataRow;
use loonfs_types::format::manifest::NamespaceManifestEnvelope;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self {
            blocks: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_decoded_bytes)),
            filter_stats: MetadataSegmentFilterStatsInner::default(),
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
            Arc::new(SharedSegmentBlocks::new(max_decoded_bytes)),
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
    }

    pub(super) fn record_filter_false_positive(&self) {
        self.blocks
            .filter_stats
            .filter_false_positives
            .fetch_add(1, Ordering::Relaxed);
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
    /// basis only while this check is younger than the revalidation bound.
    pub basis_checked: Observation,
    /// When the WAL probe last confirmed `head`, or when the head was loaded.
    /// The observation is taken before the probe or the load is sent, so
    /// `head` includes every commit acknowledged before it. An eventual read
    /// within its maximum staleness reads at `head` without a request.
    pub head_confirmed: Observation,
}

impl CachedHead {
    /// The order that [`HeadStateCache::replace_head`] compares heads in.
    fn order(&self) -> (ChangeSeq, ManifestNo) {
        (self.head.seq, self.basis.manifest_no())
    }

    fn heap_bytes(&self) -> usize {
        self.head.heap_bytes() + self.basis.0.heap_bytes()
    }
}

/// What one runtime has learned about one namespace's head while a read or
/// a writer session uses the record.
///
/// The head itself is an immutable [`CachedHead`] snapshot in the head-state
/// cache, weighed when it's inserted and evicted like any other entry. The
/// record is kept outside the cache's budget, with a few words and the head
/// of the last refresh. It lives while a read or a writer session
/// references it and goes away when the namespace is idle, which is when no
/// read or position that predates a change can still install a head.
#[derive(Debug, Default)]
pub struct HeadRecord {
    state: std::sync::Mutex<RecordState>,
    /// The runtime's refreshes of the namespace's head, which the reads of
    /// the namespace share. It keeps the head that the last refresh
    /// returned, which the cache might have evicted since, until the next
    /// refresh or until nothing references the record.
    refresh: SharedCheck<std::result::Result<Arc<CachedHead>, CoreError>>,
}

/// The state behind a [`HeadRecord`]'s lock.
#[derive(Debug, Default)]
struct RecordState {
    /// The order of the newest head that the record installed or saw. It
    /// stays after the cache evicts that head.
    floor: Option<(ChangeSeq, ManifestNo)>,
    /// How many times the runtime dropped the namespace's cached head for a
    /// change of unknown effect. A load installs its head only if the
    /// generation hasn't moved since the load started.
    generation: u64,
}

impl HeadRecord {
    /// Returns the record's generation, which a load takes before it sends
    /// anything and passes to [`HeadStateCache::replace_head`].
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    /// Returns the runtime's refreshes of the namespace's head, which the
    /// reads of the namespace share.
    pub fn refresh(&self) -> &SharedCheck<std::result::Result<Arc<CachedHead>, CoreError>> {
        &self.refresh
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecordState> {
        // A critical section under this lock compares heads and calls the
        // head-state cache, which doesn't call back into the record, so the
        // lock isn't poisoned by anything that the record's own code doesn't
        // control.
        self.state
            .lock()
            .expect("the head record lock should not be poisoned")
    }
}

/// The number of head records the map keeps before its first scan for
/// records that nothing references.
const MIN_RECORDS_BEFORE_COMPACTING: usize = 64;

/// Returns `candidate` if it was taken in a later millisecond than `cached`.
/// Two observations in the same millisecond keep `cached`, so a tie never
/// moves a confirmation.
fn later(cached: &Observation, candidate: Observation) -> Option<Observation> {
    (cached.age_at(&candidate) > 0).then_some(candidate)
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
    Head(Arc<CachedHead>),
    Tail(Arc<ProjectedWalTail>),
}

impl HeadStateEntry {
    fn into_head(self) -> Option<Arc<CachedHead>> {
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
            Self::Head(head) => arc_bytes::<CachedHead>() + head.heap_bytes(),
            Self::Tail(tail) => tail.decoded_bytes(),
        }
    }
}

/// Namespace heads and WAL-tail projections for every scope, under one
/// byte budget and one recency order.
pub struct SharedHeadState {
    entries: DecodedBlockCache<(CacheScope, HeadStateKey), HeadStateEntry>,
    max_decoded_bytes: usize,
    counters: HeadStateCounters,
    records: std::sync::Mutex<HeadRecords>,
}

/// The live [`HeadRecord`]s of every scope.
#[derive(Debug, Default)]
struct HeadRecords {
    records: HashMap<(CacheScope, NamespaceId), Weak<HeadRecord>>,
    /// The map's length at which the next lookup drops the records that
    /// nothing references. It doubles with the live records, so the scan
    /// costs a constant amount per lookup on average.
    compact_at: usize,
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
    pub fn new(max_decoded_bytes: usize) -> Self {
        Self {
            entries: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_decoded_bytes)),
            max_decoded_bytes,
            counters: HeadStateCounters::default(),
            records: std::sync::Mutex::default(),
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
            Arc::new(SharedHeadState::new(max_decoded_bytes)),
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

    /// Returns the namespace's head record, creating it when nothing
    /// references one.
    pub fn record(&self, namespace_id: &NamespaceId) -> Arc<HeadRecord> {
        // A critical section under this lock only edits a map, so it can't
        // panic and poison the lock.
        let mut records = self
            .shared
            .records
            .lock()
            .expect("the head record map lock should not be poisoned");
        let key = (self.scope, namespace_id.clone());
        if let Some(record) = records.records.get(&key).and_then(Weak::upgrade) {
            return record;
        }
        if records.records.len() >= records.compact_at {
            records
                .records
                .retain(|_, record| record.strong_count() > 0);
            records.compact_at = (2 * records.records.len()).max(MIN_RECORDS_BEFORE_COMPACTING);
        }
        let record = Arc::new(HeadRecord::default());
        records.records.insert(key, Arc::downgrade(&record));
        record
    }

    /// Returns the namespace's cached head and marks it recently used.
    pub fn get_head(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedHead>> {
        self.shared
            .entries
            .get(&self.head_key(namespace_id))
            .and_then(HeadStateEntry::into_head)
    }

    /// Installs `candidate` as the namespace's cached head if it's newer than
    /// the current head, and returns the head that the read starts from.
    ///
    /// `started` is the record's generation when the load of `candidate`
    /// began. If [`Self::invalidate_head`] dropped the head since, the load
    /// might predate the change that the drop is for, so nothing is
    /// installed, and the read starts from the candidate. The next read then
    /// loads the head by discovery.
    ///
    /// The current head is the higher of the cached head and the floor of
    /// `record`, the newest head that the record installed or saw, which
    /// stays after the cache evicts that head. A head is newer when
    /// its sequence is higher, or when its sequence is the same and its basis
    /// is a later manifest.
    ///
    /// - A candidate below the current head changes nothing. The read starts
    ///   from the cached head, or from the candidate when the cache evicted
    ///   the head the floor came from.
    /// - A candidate equal to the cached head takes the later of each
    ///   confirmation, because confirmations only move forward. A new
    ///   snapshot is cached only when a confirmation moved.
    /// - A newer candidate becomes the cached head.
    ///
    /// A snapshot too large for the cache isn't cached, and the smaller head
    /// it replaces is dropped with it, so an eventual read misses and
    /// confirms through the record. The read starts from the candidate, and
    /// the floor still moves, so the head that reads start from never moves
    /// backwards while the record lives.
    ///
    /// Every install and drop of a namespace's head runs under its record's
    /// lock, so the head's entry in the cache can't change between this
    /// function's peek and its insert, except by an eviction.
    pub fn replace_head(
        &self,
        record: &HeadRecord,
        started: u64,
        candidate: CachedHead,
    ) -> Arc<CachedHead> {
        let key = self.head_key(&candidate.head.namespace_id);
        let mut state = record.lock();
        if state.generation != started {
            return Arc::new(candidate);
        }
        let cached = self
            .shared
            .entries
            .peek(&key)
            .and_then(HeadStateEntry::into_head);
        let current = state
            .floor
            .max(cached.as_ref().map(|cached| cached.order()));
        state.floor = current;
        let next = match current.map(|current| candidate.order().cmp(&current)) {
            Some(std::cmp::Ordering::Less) => {
                return match cached {
                    Some(cached) if cached.order() == current.expect("a current head") => cached,
                    _ => Arc::new(candidate),
                };
            }
            Some(std::cmp::Ordering::Equal) => match &cached {
                Some(cached) if cached.order() == candidate.order() => {
                    let basis_checked = later(&cached.basis_checked, candidate.basis_checked);
                    let head_confirmed = later(&cached.head_confirmed, candidate.head_confirmed);
                    if basis_checked.is_none() && head_confirmed.is_none() {
                        return Arc::clone(cached);
                    }
                    CachedHead {
                        basis_checked: basis_checked
                            .unwrap_or_else(|| cached.basis_checked.clone()),
                        head_confirmed: head_confirmed
                            .unwrap_or_else(|| cached.head_confirmed.clone()),
                        ..CachedHead::clone(cached)
                    }
                }
                _ => candidate,
            },
            Some(std::cmp::Ordering::Greater) | None => candidate,
        };
        state.floor = Some(next.order());
        let next = Arc::new(next);
        let installed = self
            .shared
            .entries
            .insert(key.clone(), HeadStateEntry::Head(Arc::clone(&next)));
        if !installed {
            // The cache didn't keep the new head, because its snapshot is too
            // large for the budget or the cache is disabled. The cached head
            // is older, and an eventual read doesn't consult the record, so
            // the older head mustn't stay.
            self.shared.entries.invalidate(&key);
        }
        next
    }

    /// Removes the namespace's cached head, after the runtime learns of a
    /// change whose effect on the head it can't identify, so the next read
    /// loads the head by discovery.
    ///
    /// The drop moves the record's generation, so a load that started before
    /// the drop installs nothing when it finishes (see
    /// [`Self::replace_head`]). A load in flight keeps the record, so the
    /// generation it took is still on the record that the drop moves.
    pub fn invalidate_head(&self, namespace_id: &NamespaceId) {
        let record = self.record(namespace_id);
        let mut state = record.lock();
        state.generation += 1;
        self.shared.entries.invalidate(&self.head_key(namespace_id));
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
        tail
    }

    pub fn insert_tail(&self, key: WalTailProjectionCacheKey, tail: Arc<ProjectedWalTail>) {
        let shared = &self.shared;
        if shared.entries.insert(
            (self.scope, HeadStateKey::Tail(key)),
            HeadStateEntry::Tail(tail),
        ) {
            shared.counters.tail_inserts.fetch_add(1, Ordering::Relaxed);
        }
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
        CachedHead, DecodedMetadataSegmentBlock, HeadStateCache, MetadataBasis,
        MetadataSegmentCache, MetadataSegmentCacheKey, NamespaceReadState, ObjectStoreScope,
        Observation, SharedSegmentBlocks, WalTailProjectionCacheKey,
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
        let shared = Arc::new(SharedSegmentBlocks::new(usize::MAX));
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
            basis_checked: checked.clone(),
            head_confirmed: checked.clone(),
        }
    }

    fn order(head: &CachedHead) -> (u64, u64) {
        (head.head.seq.0, head.basis.manifest_no().0)
    }

    /// Creates a head like [`head_at`] whose namespace grants access to
    /// `principals` principals, each with a 60-byte ID.
    fn head_with_grants(seq: u64, principals: usize, checked: &Observation) -> CachedHead {
        let grants = (0..principals)
            .map(|index| {
                (
                    loonfs_types::PrincipalId::parse(format!("prn_{index:056}"))
                        .expect("principal"),
                    loonfs_types::AccessRights::from_iter([loonfs_types::AccessRight::Read]),
                )
            })
            .collect();
        let mut head = head_at(seq, 1, checked);
        head.head.access = loonfs_types::NamespaceAccess::Acl {
            principal_scope: loonfs_types::PrincipalScope::parse("org").expect("scope"),
            root_grants: loonfs_types::AccessGrants::new(grants).expect("grants"),
        };
        head
    }

    /// Checks that a candidate at a lower head sequence, or at the same
    /// sequence against an earlier manifest, doesn't replace the cached
    /// head, and that the read starts from the cached head.
    #[test]
    fn a_lower_head_does_not_replace_the_cached_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(&record, record.generation(), head_at(5, 2, &now));
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(4, 3, &now))),
            (5, 2)
        );
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(5, 1, &now))),
            (5, 2)
        );
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert_eq!(order(&cached), (5, 2));
    }

    /// Checks that a candidate at a higher head sequence, or at the same
    /// sequence against a later manifest, becomes the cached head.
    #[test]
    fn a_higher_head_or_a_later_manifest_replaces_the_cached_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(5, 1, &now))),
            (5, 1)
        );
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(5, 2, &now))),
            (5, 2)
        );
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(6, 1, &now))),
            (6, 1)
        );
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert_eq!(order(&cached), (6, 1));
    }

    /// Checks that a candidate equal to the cached head takes the newer of
    /// each confirmation into a new snapshot, and that a candidate with no
    /// newer confirmation leaves the cached snapshot in place.
    #[test]
    fn an_equal_head_keeps_the_newer_confirmations() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let early = Observation::now(clock.clone());
        clock.advance_ms(10);
        let late = Observation::now(clock.clone());
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(
            &record,
            record.generation(),
            CachedHead {
                head_confirmed: late.clone(),
                ..head_at(5, 1, &early)
            },
        );
        let merged = cache.replace_head(
            &record,
            record.generation(),
            CachedHead {
                basis_checked: late.clone(),
                ..head_at(5, 1, &early)
            },
        );
        assert_eq!(merged.basis_checked.age_ms(), 0);
        assert_eq!(merged.head_confirmed.age_ms(), 0);
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &merged));
        let unchanged = cache.replace_head(&record, record.generation(), head_at(5, 1, &early));
        assert!(Arc::ptr_eq(&unchanged, &merged));
        let same_millisecond =
            cache.replace_head(&record, record.generation(), head_at(5, 1, &late));
        assert!(
            Arc::ptr_eq(&same_millisecond, &merged),
            "a confirmation in the same millisecond doesn't move"
        );
    }

    /// Checks the record's floor while the cache has evicted or dropped the
    /// head it came from: a candidate below the floor isn't cached and the
    /// read starts from the candidate, an equal candidate is cached, and a
    /// higher one is cached and moves the floor.
    #[test]
    fn the_record_keeps_out_a_head_below_an_evicted_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(&record, record.generation(), head_at(5, 2, &now));
        cache.invalidate_head(&namespace("demo"));
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(4, 3, &now))),
            (4, 3)
        );
        assert!(cache.get_head(&namespace("demo")).is_none());
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(5, 2, &now))),
            (5, 2)
        );
        assert_eq!(
            cache
                .get_head(&namespace("demo"))
                .map(|cached| order(&cached)),
            Some((5, 2))
        );
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(6, 1, &now))),
            (6, 1)
        );
        cache.invalidate_head(&namespace("demo"));
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(5, 9, &now))),
            (5, 9)
        );
        assert!(cache.get_head(&namespace("demo")).is_none());
    }

    /// Checks that a load that started before the namespace's head was
    /// dropped installs nothing, and that its read starts from the load's
    /// own head, while a load that started after the drop installs its head.
    #[test]
    fn a_load_that_started_before_a_drop_installs_nothing() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(&record, record.generation(), head_at(5, 1, &now));
        let started = record.generation();
        cache.invalidate_head(&namespace("demo"));
        let head = cache.replace_head(&record, started, head_at(6, 1, &now));
        assert_eq!(order(&head), (6, 1));
        assert!(cache.get_head(&namespace("demo")).is_none());
        let head = cache.replace_head(&record, record.generation(), head_at(6, 1, &now));
        assert_eq!(order(&head), (6, 1));
        assert!(cache.get_head(&namespace("demo")).is_some());
    }

    /// Checks that a namespace head larger than the whole head-state budget
    /// isn't cached, that the cache keeps only the bytes it weighed, and
    /// that nothing keeps the head once the read that loaded it is done.
    #[test]
    fn a_head_larger_than_the_budget_is_not_cached() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(4096);
        let record = cache.record(&namespace("demo"));
        let large = head_with_grants(5, 1_000, &now);
        assert!(large.heap_bytes() > 16 * 4096);
        let head = cache.replace_head(&record, record.generation(), large);
        assert_eq!(order(&head), (5, 1));
        assert!(cache.get_head(&namespace("demo")).is_none());
        let stats = cache.stats();
        assert_eq!(stats.rejections, 1);
        assert!(stats.cached_decoded_bytes <= 4096, "{stats:?}");
        let weak = Arc::downgrade(&head);
        drop(head);
        assert!(weak.upgrade().is_none(), "nothing keeps the head");
    }

    /// Checks that a newer head too large for the cache drops the smaller
    /// head it replaces: the read starts from the newer head, the cache has
    /// no head for the namespace afterwards, the cache records the refusal
    /// as a rejection, and the record's floor is at the newer head.
    #[test]
    fn a_newer_head_too_large_to_cache_drops_the_older_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(4096);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(&record, record.generation(), head_at(0, 1, &now));
        assert!(cache.get_head(&namespace("demo")).is_some());
        let rejections = cache.stats().rejections;
        let large = head_with_grants(1, 1_000, &now);
        assert!(large.heap_bytes() > 4096);
        let head = cache.replace_head(&record, record.generation(), large);
        assert_eq!(order(&head), (1, 1));
        assert!(cache.get_head(&namespace("demo")).is_none());
        assert_eq!(cache.stats().rejections, rejections + 1);
        assert_eq!(record.lock().floor, Some((ChangeSeq(1), ManifestNo(1))));
    }

    /// Checks that a head that grows within the budget is charged its new
    /// weight when the larger snapshot replaces the smaller one.
    #[test]
    fn a_larger_head_is_charged_its_weight() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        let small = cache.replace_head(&record, record.generation(), head_at(5, 1, &now));
        let before = cache.stats().cached_decoded_bytes;
        let large =
            cache.replace_head(&record, record.generation(), head_with_grants(6, 100, &now));
        let after = cache.stats().cached_decoded_bytes;
        assert!(
            after >= before + large.heap_bytes() - small.heap_bytes(),
            "{before} bytes before, {after} after"
        );
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &large));
    }

    /// Checks that the record map drops a record that nothing references, and
    /// that the next record of an idle namespace starts with no floor.
    #[test]
    fn an_idle_namespace_gets_a_fresh_record() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let record = cache.record(&namespace("demo"));
        cache.replace_head(&record, record.generation(), head_at(5, 1, &now));
        cache.invalidate_head(&namespace("demo"));
        drop(record);
        let record = cache.record(&namespace("demo"));
        assert_eq!(
            order(&cache.replace_head(&record, record.generation(), head_at(4, 1, &now))),
            (4, 1)
        );
        assert!(cache.get_head(&namespace("demo")).is_some());
        drop(record);
        for index in 0..200 {
            drop(cache.record(&namespace(&format!("idle-{index}"))));
        }
        let records = cache.shared.records.lock().expect("records").records.len();
        assert!(records <= 2 * 64, "{records} records");
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
