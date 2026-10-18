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
    CheckStart, DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey, DecodedSegmentBlock, SegmentCacheKey,
};
use crate::error::CoreError;
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::state::NamespaceReadState;
use crate::read_working_memory::ReadWorkingMemory;
use crate::time::Observation;
use crate::wal::ProjectedWalTail;
use loonfs_types::format::manifest::MetadataRow;
use loonfs_types::format::manifest::NamespaceManifestEnvelope;
use loonfs_types::{ChangeSeq, ManifestNo, NamespaceId};
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// Counters and occupancy of the shared segment blocks.
///
/// The first fields are the counters that every cache reports (see
/// [`DecodedBlockCacheStats`](crate::block_cache::DecodedBlockCacheStats)),
/// and the rest count what the segment filters did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataSegmentCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    pub cached_decoded_bytes: usize,
    /// Segments a scan skipped because their bloom filter ruled the lookup
    /// key out before any index or data fetch.
    pub filter_skips: usize,
    /// Segments whose filter admitted a lookup that then matched no rows.
    /// Approximate: a lookup narrower than the filter key (an exact unbind,
    /// a single revision) can count a true admission here.
    pub filter_false_positives: usize,
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
            evicted_decoded_bytes: blocks.evicted_decoded_bytes,
            rejections: blocks.rejections,
            rejected_decoded_bytes: blocks.rejected_decoded_bytes,
            cached_decoded_bytes: blocks.cached_decoded_bytes,
            filter_skips: self.filter_stats.filter_skips.load(Ordering::Relaxed),
            filter_false_positives: self
                .filter_stats
                .filter_false_positives
                .load(Ordering::Relaxed),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailProjectionCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub head_seq: ChangeSeq,
}

impl DecodedBlockKey for WalTailProjectionCacheKey {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(&self.namespace_id)
    }
}

impl DecodedBlockKey for NamespaceId {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(self)
    }
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
    /// The durable order that [`HeadStateCache::replace_head`] compares heads
    /// in: head sequence, then manifest number.
    fn order(&self) -> (ChangeSeq, ManifestNo) {
        (self.head.seq, self.basis.manifest_no())
    }

    /// Returns whether this head replaces `cached`: whether it's newer in the
    /// durable order, or the same head with a later confirmation and no
    /// earlier one.
    fn supersedes(&self, cached: &CachedHead) -> bool {
        match self.order().cmp(&cached.order()) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                let checks = [
                    (&cached.basis_checked, &self.basis_checked),
                    (&cached.head_confirmed, &self.head_confirmed),
                ];
                checks.iter().all(|(cached, own)| own.age_at(cached) == 0)
                    && checks.iter().any(|(cached, own)| cached.age_at(own) > 0)
            }
        }
    }

    fn heap_bytes(&self) -> usize {
        self.head.heap_bytes() + self.basis.0.heap_bytes()
    }
}

impl DecodedBlock for Arc<CachedHead> {
    fn weight(&self) -> usize {
        arc_bytes::<CachedHead>() + CachedHead::heap_bytes(self)
    }
}

impl DecodedBlock for Arc<ProjectedWalTail> {
    fn weight(&self) -> usize {
        ProjectedWalTail::decoded_bytes(self)
    }
}

/// Returns `candidate` if it was taken in a later millisecond than `cached`.
/// Two observations in the same millisecond keep `cached`, so a tie never
/// moves a confirmation.
fn later(cached: &Observation, candidate: Observation) -> Option<Observation> {
    (cached.age_at(&candidate) > 0).then_some(candidate)
}

/// Namespace heads and WAL-tail projections for every scope, each kind
/// under its own byte budget and recency order, so a tail can't evict a
/// head.
pub struct SharedHeadState {
    heads: DecodedBlockCache<(CacheScope, NamespaceId), Arc<CachedHead>>,
    tails: DecodedBlockCache<(CacheScope, WalTailProjectionCacheKey), Arc<ProjectedWalTail>>,
}

impl std::fmt::Debug for SharedHeadState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedHeadState")
            .field("head_stats", &self.head_stats())
            .field("tail_stats", &self.tail_stats())
            .finish_non_exhaustive()
    }
}

impl SharedHeadState {
    /// Returns head state that keeps up to `max_head_bytes` of namespace
    /// heads and up to `max_tail_bytes` of WAL-tail projections.
    pub fn new(max_head_bytes: usize, max_tail_bytes: usize) -> Self {
        Self {
            heads: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_head_bytes)),
            tails: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_tail_bytes)),
        }
    }

    /// Returns the counters and occupancy of the namespace heads.
    pub fn head_stats(&self) -> DecodedBlockCacheStats {
        self.heads.stats()
    }

    /// Returns the counters and occupancy of the WAL-tail projections.
    pub fn tail_stats(&self) -> DecodedBlockCacheStats {
        self.tails.stats()
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

    /// A view over head state no other view reads, with a budget of
    /// `max_decoded_bytes` for heads and another for tails.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedHeadState::new(max_decoded_bytes, max_decoded_bytes)),
            CacheScope::new(0),
        )
    }

    /// Counters of the namespace heads in the whole shared cache, across
    /// every scope.
    pub fn head_stats(&self) -> DecodedBlockCacheStats {
        self.shared.head_stats()
    }

    /// Counters of the WAL-tail projections in the whole shared cache,
    /// across every scope.
    pub fn tail_stats(&self) -> DecodedBlockCacheStats {
        self.shared.tail_stats()
    }

    fn head_key(&self, namespace_id: &NamespaceId) -> (CacheScope, NamespaceId) {
        (self.scope, namespace_id.clone())
    }

    fn tail_key(&self, key: &WalTailProjectionCacheKey) -> (CacheScope, WalTailProjectionCacheKey) {
        (self.scope, key.clone())
    }

    /// Returns the namespace's cached head and marks it recently used.
    pub fn get_head(&self, namespace_id: &NamespaceId) -> Option<Arc<CachedHead>> {
        self.shared.heads.get(&self.head_key(namespace_id))
    }

    /// Returns `current()`, or the head that a check of the namespace's head
    /// returns, which the concurrent reads of the namespace share.
    ///
    /// A read waits for the check in flight, and then takes `current()` when
    /// it's `Some`, or else the outcome of a check that started after the
    /// read arrived, or else runs `check` (see [`SharedCheck`]). So a wave of
    /// concurrent reads costs at most two checks. The head that `check`
    /// returns is installed through [`Self::replace_head`], and the reads
    /// start from the head that it returns.
    ///
    /// If [`Self::invalidate_head`] dropped the head while the check ran, the
    /// check might predate the change that the drop is for. If an eviction
    /// dropped it, the evicted head might be newer than the check's. Either
    /// way, nothing is installed, the reads start from the check's head, and
    /// the next read loads the head afresh.
    ///
    /// [`SharedCheck`]: crate::cache::SharedCheck
    pub async fn check_or_join_head<F, Fut>(
        &self,
        namespace_id: &NamespaceId,
        current: impl FnOnce() -> Option<Arc<CachedHead>>,
        check: F,
    ) -> std::result::Result<Arc<CachedHead>, CoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<CachedHead, CoreError>>,
    {
        self.shared
            .heads
            .check_or_join(&self.head_key(namespace_id), current, |start| async move {
                let candidate = check().await?;
                Ok(self.install_head(Some(start), candidate))
            })
            .await
    }

    /// Installs `candidate` as the namespace's cached head if it's newer than
    /// the cached head, and returns the head that a read starts from.
    ///
    /// A head is newer when its sequence is higher, or when its sequence is
    /// the same and its basis is a later manifest.
    ///
    /// - A candidate below the cached head changes nothing, and the read
    ///   starts from the cached head.
    /// - A candidate equal to the cached head takes the later of each
    ///   confirmation, because confirmations only move forward. A new
    ///   snapshot is cached only when a confirmation moved.
    /// - A newer candidate becomes the cached head.
    ///
    /// The cache compares the heads and inserts under one lock, so a head
    /// never replaces a newer one, whatever order the installs run in.
    ///
    /// A snapshot too large for the cache isn't cached, and the smaller head
    /// it replaces is dropped with it, so an eventual read misses and
    /// confirms the head with a check. The read starts from the candidate.
    pub fn replace_head(&self, candidate: CachedHead) -> Arc<CachedHead> {
        self.install_head(None, candidate)
    }

    /// Installs `candidate` as [`Self::replace_head`] does, unless an
    /// invalidation or an eviction of the head passed `start`, the start of
    /// the check that loaded `candidate`.
    fn install_head(&self, start: Option<CheckStart>, candidate: CachedHead) -> Arc<CachedHead> {
        let key = self.head_key(&candidate.head.namespace_id);
        let next = match self.shared.heads.peek(&key) {
            Some(cached) if cached.order() == candidate.order() => {
                let basis_checked = later(&cached.basis_checked, candidate.basis_checked);
                let head_confirmed = later(&cached.head_confirmed, candidate.head_confirmed);
                if basis_checked.is_none() && head_confirmed.is_none() {
                    return cached;
                }
                CachedHead {
                    basis_checked: basis_checked.unwrap_or_else(|| cached.basis_checked.clone()),
                    head_confirmed: head_confirmed.unwrap_or_else(|| cached.head_confirmed.clone()),
                    ..CachedHead::clone(&cached)
                }
            }
            _ => candidate,
        };
        let next = Arc::new(next);
        self.shared
            .heads
            .insert_if(key, Arc::clone(&next), start, |cached| {
                next.supersedes(cached)
            })
            .unwrap_or(next)
    }

    /// Removes the namespace's cached head, after the runtime learns of a
    /// change whose effect on the head it can't identify, so the next read
    /// loads the head by discovery.
    ///
    /// A check of the head in flight installs nothing when it finishes (see
    /// [`Self::check_or_join_head`]).
    pub fn invalidate_head(&self, namespace_id: &NamespaceId) {
        self.shared.heads.invalidate(&self.head_key(namespace_id));
    }

    pub fn get_tail(&self, key: &WalTailProjectionCacheKey) -> Option<Arc<ProjectedWalTail>> {
        self.shared.tails.get(&self.tail_key(key))
    }

    /// Returns the WAL-tail projection under `key`, or loads it with `load`
    /// and caches it.
    ///
    /// Concurrent calls for one key share one load, so the reads at one head
    /// that miss its tail replay the tail once.
    pub(crate) async fn get_or_load_tail<E, F, Fut>(
        &self,
        key: &WalTailProjectionCacheKey,
        load: F,
    ) -> Result<Arc<ProjectedWalTail>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<ProjectedWalTail>, E>>,
    {
        self.shared
            .tails
            .get_or_load(&self.tail_key(key), load)
            .await
    }

    pub fn insert_tail(&self, key: WalTailProjectionCacheKey, tail: Arc<ProjectedWalTail>) {
        self.shared.tails.insert((self.scope, key), tail);
    }

    /// Returns the budget needed to cache this WAL-tail projection on its own.
    #[cfg(any(test, feature = "test-support"))]
    pub fn tail_charged_bytes(
        &self,
        key: &WalTailProjectionCacheKey,
        tail: &Arc<ProjectedWalTail>,
    ) -> usize {
        crate::block_cache::charged_bytes(&self.tail_key(key), tail)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheScope, CachedHead, CoreError, DecodedMetadataSegmentBlock, HeadStateCache,
        MetadataBasis, MetadataSegmentCache, MetadataSegmentCacheKey, NamespaceReadState,
        ObjectStoreScope, Observation, SharedSegmentBlocks, WalTailProjectionCacheKey,
    };
    use crate::block_cache::{charged_bytes, DecodedBlock, SegmentBlockKind};
    use crate::metadata::{InodeRecord, MetadataState};
    use crate::wal::ProjectedWalTail;
    use futures::poll;
    use loonfs_types::format::control::{ManifestRef, NamespaceStatus};
    use loonfs_types::format::segment_blocks::DecodedDataBlock;
    use loonfs_types::{ActorId, ChangeSeq, InodeId, InodeKind, ManifestNo, NamespaceId};
    use loonfs_types::{WalNo, WriterEpoch};
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tokio::sync::watch;

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
        head_of("demo", seq, manifest_no, checked)
    }

    /// Creates a head like [`head_at`] for namespace `name`.
    fn head_of(name: &str, seq: u64, manifest_no: u64, checked: &Observation) -> CachedHead {
        let namespace_id = namespace(name);
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

    /// Creates a check of a head that waits until `released` is true, and
    /// then returns `head`.
    async fn check_after(
        released: &watch::Receiver<bool>,
        head: CachedHead,
    ) -> Result<CachedHead, CoreError> {
        released
            .clone()
            .wait_for(|released| *released)
            .await
            .expect("the test keeps the sender");
        Ok(head)
    }

    /// Checks that a check whose head is lower than the cached head, at a
    /// lower head sequence or at the same sequence against an earlier
    /// manifest, doesn't replace the cached head, and that the read starts
    /// from the cached head.
    #[tokio::test]
    async fn a_lower_head_does_not_replace_the_cached_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        cache.replace_head(head_at(5, 2, &now));
        for lower in [head_at(4, 3, &now), head_at(5, 1, &now)] {
            let read = cache
                .check_or_join_head(&namespace("demo"), || None, || async { Ok(lower) })
                .await
                .expect("the check's head");
            assert_eq!(order(&read), (5, 2));
        }
        assert_eq!(order(&cache.replace_head(head_at(4, 3, &now))), (5, 2));
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert_eq!(order(&cached), (5, 2));
    }

    /// Checks that a candidate at a higher head sequence, or at the same
    /// sequence against a later manifest, becomes the cached head.
    #[test]
    fn a_higher_head_or_a_later_manifest_replaces_the_cached_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        for (seq, manifest_no) in [(5, 1), (5, 2), (6, 1)] {
            let installed = cache.replace_head(head_at(seq, manifest_no, &now));
            assert_eq!(order(&installed), (seq, manifest_no));
        }
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
        cache.replace_head(CachedHead {
            head_confirmed: late.clone(),
            ..head_at(5, 1, &early)
        });
        let merged = cache.replace_head(CachedHead {
            basis_checked: late.clone(),
            ..head_at(5, 1, &early)
        });
        assert_eq!(merged.basis_checked.age_ms(), 0);
        assert_eq!(merged.head_confirmed.age_ms(), 0);
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &merged));
        let unchanged = cache.replace_head(head_at(5, 1, &early));
        assert!(Arc::ptr_eq(&unchanged, &merged));
        let same_millisecond = cache.replace_head(head_at(5, 1, &late));
        assert!(
            Arc::ptr_eq(&same_millisecond, &merged),
            "a confirmation in the same millisecond doesn't move"
        );
    }

    /// Checks that a check during which the head is invalidated, or during
    /// which a newer head is installed and then evicted, installs nothing,
    /// that the read starts from the check's own head, and that the next
    /// check installs its head.
    #[tokio::test]
    async fn a_check_whose_head_is_dropped_while_it_runs_installs_nothing() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let budget = charged_bytes(
            &(CacheScope::new(0), demo.clone()),
            &Arc::new(head_at(7, 1, &now)),
        );
        let invalidate = |cache: &HeadStateCache| cache.invalidate_head(&demo);
        let evict = |cache: &HeadStateCache| {
            cache.replace_head(head_at(7, 1, &now));
            assert!(cache.get_head(&demo).is_some(), "the newer head fits");
            // A head of another namespace, charged the same, takes its place.
            cache.replace_head(head_of("omed", 7, 1, &now));
            assert!(cache.get_head(&demo).is_none(), "the newer head is evicted");
        };
        for drop_head in [&invalidate as &dyn Fn(&HeadStateCache), &evict] {
            let cache = HeadStateCache::unshared(budget);
            let (release, released) = watch::channel(false);
            let mut read = pin!(cache.check_or_join_head(
                &demo,
                || None,
                || { check_after(&released, head_at(6, 1, &now)) }
            ));
            assert!(poll!(read.as_mut()).is_pending());
            drop_head(&cache);
            release.send(true).expect("the check waits for the release");
            let read = read.await.expect("the check's head");
            assert_eq!(
                order(&read),
                (6, 1),
                "the read starts from the check's head"
            );
            assert!(
                cache.get_head(&demo).is_none(),
                "the check installs nothing"
            );
            cache
                .check_or_join_head(&demo, || None, || async { Ok(head_at(6, 1, &now)) })
                .await
                .expect("the next check's head");
            let cached = cache
                .get_head(&demo)
                .expect("the next check installs its head");
            assert_eq!(order(&cached), (6, 1));
        }
    }

    /// Checks that the reads of one head that arrive while a check runs
    /// share one more check, that a read of another namespace runs its own
    /// check alongside, and that the heads keep no in-flight entry once the
    /// reads are done.
    #[tokio::test]
    async fn a_wave_of_reads_of_one_head_shares_at_most_two_checks() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let checks = AtomicUsize::new(0);
        let (release, released) = watch::channel(false);
        let read = |name: &'static str, seq| {
            let (cache, checks, released, now) = (&cache, &checks, &released, &now);
            async move {
                cache
                    .check_or_join_head(
                        &namespace(name),
                        || None,
                        || {
                            checks.fetch_add(1, AtomicOrdering::SeqCst);
                            check_after(released, head_of(name, seq, 1, now))
                        },
                    )
                    .await
                    .expect("the check's head")
            }
        };
        let mut first = pin!(read("demo", 5));
        assert!(poll!(first.as_mut()).is_pending());
        let mut wave = pin!(futures::future::join_all((0..4).map(|_| read("demo", 6))));
        assert!(poll!(wave.as_mut()).is_pending());
        let mut other = pin!(read("omed", 1));
        assert!(poll!(other.as_mut()).is_pending());
        assert_eq!(
            checks.load(AtomicOrdering::SeqCst),
            2,
            "the other namespace's check runs alongside the first"
        );
        release.send(true).expect("the checks wait for the release");
        let (first, wave, other) = futures::join!(first, wave, other);
        assert_eq!(order(&first), (5, 1));
        let wave: Vec<_> = wave.iter().map(|head| order(head)).collect();
        assert_eq!(wave, [(6, 1); 4], "the wave shares the second check");
        assert_eq!(order(&other), (1, 1));
        assert_eq!(checks.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(cache.shared.heads.in_flight_len(), 0);
    }

    /// Checks that WAL-tail projections, from small ones to ones heavier
    /// than the tail budget, evict no namespace head while they evict each
    /// other.
    #[test]
    fn a_tail_of_any_size_evicts_no_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(4096);
        cache.replace_head(head_at(5, 1, &now));
        for (index, inodes) in [1, 4, 16, 64, 256].into_iter().cycle().take(20).enumerate() {
            let rows = (0..inodes)
                .map(|inode| InodeRecord {
                    inode_id: InodeId(inode),
                    inode_kind: InodeKind::File,
                    committed_seq: ChangeSeq(3),
                    commit_id: loonfs_types::CommitId::parse("c_cache_row").expect("commit id"),
                    committed_by: loonfs_test_support::test_actor(),
                    committed_at_ms: 3_000,
                })
                .collect();
            let rows = MetadataState::from_rows(
                rows,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
            let key = WalTailProjectionCacheKey {
                namespace_id: namespace("demo"),
                manifest_no: ManifestNo(index as u64),
                head_seq: ChangeSeq(5),
            };
            cache.insert_tail(key, Arc::new(ProjectedWalTail::from_rows(rows)));
        }
        let (heads, tails) = (cache.head_stats(), cache.tail_stats());
        assert!(tails.evictions > 0 && tails.rejections > 0, "{tails:?}");
        assert_eq!(heads.evictions, 0, "{heads:?}");
        assert!(cache.get_head(&namespace("demo")).is_some());
    }

    /// Checks that a namespace head larger than the whole head budget isn't
    /// cached, that the cache keeps only the bytes it weighed, and that
    /// nothing keeps the head once the read that loaded it is done.
    #[test]
    fn a_head_larger_than_the_budget_is_not_cached() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(4096);
        let large = head_with_grants(5, 1_000, &now);
        assert!(large.heap_bytes() > 16 * 4096);
        let head = cache.replace_head(large);
        assert_eq!(order(&head), (5, 1));
        assert!(cache.get_head(&namespace("demo")).is_none());
        let stats = cache.head_stats();
        assert_eq!(stats.rejections, 1);
        assert!(stats.cached_decoded_bytes <= 4096, "{stats:?}");
        let weak = Arc::downgrade(&head);
        drop(head);
        assert!(weak.upgrade().is_none(), "nothing keeps the head");
    }

    /// Checks that a newer head too large for the cache drops the smaller
    /// head it replaces: the read starts from the newer head, the cache has
    /// no head for the namespace afterwards, the cache records the refusal
    /// as a rejection, and a check that was in flight installs its older
    /// head nowhere afterwards.
    #[tokio::test]
    async fn a_newer_head_too_large_to_cache_drops_the_older_head() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let cache = HeadStateCache::unshared(4096);
        cache.replace_head(head_at(0, 1, &now));
        assert!(cache.get_head(&demo).is_some());
        let rejections = cache.head_stats().rejections;
        let (release, released) = watch::channel(false);
        let mut check = pin!(cache.check_or_join_head(
            &demo,
            || None,
            || { check_after(&released, head_at(0, 2, &now)) }
        ));
        assert!(poll!(check.as_mut()).is_pending());
        let large = head_with_grants(1, 1_000, &now);
        assert!(large.heap_bytes() > 4096);
        let head = cache.replace_head(large);
        assert_eq!(order(&head), (1, 1));
        assert!(cache.get_head(&demo).is_none());
        assert_eq!(cache.head_stats().rejections, rejections + 1);
        release.send(true).expect("the check waits for the release");
        assert_eq!(order(&check.await.expect("the check's head")), (0, 2));
        assert!(
            cache.get_head(&demo).is_none(),
            "a check that started before the newer head installs nothing"
        );
    }

    /// Checks that a head that grows within the budget is charged its new
    /// weight when the larger snapshot replaces the smaller one.
    #[test]
    fn a_larger_head_is_charged_its_weight() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(1 << 20);
        let small = cache.replace_head(head_at(5, 1, &now));
        let before = cache.head_stats().cached_decoded_bytes;
        let large = cache.replace_head(head_with_grants(6, 100, &now));
        let after = cache.head_stats().cached_decoded_bytes;
        assert!(
            after >= before + large.heap_bytes() - small.heap_bytes(),
            "{before} bytes before, {after} after"
        );
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &large));
    }

    /// Checks that a zero budget keeps no head and no tail and records no
    /// lookups, and that a tail load still returns the tail it loaded.
    #[tokio::test]
    async fn a_zero_budget_keeps_and_counts_nothing() {
        let now = crate::test_support::observed_now();
        let cache = HeadStateCache::unshared(0);
        cache.replace_head(head_at(5, 1, &now));
        assert!(cache.get_head(&namespace("demo")).is_none());

        let key = WalTailProjectionCacheKey {
            namespace_id: namespace("demo"),
            manifest_no: ManifestNo(1),
            head_seq: ChangeSeq(5),
        };
        let tail = Arc::new(ProjectedWalTail::default());
        cache.insert_tail(key.clone(), Arc::clone(&tail));
        assert!(cache.get_tail(&key).is_none());
        let loaded = cache
            .get_or_load_tail(&key, || async { Ok::<_, String>(Arc::clone(&tail)) })
            .await
            .expect("the load returns its tail");
        assert!(Arc::ptr_eq(&loaded, &tail));
        assert!(cache.get_tail(&key).is_none());
        assert_eq!((cache.head_stats(), cache.tail_stats()), Default::default());
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
