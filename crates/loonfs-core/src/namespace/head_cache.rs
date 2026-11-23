//! The shared cache of namespace heads and WAL-tail projections that reads
//! start from, and the per-runtime views that reads go through.
//!
//! A view adds the runtime's [`CacheScope`] to every key, so no runtime reads
//! the namespace heads or WAL-tail projections of another runtime. Eviction
//! runs across every scope.

use crate::block_cache::CacheScope;
use crate::block_cache::{
    CheckStart, DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey,
};
use crate::control_object::ControlObjectLoadError;
use crate::engine::RuntimeReadContext;
use crate::error::CoreError;
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::manifest::move_read_basis;
use crate::namespace::basis::ReadBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::{Checked, Held};
use crate::namespace::store::NamespaceStore;
use crate::time::Observation;
use crate::wal::probe_namespace_wal;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitSeq, ManifestNo, NamespaceId, WalNo};
use std::sync::Arc;

/// Identifies one projected WAL tail, the tail above a manifest that ends
/// at a head.
///
/// The head's WAL number is part of the key, because a WAL object with no
/// commits, such as a floor object, moves the head without changing its
/// sequence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WalTailCacheKey {
    pub namespace_id: NamespaceId,
    pub manifest_no: ManifestNo,
    pub head_seq: CommitSeq,
    pub wal_no: WalNo,
}

impl DecodedBlockKey for WalTailCacheKey {
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
    /// The head and the manifest it was read above, with the last check of
    /// that manifest.
    basis: Held<ReadBasis>,
    /// When the WAL probe last confirmed the head, or when the head was
    /// loaded. The observation is taken before the probe or the load is sent,
    /// so the head includes every commit acknowledged before it. An eventual
    /// read within its maximum staleness reads at the head without a request.
    head_confirmed: Observation,
}

impl CachedHead {
    /// Returns the cached head of `basis`, a head that a load or a WAL probe
    /// confirmed at `head_confirmed`.
    pub fn new(basis: Held<ReadBasis>, head_confirmed: Observation) -> Self {
        Self {
            basis,
            head_confirmed,
        }
    }

    /// Returns the head and the manifest it was read above, with the last
    /// check of that manifest.
    pub fn basis(&self) -> &Held<ReadBasis> {
        &self.basis
    }

    /// Returns when the WAL probe last confirmed the head, or when the head
    /// was loaded.
    pub fn head_confirmed(&self) -> &Observation {
        &self.head_confirmed
    }

    /// Returns the head and the manifest it was read above.
    pub fn read_basis(&self) -> &ReadBasis {
        self.basis.read_basis()
    }

    /// Checks whether the head's manifest is still the newest, as
    /// [`NamespaceStore::check_newest`] does, and keeps what the check
    /// learned.
    pub async fn check_manifest<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        observed: Observation,
    ) -> Result<Checked, CoreError> {
        namespace.check_newest(&mut self.basis, observed).await
    }

    /// Advances the head over the WAL objects after it, as
    /// [`probe_namespace_wal`] reads them, and returns `false` when the head
    /// needs a discovery instead. When the probe returns `true`, the head is
    /// confirmed at `observed`, an observation that the caller read before
    /// the probe.
    pub async fn probe_wal<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        head_and_wal_tail_cache: &HeadAndWalTailCache,
        observed: Observation,
    ) -> std::result::Result<bool, ControlObjectLoadError> {
        let mut context = RuntimeReadContext::new(&self.basis, None);
        if !probe_namespace_wal(store, head_and_wal_tail_cache, &mut context).await? {
            return Ok(false);
        }
        self.basis.read_basis_mut().head = context.into_read_basis().head;
        self.head_confirmed = observed;
        Ok(true)
    }

    /// The durable order that [`HeadAndWalTailCache::replace_head`] compares
    /// heads in: head sequence, then manifest number, then WAL number. A
    /// floor object moves the WAL number alone.
    fn order(&self) -> (CommitSeq, ManifestNo, WalNo) {
        (
            self.read_basis().head.seq,
            self.read_basis().basis.manifest_no(),
            self.read_basis().head.wal_no,
        )
    }

    /// Merges `other`, a head at the same place in the durable order, into
    /// this head, and returns whether this head changed. This head takes the
    /// younger check time and the higher boundary (see [`Held::merge`]) and
    /// the later head confirmation. Two observations in the same millisecond
    /// keep this head's confirmation, so a tie never moves a confirmation.
    fn merge(&mut self, other: &CachedHead) -> bool {
        let checked = self.basis.merge(&other.basis);
        let confirmed = self.head_confirmed.age_at(&other.head_confirmed) > 0;
        if confirmed {
            self.head_confirmed = other.head_confirmed.clone();
        }
        checked || confirmed
    }

    /// Returns whether this head replaces `cached`: whether it's newer in the
    /// durable order, or, at the same place in that order, whether merging
    /// `cached` into this head changes nothing and merging this head into
    /// `cached` changes something. So this head's check time, head
    /// confirmation and boundary are each at least `cached`'s, and one of
    /// the three is later or higher.
    fn supersedes(&self, cached: &CachedHead) -> bool {
        match self.order().cmp(&cached.order()) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => !self.clone().merge(cached) && cached.clone().merge(self),
        }
    }

    fn heap_bytes(&self) -> usize {
        self.read_basis().head.heap_bytes()
            + self.read_basis().basis.manifest().heap_bytes()
            + self.basis.boundary().heap_bytes()
    }
}

/// Returns the head that replaces `cached` when `candidate` is installed, or
/// `None` when `cached` stays: a candidate at the same place in the durable
/// order as `cached` is merged into `cached` (see [`CachedHead::merge`]), and
/// any other candidate is compared whole by [`CachedHead::supersedes`].
fn next_head(cached: &CachedHead, candidate: CachedHead) -> Option<CachedHead> {
    if cached.order() != candidate.order() {
        return Some(candidate);
    }
    let mut next = CachedHead::clone(cached);
    next.merge(&candidate).then_some(next)
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

/// Namespace heads and WAL-tail projections for every scope, each kind
/// under its own byte budget and recency order, so a tail can't evict a
/// head.
pub struct SharedHeadsAndWalTails {
    heads: DecodedBlockCache<(CacheScope, NamespaceId), Arc<CachedHead>>,
    tails: DecodedBlockCache<(CacheScope, WalTailCacheKey), Arc<ProjectedWalTail>>,
}

impl std::fmt::Debug for SharedHeadsAndWalTails {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedHeadsAndWalTails")
            .field("head_stats", &self.head_stats())
            .field("tail_stats", &self.tail_stats())
            .finish_non_exhaustive()
    }
}

impl SharedHeadsAndWalTails {
    /// Returns a cache that keeps up to `max_head_bytes` of namespace heads
    /// and up to `max_wal_tail_bytes` of WAL-tail projections.
    pub fn new(max_head_bytes: usize, max_wal_tail_bytes: usize) -> Self {
        Self {
            heads: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_head_bytes)),
            tails: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_wal_tail_bytes)),
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

/// One runtime's view of the shared namespace heads and WAL-tail projections.
#[derive(Debug)]
pub struct HeadAndWalTailCache {
    shared: Arc<SharedHeadsAndWalTails>,
    scope: CacheScope,
}

impl HeadAndWalTailCache {
    pub fn new(shared: Arc<SharedHeadsAndWalTails>, scope: CacheScope) -> Self {
        Self { shared, scope }
    }

    /// A view over heads and WAL tails that no other view reads, with a
    /// budget of `max_decoded_bytes` for heads and another for tails.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedHeadsAndWalTails::new(
                max_decoded_bytes,
                max_decoded_bytes,
            )),
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

    fn tail_key(&self, key: &WalTailCacheKey) -> (CacheScope, WalTailCacheKey) {
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
    /// the same and its basis is a later manifest, or when both are the same
    /// and its WAL number is higher. A floor object raises the WAL number
    /// and the head's retention floor, and nothing else.
    ///
    /// - A candidate below the cached head changes nothing, and the read
    ///   starts from the cached head.
    /// - A candidate equal to the cached head is merged into it, so the head
    ///   takes the later of each confirmation and the higher boundary,
    ///   because neither moves backward. A new snapshot is cached only when
    ///   the merge changed the head.
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
        let key = self.head_key(&candidate.read_basis().head.namespace_id);
        let next = match self.shared.heads.peek(&key) {
            Some(cached) => match next_head(&cached, candidate) {
                Some(next) => next,
                None => return cached,
            },
            None => candidate,
        };
        let next = Arc::new(next);
        self.shared
            .heads
            .insert_if(key, Arc::clone(&next), start, |cached| {
                next.supersedes(cached)
            })
            .unwrap_or(next)
    }

    /// Installs the head that `candidate` returns for the cached head, by the
    /// rule of [`Self::replace_head`], and returns the head that the cache
    /// keeps, or `None` when the namespace has no cached head.
    ///
    /// The cache reads the cached head and installs the candidate under one
    /// lock, so a head that an invalidation dropped stays dropped: the
    /// candidate is never installed in place of a head that isn't there.
    fn replace_present_head(
        &self,
        namespace_id: &NamespaceId,
        candidate: impl FnOnce(&CachedHead) -> CachedHead,
    ) -> Option<Arc<CachedHead>> {
        self.shared
            .heads
            .replace_present(&self.head_key(namespace_id), |cached| {
                let next = next_head(cached, candidate(cached))?;
                next.supersedes(cached).then(|| Arc::new(next))
            })
    }

    /// Moves the namespace's cached head onto `manifest`, a manifest that
    /// this runtime published or found current, by the rule a writer
    /// position moves by (see
    /// [`NamespaceCommitEngine::record_manifest_published`](crate::publish::NamespaceCommitEngine::record_manifest_published)),
    /// and caches the WAL tail above the manifest under the moved head's key.
    ///
    /// A manifest at or below the cached head's basis leaves the head where
    /// it is, and the head takes the manifest's check. Any other call makes
    /// the checks of the head in flight install nothing, and then installs
    /// the moved head, or, when no head is cached, the head can't move, or
    /// its tail isn't cached, drops the head as [`Self::invalidate_head`]
    /// does. A head that a check installed against an older manifest before
    /// the checks were marked is dropped too. Either install happens only
    /// while the namespace still has a cached head (see
    /// [`Self::replace_present_head`]), so a head that an invalidation
    /// dropped meanwhile stays dropped.
    ///
    /// The head keeps its head confirmation, and merges `manifest`'s check
    /// (see [`Held::merge`]).
    pub fn move_head(&self, namespace_id: &NamespaceId, manifest: &Held<LoadedManifest>) {
        let Some(cached) = self.get_head(namespace_id) else {
            self.invalidate_head(namespace_id);
            return;
        };
        if manifest.manifest_no() <= cached.basis.manifest_no() {
            self.replace_present_head(namespace_id, |cached| {
                let mut checked = CachedHead::clone(cached);
                checked.basis.merge(manifest);
                checked
            });
            return;
        }
        if let Some(moved) = self.moved_head(namespace_id, &cached, manifest) {
            self.install_moved_head(namespace_id, manifest, moved);
        }
    }

    /// Returns `cached` moved onto `manifest`, a later manifest, and caches
    /// its WAL tail, or returns `None` when [`Self::move_head`] installs
    /// nothing.
    fn moved_head(
        &self,
        namespace_id: &NamespaceId,
        cached: &CachedHead,
        manifest: &Held<LoadedManifest>,
    ) -> Option<CachedHead> {
        let tail_key = |read_basis: &ReadBasis| WalTailCacheKey {
            namespace_id: namespace_id.clone(),
            manifest_no: read_basis.basis.manifest_no(),
            head_seq: read_basis.head.seq,
            wal_no: read_basis.head.wal_no,
        };
        let mut basis = cached.basis.clone();
        let Some(mut tail) = self.get_tail(&tail_key(basis.read_basis())) else {
            self.invalidate_head(namespace_id);
            return None;
        };
        if !move_read_basis(&mut basis, &mut tail, manifest) {
            self.invalidate_head(namespace_id);
            return None;
        }
        self.insert_tail(tail_key(basis.read_basis()), tail);
        Some(CachedHead {
            basis,
            head_confirmed: cached.head_confirmed.clone(),
        })
    }

    /// Marks the checks of the head in flight, installs `moved`, and drops
    /// the cached head if the head the cache keeps is against a manifest
    /// older than `manifest`.
    fn install_moved_head(
        &self,
        namespace_id: &NamespaceId,
        manifest: &Held<LoadedManifest>,
        moved: CachedHead,
    ) {
        self.invalidate_head_in_flight(namespace_id);
        let kept = self.replace_present_head(namespace_id, |_| moved);
        if kept.is_some_and(|kept| kept.basis.manifest_no() < manifest.manifest_no()) {
            self.invalidate_head(namespace_id);
        }
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

    /// Makes the checks of the namespace's head in flight install nothing
    /// when they finish, and keeps the cached head.
    fn invalidate_head_in_flight(&self, namespace_id: &NamespaceId) {
        self.shared
            .heads
            .invalidate_in_flight(&self.head_key(namespace_id));
    }

    pub fn get_tail(&self, key: &WalTailCacheKey) -> Option<Arc<ProjectedWalTail>> {
        self.shared.tails.get(&self.tail_key(key))
    }

    /// Returns the WAL-tail projection under `key`, or loads it with `load`
    /// and caches it.
    ///
    /// Concurrent calls for one key share one load, so the reads at one head
    /// that miss its tail replay the tail once.
    pub(crate) async fn get_or_load_tail<E, F, Fut>(
        &self,
        key: &WalTailCacheKey,
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

    pub fn insert_tail(&self, key: WalTailCacheKey, tail: Arc<ProjectedWalTail>) {
        self.shared.tails.insert((self.scope, key), tail);
    }

    /// Returns the budget needed to cache this WAL-tail projection on its own.
    #[cfg(any(test, feature = "test-support"))]
    pub fn tail_charged_bytes(&self, key: &WalTailCacheKey, tail: &Arc<ProjectedWalTail>) -> usize {
        crate::block_cache::charged_bytes(&self.tail_key(key), tail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache::charged_bytes;
    use crate::metadata::{InodeRecord, MetadataState};
    use crate::namespace::basis::MetadataBasis;
    use crate::namespace::boundary::{KeptBoundary, LoadedBoundary};
    use crate::namespace::state::NamespaceReadState;

    use crate::wal::ProjectedWalTail;
    use futures::poll;
    use loonfs_types::format::control::{ManifestRef, NamespaceStatus};

    use loonfs_types::{CommitSeq, InodeId, InodeKind, ManifestNo, NamespaceId};
    use loonfs_types::{WalNo, WriterEpoch};
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use tokio::sync::watch;

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
        let read_basis = ReadBasis {
            head: NamespaceReadState {
                namespace_id: namespace_id.clone(),
                created_at_ms: 1_000,
                created_by: loonfs_test_support::test_actor(),
                fork_basis: None,
                pool_id: loonfs_test_support::ids::numbered_pool_id(1),
                status: NamespaceStatus::Active {},
                writer_epoch: WriterEpoch(1),
                writer: None,
                seq: CommitSeq(seq),
                wal_no: WalNo(seq),
                folded_wal_no: WalNo(0),
                retention_floor_seq: CommitSeq(0),
            },
            basis: MetadataBasis::new(ManifestRef {
                owner_namespace_id: namespace_id,
                manifest_no: ManifestNo(manifest_no),
                head_seq: CommitSeq(0),
                payload_checksum: loonfs_types::sha256_digest(b"checksum"),
            }),
        };
        CachedHead::new(
            Held::new(read_basis, checked.clone(), KeptBoundary::default()),
            checked.clone(),
        )
    }

    /// Returns `head` with its manifest checked at `checked` and its boundary
    /// `boundary`.
    fn rechecked(head: CachedHead, checked: &Observation, boundary: KeptBoundary) -> CachedHead {
        CachedHead {
            basis: Held::new(head.read_basis().clone(), checked.clone(), boundary),
            ..head
        }
    }

    fn order(head: &CachedHead) -> (u64, u64) {
        (
            head.read_basis().head.seq.0,
            head.read_basis().basis.manifest_no().0,
        )
    }

    /// Returns the boundary object of namespace `demo` at manifest
    /// `manifest_no`, read with the ETag `etag`.
    fn boundary_at(manifest_no: u64, etag: String) -> KeptBoundary {
        KeptBoundary::read(Some(LoadedBoundary {
            object_key: "namespaces/demo/boundary.json".to_owned(),
            etag,
            state: loonfs_types::format::control::BoundaryPayload {
                namespace_id: namespace("demo"),
                boundary: loonfs_types::format::control::ManifestBoundary::Manifest {
                    manifest_no: ManifestNo(manifest_no),
                },
            },
        }))
    }

    /// Creates a head like [`head_at`] that keeps a manifest boundary whose
    /// etag is `etag_bytes` bytes long.
    fn large_head(seq: u64, etag_bytes: usize, checked: &Observation) -> CachedHead {
        let boundary = boundary_at(1, "e".repeat(etag_bytes));
        rechecked(head_at(seq, 1, checked), checked, boundary)
    }

    /// Creates a publication of manifest `manifest_no` in namespace `demo`
    /// at the head of a basis made by [`head_at`], so a head made by
    /// [`head_at`] moves onto it and keeps its tail.
    fn published_at(manifest_no: u64, started: &Observation) -> Held<LoadedManifest> {
        let payload = loonfs_types::format::manifest::NamespaceManifestPayload {
            manifest_no: ManifestNo(manifest_no),
            writer_epoch: WriterEpoch(1),
            ..loonfs_types::format::manifest::NamespaceManifestPayload::initial(
                namespace("demo"),
                loonfs_test_support::ids::numbered_pool_id(1),
                1_000,
                loonfs_test_support::test_actor(),
            )
            .activated()
        };
        crate::test_support::held_manifest(payload, started.clone())
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
        let cache = HeadAndWalTailCache::unshared(1 << 20);
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
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        for (seq, manifest_no) in [(5, 1), (5, 2), (6, 1)] {
            let installed = cache.replace_head(head_at(seq, manifest_no, &now));
            assert_eq!(order(&installed), (seq, manifest_no));
        }
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert_eq!(order(&cached), (6, 1));
    }

    /// Checks that a candidate at the cached head's sequence and manifest
    /// with a higher WAL number, which is the head after a floor object,
    /// becomes the cached head, and that the head before the floor object
    /// doesn't replace it.
    #[test]
    fn a_head_after_a_floor_object_replaces_the_cached_head() {
        let now = crate::test_support::observed_now();
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(5, 1, &now));
        let mut after_floor_object = head_at(5, 1, &now);
        after_floor_object.basis.read_basis_mut().head.wal_no = WalNo(6);
        after_floor_object
            .basis
            .read_basis_mut()
            .head
            .retention_floor_seq = CommitSeq(3);
        let installed = cache.replace_head(after_floor_object);
        assert_eq!(
            installed.read_basis().head.retention_floor_seq,
            CommitSeq(3)
        );
        let kept = cache.replace_head(head_at(5, 1, &now));
        assert_eq!(kept.read_basis().head.wal_no, WalNo(6));
        assert_eq!(kept.read_basis().head.retention_floor_seq, CommitSeq(3));
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
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(CachedHead {
            head_confirmed: late.clone(),
            ..head_at(5, 1, &early)
        });
        let replaced = cache.replace_head(rechecked(
            head_at(5, 1, &early),
            &late,
            KeptBoundary::default(),
        ));
        assert_eq!(replaced.basis().checked().age_ms(), 0);
        assert_eq!(replaced.head_confirmed().age_ms(), 0);
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &replaced));
        let unchanged = cache.replace_head(head_at(5, 1, &early));
        assert!(Arc::ptr_eq(&unchanged, &replaced));
        let same_millisecond = cache.replace_head(head_at(5, 1, &late));
        assert!(
            Arc::ptr_eq(&same_millisecond, &replaced),
            "a confirmation in the same millisecond doesn't move"
        );
    }

    /// Checks that a candidate equal to the cached head whose check read the
    /// boundary object in the same millisecond as the cached head's check,
    /// which kept no boundary, replaces the cached head, so the cache keeps
    /// the boundary.
    #[test]
    fn a_boundary_read_in_the_same_millisecond_replaces_the_cached_head() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let now = Observation::now(clock);
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(5, 1, &now));
        let boundary = boundary_at(1, "\"boundary-etag\"".to_owned());
        let read = rechecked(head_at(5, 1, &now), &now, boundary.clone());
        let replaced = cache.replace_head(read);
        assert_eq!(replaced.basis().boundary(), &boundary);
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert_eq!(cached.basis().boundary(), &boundary);
    }

    /// Checks that a candidate at the cached head's place in the durable
    /// order with later checks and a lower boundary doesn't replace a cached
    /// head with a higher boundary, and that the merge of the two does.
    #[test]
    fn a_head_with_a_lower_boundary_does_not_replace_the_cached_head() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let early = Observation::now(clock.clone());
        clock.advance_ms(10);
        let late = Observation::now(clock.clone());
        let higher = boundary_at(2, "\"higher\"".to_owned());
        let cached = rechecked(head_at(5, 1, &early), &early, higher.clone());
        let lower = rechecked(
            CachedHead {
                head_confirmed: late.clone(),
                ..head_at(5, 1, &late)
            },
            &late,
            boundary_at(1, "\"lower\"".to_owned()),
        );
        assert!(!lower.supersedes(&cached));
        let merged = next_head(&cached, lower).expect("the later checks change the head");
        assert!(merged.supersedes(&cached));
        assert_eq!(merged.basis().boundary(), &higher);
        assert_eq!(merged.basis().checked(), &late);
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
        let invalidate = |cache: &HeadAndWalTailCache| cache.invalidate_head(&demo);
        let evict = |cache: &HeadAndWalTailCache| {
            cache.replace_head(head_at(7, 1, &now));
            assert!(cache.get_head(&demo).is_some(), "the newer head fits");
            // A head of another namespace, charged the same, takes its place.
            cache.replace_head(head_of("omed", 7, 1, &now));
            assert!(cache.get_head(&demo).is_none(), "the newer head is evicted");
        };
        for drop_head in [&invalidate as &dyn Fn(&HeadAndWalTailCache), &evict] {
            let cache = HeadAndWalTailCache::unshared(budget);
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

    /// Checks that a publication recorded while no head is cached drops the
    /// head for the check in flight, so the check's older head isn't
    /// installed, and that the read starts from the check's own head.
    #[tokio::test]
    async fn a_publication_with_no_cached_head_drops_the_head_a_check_in_flight_loads() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        let (release, released) = watch::channel(false);
        let mut read = pin!(cache.check_or_join_head(
            &demo,
            || None,
            || { check_after(&released, head_at(6, 1, &now)) }
        ));
        assert!(poll!(read.as_mut()).is_pending());
        cache.move_head(&demo, &published_at(2, &now));
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
    }

    /// Checks that a publication that moves the cached head drops the head
    /// for the check in flight before it installs the moved head, so the
    /// check's head, newer in sequence but against the older manifest, isn't
    /// installed over the moved head, and that an older publication's moved
    /// head arriving after a newer move doesn't replace the newer head.
    #[tokio::test]
    async fn a_publication_that_moves_the_cached_head_drops_the_head_a_check_in_flight_loads() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(6, 1, &now));
        let tail_key = WalTailCacheKey {
            namespace_id: demo.clone(),
            manifest_no: ManifestNo(1),
            head_seq: CommitSeq(6),
            wal_no: WalNo(6),
        };
        cache.insert_tail(tail_key, Arc::new(ProjectedWalTail::default()));
        let (release, released) = watch::channel(false);
        let mut read = pin!(cache.check_or_join_head(
            &demo,
            || None,
            || { check_after(&released, head_at(7, 1, &now)) }
        ));
        assert!(poll!(read.as_mut()).is_pending());
        cache.move_head(&demo, &published_at(2, &now));
        let moved = cache.get_head(&demo).expect("the moved head");
        assert_eq!(order(&moved), (6, 2));
        release.send(true).expect("the check waits for the release");
        let read = read.await.expect("the check's head");
        assert_eq!(
            order(&read),
            (7, 1),
            "the read starts from the check's head"
        );
        let cached = cache.get_head(&demo).expect("the moved head stays");
        assert!(Arc::ptr_eq(&cached, &moved), "the check installs nothing");
        cache.move_head(&demo, &published_at(3, &now));
        cache.replace_head(head_at(6, 2, &now));
        let cached = cache.get_head(&demo).expect("the head moved again");
        assert_eq!(
            order(&cached),
            (6, 3),
            "an older publication's moved head arriving late changes nothing"
        );
    }

    /// Checks that a head a check installs against the older manifest while
    /// a publication moves the cached head, after the publication read the
    /// cached head and before it marked the checks in flight, is dropped
    /// rather than kept above the moved head.
    #[test]
    fn a_head_a_check_installs_while_a_publication_moves_the_cached_head_is_dropped() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(6, 1, &now));
        let tail_key = WalTailCacheKey {
            namespace_id: demo.clone(),
            manifest_no: ManifestNo(1),
            head_seq: CommitSeq(6),
            wal_no: WalNo(6),
        };
        cache.insert_tail(tail_key, Arc::new(ProjectedWalTail::default()));
        let published = published_at(2, &now);
        let cached = cache.get_head(&demo).expect("a cached head");
        let moved = cache
            .moved_head(&demo, &cached, &published)
            .expect("the head moves");
        assert_eq!(order(&moved), (6, 2));
        cache.replace_head(head_at(7, 1, &now));
        cache.install_moved_head(&demo, &published, moved);
        assert!(
            cache.get_head(&demo).is_none(),
            "the head against the older manifest is dropped"
        );
    }

    /// Checks that a head that a publication moved stays dropped when an
    /// invalidation drops the cached head after the publication read it and
    /// before it installs the moved head.
    #[test]
    fn a_moved_head_does_not_bring_back_a_head_that_an_invalidation_dropped() {
        let now = crate::test_support::observed_now();
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(6, 1, &now));
        cache.insert_tail(
            WalTailCacheKey {
                namespace_id: demo.clone(),
                manifest_no: ManifestNo(1),
                head_seq: CommitSeq(6),
                wal_no: WalNo(6),
            },
            Arc::new(ProjectedWalTail::default()),
        );
        let published = published_at(2, &now);
        let cached = cache.get_head(&demo).expect("a cached head");
        let moved = cache
            .moved_head(&demo, &cached, &published)
            .expect("the head moves");
        cache.invalidate_head(&demo);
        cache.install_moved_head(&demo, &published, moved);
        assert!(
            cache.get_head(&demo).is_none(),
            "the dropped head stays dropped"
        );
    }

    /// Checks that a publication that found the cached head's manifest
    /// current gives the cached head that publication's check when it's
    /// younger, and leaves the head where it is.
    #[test]
    fn a_publication_that_finds_the_cached_manifest_current_confirms_it() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let early = Observation::now(clock.clone());
        clock.advance_ms(10);
        let late = Observation::now(clock.clone());
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(6, 2, &early));
        cache.move_head(&demo, &published_at(2, &late));
        let cached = cache.get_head(&demo).expect("the head stays");
        assert_eq!(order(&cached), (6, 2));
        assert_eq!(cached.basis().checked(), &late);
        assert_eq!(cached.head_confirmed(), &early);
    }

    /// Checks that a publication that found the cached head's manifest
    /// current, with a check in the same millisecond as the cached head's
    /// and a boundary object that the cached head doesn't keep, raises the
    /// cached head's boundary to that object.
    #[test]
    fn a_publication_that_finds_the_cached_manifest_current_raises_its_boundary() {
        let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(1_000));
        let now = Observation::now(clock);
        let demo = namespace("demo");
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        cache.replace_head(head_at(6, 2, &now));
        let boundary = boundary_at(1, "\"boundary-etag\"".to_owned());
        let published = published_at(2, &now);
        let published = Held::new(
            published.manifest().clone(),
            published.checked().clone(),
            boundary.clone(),
        );
        cache.move_head(&demo, &published);
        let cached = cache.get_head(&demo).expect("the head stays");
        assert_eq!(order(&cached), (6, 2));
        assert_eq!(cached.basis().boundary(), &boundary);
    }

    /// Checks that the reads of one head that arrive while a check runs
    /// share one more check, that a read of another namespace runs its own
    /// check alongside, and that the heads keep no in-flight entry once the
    /// reads are done.
    #[tokio::test]
    async fn a_wave_of_reads_of_one_head_shares_at_most_two_checks() {
        let now = crate::test_support::observed_now();
        let cache = HeadAndWalTailCache::unshared(1 << 20);
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
        let cache = HeadAndWalTailCache::unshared(4096);
        cache.replace_head(head_at(5, 1, &now));
        for (index, inodes) in [1, 4, 16, 64, 256].into_iter().cycle().take(20).enumerate() {
            let rows = (0..inodes)
                .map(|inode| InodeRecord {
                    inode_id: InodeId(inode),
                    inode_kind: InodeKind::File,
                    committed_seq: CommitSeq(3),
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
            let key = WalTailCacheKey {
                namespace_id: namespace("demo"),
                manifest_no: ManifestNo(index as u64),
                head_seq: CommitSeq(5),
                wal_no: WalNo(5),
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
        let cache = HeadAndWalTailCache::unshared(4096);
        let large = large_head(5, 100_000, &now);
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
        let cache = HeadAndWalTailCache::unshared(4096);
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
        let large = large_head(1, 100_000, &now);
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
        let cache = HeadAndWalTailCache::unshared(1 << 20);
        let small = cache.replace_head(head_at(5, 1, &now));
        let before = cache.head_stats().cached_decoded_bytes;
        let large = cache.replace_head(large_head(6, 10_000, &now));
        let after = cache.head_stats().cached_decoded_bytes;
        assert!(
            after >= before + large.heap_bytes() - small.heap_bytes(),
            "{before} bytes before, {after} after"
        );
        let cached = cache.get_head(&namespace("demo")).expect("a cached head");
        assert!(Arc::ptr_eq(&cached, &large));
    }

    /// Checks that a cached head's weight grows by the strings of the
    /// manifest boundary that the head keeps.
    #[test]
    fn a_head_is_charged_the_boundary_it_keeps() {
        let now = crate::test_support::observed_now();
        let without = head_at(5, 1, &now);
        let boundary = LoadedBoundary {
            object_key: "namespaces/demo/boundary.json".to_owned(),
            etag: "\"boundary-etag\"".to_owned(),
            state: loonfs_types::format::control::BoundaryPayload {
                namespace_id: namespace("demo"),
                boundary: loonfs_types::format::control::ManifestBoundary::Manifest {
                    manifest_no: ManifestNo(1),
                },
            },
        };
        let kept = boundary.object_key.capacity()
            + boundary.etag.capacity()
            + HeapBytes::heap_bytes(&boundary.state.namespace_id);
        let checked = without.basis().checked().clone();
        let with = rechecked(
            without.clone(),
            &checked,
            KeptBoundary::read(Some(boundary)),
        );
        assert_eq!(Arc::new(with).weight(), Arc::new(without).weight() + kept);
    }

    /// Checks that a zero budget keeps no head and no tail and records no
    /// lookups, and that a tail load still returns the tail it loaded.
    #[tokio::test]
    async fn a_zero_budget_keeps_and_counts_nothing() {
        let now = crate::test_support::observed_now();
        let cache = HeadAndWalTailCache::unshared(0);
        cache.replace_head(head_at(5, 1, &now));
        assert!(cache.get_head(&namespace("demo")).is_none());

        let key = WalTailCacheKey {
            namespace_id: namespace("demo"),
            manifest_no: ManifestNo(1),
            head_seq: CommitSeq(5),
            wal_no: WalNo(5),
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
}
