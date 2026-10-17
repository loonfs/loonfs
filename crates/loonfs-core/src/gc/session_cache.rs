//! Completed upload sessions that garbage collection keeps between passes.

use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey,
};
use crate::heap_bytes::HeapBytes;
use crate::manifest::CacheScope;
use loonfs_types::{ContentId, UploadId};
use std::sync::Arc;

/// The two facts that garbage collection uses from a completed upload
/// session's record.
///
/// LoonFS refuses every update to a completed upload session record, so both
/// facts stay true for as long as the record exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletedUploadSession {
    /// The content that the session uploaded, which a pass protects.
    pub(crate) content_id: ContentId,
    /// When the session was completed. The sweep deletes the record once the
    /// content reclamation grace has passed since this time.
    pub(crate) completed_at_ms: u64,
}

impl DecodedBlock for CompletedUploadSession {
    fn weight(&self) -> usize {
        self.heap_bytes()
    }
}

impl DecodedBlockKey for UploadId {
    fn heap_bytes(&self) -> usize {
        HeapBytes::heap_bytes(self)
    }
}

/// Completed upload sessions for every scope, under one byte budget.
///
/// The cache evicts the least recently used session to stay within the
/// budget. An evicted session costs one record read on a later pass.
pub struct SharedCompletedUploadSessions {
    sessions: DecodedBlockCache<(CacheScope, UploadId), CompletedUploadSession>,
}

impl std::fmt::Debug for SharedCompletedUploadSessions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedCompletedUploadSessions")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl SharedCompletedUploadSessions {
    /// Creates an empty set of sessions under a budget of `max_bytes`.
    ///
    /// A budget of zero keeps none.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            sessions: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_bytes)),
        }
    }

    /// Counters and occupancy of the sessions of every scope.
    pub fn stats(&self) -> DecodedBlockCacheStats {
        self.sessions.stats()
    }
}

/// One runtime's view of shared completed upload sessions.
///
/// An entry is keyed by the runtime's scope and the upload ID, without the
/// namespace ID. An upload ID is 128 random bits, so it's unique across the
/// namespaces of a store, and a pass looks up only the IDs that it just
/// listed under one namespace's prefix. The scope keeps two runtimes that
/// share a metadata cache apart, as it does for their head state.
#[derive(Debug)]
pub struct CompletedUploadSessionCache {
    shared: Arc<SharedCompletedUploadSessions>,
    scope: CacheScope,
}

impl CompletedUploadSessionCache {
    pub fn new(shared: Arc<SharedCompletedUploadSessions>, scope: CacheScope) -> Self {
        Self { shared, scope }
    }

    /// A view over sessions no other view reads.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedCompletedUploadSessions::new(max_bytes)),
            CacheScope::new(0),
        )
    }

    fn key(&self, upload_id: &UploadId) -> (CacheScope, UploadId) {
        (self.scope, upload_id.clone())
    }

    /// Returns the completed session, if the cache keeps it.
    pub(crate) fn get(&self, upload_id: &UploadId) -> Option<CompletedUploadSession> {
        self.shared.sessions.get(&self.key(upload_id))
    }

    /// Adds a completed session, and evicts the least recently used sessions
    /// when the addition takes the cache over its budget.
    pub(crate) fn insert(&self, upload_id: UploadId, session: CompletedUploadSession) {
        self.shared
            .sessions
            .insert((self.scope, upload_id), session);
    }

    /// Removes a session whose record the sweep deletes.
    pub(crate) fn invalidate(&self, upload_id: &UploadId) {
        self.shared.sessions.invalidate(&self.key(upload_id));
    }

    /// Counts the sessions that the cache keeps, under every scope.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shared.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{CompletedUploadSession, CompletedUploadSessionCache};
    use crate::block_cache::{charged_bytes, DecodedBlockCacheStats};
    use crate::manifest::CacheScope;
    use loonfs_types::{ContentId, UploadId};

    fn session() -> CompletedUploadSession {
        CompletedUploadSession {
            content_id: ContentId::generate(),
            completed_at_ms: 1_000,
        }
    }

    /// Checks that an insert past the budget evicts the least recently used
    /// session and keeps the others, and that `invalidate` removes one
    /// session without counting an eviction.
    #[test]
    fn an_insert_past_the_budget_evicts_the_least_recently_used_session() {
        let upload_ids: Vec<UploadId> = (0..3).map(|_| UploadId::generate()).collect();
        let fill = |cache: &CompletedUploadSessionCache| {
            cache.insert(upload_ids[0].clone(), session());
            cache.insert(upload_ids[1].clone(), session());
            assert!(cache.get(&upload_ids[0]).is_some());
        };
        // The budget fits the first two sessions exactly, after the lookup
        // marks the first one recently used.
        let unbounded = CompletedUploadSessionCache::unshared(usize::MAX);
        fill(&unbounded);
        let budget = unbounded.shared.stats().cached_decoded_bytes;
        let cache = CompletedUploadSessionCache::unshared(budget);
        fill(&cache);
        assert_eq!(cache.shared.stats().evictions, 0);

        cache.insert(upload_ids[2].clone(), session());
        assert_eq!(cache.shared.stats().evictions, 1);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&upload_ids[1]).is_none());
        assert!(cache.get(&upload_ids[0]).is_some());
        assert!(cache.get(&upload_ids[2]).is_some());

        cache.invalidate(&upload_ids[0]);
        assert!(cache.get(&upload_ids[0]).is_none());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.shared.stats().evictions, 1);
    }

    /// Checks that a session heavier than the whole budget is rejected, and
    /// that a zero budget keeps no session and records nothing.
    #[test]
    fn a_session_heavier_than_the_budget_is_rejected_and_a_zero_budget_keeps_none() {
        let upload_id = UploadId::generate();
        let charge = charged_bytes(&(CacheScope::new(0), upload_id.clone()), &session());
        let heavy = CompletedUploadSessionCache::unshared(charge - 1);
        heavy.insert(upload_id.clone(), session());
        assert!(heavy.get(&upload_id).is_none());
        let stats = heavy.shared.stats();
        assert_eq!((stats.inserts, stats.rejections, stats.misses), (0, 1, 1));

        let disabled = CompletedUploadSessionCache::unshared(0);
        disabled.insert(upload_id.clone(), session());
        assert!(disabled.get(&upload_id).is_none());
        assert_eq!(disabled.shared.stats(), DecodedBlockCacheStats::default());
    }
}
