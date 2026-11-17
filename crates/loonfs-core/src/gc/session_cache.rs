//! Completed upload sessions that garbage collection keeps between collections.

use crate::block_cache::CacheScope;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey,
};
use crate::heap_bytes::HeapBytes;
use loonfs_types::{ContentId, NamespaceId, UploadId};
use std::sync::Arc;

/// The facts that garbage collection uses from a completed upload session's
/// outcome record.
///
/// LoonFS never changes an outcome record, so these facts stay true for as
/// long as the session's records exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletedUploadSession {
    /// The namespace that created the session, whose retirement status the
    /// sweep applies to the session.
    pub(crate) namespace_id: NamespaceId,
    /// The content that the session uploaded, which a collection protects.
    pub(crate) content_id: ContentId,
    /// When the session was completed. The sweep deletes the session's
    /// records once the content reclamation grace has passed since this time.
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
/// budget. An evicted session costs one record read on a later collection.
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
/// namespaces of a store, and a collection looks up only the IDs that it just
/// listed under one namespace's prefix. The scope keeps two runtimes that
/// share a metadata cache apart, as it does for their heads and WAL tails.
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
