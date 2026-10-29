//! A cache of small content objects, keyed by the object each entry was read
//! from.
//!
//! A content object is written once and verified against its reference's
//! checksum when it is read from the store, so LoonFS doesn't invalidate
//! entries here, and recency evicts them to stay within the byte budget.
//!
//! The cache sits below the path-level access check. LoonFS reads a content
//! object through a content location that a metadata view resolved after
//! checking the caller's access to the path or inode. So a caller reaches an
//! entry that another caller added only after passing the same check.

use crate::block_cache::ObjectStoreScope;
use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
};
use bytes::Bytes;
use std::sync::Arc;

impl DecodedBlock for Bytes {
    fn weight(&self) -> usize {
        // Cloning owned Bytes also allocates its shared buffer metadata.
        self.len() + 3 * std::mem::size_of::<usize>()
    }
}

/// Small content objects for every runtime, under one byte budget and one
/// recency order.
pub struct SharedObjects {
    /// The bytes of each content object from its first byte. A content
    /// reference covers a prefix of its object, so an entry can be shorter
    /// than the object.
    content_objects: DecodedBlockCache<(ObjectStoreScope, String), Bytes>,
    max_cached_object_bytes: u64,
}

impl std::fmt::Debug for SharedObjects {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedObjects")
            .field("max_cached_object_bytes", &self.max_cached_object_bytes)
            .field("content_object_stats", &self.content_object_stats())
            .finish_non_exhaustive()
    }
}

impl SharedObjects {
    /// Returns shared objects that keep up to `max_content_object_bytes` of
    /// content objects, and no content object larger than
    /// `max_cached_object_bytes`.
    pub fn new(max_content_object_bytes: usize, max_cached_object_bytes: u64) -> Self {
        Self {
            content_objects: DecodedBlockCache::new(DecodedBlockCacheConfig::new(
                max_content_object_bytes,
            )),
            max_cached_object_bytes,
        }
    }

    /// Returns the counters and occupancy of the content objects.
    pub fn content_object_stats(&self) -> DecodedBlockCacheStats {
        self.content_objects.stats()
    }
}

/// One runtime's view of shared decoded objects, under the scope of the
/// runtime's object store.
///
/// Every view adds its [`ObjectStoreScope`] to each key, so runtimes over one
/// object store share entries, and runtimes over different object stores
/// never read each other's entries. (Two object stores can contain different
/// objects under the same key.)
#[derive(Debug)]
pub struct ObjectCache {
    shared: Arc<SharedObjects>,
    scope: ObjectStoreScope,
}

impl ObjectCache {
    pub fn new(shared: Arc<SharedObjects>, scope: ObjectStoreScope) -> Self {
        Self { shared, scope }
    }

    /// A view over objects no other view reads, with a budget of
    /// `max_decoded_bytes`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize, max_cached_object_bytes: u64) -> Self {
        Self::new(
            Arc::new(SharedObjects::new(
                max_decoded_bytes,
                max_cached_object_bytes,
            )),
            ObjectStoreScope::new(0),
        )
    }

    /// Counters of the content objects in the whole shared cache, across
    /// every view.
    pub fn content_object_stats(&self) -> DecodedBlockCacheStats {
        self.shared.content_object_stats()
    }

    fn content_key(&self, object_key: &str) -> (ObjectStoreScope, String) {
        (self.scope, object_key.to_owned())
    }

    /// Returns the first `end` bytes of the content object, if the cache
    /// has at least that many. A range that ends past the largest object
    /// the cache keeps isn't looked up.
    ///
    /// An entry shorter than `end` can't serve the read, so the lookup
    /// records a miss and leaves the entry's recency as it was.
    pub(crate) fn get_content(&self, object_key: &str, end: u64) -> Option<Bytes> {
        if end > self.shared.max_cached_object_bytes {
            return None;
        }
        let key = self.content_key(object_key);
        let content_objects = &self.shared.content_objects;
        if content_objects
            .peek(&key)
            .is_some_and(|cached| (cached.len() as u64) < end)
        {
            content_objects.record_miss();
            return None;
        }
        // A concurrent insert can replace the entry after the check above,
        // so the entry's length is checked again.
        content_objects
            .get(&key)
            .filter(|cached| cached.len() as u64 >= end)
    }

    /// Keeps the first bytes of a content object after a read verified them
    /// against a content reference, unless the bytes are empty or the cache
    /// already has as many.
    pub(crate) fn insert_content(&self, object_key: &str, bytes: &[u8]) {
        // A read of no bytes sends no store request, because `read_range`
        // builds no requests for an empty range. So an empty entry would
        // charge the budget for a hit that saves nothing.
        if bytes.is_empty() || bytes.len() as u64 > self.shared.max_cached_object_bytes {
            return;
        }
        let key = self.content_key(object_key);
        let content_objects = &self.shared.content_objects;
        if content_objects
            .peek(&key)
            .is_some_and(|cached| cached.len() >= bytes.len())
        {
            return;
        }
        // A copy keeps the entry's allocation exactly as large as its
        // weight, whatever buffer the store returned the bytes in.
        content_objects.insert(key, Bytes::copy_from_slice(bytes));
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectCache, ObjectStoreScope, SharedObjects};
    use crate::block_cache::{charged_bytes, DecodedBlockCacheStats};
    use bytes::Bytes;
    use std::sync::Arc;

    /// Returns the charge of a content entry of `len` bytes under
    /// `object_key`, which includes the key and the cache's own allocation.
    fn content_charge(object_key: &str, len: usize) -> usize {
        let key = (ObjectStoreScope::new(0), object_key.to_owned());
        charged_bytes(&key, &Bytes::from(vec![0; len]))
    }

    /// Checks that each content object is charged its length with its key
    /// and the cache's own allocation, that the least recently used object
    /// is evicted to stay within the budget, and that an object over the
    /// size cap isn't kept.
    #[test]
    fn content_inserts_are_charged_and_evicted_under_the_budget() {
        let entry = content_charge("a", 400);
        assert!(entry > 400);
        // The budget fits two entries but not three.
        let cache = ObjectCache::unshared(2 * entry + entry / 2, 600);
        cache.insert_content("a", &[1; 400]);
        cache.insert_content("b", &[2; 400]);
        assert_eq!(cache.content_object_stats().cached_decoded_bytes, 2 * entry);

        cache.insert_content("large", &[3; 700]);
        assert!(
            cache.get_content("large", 700).is_none(),
            "an object over the size cap is neither kept nor looked up"
        );
        assert_eq!(cache.content_object_stats().inserts, 2);

        assert!(cache.get_content("a", 400).is_some());
        cache.insert_content("c", &[4; 400]);
        let stats = cache.content_object_stats();
        assert_eq!(
            (stats.evictions, stats.cached_decoded_bytes),
            (1, 2 * entry)
        );
        assert!(
            cache.get_content("b", 1).is_none(),
            "the least recently used object is evicted first"
        );

        cache.insert_content("a", &[1; 100]);
        assert_eq!(
            cache.content_object_stats().inserts,
            3,
            "a shorter prefix doesn't replace a longer one"
        );
        assert!(
            cache.get_content("a", 401).is_none(),
            "a range past the cached bytes goes to the store"
        );
        assert_eq!(
            cache.get_content("a", 400).map(|bytes| bytes.len()),
            Some(400)
        );
        let stats = cache.content_object_stats();
        assert_eq!(
            (stats.hits, stats.misses),
            (2, 2),
            "a lookup past the cached bytes records a miss"
        );
    }

    /// Checks that views over two object stores read their own entries for
    /// the same object key under one budget, and that two views over one
    /// object store read the same entry.
    #[test]
    fn views_read_only_the_objects_of_their_own_object_store() {
        let shared = Arc::new(SharedObjects::new(1_000, 1_000));
        let view = |scope| ObjectCache::new(Arc::clone(&shared), ObjectStoreScope::new(scope));
        let (first, second, first_again) = (view(1), view(2), view(1));
        first.insert_content("key", b"first");
        assert!(second.get_content("key", 5).is_none());
        second.insert_content("key", b"other");
        for (view, bytes) in [
            (&first, b"first"),
            (&second, b"other"),
            (&first_again, b"first"),
        ] {
            assert_eq!(view.get_content("key", 5).as_deref(), Some(&bytes[..]));
        }
        assert_eq!(
            shared.content_object_stats().cached_decoded_bytes,
            2 * content_charge("key", 5)
        );
    }

    /// Checks that an empty content object isn't kept, because a read of no
    /// bytes sends no store request that an entry could save.
    #[test]
    fn an_empty_content_object_is_not_kept() {
        let cache = ObjectCache::unshared(1_000, 1_000);
        cache.insert_content("empty", &[]);
        assert!(cache
            .shared
            .content_objects
            .peek(&cache.content_key("empty"))
            .is_none());
        assert!(cache.get_content("empty", 0).is_none());
        let stats = cache.content_object_stats();
        assert_eq!((stats.inserts, stats.cached_decoded_bytes), (0, 0));
    }

    /// Checks that a zero budget keeps no objects and records no lookups.
    #[test]
    fn a_zero_budget_keeps_and_counts_nothing() {
        let cache = ObjectCache::unshared(0, 1_000);
        cache.insert_content("key", b"bytes");
        assert!(cache.get_content("key", 5).is_none());
        assert_eq!(
            cache.content_object_stats(),
            DecodedBlockCacheStats::default()
        );
    }
}
