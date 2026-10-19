//! A cache of decoded WAL objects and small content objects, keyed by the
//! object each entry was read from.
//!
//! Both kinds of object are immutable. A WAL number identifies one object
//! that's never replaced or recreated, and a content object is written once
//! and verified against its reference's checksum when it is read from the
//! store. So LoonFS doesn't invalidate entries here, and recency evicts them
//! to stay within the byte budget.
//!
//! The cache sits below the path-level access check. LoonFS replays WAL
//! objects into the metadata view that every read checks access against, and
//! it reads a content object through a content location that a metadata view
//! resolved after checking the caller's access to the path or inode. So a
//! caller reaches an entry that another caller added only after passing the
//! same check.

use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockKey, KindCounters,
};
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::manifest::ObjectStoreScope;
use bytes::Bytes;
use loonfs_types::format::wal::WalObjectEnvelope;
use loonfs_types::{NamespaceId, WalNo};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ObjectKey {
    Wal {
        namespace_id: NamespaceId,
        wal_no: WalNo,
    },
    Content {
        object_key: String,
    },
}

impl DecodedBlockKey for ObjectKey {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Wal { namespace_id, .. } => HeapBytes::heap_bytes(namespace_id),
            Self::Content { object_key } => HeapBytes::heap_bytes(object_key),
        }
    }
}

#[derive(Debug, Clone)]
enum CachedObject {
    Wal(Arc<WalObjectEnvelope>),
    /// The object's bytes from its first byte. A content reference covers a
    /// prefix of its object, so an entry can be shorter than the object.
    Content(Bytes),
}

impl DecodedBlock for CachedObject {
    fn weight(&self) -> usize {
        match self {
            Self::Wal(envelope) => arc_bytes::<WalObjectEnvelope>() + envelope.heap_bytes(),
            // Cloning owned Bytes also allocates its shared buffer metadata.
            Self::Content(bytes) => bytes.len() + 3 * std::mem::size_of::<usize>(),
        }
    }
}

/// Counters and occupancy of the shared objects.
///
/// The first fields are the counters that every cache reports (see
/// [`DecodedBlockCacheStats`](crate::block_cache::DecodedBlockCacheStats)),
/// and the rest count the lookups and inserts of each kind of object.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub inserts: usize,
    pub evictions: usize,
    pub evicted_decoded_bytes: usize,
    pub rejections: usize,
    pub rejected_decoded_bytes: usize,
    pub cached_decoded_bytes: usize,
    pub wal_hits: usize,
    pub wal_misses: usize,
    pub wal_inserts: usize,
    pub content_hits: usize,
    pub content_misses: usize,
    pub content_inserts: usize,
}

/// Decoded WAL objects and small content objects for every runtime, under
/// one byte budget and one recency order.
pub struct SharedObjects {
    entries: DecodedBlockCache<(ObjectStoreScope, ObjectKey), CachedObject>,
    max_cached_object_bytes: u64,
    wal: KindCounters,
    content: KindCounters,
}

impl std::fmt::Debug for SharedObjects {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedObjects")
            .field("max_cached_object_bytes", &self.max_cached_object_bytes)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl SharedObjects {
    /// `max_cached_object_bytes` is the largest content object the cache
    /// keeps.
    pub fn new(max_decoded_bytes: usize, max_cached_object_bytes: u64) -> Self {
        Self {
            entries: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_decoded_bytes)),
            max_cached_object_bytes,
            wal: KindCounters::default(),
            content: KindCounters::default(),
        }
    }

    pub fn stats(&self) -> ObjectCacheStats {
        let entries = self.entries.stats();
        let (wal, content) = (self.wal.stats(), self.content.stats());
        ObjectCacheStats {
            hits: entries.hits,
            misses: entries.misses,
            inserts: entries.inserts,
            evictions: entries.evictions,
            evicted_decoded_bytes: entries.evicted_decoded_bytes,
            rejections: entries.rejections,
            rejected_decoded_bytes: entries.rejected_decoded_bytes,
            cached_decoded_bytes: entries.cached_decoded_bytes,
            wal_hits: wal.hits,
            wal_misses: wal.misses,
            wal_inserts: wal.inserts,
            content_hits: content.hits,
            content_misses: content.misses,
            content_inserts: content.inserts,
        }
    }
}

/// One runtime's view of shared decoded objects, under the scope of the
/// runtime's object store.
///
/// Every view adds its [`ObjectStoreScope`] to each key, so runtimes over one
/// object store share entries, and runtimes over different object stores
/// never read each other's entries. (Two object stores can contain different
/// WAL objects under the same key.)
#[derive(Debug)]
pub struct ObjectCache {
    shared: Arc<SharedObjects>,
    scope: ObjectStoreScope,
}

impl ObjectCache {
    pub fn new(shared: Arc<SharedObjects>, scope: ObjectStoreScope) -> Self {
        Self { shared, scope }
    }

    /// A view over objects no other view reads.
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

    /// Counters for the whole shared cache, across every view.
    pub fn stats(&self) -> ObjectCacheStats {
        self.shared.stats()
    }

    fn wal_key(&self, namespace_id: &NamespaceId, wal_no: WalNo) -> (ObjectStoreScope, ObjectKey) {
        (
            self.scope,
            ObjectKey::Wal {
                namespace_id: namespace_id.clone(),
                wal_no,
            },
        )
    }

    fn content_key(&self, object_key: &str) -> (ObjectStoreScope, ObjectKey) {
        (
            self.scope,
            ObjectKey::Content {
                object_key: object_key.to_owned(),
            },
        )
    }

    /// Returns the decoded WAL object at `wal_no`, if the cache has it.
    pub(crate) fn get_wal_object(
        &self,
        namespace_id: &NamespaceId,
        wal_no: WalNo,
    ) -> Option<Arc<WalObjectEnvelope>> {
        self.shared.entries.get_kind(
            &self.wal_key(namespace_id, wal_no),
            &self.shared.wal,
            |object| match object {
                CachedObject::Wal(envelope) => Some(envelope),
                CachedObject::Content(_) => None,
            },
        )
    }

    /// Keeps a WAL object that decoded and matched the number and namespace
    /// of its key.
    pub(crate) fn insert_wal_object(&self, envelope: &Arc<WalObjectEnvelope>) {
        let payload = envelope.payload();
        self.shared.entries.insert_kind(
            self.wal_key(&payload.namespace_id, payload.wal_no),
            CachedObject::Wal(Arc::clone(envelope)),
            &self.shared.wal,
        );
    }

    /// Returns the first `end` bytes of the content object, if the cache
    /// has at least that many. A range that ends past the largest object
    /// the cache keeps isn't looked up.
    pub(crate) fn get_content(&self, object_key: &str, end: u64) -> Option<Bytes> {
        if end > self.shared.max_cached_object_bytes {
            return None;
        }
        self.shared.entries.get_kind(
            &self.content_key(object_key),
            &self.shared.content,
            |object| match object {
                CachedObject::Content(bytes) if bytes.len() as u64 >= end => Some(bytes),
                CachedObject::Content(_) | CachedObject::Wal(_) => None,
            },
        )
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
        if let Some(CachedObject::Content(cached)) = self.shared.entries.peek(&key) {
            if cached.len() >= bytes.len() {
                return;
            }
        }
        // A copy keeps the entry's allocation exactly as large as its
        // weight, whatever buffer the store returned the bytes in.
        self.shared.entries.insert_kind(
            key,
            CachedObject::Content(Bytes::copy_from_slice(bytes)),
            &self.shared.content,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedObject, ObjectCache, ObjectKey, ObjectStoreScope, SharedObjects};
    use crate::block_cache::charged_bytes;
    use bytes::Bytes;
    use loonfs_types::{NamespaceId, WalNo};
    use std::sync::Arc;

    /// Returns the charge of a content entry of `len` bytes under
    /// `object_key`, which includes the key and the cache's own allocation.
    fn content_charge(object_key: &str, len: usize) -> usize {
        let key = (
            ObjectStoreScope::new(0),
            ObjectKey::Content {
                object_key: object_key.to_owned(),
            },
        );
        charged_bytes(&key, &CachedObject::Content(Bytes::from(vec![0; len])))
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
        assert_eq!(cache.stats().cached_decoded_bytes, 2 * entry);

        cache.insert_content("large", &[3; 700]);
        assert!(
            cache.get_content("large", 700).is_none(),
            "an object over the size cap is neither kept nor looked up"
        );
        assert_eq!(cache.stats().content_inserts, 2);

        assert!(cache.get_content("a", 400).is_some());
        cache.insert_content("c", &[4; 400]);
        let stats = cache.stats();
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
            cache.stats().content_inserts,
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
        let stats = cache.stats();
        assert_eq!((stats.content_hits, stats.content_misses), (2, 2));
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
            shared.stats().cached_decoded_bytes,
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
            .entries
            .peek(&cache.content_key("empty"))
            .is_none());
        assert!(cache.get_content("empty", 0).is_none());
        let stats = cache.stats();
        assert_eq!((stats.content_inserts, stats.cached_decoded_bytes), (0, 0));
    }

    /// Checks that a zero budget keeps no objects and records no lookups of
    /// either kind.
    #[test]
    fn a_zero_budget_keeps_and_counts_nothing() {
        let cache = ObjectCache::unshared(0, 1_000);
        cache.insert_content("key", b"bytes");
        assert!(cache.get_content("key", 5).is_none());
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        assert!(cache.get_wal_object(&namespace_id, WalNo(1)).is_none());
        assert_eq!(cache.stats(), super::ObjectCacheStats::default());
    }
}
