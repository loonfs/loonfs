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
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
};
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::manifest::ObjectStoreScope;
use bytes::Bytes;
use loonfs_types::format::wal::WalObjectEnvelope;
use loonfs_types::{NamespaceId, WalNo};
use std::sync::atomic::{AtomicUsize, Ordering};
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
            Self::Content(bytes) => bytes.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectCacheStats {
    pub wal_hits: usize,
    pub wal_misses: usize,
    pub wal_inserts: usize,
    pub content_hits: usize,
    pub content_misses: usize,
    pub content_inserts: usize,
    pub evictions: usize,
    pub cached_decoded_bytes: usize,
}

/// Lookups and inserts of one kind of object.
#[derive(Default)]
struct KindCounters {
    hits: AtomicUsize,
    misses: AtomicUsize,
    inserts: AtomicUsize,
    observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
}

impl KindCounters {
    fn new(observer: Option<Arc<dyn DecodedBlockCacheObserver>>) -> Self {
        Self {
            observer,
            ..Self::default()
        }
    }

    fn record_lookup<T>(&self, found: Option<T>) -> Option<T> {
        let counter = match found {
            Some(_) => &self.hits,
            None => &self.misses,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            match found {
                Some(_) => observer.hit(),
                None => observer.miss(),
            }
        }
        found
    }

    fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.observer {
            observer.insert();
        }
    }
}

/// Decoded WAL objects and small content objects for every runtime, under
/// one byte budget and one recency order.
///
/// An object heavier than the whole budget isn't kept, because inserting it
/// would evict everything else first.
pub struct SharedObjects {
    entries: DecodedBlockCache<(ObjectStoreScope, ObjectKey), CachedObject>,
    max_decoded_bytes: usize,
    max_cached_object_bytes: u64,
    wal: KindCounters,
    content: KindCounters,
}

impl std::fmt::Debug for SharedObjects {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedObjects")
            .field("max_decoded_bytes", &self.max_decoded_bytes)
            .field("max_cached_object_bytes", &self.max_cached_object_bytes)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl SharedObjects {
    /// `max_cached_object_bytes` is the largest content object the cache
    /// keeps. `observer` sees evictions and retained bytes across both kinds
    /// of entry, and `wal_observer` and `content_observer` see the lookups
    /// and inserts of one kind each.
    pub fn new(
        max_decoded_bytes: usize,
        max_cached_object_bytes: u64,
        observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
        wal_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
        content_observer: Option<Arc<dyn DecodedBlockCacheObserver>>,
    ) -> Self {
        Self {
            entries: DecodedBlockCache::new(DecodedBlockCacheConfig {
                max_decoded_bytes,
                observer,
            }),
            max_decoded_bytes,
            max_cached_object_bytes,
            wal: KindCounters::new(wal_observer),
            content: KindCounters::new(content_observer),
        }
    }

    pub fn stats(&self) -> ObjectCacheStats {
        let entries = self.entries.stats();
        let load = |counter: &AtomicUsize| counter.load(Ordering::Relaxed);
        ObjectCacheStats {
            wal_hits: load(&self.wal.hits),
            wal_misses: load(&self.wal.misses),
            wal_inserts: load(&self.wal.inserts),
            content_hits: load(&self.content.hits),
            content_misses: load(&self.content.misses),
            content_inserts: load(&self.content.inserts),
            evictions: entries.evictions,
            cached_decoded_bytes: entries.cached_decoded_bytes,
        }
    }

    fn insert(&self, key: (ObjectStoreScope, ObjectKey), object: CachedObject) -> bool {
        if self.max_decoded_bytes == 0 || object.weight() > self.max_decoded_bytes {
            return false;
        }
        self.entries.insert(key, object);
        true
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
                None,
                None,
                None,
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
        if self.shared.max_decoded_bytes == 0 {
            return None;
        }
        let found = match self.shared.entries.get(&self.wal_key(namespace_id, wal_no)) {
            Some(CachedObject::Wal(envelope)) => Some(envelope),
            Some(CachedObject::Content(_)) | None => None,
        };
        self.shared.wal.record_lookup(found)
    }

    /// Keeps a WAL object that decoded and matched the number and namespace
    /// of its key.
    pub(crate) fn insert_wal_object(&self, envelope: &Arc<WalObjectEnvelope>) {
        let payload = envelope.payload();
        if self.shared.insert(
            self.wal_key(&payload.namespace_id, payload.wal_no),
            CachedObject::Wal(Arc::clone(envelope)),
        ) {
            self.shared.wal.record_insert();
        }
    }

    /// Returns the first `end` bytes of the content object, if the cache
    /// has at least that many. A range that ends past the largest object
    /// the cache keeps isn't looked up.
    pub(crate) fn get_content(&self, object_key: &str, end: u64) -> Option<Bytes> {
        if self.shared.max_decoded_bytes == 0 || end > self.shared.max_cached_object_bytes {
            return None;
        }
        let found = match self.shared.entries.get(&self.content_key(object_key)) {
            Some(CachedObject::Content(bytes)) if bytes.len() as u64 >= end => Some(bytes),
            Some(CachedObject::Content(_) | CachedObject::Wal(_)) | None => None,
        };
        self.shared.content.record_lookup(found)
    }

    /// Keeps the first bytes of a content object after a read verified them
    /// against a content reference, unless the cache already has as many.
    pub(crate) fn insert_content(&self, object_key: &str, bytes: &[u8]) {
        if bytes.len() as u64 > self.shared.max_cached_object_bytes {
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
        if self
            .shared
            .insert(key, CachedObject::Content(Bytes::copy_from_slice(bytes)))
        {
            self.shared.content.record_insert();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectCache, ObjectStoreScope, SharedObjects};
    use std::sync::Arc;

    /// Checks that each content object is charged its length, that the least
    /// recently used object is evicted to stay within the budget, and that
    /// an object over the size cap isn't kept.
    #[test]
    fn content_inserts_are_charged_and_evicted_under_the_budget() {
        let cache = ObjectCache::unshared(1_000, 600);
        cache.insert_content("a", &[1; 400]);
        cache.insert_content("b", &[2; 400]);
        assert_eq!(cache.stats().cached_decoded_bytes, 800);

        cache.insert_content("large", &[3; 700]);
        assert!(
            cache.get_content("large", 700).is_none(),
            "an object over the size cap is neither kept nor looked up"
        );
        assert_eq!(cache.stats().content_inserts, 2);

        assert!(cache.get_content("a", 400).is_some());
        cache.insert_content("c", &[4; 400]);
        let stats = cache.stats();
        assert_eq!((stats.evictions, stats.cached_decoded_bytes), (1, 800));
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
        let shared = Arc::new(SharedObjects::new(1_000, 1_000, None, None, None));
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
        assert_eq!(shared.stats().cached_decoded_bytes, 10);
    }

    /// Checks that a zero budget keeps no objects and records no lookups.
    #[test]
    fn a_zero_budget_keeps_and_counts_nothing() {
        let cache = ObjectCache::unshared(0, 1_000);
        cache.insert_content("key", b"bytes");
        assert!(cache.get_content("key", 5).is_none());
        assert_eq!(cache.stats(), super::ObjectCacheStats::default());
    }
}
