//! Caches of decoded WAL objects and small content objects, keyed by the
//! object each entry was read from.
//!
//! Both kinds of object are immutable. A WAL number identifies one object
//! that's never replaced or recreated, and a content object is written once
//! and verified against its reference's checksum when it is read from the
//! store. So LoonFS doesn't invalidate entries here, and recency evicts them
//! to stay within each kind's byte budget.
//!
//! The caches sit below the path-level access check. LoonFS replays WAL
//! objects into the metadata view that every read checks access against, and
//! it reads a content object through a content location that a metadata view
//! resolved after checking the caller's access to the path or inode. So a
//! caller reaches an entry that another caller added only after passing the
//! same check.

use crate::block_cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
    DecodedBlockKey,
};
use crate::heap_bytes::{arc_bytes, HeapBytes};
use crate::manifest::ObjectStoreScope;
use bytes::Bytes;
use loonfs_types::format::wal::WalObjectEnvelope;
use loonfs_types::{NamespaceId, WalNo};
use std::sync::Arc;

impl DecodedBlockKey for WalNo {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl DecodedBlock for Arc<WalObjectEnvelope> {
    fn weight(&self) -> usize {
        arc_bytes::<WalObjectEnvelope>() + HeapBytes::heap_bytes(&**self)
    }
}

impl DecodedBlock for Bytes {
    fn weight(&self) -> usize {
        // Cloning owned Bytes also allocates its shared buffer metadata.
        self.len() + 3 * std::mem::size_of::<usize>()
    }
}

/// Decoded WAL objects and small content objects for every runtime, each
/// kind under a byte budget and a recency order of its own.
///
/// The two kinds don't share a budget, so a burst of reads of content
/// objects of up to 1 MiB can't evict the WAL objects that readers replay,
/// and replays can't evict the content objects that reads keep.
pub struct SharedObjects {
    wal_objects: DecodedBlockCache<(ObjectStoreScope, NamespaceId, WalNo), Arc<WalObjectEnvelope>>,
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
            .field("wal_object_stats", &self.wal_object_stats())
            .field("content_object_stats", &self.content_object_stats())
            .finish_non_exhaustive()
    }
}

impl SharedObjects {
    /// Returns shared objects that keep up to `max_wal_object_bytes` of
    /// decoded WAL objects and up to `max_content_object_bytes` of content
    /// objects, and no content object larger than `max_cached_object_bytes`.
    pub fn new(
        max_wal_object_bytes: usize,
        max_content_object_bytes: usize,
        max_cached_object_bytes: u64,
    ) -> Self {
        Self {
            wal_objects: DecodedBlockCache::new(DecodedBlockCacheConfig::new(max_wal_object_bytes)),
            content_objects: DecodedBlockCache::new(DecodedBlockCacheConfig::new(
                max_content_object_bytes,
            )),
            max_cached_object_bytes,
        }
    }

    /// Returns the counters and occupancy of the WAL objects.
    pub fn wal_object_stats(&self) -> DecodedBlockCacheStats {
        self.wal_objects.stats()
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

    /// A view over objects no other view reads, with a budget of
    /// `max_decoded_bytes` for each kind of object.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_decoded_bytes: usize, max_cached_object_bytes: u64) -> Self {
        Self::new(
            Arc::new(SharedObjects::new(
                max_decoded_bytes,
                max_decoded_bytes,
                max_cached_object_bytes,
            )),
            ObjectStoreScope::new(0),
        )
    }

    /// Counters of the WAL objects in the whole shared cache, across every
    /// view.
    pub fn wal_object_stats(&self) -> DecodedBlockCacheStats {
        self.shared.wal_object_stats()
    }

    /// Counters of the content objects in the whole shared cache, across
    /// every view.
    pub fn content_object_stats(&self) -> DecodedBlockCacheStats {
        self.shared.content_object_stats()
    }

    fn wal_key(
        &self,
        namespace_id: &NamespaceId,
        wal_no: WalNo,
    ) -> (ObjectStoreScope, NamespaceId, WalNo) {
        (self.scope, namespace_id.clone(), wal_no)
    }

    fn content_key(&self, object_key: &str) -> (ObjectStoreScope, String) {
        (self.scope, object_key.to_owned())
    }

    /// Returns the decoded WAL object at `wal_no`, if the cache has it.
    pub(crate) fn get_wal_object(
        &self,
        namespace_id: &NamespaceId,
        wal_no: WalNo,
    ) -> Option<Arc<WalObjectEnvelope>> {
        self.shared
            .wal_objects
            .get(&self.wal_key(namespace_id, wal_no))
    }

    /// Keeps a WAL object that decoded and matched the number and namespace
    /// of its key.
    pub(crate) fn insert_wal_object(&self, envelope: &Arc<WalObjectEnvelope>) {
        let payload = envelope.payload();
        self.shared.wal_objects.insert(
            self.wal_key(&payload.namespace_id, payload.wal_no),
            Arc::clone(envelope),
        );
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
    use loonfs_types::format::wal::{
        encode_wal_object_envelope_zstd, WalObjectEnvelope, WalObjectPayload,
    };
    use loonfs_types::{ChangeSeq, InodeId, NamespaceId, WalNo, WriterEpoch};
    use std::sync::Arc;

    /// Returns the charge of a content entry of `len` bytes under
    /// `object_key`, which includes the key and the cache's own allocation.
    fn content_charge(object_key: &str, len: usize) -> usize {
        let key = (ObjectStoreScope::new(0), object_key.to_owned());
        charged_bytes(&key, &Bytes::from(vec![0; len]))
    }

    /// Returns a WAL object of `namespace_id` with no commits.
    fn wal_object(namespace_id: &NamespaceId, wal_no: u64) -> Arc<WalObjectEnvelope> {
        let payload = WalObjectPayload {
            namespace_id: namespace_id.clone(),
            wal_no: WalNo(wal_no),
            writer_epoch: WriterEpoch(1),
            head_seq: ChangeSeq(wal_no),
            next_inode_id: InodeId(2),
            records: Vec::new(),
        };
        Arc::new(
            encode_wal_object_envelope_zstd(payload)
                .expect("encode a WAL object")
                .into_envelope(),
        )
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

    /// Checks that a burst of content objects of the largest size the cache
    /// keeps evicts no WAL object, and that a burst of WAL objects evicts no
    /// content object, because each kind has a budget of its own.
    #[test]
    fn each_kind_of_object_evicts_only_its_own_kind() {
        const MIB: usize = 1024 * 1024;
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let wal_entry = charged_bytes(
            &(ObjectStoreScope::new(0), namespace_id.clone(), WalNo(1)),
            &wal_object(&namespace_id, 1),
        );
        let content_entry = content_charge("content-0", MIB);
        // Each budget fits two entries of its kind but not three.
        let shared = Arc::new(SharedObjects::new(
            2 * wal_entry + wal_entry / 2,
            2 * content_entry + content_entry / 2,
            MIB as u64,
        ));
        let cache = ObjectCache::new(Arc::clone(&shared), ObjectStoreScope::new(0));
        for wal_no in [1, 2] {
            cache.insert_wal_object(&wal_object(&namespace_id, wal_no));
        }

        let content = vec![7; MIB];
        for index in 0..16 {
            cache.insert_content(&format!("content-{index}"), &content);
        }
        for wal_no in [1, 2] {
            assert!(
                cache.get_wal_object(&namespace_id, WalNo(wal_no)).is_some(),
                "content objects don't evict WAL object {wal_no}"
            );
        }
        assert_eq!(shared.content_object_stats().evictions, 14);

        for wal_no in 3..19 {
            cache.insert_wal_object(&wal_object(&namespace_id, wal_no));
        }
        for index in [14, 15] {
            assert!(
                cache
                    .get_content(&format!("content-{index}"), MIB as u64)
                    .is_some(),
                "WAL objects don't evict content object {index}"
            );
        }
        assert_eq!(
            (
                shared.wal_object_stats().evictions,
                shared.content_object_stats().evictions
            ),
            (16, 14)
        );
    }

    /// Checks that views over two object stores read their own entries for
    /// the same object key under one budget, and that two views over one
    /// object store read the same entry.
    #[test]
    fn views_read_only_the_objects_of_their_own_object_store() {
        let shared = Arc::new(SharedObjects::new(1_000, 1_000, 1_000));
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

    /// Checks that a zero budget keeps no objects and records no lookups of
    /// either kind.
    #[test]
    fn a_zero_budget_keeps_and_counts_nothing() {
        let cache = ObjectCache::unshared(0, 1_000);
        cache.insert_content("key", b"bytes");
        assert!(cache.get_content("key", 5).is_none());
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        cache.insert_wal_object(&wal_object(&namespace_id, 1));
        assert!(cache.get_wal_object(&namespace_id, WalNo(1)).is_none());
        let nothing = DecodedBlockCacheStats::default();
        assert_eq!(
            (cache.wal_object_stats(), cache.content_object_stats()),
            (nothing, nothing)
        );
    }
}
