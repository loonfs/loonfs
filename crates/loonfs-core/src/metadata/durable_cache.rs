//! [`DurableVisibilityCache`]: the batch-scoped memo of durable-layer row
//! lookups shared across a publish batch's candidates, plus the cache key
//! types and the shared-row handle its hits return.

use super::visibility::BindingIdentity;
use super::{DirentryBindRecord, DirentryUnbindRecord, InodeRecord, SubtreeTombstoneRecord};
use loonfs_api::{InodeId, NameKey};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Batch-scoped memo of durable-layer lookups: everything except the
/// mutation overlay (manifest segments, WAL tail, in-memory base). Publish
/// batches attach one so repeated path walks across candidates scan each
/// durable key once; the overlay — the only layer that changes between
/// candidates — is composed per lookup on top.
///
/// Cached vectors are raw, unfiltered rows; callers apply their own seq
/// filters, so answers hold regardless of the composed view's visible seq.
#[derive(Debug, Default)]
pub(crate) struct DurableVisibilityCache {
    inner: Mutex<DurableVisibilityCacheInner>,
}

#[derive(Debug, Default)]
pub(super) struct DurableVisibilityCacheInner {
    pub(super) inodes: HashMap<InodeId, Option<InodeRecord>>,
    pub(super) binds_for_parent_name: HashMap<ParentNameCacheKey, Arc<Vec<DirentryBindRecord>>>,
    pub(super) binds_for_child: HashMap<InodeId, Arc<Vec<DirentryBindRecord>>>,
    pub(super) unbinds_for_binding: HashMap<BindingIdentity, Arc<Vec<DirentryUnbindRecord>>>,
    pub(super) tombstones_for_root: HashMap<InodeId, Arc<Vec<SubtreeTombstoneRecord>>>,
    hits: u64,
    misses: u64,
}

/// A durable row set handed out of the shared cache plus the composed
/// view's overlay rows for the same key. Iteration chains the two, so a
/// cache hit never copies the cached vector — only the overlay rows (empty
/// outside commit validation) are owned per lookup.
pub(crate) struct SharedRows<T> {
    pub(super) durable: Arc<Vec<T>>,
    pub(super) overlay: Vec<T>,
}

impl<T> SharedRows<T> {
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.durable.iter().chain(self.overlay.iter())
    }
}

/// Hit/miss counts, pinning cache engagement in tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableVisibilityCacheStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
}

impl DurableVisibilityCache {
    #[cfg(test)]
    pub(crate) fn stats(&self) -> DurableVisibilityCacheStats {
        let inner = self.lock();
        DurableVisibilityCacheStats {
            hits: inner.hits,
            misses: inner.misses,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DurableVisibilityCacheInner> {
        self.inner
            .lock()
            .expect("durable visibility cache lock should not be poisoned")
    }

    pub(super) fn get<K: std::hash::Hash + Eq, V: Clone>(
        &self,
        map: impl FnOnce(&mut DurableVisibilityCacheInner) -> &mut HashMap<K, V>,
        key: &K,
    ) -> Option<V> {
        let mut inner = self.lock();
        let hit = map(&mut inner).get(key).cloned();
        if hit.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        hit
    }

    pub(super) fn insert<K: std::hash::Hash + Eq, V>(
        &self,
        map: impl FnOnce(&mut DurableVisibilityCacheInner) -> &mut HashMap<K, V>,
        key: K,
        value: V,
    ) {
        let mut inner = self.lock();
        map(&mut inner).insert(key, value);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ParentNameCacheKey {
    pub(super) parent_inode_id: InodeId,
    pub(super) name_key: NameKey,
}
