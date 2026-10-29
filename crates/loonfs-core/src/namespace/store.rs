//! [`NamespaceStore`]: one namespace's object store with the caches, clocks,
//! content write permits, and run policy that its operations use. Each
//! operation is a method in the module that implements it.

use crate::manifest::{MetadataRunPolicy, MetadataSegmentCache};
use crate::permit_pool::PermitPool;
use loonfs_types::{MonotonicTimer, NamespaceId, WallClock};
use std::sync::Arc;

/// One namespace's object store, with the segment cache, clocks, content
/// write permits, and run policy that the namespace's operations use.
#[derive(Debug, Clone)]
pub struct NamespaceStore<S> {
    pub(crate) store: S,
    pub(crate) namespace_id: NamespaceId,
    /// The decoded segment blocks that reads, folds, and compactions go
    /// through, with the read working memory and the read-ahead window.
    pub(crate) segment_cache: Arc<MetadataSegmentCache>,
    /// The clock behind every timestamp an operation records.
    pub(crate) wall_clock: Arc<dyn WallClock>,
    /// The timer behind every deadline and observation an operation starts.
    pub(crate) timer: Arc<dyn MonotonicTimer>,
    /// Where every content write takes its permit.
    pub(crate) content_writes: Arc<PermitPool>,
    /// The budgets that folds and compactions run under.
    pub(crate) run_policy: MetadataRunPolicy,
}

impl<S> NamespaceStore<S> {
    /// Returns the components of `namespace_id` in `store`.
    pub fn new(
        store: S,
        namespace_id: NamespaceId,
        segment_cache: Arc<MetadataSegmentCache>,
        wall_clock: Arc<dyn WallClock>,
        timer: Arc<dyn MonotonicTimer>,
        content_writes: Arc<PermitPool>,
        run_policy: MetadataRunPolicy,
    ) -> Self {
        Self {
            store,
            namespace_id,
            segment_cache,
            wall_clock,
            timer,
            content_writes,
            run_policy,
        }
    }

    /// Returns the same components for `namespace_id`, over a borrow of this
    /// store, such as for a fork's target.
    pub fn with_namespace(&self, namespace_id: NamespaceId) -> NamespaceStore<&S> {
        NamespaceStore {
            store: &self.store,
            namespace_id,
            segment_cache: Arc::clone(&self.segment_cache),
            wall_clock: Arc::clone(&self.wall_clock),
            timer: Arc::clone(&self.timer),
            content_writes: Arc::clone(&self.content_writes),
            run_policy: self.run_policy,
        }
    }

    /// Returns the namespace these components belong to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }
}
