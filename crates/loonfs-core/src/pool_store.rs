//! [`PoolStore`]: one pool's object store with the caches, clocks, content
//! write permits, and run policy that reading the pool's members uses.

use crate::control_object::{expect_identity_field, load_control_object, ControlObjectLoadError};
use crate::error::{CoreError, Result};
use crate::namespace::head_cache::HeadAndWalTailCache;
use crate::namespace::store::NamespaceStore;
use crate::object_cache::ObjectCache;
use crate::permit_pool::PermitPool;
use crate::segment::{MetadataRunPolicy, MetadataSegmentCache};
use crate::time::OperationClock;
use loonfs_objectstore::keys::pool_record;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ControlObjectKind, PoolPayload};
use loonfs_types::{MonotonicTimer, NamespaceId, PoolId, WallClock};
use std::sync::Arc;

/// One pool's object store, with the components that a member's
/// [`NamespaceStore`] is built from.
#[derive(Debug, Clone)]
pub struct PoolStore<S> {
    pub(crate) store: S,
    pub(crate) pool_id: PoolId,
    /// The decoded segment blocks that member reads go through, with the
    /// read working memory.
    pub(crate) segment_cache: Arc<MetadataSegmentCache>,
    pub(crate) head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
    pub(crate) object_cache: Arc<ObjectCache>,
    pub(crate) wall_clock: Arc<dyn WallClock>,
    pub(crate) timer: Arc<dyn MonotonicTimer>,
    pub(crate) content_writes: Arc<PermitPool>,
    pub(crate) run_policy: MetadataRunPolicy,
}

impl<S> PoolStore<S> {
    /// Returns the components of `pool_id` in `store`.
    #[allow(
        clippy::too_many_arguments,
        reason = "each component is required, and each has its own type"
    )]
    pub fn new(
        store: S,
        pool_id: PoolId,
        segment_cache: Arc<MetadataSegmentCache>,
        head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
        object_cache: Arc<ObjectCache>,
        wall_clock: Arc<dyn WallClock>,
        timer: Arc<dyn MonotonicTimer>,
        content_writes: Arc<PermitPool>,
        run_policy: MetadataRunPolicy,
    ) -> Self {
        Self {
            store,
            pool_id,
            segment_cache,
            head_and_wal_tail_cache,
            object_cache,
            wall_clock,
            timer,
            content_writes,
            run_policy,
        }
    }

    /// Returns the components of `namespace_id`, a member of the pool, over a
    /// borrow of this store.
    pub(crate) fn member(&self, namespace_id: NamespaceId) -> NamespaceStore<&S> {
        NamespaceStore::new(
            &self.store,
            namespace_id,
            Arc::clone(&self.segment_cache),
            Arc::clone(&self.head_and_wal_tail_cache),
            Arc::clone(&self.object_cache),
            Arc::clone(&self.wall_clock),
            Arc::clone(&self.timer),
            Arc::clone(&self.content_writes),
            self.run_policy,
        )
    }

    /// Starts the clock of one operation at the current times of the wall
    /// clock and the timer.
    pub fn start_clock(&self) -> OperationClock {
        OperationClock::start(self.wall_clock.now_ms(), Arc::clone(&self.timer))
    }

    /// Returns the pool these components belong to.
    pub fn pool_id(&self) -> &PoolId {
        &self.pool_id
    }
}

impl<S: ObjectStore> PoolStore<S> {
    /// Loads the pool's record, or returns `None` if it's missing.
    pub(crate) async fn load_pool_record(&self) -> Result<Option<PoolPayload>> {
        let loaded = load_control_object(
            &self.store,
            pool_record(&self.pool_id),
            ControlObjectKind::Pool,
            |object_key, record: &PoolPayload| {
                expect_identity_field(
                    object_key,
                    "pool id",
                    self.pool_id.as_str(),
                    record.pool_id.as_str(),
                )
            },
        )
        .await;
        match loaded {
            Ok(loaded) => Ok(Some(loaded.state)),
            Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
            Err(error) => Err(CoreError::ControlObjectLoad(error)),
        }
    }
}
