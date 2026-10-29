//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use crate::fs::RuntimeCore;
use crate::trace::phase_span;
use crate::NamespaceId;
use futures::future::BoxFuture;
use loonfs_core::cache::{CachedHead, HeadAndWalTailCache, SharedCheck, WalTailCacheKey};
use loonfs_core::publish::WriterPosition;
use loonfs_core::time::Observation;
use loonfs_core::Phase;
use loonfs_core::PublishedManifest;
use loonfs_core::{CoreError, MetadataProjectionLoadError};
use loonfs_types::ManifestNo;
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::Instrument;

/// The writer sessions of a writable runtime, as its reads see them.
///
/// The publisher registry implements this trait. The runtime core keeps a
/// weak reference to the registry, because the registry keeps the core.
pub(crate) trait HeadPositions: Send + Sync {
    /// Returns the position slot of the namespace's writer session, or
    /// `None` when the runtime has no session for the namespace.
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>>;

    /// Drops the position of the namespace's writer session, both the copy
    /// that reads take and the commit engine's own, once no other task has
    /// the engine locked, so the session's next publish loads the namespace
    /// from the store.
    fn drop_position<'a>(&'a self, namespace_id: &'a NamespaceId) -> BoxFuture<'a, ()>;

    /// Moves the position of the namespace's writer session onto
    /// `published`, a manifest above the position's, the way the session's
    /// own publications move it, once no other task has the engine locked.
    fn move_position<'a>(
        &'a self,
        namespace_id: &'a NamespaceId,
        published: PublishedManifest,
    ) -> BoxFuture<'a, ()>;
}

/// The position of one writer session's commit engine, kept where reads can
/// take it without locking the engine.
///
/// The publisher stores the engine's position here each time a task
/// releases the engine, so a read after a commit's acknowledgement sees the
/// commit.
#[derive(Debug)]
pub(crate) struct ReadPositionSlot {
    /// The runtime's read cache, where [`Self::clear`] installs the position
    /// as the namespace's cached head, with the position's tail.
    head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
    position: Mutex<Option<Arc<WriterPosition>>>,
    /// The session's checks of the position, which the reads that need a
    /// check share.
    check: SharedCheck<Result<(), CoreError>>,
}

impl ReadPositionSlot {
    /// Returns an empty slot for a writer session in the runtime whose read
    /// cache is `head_and_wal_tail_cache`.
    pub(crate) fn new(head_and_wal_tail_cache: Arc<HeadAndWalTailCache>) -> Self {
        Self {
            head_and_wal_tail_cache,
            position: Mutex::default(),
            check: SharedCheck::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<WriterPosition>>> {
        // A critical section under this lock only moves an `Arc`, so it can't
        // panic and poison the lock.
        self.position
            .lock()
            .expect("the read position lock should not be poisoned")
    }

    /// Returns the position that reads start from, or `None` when reads take
    /// the read cache.
    pub(crate) fn current(&self) -> Option<Arc<WriterPosition>> {
        self.lock().clone()
    }

    /// Returns the engine's position as the slot will store it.
    ///
    /// A read's check of the position that the slot has now stays when the
    /// engine's own check is older. A check that found no successor of a
    /// manifest also confirms every later manifest, because each later
    /// manifest was published after the check.
    pub(crate) fn prepare(&self, position: Option<WriterPosition>) -> Option<Arc<WriterPosition>> {
        let mut position = position?;
        if let Some(current) = self.current() {
            if current.basis_checked.age_ms() < position.basis_checked.age_ms() {
                position.basis_checked = current.basis_checked.clone();
            }
        }
        Some(Arc::new(position))
    }

    /// Stores a position that [`Self::prepare`] returned. Only
    /// [`Self::clear`] removes a position.
    pub(crate) fn store(&self, position: Arc<WriterPosition>) {
        *self.lock() = Some(position);
    }

    /// Removes the position, so reads take the namespace's cached head until
    /// the engine's next release stores one again, and returns whether the
    /// slot had a position.
    ///
    /// The position becomes the namespace's cached head first, through
    /// [`HeadAndWalTailCache::replace_head`](loonfs_core::cache::HeadAndWalTailCache::replace_head),
    /// with both checks at the position's last successor check, because
    /// that check confirmed a head that this runtime wrote itself. So the
    /// first read after the position starts from a head at least as new as
    /// the reads from the position returned, whatever older load finishes
    /// after it.
    ///
    /// The position's WAL tail goes into the read cache before the head,
    /// under the key of the position's manifest and head, so the next read
    /// from that head takes the tail without replaying the WAL. A tail under
    /// its key is never wrong (see
    /// [`RuntimeCore::invalidate_namespace_read_cache`]), so the slot hands
    /// the tail over at every end of a position. After another writer's
    /// claim, the caller then drops the cached head, and the tail stays
    /// unused unless a later load finds the same head.
    pub(crate) fn clear(&self) -> bool {
        let Some(position) = self.current() else {
            return false;
        };
        self.head_and_wal_tail_cache.insert_tail(
            WalTailCacheKey {
                namespace_id: position.read_basis.head.namespace_id.clone(),
                manifest_no: position.read_basis.basis.manifest_no(),
                head_seq: position.read_basis.head.seq,
            },
            Arc::clone(&position.tail),
        );
        self.head_and_wal_tail_cache.replace_head(CachedHead {
            read_basis: position.read_basis.clone(),
            basis_checked: position.basis_checked.clone(),
            head_confirmed: position.basis_checked.clone(),
        });
        *self.lock() = None;
        true
    }

    /// Records that a check at `checked` found no successor of manifest
    /// `manifest_no`.
    ///
    /// The check applies to the position that the slot has now if that
    /// position's manifest is `manifest_no` or a later one, as
    /// [`Self::prepare`] explains, unless the position has a later check
    /// already.
    fn record_check(&self, checked: Observation, manifest_no: ManifestNo) {
        let Some(current) = self.current() else {
            return;
        };
        if current.read_basis.basis.manifest_no() < manifest_no
            || current.basis_checked.age_ms() <= checked.age_ms()
        {
            return;
        }
        let updated = Arc::new(WriterPosition {
            basis_checked: checked,
            ..WriterPosition::clone(&current)
        });
        let mut slot = self.lock();
        if slot
            .as_ref()
            .is_some_and(|position| Arc::ptr_eq(position, &current))
        {
            *slot = Some(updated);
        }
    }
}

impl RuntimeCore {
    /// Returns the position of the namespace's writer session in this
    /// runtime for a read with `max_staleness_ms` to start from, or `None`
    /// when the read starts from the read cache instead.
    ///
    /// A read takes the position as it is while the position's last check is
    /// younger than the read's maximum staleness. A read without one (a
    /// `strong` read), or past it, needs a check of the position (see
    /// [`Self::check_writer_position`]). It waits for the check in flight,
    /// and then takes the position if that check confirmed it within the
    /// read's maximum staleness, or else shares the next check or runs it
    /// (see [`SharedCheck`]). So a wave of concurrent reads costs at most two
    /// checks.
    ///
    /// A check is trusted only while its answer arrives less than
    /// `READ_REVALIDATION_BOUND_MS` after the position's last check (format
    /// section 4.2), so a position past that bound is dropped without one.
    pub(crate) async fn checked_writer_position(
        &self,
        namespace_id: &NamespaceId,
        max_staleness_ms: Option<u64>,
    ) -> Result<Option<Arc<WriterPosition>>, CoreError> {
        let Some(positions) = self
            .inner
            .head_positions
            .get()
            .and_then(std::sync::Weak::upgrade)
        else {
            return Ok(None);
        };
        let Some(slot) = positions.read_position_slot(namespace_id) else {
            return Ok(None);
        };
        let Some(position) = slot.current() else {
            return Ok(None);
        };
        if !position.basis_checked.is_within_revalidation_bound() {
            positions.drop_position(namespace_id).await;
            return Ok(None);
        }
        let fresh = |position: &WriterPosition| {
            max_staleness_ms
                .is_some_and(|max_staleness_ms| position.basis_checked.age_ms() < max_staleness_ms)
        };
        if fresh(&position) {
            return Ok(Some(position));
        }
        slot.check
            .run(
                || {
                    slot.current()
                        .filter(|position| fresh(position))
                        .map(|_| Ok(()))
                },
                || {
                    self.check_writer_position(namespace_id, positions.as_ref(), &slot)
                        .instrument(phase_span!(
                            self,
                            Phase::PositionManifestProbe,
                            namespace_id
                        ))
                },
            )
            .await?;
        Ok(slot.current())
    }

    /// Checks the position's manifest for a successor with one HEAD.
    ///
    /// A HEAD that finds nothing records the check. A HEAD that finds a
    /// successor starts the walk of [`same_epoch_manifests_above`]. Each
    /// manifest with the position's writer epoch (a fold, a compaction, a
    /// checkpoint, a snapshot, or a retention advance, by this runtime or by
    /// another process) describes the head this session wrote, so the
    /// position moves onto them in order, as it moves onto the manifests this
    /// runtime publishes (see [`HeadPositions::move_position`]). The walk
    /// that ends on the newest manifest is that manifest's successor check,
    /// so a position that moved onto it records the check. Anything else
    /// the walk finds, or an answer that arrives `READ_REVALIDATION_BOUND_MS`
    /// or more after the position's last check, drops the position. A move
    /// or a drop waits for a publish that has the engine locked.
    async fn check_writer_position(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
    ) -> Result<(), CoreError> {
        let Some(position) = slot.current() else {
            return Ok(());
        };
        let observed = Observation::now(Arc::clone(&self.inner.timer));
        let namespace = self.namespace_store(namespace_id);
        let successor = namespace
            .manifest_has_successor(position.read_basis.basis.manifest_no())
            .await
            .map_err(MetadataProjectionLoadError::LoadHead)?;
        let moves = if successor {
            namespace
                .same_epoch_manifests_above(
                    position.read_basis.basis.manifest(),
                    position.read_basis.head.writer_epoch,
                )
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?
        } else {
            Some(Vec::new())
        };
        let Some(moves) = moves.filter(|_| position.basis_checked.is_within_revalidation_bound())
        else {
            // The position becomes the cached head, which a successor or a
            // late answer makes out of date, so the head is dropped after it,
            // and the next read loads the namespace by discovery.
            positions.drop_position(namespace_id).await;
            self.invalidate_namespace_read_cache(namespace_id);
            return Ok(());
        };
        let newest = moves
            .last()
            .map_or(position.read_basis.basis.manifest_no(), |moved| {
                moved.manifest.manifest_no
            });
        for published in moves {
            positions.move_position(namespace_id, published).await;
        }
        slot.record_check(observed, newest);
        Ok(())
    }
}
