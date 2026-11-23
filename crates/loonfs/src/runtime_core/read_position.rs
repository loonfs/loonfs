//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use super::RuntimeCore;
use crate::trace::phase_span;
use crate::NamespaceId;
use futures::future::BoxFuture;
use loonfs_core::cache::{CachedHead, HeadAndWalTailCache, SharedCheck, WalTailCacheKey};
use loonfs_core::control::{Held, HeldManifestForm, LoadedManifest};
use loonfs_core::publish::{PositionCheck, WriterPosition};
use loonfs_core::CoreError;
use loonfs_core::Phase;
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
        published: Held<LoadedManifest>,
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
    /// engine's own check is older (see [`Held::merge`]).
    pub(crate) fn prepare(&self, position: Option<WriterPosition>) -> Option<Arc<WriterPosition>> {
        let mut position = position?;
        if let Some(current) = self.current() {
            position.merge_check(current.basis());
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
        let read_basis = position.read_basis();
        self.head_and_wal_tail_cache.insert_tail(
            WalTailCacheKey {
                namespace_id: read_basis.head().namespace_id.clone(),
                manifest_no: read_basis.basis().manifest_no(),
                head_seq: read_basis.head().seq,
                wal_no: read_basis.head().wal_no,
            },
            Arc::clone(position.tail()),
        );
        self.head_and_wal_tail_cache.replace_head(CachedHead::new(
            position.basis().clone(),
            position.basis().checked().clone(),
        ));
        *self.lock() = None;
        true
    }

    /// Records `check`, a check that found its manifest the newest, on the
    /// position that the slot has now (see [`Held::merge`]).
    fn record_check<M: HeldManifestForm>(&self, check: &Held<M>) {
        let Some(current) = self.current() else {
            return;
        };
        let mut updated = WriterPosition::clone(&current);
        if !updated.merge_check(check) {
            return;
        }
        let updated = Arc::new(updated);
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
        let fresh = |position: &WriterPosition| {
            max_staleness_ms.is_some_and(|max_staleness_ms| {
                position.basis().checked().age_ms() < max_staleness_ms
            })
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

    /// Records `check`, a check that found its manifest the newest, on the
    /// position of the namespace's writer session (see
    /// [`ReadPositionSlot::record_check`]), and returns the position when a
    /// check that began at or after `check`'s confirms it.
    ///
    /// A check by another call, such as a snapshot's verification after its
    /// record write, then serves a `strong` read that started before
    /// `check`, as the session's own check does. A runtime without the
    /// namespace's writer session, a position on an older manifest, and a
    /// position that the check doesn't confirm return `None`.
    pub(crate) fn record_writer_position_check(
        &self,
        namespace_id: &NamespaceId,
        check: &Held<LoadedManifest>,
    ) -> Option<Arc<WriterPosition>> {
        let slot = self
            .inner
            .head_positions
            .get()
            .and_then(std::sync::Weak::upgrade)?
            .read_position_slot(namespace_id)?;
        slot.record_check(check);
        slot.current()
            .filter(|position| position.basis().is_confirmed_by(check))
    }

    /// Checks the position (see [`WriterPosition::check`]), and applies
    /// what the check found: it records a check that found the position's
    /// manifest the newest, moves the position onto the newest manifest of
    /// its writer epoch, as it moves onto the manifests this runtime
    /// publishes (see [`HeadPositions::move_position`]), or drops the
    /// position. A move or a drop waits for a publish that has the engine
    /// locked.
    async fn check_writer_position(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
    ) -> Result<(), CoreError> {
        let Some(position) = slot.current() else {
            return Ok(());
        };
        let observed = self.observation();
        let namespace = self.namespace_store(namespace_id);
        match position.check(&namespace, observed).await? {
            PositionCheck::Newest(checked) => slot.record_check(&checked),
            PositionCheck::MoveTo(newest) => {
                positions.move_position(namespace_id, newest.clone()).await;
                slot.record_check(&newest);
            }
            PositionCheck::Drop => {
                // The position becomes the cached head, which the check found
                // out of date, so the head is dropped after it, and the next
                // read loads the namespace by discovery.
                positions.drop_position(namespace_id).await;
                self.invalidate_namespace_read_cache(namespace_id);
            }
        }
        Ok(())
    }
}
