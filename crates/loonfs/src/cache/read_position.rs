//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use crate::fs::RuntimeCore;
use crate::NamespaceId;
use loonfs_core::cache::{CachedHead, HeadRecord, HeadStateCache};
use loonfs_core::control::{manifest_has_successor, manifests_above};
use loonfs_core::publish::ReadPosition;
use loonfs_core::time::Observation;
use loonfs_core::PublishedManifest;
use loonfs_core::{CoreError, MetadataProjectionLoadError};
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// that reads take and the commit engine's own, so the session's next
    /// publish loads the namespace from the store.
    fn drop_position(&self, namespace_id: &NamespaceId);

    /// Moves the position of the namespace's writer session onto
    /// `published`, a manifest above the position's, the way the session's
    /// own publications move it: at once when no task has the engine
    /// locked, otherwise when the task that has it releases it. It never
    /// waits for the engine.
    fn move_position(&self, namespace_id: &NamespaceId, published: PublishedManifest);
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
    /// as the namespace's cached head.
    head_state: Arc<HeadStateCache>,
    /// The namespace's head record, which the slot keeps while the session
    /// lasts. A change that the runtime learns of while the session has a
    /// position stays on the record, so the position's install when it ends
    /// doesn't keep a confirmation from before the change.
    record: Arc<HeadRecord>,
    position: Mutex<Option<Arc<ReadPosition>>>,
    /// Set while one read checks the position's manifest for a successor,
    /// so concurrent reads send one HEAD.
    checking: AtomicBool,
}

impl ReadPositionSlot {
    /// Returns an empty slot for the writer session of `namespace_id`, in the
    /// runtime whose read cache is `head_state`.
    pub(crate) fn new(namespace_id: &NamespaceId, head_state: Arc<HeadStateCache>) -> Self {
        Self {
            record: head_state.record(namespace_id),
            head_state,
            position: Mutex::default(),
            checking: AtomicBool::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<ReadPosition>>> {
        // A critical section under this lock only moves an `Arc`, so it can't
        // panic and poison the lock.
        self.position
            .lock()
            .expect("the read position lock should not be poisoned")
    }

    /// Returns the position that reads start from, or `None` when reads take
    /// the read cache.
    pub(crate) fn current(&self) -> Option<Arc<ReadPosition>> {
        self.lock().clone()
    }

    /// Returns the engine's position as the slot will store it.
    ///
    /// A read's check of the position that the slot has now stays when the
    /// engine's own check is older. A check that found no successor of a
    /// manifest also confirms every later manifest, because each later
    /// manifest was published after the check.
    pub(crate) fn prepare(&self, position: Option<ReadPosition>) -> Option<Arc<ReadPosition>> {
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
    pub(crate) fn store(&self, position: Arc<ReadPosition>) {
        *self.lock() = Some(position);
    }

    /// Removes the position, so reads take the namespace's cached head until
    /// the engine's next release stores one again, and returns whether the
    /// slot had a position.
    ///
    /// The position becomes the namespace's cached head first, through
    /// [`HeadStateCache::replace_head`](loonfs_core::cache::HeadStateCache::replace_head),
    /// with both checks at the position's last successor check, because
    /// that check confirmed a head that this runtime wrote itself. So the
    /// first read after the position starts from a head at least as new as
    /// the reads from the position returned, whatever older load finishes
    /// after it.
    pub(crate) fn clear(&self) -> bool {
        let Some(position) = self.current() else {
            return false;
        };
        self.head_state.replace_head(
            &self.record,
            CachedHead {
                head: position.head.clone(),
                basis: position.basis.clone(),
                basis_checked: Some(position.basis_checked.clone()),
                head_confirmed: Some(position.basis_checked.clone()),
            },
        );
        *self.lock() = None;
        true
    }

    /// Records that a HEAD sent at `checked` found no successor of the
    /// manifest of the position that the read took.
    ///
    /// The check applies to the position that the slot has now, as
    /// [`Self::prepare`] explains, unless that position has a later check
    /// already or the slot has none.
    fn record_check(&self, checked: Observation) {
        let Some(current) = self.current() else {
            return;
        };
        if current.basis_checked.age_ms() <= checked.age_ms() {
            return;
        }
        let updated = Arc::new(ReadPosition {
            basis_checked: checked,
            ..ReadPosition::clone(&current)
        });
        let mut slot = self.lock();
        if slot
            .as_ref()
            .is_some_and(|position| Arc::ptr_eq(position, &current))
        {
            *slot = Some(updated);
        }
    }

    /// Starts a successor check of the position's manifest, or returns
    /// `None` while another read's check runs.
    fn start_check(&self) -> Option<PositionCheck<'_>> {
        // Acquire pairs with the Release in `PositionCheck::drop`, so the
        // read that starts the next check sees the check recorded before it.
        self.checking
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| PositionCheck { slot: self })
    }
}

/// One read's successor check of a position's manifest. Dropping it, also
/// when the read is cancelled, lets the next read check.
struct PositionCheck<'a> {
    slot: &'a ReadPositionSlot,
}

impl Drop for PositionCheck<'_> {
    fn drop(&mut self) {
        // Release pairs with the Acquire in `ReadPositionSlot::start_check`.
        self.slot.checking.store(false, Ordering::Release);
    }
}

impl RuntimeCore {
    /// Returns the position of the namespace's writer session in this
    /// runtime for a read to start from, or `None` when the read starts from
    /// the read cache instead.
    ///
    /// A read checks the position's manifest for a successor with one HEAD
    /// once the last check is a manifest revalidation interval old. A
    /// successor of a later writer epoch is a new writer's claim, which
    /// drops the position, and the read cache's reload finds the claim.
    /// Concurrent reads share one check, and a read that finds another
    /// read's check running starts from the position without waiting. With
    /// an interval of zero, every read checks for itself, so each read sees
    /// a claim published before the read started.
    ///
    /// A successor that keeps the position's writer epoch (a fold, a
    /// compaction, a checkpoint, a snapshot, or a retention advance, by this
    /// runtime or by another process) describes the head this session
    /// wrote, so the position moves onto it and the read starts from the
    /// position (see `position_past_same_epoch_successors`). A claim above
    /// such manifests still drops the position, so the takeover window is
    /// one interval either way.
    ///
    /// A check is trusted only while its answer arrives less than
    /// `READ_REVALIDATION_BOUND_MS` after the position's last check (format
    /// section 4.2), so a position past that bound is dropped without one.
    pub(crate) async fn checked_writer_position(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<Option<Arc<ReadPosition>>, CoreError> {
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
        let checked = &position.basis_checked;
        if !checked.is_within_revalidation_bound() {
            positions.drop_position(namespace_id);
            return Ok(None);
        }
        let interval_ms = self.inner.config.manifest_revalidation_interval_ms;
        if checked.age_ms() < interval_ms {
            return Ok(Some(position));
        }
        let _check = match slot.start_check() {
            Some(check) => Some(check),
            None if interval_ms == 0 => None,
            None => return Ok(Some(position)),
        };
        let observed = Observation::now(Arc::clone(&self.inner.timer));
        let successor =
            manifest_has_successor(self.store(), namespace_id, position.basis.manifest_no())
                .instrument(tracing::debug_span!(
                    target: "loonfs::page",
                    "loonfs.phase",
                    phase = "position_manifest_probe"
                ))
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?;
        if !checked.is_within_revalidation_bound() {
            positions.drop_position(namespace_id);
            return Ok(None);
        }
        if successor {
            let kept = self
                .position_past_same_epoch_successors(
                    namespace_id,
                    positions.as_ref(),
                    &slot,
                    position,
                    observed,
                )
                .await?;
            if kept.is_none() {
                // The position becomes the cached head, which a successor
                // makes out of date, so the head is marked unconfirmed after.
                positions.drop_position(namespace_id);
                self.invalidate_namespace_read_cache(namespace_id);
            }
            return Ok(kept);
        }
        slot.record_check(observed);
        Ok(Some(position))
    }

    /// Returns the position for a read whose check found a successor of the
    /// position's manifest, or `None` when the read drops the position.
    ///
    /// The read loads the manifests above the position's. While the newest
    /// is an active manifest of the position's writer epoch, each describes
    /// the head this session wrote, so the position moves onto them in
    /// order, as it moves onto the manifests this runtime publishes (see
    /// [`HeadPositions::move_position`]). A manifest of a later epoch is a
    /// new writer's claim, and a tombstone ends the namespace, so the read
    /// drops the position. The walk that ends on the newest manifest is
    /// that manifest's successor check, so a position that moved onto it
    /// records the check. A position that a locked engine keeps where it
    /// was records none, because its manifest has a successor.
    async fn position_past_same_epoch_successors(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
        position: Arc<ReadPosition>,
        observed: Observation,
    ) -> Result<Option<Arc<ReadPosition>>, CoreError> {
        let above = manifests_above(self.store(), namespace_id, position.basis.manifest_no())
            .instrument(tracing::debug_span!(
                target: "loonfs::page",
                "loonfs.phase",
                phase = "position_manifest_probe"
            ))
            .await
            .map_err(MetadataProjectionLoadError::LoadHead)?;
        let Some(newest) = above.last() else {
            return Ok(None);
        };
        let payload = newest.envelope.payload();
        if payload.writer_epoch != position.head.writer_epoch
            || payload.status.is_deleted()
            || !position.basis_checked.is_within_revalidation_bound()
        {
            return Ok(None);
        }
        let newest_no = newest.manifest().manifest_no;
        let mut head_seq = position.basis.0.head_seq;
        for manifest in &above {
            positions.move_position(namespace_id, PublishedManifest::above(manifest, head_seq));
            head_seq = manifest.manifest().head_seq;
        }
        let current = slot.current();
        if current
            .as_ref()
            .is_some_and(|moved| moved.basis.manifest_no() == newest_no)
        {
            slot.record_check(observed);
            return Ok(slot.current());
        }
        Ok(current)
    }
}
