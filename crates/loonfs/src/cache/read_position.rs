//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use crate::fs::RuntimeCore;
use crate::NamespaceId;
use loonfs_core::cache::{CachedHead, HeadRecord, HeadStateCache};
use loonfs_core::control::{manifest_has_successor, same_epoch_manifests_above};
use loonfs_core::publish::ReadPosition;
use loonfs_core::time::Observation;
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
    /// The session's last finished check of the position. A read that runs a
    /// check keeps this locked until the check ends, so one check runs at a
    /// time and the reads that need one wait for it. (It's a `tokio` mutex
    /// because it stays locked across the check's requests.)
    check: tokio::sync::Mutex<Option<FinishedCheck>>,
}

/// When the session's last check of its position started, and how it ended.
#[derive(Debug)]
struct FinishedCheck {
    observed: Observation,
    outcome: Result<(), CoreError>,
}

impl ReadPositionSlot {
    /// Returns an empty slot for the writer session of `namespace_id`, in the
    /// runtime whose read cache is `head_state`.
    pub(crate) fn new(namespace_id: &NamespaceId, head_state: Arc<HeadStateCache>) -> Self {
        Self {
            record: head_state.record(namespace_id),
            head_state,
            position: Mutex::default(),
            check: tokio::sync::Mutex::default(),
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
        if current.basis.manifest_no() < manifest_no
            || current.basis_checked.age_ms() <= checked.age_ms()
        {
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
}

impl RuntimeCore {
    /// Returns the position of the namespace's writer session in this
    /// runtime for a read with `max_staleness_ms` to start from, or `None`
    /// when the read starts from the read cache instead.
    ///
    /// A read takes the position as it is while the position's last check is
    /// younger than the read's maximum staleness. A read without one (a
    /// `strong` read), or past it, waits for a check of the position (see
    /// [`Self::check_writer_position`]) that started after the read did. So a
    /// read that finds a check running waits for that check and then for the
    /// next one, which every read that arrives during the first check
    /// shares, and a wave of concurrent reads costs at most two checks.
    ///
    /// A check is trusted only while its answer arrives less than
    /// `READ_REVALIDATION_BOUND_MS` after the position's last check (format
    /// section 4.2), so a position past that bound is dropped without one.
    pub(crate) async fn checked_writer_position(
        &self,
        namespace_id: &NamespaceId,
        max_staleness_ms: Option<u64>,
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
        if max_staleness_ms.is_some_and(|max_staleness_ms| checked.age_ms() < max_staleness_ms) {
            return Ok(Some(position));
        }
        let started = Observation::now(Arc::clone(&self.inner.timer));
        let mut last = slot.check.lock().await;
        if !last
            .as_ref()
            .is_some_and(|check| check.observed.is_after(&started))
        {
            let observed = Observation::now(Arc::clone(&self.inner.timer));
            let outcome = self
                .check_writer_position(namespace_id, positions.as_ref(), &slot, &observed)
                .instrument(tracing::debug_span!(
                    target: "loonfs::page",
                    "loonfs.phase",
                    phase = "position_manifest_probe"
                ))
                .await;
            *last = Some(FinishedCheck { observed, outcome });
        }
        if let Some(FinishedCheck {
            outcome: Err(error),
            ..
        }) = last.as_ref()
        {
            return Err(error.clone());
        }
        drop(last);
        Ok(slot.current())
    }

    /// Checks the position's manifest for a successor with one HEAD, sent
    /// after `observed`.
    ///
    /// A HEAD that finds nothing records the check. A HEAD that finds a
    /// successor starts the walk of [`same_epoch_manifests_above`]. Each
    /// manifest with the position's writer epoch (a fold, a compaction, a
    /// checkpoint, a snapshot, or a retention advance, by this runtime or by
    /// another process) describes the head this session wrote, so the
    /// position moves onto them in order, as it moves onto the manifests this
    /// runtime publishes (see [`HeadPositions::move_position`]). The walk
    /// that ends on the newest manifest is that manifest's successor check,
    /// so a position that moved onto it records the check. A position that a
    /// locked engine keeps where it was records none, because its manifest
    /// has a successor. Anything else the walk finds, or an answer that
    /// arrives `READ_REVALIDATION_BOUND_MS` or more after the position's last
    /// check, drops the position.
    async fn check_writer_position(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
        observed: &Observation,
    ) -> Result<(), CoreError> {
        let Some(position) = slot.current() else {
            return Ok(());
        };
        let successor =
            manifest_has_successor(self.store(), namespace_id, position.basis.manifest_no())
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?;
        let moves = if successor {
            same_epoch_manifests_above(
                self.store(),
                namespace_id,
                position.basis.manifest(),
                position.head.writer_epoch,
            )
            .await
            .map_err(MetadataProjectionLoadError::LoadHead)?
        } else {
            Some(Vec::new())
        };
        let Some(moves) = moves.filter(|_| position.basis_checked.is_within_revalidation_bound())
        else {
            // The position becomes the cached head, which a successor or a
            // late answer makes out of date, so the head is marked
            // unconfirmed after.
            positions.drop_position(namespace_id);
            self.invalidate_namespace_read_cache(namespace_id);
            return Ok(());
        };
        let newest = moves.last().map_or(position.basis.manifest_no(), |moved| {
            moved.manifest().manifest_no
        });
        for published in moves {
            positions.move_position(namespace_id, published);
        }
        slot.record_check(observed.clone(), newest);
        Ok(())
    }
}
