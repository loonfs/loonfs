//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use crate::fs::RuntimeCore;
use crate::NamespaceId;
use loonfs_core::cache::HeadStateCache;
use loonfs_core::control::manifest_has_successor;
use loonfs_core::publish::ReadPosition;
use loonfs_core::time::Observation;
use loonfs_core::{CoreError, MetadataProjectionLoadError};
use loonfs_types::ManifestNo;
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

    /// Applies a position change that waits for the namespace's commit
    /// engine, if no task has the engine locked, and returns whether the
    /// engine was free. It never waits for the engine.
    fn apply_pending_position(&self, namespace_id: &NamespaceId) -> bool;
}

/// The position of one writer session's commit engine, kept where reads can
/// take it without locking the engine.
///
/// The publisher stores the engine's position here each time a task
/// releases the engine, so a read after a commit's acknowledgement sees the
/// commit.
#[derive(Debug)]
pub(crate) struct ReadPositionSlot {
    /// The namespace of the writer session.
    namespace_id: NamespaceId,
    /// The runtime's read cache, whose anchor for the namespace
    /// [`Self::clear`] drops.
    head_state: Arc<HeadStateCache>,
    position: Mutex<Option<Arc<ReadPosition>>>,
    /// Set while one read checks the position's manifest for a successor,
    /// so concurrent reads send one HEAD.
    checking: AtomicBool,
    /// The numbers of the manifests above the position's manifest that this
    /// runtime published for the namespace, in order, recorded before the
    /// engine's position moves to them. At most [`MAX_PUBLISHED_AHEAD`].
    published: Mutex<Vec<ManifestNo>>,
}

/// The most manifests above the position's that a slot remembers as this
/// runtime's own. A runtime publishes a few manifests between two releases
/// of the engine at most, and a check that finds a manifest the slot forgot
/// drops the position, which costs one reload.
const MAX_PUBLISHED_AHEAD: usize = 16;

impl ReadPositionSlot {
    /// Returns an empty slot for the writer session of `namespace_id`, in the
    /// runtime whose read cache is `head_state`.
    pub(crate) fn new(namespace_id: NamespaceId, head_state: Arc<HeadStateCache>) -> Self {
        Self {
            namespace_id,
            head_state,
            position: Mutex::default(),
            checking: AtomicBool::default(),
            published: Mutex::default(),
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

    /// Stores a position that [`Self::prepare`] returned, and returns whether
    /// the slot had no position before. Only [`Self::clear`] removes a
    /// position.
    pub(crate) fn store(&self, position: Arc<ReadPosition>) -> bool {
        let basis_no = position.basis.manifest_no();
        self.lock_published()
            .retain(|manifest_no| *manifest_no > basis_no);
        self.lock().replace(position).is_none()
    }

    /// Removes the position, so reads take the read cache until the engine's
    /// next release stores one again.
    ///
    /// When the slot has a position, the read cache's anchor for the
    /// namespace is dropped first, so the first read after the position
    /// loads or confirms the head. A read that was already confirming an
    /// anchor when the position started can store that anchor after the
    /// position started, while reads take the position. Without the drop, a
    /// bounded read after the position ends could read at that anchor's
    /// head, which is older than the reads from the position returned.
    pub(crate) fn clear(&self) {
        if self.current().is_some() {
            self.head_state.invalidate_anchor(&self.namespace_id);
        }
        *self.lock() = None;
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

    fn lock_published(&self) -> MutexGuard<'_, Vec<ManifestNo>> {
        // A critical section under this lock only edits a short vector, so it
        // can't panic and poison the lock.
        self.published
            .lock()
            .expect("the published manifest lock should not be poisoned")
    }

    /// Records that this runtime published manifest `manifest_no` for the
    /// namespace, before anything waits for the engine to move the position
    /// there.
    pub(crate) fn record_published(&self, manifest_no: ManifestNo) {
        let mut published = self.lock_published();
        if let Err(index) = published.binary_search(&manifest_no) {
            published.insert(index, manifest_no);
        }
        if published.len() > MAX_PUBLISHED_AHEAD {
            published.remove(0);
        }
    }

    /// Whether this runtime published manifest `manifest_no` itself.
    fn published_here(&self, manifest_no: ManifestNo) -> bool {
        self.lock_published().binary_search(&manifest_no).is_ok()
    }

    /// Returns the newest manifest in the run of consecutive manifests that
    /// this runtime published, starting at `first`.
    fn published_run_end(&self, first: ManifestNo) -> ManifestNo {
        let published = self.lock_published();
        let mut end = first;
        while let Ok(next) = end.successor() {
            if published.binary_search(&next).is_err() {
                break;
            }
            end = next;
        }
        end
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
    /// successor that another process published, such as a new writer's
    /// claim or a compaction, drops the position, and the read cache's
    /// reload finds the successor. Concurrent reads share one check, and a
    /// read that finds another read's check running starts from the
    /// position without waiting. With an interval of zero, every read checks
    /// for itself, so each read sees a successor published before the read
    /// started.
    ///
    /// A successor can also be a manifest that this runtime published
    /// itself (a fold, a compaction, a checkpoint, a snapshot, or a
    /// retention advance) whose move of the position still waits for the
    /// engine. The check then goes on past this runtime's own manifests (see
    /// `position_past_own_manifests`), so a new writer's claim after them
    /// still drops the position. The takeover window is one interval either
    /// way, and with an interval of zero a read sees every manifest that
    /// another process published before the read started.
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
            let successor_no = position.basis.manifest_no().successor().ok();
            let kept = match successor_no.filter(|no| slot.published_here(*no)) {
                Some(successor_no) => {
                    self.position_past_own_manifests(
                        namespace_id,
                        positions.as_ref(),
                        &slot,
                        position,
                        successor_no,
                    )
                    .await?
                }
                None => None,
            };
            if kept.is_none() {
                positions.drop_position(namespace_id);
            }
            return Ok(kept);
        }
        slot.record_check(observed);
        Ok(Some(position))
    }

    /// Returns the position for a read whose check found `successor_no`, a
    /// manifest that this runtime published itself, or `None` when the read
    /// drops the position.
    ///
    /// The check goes on past this runtime's own manifests with one HEAD,
    /// for the first manifest number that this runtime didn't publish. A
    /// manifest there is another process's, such as a new writer's claim.
    /// Otherwise the read starts from the position, moved to this runtime's
    /// manifest if the engine is free to apply the move, and records no
    /// check, because the position's manifest has a successor.
    async fn position_past_own_manifests(
        &self,
        namespace_id: &NamespaceId,
        positions: &dyn HeadPositions,
        slot: &ReadPositionSlot,
        position: Arc<ReadPosition>,
        successor_no: ManifestNo,
    ) -> Result<Option<Arc<ReadPosition>>, CoreError> {
        let newest_own = slot.published_run_end(successor_no);
        let foreign = manifest_has_successor(self.store(), namespace_id, newest_own)
            .instrument(tracing::debug_span!(
                target: "loonfs::page",
                "loonfs.phase",
                phase = "position_manifest_probe"
            ))
            .await
            .map_err(MetadataProjectionLoadError::LoadHead)?;
        if foreign || !position.basis_checked.is_within_revalidation_bound() {
            return Ok(None);
        }
        if positions.apply_pending_position(namespace_id) {
            let moved = slot
                .current()
                .filter(|moved| moved.basis.manifest_no() >= successor_no);
            if moved.is_some() {
                return Ok(moved);
            }
        }
        Ok(Some(position))
    }
}
