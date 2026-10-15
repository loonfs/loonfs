//! The positions of the writer sessions in this runtime, which reads of a
//! namespace with an open writer session start from.

use crate::NamespaceId;
use loonfs_core::publish::ReadPosition;
use std::sync::{Arc, Mutex, MutexGuard};

/// The writer sessions of a writable runtime, as its reads see them.
///
/// The publisher registry implements this trait. The runtime core keeps a
/// weak reference to the registry, because the registry keeps the core.
pub(crate) trait HeadPositions: Send + Sync {
    /// Returns the position slot of the namespace's writer session, or
    /// `None` when the runtime has no session for the namespace.
    fn read_position_slot(&self, namespace_id: &NamespaceId) -> Option<Arc<ReadPositionSlot>>;
}

/// The position of one writer session's commit engine, kept where reads can
/// take it without locking the engine.
///
/// The publisher stores the engine's position here each time a task
/// releases the engine, so a read after a commit's acknowledgement sees the
/// commit.
#[derive(Debug, Default)]
pub(crate) struct ReadPositionSlot {
    position: Mutex<Option<Arc<ReadPosition>>>,
}

impl ReadPositionSlot {
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
    /// the slot had no position before.
    pub(crate) fn store(&self, position: Option<Arc<ReadPosition>>) -> bool {
        let mut current = self.lock();
        let started = current.is_none() && position.is_some();
        *current = position;
        started
    }

    /// Removes the position, so reads take the read cache until the engine's
    /// next release stores one again.
    pub(crate) fn clear(&self) {
        *self.lock() = None;
    }
}
