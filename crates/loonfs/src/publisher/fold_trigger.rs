//! The rule that makes a fold of a namespace's WAL tail due.

use crate::RuntimeConfig;
use loonfs_core::limits::FOLD_AT_WAL_BYTES;
use std::num::NonZeroU64;

/// When a writable runtime folds a namespace's WAL tail.
///
/// A fold is due once the tail reaches a number of WAL objects or
/// `FOLD_AT_WAL_BYTES` of unfolded WAL bytes, or once the tail's newest
/// commit reaches the runtime's idle fold age. A writer session folds at the
/// sizes after its own publishes, and a host's metadata call also folds a
/// tail that reaches the idle fold age (see
/// [`PublisherRegistry::fold_namespace`](super::PublisherRegistry::fold_namespace)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldTrigger {
    idle_fold_after_ms: Option<NonZeroU64>,
}

impl FoldTrigger {
    /// Returns the fold rule of a runtime with `config`.
    pub(crate) fn new(config: &RuntimeConfig) -> Self {
        Self {
            idle_fold_after_ms: config.idle_fold_after_ms,
        }
    }

    /// Returns whether a tail of `objects` WAL objects and `wal_bytes`
    /// unfolded WAL bytes has reached `max_wal_tail_objects` WAL objects or
    /// `FOLD_AT_WAL_BYTES`.
    pub(crate) fn fold_is_due(
        &self,
        max_wal_tail_objects: u64,
        objects: u64,
        wal_bytes: usize,
    ) -> bool {
        objects >= max_wal_tail_objects || wal_bytes >= FOLD_AT_WAL_BYTES
    }

    /// Returns when a tail whose newest commit is at `newest_commit_at_ms`
    /// reaches the idle fold age, on the clock that read
    /// `newest_commit_at_ms`, or `None` when the runtime doesn't fold idle
    /// tails.
    pub(crate) fn idle_fold_due_at_ms(&self, newest_commit_at_ms: u64) -> Option<u64> {
        self.idle_fold_after_ms
            .map(|after_ms| newest_commit_at_ms.saturating_add(after_ms.get()))
    }
}
