//! The rule that makes a fold of a namespace's WAL tail due.

use crate::InlineContentPolicy;
use std::num::NonZeroU64;

/// When a writable runtime folds a namespace's WAL tail.
///
/// A fold is due once the tail reaches a number of WAL objects or the
/// runtime's inline fold size, or once the tail's newest commit reaches the
/// runtime's idle fold age. A writer session folds at the sizes after its own
/// publishes, and [`Maintenance`](crate::Maintenance) also folds a tail that
/// reaches the idle fold age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldTrigger {
    inline_content_fold_at_bytes: usize,
    idle_fold_after_ms: Option<NonZeroU64>,
}

impl FoldTrigger {
    pub(crate) fn new(
        inline_content: &InlineContentPolicy,
        idle_fold_after_ms: Option<NonZeroU64>,
    ) -> Self {
        Self {
            inline_content_fold_at_bytes: inline_content.inline_content_fold_at_bytes,
            idle_fold_after_ms,
        }
    }

    /// Returns whether a tail of `objects` WAL objects with `inline_bytes` of
    /// inline content has reached `max_wal_tail_objects` WAL objects or the
    /// inline fold size.
    pub(crate) fn fold_is_due(
        &self,
        max_wal_tail_objects: u64,
        objects: u64,
        inline_bytes: usize,
    ) -> bool {
        objects >= max_wal_tail_objects || inline_bytes >= self.inline_content_fold_at_bytes
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

impl Default for FoldTrigger {
    fn default() -> Self {
        Self::new(
            &InlineContentPolicy::default(),
            NonZeroU64::new(crate::config::DEFAULT_IDLE_FOLD_AFTER_MS),
        )
    }
}
