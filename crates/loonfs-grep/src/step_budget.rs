//! The limit on grep steps that hold file content or index segments, which
//! a host shares across its grep workers.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Semaphore, SemaphorePermit};

/// Default for how many grep build and reorganize steps hold file content or
/// index segments at once.
pub const DEFAULT_MAX_CONCURRENT_GREP_STEPS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// How many grep build and reorganize steps hold file content or index
/// segments at once.
///
/// Clones share one set of permits. A host that passes clones of one budget
/// to every grep worker keeps one ceiling for all of them, however many
/// runtimes or stores those workers serve.
#[derive(Debug, Clone)]
pub struct GrepStepBudget {
    inner: Arc<GrepStepBudgetInner>,
}

#[derive(Debug)]
struct GrepStepBudgetInner {
    permits: Semaphore,
    max_concurrent_steps: usize,
    waiting: AtomicUsize,
}

/// Steps that hold a permit of a [`GrepStepBudget`], and steps that wait for
/// one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrepStepBudgetStats {
    /// Steps that hold a permit.
    pub running: usize,
    /// Steps that found work and wait for a permit.
    pub waiting: usize,
}

impl GrepStepBudget {
    /// Creates a budget that lets `max_concurrent_steps` steps hold file
    /// content or index segments at once.
    pub fn new(max_concurrent_steps: NonZeroUsize) -> Self {
        let max_concurrent_steps = max_concurrent_steps.get().min(Semaphore::MAX_PERMITS);
        Self {
            inner: Arc::new(GrepStepBudgetInner {
                permits: Semaphore::new(max_concurrent_steps),
                max_concurrent_steps,
                waiting: AtomicUsize::new(0),
            }),
        }
    }

    /// Reports the steps running and waiting right now.
    pub fn stats(&self) -> GrepStepBudgetStats {
        GrepStepBudgetStats {
            running: self.inner.max_concurrent_steps - self.inner.permits.available_permits(),
            waiting: self.inner.waiting.load(Ordering::Relaxed),
        }
    }

    /// Waits for a permit to hold file content or index segments. Dropping
    /// the wait or the permit returns it.
    pub(crate) async fn permit(&self) -> SemaphorePermit<'_> {
        let _waiting = WaitingStep::new(&self.inner.waiting);
        self.inner
            .permits
            .acquire()
            .await
            .expect("grep step permit semaphore should remain open")
    }
}

impl Default for GrepStepBudget {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_GREP_STEPS)
    }
}

/// A step counted as waiting until its wait ends, whether it got a permit
/// or was dropped.
struct WaitingStep<'a> {
    waiting: &'a AtomicUsize,
}

impl<'a> WaitingStep<'a> {
    fn new(waiting: &'a AtomicUsize) -> Self {
        waiting.fetch_add(1, Ordering::Relaxed);
        Self { waiting }
    }
}

impl Drop for WaitingStep<'_> {
    fn drop(&mut self) {
        self.waiting.fetch_sub(1, Ordering::Relaxed);
    }
}
