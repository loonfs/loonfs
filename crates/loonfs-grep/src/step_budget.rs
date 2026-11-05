//! The limit on grep steps that keep file content or index segments, which
//! a host shares across its grep workers.

use loonfs::engine::{BudgetPermit, PermitPool, PoolCounts};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default for how many grep build and reorganize steps keep file content or
/// index segments at once.
pub const DEFAULT_MAX_CONCURRENT_GREP_STEPS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// How many grep build and reorganize steps keep file content or index
/// segments at once.
///
/// Clones share one set of permits. A host that passes clones of one budget
/// to every grep worker keeps one ceiling for all of them, however many
/// runtimes or stores those workers serve.
#[derive(Debug, Clone)]
pub struct GrepStepBudget {
    permits: Arc<PermitPool>,
}

impl GrepStepBudget {
    /// Creates a budget that lets `max_concurrent_steps` steps keep file
    /// content or index segments at once.
    pub fn new(max_concurrent_steps: NonZeroUsize) -> Self {
        Self {
            permits: Arc::new(PermitPool::new(max_concurrent_steps, None)),
        }
    }

    /// Reports the steps that have a permit and the steps that found work
    /// and wait for one.
    pub fn stats(&self) -> PoolCounts {
        self.permits.counts()
    }

    /// Waits for a permit to keep file content or index segments. Dropping
    /// the wait or the permit returns it.
    pub(crate) async fn permit(&self) -> BudgetPermit<'_> {
        self.permits.acquire().await
    }
}

impl Default for GrepStepBudget {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_GREP_STEPS)
    }
}
