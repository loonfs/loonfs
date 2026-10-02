//! The work in flight that one or more writable runtimes share.

use crate::metrics::{ExecutionBudgetInstruments, MetricsRecorder, PermitPoolGauges};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{Semaphore, SemaphorePermit};

#[cfg(test)]
mod tests;

/// Default maximum WAL-tail folds that the runtimes sharing one budget run at
/// once.
pub const DEFAULT_MAX_CONCURRENT_FOLDS: usize = 2;
/// Default maximum metadata merges, bounded or streaming, that the runtimes
/// sharing one budget run at once.
pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 2;

/// Work in flight that one or more writable runtimes share: WAL folds,
/// metadata merges, and the decoded input one merge may hold.
///
/// Each [`LoonFs`](crate::LoonFs) built with the budget takes its fold and
/// compaction permits from it, so the limits bound the work of every runtime
/// that shares it. Waits are first come, first served, whichever runtime
/// they belong to. No runtime has a reserved share, so an idle runtime holds
/// nothing back. A host that wants to isolate runtimes from each other gives
/// them separate budgets. Clones share the budget. A writable runtime built
/// without one creates a private budget with the default limits.
#[derive(Clone)]
pub struct ExecutionBudget {
    inner: Arc<ExecutionBudgetInner>,
}

struct ExecutionBudgetInner {
    folds: PermitPool,
    compactions: PermitPool,
    max_merge_input_bytes: NonZeroUsize,
}

impl fmt::Debug for ExecutionBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionBudget")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Default for ExecutionBudget {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ExecutionBudget {
    /// Starts a budget builder with the default limits and no metrics
    /// recorder.
    pub fn builder() -> ExecutionBudgetBuilder {
        ExecutionBudgetBuilder {
            max_concurrent_folds: const { NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_FOLDS).unwrap() },
            max_concurrent_compactions: const {
                NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_COMPACTIONS).unwrap()
            },
            max_merge_input_bytes: const {
                NonZeroUsize::new(
                    loonfs_types::format::sst_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES,
                )
                .unwrap()
            },
            metrics_recorder: None,
        }
    }

    /// Snapshots the work running and waiting under the budget, summed over
    /// every runtime that shares it.
    pub fn stats(&self) -> ExecutionBudgetStats {
        let folds = self.inner.folds.counts();
        let compactions = self.inner.compactions.counts();
        ExecutionBudgetStats {
            folds_running: folds.running,
            folds_waiting: folds.waiting,
            compactions_running: compactions.running,
            compactions_waiting: compactions.waiting,
        }
    }

    /// Waits for a fold permit. Every fold holds one, and so does every
    /// operation that may fold first: a namespace deletion, and the creation
    /// of a checkpoint, a snapshot, or a fork of the current head.
    pub(crate) async fn fold_permit(&self) -> BudgetPermit<'_> {
        self.inner.folds.acquire().await
    }

    /// Waits for a compaction permit. Every bounded compaction step and every
    /// streaming compaction holds one.
    pub(crate) async fn compaction_permit(&self) -> BudgetPermit<'_> {
        self.inner.compactions.acquire().await
    }

    /// The decoded metadata bytes one merge may hold.
    pub(crate) fn max_merge_input_bytes(&self) -> NonZeroUsize {
        self.inner.max_merge_input_bytes
    }
}

/// Builder for an [`ExecutionBudget`].
#[must_use]
pub struct ExecutionBudgetBuilder {
    max_concurrent_folds: NonZeroUsize,
    max_concurrent_compactions: NonZeroUsize,
    max_merge_input_bytes: NonZeroUsize,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl ExecutionBudgetBuilder {
    /// Sets the maximum number of WAL tails folded at once. Session folds,
    /// [`Maintenance`](crate::Maintenance) folds, namespace deletions, and
    /// the creation of checkpoints, snapshots, and forks of the current head
    /// all take a fold permit, because each may fold. Defaults to
    /// [`DEFAULT_MAX_CONCURRENT_FOLDS`].
    pub fn max_concurrent_folds(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_folds = limit;
        self
    }

    /// Sets the maximum number of metadata merges run at once, bounded
    /// compaction steps and streaming compactions alike, whether sessions or
    /// [`Maintenance`](crate::Maintenance) calls start them. A merge never
    /// holds a fold permit. Defaults to
    /// [`DEFAULT_MAX_CONCURRENT_COMPACTIONS`].
    pub fn max_concurrent_compactions(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_compactions = limit;
        self
    }

    /// Sets the decoded metadata bytes one merge may hold. A maintenance step
    /// merges inline only the runs that fit; a larger window runs as a
    /// streaming compaction that holds at most this much at once. Defaults
    /// to 64 MiB.
    pub fn max_merge_input_bytes(mut self, max_merge_input_bytes: NonZeroUsize) -> Self {
        self.max_merge_input_bytes = max_merge_input_bytes;
        self
    }

    /// Installs the metrics recorder the budget reports its running and
    /// waiting work to (see [`crate::metrics`]). The budget registers its
    /// gauges once, when it is built. A budget built without one reports
    /// nothing. Give each budget its own recorder: two budgets on one
    /// recorder set the same gauges.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Builds the budget.
    pub fn build(self) -> ExecutionBudget {
        let ExecutionBudgetInstruments { folds, compactions } =
            ExecutionBudgetInstruments::new(self.metrics_recorder.as_deref());
        ExecutionBudget {
            inner: Arc::new(ExecutionBudgetInner {
                folds: PermitPool::new(self.max_concurrent_folds, folds),
                compactions: PermitPool::new(self.max_concurrent_compactions, compactions),
                max_merge_input_bytes: self.max_merge_input_bytes,
            }),
        }
    }
}

/// Work running and waiting under an [`ExecutionBudget`], summed over every
/// runtime that shares it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionBudgetStats {
    /// Folds, and operations that may fold first, holding a fold permit.
    pub folds_running: usize,
    /// Folds, and operations that may fold first, waiting for a fold permit.
    pub folds_waiting: usize,
    /// Metadata merges holding a compaction permit.
    pub compactions_running: usize,
    /// Metadata merges waiting for a compaction permit.
    pub compactions_waiting: usize,
}

/// The permits for one kind of work, and the count of the work that holds
/// them or waits for them.
struct PermitPool {
    permits: Semaphore,
    counts: Mutex<PoolCounts>,
    gauges: PermitPoolGauges,
}

#[derive(Debug, Clone, Copy, Default)]
struct PoolCounts {
    running: usize,
    waiting: usize,
}

impl PermitPool {
    fn new(limit: NonZeroUsize, gauges: PermitPoolGauges) -> Self {
        Self {
            permits: Semaphore::new(limit.get().min(Semaphore::MAX_PERMITS)),
            counts: Mutex::default(),
            gauges,
        }
    }

    async fn acquire(&self) -> BudgetPermit<'_> {
        self.update(|counts| counts.waiting += 1);
        let mut permit = BudgetPermit {
            pool: self,
            permit: None,
        };
        let acquired = self
            .permits
            .acquire()
            .await
            .expect("execution budget permits should remain open");
        self.update(|counts| {
            counts.waiting -= 1;
            counts.running += 1;
        });
        permit.permit = Some(acquired);
        permit
    }

    fn counts(&self) -> PoolCounts {
        *self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn update(&self, change: impl FnOnce(&mut PoolCounts)) {
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        change(&mut counts);
        self.gauges.report(counts.running, counts.waiting);
    }
}

/// A permit from an [`ExecutionBudget`], or a place in the queue for one.
/// Dropping it returns the permit or gives up the place.
pub(crate) struct BudgetPermit<'a> {
    pool: &'a PermitPool,
    permit: Option<SemaphorePermit<'a>>,
}

impl Drop for BudgetPermit<'_> {
    fn drop(&mut self) {
        let permit = self.permit.take();
        let running = permit.is_some();
        self.pool.update(|counts| {
            if running {
                counts.running -= 1;
            } else {
                counts.waiting -= 1;
            }
        });
        // The count falls before the permit returns, so the running count
        // never passes the limit.
        drop(permit);
    }
}
