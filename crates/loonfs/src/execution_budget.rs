//! The work in flight that one or more writable runtimes share.

use crate::metrics::{
    AdmissionInstruments, ExecutionBudgetInstruments, MetricsRecorder, PermitPoolGauges,
};
use crate::CoreError;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{Semaphore, SemaphorePermit};

#[cfg(test)]
mod tests;

/// Default maximum WAL-tail folds that the runtimes sharing one budget run at
/// once.
pub const DEFAULT_MAX_CONCURRENT_FOLDS: usize = 2;
/// Default maximum metadata merges, bounded or streaming, that the runtimes
/// sharing one budget run at once.
pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: usize = 2;
pub(crate) const DEFAULT_MAX_CONCURRENT_PUBLICATIONS: usize = 8;
const DEFAULT_MAX_ADMITTED_REQUESTS: usize = 8192;
const DEFAULT_MAX_ADMITTED_BYTES: usize = 64 * 1024 * 1024;

/// Work in flight that one or more writable runtimes share: admitted
/// publication requests, running publications, WAL folds, metadata merges,
/// and the decoded input one merge may hold.
///
/// Each [`LoonFs`](crate::LoonFs) built with the budget charges the
/// publication requests it admits to the budget's totals, and takes its
/// publication, fold, and compaction permits from it, so the limits bound the
/// work of every runtime that shares it. Admission never waits: a request
/// past an admitted total fails at once with `commit_queue_full`. Permit
/// waits are first come, first served, whichever runtime they belong to. No
/// runtime has a reserved share, so an idle runtime holds nothing back. A
/// host that wants to isolate runtimes from each other gives them separate
/// budgets. The per-namespace admission limits stay with each runtime (see
/// [`PublicationLimits`](crate::PublicationLimits)). Clones share the budget.
/// A writable runtime built without one creates a private budget with the
/// default limits.
#[derive(Clone)]
pub struct ExecutionBudget {
    inner: Arc<ExecutionBudgetInner>,
}

struct ExecutionBudgetInner {
    admission: AdmittedTotals,
    publications: PermitPool,
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
            max_admitted_requests: const { NonZeroUsize::new(DEFAULT_MAX_ADMITTED_REQUESTS).unwrap() },
            max_admitted_bytes: const { NonZeroUsize::new(DEFAULT_MAX_ADMITTED_BYTES).unwrap() },
            max_concurrent_publications: const {
                NonZeroUsize::new(DEFAULT_MAX_CONCURRENT_PUBLICATIONS).unwrap()
            },
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

    /// Snapshots the work admitted, running, and waiting under the budget,
    /// summed over every runtime that shares it.
    pub fn stats(&self) -> ExecutionBudgetStats {
        let admitted = *self.inner.admission.lock();
        let publications = self.inner.publications.counts();
        let folds = self.inner.folds.counts();
        let compactions = self.inner.compactions.counts();
        ExecutionBudgetStats {
            admitted_requests: admitted.requests,
            admitted_bytes: admitted.bytes,
            publications_running: publications.running,
            publications_waiting: publications.waiting,
            folds_running: folds.running,
            folds_waiting: folds.waiting,
            compactions_running: compactions.running,
            compactions_waiting: compactions.waiting,
        }
    }

    /// Charges one admitted publication request of `estimated_bytes` to the
    /// totals. A request past either total is refused with
    /// `commit_queue_full` and charges nothing.
    pub(crate) fn charge_admission(&self, estimated_bytes: usize) -> Result<(), CoreError> {
        self.inner.admission.charge(estimated_bytes)
    }

    /// Returns what [`Self::charge_admission`] charged, once the request's
    /// work settles.
    pub(crate) fn refund_admission(&self, estimated_bytes: usize) {
        self.inner.admission.refund(estimated_bytes);
    }

    /// Waits for a publication permit. A namespace's worker holds one while
    /// it publishes a batch.
    pub(crate) async fn publication_permit(&self) -> BudgetPermit<'_> {
        self.inner.publications.acquire().await
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
    max_admitted_requests: NonZeroUsize,
    max_admitted_bytes: NonZeroUsize,
    max_concurrent_publications: NonZeroUsize,
    max_concurrent_folds: NonZeroUsize,
    max_concurrent_compactions: NonZeroUsize,
    max_merge_input_bytes: NonZeroUsize,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl ExecutionBudgetBuilder {
    /// Sets the maximum publication requests admitted and not yet settled,
    /// across every namespace of every runtime that shares the budget. Every
    /// admitted caller counts, including duplicate commits and namespace
    /// deletes, until its work settles, even if the caller disconnects. A
    /// request past the limit fails at once with `commit_queue_full`.
    /// Defaults to 8,192.
    pub fn max_admitted_requests(mut self, limit: NonZeroUsize) -> Self {
        self.max_admitted_requests = limit;
        self
    }

    /// Sets the approximate retained bytes of the publication requests
    /// admitted and not yet settled, counted like
    /// [`Self::max_admitted_requests`]. The estimate includes request data,
    /// prepared proofs, and waiter overhead; it is not a bound on allocator
    /// capacity or on the working memory of a publication. A request past the
    /// limit fails at once with `commit_queue_full`. Defaults to 64 MiB.
    pub fn max_admitted_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_admitted_bytes = limit;
        self
    }

    /// Sets the maximum number of publication batches run at once. Admitted
    /// work waits in its namespace's queue for a permit. A namespace delete
    /// takes a fold permit instead. Defaults to 8.
    pub fn max_concurrent_publications(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_publications = limit;
        self
    }

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

    /// Installs the metrics recorder the budget reports its admitted,
    /// running, and waiting work to (see [`crate::metrics`]). The budget
    /// registers its instruments once, when it is built. A budget built
    /// without one reports nothing. Give each budget its own recorder: two
    /// budgets on one recorder set the same gauges.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Builds the budget.
    pub fn build(self) -> ExecutionBudget {
        let ExecutionBudgetInstruments {
            admission,
            publications,
            folds,
            compactions,
        } = ExecutionBudgetInstruments::new(self.metrics_recorder.as_deref());
        ExecutionBudget {
            inner: Arc::new(ExecutionBudgetInner {
                admission: AdmittedTotals {
                    max_requests: self.max_admitted_requests,
                    max_bytes: self.max_admitted_bytes,
                    admitted: Mutex::default(),
                    instruments: admission,
                },
                publications: PermitPool::new(self.max_concurrent_publications, publications),
                folds: PermitPool::new(self.max_concurrent_folds, folds),
                compactions: PermitPool::new(self.max_concurrent_compactions, compactions),
                max_merge_input_bytes: self.max_merge_input_bytes,
            }),
        }
    }
}

/// Work admitted, running, and waiting under an [`ExecutionBudget`], summed
/// over every runtime that shares it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionBudgetStats {
    /// Publication requests admitted and not yet settled.
    pub admitted_requests: usize,
    /// Estimated retained bytes of the publication requests admitted and not
    /// yet settled.
    pub admitted_bytes: usize,
    /// Publication batches holding a publication permit.
    pub publications_running: usize,
    /// Namespace publication workers waiting for a publication permit.
    pub publications_waiting: usize,
    /// Folds, and operations that may fold first, holding a fold permit.
    pub folds_running: usize,
    /// Folds, and operations that may fold first, waiting for a fold permit.
    pub folds_waiting: usize,
    /// Metadata merges holding a compaction permit.
    pub compactions_running: usize,
    /// Metadata merges waiting for a compaction permit.
    pub compactions_waiting: usize,
}

/// The publication requests admitted and not yet settled, and the limits on
/// them.
struct AdmittedTotals {
    max_requests: NonZeroUsize,
    max_bytes: NonZeroUsize,
    admitted: Mutex<Admitted>,
    instruments: AdmissionInstruments,
}

#[derive(Debug, Clone, Copy, Default)]
struct Admitted {
    requests: usize,
    bytes: usize,
}

impl AdmittedTotals {
    fn lock(&self) -> MutexGuard<'_, Admitted> {
        self.admitted.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn charge(&self, estimated_bytes: usize) -> Result<(), CoreError> {
        let mut admitted = self.lock();
        if admitted.requests >= self.max_requests.get()
            || estimated_bytes > self.max_bytes.get().saturating_sub(admitted.bytes)
        {
            self.instruments.rejection();
            return Err(CoreError::CommitQueueFull);
        }
        admitted.requests += 1;
        admitted.bytes += estimated_bytes;
        self.instruments.report(admitted.requests, admitted.bytes);
        Ok(())
    }

    fn refund(&self, estimated_bytes: usize) {
        let mut admitted = self.lock();
        admitted.requests -= 1;
        admitted.bytes -= estimated_bytes;
        self.instruments.report(admitted.requests, admitted.bytes);
    }
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
