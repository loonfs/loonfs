//! The work in flight that one or more runtimes share.

use crate::metrics::{MetricsRecorder, NoopMetricsRecorder};
use loonfs_core::cache::ReadWorkingMemory;
pub(crate) use loonfs_core::BudgetPermit;
use loonfs_core::PermitPool;
use metrics::{AdmissionInstruments, ExecutionBudgetInstruments};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

mod metrics;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod tests;

/// Default maximum WAL-tail folds that the runtimes sharing one budget run at
/// once.
pub const DEFAULT_MAX_CONCURRENT_FOLDS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
/// Default maximum metadata compaction units that the runtimes sharing one
/// budget run at once.
pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: NonZeroUsize = NonZeroUsize::new(2).unwrap();
/// Default maximum public reads that runtimes sharing one budget run at once.
pub const DEFAULT_MAX_CONCURRENT_READS: NonZeroUsize = NonZeroUsize::new(64).unwrap();
pub(crate) const DEFAULT_MAX_CONCURRENT_PUBLICATIONS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// Default bytes retained across read, publication, and fold memos.
pub const DEFAULT_MAX_READ_WORKING_BYTES: usize = 256 * 1024 * 1024;
/// Default maximum content object writes that the runtimes sharing one
/// budget run at once.
pub const DEFAULT_MAX_CONCURRENT_CONTENT_WRITES: NonZeroUsize = NonZeroUsize::new(32).unwrap();
const DEFAULT_MAX_ADMITTED_REQUESTS: NonZeroUsize = NonZeroUsize::new(8192).unwrap();
const DEFAULT_MAX_ADMITTED_BYTES: NonZeroUsize = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
/// Default merge allowance, the estimated peak memory of publishing the one
/// merge admitted at a time.
///
/// Admission estimates a merge's peak as three times its planned WAL bytes.
/// A file create plans about 4,850 bytes, so the default admits a merge of
/// about 36,000 file creates. One WAL object fits about 110,000 creates, and
/// an allowance of 1.5 GiB, three times the WAL object's 512 MiB, admits
/// every merge that fits one WAL object.
pub const DEFAULT_MAX_MERGE_BYTES: usize = 512 * 1024 * 1024;

/// Work in flight that one or more runtimes share.
///
/// It covers admitted publication requests, running reads and publications, WAL
/// folds, metadata compactions, the decoded input one compaction may keep, content
/// writes, and retained read working memory.
///
/// Each [`LoonFs`](crate::LoonFs) built with the budget charges the
/// publication requests it admits to the budget's totals, and takes its
/// read, publication, fold, compaction, and content write permits from it, so the limits bound the
/// work of every runtime that shares it. Publication admission never waits: a request
/// past an admitted total fails at once with `commit_queue_full`. A merge
/// isn't charged to the byte total. The budget admits one merge at a time,
/// and a merge whose estimated peak, three times its planned WAL bytes,
/// exceeds the merge allowance fails with `merge_too_large`. Permit
/// waits are first come, first served, whichever runtime they belong to. No
/// runtime has a reserved share, so an idle runtime reserves nothing. A
/// host that wants to isolate runtimes from each other gives them separate
/// budgets. The per-namespace admission limits stay with each runtime (see
/// [`PublicationPolicy`](crate::PublicationPolicy)). Clones share the budget.
/// A runtime built without one creates a private budget with the
/// default limits.
#[derive(Clone)]
pub struct ExecutionBudget {
    inner: Arc<ExecutionBudgetInner>,
}

struct ExecutionBudgetInner {
    read_working_memory: Arc<ReadWorkingMemory>,
    admission: AdmittedTotals,
    reads: PermitPool,
    publications: PermitPool,
    folds: PermitPool,
    compactions: PermitPool,
    max_compaction_input_bytes: NonZeroUsize,
    content_writes: Arc<PermitPool>,
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
            max_read_working_bytes: DEFAULT_MAX_READ_WORKING_BYTES,
            max_admitted_requests: DEFAULT_MAX_ADMITTED_REQUESTS,
            max_admitted_bytes: DEFAULT_MAX_ADMITTED_BYTES,
            max_merge_bytes: DEFAULT_MAX_MERGE_BYTES,
            max_concurrent_reads: DEFAULT_MAX_CONCURRENT_READS,
            max_concurrent_publications: DEFAULT_MAX_CONCURRENT_PUBLICATIONS,
            max_concurrent_folds: DEFAULT_MAX_CONCURRENT_FOLDS,
            max_concurrent_compactions: DEFAULT_MAX_CONCURRENT_COMPACTIONS,
            max_compaction_input_bytes:
                loonfs_types::format::segment_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES,
            max_concurrent_content_writes: DEFAULT_MAX_CONCURRENT_CONTENT_WRITES,
            metrics_recorder: None,
        }
    }

    /// Snapshots the work admitted, running, and waiting under the budget,
    /// summed over every runtime that shares it.
    pub fn stats(&self) -> ExecutionBudgetStats {
        let admitted = *self.inner.admission.lock();
        let reads = self.inner.reads.counts();
        let publications = self.inner.publications.counts();
        let folds = self.inner.folds.counts();
        let compactions = self.inner.compactions.counts();
        let content_writes = self.inner.content_writes.counts();
        ExecutionBudgetStats {
            read_working_bytes: self.inner.read_working_memory.in_use(),
            admitted_requests: admitted.requests,
            admitted_bytes: admitted.bytes,
            admitted_merges: admitted.merges,
            reads_running: reads.running,
            reads_waiting: reads.waiting,
            publications_running: publications.running,
            publications_waiting: publications.waiting,
            folds_running: folds.running,
            folds_waiting: folds.waiting,
            compactions_running: compactions.running,
            compactions_waiting: compactions.waiting,
            content_writes_running: content_writes.running,
            content_writes_waiting: content_writes.waiting,
        }
    }

    pub(crate) fn content_writes(&self) -> Arc<PermitPool> {
        Arc::clone(&self.inner.content_writes)
    }

    pub(crate) fn read_working_memory(&self) -> Arc<ReadWorkingMemory> {
        Arc::clone(&self.inner.read_working_memory)
    }

    /// Charges one admitted publication request to the totals. A request
    /// past a total, or a merge while another merge is admitted, is refused,
    /// and the change says so and charges nothing. Pass the change to
    /// [`Self::report_admission`] once the caller has unlocked every lock.
    pub(crate) fn charge_admission(&self, charge: AdmissionCharge) -> AdmissionChange {
        self.inner.admission.charge(charge)
    }

    /// Moves an admitted request's charge from `from` bytes to `to` bytes. A
    /// growth past the byte total is refused, and the change says so and
    /// leaves the totals as they were. Pass the change to
    /// [`Self::report_admission`] once the caller has unlocked every lock.
    pub(crate) fn reweigh_admission(&self, from: usize, to: usize) -> AdmissionChange {
        self.inner.admission.reweigh(from, to)
    }

    /// Returns what [`Self::charge_admission`] charged, once the request's
    /// work settles. Pass the change to [`Self::report_admission`] once the
    /// caller has unlocked every lock.
    pub(crate) fn refund_admission(&self, charge: AdmissionCharge) -> AdmissionChange {
        self.inner.admission.refund(charge)
    }

    /// Returns the merge allowance, the estimated peak memory that publishing
    /// one merge may have, which is zero when merges are off.
    pub fn max_merge_bytes(&self) -> usize {
        self.inner.admission.max_merge_bytes
    }

    /// Reports a charge or a refund to the metrics recorder. The recorder is
    /// caller code, so it never runs under a lock.
    pub(crate) fn report_admission(&self, change: AdmissionChange) {
        let instruments = &self.inner.admission.instruments;
        match change {
            AdmissionChange::Admitted(totals) => {
                instruments.report(totals.requests, totals.bytes, totals.merges);
            }
            AdmissionChange::Rejected => instruments.rejection(),
            AdmissionChange::MergeSlotTaken => {}
        }
    }

    pub(crate) async fn read_permit(&self) -> BudgetPermit<'_> {
        self.inner.reads.acquire().await
    }

    /// Waits for a publication permit. A namespace's worker has one while
    /// it publishes a batch.
    pub(crate) async fn publication_permit(&self) -> BudgetPermit<'_> {
        self.inner.publications.acquire().await
    }

    /// Waits for a fold permit. Every fold has one, and so does every
    /// operation that may fold first: a namespace deletion, and the creation
    /// of a checkpoint, a snapshot, or a fork of the current head.
    pub(crate) async fn fold_permit(&self) -> BudgetPermit<'_> {
        self.inner.folds.acquire().await
    }

    /// Waits for a compaction permit. Every compaction unit takes one.
    pub(crate) async fn compaction_permit(&self) -> BudgetPermit<'_> {
        self.inner.compactions.acquire().await
    }

    /// The decoded metadata bytes one compaction may keep.
    pub(crate) fn max_compaction_input_bytes(&self) -> NonZeroUsize {
        self.inner.max_compaction_input_bytes
    }
}

/// Builder for an [`ExecutionBudget`].
#[must_use]
pub struct ExecutionBudgetBuilder {
    max_read_working_bytes: usize,
    max_admitted_requests: NonZeroUsize,
    max_admitted_bytes: NonZeroUsize,
    max_merge_bytes: usize,
    max_concurrent_reads: NonZeroUsize,
    max_concurrent_publications: NonZeroUsize,
    max_concurrent_folds: NonZeroUsize,
    max_concurrent_compactions: NonZeroUsize,
    max_compaction_input_bytes: NonZeroUsize,
    max_concurrent_content_writes: NonZeroUsize,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl ExecutionBudgetBuilder {
    /// Sets the bytes retained by read, publication, and fold memos across
    /// all runtimes sharing this budget. A full pool makes a memo evict its
    /// own entries or leave blocks unretained. Reads never wait or fail for
    /// memory. Defaults to 256 MiB; zero disables retention.
    pub fn max_read_working_bytes(mut self, bytes: usize) -> Self {
        self.max_read_working_bytes = bytes;
        self
    }

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
    /// limit fails at once with `commit_queue_full`. A merge isn't charged to
    /// it. Defaults to 64 MiB.
    pub fn max_admitted_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_admitted_bytes = limit;
        self
    }

    /// Sets the merge allowance, which limits the estimated peak memory of
    /// publishing one merge, three times the merge's planned WAL bytes. The
    /// runtimes that share the budget admit one merge at a time, and a merge
    /// that arrives while one is admitted fails at once with
    /// `commit_queue_full`. A merge whose estimate exceeds the allowance
    /// fails with `merge_too_large`. Defaults to
    /// [`DEFAULT_MAX_MERGE_BYTES`]; zero turns merges off, so a merge, a
    /// rebase, and the next-range query return `not_supported`.
    pub fn max_merge_bytes(mut self, bytes: usize) -> Self {
        self.max_merge_bytes = bytes;
        self
    }

    /// Limits public reads across every runtime sharing this budget. Reads wait
    /// before their first store request. Each pager page takes one permit.
    /// Streams release it after metadata and content lookup, before returning
    /// the body. Internal reads made by mutations and maintenance take none.
    /// Defaults to [`DEFAULT_MAX_CONCURRENT_READS`].
    pub fn max_concurrent_reads(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_reads = limit;
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

    /// Sets the maximum number of metadata compaction units run at once,
    /// whether sessions or [`Maintenance`](crate::Maintenance) calls start
    /// them. A compaction never
    /// has a fold permit. Defaults to
    /// [`DEFAULT_MAX_CONCURRENT_COMPACTIONS`].
    pub fn max_concurrent_compactions(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_compactions = limit;
        self
    }

    /// Sets the decoded metadata bytes one compaction keeps in memory at once,
    /// whatever the size of its window. Defaults to 64 MiB.
    pub fn max_compaction_input_bytes(mut self, max_compaction_input_bytes: NonZeroUsize) -> Self {
        self.max_compaction_input_bytes = max_compaction_input_bytes;
        self
    }

    /// Sets the maximum number of content object writes run at once. Folds,
    /// validation, and direct downloads take a permit for each content
    /// object they write, and staging takes one for each upload, until the
    /// write finishes. Defaults to [`DEFAULT_MAX_CONCURRENT_CONTENT_WRITES`].
    pub fn max_concurrent_content_writes(mut self, limit: NonZeroUsize) -> Self {
        self.max_concurrent_content_writes = limit;
        self
    }

    /// Installs the metrics recorder the budget reports its admitted,
    /// running, and waiting work to (see [`crate::metrics`]). The budget
    /// registers its instruments once, when it is built. A budget built
    /// without one reports into a [`NoopMetricsRecorder`]. Give each budget its own recorder: two
    /// budgets on one recorder set the same gauges.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    /// Builds the budget.
    pub fn build(self) -> ExecutionBudget {
        let ExecutionBudgetInstruments {
            admission,
            reads,
            publications,
            folds,
            compactions,
            content_writes,
            read_working_memory,
        } = match &self.metrics_recorder {
            Some(recorder) => ExecutionBudgetInstruments::new(recorder.as_ref()),
            None => ExecutionBudgetInstruments::new(&NoopMetricsRecorder),
        };
        ExecutionBudget {
            inner: Arc::new(ExecutionBudgetInner {
                read_working_memory: Arc::new(ReadWorkingMemory::new(
                    self.max_read_working_bytes,
                    Some(read_working_memory),
                )),
                admission: AdmittedTotals {
                    max_requests: self.max_admitted_requests,
                    max_bytes: self.max_admitted_bytes,
                    max_merge_bytes: self.max_merge_bytes,
                    admitted: Mutex::default(),
                    instruments: admission,
                },
                reads: PermitPool::new(self.max_concurrent_reads, Some(reads)),
                publications: PermitPool::new(self.max_concurrent_publications, Some(publications)),
                folds: PermitPool::new(self.max_concurrent_folds, Some(folds)),
                compactions: PermitPool::new(self.max_concurrent_compactions, Some(compactions)),
                max_compaction_input_bytes: self.max_compaction_input_bytes,
                content_writes: Arc::new(PermitPool::new(
                    self.max_concurrent_content_writes,
                    Some(content_writes),
                )),
            }),
        }
    }
}

/// Work admitted, running, and waiting under an [`ExecutionBudget`], summed
/// over every runtime that shares it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionBudgetStats {
    /// Bytes retained by metadata block memos, including index and filter sections.
    pub read_working_bytes: usize,
    /// Publication requests admitted and not yet settled.
    pub admitted_requests: usize,
    /// Estimated retained bytes of the publication requests admitted and not
    /// yet settled, other than a merge.
    pub admitted_bytes: usize,
    /// Merges admitted and not yet settled, which is at most one.
    pub admitted_merges: usize,
    /// Public reads that have a read permit.
    pub reads_running: usize,
    /// Public reads waiting for a read permit.
    pub reads_waiting: usize,
    /// Publication batches that have a publication permit.
    pub publications_running: usize,
    /// Namespace publication workers waiting for a publication permit.
    pub publications_waiting: usize,
    /// Folds, and operations that may fold first, that have a fold permit.
    pub folds_running: usize,
    /// Folds, and operations that may fold first, waiting for a fold permit.
    pub folds_waiting: usize,
    /// Metadata compactions that have a compaction permit.
    pub compactions_running: usize,
    /// Metadata compactions waiting for a compaction permit.
    pub compactions_waiting: usize,
    /// Content object writes that have a content write permit.
    pub content_writes_running: usize,
    /// Content object writes waiting for a content write permit.
    pub content_writes_waiting: usize,
}

/// The publication requests admitted and not yet settled, and the limits on
/// them.
struct AdmittedTotals {
    max_requests: NonZeroUsize,
    max_bytes: NonZeroUsize,
    max_merge_bytes: usize,
    admitted: Mutex<Admitted>,
    instruments: AdmissionInstruments,
}

/// The merges that the runtimes sharing a budget admit at once.
const MAX_ADMITTED_MERGES: usize = 1;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Admitted {
    requests: usize,
    bytes: usize,
    merges: usize,
}

/// What one admitted publication request is charged to the totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionCharge {
    /// One request and its estimated bytes.
    Request { estimated_bytes: usize },
    /// One request and one merge. The merge's estimated bytes fit the merge
    /// allowance, and no byte total is charged for them.
    Merge,
}

impl AdmissionCharge {
    /// Returns the bytes charged to the byte totals.
    pub(crate) fn estimated_bytes(self) -> usize {
        match self {
            Self::Request { estimated_bytes } => estimated_bytes,
            Self::Merge => 0,
        }
    }

    fn merges(self) -> usize {
        usize::from(self == Self::Merge)
    }
}

/// What a charge or a refund did to the admission totals.
#[derive(Debug, Clone, Copy)]
#[must_use = "report the change with `ExecutionBudget::report_admission`"]
pub(crate) enum AdmissionChange {
    /// The totals after the charge or the refund.
    Admitted(Admitted),
    /// The charge would pass the request total or the byte total, so nothing
    /// was charged.
    Rejected,
    /// The charge is a merge's, and another merge is admitted, so nothing was
    /// charged.
    MergeSlotTaken,
}

impl AdmittedTotals {
    fn lock(&self) -> MutexGuard<'_, Admitted> {
        self.admitted
            .lock()
            .expect("the admission totals lock should not be poisoned")
    }

    fn charge(&self, charge: AdmissionCharge) -> AdmissionChange {
        let mut admitted = self.lock();
        let (estimated_bytes, merges) = (charge.estimated_bytes(), charge.merges());
        if admitted.requests >= self.max_requests.get()
            || estimated_bytes > self.max_bytes.get().saturating_sub(admitted.bytes)
        {
            return AdmissionChange::Rejected;
        }
        if merges > MAX_ADMITTED_MERGES - admitted.merges {
            return AdmissionChange::MergeSlotTaken;
        }
        admitted.requests += 1;
        admitted.bytes += estimated_bytes;
        admitted.merges += merges;
        AdmissionChange::Admitted(*admitted)
    }

    fn reweigh(&self, from: usize, to: usize) -> AdmissionChange {
        let mut admitted = self.lock();
        let others = admitted.bytes - from;
        if to > self.max_bytes.get().saturating_sub(others) {
            return AdmissionChange::Rejected;
        }
        admitted.bytes = others + to;
        AdmissionChange::Admitted(*admitted)
    }

    fn refund(&self, charge: AdmissionCharge) -> AdmissionChange {
        // A refund runs in a drop, which can run while a panic unwinds, and a
        // second panic then aborts the process, so this recovers a poisoned
        // lock instead of panicking. The critical section only updates three
        // counts.
        let mut admitted = self.admitted.lock().unwrap_or_else(PoisonError::into_inner);
        admitted.requests -= 1;
        admitted.bytes -= charge.estimated_bytes();
        admitted.merges -= charge.merges();
        AdmissionChange::Admitted(*admitted)
    }
}
