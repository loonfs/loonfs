//! Scheduler for bounded background maintenance.
//!
//! Each [`MaintenanceJob`] performs one unit of work for one namespace. The
//! runner manages concurrency, retries, deadlines, and continuations. It only
//! maintains namespaces used by this process or explicitly assigned to it.

mod admission;
mod compaction;
mod jobs;
#[cfg(test)]
mod tests;

use crate::metrics::{RuntimeInstruments, RESULT_ERROR};
use crate::{ChangeSeq, NamespaceId, Result, RuntimeError};
use admission::{Admission, MaintenanceDispatch, MaintenanceKey, StepOutcome};
pub(crate) use compaction::{BackgroundCompactions, CompactionStart};
use futures::FutureExt as _;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub(crate) use jobs::{
    completed_upload_reclaim_at_ms, register_core_jobs, upload_session_reclaim_at_ms,
};

/// Interval between reconciliation sweeps of admitted maintenance keys.
///
/// Sweeps recover work that was blocked, left behind by a quiet writer, or
/// missed by a trigger. This interval is the maximum expected delay before
/// such work is probed again.
const RECONCILE_INTERVAL_MS: u64 = 60_000;

/// Most admitted keys one reconciliation sweep probes. The sweep resumes
/// where it stopped, so a large admitted set costs more sweeps rather than
/// one long one.
const MAX_RECONCILE_PROBES_PER_SWEEP: usize = 64;

/// Controls background maintenance started by a writer.
///
/// Background work may compact metadata and collect expired uploads. It never
/// advances the retention floor; that remains an explicit
/// [`FsAdmin`](crate::FsAdmin) operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsBackgroundWork {
    /// Run maintenance for namespaces used or explicitly assigned to this
    /// process.
    Enabled,
    /// Disable automatic maintenance. Explicit admin operations remain
    /// available.
    ManualOnly,
}

/// Stable identifier for a registered maintenance job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaintenanceJobId(&'static str);

impl MaintenanceJobId {
    /// Flushes the WAL tail after it reaches its threshold, then runs one
    /// bounded reorganization step. Registered for every writer.
    pub const METADATA: Self = Self("metadata");
    /// Garbage collection: one bounded mark-and-sweep pass. Registered by
    /// the runtime on every write-capable handle.
    pub const GC: Self = Self("gc");

    /// Names a job an extension registers.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The name as it appears in traces.
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for MaintenanceJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Result category for one bounded maintenance step.
///
/// Jobs report what happened; the runner decides whether and when to schedule
/// the key again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceStepConclusion {
    /// Durable state advanced. The job may run again immediately.
    Progressed,
    /// No work was available. The job waits for a trigger or reconciliation.
    Idle,
    /// Work exists, but this step cannot process it within its limits.
    Blocked,
    /// Concurrent work changed the state. The job may retry immediately.
    Superseded,
    /// This job is not enabled for the namespace.
    NotEnabled,
}

impl MaintenanceStepConclusion {
    /// The label traces use, and the one a host reporting a step it drove
    /// itself should use with it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Progressed => "progressed",
            Self::Idle => "idle",
            Self::Blocked => "blocked",
            Self::Superseded => "superseded",
            Self::NotEnabled => "not_enabled",
        }
    }
}

/// Scheduling information returned by one bounded maintenance step.
///
/// The runner stores the conclusion, optional continuation, and earliest next
/// run time so jobs do not maintain separate scheduler state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStepReport {
    /// What the step accomplished.
    pub conclusion: MaintenanceStepConclusion,
    /// Opaque state that lets the next step resume this job.
    ///
    /// The runner keeps it while the key is making progress or waiting for
    /// room to work, clears it on [`MaintenanceStepConclusion::Idle`], and
    /// drops it with the key on [`MaintenanceStepConclusion::NotEnabled`].
    /// It never crosses a process boundary: a job that cannot restart its
    /// pass from the beginning safely must not use this.
    pub continuation: Option<String>,
    /// Earliest Unix millisecond when the observed work may become eligible.
    ///
    /// The runner keeps the earliest deadline reported by either a trigger or a
    /// step. `None` means this step did not observe a deadline; it does not prove
    /// that no deadline exists.
    pub not_before_ms: Option<u64>,
}

impl MaintenanceStepReport {
    /// Creates a report without a continuation or deadline.
    pub fn concluded(conclusion: MaintenanceStepConclusion) -> Self {
        Self {
            conclusion,
            continuation: None,
            not_before_ms: None,
        }
    }
}

/// State available to maintenance jobs after a publish attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespacePublication {
    /// Highest sequence committed by the attempt, if any.
    pub committed_through_seq: Option<ChangeSeq>,
    /// Visible WAL-tail length, or zero if the attempt did not read it.
    pub wal_tail_segments: u64,
}

/// Result of checking whether a maintenance job has work to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceProbe {
    /// Work is waiting.
    Due,
    /// No work is waiting.
    Idle,
}

/// A bounded maintenance operation that runs for one namespace at a time.
///
/// Each call reloads durable state, performs at most one unit of work, and
/// reports the result. Steps may run more than once, so implementations must
/// be idempotent through their durable compare-and-swap.
#[async_trait::async_trait]
pub trait MaintenanceJob: Send + Sync + 'static {
    /// This job's stable identity.
    fn id(&self) -> MaintenanceJobId;

    /// Runs one bounded step against `namespace_id`'s durable state.
    ///
    /// `continuation` is whatever this job's last step for this namespace
    /// returned, or `None` for a fresh pass — after a restart, after an
    /// idle conclusion, or the first time the key is admitted. A job that
    /// has no position to carry ignores it and returns
    /// [`MaintenanceStepReport::concluded`].
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceStepReport>;

    /// Checks whether `namespace_id` has work waiting. Called only during
    /// reconciliation.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe>;

    /// Returns whether this publish attempt should nudge the job.
    ///
    /// A nudge is only a scheduling hint; the job must reload durable state.
    fn should_nudge_after_publication(&self, _publication: &NamespacePublication) -> bool {
        false
    }
}

/// Clock used to schedule durable Unix-millisecond deadlines.
///
/// This clock controls only when a step is attempted. Each job validates
/// eligibility from durable state, so an early wake can waste a read but
/// cannot make an unsafe update.
pub(crate) trait MaintenanceClock: fmt::Debug + Send + Sync {
    /// Unix milliseconds.
    fn now_ms(&self) -> u64;

    /// Returns a value in `0..span_ms`, or `0` when `span_ms` is zero.
    fn jitter_below_ms(&self, span_ms: u64) -> u64;
}

/// The process clock.
#[derive(Debug)]
pub(crate) struct SystemMaintenanceClock {
    /// Counter behind the backoff jitter, advanced once per draw.
    jitter: std::sync::atomic::AtomicU64,
}

/// The odd increment SplitMix64 walks its counter by.
const JITTER_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl Default for SystemMaintenanceClock {
    fn default() -> Self {
        // Use a per-instance random seed so hosts retry at different times.
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(JITTER_GAMMA);
        Self {
            jitter: std::sync::atomic::AtomicU64::new(hasher.finish()),
        }
    }
}

impl MaintenanceClock for SystemMaintenanceClock {
    fn now_ms(&self) -> u64 {
        // Treat a pre-epoch system clock as Unix time zero. This leaves durable
        // deadlines in the future instead of making every deadline immediately due
        // and causing a tight scheduler loop. Durable mutation paths reject the same
        // invalid clock independently.
        loonfs_core::time::current_time_ms().unwrap_or(0)
    }

    fn jitter_below_ms(&self, span_ms: u64) -> u64 {
        if span_ms == 0 {
            return 0;
        }
        let counter = self
            .jitter
            .fetch_add(JITTER_GAMMA, std::sync::atomic::Ordering::Relaxed);
        split_mix_64(counter) % span_ms
    }
}

/// Applies the SplitMix64 finalizer to the jitter counter.
fn split_mix_64(counter: u64) -> u64 {
    let mut mixed = counter;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// Cloneable handle for non-blocking maintenance hints.
///
/// Nudges coalesce and never wait for a permit. They are ignored after
/// admission closes or after the owning writer is dropped.
#[derive(Clone)]
pub struct MaintenanceHandle {
    inner: Weak<RunnerInner>,
}

impl fmt::Debug for MaintenanceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("MaintenanceHandle").finish()
    }
}

impl MaintenanceHandle {
    /// The wall clock this runner schedules against.
    ///
    /// A caller that has just created a durable deadline reads it here and
    /// derives the not-before time from that reading, so the schedule and
    /// the runner agree on what "now" is.
    pub fn now_ms(&self) -> u64 {
        match self.inner.upgrade() {
            Some(inner) => inner.clock.now_ms(),
            None => SystemMaintenanceClock::default().now_ms(),
        }
    }

    /// Schedules `job` for `namespace_id` when a permit is available.
    /// Repeated calls coalesce.
    pub fn nudge(&self, job: MaintenanceJobId, namespace_id: &NamespaceId) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        nudge_key(&inner, job, namespace_id, None);
    }

    /// Schedules `job` for `namespace_id` at or after `not_before_ms`.
    /// Repeated calls keep the earliest time.
    pub fn nudge_not_before(
        &self,
        job: MaintenanceJobId,
        namespace_id: &NamespaceId,
        not_before_ms: u64,
    ) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        nudge_key(&inner, job, namespace_id, Some(not_before_ms));
    }
}

/// Owner of background maintenance for one write-capable handle:
/// registration, admission, and the shutdown that settles both.
pub(crate) struct MaintenanceRunner {
    inner: Arc<RunnerInner>,
}

/// Process-wide counters included in maintenance trace events.
///
/// They show whether retry backoff and deadline wakes are occurring without
/// adding per-key metric cardinality.
#[derive(Debug, Default)]
struct RunnerCounters {
    /// Steps that failed and were scheduled for a backoff retry.
    backoff_scheduled: std::sync::atomic::AtomicU64,
    /// Timer wakes that found at least one not-before deadline arrived.
    not_before_wakes: std::sync::atomic::AtomicU64,
}

impl RunnerCounters {
    fn bump(counter: &std::sync::atomic::AtomicU64) -> u64 {
        counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1)
    }
}

pub(crate) struct RunnerInner {
    policy: FsBackgroundWork,
    /// Runtime that owns spawned work. Handle builders pin the runtime they
    /// were opened on; `None` resolves the runtime driving the triggering
    /// call at spawn time.
    runtime: Option<tokio::runtime::Handle>,
    clock: Arc<dyn MaintenanceClock>,
    jobs: Mutex<BTreeMap<MaintenanceJobId, Arc<dyn MaintenanceJob>>>,
    /// One lock over the shutdown flag, the admission book, and the task
    /// registry. Registration checks the flag inside the same critical
    /// section, so a shutdown that drained an empty registry can never race
    /// a spawn into running unobserved.
    state: Mutex<RunnerState>,
    wake: Arc<Notify>,
    counters: RunnerCounters,
    /// Tasks a later spawn reaped after they panicked. Their join handles no
    /// longer reach [`MaintenanceRunner::drain`], so the count does.
    panicked_tasks: std::sync::atomic::AtomicUsize,
    /// Where settled steps report. The two [`RunnerCounters`] above stay
    /// where they are: they answer "is this happening at all", which a
    /// number on an existing trace answers for a reader with no metrics
    /// pipeline at all.
    instruments: Arc<RuntimeInstruments>,
    /// The streaming metadata compactions running under this runner, one per
    /// namespace at most, and the pressure that decides when one has to
    /// start. Held here rather than beside the admission book because a
    /// compaction is not a bounded step: it takes no admission permit, it
    /// runs for as long as it needs, and what the runner owes it is a spawn,
    /// a cancellation at shutdown, and the drain every spawned task gets.
    compactions: Arc<Mutex<BTreeMap<NamespaceId, compaction::NamespaceCompactions>>>,
    /// What caps compactions across every namespace this process serves. The
    /// map above stops two jobs sharing a namespace; this stops one process
    /// holding a job's worth of memory per namespace it has touched.
    compaction_permits: Arc<tokio::sync::Semaphore>,
}

struct RunnerState {
    admission: Admission,
    tasks: Vec<JoinHandle<()>>,
    /// The one timer task: it wakes for not-before times, error backoffs,
    /// and the reconciliation interval. Started on first admitted work and
    /// joined by the drain that follows a shutdown.
    scheduler: Option<JoinHandle<()>>,
    next_reconcile_ms: u64,
}

impl MaintenanceRunner {
    pub(crate) fn new(
        policy: FsBackgroundWork,
        runtime: Option<tokio::runtime::Handle>,
        max_concurrent: std::num::NonZeroUsize,
        instruments: Arc<RuntimeInstruments>,
    ) -> Self {
        Self::with_clock(
            policy,
            runtime,
            max_concurrent,
            Arc::new(SystemMaintenanceClock::default()),
            instruments,
        )
    }

    pub(crate) fn with_clock(
        policy: FsBackgroundWork,
        runtime: Option<tokio::runtime::Handle>,
        max_concurrent: std::num::NonZeroUsize,
        clock: Arc<dyn MaintenanceClock>,
        instruments: Arc<RuntimeInstruments>,
    ) -> Self {
        let next_reconcile_ms = clock.now_ms().saturating_add(RECONCILE_INTERVAL_MS);
        Self {
            inner: Arc::new(RunnerInner {
                policy,
                runtime,
                jobs: Mutex::new(BTreeMap::new()),
                state: Mutex::new(RunnerState {
                    admission: Admission::new(max_concurrent.get(), Arc::clone(&clock)),
                    tasks: Vec::new(),
                    scheduler: None,
                    next_reconcile_ms,
                }),
                clock,
                wake: Arc::new(Notify::new()),
                counters: RunnerCounters::default(),
                panicked_tasks: std::sync::atomic::AtomicUsize::new(0),
                instruments,
                compactions: Arc::new(Mutex::new(BTreeMap::new())),
                compaction_permits: Arc::new(tokio::sync::Semaphore::new(
                    compaction::MAX_CONCURRENT_COMPACTIONS,
                )),
            }),
        }
    }

    /// Returns a cloneable scheduling handle that cannot shut down admission.
    pub(crate) fn handle(&self) -> MaintenanceHandle {
        MaintenanceHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Returns a shared view of running streaming compactions.
    pub(crate) fn compactions(&self) -> BackgroundCompactions {
        BackgroundCompactions::new(&self.inner)
    }

    /// Returns the job registered under `id`.
    pub(crate) fn job(&self, id: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.inner.job(id)
    }

    /// Registers a job. Manual-only runners retain jobs for explicit use.
    pub(crate) fn register(&self, job: Arc<dyn MaintenanceJob>) -> Result<MaintenanceJobId> {
        let id = job.id();
        let mut jobs = self.inner.lock_jobs();
        if jobs.contains_key(&id) {
            return Err(RuntimeError::Config(format!(
                "maintenance job `{id}` is already registered"
            )));
        }
        jobs.insert(id, job);
        Ok(id)
    }

    /// Stops admission, discards queued work, and cancels compactions.
    ///
    /// Running steps and cancelled compactions remain visible to
    /// [`Self::drain`]. This method is synchronous so admission closes before
    /// any shutdown await can allow another job to start.
    pub(crate) fn close_admission(&self) {
        self.inner.lock_state().admission.close();
        self.compactions().cancel_all();
        self.inner.wake.notify_one();
    }

    /// Waits for every spawned step to finish, surfacing panics.
    ///
    /// Loops because an in-flight write may schedule more work while an open
    /// handle waits, and because a finishing step hands its permit to the
    /// next queued key before it exits.
    pub(crate) async fn drain(&self) -> Result<()> {
        let mut panicked = 0usize;
        // The timer task only ends once admission closes; joining it before
        // then would never return.
        let scheduler = {
            let mut state = self.inner.lock_state();
            state
                .admission
                .is_closed()
                .then(|| state.scheduler.take())
                .flatten()
        };
        if let Some(scheduler) = scheduler {
            self.inner.wake.notify_one();
            if scheduler.await.is_err_and(|error| error.is_panic()) {
                panicked += 1;
            }
        }
        loop {
            let drained = std::mem::take(&mut self.inner.lock_state().tasks);
            if drained.is_empty() {
                break;
            }
            for task in drained {
                if task.await.is_err_and(|error| error.is_panic()) {
                    panicked += 1;
                }
            }
        }
        panicked += self
            .inner
            .panicked_tasks
            .load(std::sync::atomic::Ordering::SeqCst);
        if panicked > 0 {
            return Err(RuntimeError::RuntimeTask(format!(
                "{panicked} background maintenance task(s) panicked"
            )));
        }
        Ok(())
    }

    /// Nudges interested jobs after a publish attempt.
    ///
    /// Nudges are non-blocking, coalesced, and ignored when automatic
    /// maintenance is disabled or closed.
    pub(crate) fn nudge_jobs_after_publication(
        &self,
        namespace_id: &NamespaceId,
        publication: &NamespacePublication,
    ) {
        // Release the job lock before acquiring scheduling state.
        let subscribers: Vec<MaintenanceJobId> = self
            .inner
            .lock_jobs()
            .iter()
            .filter(|(_, job)| job.should_nudge_after_publication(publication))
            .map(|(id, _)| *id)
            .collect();
        for job in subscribers {
            nudge_key(&self.inner, job, namespace_id, None);
        }
    }

    /// Returns whether this key is currently pending. Used only by tests and
    /// diagnostics.
    #[cfg(test)]
    pub(crate) fn is_pending(&self, job: MaintenanceJobId, namespace_id: &NamespaceId) -> bool {
        self.inner
            .lock_state()
            .admission
            .is_pending(&MaintenanceKey::new(job, namespace_id))
    }

    /// The timed obligation the key still carries.
    #[cfg(test)]
    pub(crate) fn not_before_ms(
        &self,
        job: MaintenanceJobId,
        namespace_id: &NamespaceId,
    ) -> Option<u64> {
        self.inner
            .lock_state()
            .admission
            .not_before_ms(&MaintenanceKey::new(job, namespace_id))
    }

    /// Runs one reconciliation sweep now instead of on the interval.
    #[cfg(test)]
    pub(crate) async fn reconcile_now(&self) {
        reconcile(&self.inner).await;
        dispatch_ready(&self.inner);
    }

    /// Does what the timer does on a wake, without waiting for one: promote
    /// arrived obligations, then dispatch.
    #[cfg(test)]
    pub(crate) fn dispatch_now(&self) {
        let now_ms = self.inner.clock.now_ms();
        self.inner.lock_state().admission.promote_due(now_ms);
        dispatch_ready(&self.inner);
    }

    #[cfg(test)]
    pub(crate) fn is_registered(&self, job: MaintenanceJobId) -> bool {
        self.inner.lock_jobs().contains_key(&job)
    }

    /// Permits currently held by running chains.
    #[cfg(test)]
    pub(crate) fn running_steps(&self) -> usize {
        self.inner.lock_state().admission.running()
    }
}

impl RunnerInner {
    fn lock_state(&self) -> MutexGuard<'_, RunnerState> {
        self.state.lock().expect("maintenance state lock poisoned")
    }

    fn lock_jobs(&self) -> MutexGuard<'_, BTreeMap<MaintenanceJobId, Arc<dyn MaintenanceJob>>> {
        self.jobs.lock().expect("maintenance job lock poisoned")
    }

    fn job(&self, id: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.lock_jobs().get(&id).cloned()
    }

    fn runtime(&self) -> Option<tokio::runtime::Handle> {
        self.runtime
            .clone()
            .or_else(|| tokio::runtime::Handle::try_current().ok())
    }

    /// Spawns maintenance on the owning runtime and tracks it for shutdown.
    ///
    /// If no runtime is available or admission has closed, the future is
    /// dropped. It must release any claimed key or permit when dropped.
    pub(super) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let Some(runtime) = self.runtime() else {
            return;
        };
        let mut state = self.lock_state();
        if state.admission.is_closed() {
            // A shutdown between this work's claim and now must win: the
            // drain may already have observed an empty registry. Release the
            // lock before dropping the future — releasing its claim
            // re-enters this mutex.
            drop(state);
            drop(future);
            return;
        }
        state.tasks.retain_mut(|task| {
            if !task.is_finished() {
                return true;
            }
            if task
                .now_or_never()
                .is_some_and(|outcome| outcome.is_err_and(|error| error.is_panic()))
            {
                self.panicked_tasks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            false
        });
        state.tasks.push(runtime.spawn(future));
    }
}

/// Records a request against a key and starts whatever it makes runnable.
fn nudge_key(
    inner: &Arc<RunnerInner>,
    job: MaintenanceJobId,
    namespace_id: &NamespaceId,
    not_before_ms: Option<u64>,
) {
    if inner.policy != FsBackgroundWork::Enabled {
        return;
    }
    let now_ms = inner.clock.now_ms();
    let key = MaintenanceKey::new(job, namespace_id);
    {
        let mut state = inner.lock_state();
        if state.admission.is_closed() {
            return;
        }
        match not_before_ms {
            Some(at_ms) => state.admission.nudge_at(key, at_ms, now_ms),
            None => state.admission.nudge(key),
        }
    }
    ensure_scheduler(inner);
    dispatch_ready(inner);
    // A newly planted obligation may be sooner than what the timer is
    // sleeping on.
    inner.wake.notify_one();
}

/// Claims every permit the ready keys can fill and spawns a chain for each.
fn dispatch_ready(inner: &Arc<RunnerInner>) {
    let mut dispatched = 0usize;
    loop {
        let now_ms = inner.clock.now_ms();
        let claimed = inner.lock_state().admission.try_dispatch(now_ms);
        let Some(dispatch) = claimed else {
            break;
        };
        dispatched += 1;
        spawn_chain(inner, dispatch);
    }
    if dispatched > 0 {
        // Report the queue remaining after dispatch. Sustained queue growth or rising
        // wait time indicates that the shared permit limit is too low. Metrics are
        // aggregate and do not include namespace IDs.
        let now_ms = inner.clock.now_ms();
        let state = inner.lock_state();
        tracing::debug!(
            dispatched,
            ready_queued = state.admission.ready_queued(),
            oldest_queued_ms = state.admission.oldest_queued_ms(now_ms),
            "maintenance keys dispatched"
        );
    }
}

fn spawn_chain(inner: &Arc<RunnerInner>, dispatch: MaintenanceDispatch) {
    // The guard is built here rather than inside the future's body so the
    // future owns it before it is ever polled. A spawn refused by a
    // shutdown drops the future without running it, and that drop is what
    // gives the claimed key and its permit back.
    let mut chain = PermitChain {
        inner: Arc::clone(inner),
        dispatch: Some(dispatch),
    };
    inner.spawn(async move {
        while let Some(dispatch) = chain.dispatch.clone() {
            let outcome = run_step(&chain.inner, &dispatch).await;
            let now_ms = chain.inner.clock.now_ms();
            chain.dispatch =
                chain
                    .inner
                    .lock_state()
                    .admission
                    .finish(&dispatch.key, outcome, now_ms);
        }
        // The last finish may have parked a key on a deadline the timer has
        // not seen, and the permit it gave back may let a waiting key run.
        chain.inner.wake.notify_one();
    });
}

/// Owns one maintenance permit and releases it exactly once.
///
/// Normal completion may transfer the permit to the next ready key. Dropping
/// the chain after a panic, refused spawn, or runtime shutdown abandons the
/// current key and returns the permit.
struct PermitChain {
    inner: Arc<RunnerInner>,
    dispatch: Option<MaintenanceDispatch>,
}

impl Drop for PermitChain {
    fn drop(&mut self) {
        if let Some(dispatch) = self.dispatch.take() {
            self.inner.lock_state().admission.abandon(&dispatch.key);
            self.inner.wake.notify_one();
        }
    }
}

/// Runs one claimed step. Everything it needs about the key came with the
/// claim, so nothing here reads scheduling state back out of the book.
#[allow(clippy::disallowed_methods)]
// Monotonic time is used only to record step duration.
async fn run_step(inner: &Arc<RunnerInner>, dispatch: &MaintenanceDispatch) -> StepOutcome {
    let key = &dispatch.key;
    let Some(job) = inner.job(key.job) else {
        // Nudged for a job nobody registered: there is nothing to run and
        // nothing to reconcile.
        return StepOutcome::Concluded(MaintenanceStepReport::concluded(
            MaintenanceStepConclusion::NotEnabled,
        ));
    };
    let continuation = dispatch.continuation.as_deref();
    let queued_ms = dispatch.queue_wait_ms;
    let started = tokio::time::Instant::now();
    match job.step(&key.namespace_id, continuation).await {
        Ok(result) => {
            let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            inner
                .instruments
                .maintenance_step(key.job, result.conclusion, queued_ms, elapsed_ms);
            tracing::debug!(
                job = %key.job,
                namespace_id = %key.namespace_id,
                conclusion = result.conclusion.as_str(),
                resumed = continuation.is_some(),
                continues = result.continuation.is_some(),
                not_before_ms = ?result.not_before_ms,
                // The two halves of what a step cost: how long it waited
                // for a permit, and how long it then took.
                queued_ms,
                elapsed_ms,
                "maintenance step settled"
            );
            StepOutcome::Concluded(result)
        }
        Err(error) => {
            inner
                .instruments
                .maintenance_step_failed(key.job, queued_ms);
            tracing::info!(
                job = %key.job,
                namespace_id = %key.namespace_id,
                result = RESULT_ERROR,
                error = %error,
                queued_ms,
                backoff_scheduled = RunnerCounters::bump(&inner.counters.backoff_scheduled),
                "maintenance step failed; backing off"
            );
            StepOutcome::Failed
        }
    }
}

/// Starts the timer task if it is not already running.
fn ensure_scheduler(inner: &Arc<RunnerInner>) {
    let Some(runtime) = inner.runtime() else {
        return;
    };
    let mut state = inner.lock_state();
    if state.scheduler.is_some() || state.admission.is_closed() {
        return;
    }
    let weak = Arc::downgrade(inner);
    let wake = Arc::clone(&inner.wake);
    state.scheduler = Some(runtime.spawn(scheduler_loop(weak, wake)));
}

/// Scheduler task for deadlines and periodic reconciliation.
///
/// It sleeps until the earliest key deadline or reconciliation interval. The
/// task holds only a weak runner reference, so dropping the runner lets the
/// task exit even without explicit shutdown.
async fn scheduler_loop(inner: Weak<RunnerInner>, wake: Arc<Notify>) {
    loop {
        let delay = {
            let Some(inner) = inner.upgrade() else {
                break;
            };
            let Some(delay) = next_wake_delay(&inner) else {
                break;
            };
            delay
        };
        wait_for_deadline(delay, &wake).await;
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if inner.lock_state().admission.is_closed() {
            break;
        }
        let now_ms = inner.clock.now_ms();
        let promoted = inner.lock_state().admission.promote_due(now_ms);
        if promoted > 0 {
            // Distinguishes a wake a durable deadline caused from the far
            // more common one a nudge or the reconciliation interval did.
            tracing::debug!(
                promoted,
                not_before_wakes = RunnerCounters::bump(&inner.counters.not_before_wakes),
                "maintenance deadlines arrived"
            );
        }
        if take_reconcile_turn(&inner, now_ms) {
            reconcile(&inner).await;
        }
        dispatch_ready(&inner);
    }
}

#[allow(clippy::disallowed_methods)]
async fn wait_for_deadline(delay: Duration, wake: &Notify) {
    // The runner's one coalescing timer, at the scheduling boundary the
    // workspace lint points to: it decides when to look, never what is true.
    tokio::select! {
        () = tokio::time::sleep(delay) => {}
        () = wake.notified() => {}
    }
}

/// Returns the delay until the next deadline or reconciliation sweep.
/// `None` means admission is closed.
fn next_wake_delay(inner: &Arc<RunnerInner>) -> Option<Duration> {
    let now_ms = inner.clock.now_ms();
    let state = inner.lock_state();
    if state.admission.is_closed() {
        return None;
    }
    let deadline_ms = match state.admission.earliest_deadline_ms(now_ms) {
        Some(deadline_ms) => deadline_ms.min(state.next_reconcile_ms),
        None => state.next_reconcile_ms,
    };
    // Never a zero-length sleep: a deadline that has already arrived is work
    // for the dispatch below, not a reason to wake in a loop.
    let wait_ms = deadline_ms
        .saturating_sub(now_ms)
        .clamp(1, RECONCILE_INTERVAL_MS);
    Some(Duration::from_millis(wait_ms))
}

/// Whether this wake owns the next reconciliation sweep.
fn take_reconcile_turn(inner: &Arc<RunnerInner>, now_ms: u64) -> bool {
    let mut state = inner.lock_state();
    if now_ms < state.next_reconcile_ms {
        return false;
    }
    state.next_reconcile_ms = now_ms.saturating_add(RECONCILE_INTERVAL_MS);
    true
}

/// Probes a bounded batch of admitted maintenance keys.
///
/// Reconciliation never discovers namespaces. It covers only namespaces
/// touched by this process or explicitly assigned to it. The admitted set
/// starts empty after each process restart and grows through nudges.
async fn reconcile(inner: &Arc<RunnerInner>) {
    let batch = inner
        .lock_state()
        .admission
        .reconcile_batch(MAX_RECONCILE_PROBES_PER_SWEEP);
    let mut probes = 0usize;
    let mut re_admitted = 0usize;
    for key in batch {
        let Some(job) = inner.job(key.job) else {
            inner.lock_state().admission.forget(&key);
            continue;
        };
        probes += 1;
        match job.probe(&key.namespace_id).await {
            Ok(MaintenanceProbe::Due) => {
                re_admitted += 1;
                tracing::debug!(
                    job = %key.job,
                    namespace_id = %key.namespace_id,
                    "reconciliation re-admitted a key with work waiting"
                );
                let mut state = inner.lock_state();
                if !state.admission.is_closed() {
                    state.admission.nudge(key);
                }
            }
            // Only a probe may forget a key, and only one that owes nothing:
            // a lease-dated obligation outlives any number of quiet probes.
            Ok(MaintenanceProbe::Idle) => inner.lock_state().admission.forget_if_idle(&key),
            // An unreadable namespace stays admitted: the next sweep asks
            // again rather than deciding on a failed read. Staying admitted
            // is not the same as being fine, and a quiescent namespace whose
            // probe fails forever has nothing else to report it, so this is
            // logged and counted exactly as a failed step is.
            Err(error) => {
                inner.instruments.maintenance_probe_failed(key.job);
                tracing::info!(
                    job = %key.job,
                    namespace_id = %key.namespace_id,
                    result = RESULT_ERROR,
                    error = %error,
                    "reconciliation probe failed; the key stays admitted"
                );
            }
        }
    }
    // What one sweep cost, against the probe budget above: a sweep that
    // keeps hitting the cap is an admitted set larger than one interval can
    // walk.
    tracing::debug!(
        probes,
        re_admitted,
        probe_budget = MAX_RECONCILE_PROBES_PER_SWEEP,
        "maintenance reconciliation swept"
    );
}
