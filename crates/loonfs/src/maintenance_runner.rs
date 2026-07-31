//! One runner admits every background maintenance step.
//!
//! A [`MaintenanceJob`] knows how to do one bounded piece of upkeep for one
//! namespace: it re-reads durable state, does at most one unit of work,
//! publishes through a compare-and-swap, and reports a
//! [`MaintenanceStepConclusion`]. It decides nothing about when to run.
//!
//! The [`MaintenanceRunner`] decides that, and it is the only thing that
//! does. It holds the pending `{job, namespace}` keys, the one permit pool
//! every job shares, the per-key error backoff, the not-before times a
//! wall-clock obligation plants, and a bounded sweep over the keys it has
//! admitted. Triggers — a publish crossing a threshold, an upload session
//! opening — are hints: level-triggered, never authoritative, and safe to
//! lose because the durable state a step re-reads is the truth.
//!
//! LoonFS never creates a hidden runtime for maintenance. Steps are spawned
//! on the writer's own owning runtime and stay visible to shutdown through
//! the registry here. Only a writer owns a runner: readers and admins
//! schedule nothing.

mod admission;
mod jobs;
#[cfg(test)]
mod tests;

use crate::{NamespaceId, Result, RuntimeError};
use admission::{Admission, MaintenanceKey, StepOutcome};
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

/// How often the runner sweeps the keys it has admitted, looking for work no
/// trigger reported.
///
/// Triggers are hints, and a hint can be lost: a step that concluded
/// `Blocked`, a namespace whose last writer went quiet mid-backlog, a key
/// cleared by a shutdown earlier in this process. The sweep is the answer to
/// all of them, so its period is how long such work may sit — short enough
/// to be a hiccup, long enough that one cheap probe per admitted key is
/// nothing next to the traffic the same process is serving.
const RECONCILE_INTERVAL_MS: u64 = 60_000;

/// Most admitted keys one reconciliation sweep probes. The sweep resumes
/// where it stopped, so a large admitted set costs more sweeps rather than
/// one long one.
const MAX_RECONCILE_PROBES_PER_SWEEP: usize = 64;

/// Writer-initiated background maintenance policy.
///
/// The policy governs only maintenance a write-capable handle schedules for
/// itself: the non-destructive metadata steps that keep read cost bounded
/// once the WAL tail crosses its threshold, and the garbage-collection
/// passes that reclaim what the upload sessions this writer opened leave
/// behind once their leases pass. It never advances the retention floor —
/// surrendering replay history stays an explicit
/// [`FsAdmin`](crate::FsAdmin) operation with no scheduler behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsBackgroundWork {
    /// The writer may schedule non-destructive maintenance for itself,
    /// spawned on the writer's owning runtime.
    Enabled,
    /// The writer never auto-schedules maintenance. Jobs may still be
    /// registered, and explicit [`FsAdmin`](crate::FsAdmin) maintenance
    /// calls still work.
    ManualOnly,
}

/// Stable identity of a registered maintenance job.
///
/// The name is part of the admission key and of every trace the job's steps
/// emit, so it outlives any one registration and must not drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaintenanceJobId(&'static str);

impl MaintenanceJobId {
    /// Metadata upkeep: flush the WAL tail past its threshold, then fold one
    /// bounded reorganization unit. Registered by the runtime on every
    /// write-capable handle.
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

/// What one bounded maintenance step accomplished.
///
/// The conclusion is the executor's whole vocabulary for scheduling: it says
/// what happened, and the runner decides what that means for the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceStepConclusion {
    /// Durable state advanced. The key is eligible again at once — behind
    /// whatever else is waiting, so a long backlog stays fair to its peers.
    Progressed,
    /// Nothing to do. The key parks until something nudges it or a
    /// reconciliation sweep finds work.
    Idle,
    /// There is work, and this step's policy cannot make progress on it —
    /// an input that does not fit the per-step budget, for one. Parks like
    /// [`Self::Idle`]: requeueing zero-progress work would only spin.
    Blocked,
    /// Another writer won the race this step was in. The key is eligible
    /// again at once, to take the race against what actually landed.
    Superseded,
    /// This job has nothing to maintain for this namespace at all. The
    /// runner forgets the key.
    NotEnabled,
}

impl MaintenanceStepConclusion {
    /// The label traces use.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Progressed => "progressed",
            Self::Idle => "idle",
            Self::Blocked => "blocked",
            Self::Superseded => "superseded",
            Self::NotEnabled => "not_enabled",
        }
    }
}

/// What a reconciliation probe found.
///
/// A probe is the cheapest question a job can answer — one status read, or
/// no read at all — and it exists so a sweep can re-admit forgotten work
/// without running a step to find out there was none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceProbe {
    /// Durable state shows work waiting. The runner re-nudges the key.
    Due,
    /// Nothing waiting. With no timed obligation left, the runner forgets
    /// the key until something nudges it again.
    Idle,
}

/// One kind of bounded background upkeep, for one namespace at a time.
///
/// An executor re-reads durable state on every call — it never trusts what
/// the trigger that woke it claimed — does at most one bounded unit of work,
/// and reports what happened. Delivery is at-least-once and conclusions are
/// idempotent through the compare-and-swap each step publishes with, so a
/// duplicated step costs a round trip and nothing else.
#[async_trait::async_trait]
pub trait MaintenanceJob: Send + Sync + 'static {
    /// This job's stable identity.
    fn id(&self) -> MaintenanceJobId;

    /// Runs one bounded step against `namespace_id`'s durable state.
    async fn step(&self, namespace_id: &NamespaceId) -> Result<MaintenanceStepConclusion>;

    /// Answers whether `namespace_id` has work waiting, as cheaply as this
    /// job can. Called only by reconciliation, never on the hot path.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe>;
}

/// Wall-clock milliseconds the runner schedules against.
///
/// Scheduling is all this clock decides. The deadlines it is compared with —
/// an upload session's lease, a completed session's reclamation grace — are
/// unix milliseconds stamped into durable records, so the runner has to read
/// the same kind of clock to know when they arrive. Nothing here gates
/// correctness: firing a step early costs one round trip that concludes
/// there was nothing to do, because every executor re-derives its own
/// safety from durable state under its own mutation context.
pub(crate) trait MaintenanceClock: fmt::Debug + Send + Sync {
    /// Unix milliseconds.
    fn now_ms(&self) -> u64;
}

/// The process clock.
#[derive(Debug, Default)]
pub(crate) struct SystemMaintenanceClock;

impl MaintenanceClock for SystemMaintenanceClock {
    fn now_ms(&self) -> u64 {
        // A clock before the unix epoch reads as the epoch here rather than
        // failing: it can only make a scheduling decision early, and every
        // durable path already refuses such a clock outright.
        crate::time::current_time_ms().unwrap_or(0)
    }
}

/// Cloneable, non-blocking way to tell the runner a key may have work.
///
/// Nudges are hints. They never block the caller, never wait for a permit,
/// and are simply dropped once the runner's admission has closed or the
/// writer that owns it is gone — a write path holding one of these is not
/// on the hook for the maintenance it suggests.
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
            None => SystemMaintenanceClock.now_ms(),
        }
    }

    /// Asks for `job` to run against `namespace_id` as soon as a permit is
    /// free. Repeated asks coalesce into one run.
    pub fn nudge(&self, job: MaintenanceJobId, namespace_id: &NamespaceId) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        nudge_at(&inner, job, namespace_id, None);
    }

    /// Asks for `job` to run against `namespace_id` once `not_before_ms`
    /// passes, whether or not anything else asks in the meantime.
    ///
    /// This is how a wall-clock obligation — an upload lease that will
    /// expire, content whose reclamation grace will pass — becomes admitted
    /// work instead of something only the next unrelated write would notice.
    /// Repeated asks keep the soonest time.
    pub fn nudge_not_before(
        &self,
        job: MaintenanceJobId,
        namespace_id: &NamespaceId,
        not_before_ms: u64,
    ) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        nudge_at(&inner, job, namespace_id, Some(not_before_ms));
    }
}

/// Owner of background maintenance for one write-capable handle:
/// registration, admission, and the shutdown that settles both.
pub(crate) struct MaintenanceRunner {
    inner: Arc<RunnerInner>,
}

struct RunnerInner {
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
    ) -> Self {
        Self::with_clock(
            policy,
            runtime,
            max_concurrent,
            Arc::new(SystemMaintenanceClock),
        )
    }

    pub(crate) fn with_clock(
        policy: FsBackgroundWork,
        runtime: Option<tokio::runtime::Handle>,
        max_concurrent: std::num::NonZeroUsize,
        clock: Arc<dyn MaintenanceClock>,
    ) -> Self {
        let next_reconcile_ms = clock.now_ms().saturating_add(RECONCILE_INTERVAL_MS);
        Self {
            inner: Arc::new(RunnerInner {
                policy,
                runtime,
                clock,
                jobs: Mutex::new(BTreeMap::new()),
                state: Mutex::new(RunnerState {
                    admission: Admission::new(max_concurrent.get()),
                    tasks: Vec::new(),
                    scheduler: None,
                    next_reconcile_ms,
                }),
                wake: Arc::new(Notify::new()),
            }),
        }
    }

    /// A cloneable nudge-only view. Every trigger holds one of these; only
    /// this runner can shut admission down.
    pub(crate) fn handle(&self) -> MaintenanceHandle {
        MaintenanceHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Registers an executor under its own id.
    ///
    /// Registration is accepted under either policy: with
    /// [`FsBackgroundWork::ManualOnly`] the runner simply never nudges what
    /// it knows about.
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

    /// Rejects further scheduling and discards work still waiting for a
    /// permit. Running steps stay visible to [`Self::drain`].
    ///
    /// Synchronous on purpose: a shutdown has to close admission before its
    /// first await, or a step finishing during the publication drain hands
    /// its slot to work the shutdown already decided to drop.
    pub(crate) fn shut_down(&self) {
        self.inner.lock_state().admission.shut_down();
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
        if panicked > 0 {
            return Err(RuntimeError::RuntimeTask(format!(
                "{panicked} background maintenance task(s) panicked"
            )));
        }
        Ok(())
    }

    /// Whether the key is admitted with work still owed to it. Test and
    /// diagnostic seam; nothing on a hot path asks.
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
    pub(crate) fn tick_now(&self) {
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
        match &self.runtime {
            Some(handle) => Some(handle.clone()),
            None => tokio::runtime::Handle::try_current().ok(),
        }
    }

    /// Spawns work on the owning runtime and registers it for shutdown, or
    /// refuses after a shutdown. Dropping the future without running it must
    /// release whatever it holds, so both refusals here clean up by
    /// dropping.
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
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
        state.tasks.retain(|task| !task.is_finished());
        state.tasks.push(runtime.spawn(future));
    }
}

/// Records a request against a key and starts whatever it makes runnable.
fn nudge_at(
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
            Some(at_ms) => state.admission.nudge_not_before(key, at_ms, now_ms),
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
    loop {
        let now_ms = inner.clock.now_ms();
        let dispatched = inner.lock_state().admission.try_dispatch(now_ms);
        let Some(key) = dispatched else {
            break;
        };
        spawn_chain(inner, key);
    }
}

fn spawn_chain(inner: &Arc<RunnerInner>, key: MaintenanceKey) {
    // The guard is built here rather than inside the future's body so the
    // future owns it before it is ever polled. A spawn refused by a
    // shutdown drops the future without running it, and that drop is what
    // gives the claimed key and its permit back.
    let mut chain = PermitChain {
        inner: Arc::clone(inner),
        key: Some(key),
    };
    inner.spawn(async move {
        while let Some(key) = chain.key.clone() {
            let outcome = run_step(&chain.inner, &key).await;
            let now_ms = chain.inner.clock.now_ms();
            chain.key = chain
                .inner
                .lock_state()
                .admission
                .finish(&key, outcome, now_ms);
        }
        // The last finish may have parked a key on a deadline the timer has
        // not seen, and the permit it gave back may let a waiting key run.
        chain.inner.wake.notify_one();
    });
}

/// Holds one permit across a chain of steps and gives it back exactly once.
///
/// The chain hands its permit straight to the next key on every ordinary
/// finish. Dropping — on a panic, on a refused spawn, or on a task discarded
/// with its runtime — releases the key and the permit instead, so neither is
/// ever stranded.
struct PermitChain {
    inner: Arc<RunnerInner>,
    key: Option<MaintenanceKey>,
}

impl Drop for PermitChain {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.inner.lock_state().admission.abandon(&key);
            self.inner.wake.notify_one();
        }
    }
}

async fn run_step(inner: &Arc<RunnerInner>, key: &MaintenanceKey) -> StepOutcome {
    let Some(job) = inner.job(key.job) else {
        // Nudged for a job nobody registered: there is nothing to run and
        // nothing to reconcile.
        return StepOutcome::Concluded(MaintenanceStepConclusion::NotEnabled);
    };
    let started = tokio::time::Instant::now();
    match job.step(&key.namespace_id).await {
        Ok(conclusion) => {
            tracing::debug!(
                job = %key.job,
                namespace_id = %key.namespace_id,
                conclusion = conclusion.as_str(),
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "maintenance step settled"
            );
            StepOutcome::Concluded(conclusion)
        }
        Err(error) => {
            tracing::info!(
                job = %key.job,
                namespace_id = %key.namespace_id,
                result = "error",
                error = %error,
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

/// The one timer: it wakes for the soonest deadline any key is parked on,
/// and at least once per reconciliation interval.
///
/// It holds the runner weakly and re-upgrades after every sleep, so a
/// handle dropped without a shutdown lets this task exit on its own rather
/// than keeping the runtime state alive behind it.
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
        inner.lock_state().admission.promote_due(now_ms);
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

/// How long the timer may sleep: until the soonest deadline, and never past
/// the reconciliation interval — which also bounds how long the task
/// outlives a runner nobody holds any more. `None` means admission closed.
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

/// Asks a bounded slice of the admitted keys whether they have work.
///
/// Scope is what this process admitted and nothing else: no namespace
/// discovery, no listing. Across a restart that scope is empty, so recovery
/// of work admitted by a previous process is best-effort until an external
/// queue owns admission; within one process lifetime nothing admitted is
/// permanently lost.
async fn reconcile(inner: &Arc<RunnerInner>) {
    let batch = inner
        .lock_state()
        .admission
        .reconcile_batch(MAX_RECONCILE_PROBES_PER_SWEEP);
    for key in batch {
        let Some(job) = inner.job(key.job) else {
            inner.lock_state().admission.forget(&key);
            continue;
        };
        match job.probe(&key.namespace_id).await {
            Ok(MaintenanceProbe::Due) => {
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
            // again rather than deciding on a failed read.
            Err(error) => tracing::debug!(
                job = %key.job,
                namespace_id = %key.namespace_id,
                result = "error",
                error = %error,
                "reconciliation probe failed"
            ),
        }
    }
}
