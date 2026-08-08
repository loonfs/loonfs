//! One runner admits every background maintenance step.
//!
//! A [`MaintenanceJob`] knows how to do one bounded piece of upkeep for one
//! namespace: it re-reads durable state, does at most one unit of work,
//! publishes through a compare-and-swap, and reports a
//! [`MaintenanceStepResult`]. It decides nothing about when to run.
//!
//! The [`MaintenanceRunner`] decides that, and it is the only thing that
//! does. It holds the pending `{job, namespace}` keys, the one permit pool
//! every job shares, the per-key error backoff, the not-before times a
//! wall-clock obligation plants, the opaque continuation a bounded job
//! stopped at, and a bounded sweep over the keys it has admitted. Triggers
//! — a publish crossing a threshold, an upload session opening — are hints:
//! level-triggered, never authoritative, and safe to lose because the
//! durable state a step re-reads is the truth.
//!
//! A job keeps no scheduling state of its own. Where its last bounded step
//! stopped comes back to it as the `continuation` argument of the next one,
//! so there is one place that knows what a key is waiting for. That state
//! is in memory and performance-only: a step rebuilds every safety proof
//! from durable state whatever position it starts from, so a continuation
//! lost with the process costs a restarted pass and authorizes nothing.
//!
//! ## What automatic maintenance covers
//!
//! Automatic maintenance covers namespaces touched by the running process
//! and namespaces explicitly assigned to a maintenance host. It never
//! discovers namespaces: LoonFS has no operation that enumerates them, and
//! this runner introduces none. A namespace enters the admitted set by
//! being nudged — by a write, a query, a timed obligation, or an explicit
//! assignment — and reconciliation revisits exactly that set. A namespace
//! that no process has touched and no host has been assigned is outside
//! this guarantee, and an operator brings it back in by assigning it.
//!
//! LoonFS never creates a hidden runtime for maintenance. Steps are spawned
//! on the writer's own owning runtime and stay visible to shutdown through
//! the registry here. Only a writer owns a runner: readers and admins
//! schedule nothing.

mod admission;
mod jobs;
#[cfg(test)]
mod tests;

use crate::metrics::RuntimeInstruments;
use crate::{NamespaceId, Result, RuntimeError};
use admission::{Admission, MaintenanceDispatch, MaintenanceKey, StepOutcome};
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
    ///
    /// The namespaces it covers are the ones this process touches and the
    /// ones a host explicitly assigns to it — never a discovered set.
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

/// Everything one bounded step tells the runner.
///
/// A job returns this instead of a bare conclusion so that the scheduling
/// state it produces — where it stopped, and when it should next be looked
/// at — lives in the runner rather than in a map beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStepResult {
    /// What the step accomplished.
    pub conclusion: MaintenanceStepConclusion,
    /// Where this step stopped, for the next one to resume from. Opaque to
    /// the runner, which stores it and hands it straight back.
    ///
    /// The runner keeps it while the key is making progress or waiting for
    /// room to work, clears it on [`MaintenanceStepConclusion::Idle`], and
    /// drops it with the key on [`MaintenanceStepConclusion::NotEnabled`].
    /// It never crosses a process boundary: a job that cannot restart its
    /// pass from the beginning safely must not use this.
    pub continuation: Option<String>,
    /// The earliest wall-clock millisecond this step saw work becoming
    /// eligible — a lease that will expire, a grace window that will pass.
    ///
    /// It joins the deadlines triggers plant, under the same rule: the
    /// soonest of them is when the runner wakes, the latest is when the key
    /// stops owing anything. `None` when the step observed no deadline,
    /// which is not a claim that there is none.
    pub not_before_ms: Option<u64>,
}

impl MaintenanceStepResult {
    /// A conclusion with nothing to resume from and no deadline observed —
    /// what a job whose whole position is durable returns.
    pub fn concluded(conclusion: MaintenanceStepConclusion) -> Self {
        Self {
            conclusion,
            continuation: None,
            not_before_ms: None,
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
    ///
    /// `continuation` is whatever this job's last step for this namespace
    /// returned, or `None` for a fresh pass — after a restart, after an
    /// idle conclusion, or the first time the key is admitted. A job that
    /// has no position to carry ignores it and returns
    /// [`MaintenanceStepResult::concluded`].
    async fn step(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult>;

    /// Answers whether `namespace_id` has work waiting, as cheaply as this
    /// job can. Called only by reconciliation, never on the hot path.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe>;

    /// Whether a landed publication is a reason to look at this job's work
    /// for the namespace that published it.
    ///
    /// A job that derives something from the namespace's own history — an
    /// index, a projection — says `true` and is nudged after every
    /// publication the writer commits, so it needs no hook of its own on
    /// the write path. The nudge is a hint like any other: it is dropped
    /// when admission is closed, and the step it suggests re-reads what the
    /// publication committed rather than trusting it.
    ///
    /// Jobs that answer to durable deadlines rather than to writes leave
    /// this `false`, which is why it is the default.
    fn nudged_by_publications(&self) -> bool {
        false
    }
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

    /// A draw uniform in `0..span_ms`, and `0` when `span_ms` is zero.
    ///
    /// The error backoff jitters with this. It rides on the clock because
    /// it answers the same kind of question — when to look again — and
    /// because a test that substitutes one has to be able to name the
    /// delays it asserts on.
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
        // Seeded per instance, so two hosts riding out one provider outage
        // do not draw the same retry sequence. The standard library's own
        // randomly keyed hasher is the seed: this decides when to look
        // again and nothing else, so it needs spread rather than secrecy.
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
        // A clock before the unix epoch reads as the epoch here rather than
        // failing. Every eligibility test is `at_ms <= now_ms`, so the
        // epoch is the reading under which nothing timed comes due: work
        // that answers to a durable deadline parks until something nudges
        // it. That is the safe end to fall off. The opposite fallback would
        // make every deadline due at once, and since the timer's sleep is
        // floored at one millisecond it would drive reconciliation sweeps
        // at that floor for as long as the clock stayed wrong. A pre-epoch
        // clock is also a condition every durable path refuses outright, so
        // there is no useful maintenance to run through it either way.
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

/// SplitMix64's finalizer: three xor-shift-multiply rounds over a counter.
///
/// Two keys that fail in the same millisecond draw consecutive counters,
/// and this is what makes those two draws land far apart.
fn split_mix_64(counter: u64) -> u64 {
    let mut mixed = counter;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
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
            None => SystemMaintenanceClock::default().now_ms(),
        }
    }

    /// Asks for `job` to run against `namespace_id` as soon as a permit is
    /// free. Repeated asks coalesce into one run.
    pub fn nudge(&self, job: MaintenanceJobId, namespace_id: &NamespaceId) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        nudge_key(&inner, job, namespace_id, None);
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
        nudge_key(&inner, job, namespace_id, Some(not_before_ms));
    }
}

/// Owner of background maintenance for one write-capable handle:
/// registration, admission, and the shutdown that settles both.
pub(crate) struct MaintenanceRunner {
    inner: Arc<RunnerInner>,
}

/// Running totals a trace reports beside the event that moved them.
///
/// Counters rather than a metrics framework, and process-wide rather than
/// per key: what an operator asks of them is whether backoffs and timed
/// wakes are happening at all and roughly how often, which a monotonic
/// number on an existing event answers without any new cardinality.
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
    counters: RunnerCounters,
    /// Where settled steps report. The two [`RunnerCounters`] above stay
    /// where they are: they answer "is this happening at all", which a
    /// number on an existing trace answers for a reader with no metrics
    /// pipeline at all.
    instruments: Arc<RuntimeInstruments>,
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
                instruments,
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

    /// The executor registered under `id`.
    ///
    /// For a host that drives bounded steps itself instead of through
    /// admission — a catch-up command with a caller's budget on it. The
    /// steps are the same bounded, compare-and-swap-published units this
    /// runner admits; they simply have no scheduler in front of them.
    pub(crate) fn job(&self, id: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.inner.job(id)
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
    pub(crate) fn close_admission(&self) {
        self.inner.lock_state().admission.close();
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

    /// Tells every job that subscribes to publications that `namespace_id`
    /// just committed one.
    ///
    /// The write path calls this instead of knowing which jobs care, so a
    /// job that wants publication nudges gets them by saying so on the
    /// trait rather than by a host wiring an observer to it. Each nudge is
    /// an ordinary one: non-blocking, coalescing, and dropped once
    /// admission is closed or the policy is
    /// [`FsBackgroundWork::ManualOnly`].
    pub(crate) fn nudge_publication_subscribers(&self, namespace_id: &NamespaceId) {
        // The subscriber list is read out from under the job lock before
        // any nudge takes the scheduling lock: the two are never held
        // together anywhere, and this is the one place that would.
        let subscribers: Vec<MaintenanceJobId> = self
            .inner
            .lock_jobs()
            .iter()
            .filter(|(_, job)| job.nudged_by_publications())
            .map(|(id, _)| *id)
            .collect();
        for job in subscribers {
            nudge_key(&self.inner, job, namespace_id, None);
        }
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
        // What is left behind is the useful half: a queue that keeps
        // growing, or an oldest wait that keeps climbing, is the permit
        // pool being too small for what this process admits. Queue-wide
        // numbers, so no key names appear.
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

/// Holds one permit across a chain of steps and gives it back exactly once.
///
/// The chain hands its permit straight to the next key on every ordinary
/// finish. Dropping — on a panic, on a refused spawn, or on a task discarded
/// with its runtime — releases the key and the permit instead, so neither is
/// ever stranded.
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
async fn run_step(inner: &Arc<RunnerInner>, dispatch: &MaintenanceDispatch) -> StepOutcome {
    let key = &dispatch.key;
    let Some(job) = inner.job(key.job) else {
        // Nudged for a job nobody registered: there is nothing to run and
        // nothing to reconcile.
        return StepOutcome::Concluded(MaintenanceStepResult::concluded(
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
                result = "error",
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
/// discovery, no listing. Automatic maintenance therefore covers the
/// namespaces this process has touched and the ones a host has explicitly
/// assigned to it — a set that starts empty at every start-up and grows
/// only by being nudged. Nothing admitted is lost within one process
/// lifetime; a namespace outside that set is outside the guarantee until
/// something touches it or an operator assigns it.
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
                    result = "error",
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
