//! Optional in-process scheduler for registered maintenance jobs.

use super::admission::{Admission, MaintenanceDispatch, MaintenanceKey, StepOutcome};
use super::hints::{dropped_hints, MaintenanceHintReceiver};
use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceHint, MaintenanceJob,
    MaintenanceJobId, MaintenanceProbe, MaintenanceRegistry, MaintenanceRunReport,
};
use crate::metrics::{MaintenanceInstruments, MetricsRecorder, RESULT_ERROR};
use crate::{NamespaceId, Result, RuntimeError};
use futures::FutureExt as _;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Interval between reconciliation sweeps of admitted maintenance keys.
///
/// Sweeps recover work that was blocked, left behind by a quiet writer, or
/// missed by a trigger. This interval is the maximum expected delay before
/// such work is probed again.
pub(crate) const RECONCILE_INTERVAL_MS: u64 = 60_000;

/// Most admitted keys one reconciliation sweep probes. The sweep resumes
/// where it stopped, so a large admitted set costs more sweeps rather than
/// one long one.
const MAX_RECONCILE_PROBES_PER_SWEEP: usize = 64;

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
pub(crate) const JITTER_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

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
pub(crate) fn split_mix_64(counter: u64) -> u64 {
    let mut mixed = counter;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// Cloneable handle for non-blocking maintenance hints.
///
/// Nudges coalesce and never wait for a permit. They are ignored after
/// admission closes or after the owning runner is dropped.
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
            None => loonfs_core::time::current_time_ms().unwrap_or(0),
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

    /// Applies one best-effort scheduling hint.
    pub fn hint(&self, hint: MaintenanceHint) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        match hint {
            MaintenanceHint::Published(publication) => {
                for id in inner.registry.job_ids() {
                    if inner
                        .registry
                        .get(id)
                        .is_some_and(|job| job.should_run_after_publication(&publication))
                    {
                        nudge_if_inactive(&inner, id, &publication.namespace_id);
                    }
                }
            }
            MaintenanceHint::WalFoldFinished { namespace_id } => {
                for id in inner.registry.job_ids() {
                    if inner
                        .registry
                        .get(id)
                        .is_some_and(|job| job.should_run_after_fold())
                    {
                        nudge_if_inactive(&inner, id, &namespace_id);
                    }
                }
            }
            MaintenanceHint::DueAt {
                namespace_id,
                job,
                not_before_ms,
            } => nudge_key(&inner, job, &namespace_id, Some(not_before_ms)),
        }
    }
}

/// Optional in-process scheduler over a maintenance registry.
#[derive(Clone)]
pub struct MaintenanceRunner {
    inner: Arc<RunnerInner>,
}

/// Builder for [`MaintenanceRunner`].
pub struct MaintenanceRunnerBuilder {
    registry: MaintenanceRegistry,
    max_concurrent: usize,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    #[cfg(test)]
    clock: Option<Arc<dyn MaintenanceClock>>,
}

/// Current process-local scheduler state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceRunnerStats {
    /// Jobs currently registered.
    pub jobs_registered: usize,
    /// Job and namespace keys in reconciliation scope.
    pub keys_admitted: usize,
    /// Invocations holding scheduler permits.
    pub running: usize,
    /// Age of the oldest ready key.
    pub oldest_queued_ms: u64,
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
    /// Runtime that owns spawned work.
    runtime: tokio::runtime::Handle,
    clock: Arc<dyn MaintenanceClock>,
    registry: MaintenanceRegistry,
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
    instruments: Arc<MaintenanceInstruments>,
}

struct RunnerState {
    admission: Admission,
    tasks: Vec<JoinHandle<()>>,
    hint_tasks: Vec<JoinHandle<()>>,
    hint_controls: Vec<tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>>,
    cancellations: std::collections::BTreeMap<MaintenanceKey, MaintenanceCancellation>,
    /// The one timer task: it wakes for not-before times, error backoffs,
    /// and the reconciliation interval. Started on first admitted work and
    /// joined by the drain that follows a shutdown.
    scheduler: Option<JoinHandle<()>>,
    next_reconcile_ms: u64,
}

impl MaintenanceRunner {
    /// Starts a builder over `registry`.
    pub fn builder(registry: MaintenanceRegistry) -> MaintenanceRunnerBuilder {
        MaintenanceRunnerBuilder {
            registry,
            max_concurrent: crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            metrics_recorder: None,
            #[cfg(test)]
            clock: None,
        }
    }

    fn new(
        registry: MaintenanceRegistry,
        runtime: tokio::runtime::Handle,
        max_concurrent: usize,
        clock: Arc<dyn MaintenanceClock>,
        metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    ) -> Self {
        let next_reconcile_ms = clock.now_ms().saturating_add(RECONCILE_INTERVAL_MS);
        Self {
            inner: Arc::new(RunnerInner {
                runtime,
                registry,
                state: Mutex::new(RunnerState {
                    admission: Admission::new(max_concurrent, Arc::clone(&clock)),
                    tasks: Vec::new(),
                    hint_tasks: Vec::new(),
                    hint_controls: Vec::new(),
                    cancellations: std::collections::BTreeMap::new(),
                    scheduler: None,
                    next_reconcile_ms,
                }),
                clock,
                wake: Arc::new(Notify::new()),
                counters: RunnerCounters::default(),
                panicked_tasks: std::sync::atomic::AtomicUsize::new(0),
                instruments: MaintenanceInstruments::new(metrics_recorder),
            }),
        }
    }

    /// Returns a cloneable scheduling handle that cannot shut down admission.
    pub fn handle(&self) -> MaintenanceHandle {
        MaintenanceHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Returns the registry this runner schedules.
    pub fn registry(&self) -> &MaintenanceRegistry {
        &self.inner.registry
    }

    /// Stops admission, discards queued work, and cancels running invocations.
    ///
    /// Running steps and cancelled compactions remain visible to
    /// [`Self::drain`]. This method is synchronous so admission closes before
    /// any shutdown await can allow another job to start.
    pub fn close_admission(&self) {
        let mut state = self.inner.lock_state();
        state.admission.close();
        for cancellation in state.cancellations.values() {
            cancellation.cancel();
        }
        drop(state);
        self.inner.wake.notify_one();
    }

    /// Waits for every spawned step to finish, surfacing panics.
    ///
    /// Loops because an in-flight write may schedule more work while an open
    /// handle waits, and because a finishing step hands its permit to the
    /// next queued key before it exits.
    pub async fn drain(&self) -> Result<()> {
        let controls = self.inner.lock_state().hint_controls.clone();
        for control in controls {
            let (settled, wait) = tokio::sync::oneshot::channel();
            if control.send(settled).is_ok() {
                let _ = wait.await;
            }
        }
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
        let hint_tasks = {
            let mut state = self.inner.lock_state();
            if state.admission.is_closed() {
                std::mem::take(&mut state.hint_tasks)
            } else {
                Vec::new()
            }
        };
        for task in hint_tasks {
            if task.await.is_err_and(|error| error.is_panic()) {
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

    /// Closes admission and drains all tasks.
    pub async fn shutdown(&self) -> Result<()> {
        self.close_admission();
        self.drain().await
    }

    /// Returns a process-local scheduler snapshot.
    pub fn stats(&self) -> MaintenanceRunnerStats {
        let now_ms = self.inner.clock.now_ms();
        let state = self.inner.lock_state();
        MaintenanceRunnerStats {
            jobs_registered: self.inner.registry.job_ids().len(),
            keys_admitted: state.admission.keys_admitted(),
            running: state.admission.running(),
            oldest_queued_ms: state.admission.oldest_queued_ms(now_ms),
        }
    }

    /// Forwards a bounded relay into this runner.
    pub fn attach_hints(&self, mut receiver: MaintenanceHintReceiver) {
        let weak = Arc::downgrade(&self.inner);
        let wake = Arc::clone(&self.inner.wake);
        let (control, mut commands) =
            tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
        let task = self.inner.runtime.spawn(async move {
            loop {
                tokio::select! {
                    hint = receiver.receiver.recv() => {
                        let Some(hint) = hint else { break };
                        let Some(inner) = weak.upgrade() else { break };
                        MaintenanceHandle { inner: Arc::downgrade(&inner) }.hint(hint);
                        inner.instruments.hints_dropped(
                            dropped_hints().saturating_sub(receiver.dropped_at_creation),
                        );
                    }
                    command = commands.recv() => {
                        let Some(settled) = command else { break };
                        while let Ok(hint) = receiver.receiver.try_recv() {
                            let Some(inner) = weak.upgrade() else { break };
                            MaintenanceHandle { inner: Arc::downgrade(&inner) }.hint(hint);
                        }
                        let _ = settled.send(());
                    }
                    () = wake.notified() => {
                        let Some(inner) = weak.upgrade() else { break };
                        if inner.lock_state().admission.is_closed() {
                            break;
                        }
                    }
                }
            }
        });
        let mut state = self.inner.lock_state();
        state.hint_controls.push(control);
        state.hint_tasks.push(task);
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
        self.inner.registry.get(job).is_some()
    }

    /// Permits currently held by running chains.
    #[cfg(test)]
    pub(crate) fn running_steps(&self) -> usize {
        self.inner.lock_state().admission.running()
    }
}

impl MaintenanceRunnerBuilder {
    /// Sets the shared invocation permit count.
    pub fn max_concurrent(mut self, permits: usize) -> Self {
        self.max_concurrent = permits;
        self
    }

    /// Registers maintenance instruments with `recorder`.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(recorder);
        self
    }

    #[cfg(test)]
    pub(crate) fn clock(mut self, clock: Arc<dyn MaintenanceClock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Builds the runner on the current Tokio runtime.
    pub fn build(self) -> Result<MaintenanceRunner> {
        if self.max_concurrent == 0 {
            return Err(RuntimeError::Config(
                "`max_concurrent` must be greater than zero".to_owned(),
            ));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            RuntimeError::Config(
                "maintenance runner must be built inside a Tokio runtime".to_owned(),
            )
        })?;
        #[cfg(test)]
        let clock = self
            .clock
            .unwrap_or_else(|| Arc::new(SystemMaintenanceClock::default()));
        #[cfg(not(test))]
        let clock = Arc::new(SystemMaintenanceClock::default()) as Arc<dyn MaintenanceClock>;
        Ok(MaintenanceRunner::new(
            self.registry,
            runtime,
            self.max_concurrent,
            clock,
            self.metrics_recorder,
        ))
    }
}

impl RunnerInner {
    fn lock_state(&self) -> MutexGuard<'_, RunnerState> {
        self.state.lock().expect("maintenance state lock poisoned")
    }

    fn job(&self, id: MaintenanceJobId) -> Option<Arc<dyn MaintenanceJob>> {
        self.registry.get(id)
    }

    /// Spawns maintenance on the owning runtime and tracks it for shutdown.
    ///
    /// If admission has closed, the future is dropped. It must release any
    /// claimed key or permit when dropped.
    pub(super) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
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
        state.tasks.push(self.runtime.spawn(future));
    }
}

/// Records a request against a key and starts whatever it makes runnable.
fn nudge_key(
    inner: &Arc<RunnerInner>,
    job: MaintenanceJobId,
    namespace_id: &NamespaceId,
    not_before_ms: Option<u64>,
) {
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

fn nudge_if_inactive(inner: &Arc<RunnerInner>, job: MaintenanceJobId, namespace_id: &NamespaceId) {
    let key = MaintenanceKey::new(job, namespace_id);
    {
        let mut state = inner.lock_state();
        if state.admission.is_closed() || state.admission.is_active(&key) {
            return;
        }
        state.admission.nudge(key);
    }
    ensure_scheduler(inner);
    dispatch_ready(inner);
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

struct InvocationCancellation {
    inner: Arc<RunnerInner>,
    key: MaintenanceKey,
    cancellation: MaintenanceCancellation,
}

impl InvocationCancellation {
    fn new(inner: &Arc<RunnerInner>, key: &MaintenanceKey) -> Self {
        let cancellation = MaintenanceCancellation::new();
        let mut state = inner.lock_state();
        if state.admission.is_closed() {
            cancellation.cancel();
        }
        state
            .cancellations
            .insert(key.clone(), cancellation.clone());
        report_compactions(&state, &inner.instruments);
        drop(state);
        Self {
            inner: Arc::clone(inner),
            key: key.clone(),
            cancellation,
        }
    }
}

impl Drop for InvocationCancellation {
    fn drop(&mut self) {
        let mut state = self.inner.lock_state();
        state.cancellations.remove(&self.key);
        report_compactions(&state, &self.inner.instruments);
    }
}

fn report_compactions(state: &RunnerState, instruments: &MaintenanceInstruments) {
    let active = state
        .cancellations
        .keys()
        .filter(|key| key.job == MaintenanceJobId::METADATA_COMPACTION)
        .count();
    let running = active.min(super::metadata_compaction::MAX_CONCURRENT_COMPACTIONS);
    instruments.compactions(running, active.saturating_sub(running));
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
        return StepOutcome::Concluded(MaintenanceRunReport::concluded(
            MaintenanceConclusion::NotEnabled,
        ));
    };
    let continuation = dispatch.continuation.as_deref();
    let queued_ms = dispatch.queue_wait_ms;
    let started = tokio::time::Instant::now();
    let invocation = InvocationCancellation::new(inner, key);
    match job
        .run(&key.namespace_id, continuation, &invocation.cancellation)
        .await
    {
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
            if let Some(follow_up) = result.follow_up {
                inner.instruments.follow_up(follow_up);
                nudge_if_inactive(inner, follow_up, &key.namespace_id);
            }
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
    let mut state = inner.lock_state();
    if state.scheduler.is_some() || state.admission.is_closed() {
        return;
    }
    let weak = Arc::downgrade(inner);
    let wake = Arc::clone(&inner.wake);
    state.scheduler = Some(inner.runtime.spawn(scheduler_loop(weak, wake)));
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
