//! Synchronous scheduling state for maintenance jobs.
//!
//! The runner's lock protects admission, coalescing, fairness, backoff,
//! deadlines, continuations, and shutdown. The parent module handles async
//! execution, permits, and timers.

use super::{MaintenanceClock, MaintenanceJobId, MaintenanceStepConclusion, MaintenanceStepReport};
use crate::NamespaceId;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// Smallest error backoff window, and the one a first failure draws from.
///
/// Small on purpose: one flaky call should come back quickly.
const ERROR_BACKOFF_BASE_MS: u64 = 10;
/// Maximum per-key error backoff window.
///
/// A one-minute cap prevents all admitted namespaces from retrying rapidly
/// during a provider outage while still detecting recovery within one
/// reconciliation interval.
const ERROR_BACKOFF_CAP_MS: u64 = 60_000;

/// One admitted unit of maintenance: a registered job, and the namespace it
/// runs against.
///
/// Ordering is by job first, so a reconciliation sweep walks one job's
/// namespaces together.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MaintenanceKey {
    pub(crate) job: MaintenanceJobId,
    pub(crate) namespace_id: NamespaceId,
}

impl MaintenanceKey {
    pub(crate) fn new(job: MaintenanceJobId, namespace_id: &NamespaceId) -> Self {
        Self {
            job,
            namespace_id: namespace_id.clone(),
        }
    }
}

/// A claimed maintenance step and the scheduling data needed to run it.
///
/// This value is captured while holding the admission lock. The executor
/// does not read the key's mutable scheduling state again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaintenanceDispatch {
    /// The key this permit is running.
    pub(crate) key: MaintenanceKey,
    /// Where this key's job said its last step stopped, opaque here and
    /// handed straight back to the step about to run.
    pub(crate) continuation: Option<String>,
    /// How long the claimed run waited between taking its ticket and holding
    /// a permit. Read by the step's own trace.
    pub(crate) queue_wait_ms: u64,
}

/// How one step ended, from admission's point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepOutcome {
    /// The executor answered. The result decides what happens to the key:
    /// its conclusion when to run again, its continuation where to resume,
    /// its not-before time when a deadline it saw comes due.
    Concluded(MaintenanceStepReport),
    /// The executor failed. The key is retried after its backoff, from
    /// wherever its last step left it.
    Failed,
}

/// One run a key has asked for.
#[derive(Debug, Clone, Copy)]
struct ReadyRun {
    /// Arrival ticket. Ticket order is the fairness rule: among eligible
    /// runs the one that has waited longest takes the next free permit.
    ticket: u64,
    /// When that ticket was taken, which is what the claim reports as the
    /// run's queue wait.
    queued_at_ms: u64,
    /// Backoff gate: not claimable before this instant.
    eligible_at_ms: Option<u64>,
}

impl ReadyRun {
    fn queued(ticket: u64, now_ms: u64) -> Self {
        Self {
            ticket,
            queued_at_ms: now_ms,
            eligible_at_ms: None,
        }
    }

    fn gated(ticket: u64, now_ms: u64, until_ms: u64) -> Self {
        Self {
            eligible_at_ms: Some(until_ms),
            ..Self::queued(ticket, now_ms)
        }
    }

    /// Coalesces two requests for the same key.
    ///
    /// The older ticket and queue time are preserved. The result also preserves
    /// the later eligibility time, so a new request cannot bypass backoff.
    fn coalesced(self, other: Self) -> Self {
        Self {
            ticket: self.ticket.min(other.ticket),
            queued_at_ms: self.queued_at_ms.min(other.queued_at_ms),
            // `None` sorts below `Some`, so an ungated request never erases
            // a backoff the other one is serving.
            eligible_at_ms: self.eligible_at_ms.max(other.eligible_at_ms),
        }
    }

    fn is_eligible(&self, now_ms: u64) -> bool {
        self.eligible_at_ms.is_none_or(|at_ms| at_ms <= now_ms)
    }
}

/// Where one key stands in the singleflight.
#[derive(Debug, Default, Clone, Copy)]
enum KeyRunState {
    /// Nothing asked for, nothing running.
    #[default]
    Parked,
    /// One run asked for, waiting for a permit.
    Queued(ReadyRun),
    /// A step is running. `rerun` is a request that arrived while it ran,
    /// deferred rather than dropped — and deferred rather than run first,
    /// because it has the newest ticket there is.
    Running { rerun: Option<ReadyRun> },
}

/// The earliest and latest outstanding deadlines for one key.
///
/// The earliest deadline determines the next wake. After it fires, the
/// latest deadline is retained so later obligations are not lost. Intermediate
/// deadlines need no separate storage because a run at the latest time covers
/// all earlier obligations.
#[derive(Debug, Clone, Copy)]
struct TimedObligations {
    earliest_at_ms: u64,
    latest_at_ms: u64,
}

impl TimedObligations {
    fn at(at_ms: u64) -> Self {
        Self {
            earliest_at_ms: at_ms,
            latest_at_ms: at_ms,
        }
    }

    fn merged(self, at_ms: u64) -> Self {
        Self {
            earliest_at_ms: self.earliest_at_ms.min(at_ms),
            latest_at_ms: self.latest_at_ms.max(at_ms),
        }
    }

    /// Re-arms the latest outstanding deadline after the earliest one fires.
    ///
    /// Returns `None` when all recorded deadlines have passed.
    fn re_armed(self, now_ms: u64) -> Option<Self> {
        (self.latest_at_ms > now_ms).then(|| Self::at(self.latest_at_ms))
    }
}

/// What one key is waiting for.
#[derive(Debug, Default)]
struct KeyState {
    run: KeyRunState,
    /// Deadlines that remain active independently of the current run.
    ///
    /// A publication-triggered run must not cancel a later lease or grace-period
    /// deadline.
    obligations: Option<TimedObligations>,
    /// Consecutive failed steps since the last conclusion, for the backoff.
    consecutive_failures: u32,
    /// Opaque continuation returned by the previous step.
    ///
    /// Admission stores it and passes it to the next step. Losing it only
    /// restarts the scan because each step revalidates durable state.
    continuation: Option<String>,
}

impl KeyState {
    /// The run waiting for a permit, if this key has one.
    fn queued_run(&self) -> Option<ReadyRun> {
        match self.run {
            KeyRunState::Queued(run) => Some(run),
            KeyRunState::Parked | KeyRunState::Running { .. } => None,
        }
    }

    /// The run this key has asked for, whether it is waiting for a permit or
    /// deferred behind the step that is running.
    fn requested_run(&self) -> Option<ReadyRun> {
        match self.run {
            KeyRunState::Queued(run) | KeyRunState::Running { rerun: Some(run) } => Some(run),
            KeyRunState::Parked | KeyRunState::Running { rerun: None } => None,
        }
    }

    fn is_parked(&self) -> bool {
        matches!(self.run, KeyRunState::Parked)
    }

    fn is_running(&self) -> bool {
        matches!(self.run, KeyRunState::Running { .. })
    }

    #[cfg(test)]
    fn is_pending(&self) -> bool {
        self.requested_run().is_some() || self.obligations.is_some()
    }

    /// Records one request to run this key, coalescing into whatever run it
    /// has already asked for.
    fn request_run(&mut self, ticket: u64, now_ms: u64) {
        let asked = ReadyRun::queued(ticket, now_ms);
        self.run = match self.run {
            KeyRunState::Parked => KeyRunState::Queued(asked),
            KeyRunState::Queued(queued) => KeyRunState::Queued(queued.coalesced(asked)),
            KeyRunState::Running { rerun } => KeyRunState::Running {
                rerun: Some(rerun.map_or(asked, |deferred| deferred.coalesced(asked))),
            },
        };
    }

    /// Moves this key's queued run into the running slot, reporting the wait
    /// it just ended. `None` when the key has no queued run to claim.
    fn claim(&mut self, now_ms: u64) -> Option<u64> {
        let run = self.queued_run()?;
        self.run = KeyRunState::Running { rerun: None };
        Some(now_ms.saturating_sub(run.queued_at_ms))
    }

    /// Ends the running step, leaving one queued run out of what the
    /// conclusion asks for and what a request deferred while the step ran —
    /// two requests for the same key, and so one run.
    fn settle(&mut self, concluded: Option<ReadyRun>) {
        self.run = match (self.requested_run(), concluded) {
            (Some(deferred), Some(concluded)) => KeyRunState::Queued(deferred.coalesced(concluded)),
            (Some(run), None) | (None, Some(run)) => KeyRunState::Queued(run),
            (None, None) => KeyRunState::Parked,
        };
    }

    /// Drops everything this key is waiting for. A step already running is
    /// not scheduling: it stays, and reports through [`Admission::finish`].
    fn clear_schedule(&mut self) {
        self.run = match self.run {
            KeyRunState::Running { .. } => KeyRunState::Running { rerun: None },
            KeyRunState::Parked | KeyRunState::Queued(_) => KeyRunState::Parked,
        };
        self.obligations = None;
    }
}

/// The runner's admission book.
#[derive(Debug)]
pub(crate) struct Admission {
    closed: bool,
    next_ticket: u64,
    /// Cap on steps running at once across every job and namespace. The
    /// per-key singleflight bounds each key to one step; this bounds how
    /// many keys may step together, so a burst across many namespaces
    /// cannot fan out into unbounded maintenance.
    max_concurrent: usize,
    /// Permits in use. Held by a running chain across its whole run,
    /// including the handoff from one key to the next.
    running: usize,
    /// Every key this process has admitted, with what it is waiting for.
    ///
    /// This map is the reconciliation scope, and the only scope: a key gets
    /// here by being nudged, never by listing a store or discovering a
    /// namespace.
    keys: BTreeMap<MaintenanceKey, KeyState>,
    /// Where the next reconciliation sweep resumes, so a probe budget
    /// cannot starve the tail of a large admitted set.
    reconcile_cursor: Option<MaintenanceKey>,
    /// Here for the draw the error backoff jitters with, and for the queue
    /// timestamp [`Self::nudge`] stamps — an observability field, not an
    /// input to anything. Every `now_ms` a decision needs is still passed
    /// in, so the tests below can move time by hand and name the delay they
    /// expect.
    clock: Arc<dyn MaintenanceClock>,
}

impl Admission {
    pub(crate) fn new(max_concurrent: usize, clock: Arc<dyn MaintenanceClock>) -> Self {
        Self {
            closed: false,
            next_ticket: 0,
            max_concurrent,
            running: 0,
            keys: BTreeMap::new(),
            reconcile_cursor: None,
            clock,
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Permits currently held by running chains.
    #[cfg(test)]
    pub(crate) fn running(&self) -> usize {
        self.running
    }

    /// Whether the key is admitted with work still owed to it: waiting for a
    /// permit, or carrying a not-before obligation.
    #[cfg(test)]
    pub(crate) fn is_pending(&self, key: &MaintenanceKey) -> bool {
        self.keys.get(key).is_some_and(KeyState::is_pending)
    }

    /// The soonest obligation the key still carries, if any.
    #[cfg(test)]
    pub(crate) fn not_before_ms(&self, key: &MaintenanceKey) -> Option<u64> {
        self.keys
            .get(key)
            .and_then(|state| state.obligations)
            .map(|owed| owed.earliest_at_ms)
    }

    /// Where the key's job stopped last time. A claim carries this to the
    /// step it dispatches; nothing on the running path reads it back out.
    #[cfg(test)]
    pub(crate) fn continuation(&self, key: &MaintenanceKey) -> Option<String> {
        self.keys
            .get(key)
            .and_then(|state| state.continuation.clone())
    }

    /// Keys holding a ticket: work admitted and waiting for a permit.
    pub(crate) fn ready_queued(&self) -> usize {
        self.keys
            .values()
            .filter(|state| state.queued_run().is_some())
            .count()
    }

    /// How long the longest-waiting queued key has been waiting, or `0`
    /// when nothing is queued.
    pub(crate) fn oldest_queued_ms(&self, now_ms: u64) -> u64 {
        self.keys
            .values()
            .filter_map(|state| state.queued_run())
            .map(|run| run.queued_at_ms)
            .min()
            .map_or(0, |queued_at_ms| now_ms.saturating_sub(queued_at_ms))
    }

    fn take_ticket(&mut self) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket = self.next_ticket.saturating_add(1);
        ticket
    }

    /// Records a request to run the key as soon as a permit is free.
    ///
    /// Repeated requests for one key coalesce into the single pending run
    /// its first ticket bought, including requests that arrive while the
    /// key's own step is running.
    pub(crate) fn nudge(&mut self, key: MaintenanceKey) {
        if self.closed {
            return;
        }
        let ticket = self.take_ticket();
        // The only decision-free clock reading here: the queue timestamp a
        // trace reports. Every scheduling decision still takes its `now_ms`
        // as an argument.
        let now_ms = self.clock.now_ms();
        self.keys
            .entry(key)
            .or_default()
            .request_run(ticket, now_ms);
    }

    /// Records a deadline that makes the key runnable at `at_ms`.
    ///
    /// Multiple deadlines retain the earliest next wake and the latest
    /// outstanding obligation. A deadline that has already passed becomes an
    /// immediate nudge.
    pub(crate) fn nudge_at(&mut self, key: MaintenanceKey, at_ms: u64, now_ms: u64) {
        if self.closed {
            return;
        }
        if at_ms <= now_ms {
            self.nudge(key);
            return;
        }
        let owed = &mut self.keys.entry(key).or_default().obligations;
        *owed = Some(match *owed {
            Some(existing) => existing.merged(at_ms),
            None => TimedObligations::at(at_ms),
        });
    }

    /// Queues keys whose earliest deadline has arrived.
    ///
    /// If a key also has a later deadline, that deadline remains armed. Returns
    /// the number of keys promoted.
    pub(crate) fn promote_due(&mut self, now_ms: u64) -> usize {
        if self.closed {
            return 0;
        }
        let mut ticket = self.next_ticket;
        let mut promoted = 0usize;
        for state in self.keys.values_mut() {
            let Some(owed) = state
                .obligations
                .filter(|owed| owed.earliest_at_ms <= now_ms)
            else {
                continue;
            };
            promoted += 1;
            state.obligations = owed.re_armed(now_ms);
            state.request_run(ticket, now_ms);
            ticket = ticket.saturating_add(1);
        }
        self.next_ticket = ticket;
        promoted
    }

    /// Claims a permit for the fairest eligible key, if there is one and the
    /// pool has room. The caller must run the returned dispatch and report
    /// back through [`Self::finish`] or [`Self::abandon`].
    pub(crate) fn try_dispatch(&mut self, now_ms: u64) -> Option<MaintenanceDispatch> {
        if self.closed || self.running >= self.max_concurrent {
            return None;
        }
        let dispatch = self.claim_oldest_eligible(now_ms)?;
        self.running = self.running.saturating_add(1);
        Some(dispatch)
    }

    /// Applies a step result and transfers its permit to the oldest eligible
    /// queued run.
    ///
    /// Requests received during the step keep their own tickets and do not jump
    /// ahead. Applying the result and choosing the next run under one lock
    /// prevents lost requests and keeps busy namespaces from monopolizing the
    /// permit pool.
    pub(crate) fn finish(
        &mut self,
        key: &MaintenanceKey,
        outcome: StepOutcome,
        now_ms: u64,
    ) -> Option<MaintenanceDispatch> {
        self.apply(key, outcome, now_ms);
        let next = if self.closed {
            None
        } else {
            self.claim_oldest_eligible(now_ms)
        };
        if next.is_none() {
            self.running = self.running.saturating_sub(1);
        }
        next
    }

    /// Releases a key and its permit without deciding anything about it —
    /// the path a panicked or dropped step takes.
    pub(crate) fn abandon(&mut self, key: &MaintenanceKey) {
        if let Some(state) = self.keys.get_mut(key) {
            state.settle(None);
        }
        self.running = self.running.saturating_sub(1);
    }

    /// Forgets a key entirely: it is no longer reconciled and owes nothing.
    pub(crate) fn forget(&mut self, key: &MaintenanceKey) {
        if self.keys.get(key).is_some_and(KeyState::is_running) {
            return;
        }
        self.keys.remove(key);
    }

    /// Forgets a key a probe found idle, unless it still owes a timed
    /// obligation — a lease-dated GC key outlives any number of quiet
    /// probes.
    pub(crate) fn forget_if_idle(&mut self, key: &MaintenanceKey) {
        let forgettable = self
            .keys
            .get(key)
            .is_some_and(|state| state.is_parked() && state.obligations.is_none());
        if forgettable {
            self.keys.remove(key);
        }
    }

    /// Rejects further scheduling and drops everything still waiting.
    /// Running steps stay visible to the drain.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        for state in self.keys.values_mut() {
            state.clear_schedule();
        }
    }

    /// Returns the next future deadline from obligations or backoff.
    ///
    /// Expired deadlines are omitted because those keys are already eligible;
    /// they will run when a permit becomes available.
    pub(crate) fn earliest_deadline_ms(&self, now_ms: u64) -> Option<u64> {
        self.keys
            .values()
            .flat_map(|state| {
                [
                    state.obligations.map(|owed| owed.earliest_at_ms),
                    state.requested_run().and_then(|run| run.eligible_at_ms),
                ]
            })
            .flatten()
            .filter(|deadline_ms| *deadline_ms > now_ms)
            .min()
    }

    /// The next slice of admitted keys to probe, resuming where the last
    /// sweep stopped and skipping keys that are already running or already
    /// queued — probing those would only ask a question the queue answers.
    pub(crate) fn reconcile_batch(&mut self, budget: usize) -> Vec<MaintenanceKey> {
        if self.closed || budget == 0 {
            return Vec::new();
        }
        let batch: Vec<MaintenanceKey> = match self.reconcile_cursor.clone() {
            Some(cursor) => self
                .keys
                .range((Bound::Excluded(cursor.clone()), Bound::Unbounded))
                .chain(self.keys.range((Bound::Unbounded, Bound::Included(cursor))))
                .filter(|(_, state)| state.is_parked())
                .map(|(key, _)| key.clone())
                .take(budget)
                .collect(),
            None => self
                .keys
                .iter()
                .filter(|(_, state)| state.is_parked())
                .map(|(key, _)| key.clone())
                .take(budget)
                .collect(),
        };
        self.reconcile_cursor = batch.last().cloned();
        batch
    }

    /// Ends the running step: what it concluded decides the key's
    /// continuation and whether it is queued again, and the key stops
    /// running either way.
    fn apply(&mut self, key: &MaintenanceKey, outcome: StepOutcome, now_ms: u64) {
        let result = match outcome {
            StepOutcome::Failed => {
                self.record_failure(key, now_ms);
                return;
            }
            StepOutcome::Concluded(result) => result,
        };
        if result.conclusion == MaintenanceStepConclusion::NotEnabled {
            // The job has nothing to maintain here at all. Drop the key —
            // its continuation and its obligations included — rather than
            // reconcile it forever.
            self.keys.remove(key);
            return;
        }
        let ticket = self.take_ticket();
        if let Some(state) = self.keys.get_mut(key) {
            state.consecutive_failures = 0;
            let concluding_run = match result.conclusion {
                // Work happened, or the step lost a race it should simply
                // take again: eligible immediately, behind whatever else is
                // waiting, resuming from wherever this step stopped.
                MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded => {
                    state.continuation = result.continuation;
                    Some(ReadyRun::queued(ticket, now_ms))
                }
                // Work is left and this step's policy could not move it.
                // Parks like `Idle` — requeueing zero-progress work would
                // only spin — but keeps where the step stopped, so a retry
                // with room to work resumes instead of walking the same
                // ground again.
                MaintenanceStepConclusion::Blocked => {
                    state.continuation = result.continuation;
                    None
                }
                // Nothing to do. Whatever the last pass was carrying is
                // spent, and the next step starts a fresh one.
                MaintenanceStepConclusion::Idle => {
                    state.continuation = None;
                    None
                }
                MaintenanceStepConclusion::NotEnabled => None,
            };
            state.settle(concluding_run);
        }
        // A deadline the step itself observed joins the ones triggers
        // plant, under the same merge: soonest is when to wake, latest is
        // when the key stops owing anything.
        if let Some(at_ms) = result.not_before_ms {
            self.nudge_at(key.clone(), at_ms, now_ms);
        }
    }

    /// Backs a failed key off and leaves its continuation alone: a failure
    /// says nothing about where the last step stopped, and the retry should
    /// pick up from the same place.
    fn record_failure(&mut self, key: &MaintenanceKey, now_ms: u64) {
        let ticket = self.take_ticket();
        let Some(state) = self.keys.get_mut(key) else {
            return;
        };
        let failures = state.consecutive_failures.saturating_add(1);
        let delay_ms = backoff_delay_ms(failures, self.clock.as_ref());
        state.consecutive_failures = failures;
        state.settle(Some(ReadyRun::gated(
            ticket,
            now_ms,
            now_ms.saturating_add(delay_ms),
        )));
    }

    /// The key whose eligible queued run has waited longest.
    ///
    /// Linear in the admitted set, which is bounded by the namespaces this
    /// process has touched and re-walked once per finished step — far
    /// cheaper than the object-store round trips a step itself makes.
    fn pick_eligible(&self, now_ms: u64) -> Option<MaintenanceKey> {
        self.keys
            .iter()
            .filter_map(|(key, state)| state.queued_run().map(|run| (run, key)))
            .filter(|(run, _)| run.is_eligible(now_ms))
            .min_by_key(|(run, _)| run.ticket)
            .map(|(_, key)| key.clone())
    }

    /// Runs the fairest eligible key on this permit and reports what its
    /// step needs. The permit count is the caller's business, because a
    /// handoff keeps it.
    fn claim_oldest_eligible(&mut self, now_ms: u64) -> Option<MaintenanceDispatch> {
        let key = self.pick_eligible(now_ms)?;
        let state = self.keys.get_mut(&key)?;
        let queue_wait_ms = state.claim(now_ms)?;
        Some(MaintenanceDispatch {
            key,
            continuation: state.continuation.clone(),
            queue_wait_ms,
        })
    }
}

/// How long a key waits after `consecutive_failures` failed steps.
///
/// Draws a full-jitter delay from the exponential backoff window.
///
/// Randomizing each key's delay prevents a provider outage from producing a
/// synchronized retry wave.
fn backoff_delay_ms(consecutive_failures: u32, clock: &dyn MaintenanceClock) -> u64 {
    clock.jitter_below_ms(backoff_window_ms(consecutive_failures))
}

/// The window that delay is drawn from: [`ERROR_BACKOFF_BASE_MS`] doubling
/// per consecutive failure, up to [`ERROR_BACKOFF_CAP_MS`].
fn backoff_window_ms(consecutive_failures: u32) -> u64 {
    let shift = consecutive_failures.saturating_sub(1).min(16);
    ERROR_BACKOFF_BASE_MS
        .saturating_mul(1_u64 << shift)
        .min(ERROR_BACKOFF_CAP_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance_runner::{split_mix_64, JITTER_GAMMA};
    use loonfs_test_support::ids::namespace_id;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    const NOW: u64 = 1_000_000;

    /// The clock these tests inject.
    ///
    /// Every scheduling decision takes its `now_ms` as an argument, so this
    /// exists for one thing: naming the draw the backoff jitters with.
    /// `Top` puts every delay at the last millisecond of its window, which
    /// makes an expected retry time something a test can write down;
    /// `Walking` draws a different value each time, which is what a real
    /// full-jitter clock does and what desynchronization needs.
    #[derive(Debug)]
    enum TestClock {
        Top,
        Walking(AtomicU64),
    }

    impl TestClock {
        fn top() -> Arc<Self> {
            Arc::new(Self::Top)
        }

        fn walking() -> Arc<Self> {
            Arc::new(Self::Walking(AtomicU64::new(0)))
        }
    }

    impl MaintenanceClock for TestClock {
        fn now_ms(&self) -> u64 {
            NOW
        }

        fn jitter_below_ms(&self, span_ms: u64) -> u64 {
            if span_ms == 0 {
                return 0;
            }
            match self {
                Self::Top => span_ms - 1,
                Self::Walking(draws) => draws.fetch_add(1, Ordering::Relaxed) % span_ms,
            }
        }
    }

    /// An admission book whose backoff lands at the top of every window.
    fn book(max_concurrent: usize) -> Admission {
        Admission::new(max_concurrent, TestClock::top())
    }

    fn metadata(name: &str) -> MaintenanceKey {
        MaintenanceKey::new(MaintenanceJobId::METADATA, &namespace_id(name))
    }

    fn gc(name: &str) -> MaintenanceKey {
        MaintenanceKey::new(MaintenanceJobId::GC, &namespace_id(name))
    }

    fn concluded(conclusion: MaintenanceStepConclusion) -> StepOutcome {
        StepOutcome::Concluded(MaintenanceStepReport::concluded(conclusion))
    }

    /// A conclusion that also tells the runner where the step stopped.
    fn continuing(conclusion: MaintenanceStepConclusion, continuation: &str) -> StepOutcome {
        StepOutcome::Concluded(MaintenanceStepReport {
            conclusion,
            continuation: Some(continuation.to_owned()),
            not_before_ms: None,
        })
    }

    fn idle() -> StepOutcome {
        concluded(MaintenanceStepConclusion::Idle)
    }

    /// The key a claim landed on. Most of these tests are about which key
    /// runs next; the ones about what a claim carries assert on the whole
    /// dispatch.
    fn claimed(dispatch: Option<MaintenanceDispatch>) -> Option<MaintenanceKey> {
        dispatch.map(|dispatch| dispatch.key)
    }

    /// The delay the `Top` clock produces for the nth consecutive failure.
    fn top_delay_ms(consecutive_failures: u32) -> u64 {
        backoff_window_ms(consecutive_failures) - 1
    }

    /// Ported from the previous scheduler: a request arriving during an
    /// active step defers and reruns exactly once.
    #[test]
    fn a_request_during_an_active_step_defers_and_reruns_exactly_once() {
        let mut admission = book(8);
        let key = metadata("demo");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "one in-flight step per key"
        );
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.finish(&key, idle(), NOW)),
            Some(key.clone()),
            "with nothing else waiting the deferred request is the oldest \
             eligible run, so it takes the permit the step gave back"
        );
        assert_eq!(
            claimed(admission.finish(&key, idle(), NOW)),
            None,
            "a quiet finish releases the slot"
        );
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(key.clone()),
            "the released slot claims fresh for the next crossing"
        );
    }

    /// Ported: shutdown wins over a deferred rerun.
    #[test]
    fn shutdown_wins_over_a_deferred_rerun() {
        let mut admission = book(8);
        let key = metadata("demo");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        admission.nudge(key.clone());
        admission.close();
        assert_eq!(
            claimed(admission.finish(&key, idle(), NOW)),
            None,
            "a deferred request must not rerun after shutdown closed admission"
        );
    }

    /// The fairness rule has no exception for the key that just finished: a
    /// request that arrived mid-step holds the newest ticket there is, so a
    /// peer that was already waiting runs first.
    #[test]
    fn a_waiting_peer_outranks_a_rerun_asked_for_later() {
        let mut admission = book(1);
        let (busy, peer) = (metadata("busy"), metadata("peer"));

        admission.nudge(busy.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(busy.clone()));
        admission.nudge(peer.clone());
        admission.nudge(busy.clone());

        assert_eq!(
            claimed(admission.finish(&busy, idle(), NOW)),
            Some(peer.clone()),
            "the peer took its ticket first, so the permit is the peer's"
        );
        assert_eq!(
            claimed(admission.finish(&peer, idle(), NOW)),
            Some(busy),
            "and the deferred rerun runs next, on the ticket it took"
        );
    }

    /// A job that subscribes to publications is renudged inside every step
    /// it runs, and a busy namespace publishes without pause. Its reruns
    /// must not add up to a permit it never gives back.
    #[test]
    fn a_key_renudged_on_every_run_cannot_hold_the_permit_past_a_peer() {
        let mut admission = book(1);
        let (busy, peer) = (metadata("busy"), metadata("peer"));

        admission.nudge(busy.clone());
        let mut running = admission.try_dispatch(NOW);
        admission.nudge(peer.clone());

        let mut ran = Vec::new();
        for _ in 0..4 {
            let key = running
                .map(|dispatch| dispatch.key)
                .expect("a key is running");
            // The nudge every publication plants, arriving mid-step.
            admission.nudge(key.clone());
            ran.push(key.clone());
            running = admission.finish(&key, concluded(MaintenanceStepConclusion::Progressed), NOW);
        }

        assert_eq!(
            ran,
            vec![busy.clone(), peer.clone(), busy, peer],
            "an endless chain of reruns cannot starve the peer: the two alternate"
        );
    }

    /// Losing the handoff is not losing the request.
    #[test]
    fn a_rerun_that_loses_the_handoff_runs_when_a_permit_frees() {
        let mut admission = book(1);
        let (busy, peer) = (metadata("busy"), metadata("peer"));

        admission.nudge(busy.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(busy.clone()));
        admission.nudge(peer.clone());
        admission.nudge(busy.clone());
        assert_eq!(
            claimed(admission.finish(&busy, idle(), NOW)),
            Some(peer.clone())
        );
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "the one permit is the peer's while it runs"
        );

        admission.abandon(&peer);
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(busy),
            "the rerun was queued rather than spent, and takes the freed permit"
        );
    }

    /// The other half of a claim: what the step it dispatches is told about
    /// the wait it ended.
    #[test]
    fn a_claim_reports_how_long_its_run_waited() {
        let mut admission = book(1);
        let key = metadata("waiting");

        admission.nudge(key.clone());
        let dispatch = admission
            .try_dispatch(NOW + 5_000)
            .expect("the queued key claims the permit");
        assert_eq!(
            dispatch.queue_wait_ms, 5_000,
            "the claim reports the wait between the ticket and the permit"
        );

        let handed_off = admission
            .finish(
                &key,
                concluded(MaintenanceStepConclusion::Progressed),
                NOW + 5_000,
            )
            .expect("a progressing key with nothing else waiting runs again");
        assert_eq!(
            handed_off.queue_wait_ms, 0,
            "a run created by the finish that hands it the permit waited for nothing"
        );
    }

    /// Ported: claims are singleflight and refused after close.
    #[test]
    fn claims_are_singleflight_and_refused_after_close() {
        let mut admission = book(8);
        let key = metadata("demo");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(claimed(admission.try_dispatch(NOW)), None);
        admission.abandon(&key);
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(key.clone()),
            "a released slot is claimable again"
        );
        admission.abandon(&key);
        admission.close();
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "no new claims after close"
        );
    }

    /// Ported: a freed slot claims the next key waiting at the global cap.
    #[test]
    fn a_freed_slot_claims_the_next_key_at_the_global_cap() {
        let mut admission = book(2);
        let (first, second, third) = (metadata("first"), metadata("second"), metadata("third"));

        for key in [&first, &second, &third] {
            admission.nudge(key.clone());
        }
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(first.clone()));
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(second.clone()));
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "a third key must wait for a slot"
        );
        assert_eq!(
            claimed(admission.finish(&first, idle(), NOW)),
            Some(third),
            "the finishing path transfers its slot to the queued key"
        );
    }

    /// Ported: repeated requests for one queued key coalesce to one run.
    #[test]
    fn repeated_requests_for_one_queued_key_coalesce_to_one_run() {
        let mut admission = book(1);
        let (active, queued) = (metadata("active"), metadata("queued"));

        admission.nudge(active.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(active.clone()));
        for _ in 0..8 {
            admission.nudge(queued.clone());
            assert_eq!(
                claimed(admission.try_dispatch(NOW)),
                None,
                "the queued key cannot claim the occupied slot"
            );
        }
        assert_eq!(
            claimed(admission.finish(&active, idle(), NOW)),
            Some(queued.clone()),
            "one queued run consumes all coalesced requests"
        );
        assert_eq!(
            claimed(admission.finish(&queued, idle(), NOW)),
            None,
            "coalesced requests must not create a second run"
        );
    }

    /// Ported: shutdown clears keys waiting at the global cap.
    #[test]
    fn shutdown_clears_keys_waiting_at_the_global_cap() {
        let mut admission = book(1);
        let (active, queued) = (metadata("active"), metadata("queued"));

        admission.nudge(active.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(active.clone()));
        admission.nudge(queued.clone());
        admission.close();
        assert_eq!(claimed(admission.finish(&active, idle(), NOW)), None);
        assert!(
            !admission.is_pending(&queued),
            "shutdown must clear the queue"
        );
        admission.nudge(queued.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "closed admission must not revive the cleared queue"
        );
    }

    #[test]
    fn progressed_requeues_behind_a_waiting_peer() {
        let mut admission = book(1);
        let (busy, peer) = (metadata("busy"), metadata("peer"));

        admission.nudge(busy.clone());
        admission.nudge(peer.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(busy.clone()));
        assert_eq!(
            claimed(admission.finish(&busy, concluded(MaintenanceStepConclusion::Progressed), NOW)),
            Some(peer.clone()),
            "one unit per step: the peer runs before the busy key folds again"
        );
        assert_eq!(
            claimed(admission.finish(&peer, idle(), NOW)),
            Some(busy),
            "the progressed key is still eligible and takes the next slot"
        );
    }

    #[test]
    fn progressed_keeps_running_when_nothing_else_waits() {
        let mut admission = book(1);
        let key = metadata("alone");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, concluded(MaintenanceStepConclusion::Progressed), NOW)),
            Some(key.clone()),
            "a sole progressing key folds its backlog without waiting"
        );
        assert_eq!(claimed(admission.finish(&key, idle(), NOW)), None);
    }

    #[test]
    fn blocked_parks_and_a_later_nudge_retries() {
        let mut admission = book(1);
        let key = metadata("blocked");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, concluded(MaintenanceStepConclusion::Blocked), NOW)),
            None,
            "zero-progress work must not requeue itself"
        );
        assert_eq!(claimed(admission.try_dispatch(NOW)), None);
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(key),
            "a later nudge retries the blocked key"
        );
    }

    #[test]
    fn superseded_requeues_immediately() {
        let mut admission = book(1);
        let key = metadata("raced");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, concluded(MaintenanceStepConclusion::Superseded), NOW)),
            Some(key),
            "a superseded step takes the race again"
        );
    }

    #[test]
    fn not_enabled_evicts_the_key_and_its_obligation() {
        let mut admission = book(1);
        let key = gc("gone");

        admission.nudge_at(key.clone(), NOW + 5_000, NOW);
        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, concluded(MaintenanceStepConclusion::NotEnabled), NOW)),
            None
        );
        assert!(!admission.is_pending(&key), "the key is forgotten");
        admission.promote_due(NOW + 10_000);
        assert_eq!(
            claimed(admission.try_dispatch(NOW + 10_000)),
            None,
            "an evicted key's obligation goes with it"
        );
    }

    /// The whole continuation contract in one place: a progressing step's
    /// position is stored and handed to the next one, a blocked step's is
    /// kept for a retry with room to work, an idle step's is spent, and an
    /// evicted key takes its position with it.
    #[test]
    fn the_runner_carries_a_continuation_between_steps() {
        let mut admission = book(1);
        let key = gc("paged");

        admission.nudge(key.clone());
        let first = admission
            .try_dispatch(NOW)
            .expect("the nudged key claims the permit");
        assert_eq!(first.key, key);
        assert_eq!(first.continuation, None, "a first step starts a fresh pass");

        let resumed = admission
            .finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW,
            )
            .expect("a progressing key is eligible again");
        assert_eq!(resumed.key, key);
        assert_eq!(
            resumed.continuation,
            Some("page-1".to_owned()),
            "and the handoff carries where this step stopped to the next one"
        );

        assert_eq!(
            claimed(admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Blocked, "page-2"),
                NOW
            )),
            None,
            "a blocked key parks"
        );
        assert_eq!(
            admission.continuation(&key),
            Some("page-2".to_owned()),
            "keeping its position, so a retry with a bigger budget resumes"
        );

        admission.nudge(key.clone());
        let retried = admission
            .try_dispatch(NOW)
            .expect("a later nudge claims the permit again");
        assert_eq!(retried.key, key);
        assert_eq!(
            retried.continuation,
            Some("page-2".to_owned()),
            "the parked position is what the resumed step is handed"
        );
        assert_eq!(claimed(admission.finish(&key, idle(), NOW)), None);
        assert_eq!(
            admission.continuation(&key),
            None,
            "an idle pass is a finished one: the next step starts over"
        );
    }

    #[test]
    fn an_evicted_key_drops_its_continuation() {
        let mut admission = book(1);
        let key = gc("gone");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW
            )),
            Some(key.clone())
        );
        assert_eq!(
            claimed(admission.finish(&key, concluded(MaintenanceStepConclusion::NotEnabled), NOW)),
            None
        );
        assert_eq!(
            admission.continuation(&key),
            None,
            "the key's whole state went with it"
        );

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            admission.continuation(&key),
            None,
            "and a re-admitted key starts a fresh pass"
        );
    }

    #[test]
    fn a_failed_step_keeps_where_its_last_one_stopped() {
        let mut admission = book(1);
        let key = gc("flaky");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW
            )),
            Some(key.clone())
        );
        assert_eq!(
            claimed(admission.finish(&key, StepOutcome::Failed, NOW)),
            None
        );
        assert_eq!(
            admission.continuation(&key),
            Some("page-1".to_owned()),
            "a failure says nothing about where the last step stopped"
        );
    }

    /// A deadline the step itself observed is planted the same way a
    /// trigger's is, and merges with what is already there.
    #[test]
    fn a_step_can_report_its_own_next_eligible_time() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(
                &key,
                StepOutcome::Concluded(MaintenanceStepReport {
                    conclusion: MaintenanceStepConclusion::Idle,
                    continuation: None,
                    not_before_ms: Some(NOW + 60_000),
                }),
                NOW
            )),
            None,
            "an idle step with a deadline still parks until that deadline"
        );
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 60_000));

        admission.promote_due(NOW + 60_000);
        assert_eq!(
            claimed(admission.try_dispatch(NOW + 60_000)),
            Some(key.clone()),
            "the observed deadline brings the key back on its own"
        );
        assert_eq!(
            claimed(admission.finish(
                &key,
                StepOutcome::Concluded(MaintenanceStepReport {
                    conclusion: MaintenanceStepConclusion::Progressed,
                    continuation: None,
                    not_before_ms: Some(NOW + 30_000),
                }),
                NOW + 60_000
            )),
            Some(key.clone()),
            "a deadline already past is just a nudge, and this step progressed"
        );
        assert_eq!(
            admission.not_before_ms(&key),
            None,
            "nothing future is owed"
        );
    }

    #[test]
    fn a_not_before_key_does_not_fire_early_and_fires_after() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge_at(key.clone(), NOW + 60_000, NOW);
        admission.promote_due(NOW);
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "the runner must not fire a key before its time"
        );
        admission.promote_due(NOW + 59_999);
        assert_eq!(claimed(admission.try_dispatch(NOW + 59_999)), None);
        admission.promote_due(NOW + 60_000);
        assert_eq!(
            claimed(admission.try_dispatch(NOW + 60_000)),
            Some(key),
            "the key fires once its time arrives"
        );
    }

    #[test]
    fn the_soonest_of_two_not_before_times_wins() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge_at(key.clone(), NOW + 90_000, NOW);
        admission.nudge_at(key.clone(), NOW + 30_000, NOW);
        admission.nudge_at(key.clone(), NOW + 60_000, NOW);
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 30_000));
        assert_eq!(admission.earliest_deadline_ms(NOW), Some(NOW + 30_000));
        admission.promote_due(NOW + 30_000);
        assert_eq!(claimed(admission.try_dispatch(NOW + 30_000)), Some(key));
    }

    #[test]
    fn an_unrelated_run_never_cancels_a_lease_obligation() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge_at(key.clone(), NOW + 60_000, NOW);
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(key.clone()),
            "an explicit nudge runs now; the obligation is a separate future"
        );
        assert_eq!(claimed(admission.finish(&key, idle(), NOW)), None);
        assert_eq!(
            admission.not_before_ms(&key),
            Some(NOW + 60_000),
            "the obligation survives the run it did not ask for"
        );
        admission.promote_due(NOW + 60_000);
        assert_eq!(claimed(admission.try_dispatch(NOW + 60_000)), Some(key));
    }

    #[test]
    fn a_past_not_before_time_is_just_a_nudge() {
        let mut admission = book(1);
        let key = gc("overdue");

        admission.nudge_at(key.clone(), NOW - 1, NOW);
        assert_eq!(admission.not_before_ms(&key), None);
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key));
    }

    #[test]
    fn a_failed_step_backs_off_before_its_retry() {
        let mut admission = book(1);
        let key = metadata("flaky");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, StepOutcome::Failed, NOW)),
            None,
            "a failed step waits out its backoff before retrying"
        );
        assert_eq!(claimed(admission.try_dispatch(NOW)), None);
        assert_eq!(
            claimed(admission.try_dispatch(NOW + top_delay_ms(1))),
            Some(key.clone())
        );
        assert_eq!(
            claimed(admission.finish(&key, StepOutcome::Failed, NOW)),
            None
        );
        assert_eq!(
            admission.earliest_deadline_ms(NOW),
            Some(NOW + top_delay_ms(2)),
            "consecutive failures draw from a window twice as wide"
        );
        assert_eq!(
            admission.earliest_deadline_ms(NOW + top_delay_ms(2)),
            None,
            "a backoff that has already expired is not something to wake for"
        );
    }

    #[test]
    fn the_backoff_window_doubles_to_a_one_minute_ceiling() {
        assert_eq!(backoff_window_ms(1), ERROR_BACKOFF_BASE_MS);
        assert_eq!(backoff_window_ms(2), 2 * ERROR_BACKOFF_BASE_MS);
        assert_eq!(backoff_window_ms(4), 8 * ERROR_BACKOFF_BASE_MS);
        assert_eq!(
            backoff_window_ms(30),
            ERROR_BACKOFF_CAP_MS,
            "a long outage is retried once a minute, not once a second"
        );
        assert_eq!(backoff_window_ms(u32::MAX), ERROR_BACKOFF_CAP_MS);
    }

    #[test]
    fn every_drawn_delay_stays_inside_its_window() {
        let clock = TestClock::walking();
        for failures in 1..24_u32 {
            let window_ms = backoff_window_ms(failures);
            for _ in 0..64 {
                assert!(
                    backoff_delay_ms(failures, clock.as_ref()) < window_ms,
                    "full jitter draws from inside the window, never past it"
                );
            }
        }
    }

    /// The property a provider outage depends on: keys that failed in the
    /// same millisecond must not all come back in the same millisecond.
    #[test]
    fn a_burst_of_failures_does_not_come_back_synchronized() {
        let mut admission = Admission::new(8, TestClock::walking());
        let keys = [
            metadata("one"),
            metadata("two"),
            metadata("three"),
            metadata("four"),
        ];
        for key in &keys {
            admission.nudge(key.clone());
        }
        // Every key failing at the same instant, six times over, which is
        // what one unreachable object store looks like from here. Six so
        // the window is wide enough for four draws to be distinguishable —
        // a first failure only has ten milliseconds to spread across.
        for _ in 0..6 {
            for key in &keys {
                admission.record_failure(key, NOW);
            }
        }

        let deadlines: std::collections::BTreeSet<u64> = keys
            .iter()
            .map(|key| {
                admission
                    .keys
                    .get(key)
                    .and_then(KeyState::queued_run)
                    .and_then(|run| run.eligible_at_ms)
                    .expect("a failed key carries a retry deadline")
            })
            .collect();
        assert_eq!(
            deadlines.len(),
            keys.len(),
            "keys that failed together must not retry together"
        );
        for deadline_ms in deadlines {
            assert!(
                deadline_ms < NOW + backoff_window_ms(6),
                "and every one of them inside the window it drew from"
            );
        }
    }

    #[test]
    fn a_conclusion_clears_the_failure_streak() {
        let mut admission = book(1);
        let key = metadata("recovered");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, StepOutcome::Failed, NOW)),
            None
        );
        assert_eq!(
            claimed(admission.try_dispatch(NOW + top_delay_ms(1))),
            Some(key.clone())
        );
        assert_eq!(claimed(admission.finish(&key, idle(), NOW)), None);
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            Some(key),
            "a concluded step starts the next failure streak from scratch"
        );
    }

    #[test]
    fn reconciliation_visits_admitted_keys_in_bounded_slices() {
        let mut admission = book(1);
        let keys = [metadata("a"), metadata("b"), metadata("c")];
        for key in &keys {
            admission.nudge(key.clone());
            assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
            assert_eq!(claimed(admission.finish(key, idle(), NOW)), None);
        }

        assert_eq!(
            admission.reconcile_batch(2),
            vec![keys[0].clone(), keys[1].clone()]
        );
        assert_eq!(
            admission.reconcile_batch(2),
            vec![keys[2].clone(), keys[0].clone()],
            "the sweep resumes where it stopped and wraps"
        );
    }

    #[test]
    fn reconciliation_never_visits_a_key_that_was_never_admitted() {
        let mut admission = book(1);
        let touched = metadata("touched");
        admission.nudge(touched.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(touched.clone()));
        assert_eq!(claimed(admission.finish(&touched, idle(), NOW)), None);

        assert_eq!(
            admission.reconcile_batch(16),
            vec![touched],
            "the sweep sees the one key this process admitted and nothing else"
        );
    }

    #[test]
    fn reconciliation_skips_running_and_already_queued_keys() {
        let mut admission = book(1);
        let (running, queued, parked) = (metadata("a"), metadata("b"), metadata("c"));
        admission.nudge(parked.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(parked.clone()));
        assert_eq!(claimed(admission.finish(&parked, idle(), NOW)), None);
        admission.nudge(running.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(running.clone()));
        admission.nudge(queued);

        assert_eq!(
            admission.reconcile_batch(16),
            vec![parked],
            "a probe would only ask what the queue already answers"
        );
    }

    #[test]
    fn an_idle_probe_evicts_only_a_key_that_owes_nothing() {
        let mut admission = book(1);
        let (quiet, leased) = (metadata("quiet"), gc("leased"));
        admission.nudge(quiet.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(quiet.clone()));
        assert_eq!(claimed(admission.finish(&quiet, idle(), NOW)), None);
        admission.nudge_at(leased.clone(), NOW + 60_000, NOW);

        admission.forget_if_idle(&quiet);
        admission.forget_if_idle(&leased);

        assert_eq!(
            admission.reconcile_batch(16),
            vec![leased.clone()],
            "the key that owed nothing is forgotten; the lease-dated one is not"
        );
        assert_eq!(
            admission.not_before_ms(&leased),
            Some(NOW + 60_000),
            "a lease-dated key outlives a quiet probe"
        );
    }

    #[test]
    fn a_later_obligation_re_arms_after_the_soonest_one_fires() {
        let mut admission = book(1);
        let key = gc("leased");

        // What an upload session and its completion plant: a lease that
        // passes in a day, and content reclamation a week out.
        admission.nudge_at(key.clone(), NOW + 86_400_000, NOW);
        admission.nudge_at(key.clone(), NOW + 604_800_000, NOW);
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 86_400_000));

        let day = NOW + 86_400_000;
        admission.promote_due(day);
        assert_eq!(
            claimed(admission.try_dispatch(day)),
            Some(key.clone()),
            "the soonest deadline is what the runner wakes for"
        );
        assert_eq!(claimed(admission.finish(&key, idle(), day)), None);
        assert_eq!(
            admission.not_before_ms(&key),
            Some(NOW + 604_800_000),
            "the later deadline survives the pass that ran for the earlier one"
        );

        let week = NOW + 604_800_000;
        admission.promote_due(week);
        assert_eq!(claimed(admission.try_dispatch(week)), Some(key.clone()));
        assert_eq!(claimed(admission.finish(&key, idle(), week)), None);
        assert_eq!(
            admission.not_before_ms(&key),
            None,
            "past the last deadline the key owes nothing"
        );
        admission.forget_if_idle(&key);
        assert!(admission.reconcile_batch(16).is_empty());
    }

    #[test]
    fn a_backoff_survives_an_ordinary_nudge() {
        let mut admission = book(1);
        let key = metadata("flaky");

        admission.nudge(key.clone());
        assert_eq!(claimed(admission.try_dispatch(NOW)), Some(key.clone()));
        assert_eq!(
            claimed(admission.finish(&key, StepOutcome::Failed, NOW)),
            None
        );
        admission.nudge(key.clone());
        assert_eq!(
            claimed(admission.try_dispatch(NOW)),
            None,
            "coalescing into the queued retry must not erase the gate it is serving"
        );
        assert_eq!(
            claimed(admission.try_dispatch(NOW + top_delay_ms(1))),
            Some(key),
            "and the retry still comes back when its backoff has passed"
        );
    }

    #[test]
    fn closing_admission_clears_pending_obligations() {
        let mut admission = book(1);
        let key = gc("leased");
        admission.nudge_at(key.clone(), NOW + 60_000, NOW);
        admission.close();
        assert_eq!(admission.not_before_ms(&key), None);
        assert_eq!(admission.earliest_deadline_ms(NOW), None);
        admission.promote_due(NOW + 60_000);
        assert_eq!(claimed(admission.try_dispatch(NOW + 60_000)), None);
    }

    /// Seeds the model below replays. Fixed, so a failure is the same
    /// failure on the next run and on someone else's machine.
    const MODEL_SEEDS: [u64; 8] = [1, 2, 3, 5, 8, 13, 21, 34];
    /// Actions per sequence, before admission closes and after.
    const MODEL_ACTIONS: usize = 256;
    const MODEL_ACTIONS_AFTER_CLOSE: usize = 32;

    /// A seeded draw.
    ///
    /// The runner's own SplitMix64 finalizer over a counter: this workspace
    /// has no `rand` dependency, ambient randomness is banned, and a model
    /// whose failures cannot be replayed is not worth running.
    struct Draws(u64);

    impl Draws {
        fn below(&mut self, bound: usize) -> usize {
            self.0 = self.0.wrapping_add(JITTER_GAMMA);
            let bound = u64::try_from(bound).unwrap_or(1).max(1);
            usize::try_from(split_mix_64(self.0) % bound).unwrap_or(0)
        }
    }

    /// The key the one selection rule has to pick, stated here rather than
    /// asked of the book: the eligible queued run holding the oldest ticket.
    fn oldest_eligible(admission: &Admission, now_ms: u64) -> Option<MaintenanceKey> {
        let mut oldest: Option<(u64, &MaintenanceKey)> = None;
        for (key, state) in &admission.keys {
            let Some(run) = state.queued_run().filter(|run| run.is_eligible(now_ms)) else {
                continue;
            };
            if oldest.is_none_or(|(ticket, _)| run.ticket < ticket) {
                oldest = Some((run.ticket, key));
            }
        }
        oldest.map(|(_, key)| key.clone())
    }

    /// What a caller of the book knows, held beside it.
    ///
    /// Deliberately not a second scheduler: it tracks the steps it started
    /// and has not ended, the deadlines it planted, and how often a key
    /// sitting eligible has been passed over — and holds the book to all
    /// three after every single action.
    struct Model {
        admission: Admission,
        draws: Draws,
        keys: Vec<MaintenanceKey>,
        cap: usize,
        now_ms: u64,
        closed: bool,
        /// Keys this driver dispatched and has not finished or abandoned.
        running: BTreeSet<MaintenanceKey>,
        /// The soonest and latest deadline planted per key.
        owed: BTreeMap<MaintenanceKey, (u64, u64)>,
        /// Dispatches that went past a key while it sat eligible.
        passed_over: BTreeMap<MaintenanceKey, usize>,
        action: usize,
        dispatches: usize,
        deepest_wait: usize,
    }

    impl Model {
        fn new(seed: u64, cap: usize, key_count: usize) -> Self {
            let keys = (0..key_count)
                .map(|index| {
                    let name = format!("key-{index}");
                    // Both jobs, so the admitted set is ordered by something
                    // other than the order keys arrive in.
                    if index % 2 == 0 {
                        metadata(&name)
                    } else {
                        gc(&name)
                    }
                })
                .collect();
            Self {
                admission: Admission::new(cap, TestClock::top()),
                draws: Draws(seed),
                keys,
                cap,
                now_ms: NOW,
                closed: false,
                running: BTreeSet::new(),
                owed: BTreeMap::new(),
                passed_over: BTreeMap::new(),
                action: 0,
                dispatches: 0,
                deepest_wait: 0,
            }
        }

        fn run(mut self) {
            for _ in 0..MODEL_ACTIONS {
                self.act();
                self.check();
                self.action += 1;
            }
            self.close();
            self.check();
            for _ in 0..MODEL_ACTIONS_AFTER_CLOSE {
                self.act();
                self.check();
                self.action += 1;
            }
            // A sequence that stopped claiming, or one whose keys never
            // queued behind each other, would assert nothing worth
            // asserting. Both floors are far below what the seeds produce.
            assert!(
                self.dispatches > MODEL_ACTIONS / 8,
                "the sequence ran {} steps, which is too few to have tested anything",
                self.dispatches
            );
            assert!(
                self.deepest_wait > 0,
                "no key ever waited behind another, so ticket order was never in question"
            );
        }

        fn act(&mut self) {
            let key = self.keys[self.draws.below(self.keys.len())].clone();
            let eligible = self.eligible();
            match self.draws.below(32) {
                0..=9 => self.admission.nudge(key),
                10..=13 => self.plant(key),
                14..=20 => self.dispatch(&eligible),
                21..=27 => self.finish(&eligible),
                28..=30 => self.advance_time(),
                // Closing is the one action `run` places itself: drawn, it
                // would end most sequences in their first dozen actions.
                _ => self.abandon(),
            }
        }

        /// A deadline ahead of now plants an obligation; one already past is
        /// an ordinary nudge.
        fn plant(&mut self, key: MaintenanceKey) {
            let at_ms = self
                .now_ms
                .saturating_add(u64::try_from(self.draws.below(4)).unwrap_or(0) * 1_000);
            self.admission.nudge_at(key.clone(), at_ms, self.now_ms);
            if at_ms > self.now_ms && !self.closed {
                let owed = self.owed.entry(key).or_insert((at_ms, at_ms));
                *owed = (owed.0.min(at_ms), owed.1.max(at_ms));
            }
        }

        fn dispatch(&mut self, eligible: &BTreeSet<MaintenanceKey>) {
            let expected = (!self.closed && self.running.len() < self.cap)
                .then(|| oldest_eligible(&self.admission, self.now_ms))
                .flatten();
            let dispatched = self.admission.try_dispatch(self.now_ms);
            assert_eq!(
                dispatched.as_ref().map(|dispatch| &dispatch.key),
                expected.as_ref(),
                "action {}: a claim takes the oldest eligible run, and only when a permit is free",
                self.action
            );
            self.took(dispatched, eligible);
        }

        fn finish(&mut self, eligible: &BTreeSet<MaintenanceKey>) {
            let Some(key) = self.pick_running() else {
                return;
            };
            let conclusion = match self.draws.below(6) {
                0 => Some(MaintenanceStepConclusion::Progressed),
                1 => Some(MaintenanceStepConclusion::Idle),
                2 => Some(MaintenanceStepConclusion::Blocked),
                3 => Some(MaintenanceStepConclusion::Superseded),
                4 => Some(MaintenanceStepConclusion::NotEnabled),
                // The failing step, which is the same report with a backoff
                // behind it.
                _ => None,
            };
            let outcome = conclusion.map_or(StepOutcome::Failed, concluded);
            let dispatched = self.admission.finish(&key, outcome, self.now_ms);
            self.running.remove(&key);
            if conclusion == Some(MaintenanceStepConclusion::NotEnabled) {
                // The key and everything on it, obligations included, is
                // gone.
                self.owed.remove(&key);
            }
            if dispatched.is_none() && !self.closed {
                assert!(
                    oldest_eligible(&self.admission, self.now_ms).is_none(),
                    "action {}: a permit must not be released while eligible work waits",
                    self.action
                );
            }
            self.took(dispatched, eligible);
        }

        fn advance_time(&mut self) {
            self.now_ms = self
                .now_ms
                .saturating_add(u64::try_from(1 + self.draws.below(3)).unwrap_or(1) * 1_000);
            let due = self
                .owed
                .values()
                .filter(|(earliest_at_ms, _)| *earliest_at_ms <= self.now_ms)
                .count();
            assert_eq!(
                self.admission.promote_due(self.now_ms),
                if self.closed { 0 } else { due },
                "action {}: exactly the arrived deadlines are promoted",
                self.action
            );
            let now_ms = self.now_ms;
            self.owed.retain(|_, (earliest_at_ms, latest_at_ms)| {
                if *earliest_at_ms > now_ms {
                    return true;
                }
                // The pass that fires re-arms on the latest deadline, and
                // owes nothing once that one is past too.
                *earliest_at_ms = *latest_at_ms;
                *latest_at_ms > now_ms
            });
        }

        fn abandon(&mut self) {
            let Some(key) = self.pick_running() else {
                return;
            };
            self.admission.abandon(&key);
            self.running.remove(&key);
        }

        fn close(&mut self) {
            self.admission.close();
            self.closed = true;
            self.owed.clear();
        }

        /// Records a claim: the driver is running one more key, and every
        /// key it went past has waited one claim longer.
        ///
        /// Ticket order across a whole sequence is what this bounds. A key
        /// sitting eligible can be passed over at most once per other key:
        /// only an older ticket takes a permit ahead of it, each key holds
        /// one run at a time, and every run asked for after it takes a newer
        /// ticket. A rerun that kept its permit by finishing is exactly what
        /// breaks that, and breaks it without bound.
        fn took(
            &mut self,
            dispatched: Option<MaintenanceDispatch>,
            eligible_before: &BTreeSet<MaintenanceKey>,
        ) {
            let dispatched = dispatched.map(|dispatch| dispatch.key);
            assert!(
                dispatched.is_none() || !self.closed,
                "action {}: closed admission must never dispatch",
                self.action
            );
            if let Some(dispatched) = &dispatched {
                self.dispatches += 1;
                for key in eligible_before.iter().filter(|key| *key != dispatched) {
                    *self.passed_over.entry(key.clone()).or_default() += 1;
                }
                self.running.insert(dispatched.clone());
            }
            let eligible = self.eligible();
            self.passed_over.retain(|key, _| eligible.contains(key));
            self.deepest_wait = self
                .deepest_wait
                .max(self.passed_over.values().copied().max().unwrap_or(0));
            for (key, passed) in &self.passed_over {
                assert!(
                    *passed < self.keys.len(),
                    "action {}: {key:?} sat eligible while {passed} claims went past it",
                    self.action
                );
            }
        }

        /// Everything that has to hold whatever the driver just did.
        fn check(&self) {
            let running: BTreeSet<MaintenanceKey> = self
                .admission
                .keys
                .iter()
                .filter(|(_, state)| state.is_running())
                .map(|(key, _)| key.clone())
                .collect();
            assert_eq!(
                running, self.running,
                "action {}: the book and its caller disagree about what is running",
                self.action
            );
            assert_eq!(
                self.admission.running(),
                running.len(),
                "action {}: one permit per running step, and no permit lost",
                self.action
            );
            assert!(
                running.len() <= self.cap,
                "action {}: the permit cap is a cap",
                self.action
            );
            for key in &self.keys {
                assert_eq!(
                    self.admission.not_before_ms(key),
                    self.owed
                        .get(key)
                        .map(|(earliest_at_ms, _)| *earliest_at_ms),
                    "action {}: {key:?}'s obligation is only its own to clear",
                    self.action
                );
            }
        }

        /// The keys whose queued run could be claimed right now.
        fn eligible(&self) -> BTreeSet<MaintenanceKey> {
            self.admission
                .keys
                .iter()
                .filter(|(_, state)| {
                    state
                        .queued_run()
                        .is_some_and(|run| run.is_eligible(self.now_ms))
                })
                .map(|(key, _)| key.clone())
                .collect()
        }

        fn pick_running(&mut self) -> Option<MaintenanceKey> {
            let running: Vec<MaintenanceKey> = self.running.iter().cloned().collect();
            running.get(self.draws.below(running.len())).cloned()
        }
    }

    /// The properties the book holds whatever order its callers arrive in:
    /// one step per key, permits accounted for, ticket order, nothing
    /// dispatched after a close, and an obligation only its own deadline or
    /// a close may clear.
    ///
    /// The book is synchronous under one lock, so a seeded sequence of calls
    /// is the whole concurrency story — there is no interleaving these
    /// sequences cannot produce.
    #[test]
    fn a_seeded_action_sequence_holds_every_admission_invariant() {
        for (index, seed) in MODEL_SEEDS.into_iter().enumerate() {
            Model::new(seed, 1 + index % 2, 3 + index % 3).run();
        }
    }
}
