//! Admission state: which `{job, namespace}` keys may run, which are
//! running, where each one's job stopped, and when a parked key becomes
//! eligible again.
//!
//! Everything here is synchronous and guarded by the runner's one lock, so
//! every scheduling decision — singleflight, coalescing, fairness, backoff,
//! not-before times, continuations, shutdown — can be reasoned about and
//! tested without a runtime. The async half (permits held across a step,
//! spawning, timers) lives in the parent module.

use super::{MaintenanceClock, MaintenanceJobId, MaintenanceStepConclusion, MaintenanceStepResult};
use crate::NamespaceId;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// Smallest error backoff window, and the one a first failure draws from.
///
/// Small on purpose: one flaky call should come back quickly.
const ERROR_BACKOFF_BASE_MS: u64 = 10;
/// Ceiling the per-key error backoff window doubles up to.
///
/// A minute, because this backoff is fleet-wide: it governs every key this
/// process has admitted. A cap sized for one namespace's indexer, where
/// retrying every second costs nothing, turns a provider outage into every
/// namespace retrying every second for as long as the outage lasts — a
/// second outage stacked on the first. A minute is small enough that
/// recovery is picked up within one reconciliation interval and large
/// enough that a long outage costs a trickle.
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

/// How one step ended, from admission's point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StepOutcome {
    /// The executor answered. The result decides what happens to the key:
    /// its conclusion when to run again, its continuation where to resume,
    /// its not-before time when a deadline it saw comes due.
    Concluded(MaintenanceStepResult),
    /// The executor failed. The key is retried after its backoff, from
    /// wherever its last step left it.
    Failed,
}

/// What one key is waiting for.
#[derive(Debug, Default, Clone)]
struct KeyState {
    /// Arrival ticket while the key waits for a permit; `None` means parked.
    /// Ticket order is the fairness rule: among eligible keys the one that
    /// has waited longest takes the next free slot.
    ready_ticket: Option<u64>,
    /// Wall-clock millisecond the next planted obligation comes due. Not a
    /// gate on a key some other trigger already made ready — a scheduled
    /// future readiness of its own, so an unrelated run never cancels a
    /// lease obligation.
    not_before_ms: Option<u64>,
    /// The latest deadline anything has planted on this key.
    ///
    /// Two times rather than one because the soonest is when to look and
    /// the latest is when the last thing asked for is definitely due, and a
    /// run consumes only the time it fired on. Every deadline in between is
    /// covered by the last: a pass that runs at the latest deadline
    /// reclaims everything whose own deadline has already passed, so
    /// re-arming on the latest loses nothing while keeping the state per
    /// key at two numbers instead of one per obligation.
    obligation_until_ms: Option<u64>,
    /// Error backoff deadline. Distinct from `not_before_ms` because it
    /// gates a retry of work already asked for rather than an obligation.
    retry_at_ms: Option<u64>,
    /// Consecutive failed steps since the last conclusion, for the backoff.
    consecutive_failures: u32,
    /// Where this key's job said its last step stopped, opaque here and
    /// handed straight back to the next step.
    ///
    /// It lives with the rest of the key's scheduling state so no job has
    /// to keep a map of its own beside this one. Losing it costs a
    /// restarted pass and nothing else: a step re-derives what it may do
    /// from durable state whatever position it starts from.
    continuation: Option<String>,
    /// A step for this key is running right now (the singleflight).
    inflight: bool,
}

impl KeyState {
    #[cfg(test)]
    fn is_pending(&self) -> bool {
        self.ready_ticket.is_some() || self.owes_an_obligation()
    }

    fn clear_schedule(&mut self) {
        self.ready_ticket = None;
        self.not_before_ms = None;
        self.obligation_until_ms = None;
        self.retry_at_ms = None;
    }

    fn owes_an_obligation(&self) -> bool {
        self.not_before_ms.is_some() || self.obligation_until_ms.is_some()
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
    /// Here for one thing: the draw the error backoff jitters with. Every
    /// `now_ms` a decision needs is still passed in, so the tests below can
    /// move time by hand and name the delay they expect.
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

    /// The not-before obligation the key still carries, if any.
    #[cfg(test)]
    pub(crate) fn not_before_ms(&self, key: &MaintenanceKey) -> Option<u64> {
        self.keys.get(key).and_then(|state| state.not_before_ms)
    }

    /// Where the key's job stopped last time, for the step about to run.
    pub(crate) fn continuation(&self, key: &MaintenanceKey) -> Option<String> {
        self.keys
            .get(key)
            .and_then(|state| state.continuation.clone())
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
        let state = self.keys.entry(key).or_default();
        if state.ready_ticket.is_none() {
            state.ready_ticket = Some(ticket);
        }
    }

    /// Plants a timed obligation on the key: it becomes eligible on its own
    /// once `at_ms` passes, even with nothing else asking for it.
    ///
    /// Merging keeps the soonest of the times asked for as the next wake,
    /// and the latest as the point past which nothing is still owed. A time
    /// already past is just a nudge.
    pub(crate) fn nudge_not_before(&mut self, key: MaintenanceKey, at_ms: u64, now_ms: u64) {
        if self.closed {
            return;
        }
        if at_ms <= now_ms {
            self.nudge(key);
            return;
        }
        let state = self.keys.entry(key).or_default();
        state.not_before_ms = Some(match state.not_before_ms {
            Some(existing) => existing.min(at_ms),
            None => at_ms,
        });
        state.obligation_until_ms = Some(match state.obligation_until_ms {
            Some(existing) => existing.max(at_ms),
            None => at_ms,
        });
    }

    /// Turns arrived obligations into ready keys, and re-arms a key whose
    /// latest planted deadline is still ahead of the one that just fired.
    pub(crate) fn promote_due(&mut self, now_ms: u64) {
        if self.closed {
            return;
        }
        let mut ticket = self.next_ticket;
        for state in self.keys.values_mut() {
            if state.not_before_ms.is_none_or(|at_ms| at_ms > now_ms) {
                continue;
            }
            state.not_before_ms = state
                .obligation_until_ms
                .filter(|until_ms| *until_ms > now_ms);
            if state.not_before_ms.is_none() {
                state.obligation_until_ms = None;
            }
            if state.ready_ticket.is_none() {
                state.ready_ticket = Some(ticket);
                ticket = ticket.saturating_add(1);
            }
        }
        self.next_ticket = ticket;
    }

    /// Claims a permit for the fairest eligible key, if there is one and the
    /// pool has room. The caller must run the returned key and report back
    /// through [`Self::finish`] or [`Self::abandon`].
    pub(crate) fn try_dispatch(&mut self, now_ms: u64) -> Option<MaintenanceKey> {
        if self.closed || self.running >= self.max_concurrent {
            return None;
        }
        let key = self.pick_eligible(now_ms)?;
        self.hold(&key);
        self.running = self.running.saturating_add(1);
        Some(key)
    }

    /// Ends one step and returns the key the caller must run next on the
    /// same permit, or `None` after releasing it.
    ///
    /// A request that arrived while the step ran keeps the slot and reruns
    /// the same key immediately — the finishing rerun the previous scheduler
    /// guaranteed. A `Progressed` conclusion does not take that fast path:
    /// it makes the key eligible again but sends it to the back of the
    /// queue, so folding one namespace's backlog stays fair to its peers.
    /// The outcome, the release, the pick, and the new hold share one lock,
    /// so no request is lost between freeing a slot and choosing its heir.
    pub(crate) fn finish(
        &mut self,
        key: &MaintenanceKey,
        outcome: StepOutcome,
        now_ms: u64,
    ) -> Option<MaintenanceKey> {
        let renudged = self
            .keys
            .get(key)
            .is_some_and(|state| state.ready_ticket.is_some());
        self.apply(key, outcome, now_ms);
        if let Some(state) = self.keys.get_mut(key) {
            state.inflight = false;
        }
        if self.closed {
            self.running = self.running.saturating_sub(1);
            return None;
        }
        if renudged && self.is_eligible(key, now_ms) {
            self.hold(key);
            return Some(key.clone());
        }
        match self.pick_eligible(now_ms) {
            Some(next) => {
                self.hold(&next);
                Some(next)
            }
            None => {
                self.running = self.running.saturating_sub(1);
                None
            }
        }
    }

    /// Releases a key and its permit without deciding anything about it —
    /// the path a panicked or dropped step takes.
    pub(crate) fn abandon(&mut self, key: &MaintenanceKey) {
        if let Some(state) = self.keys.get_mut(key) {
            state.inflight = false;
        }
        self.running = self.running.saturating_sub(1);
    }

    /// Forgets a key entirely: it is no longer reconciled and owes nothing.
    pub(crate) fn forget(&mut self, key: &MaintenanceKey) {
        if self.keys.get(key).is_some_and(|state| state.inflight) {
            return;
        }
        self.keys.remove(key);
    }

    /// Forgets a key a probe found idle, unless it still owes a timed
    /// obligation — a lease-dated GC key outlives any number of quiet
    /// probes.
    pub(crate) fn forget_if_idle(&mut self, key: &MaintenanceKey) {
        let forgettable = self.keys.get(key).is_some_and(|state| {
            !state.inflight && state.ready_ticket.is_none() && !state.owes_an_obligation()
        });
        if forgettable {
            self.keys.remove(key);
        }
    }

    /// Rejects further scheduling and drops everything still waiting.
    /// Running steps stay visible to the drain.
    pub(crate) fn shut_down(&mut self) {
        self.closed = true;
        for state in self.keys.values_mut() {
            state.clear_schedule();
        }
    }

    /// The soonest wall-clock millisecond after `now_ms` at which a parked
    /// key becomes eligible on its own, across obligations and backoffs.
    ///
    /// Deadlines already past are deliberately not reported: a key whose
    /// backoff has expired is eligible now, and if it is not running it is
    /// because the permit pool is full. Waking a timer for it would find
    /// the pool still full and wake again immediately; the permit that
    /// frees is what picks it up.
    pub(crate) fn earliest_deadline_ms(&self, now_ms: u64) -> Option<u64> {
        self.keys
            .values()
            .flat_map(|state| [state.not_before_ms, state.retry_at_ms])
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
                .filter(|(_, state)| !state.inflight && state.ready_ticket.is_none())
                .map(|(key, _)| key.clone())
                .take(budget)
                .collect(),
            None => self
                .keys
                .iter()
                .filter(|(_, state)| !state.inflight && state.ready_ticket.is_none())
                .map(|(key, _)| key.clone())
                .take(budget)
                .collect(),
        };
        self.reconcile_cursor = batch.last().cloned();
        batch
    }

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
            state.retry_at_ms = None;
            match result.conclusion {
                // Work happened, or the step lost a race it should simply
                // take again: eligible immediately, behind whatever else is
                // waiting, resuming from wherever this step stopped.
                MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded => {
                    state.continuation = result.continuation;
                    if state.ready_ticket.is_none() {
                        state.ready_ticket = Some(ticket);
                    }
                }
                // Work is left and this step's policy could not move it.
                // Parks like `Idle` — requeueing zero-progress work would
                // only spin — but keeps where the step stopped, so a retry
                // with room to work resumes instead of walking the same
                // ground again.
                MaintenanceStepConclusion::Blocked => state.continuation = result.continuation,
                // Nothing to do. Whatever the last pass was carrying is
                // spent, and the next step starts a fresh one.
                MaintenanceStepConclusion::Idle => state.continuation = None,
                MaintenanceStepConclusion::NotEnabled => {}
            }
        }
        // A deadline the step itself observed joins the ones triggers
        // plant, under the same merge: soonest is when to wake, latest is
        // when the key stops owing anything.
        if let Some(at_ms) = result.not_before_ms {
            self.nudge_not_before(key.clone(), at_ms, now_ms);
        }
    }

    /// Backs a failed key off and leaves its continuation alone: a failure
    /// says nothing about where the last step stopped, and the retry should
    /// pick up from the same place.
    fn record_failure(&mut self, key: &MaintenanceKey, now_ms: u64) {
        let ticket = self.take_ticket();
        let Some(failures) = self
            .keys
            .get(key)
            .map(|state| state.consecutive_failures.saturating_add(1))
        else {
            return;
        };
        let delay_ms = backoff_delay_ms(failures, self.clock.as_ref());
        let Some(state) = self.keys.get_mut(key) else {
            return;
        };
        state.consecutive_failures = failures;
        state.retry_at_ms = Some(now_ms.saturating_add(delay_ms));
        if state.ready_ticket.is_none() {
            state.ready_ticket = Some(ticket);
        }
    }

    fn is_eligible(&self, key: &MaintenanceKey, now_ms: u64) -> bool {
        self.keys.get(key).is_some_and(|state| {
            !state.inflight
                && state.ready_ticket.is_some()
                && state.retry_at_ms.is_none_or(|at_ms| at_ms <= now_ms)
        })
    }

    /// The eligible key that has waited longest.
    ///
    /// Linear in the admitted set, which is bounded by the namespaces this
    /// process has touched and re-walked once per finished step — far
    /// cheaper than the object-store round trips a step itself makes.
    fn pick_eligible(&self, now_ms: u64) -> Option<MaintenanceKey> {
        self.keys
            .iter()
            .filter(|(_, state)| {
                !state.inflight && state.retry_at_ms.is_none_or(|at_ms| at_ms <= now_ms)
            })
            .filter_map(|(key, state)| state.ready_ticket.map(|ticket| (ticket, key)))
            .min_by_key(|(ticket, _)| *ticket)
            .map(|(_, key)| key.clone())
    }

    /// Marks a key as the one this permit is running; the permit count is
    /// the caller's business, because a handoff keeps it.
    fn hold(&mut self, key: &MaintenanceKey) {
        if let Some(state) = self.keys.get_mut(key) {
            state.ready_ticket = None;
            state.inflight = true;
        }
    }
}

/// How long a key waits after `consecutive_failures` failed steps.
///
/// Full jitter over the exponential window: the delay is drawn uniformly
/// from inside [`backoff_window_ms`] rather than being the window itself.
/// Drawing is what keeps a provider outage from becoming a synchronized
/// retry wave — keys that failed in the same millisecond come back spread
/// across the window instead of together at the end of it — and it is the
/// half of the policy that matters most at fleet scale, where the same
/// error arrives for every admitted key at once.
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
    use loonfs_test_support::ids::namespace_id;
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
        StepOutcome::Concluded(MaintenanceStepResult::concluded(conclusion))
    }

    /// A conclusion that also tells the runner where the step stopped.
    fn continuing(conclusion: MaintenanceStepConclusion, continuation: &str) -> StepOutcome {
        StepOutcome::Concluded(MaintenanceStepResult {
            conclusion,
            continuation: Some(continuation.to_owned()),
            not_before_ms: None,
        })
    }

    fn idle() -> StepOutcome {
        concluded(MaintenanceStepConclusion::Idle)
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.try_dispatch(NOW),
            None,
            "one in-flight step per key"
        );
        admission.nudge(key.clone());
        assert_eq!(
            admission.finish(&key, idle(), NOW),
            Some(key.clone()),
            "the deferred request keeps the slot held and reruns the step"
        );
        assert_eq!(
            admission.finish(&key, idle(), NOW),
            None,
            "a quiet finish releases the slot"
        );
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        admission.nudge(key.clone());
        admission.shut_down();
        assert_eq!(
            admission.finish(&key, idle(), NOW),
            None,
            "a deferred request must not rerun after shutdown closed admission"
        );
    }

    /// Ported: claims are singleflight and refused after shutdown.
    #[test]
    fn claims_are_singleflight_and_refused_after_shut_down() {
        let mut admission = book(8);
        let key = metadata("demo");

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(admission.try_dispatch(NOW), None);
        admission.abandon(&key);
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
            Some(key.clone()),
            "a released slot is claimable again"
        );
        admission.abandon(&key);
        admission.shut_down();
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
            None,
            "no new claims after shutdown"
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
        assert_eq!(admission.try_dispatch(NOW), Some(first.clone()));
        assert_eq!(admission.try_dispatch(NOW), Some(second.clone()));
        assert_eq!(
            admission.try_dispatch(NOW),
            None,
            "a third key must wait for a slot"
        );
        assert_eq!(
            admission.finish(&first, idle(), NOW),
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
        assert_eq!(admission.try_dispatch(NOW), Some(active.clone()));
        for _ in 0..8 {
            admission.nudge(queued.clone());
            assert_eq!(
                admission.try_dispatch(NOW),
                None,
                "the queued key cannot claim the occupied slot"
            );
        }
        assert_eq!(
            admission.finish(&active, idle(), NOW),
            Some(queued.clone()),
            "one queued run consumes all coalesced requests"
        );
        assert_eq!(
            admission.finish(&queued, idle(), NOW),
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
        assert_eq!(admission.try_dispatch(NOW), Some(active.clone()));
        admission.nudge(queued.clone());
        admission.shut_down();
        assert_eq!(admission.finish(&active, idle(), NOW), None);
        assert!(
            !admission.is_pending(&queued),
            "shutdown must clear the queue"
        );
        admission.nudge(queued.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
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
        assert_eq!(admission.try_dispatch(NOW), Some(busy.clone()));
        assert_eq!(
            admission.finish(&busy, concluded(MaintenanceStepConclusion::Progressed), NOW),
            Some(peer.clone()),
            "one unit per step: the peer runs before the busy key folds again"
        );
        assert_eq!(
            admission.finish(&peer, idle(), NOW),
            Some(busy),
            "the progressed key is still eligible and takes the next slot"
        );
    }

    #[test]
    fn progressed_keeps_running_when_nothing_else_waits() {
        let mut admission = book(1);
        let key = metadata("alone");

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(&key, concluded(MaintenanceStepConclusion::Progressed), NOW),
            Some(key.clone()),
            "a sole progressing key folds its backlog without waiting"
        );
        assert_eq!(admission.finish(&key, idle(), NOW), None);
    }

    #[test]
    fn blocked_parks_and_a_later_nudge_retries() {
        let mut admission = book(1);
        let key = metadata("blocked");

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(&key, concluded(MaintenanceStepConclusion::Blocked), NOW),
            None,
            "zero-progress work must not requeue itself"
        );
        assert_eq!(admission.try_dispatch(NOW), None);
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
            Some(key),
            "a later nudge retries the blocked key"
        );
    }

    #[test]
    fn superseded_requeues_immediately() {
        let mut admission = book(1);
        let key = metadata("raced");

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(&key, concluded(MaintenanceStepConclusion::Superseded), NOW),
            Some(key),
            "a superseded step takes the race again"
        );
    }

    #[test]
    fn not_enabled_evicts_the_key_and_its_obligation() {
        let mut admission = book(1);
        let key = gc("gone");

        admission.nudge_not_before(key.clone(), NOW + 5_000, NOW);
        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(&key, concluded(MaintenanceStepConclusion::NotEnabled), NOW),
            None
        );
        assert!(!admission.is_pending(&key), "the key is forgotten");
        admission.promote_due(NOW + 10_000);
        assert_eq!(
            admission.try_dispatch(NOW + 10_000),
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.continuation(&key),
            None,
            "a first step starts a fresh pass"
        );

        assert_eq!(
            admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW
            ),
            Some(key.clone()),
            "a progressing key is eligible again"
        );
        assert_eq!(
            admission.continuation(&key),
            Some("page-1".to_owned()),
            "and the next step resumes where this one stopped"
        );

        assert_eq!(
            admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Blocked, "page-2"),
                NOW
            ),
            None,
            "a blocked key parks"
        );
        assert_eq!(
            admission.continuation(&key),
            Some("page-2".to_owned()),
            "keeping its position, so a retry with a bigger budget resumes"
        );

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.continuation(&key),
            Some("page-2".to_owned()),
            "the parked position is what the resumed step is handed"
        );
        assert_eq!(admission.finish(&key, idle(), NOW), None);
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW
            ),
            Some(key.clone())
        );
        assert_eq!(
            admission.finish(&key, concluded(MaintenanceStepConclusion::NotEnabled), NOW),
            None
        );
        assert_eq!(
            admission.continuation(&key),
            None,
            "the key's whole state went with it"
        );

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(
                &key,
                continuing(MaintenanceStepConclusion::Progressed, "page-1"),
                NOW
            ),
            Some(key.clone())
        );
        assert_eq!(admission.finish(&key, StepOutcome::Failed, NOW), None);
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(
                &key,
                StepOutcome::Concluded(MaintenanceStepResult {
                    conclusion: MaintenanceStepConclusion::Idle,
                    continuation: None,
                    not_before_ms: Some(NOW + 60_000),
                }),
                NOW
            ),
            None,
            "an idle step with a deadline still parks until that deadline"
        );
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 60_000));

        admission.promote_due(NOW + 60_000);
        assert_eq!(
            admission.try_dispatch(NOW + 60_000),
            Some(key.clone()),
            "the observed deadline brings the key back on its own"
        );
        assert_eq!(
            admission.finish(
                &key,
                StepOutcome::Concluded(MaintenanceStepResult {
                    conclusion: MaintenanceStepConclusion::Progressed,
                    continuation: None,
                    not_before_ms: Some(NOW + 30_000),
                }),
                NOW + 60_000
            ),
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

        admission.nudge_not_before(key.clone(), NOW + 60_000, NOW);
        admission.promote_due(NOW);
        assert_eq!(
            admission.try_dispatch(NOW),
            None,
            "the runner must not fire a key before its time"
        );
        admission.promote_due(NOW + 59_999);
        assert_eq!(admission.try_dispatch(NOW + 59_999), None);
        admission.promote_due(NOW + 60_000);
        assert_eq!(
            admission.try_dispatch(NOW + 60_000),
            Some(key),
            "the key fires once its time arrives"
        );
    }

    #[test]
    fn the_soonest_of_two_not_before_times_wins() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge_not_before(key.clone(), NOW + 90_000, NOW);
        admission.nudge_not_before(key.clone(), NOW + 30_000, NOW);
        admission.nudge_not_before(key.clone(), NOW + 60_000, NOW);
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 30_000));
        assert_eq!(admission.earliest_deadline_ms(NOW), Some(NOW + 30_000));
        admission.promote_due(NOW + 30_000);
        assert_eq!(admission.try_dispatch(NOW + 30_000), Some(key));
    }

    #[test]
    fn an_unrelated_run_never_cancels_a_lease_obligation() {
        let mut admission = book(1);
        let key = gc("leased");

        admission.nudge_not_before(key.clone(), NOW + 60_000, NOW);
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
            Some(key.clone()),
            "an explicit nudge runs now; the obligation is a separate future"
        );
        assert_eq!(admission.finish(&key, idle(), NOW), None);
        assert_eq!(
            admission.not_before_ms(&key),
            Some(NOW + 60_000),
            "the obligation survives the run it did not ask for"
        );
        admission.promote_due(NOW + 60_000);
        assert_eq!(admission.try_dispatch(NOW + 60_000), Some(key));
    }

    #[test]
    fn a_past_not_before_time_is_just_a_nudge() {
        let mut admission = book(1);
        let key = gc("overdue");

        admission.nudge_not_before(key.clone(), NOW - 1, NOW);
        assert_eq!(admission.not_before_ms(&key), None);
        assert_eq!(admission.try_dispatch(NOW), Some(key));
    }

    #[test]
    fn a_failed_step_backs_off_before_its_retry() {
        let mut admission = book(1);
        let key = metadata("flaky");

        admission.nudge(key.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(
            admission.finish(&key, StepOutcome::Failed, NOW),
            None,
            "a failed step waits out its backoff before retrying"
        );
        assert_eq!(admission.try_dispatch(NOW), None);
        assert_eq!(
            admission.try_dispatch(NOW + top_delay_ms(1)),
            Some(key.clone())
        );
        assert_eq!(admission.finish(&key, StepOutcome::Failed, NOW), None);
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
                    .and_then(|state| state.retry_at_ms)
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
        assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
        assert_eq!(admission.finish(&key, StepOutcome::Failed, NOW), None);
        assert_eq!(
            admission.try_dispatch(NOW + top_delay_ms(1)),
            Some(key.clone())
        );
        assert_eq!(admission.finish(&key, idle(), NOW), None);
        admission.nudge(key.clone());
        assert_eq!(
            admission.try_dispatch(NOW),
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
            assert_eq!(admission.try_dispatch(NOW), Some(key.clone()));
            assert_eq!(admission.finish(key, idle(), NOW), None);
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
        assert_eq!(admission.try_dispatch(NOW), Some(touched.clone()));
        assert_eq!(admission.finish(&touched, idle(), NOW), None);

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
        assert_eq!(admission.try_dispatch(NOW), Some(parked.clone()));
        assert_eq!(admission.finish(&parked, idle(), NOW), None);
        admission.nudge(running.clone());
        assert_eq!(admission.try_dispatch(NOW), Some(running.clone()));
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
        assert_eq!(admission.try_dispatch(NOW), Some(quiet.clone()));
        assert_eq!(admission.finish(&quiet, idle(), NOW), None);
        admission.nudge_not_before(leased.clone(), NOW + 60_000, NOW);

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
        admission.nudge_not_before(key.clone(), NOW + 86_400_000, NOW);
        admission.nudge_not_before(key.clone(), NOW + 604_800_000, NOW);
        assert_eq!(admission.not_before_ms(&key), Some(NOW + 86_400_000));

        let day = NOW + 86_400_000;
        admission.promote_due(day);
        assert_eq!(
            admission.try_dispatch(day),
            Some(key.clone()),
            "the soonest deadline is what the runner wakes for"
        );
        assert_eq!(admission.finish(&key, idle(), day), None);
        assert_eq!(
            admission.not_before_ms(&key),
            Some(NOW + 604_800_000),
            "the later deadline survives the pass that ran for the earlier one"
        );

        let week = NOW + 604_800_000;
        admission.promote_due(week);
        assert_eq!(admission.try_dispatch(week), Some(key.clone()));
        assert_eq!(admission.finish(&key, idle(), week), None);
        assert_eq!(
            admission.not_before_ms(&key),
            None,
            "past the last deadline the key owes nothing"
        );
        admission.forget_if_idle(&key);
        assert!(admission.reconcile_batch(16).is_empty());
    }

    #[test]
    fn shutdown_clears_pending_obligations() {
        let mut admission = book(1);
        let key = gc("leased");
        admission.nudge_not_before(key.clone(), NOW + 60_000, NOW);
        admission.shut_down();
        assert_eq!(admission.not_before_ms(&key), None);
        assert_eq!(admission.earliest_deadline_ms(NOW), None);
        admission.promote_due(NOW + 60_000);
        assert_eq!(admission.try_dispatch(NOW + 60_000), None);
    }
}
