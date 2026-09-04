//! Runner behavior over a live runtime: the parts admission alone cannot
//! show — permits held across real steps, the timer, reconciliation, the
//! drain, and the deadlines the upload paths plant.

#![allow(clippy::panic)]
// The drain test injects a step panic to assert it is surfaced.

use super::runner::{MaintenanceClock, SystemMaintenanceClock};
use super::*;
use crate::{ChangeSeq, NamespaceId, Result, RuntimeError};
use loonfs_test_support::ids::{namespace_id, nonzero_usize};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::{watch, Semaphore};

const TEST_JOB: MaintenanceJobId = MaintenanceJobId::new("test");
const WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// A clock a test moves by hand.
#[derive(Debug)]
struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    fn at(now_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            now_ms: AtomicU64::new(now_ms),
        })
    }

    fn advance_to(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }
}

impl MaintenanceClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }

    fn jitter_below_ms(&self, span_ms: u64) -> u64 {
        // The whole window, deterministically: these tests assert on when a
        // key comes back, and admission's own tests cover the draw.
        span_ms.saturating_sub(1)
    }
}

/// What one scripted step does.
#[derive(Debug, Clone)]
enum ScriptedStep {
    Conclude(MaintenanceConclusion),
    /// Conclude, and tell the runner where this step stopped.
    Continue(MaintenanceConclusion, Option<String>),
    /// Conclude, and tell the runner when what this step left behind
    /// becomes eligible — what a collection pass reports for what it
    /// retained.
    Due(MaintenanceConclusion, u64),
    Fail,
    Panic,
}

/// Holds steps inside the executor so a test can observe the runner at its
/// concurrency cap instead of racing it.
struct Gate {
    entered: watch::Sender<usize>,
    permits: Arc<Semaphore>,
}

impl Gate {
    fn closed() -> Self {
        Self {
            entered: watch::channel(0).0,
            permits: Arc::new(Semaphore::new(0)),
        }
    }

    async fn enter(&self) {
        self.entered.send_modify(|entered| *entered += 1);
        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("the gate semaphore stays open")
            .forget();
    }

    async fn wait_entered(&self, count: usize) {
        let mut entered = self.entered.subscribe();
        while *entered.borrow_and_update() < count {
            entered
                .changed()
                .await
                .expect("the gate sender outlives its waiters");
        }
    }

    fn release(&self, steps: usize) {
        self.permits.add_permits(steps);
    }
}

/// A scripted executor that records what it was asked to do.
struct TestJob {
    answers: StdMutex<VecDeque<ScriptedStep>>,
    /// What every step answers once the script runs out.
    trailing_answer: ScriptedStep,
    probe_answer: StdMutex<MaintenanceProbe>,
    steps: StdMutex<Vec<NamespaceId>>,
    /// The continuation each step was handed, in order.
    resumed_from: StdMutex<Vec<Option<String>>>,
    probes: StdMutex<Vec<NamespaceId>>,
    gate: Option<Gate>,
}

impl TestJob {
    fn answering(trailing_answer: ScriptedStep) -> Arc<Self> {
        Arc::new(Self {
            answers: StdMutex::new(VecDeque::new()),
            trailing_answer,
            probe_answer: StdMutex::new(MaintenanceProbe::Idle),
            steps: StdMutex::new(Vec::new()),
            resumed_from: StdMutex::new(Vec::new()),
            probes: StdMutex::new(Vec::new()),
            gate: None,
        })
    }

    fn idle() -> Arc<Self> {
        Self::answering(ScriptedStep::Conclude(MaintenanceConclusion::Idle))
    }

    fn scripted(
        answers: impl IntoIterator<Item = ScriptedStep>,
        trailing: ScriptedStep,
    ) -> Arc<Self> {
        let job = Self::answering(trailing);
        *job.answers.lock().expect("answers") = answers.into_iter().collect();
        job
    }

    fn gated(answers: impl IntoIterator<Item = ScriptedStep>, trailing: ScriptedStep) -> Arc<Self> {
        let mut job = Self {
            answers: StdMutex::new(answers.into_iter().collect()),
            trailing_answer: trailing,
            probe_answer: StdMutex::new(MaintenanceProbe::Idle),
            steps: StdMutex::new(Vec::new()),
            resumed_from: StdMutex::new(Vec::new()),
            probes: StdMutex::new(Vec::new()),
            gate: None,
        };
        job.gate = Some(Gate::closed());
        Arc::new(job)
    }

    fn gate(&self) -> &Gate {
        self.gate.as_ref().expect("this job was built with a gate")
    }

    fn set_probe(&self, answer: MaintenanceProbe) {
        *self.probe_answer.lock().expect("probe answer") = answer;
    }

    fn stepped(&self) -> Vec<String> {
        names(&self.steps)
    }

    /// What the runner handed each step, in order.
    fn resumed_from(&self) -> Vec<Option<String>> {
        self.resumed_from.lock().expect("resumed from").clone()
    }

    fn probed(&self) -> Vec<String> {
        names(&self.probes)
    }
}

fn names(log: &StdMutex<Vec<NamespaceId>>) -> Vec<String> {
    log.lock()
        .expect("job log")
        .iter()
        .map(|namespace_id| namespace_id.as_str().to_owned())
        .collect()
}

#[async_trait::async_trait]
impl MaintenanceJob for TestJob {
    fn id(&self) -> MaintenanceJobId {
        TEST_JOB
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        if let Some(gate) = &self.gate {
            gate.enter().await;
        }
        self.steps.lock().expect("steps").push(namespace_id.clone());
        self.resumed_from
            .lock()
            .expect("resumed from")
            .push(continuation.map(str::to_owned));
        let answer = self
            .answers
            .lock()
            .expect("answers")
            .pop_front()
            .unwrap_or_else(|| self.trailing_answer.clone());
        match answer {
            ScriptedStep::Conclude(conclusion) => Ok(MaintenanceRunReport::concluded(conclusion)),
            ScriptedStep::Continue(conclusion, continuation) => Ok(MaintenanceRunReport {
                conclusion,
                continuation,
                not_before_ms: None,
                follow_up: None,
            }),
            ScriptedStep::Due(conclusion, not_before_ms) => Ok(MaintenanceRunReport {
                conclusion,
                continuation: None,
                not_before_ms: Some(not_before_ms),
                follow_up: None,
            }),
            ScriptedStep::Fail => Err(RuntimeError::Config("scripted step failure".to_owned())),
            ScriptedStep::Panic => panic!("injected maintenance step panic"),
        }
    }

    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        self.probes
            .lock()
            .expect("probes")
            .push(namespace_id.clone());
        Ok(*self.probe_answer.lock().expect("probe answer"))
    }
}

const SUBSCRIBING_JOB: MaintenanceJobId = MaintenanceJobId::new("subscribing");

struct SubscribingJob {
    steps: StdMutex<Vec<NamespaceId>>,
    probe_answer: StdMutex<MaintenanceProbe>,
}

impl Default for SubscribingJob {
    fn default() -> Self {
        Self {
            steps: StdMutex::new(Vec::new()),
            probe_answer: StdMutex::new(MaintenanceProbe::Idle),
        }
    }
}

impl SubscribingJob {
    fn stepped(&self) -> Vec<String> {
        names(&self.steps)
    }

    fn set_probe(&self, answer: MaintenanceProbe) {
        *self.probe_answer.lock().expect("probe answer") = answer;
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for SubscribingJob {
    fn id(&self) -> MaintenanceJobId {
        SUBSCRIBING_JOB
    }

    fn should_run_after_publication(&self, publication: &NamespacePublication) -> bool {
        publication.committed_through_seq.is_some()
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        self.steps.lock().expect("steps").push(namespace_id.clone());
        Ok(MaintenanceRunReport::concluded(MaintenanceConclusion::Idle))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(*self.probe_answer.lock().expect("probe answer"))
    }
}

fn runner_with(
    clock: Arc<dyn MaintenanceClock>,
    job: Arc<dyn MaintenanceJob>,
) -> MaintenanceRunner {
    let registry = MaintenanceRegistry::new();
    registry.register(job).expect("register the test job");
    let runner = MaintenanceRunner::builder(registry)
        .max_concurrent(nonzero_usize(1).get())
        .clock(clock)
        .build()
        .expect("build the runner");
    assert!(runner.is_registered(TEST_JOB));
    runner
}

fn enabled_runner(job: Arc<dyn MaintenanceJob>) -> MaintenanceRunner {
    runner_with(Arc::new(SystemMaintenanceClock::default()), job)
}

async fn wait_for(condition: impl Fn() -> bool, what: &str) {
    tokio::time::timeout(WAIT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn a_nudge_runs_one_step_and_parks_on_idle() {
    let job = TestJob::idle();
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("demo");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the step settles");

    assert_eq!(job.stepped(), vec!["demo".to_owned()]);
    assert!(
        !runner.is_pending(TEST_JOB, &namespace_id),
        "an idle conclusion parks the key"
    );
}

#[tokio::test]
async fn progressed_requeues_immediately_and_stays_fair_at_the_cap() {
    // The first namespace has two units of work; the second has one. With a
    // single permit, the second must not wait behind the whole backlog.
    let job = TestJob::gated(
        [
            ScriptedStep::Conclude(MaintenanceConclusion::Progressed),
            ScriptedStep::Conclude(MaintenanceConclusion::Idle),
            ScriptedStep::Conclude(MaintenanceConclusion::Progressed),
        ],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let (busy, peer) = (namespace_id("busy"), namespace_id("peer"));

    runner.handle().nudge(TEST_JOB, &busy);
    // The permit is claimed and the step is inside the gate before the peer
    // asks, so the queue order under test is not a race.
    job.gate().wait_entered(1).await;
    runner.handle().nudge(TEST_JOB, &peer);
    job.gate().release(8);
    runner.drain().await.expect("both namespaces settle");

    assert_eq!(
        job.stepped(),
        vec![
            "busy".to_owned(),
            "peer".to_owned(),
            "busy".to_owned(),
            "busy".to_owned()
        ],
        "one unit per step: the waiting peer runs between the busy namespace's units"
    );
}

#[tokio::test]
async fn a_progressing_job_resumes_from_where_its_last_step_stopped() {
    let job = TestJob::scripted(
        [
            ScriptedStep::Continue(MaintenanceConclusion::Progressed, Some("page-1".to_owned())),
            ScriptedStep::Continue(MaintenanceConclusion::Progressed, Some("page-2".to_owned())),
        ],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("paged");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the pass settles");

    assert_eq!(
        job.resumed_from(),
        vec![None, Some("page-1".to_owned()), Some("page-2".to_owned())],
        "a fresh pass, then each step resuming from the one before it"
    );

    // The idle step that ended the pass spent the position, so the next
    // nudge starts over rather than resuming a finished walk.
    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the fresh pass settles");
    assert_eq!(
        job.resumed_from().last().expect("a fourth step ran"),
        &None,
        "an idle conclusion clears the continuation"
    );
}

#[tokio::test]
async fn a_blocked_job_resumes_from_where_it_parked() {
    let job = TestJob::scripted(
        [ScriptedStep::Continue(
            MaintenanceConclusion::Blocked,
            Some("page-7".to_owned()),
        )],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("parked");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the blocked step settles");
    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the retry settles");

    assert_eq!(
        job.resumed_from(),
        vec![None, Some("page-7".to_owned())],
        "a retry with room to work resumes where the blocked step stopped"
    );
}

#[tokio::test]
async fn a_not_enabled_conclusion_drops_the_continuation() {
    let job = TestJob::scripted(
        [
            ScriptedStep::Continue(MaintenanceConclusion::Progressed, Some("page-1".to_owned())),
            ScriptedStep::Conclude(MaintenanceConclusion::NotEnabled),
        ],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("gone");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("both steps settle");
    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the re-admitted step settles");

    assert_eq!(
        job.resumed_from(),
        vec![None, Some("page-1".to_owned()), None],
        "a key re-admitted after eviction starts a fresh pass"
    );
}

#[tokio::test]
async fn blocked_parks_and_a_later_nudge_retries() {
    let job = TestJob::scripted(
        [ScriptedStep::Conclude(MaintenanceConclusion::Blocked)],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("blocked");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the blocked step settles");
    assert_eq!(
        job.stepped().len(),
        1,
        "zero-progress work must not requeue itself"
    );

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the retry settles");
    assert_eq!(job.stepped().len(), 2, "a later nudge retries it");
}

#[tokio::test]
async fn superseded_takes_the_race_again_and_not_enabled_evicts() {
    let job = TestJob::scripted(
        [ScriptedStep::Conclude(MaintenanceConclusion::Superseded)],
        ScriptedStep::Conclude(MaintenanceConclusion::NotEnabled),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("raced");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("both steps settle");

    assert_eq!(
        job.stepped().len(),
        2,
        "the superseded step runs again, and the second concludes not-enabled"
    );
    runner.reconcile_now().await;
    assert!(
        job.probed().is_empty(),
        "an evicted key leaves the reconciliation scope"
    );
}

#[tokio::test]
async fn a_failed_step_backs_off_and_the_timer_retries_it() {
    let job = TestJob::scripted(
        [ScriptedStep::Fail],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("flaky");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    wait_for(
        || job.stepped().len() >= 2,
        "the backed-off retry to run on the timer's own wake",
    )
    .await;
    runner.drain().await.expect("the retry settles");
}

#[tokio::test]
async fn a_key_planted_in_the_future_does_not_fire_early() {
    let clock = ManualClock::at(1_000_000);
    let job = TestJob::idle();
    let runner = runner_with(clock.clone(), job.clone());
    let namespace_id = namespace_id("leased");

    runner
        .handle()
        .nudge_not_before(TEST_JOB, &namespace_id, 1_060_000);
    runner.drain().await.expect("nothing is running");
    assert!(
        job.stepped().is_empty(),
        "the runner must not fire a key before its time"
    );
    assert_eq!(
        runner.not_before_ms(TEST_JOB, &namespace_id),
        Some(1_060_000)
    );

    // A sooner obligation wins the merge; a later one does not move it back.
    runner
        .handle()
        .nudge_not_before(TEST_JOB, &namespace_id, 1_090_000);
    runner
        .handle()
        .nudge_not_before(TEST_JOB, &namespace_id, 1_030_000);
    assert_eq!(
        runner.not_before_ms(TEST_JOB, &namespace_id),
        Some(1_030_000),
        "the soonest time asked for wins"
    );

    clock.advance_to(1_030_000);
    runner.dispatch_now();
    runner.drain().await.expect("the due key settles");
    assert_eq!(
        job.stepped(),
        vec!["leased".to_owned()],
        "the key fires once its time arrives"
    );
}

#[tokio::test]
async fn a_step_that_reports_a_deadline_re_arms_its_own_key() {
    let clock = ManualClock::at(1_000_000);
    let job = TestJob::scripted(
        [ScriptedStep::Due(MaintenanceConclusion::Idle, 1_060_000)],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = runner_with(clock.clone(), job.clone());
    let namespace_id = namespace_id("retained");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the first pass settles");
    assert_eq!(job.stepped().len(), 1);
    assert_eq!(
        runner.not_before_ms(TEST_JOB, &namespace_id),
        Some(1_060_000),
        "an idle conclusion still parks on the deadline the step reported"
    );

    clock.advance_to(1_060_000);
    runner.dispatch_now();
    runner.drain().await.expect("the re-armed pass settles");
    assert_eq!(
        job.stepped().len(),
        2,
        "the deadline the step reported is what brought the key back, with nothing else asking"
    );
    assert_eq!(
        runner.not_before_ms(TEST_JOB, &namespace_id),
        None,
        "the second pass reported nothing left to wait for"
    );
}

#[tokio::test]
async fn a_publication_nudges_only_the_jobs_it_concerns() {
    let quiet = TestJob::idle();
    let subscriber = Arc::new(SubscribingJob::default());
    let runner = enabled_runner(quiet.clone());
    runner
        .registry()
        .register(subscriber.clone())
        .expect("register the subscribing job");
    let namespace_id = namespace_id("published");

    runner
        .handle()
        .hint(MaintenanceHint::Published(NamespacePublication {
            namespace_id: namespace_id.clone(),
            committed_through_seq: None,
            folded: false,
            wal_tail_segments: 4,
        }));
    runner.drain().await.expect("nothing was scheduled");
    assert!(
        subscriber.stepped().is_empty(),
        "a publication that committed nothing is not this job's trigger"
    );

    runner
        .handle()
        .hint(MaintenanceHint::Published(NamespacePublication {
            namespace_id: namespace_id.clone(),
            committed_through_seq: Some(ChangeSeq(7)),
            folded: false,
            wal_tail_segments: 5,
        }));
    runner.drain().await.expect("the subscriber's step settles");

    assert_eq!(subscriber.stepped(), vec!["published".to_owned()]);
    assert!(
        quiet.stepped().is_empty(),
        "a job that declares no publication trigger hears nothing"
    );
}

#[tokio::test]
async fn the_timer_admits_a_key_planted_a_moment_out_with_no_further_nudge() {
    let job = TestJob::idle();
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("soon");

    let handle = runner.handle();
    handle.nudge_not_before(TEST_JOB, &namespace_id, handle.now_ms() + 40);
    wait_for(
        || !job.stepped().is_empty(),
        "the runner's own timer to admit the key when its time arrives",
    )
    .await;
    runner.drain().await.expect("the step settles");
}

#[tokio::test]
async fn reconciliation_revisits_a_skipped_namespace_without_a_new_nudge() {
    // The cold-namespace path: a key was admitted, its step left the
    // namespace over its threshold, and nothing writes to it again. Only
    // the sweep brings it back.
    let job = TestJob::idle();
    job.set_probe(MaintenanceProbe::Due);
    let runner = enabled_runner(job.clone());
    let namespace_id = namespace_id("cold");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner.drain().await.expect("the first step settles");
    assert_eq!(job.stepped().len(), 1);

    runner.reconcile_now().await;
    runner.drain().await.expect("the re-admitted step settles");
    assert_eq!(
        job.probed(),
        vec!["cold".to_owned()],
        "the sweep asked the one admitted key"
    );
    assert_eq!(
        job.stepped().len(),
        2,
        "an over-threshold namespace is revisited with no new write"
    );
}

#[tokio::test]
async fn reconciliation_forgets_an_idle_key_and_never_visits_an_unadmitted_one() {
    let job = TestJob::idle();
    let runner = enabled_runner(job.clone());
    let admitted = namespace_id("admitted");

    runner.handle().nudge(TEST_JOB, &admitted);
    runner.drain().await.expect("the step settles");

    runner.reconcile_now().await;
    assert_eq!(job.probed(), vec!["admitted".to_owned()]);
    assert_eq!(job.stepped().len(), 1, "an idle probe schedules nothing");

    // The probe forgot the one admitted key, and nothing ever nudged any
    // other: the scope is what this process admitted, never what the store
    // holds.
    job.probes.lock().expect("probes").clear();
    runner.reconcile_now().await;
    assert!(
        job.probed().is_empty(),
        "a forgotten key is out of scope, and an untouched one was never in it"
    );
}

#[tokio::test]
async fn a_lease_obligation_survives_a_quiet_probe() {
    let clock = ManualClock::at(1_000_000);
    let job = TestJob::idle();
    let runner = runner_with(clock, job);
    let namespace_id = namespace_id("leased");

    runner
        .handle()
        .nudge_not_before(TEST_JOB, &namespace_id, 1_060_000);
    runner.reconcile_now().await;
    assert_eq!(
        runner.not_before_ms(TEST_JOB, &namespace_id),
        Some(1_060_000),
        "a probe may forget a key that owes nothing, never one that owes a deadline"
    );
}

#[tokio::test]
async fn shutdown_clears_pending_work_and_refuses_later_nudges() {
    let job = TestJob::gated([], ScriptedStep::Conclude(MaintenanceConclusion::Idle));
    let runner = enabled_runner(job.clone());
    let (active, queued) = (namespace_id("active"), namespace_id("queued"));

    runner.handle().nudge(TEST_JOB, &active);
    job.gate().wait_entered(1).await;
    assert_eq!(runner.running_steps(), 1);
    runner.handle().nudge(TEST_JOB, &queued);
    assert!(runner.is_pending(TEST_JOB, &queued));

    runner.close_admission();
    assert!(
        !runner.is_pending(TEST_JOB, &queued),
        "shutdown drops work still waiting for a permit"
    );
    job.gate().release(8);
    runner.drain().await.expect("the active step settles");

    runner.handle().nudge(TEST_JOB, &active);
    runner
        .drain()
        .await
        .expect("nothing may spawn after shutdown");
    assert_eq!(
        job.stepped(),
        vec!["active".to_owned()],
        "the cleared queue must not revive, and post-shutdown nudges are no-ops"
    );
}

#[tokio::test]
async fn drain_surfaces_a_panicked_step() {
    let job = TestJob::scripted(
        [ScriptedStep::Panic],
        ScriptedStep::Conclude(MaintenanceConclusion::Idle),
    );
    let runner = enabled_runner(job.clone());
    let panicked_namespace_id = namespace_id("panics");

    runner.handle().nudge(TEST_JOB, &panicked_namespace_id);
    let error = runner.drain().await.expect_err("a panicked step surfaces");
    assert!(
        error.to_string().contains("panicked"),
        "drain reports panicked tasks: {error}"
    );
    assert!(!runner.is_pending(TEST_JOB, &panicked_namespace_id));
}

#[tokio::test]
async fn a_nudge_for_an_unregistered_job_runs_nothing() {
    let runner = enabled_runner(TestJob::idle());
    let unknown = MaintenanceJobId::new("nobody-registered-this");
    let namespace_id = namespace_id("demo");

    runner.handle().nudge(unknown, &namespace_id);
    runner.drain().await.expect("nothing to run");
    assert!(!runner.is_pending(unknown, &namespace_id));
}

struct BlockingJob {
    id: MaintenanceJobId,
    publication: bool,
    follow_up: Option<MaintenanceJobId>,
    runs: AtomicUsize,
    entered: watch::Sender<usize>,
    permits: Arc<Semaphore>,
}

impl BlockingJob {
    fn new(
        id: MaintenanceJobId,
        publication: bool,
        follow_up: Option<MaintenanceJobId>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            publication,
            follow_up,
            runs: AtomicUsize::new(0),
            entered: watch::channel(0).0,
            permits: Arc::new(Semaphore::new(0)),
        })
    }

    async fn wait_entered(&self, count: usize) {
        let mut entered = self.entered.subscribe();
        while *entered.borrow_and_update() < count {
            entered.changed().await.expect("job remains present");
        }
    }

    fn release(&self) {
        self.permits.add_permits(1);
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for BlockingJob {
    fn id(&self) -> MaintenanceJobId {
        self.id
    }

    async fn run(
        &self,
        _namespace_id: &NamespaceId,
        _continuation: Option<&str>,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.entered.send_modify(|entered| *entered += 1);
        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("job remains open")
            .forget();
        Ok(MaintenanceRunReport {
            conclusion: MaintenanceConclusion::Blocked,
            continuation: None,
            not_before_ms: None,
            follow_up: self.follow_up,
        })
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }

    fn should_run_after_publication(&self, _publication: &NamespacePublication) -> bool {
        self.publication
    }
}

#[tokio::test]
async fn published_hints_coalesce_and_follow_ups_admit_once() {
    let metadata = BlockingJob::new(
        MaintenanceJobId::METADATA,
        true,
        Some(MaintenanceJobId::METADATA_COMPACTION),
    );
    let compaction = BlockingJob::new(MaintenanceJobId::METADATA_COMPACTION, false, None);
    let registry = MaintenanceRegistry::new();
    registry.register(metadata.clone()).expect("metadata job");
    registry
        .register(compaction.clone())
        .expect("compaction job");
    let runner = MaintenanceRunner::builder(registry)
        .max_concurrent(2)
        .build()
        .expect("runner");
    let namespace_id = namespace_id("hints");
    let publication = NamespacePublication {
        namespace_id: namespace_id.clone(),
        committed_through_seq: Some(ChangeSeq(1)),
        folded: true,
        wal_tail_segments: 0,
    };

    runner
        .handle()
        .hint(MaintenanceHint::Published(publication.clone()));
    runner
        .handle()
        .hint(MaintenanceHint::Published(publication));
    metadata.wait_entered(1).await;
    assert_eq!(metadata.runs.load(Ordering::SeqCst), 1);
    metadata.release();

    compaction.wait_entered(1).await;
    runner
        .handle()
        .nudge(MaintenanceJobId::METADATA, &namespace_id);
    metadata.wait_entered(2).await;
    metadata.release();
    wait_for(
        || metadata.runs.load(Ordering::SeqCst) == 2,
        "the second metadata report",
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(
        compaction.runs.load(Ordering::SeqCst),
        1,
        "an identical follow-up coalesces while compaction is admitted"
    );

    compaction.release();
    runner.shutdown().await.expect("runner shutdown");
}

#[tokio::test]
async fn reconciliation_recovers_a_hint_dropped_before_attachment() {
    let subscriber = Arc::new(SubscribingJob::default());
    let registry = MaintenanceRegistry::new();
    registry
        .register(subscriber.clone())
        .expect("subscriber job");
    let runner = MaintenanceRunner::builder(registry)
        .build()
        .expect("runner");
    let (observer, receiver) = MaintenanceHintRelay::new(NonZeroUsize::new(1).expect("nonzero"));
    let namespace_id = namespace_id("dropped");
    let hint = MaintenanceHint::Published(NamespacePublication {
        namespace_id: namespace_id.clone(),
        committed_through_seq: Some(ChangeSeq(1)),
        folded: false,
        wal_tail_segments: 0,
    });
    let dropped_before = MaintenanceHintRelay::dropped();

    observer(hint.clone());
    observer(hint);
    assert_eq!(MaintenanceHintRelay::dropped(), dropped_before + 1);

    runner.attach_hints(receiver);
    wait_for(
        || subscriber.stepped().len() == 1,
        "the retained hint to run",
    )
    .await;
    subscriber.set_probe(MaintenanceProbe::Due);
    runner.reconcile_now().await;
    runner.drain().await.expect("reconciled run");
    assert_eq!(subscriber.stepped().len(), 2);

    runner.shutdown().await.expect("runner shutdown");
}
