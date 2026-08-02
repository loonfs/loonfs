//! Runner behavior over a live runtime: the parts admission alone cannot
//! show — permits held across real steps, the timer, reconciliation, the
//! drain, and the deadlines the upload paths plant.

#![allow(clippy::panic)]
// The drain test injects a step panic to assert it is surfaced.

use super::*;
use crate::{
    BeginUploadRequest, CompleteUploadRequest, CreateNamespaceOptions, FsWriter, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, nonzero_usize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
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
enum StepAnswer {
    Conclude(MaintenanceStepConclusion),
    /// Conclude, and tell the runner where this step stopped.
    Continue(MaintenanceStepConclusion, Option<String>),
    /// Conclude, and tell the runner when what this step left behind
    /// becomes eligible — what a collection pass reports for what it
    /// retained.
    Due(MaintenanceStepConclusion, u64),
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
    answers: StdMutex<VecDeque<StepAnswer>>,
    /// What every step answers once the script runs out.
    trailing_answer: StepAnswer,
    probe_answer: StdMutex<MaintenanceProbe>,
    steps: StdMutex<Vec<NamespaceId>>,
    /// The continuation each step was handed, in order.
    resumed_from: StdMutex<Vec<Option<String>>>,
    probes: StdMutex<Vec<NamespaceId>>,
    gate: Option<Gate>,
}

impl TestJob {
    fn answering(trailing_answer: StepAnswer) -> Arc<Self> {
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
        Self::answering(StepAnswer::Conclude(MaintenanceStepConclusion::Idle))
    }

    fn scripted(answers: impl IntoIterator<Item = StepAnswer>, trailing: StepAnswer) -> Arc<Self> {
        let job = Self::answering(trailing);
        *job.answers.lock().expect("answers") = answers.into_iter().collect();
        job
    }

    fn gated(answers: impl IntoIterator<Item = StepAnswer>, trailing: StepAnswer) -> Arc<Self> {
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

    async fn step(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult> {
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
            StepAnswer::Conclude(conclusion) => Ok(MaintenanceStepResult::concluded(conclusion)),
            StepAnswer::Continue(conclusion, continuation) => Ok(MaintenanceStepResult {
                conclusion,
                continuation,
                not_before_ms: None,
            }),
            StepAnswer::Due(conclusion, not_before_ms) => Ok(MaintenanceStepResult {
                conclusion,
                continuation: None,
                not_before_ms: Some(not_before_ms),
            }),
            StepAnswer::Fail => Err(RuntimeError::Config("scripted step failure".to_owned())),
            StepAnswer::Panic => panic!("injected maintenance step panic"),
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

/// A job that says publications concern it, and records which namespaces
/// it was told about.
#[derive(Default)]
struct SubscribingJob {
    steps: StdMutex<Vec<NamespaceId>>,
}

impl SubscribingJob {
    fn stepped(&self) -> Vec<String> {
        names(&self.steps)
    }
}

#[async_trait::async_trait]
impl MaintenanceJob for SubscribingJob {
    fn id(&self) -> MaintenanceJobId {
        SUBSCRIBING_JOB
    }

    fn nudged_by_publications(&self) -> bool {
        true
    }

    async fn step(
        &self,
        namespace_id: &NamespaceId,
        _continuation: Option<&str>,
    ) -> Result<MaintenanceStepResult> {
        self.steps.lock().expect("steps").push(namespace_id.clone());
        Ok(MaintenanceStepResult::concluded(
            MaintenanceStepConclusion::Idle,
        ))
    }

    async fn probe(&self, _namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        Ok(MaintenanceProbe::Idle)
    }
}

fn runner_with(
    policy: FsBackgroundWork,
    clock: Arc<dyn MaintenanceClock>,
    job: Arc<dyn MaintenanceJob>,
) -> MaintenanceRunner {
    let runner = MaintenanceRunner::with_clock(
        policy,
        Some(tokio::runtime::Handle::current()),
        nonzero_usize(1),
        clock,
        RuntimeInstruments::new(None),
    );
    runner.register(job).expect("register the test job");
    runner
}

fn enabled_runner(job: Arc<dyn MaintenanceJob>) -> MaintenanceRunner {
    runner_with(
        FsBackgroundWork::Enabled,
        Arc::new(SystemMaintenanceClock::default()),
        job,
    )
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
            StepAnswer::Conclude(MaintenanceStepConclusion::Progressed),
            StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
            StepAnswer::Conclude(MaintenanceStepConclusion::Progressed),
        ],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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

/// The continuation round trip over a live runner: what a step hands back
/// is what the next step is handed, without the job holding a map of its
/// own.
#[tokio::test]
async fn a_progressing_job_resumes_from_where_its_last_step_stopped() {
    let job = TestJob::scripted(
        [
            StepAnswer::Continue(
                MaintenanceStepConclusion::Progressed,
                Some("page-1".to_owned()),
            ),
            StepAnswer::Continue(
                MaintenanceStepConclusion::Progressed,
                Some("page-2".to_owned()),
            ),
        ],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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

/// A step that ran out of budget keeps its position, so the retry picks up
/// there instead of walking the same ground again.
#[tokio::test]
async fn a_blocked_job_resumes_from_where_it_parked() {
    let job = TestJob::scripted(
        [StepAnswer::Continue(
            MaintenanceStepConclusion::Blocked,
            Some("page-7".to_owned()),
        )],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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

/// An evicted key takes its position with it: nothing survives the job
/// saying it has nothing to maintain here.
#[tokio::test]
async fn a_not_enabled_conclusion_drops_the_continuation() {
    let job = TestJob::scripted(
        [
            StepAnswer::Continue(
                MaintenanceStepConclusion::Progressed,
                Some("page-1".to_owned()),
            ),
            StepAnswer::Conclude(MaintenanceStepConclusion::NotEnabled),
        ],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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
        [StepAnswer::Conclude(MaintenanceStepConclusion::Blocked)],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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
        [StepAnswer::Conclude(MaintenanceStepConclusion::Superseded)],
        StepAnswer::Conclude(MaintenanceStepConclusion::NotEnabled),
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
        [StepAnswer::Fail],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
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
    let runner = runner_with(FsBackgroundWork::Enabled, clock.clone(), job.clone());
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
    runner.tick_now();
    runner.drain().await.expect("the due key settles");
    assert_eq!(
        job.stepped(),
        vec!["leased".to_owned()],
        "the key fires once its time arrives"
    );
}

/// The other end of the same mechanism: a step that reports a deadline
/// arms its own key with it. This is what makes a pass self-scheduling —
/// reclamation nothing planted a deadline for, and deadlines a restart
/// forgot, both come back through the next pass over the namespace.
#[tokio::test]
async fn a_step_that_reports_a_deadline_re_arms_its_own_key() {
    let clock = ManualClock::at(1_000_000);
    let job = TestJob::scripted(
        [StepAnswer::Due(MaintenanceStepConclusion::Idle, 1_060_000)],
        StepAnswer::Conclude(MaintenanceStepConclusion::Idle),
    );
    let runner = runner_with(FsBackgroundWork::Enabled, clock.clone(), job.clone());
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
    runner.tick_now();
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

/// Publications reach the jobs that asked for them and no others, so a
/// host wires nothing into the write path on any job's behalf.
#[tokio::test]
async fn a_publication_nudges_only_the_jobs_that_subscribe_to_it() {
    let quiet = TestJob::idle();
    let subscriber = Arc::new(SubscribingJob::default());
    let runner = enabled_runner(quiet.clone());
    runner
        .register(subscriber.clone())
        .expect("register the subscribing job");
    let namespace_id = namespace_id("published");

    runner.nudge_publication_subscribers(&namespace_id);
    runner.drain().await.expect("the subscriber's step settles");

    assert_eq!(subscriber.stepped(), vec!["published".to_owned()]);
    assert!(
        quiet.stepped().is_empty(),
        "a job that did not subscribe hears nothing from a publication"
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
    let runner = runner_with(FsBackgroundWork::Enabled, clock, job);
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
async fn manual_only_ignores_nudges_but_still_registers_jobs() {
    let job = TestJob::idle();
    let runner = runner_with(
        FsBackgroundWork::ManualOnly,
        Arc::new(SystemMaintenanceClock::default()),
        job.clone(),
    );
    let namespace_id = namespace_id("manual");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    runner
        .handle()
        .nudge_not_before(TEST_JOB, &namespace_id, u64::MAX);
    runner.drain().await.expect("nothing was scheduled");

    assert!(job.stepped().is_empty(), "manual-only schedules nothing");
    assert!(!runner.is_pending(TEST_JOB, &namespace_id));
    assert!(
        runner.register(TestJob::idle()).is_err(),
        "registration is accepted under either policy, so this id is taken"
    );
}

#[tokio::test]
async fn shutdown_clears_pending_work_and_refuses_later_nudges() {
    let job = TestJob::gated([], StepAnswer::Conclude(MaintenanceStepConclusion::Idle));
    let runner = enabled_runner(job.clone());
    let (active, queued) = (namespace_id("active"), namespace_id("queued"));

    runner.handle().nudge(TEST_JOB, &active);
    job.gate().wait_entered(1).await;
    runner.handle().nudge(TEST_JOB, &queued);
    assert!(runner.is_pending(TEST_JOB, &queued));

    runner.shut_down();
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
    let job = TestJob::answering(StepAnswer::Panic);
    let runner = enabled_runner(job);
    let namespace_id = namespace_id("panics");

    runner.handle().nudge(TEST_JOB, &namespace_id);
    let error = runner.drain().await.expect_err("a panicked step surfaces");
    assert!(
        error.to_string().contains("panicked"),
        "drain reports panicked tasks: {error}"
    );
    assert!(
        !runner.is_pending(TEST_JOB, &namespace_id),
        "a panicked step releases its key and its permit"
    );
}

/// The interleaving a shutdown must win, ported from the previous
/// scheduler: work claims its slot, a whole shutdown-and-drain completes
/// while the registry is still empty, and only then does the claimed work
/// reach spawn.
#[tokio::test]
async fn work_claimed_before_a_shutdown_is_refused_and_gives_its_permit_back() {
    let job = TestJob::idle();
    let runner = enabled_runner(job.clone());
    let key = MaintenanceKey::new(TEST_JOB, &namespace_id("demo"));

    runner.inner.lock_state().admission.nudge(key.clone());
    let claimed = runner
        .inner
        .lock_state()
        .admission
        .try_dispatch(0)
        .expect("the claim lands first");
    assert_eq!(runner.running_steps(), 1);

    runner.shut_down();
    runner.drain().await.expect("nothing is scheduled yet");

    spawn_chain(&runner.inner, claimed);
    runner
        .drain()
        .await
        .expect("the refused chain registered nothing");
    assert!(
        job.stepped().is_empty(),
        "a refused chain must never run a step"
    );
    assert_eq!(
        runner.running_steps(),
        0,
        "a refused spawn drops the chain, and the drop gives the permit back"
    );
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

async fn upload_writer(root: &std::path::Path, clock: Arc<dyn MaintenanceClock>) -> FsWriter {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    FsWriter::builder_with_store(store)
        .writer_id("upload-lease-writer")
        .background_work(FsBackgroundWork::Enabled)
        .maintenance_clock(clock)
        .build()
        .await
        .expect("build writer")
}

/// The lease-expiry admission decision, end to end: opening a session and
/// completing one each plant the collection deadline they create, at the
/// time the collector's own predicate will allow.
#[tokio::test]
async fn upload_paths_plant_the_collection_deadlines_they_create() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let clock = ManualClock::at(1_700_000_000_000);
    let namespace_id = namespace_id("uploads");
    let writer = upload_writer(temp_dir.path(), clock.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let begun = writer
        .begin_upload(&namespace_id, BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    assert_eq!(
        writer
            .bits
            .maintenance
            .not_before_ms(MaintenanceJobId::GC, &namespace_id),
        Some(upload_session_reclaim_at_ms(clock.now_ms())),
        "an open session's lease schedules the pass that may abort it"
    );

    let uploaded = writer
        .upload_content(&namespace_id, &begun.upload_id, b"body")
        .await
        .expect("upload content");
    writer
        .complete_upload(
            &namespace_id,
            &begun.upload_id,
            &CompleteUploadRequest::for_content_ref(uploaded.content_ref),
        )
        .await
        .expect("complete upload");
    // The session's own lease comes due first, so it stays the next wake;
    // completion's later grace is remembered and re-arms after that pass.
    assert_eq!(
        writer
            .bits
            .maintenance
            .not_before_ms(MaintenanceJobId::GC, &namespace_id),
        Some(upload_session_reclaim_at_ms(clock.now_ms())),
        "the soonest planted deadline is what the runner wakes for"
    );
    clock.advance_to(upload_session_reclaim_at_ms(1_700_000_000_000));
    writer.bits.maintenance.tick_now();
    assert_eq!(
        writer
            .bits
            .maintenance
            .not_before_ms(MaintenanceJobId::GC, &namespace_id),
        Some(completed_upload_reclaim_at_ms(1_700_000_000_000)),
        "completion's reclamation grace outlives the lease pass it did not ask for"
    );

    writer
        .shutdown()
        .await
        .expect("shut down writer background work");
    assert_eq!(
        writer
            .bits
            .maintenance
            .not_before_ms(MaintenanceJobId::GC, &namespace_id),
        None,
        "shutdown clears the obligations it will not be around to honor"
    );
}

/// The runtime registers exactly the two jobs it owns, under the ids every
/// write path nudges.
#[tokio::test]
async fn the_runtime_registers_its_metadata_and_gc_jobs() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let writer = upload_writer(temp_dir.path(), ManualClock::at(0)).await;

    for id in [MaintenanceJobId::METADATA, MaintenanceJobId::GC] {
        assert!(
            writer.bits.maintenance.is_registered(id),
            "the runtime registers `{id}` on every write-capable handle"
        );
    }
    assert!(
        !writer
            .bits
            .maintenance
            .is_registered(MaintenanceJobId::new("retention")),
        "retention is not a job and never gets one"
    );

    // An extension's executor registers alongside them, once.
    writer
        .register_maintenance_job(TestJob::idle())
        .expect("register an extension job");
    assert!(
        writer.register_maintenance_job(TestJob::idle()).is_err(),
        "an id may only be registered once"
    );
}
