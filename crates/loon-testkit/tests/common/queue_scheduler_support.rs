use loon_model::{
    ModelBrokerLeaseOutcome, ModelError, ModelJobClaimOutcome, ModelJobCompleteOutcome,
    ModelMetadataState, ModelNamespace, ModelProgressObject, ModelQueueBroker, ModelQueueClaim,
    ModelQueueJob, ModelQueueJobState, ModelQueueRepairOutcome, ModelQueueSeqPayload,
    ModelQueueShard, ModelQueueWorkClass,
};
use loon_queue::broker::{renew_broker_lease, BrokerLeaseError, BrokerLeaseOutcome};
use loon_queue::repair::{
    build_snapshot_dedupe_key, repair_lost_snapshot_enqueue, SnapshotRepairError,
    SnapshotRepairOutcome,
};
use loon_queue::types::{
    JobState, QueueBroker, QueueClaim, QueueJob, QueueShardState, SeqScopedPayload, WorkClass,
};
use loon_queue::worker::{
    claim_job, complete_job, heartbeat_job, JobClaimOutcome, JobCompleteOutcome,
    WorkerMutationError,
};
use loon_sim::SimRuntime;
use loon_testkit::invariants::{
    evaluate_progress_publish_invariants, evaluate_queue_broker_lease_invariants,
    evaluate_queue_claim_invariants, evaluate_queue_complete_invariants,
    evaluate_queue_heartbeat_invariants, evaluate_queue_repair_invariants,
    BackgroundWorkInvariantReport, ProgressInvariantSnapshot, ProgressPublishInvariantInputs,
    ProgressPublishOutcomeKind, QueueBrokerLeaseInvariantInputs, QueueBrokerLeaseOutcomeKind,
    QueueClaimInvariantInputs, QueueClaimOutcomeKind, QueueCompleteInvariantInputs,
    QueueCompleteOutcomeKind, QueueHeartbeatInvariantInputs, QueueHeartbeatOutcomeKind,
    QueueRepairInvariantInputs, QueueRepairOutcomeKind,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{ChangeSeq, HeadState, NamespaceId, ProgressState};
use serde::Deserialize;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueSimRunStatus {
    Passed,
    Failed { failure_headline: String },
    NotExecutable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueExplorationCaseLabel {
    pub case_index: usize,
    pub seed: Option<u64>,
    pub fault_summary: String,
    pub permuted_action_order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueSimRunOptions {
    pub exploration_case: Option<QueueExplorationCaseLabel>,
}

#[derive(Debug, Clone)]
pub struct QueueFixtureRunReport {
    pub rendered_trace: String,
    pub status: QueueSimRunStatus,
}

#[allow(dead_code)]
pub fn run_queue_fixture_report(relative_path: &str) -> QueueFixtureRunReport {
    let scenario = load_fixture(relative_path);
    run_queue_scenario_report(&scenario, QueueSimRunOptions::default())
}

pub fn run_queue_scenario_report(
    scenario: &Scenario,
    options: QueueSimRunOptions,
) -> QueueFixtureRunReport {
    let initial: QueueFixtureInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<QueueActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: QueueFixtureExpect = scenario.decode_expect().expect("decode expectations");

    let model_namespace = initial.head.as_ref().map(model_namespace_from_head);
    let model_progress = initial.progress.as_ref().map(model_progress_from_fixture);
    let mut model_queue = model_queue_from_fixture(
        initial
            .queue
            .as_ref()
            .expect("queue fixture should contain initial queue state"),
    );
    let mut queue = queue_from_fixture(
        initial
            .queue
            .as_ref()
            .expect("queue fixture should contain initial queue state"),
    );

    let mut runtime = SimRuntime::new();
    let mut trace = Vec::new();
    let execution = catch_unwind(AssertUnwindSafe(|| {
        if let Some(exploration_case) = &options.exploration_case {
            runtime.record_exploration_case_started(
                exploration_case.case_index,
                exploration_case.seed,
                exploration_case.fault_summary.clone(),
                exploration_case.permuted_action_order.clone(),
            );
            push_latest_runtime_trace_line(&mut trace, &runtime);
        }

        runtime.record_actor_step(
            "fixture",
            format!(
                "initial model_queue={:?} queue={:?}",
                snapshot_from_model_queue(&model_queue),
                snapshot_from_queue(&queue)
            ),
        );
        push_latest_runtime_trace_line(&mut trace, &runtime);
        let mut observed_invariants = Vec::new();
        let mut saw_stolen_claim = false;

        if initial.progress.is_some() {
            let progress = initial.progress.as_ref().expect("progress should exist");
            let progress_report =
                evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
                    expected_namespace: &progress.payload.namespace_id,
                    expected_work_class: &progress.payload.work_class,
                    before_through_seq: None,
                    requested_through_seq: progress.payload.through_seq,
                    outcome: ProgressPublishOutcomeKind::Created,
                    after_progress: &ProgressInvariantSnapshot {
                        object_key: progress.key.clone(),
                        namespace_id: progress.payload.namespace_id.clone(),
                        work_class: progress.payload.work_class.clone(),
                        through_seq: progress.payload.through_seq,
                        payload_checksum_valid: true,
                    },
                });
            assert_background_report_passes(
                scenario,
                &mut trace,
                "queue-progress",
                &progress_report,
                0,
                &mut observed_invariants,
            );
        }

        assert_queue_states_match(scenario, &trace, 0, &model_queue, &queue);

        for (index, action) in actions.iter().enumerate() {
            let action_ref = action.kind();
            let model_before = snapshot_from_model_queue(&model_queue);
            let queue_before = snapshot_from_queue(&queue);
            let prior_stolen_claim_seen = saw_stolen_claim;
            let (model_outcome, queue_outcome) = match action_ref {
                QueueActionRef::RepairLostEnqueue(action) => {
                    let namespace = model_namespace
                        .as_ref()
                        .expect("repair fixture should provide initial head state");
                    let head = initial
                        .head
                        .as_ref()
                        .expect("repair fixture should provide initial head state");
                    assert_eq!(
                        action.namespace_id, head.namespace_id,
                        "repair action namespace should match head namespace"
                    );

                    let model_result = namespace
                        .repair_lost_snapshot_enqueue(&mut model_queue, model_progress.as_ref())
                        .map(QueueOutcome::from)
                        .unwrap_or_else(|err| QueueOutcome::Error(normalize_model_error(err)));
                    let queue_result = repair_lost_snapshot_enqueue(
                        &mut queue,
                        head,
                        initial_progress_payload(&initial),
                    )
                    .map(QueueOutcome::from)
                    .unwrap_or_else(|err| QueueOutcome::Error(normalize_repair_error(err)));

                    (model_result, queue_result)
                }
                QueueActionRef::BrokerRenewLease(action) => {
                    assert_eq!(
                        action.now_ms,
                        runtime.now_ms(),
                        "broker renewal should use the scenario's current simulated time"
                    );

                    let model_result = model_queue
                        .renew_broker_lease(&action.broker, action.now_ms, action.lease_duration_ms)
                        .map(QueueOutcome::from)
                        .unwrap_or_else(|err| QueueOutcome::Error(normalize_model_error(err)));
                    let queue_result = renew_broker_lease(
                        &mut queue,
                        &action.broker,
                        action.now_ms,
                        action.lease_duration_ms,
                    )
                    .map(QueueOutcome::from)
                    .unwrap_or_else(|err| QueueOutcome::Error(normalize_broker_error(err)));

                    (model_result, queue_result)
                }
                QueueActionRef::WorkerClaim(action) => {
                    assert_eq!(
                        action.now_ms,
                        runtime.now_ms(),
                        "worker claim should use the scenario's current simulated time"
                    );

                    let model_result = model_queue
                        .claim_job(
                            &action.broker,
                            action.broker_epoch,
                            &action.worker,
                            &action.claim_token,
                            &action.job_id,
                            action.now_ms,
                            action.claim_timeout_ms,
                        )
                        .map(QueueOutcome::from)
                        .unwrap_or_else(|err| QueueOutcome::Error(normalize_model_error(err)));
                    let queue_result = claim_job(
                        &mut queue,
                        &action.broker,
                        action.broker_epoch,
                        &action.worker,
                        &action.claim_token,
                        &action.job_id,
                        action.now_ms,
                        action.claim_timeout_ms,
                    )
                    .map(QueueOutcome::from)
                    .unwrap_or_else(|err| QueueOutcome::Error(normalize_worker_error(err)));

                    if matches!(
                        queue_result,
                        QueueOutcome::Claim(ClaimSnapshot::Stolen { .. })
                    ) {
                        saw_stolen_claim = true;
                    }

                    (model_result, queue_result)
                }
                QueueActionRef::WorkerHeartbeat(action) => {
                    assert_eq!(
                        action.now_ms,
                        runtime.now_ms(),
                        "worker heartbeat should use the scenario's current simulated time"
                    );

                    let model_result = model_queue
                        .heartbeat_job(
                            &action.broker,
                            action.broker_epoch,
                            &action.job_id,
                            &action.claim_token,
                            action.now_ms,
                            action.claim_timeout_ms,
                        )
                        .map(|_| {
                            QueueOutcome::Heartbeat(HeartbeatSnapshot {
                                claim_token: action.claim_token.clone(),
                                timeout_at_ms: action
                                    .now_ms
                                    .saturating_add(action.claim_timeout_ms),
                            })
                        })
                        .unwrap_or_else(|err| QueueOutcome::Error(normalize_model_error(err)));
                    let queue_result = heartbeat_job(
                        &mut queue,
                        &action.broker,
                        action.broker_epoch,
                        &action.job_id,
                        &action.claim_token,
                        action.now_ms,
                        action.claim_timeout_ms,
                    )
                    .map(|outcome| {
                        QueueOutcome::Heartbeat(HeartbeatSnapshot {
                            claim_token: outcome.claim_token,
                            timeout_at_ms: outcome.timeout_at_ms,
                        })
                    })
                    .unwrap_or_else(|err| QueueOutcome::Error(normalize_worker_error(err)));

                    (model_result, queue_result)
                }
                QueueActionRef::WorkerComplete(action) => {
                    let model_result = model_queue
                        .complete_job(
                            &action.broker,
                            action.broker_epoch,
                            &action.job_id,
                            &action.claim_token,
                            runtime.now_ms(),
                        )
                        .map(QueueOutcome::from)
                        .unwrap_or_else(|err| QueueOutcome::Error(normalize_model_error(err)));
                    let queue_result = complete_job(
                        &mut queue,
                        &action.broker,
                        action.broker_epoch,
                        &action.job_id,
                        &action.claim_token,
                        runtime.now_ms(),
                    )
                    .map(QueueOutcome::from)
                    .unwrap_or_else(|err| QueueOutcome::Error(normalize_worker_error(err)));

                    (model_result, queue_result)
                }
                QueueActionRef::AdvanceTimeMs(delta_ms) => {
                    runtime.advance_time(delta_ms);
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    let outcome = QueueOutcome::TimeAdvanced {
                        now_ms: runtime.now_ms(),
                    };
                    (outcome.clone(), outcome)
                }
            };

            runtime.record_actor_step(
            queue_actor_id_for(action_ref),
            format!(
                "step={} action={} model_outcome={:?} queue_outcome={:?} now_ms={} model_queue={:?} queue={:?}",
                index + 1,
                action.describe(),
                model_outcome,
                queue_outcome,
                runtime.now_ms(),
                snapshot_from_model_queue(&model_queue),
                snapshot_from_queue(&queue)
            ),
        );
            push_latest_runtime_trace_line(&mut trace, &runtime);

            if model_outcome != queue_outcome {
                panic!(
                    "queue differential outcome mismatch at step {}:\n{}",
                    index + 1,
                    render_trace(&scenario, &trace)
                );
            }

            assert_queue_states_match(&scenario, &trace, index + 1, &model_queue, &queue);
            let model_after = snapshot_from_model_queue(&model_queue);
            let queue_after = snapshot_from_queue(&queue);
            let model_report = evaluate_queue_step_invariants(
                action_ref,
                &initial,
                runtime.now_ms(),
                &model_before,
                &model_after,
                &model_outcome,
                prior_stolen_claim_seen,
            );
            let queue_report = evaluate_queue_step_invariants(
                action_ref,
                &initial,
                runtime.now_ms(),
                &queue_before,
                &queue_after,
                &queue_outcome,
                prior_stolen_claim_seen,
            );
            assert_background_reports_match_and_pass(
                &scenario,
                &mut trace,
                "queue-model",
                &model_report,
                "queue-core",
                &queue_report,
                index + 1,
                &mut observed_invariants,
            );
        }

        if let Some(expected_queue) = &expect.queue {
            assert_fixture_queue_expectation(scenario, &trace, expected_queue, &queue);
            assert_fixture_queue_expectation_against_model(
                scenario,
                &trace,
                expected_queue,
                &model_queue,
            );
        }

        for invariant in &expect.invariants {
            assert!(
                observed_invariants.iter().any(|value| value == invariant),
                "missing expected invariant `{invariant}`:\n{}",
                render_trace(scenario, &trace)
            );
        }
    }));

    let status = match execution {
        Ok(()) => QueueSimRunStatus::Passed,
        Err(payload) => {
            let headline = panic_payload_to_headline(payload);
            if let Some(reason) = queue_not_executable_reason(&headline) {
                QueueSimRunStatus::NotExecutable { reason }
            } else {
                QueueSimRunStatus::Failed {
                    failure_headline: headline,
                }
            }
        }
    };

    QueueFixtureRunReport {
        rendered_trace: render_trace(scenario, &trace),
        status,
    }
}

#[derive(Debug, Deserialize)]
struct QueueFixtureInitial {
    #[serde(default)]
    head: Option<HeadState>,
    #[serde(default)]
    progress: Option<FixtureProgressObject>,
    #[serde(default)]
    queue: Option<QueueFixtureState>,
}

#[derive(Debug, Deserialize)]
struct QueueFixtureExpect {
    #[serde(default)]
    queue: Option<QueueExpectState>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureProgressObject {
    key: String,
    payload: ProgressState,
}

#[derive(Debug, Clone, Deserialize)]
struct QueueFixtureState {
    work_class: WorkClass,
    shard: u32,
    #[serde(default)]
    broker: Option<QueueBroker>,
    #[serde(default)]
    jobs: Vec<QueueFixtureJob>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueueFixtureJob {
    #[serde(default)]
    job_id: Option<String>,
    dedupe_key: String,
    state: JobState,
    payload: SeqScopedPayload,
    #[serde(default)]
    follow_up: Option<SeqScopedPayload>,
    #[serde(default)]
    claim: Option<QueueClaim>,
    #[serde(default)]
    attempts: u32,
}

#[derive(Debug, Deserialize)]
struct QueueExpectState {
    #[serde(default)]
    jobs: Option<Vec<QueueExpectJob>>,
}

#[derive(Debug, Deserialize)]
struct QueueExpectJob {
    #[serde(default)]
    job_id: Option<String>,
    #[serde(default)]
    dedupe_key: Option<String>,
    #[serde(default)]
    state: Option<JobState>,
    #[serde(default)]
    payload: Option<SeqScopedPayload>,
    #[serde(default)]
    follow_up: Option<SeqScopedPayload>,
    #[serde(default)]
    claim: Option<QueueClaim>,
    #[serde(default)]
    attempts: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct QueueActionEnvelope {
    #[serde(default)]
    repair_lost_enqueue: Option<RepairLostEnqueueAction>,
    #[serde(default)]
    broker_renew_lease: Option<BrokerRenewLeaseAction>,
    #[serde(default)]
    worker_claim: Option<WorkerClaimAction>,
    #[serde(default)]
    worker_heartbeat: Option<WorkerHeartbeatAction>,
    #[serde(default)]
    worker_complete: Option<WorkerCompleteAction>,
    #[serde(default)]
    advance_time_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RepairLostEnqueueAction {
    namespace_id: NamespaceId,
}

#[derive(Debug, Deserialize)]
struct BrokerRenewLeaseAction {
    broker: String,
    now_ms: u64,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct WorkerClaimAction {
    broker: String,
    broker_epoch: u64,
    worker: String,
    claim_token: String,
    now_ms: u64,
    claim_timeout_ms: u64,
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct WorkerHeartbeatAction {
    broker: String,
    broker_epoch: u64,
    worker: String,
    claim_token: String,
    now_ms: u64,
    claim_timeout_ms: u64,
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct WorkerCompleteAction {
    broker: String,
    broker_epoch: u64,
    worker: String,
    claim_token: String,
    job_id: String,
}

#[derive(Clone, Copy)]
enum QueueActionRef<'a> {
    RepairLostEnqueue(&'a RepairLostEnqueueAction),
    BrokerRenewLease(&'a BrokerRenewLeaseAction),
    WorkerClaim(&'a WorkerClaimAction),
    WorkerHeartbeat(&'a WorkerHeartbeatAction),
    WorkerComplete(&'a WorkerCompleteAction),
    AdvanceTimeMs(u64),
}

impl QueueActionEnvelope {
    fn kind(&self) -> QueueActionRef<'_> {
        let mut matches = Vec::new();

        if let Some(action) = &self.repair_lost_enqueue {
            matches.push(QueueActionRef::RepairLostEnqueue(action));
        }
        if let Some(action) = &self.broker_renew_lease {
            matches.push(QueueActionRef::BrokerRenewLease(action));
        }
        if let Some(action) = &self.worker_claim {
            matches.push(QueueActionRef::WorkerClaim(action));
        }
        if let Some(action) = &self.worker_heartbeat {
            matches.push(QueueActionRef::WorkerHeartbeat(action));
        }
        if let Some(action) = &self.worker_complete {
            matches.push(QueueActionRef::WorkerComplete(action));
        }
        if let Some(delta_ms) = self.advance_time_ms {
            matches.push(QueueActionRef::AdvanceTimeMs(delta_ms));
        }

        assert_eq!(
            matches.len(),
            1,
            "queue action envelope should contain exactly one action variant"
        );
        matches
            .into_iter()
            .next()
            .expect("one queue action variant")
    }

    fn describe(&self) -> String {
        match self.kind() {
            QueueActionRef::RepairLostEnqueue(action) => {
                format!("repair_lost_enqueue(namespace_id={})", action.namespace_id.as_str())
            }
            QueueActionRef::BrokerRenewLease(action) => format!(
                "broker_renew_lease(broker={}, now_ms={}, lease_duration_ms={})",
                action.broker, action.now_ms, action.lease_duration_ms
            ),
            QueueActionRef::WorkerClaim(action) => format!(
                "worker_claim(broker={}, broker_epoch={}, worker={}, claim_token={}, now_ms={}, claim_timeout_ms={}, job_id={})",
                action.broker,
                action.broker_epoch,
                action.worker,
                action.claim_token,
                action.now_ms,
                action.claim_timeout_ms,
                action.job_id
            ),
            QueueActionRef::WorkerHeartbeat(action) => format!(
                "worker_heartbeat(broker={}, broker_epoch={}, worker={}, claim_token={}, now_ms={}, claim_timeout_ms={}, job_id={})",
                action.broker,
                action.broker_epoch,
                action.worker,
                action.claim_token,
                action.now_ms,
                action.claim_timeout_ms,
                action.job_id
            ),
            QueueActionRef::WorkerComplete(action) => format!(
                "worker_complete(broker={}, broker_epoch={}, worker={}, claim_token={}, job_id={})",
                action.broker, action.broker_epoch, action.worker, action.claim_token, action.job_id
            ),
            QueueActionRef::AdvanceTimeMs(delta_ms) => {
                format!("advance_time_ms(delta_ms={delta_ms})")
            }
        }
    }
}

fn queue_actor_id_for(action: QueueActionRef<'_>) -> String {
    match action {
        QueueActionRef::RepairLostEnqueue(_) => "repair".to_owned(),
        QueueActionRef::BrokerRenewLease(action) => format!("broker:{}", action.broker),
        QueueActionRef::WorkerClaim(action) => format!("worker:{}", action.worker),
        QueueActionRef::WorkerHeartbeat(action) => format!("worker:{}", action.worker),
        QueueActionRef::WorkerComplete(action) => format!("worker:{}", action.worker),
        QueueActionRef::AdvanceTimeMs(_) => "clock".to_owned(),
    }
}

fn push_latest_runtime_trace_line(trace: &mut Vec<String>, runtime: &SimRuntime) {
    let line = runtime
        .trace()
        .last()
        .expect("runtime should contain at least one trace event")
        .render_line();
    trace.push(line);
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueueOutcome {
    Repair(RepairSnapshot),
    BrokerLease(BrokerLeaseSnapshot),
    Claim(ClaimSnapshot),
    Heartbeat(HeartbeatSnapshot),
    Complete(CompleteSnapshot),
    Error(QueueMutationError),
    TimeAdvanced { now_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RepairSnapshot {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BrokerLeaseSnapshot {
    Acquired { epoch: u64 },
    Renewed { epoch: u64 },
    TakenOver { epoch: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClaimSnapshot {
    Claimed { claim_token: String },
    Stolen { claim_token: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeartbeatSnapshot {
    claim_token: String,
    timeout_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CompleteSnapshot {
    Removed,
    PromotedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueueMutationError {
    QueueWorkClassMismatch {
        expected: String,
        actual: String,
    },
    ProgressNamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    ProgressWorkClassMismatch {
        expected: String,
        actual: String,
    },
    BrokerLeaseHeldByOther {
        active_broker_id: String,
        active_epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    MissingBrokerLease,
    BrokerLeaseMismatch {
        expected_broker_id: String,
        expected_epoch: u64,
        actual_broker_id: String,
        actual_epoch: u64,
    },
    BrokerLeaseExpired {
        broker_id: String,
        epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    JobNotFound {
        job_id: String,
    },
    JobBusy {
        job_id: String,
        worker_id: String,
        timeout_at_ms: u64,
        now_ms: u64,
    },
    JobNotClaimed {
        job_id: String,
    },
    ClaimTokenMismatch {
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueSnapshot {
    work_class: String,
    shard_id: u32,
    broker: Option<BrokerStateSnapshot>,
    jobs: Vec<JobStateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrokerStateSnapshot {
    broker_id: String,
    epoch: u64,
    lease_expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JobStateSnapshot {
    job_id: String,
    dedupe_key: String,
    state: String,
    payload: PayloadSnapshot,
    follow_up: Option<PayloadSnapshot>,
    claim: Option<ClaimStateSnapshot>,
    attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PayloadSnapshot {
    namespace_id: NamespaceId,
    through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClaimStateSnapshot {
    worker_id: String,
    claim_token: String,
    heartbeat_at_ms: u64,
    timeout_at_ms: u64,
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn model_namespace_from_head(head: &HeadState) -> ModelNamespace {
    ModelNamespace {
        namespace_id: head.namespace_id.clone(),
        head_seq: head.seq,
        active_fence_token: head.active_fence_token,
        next_inode_id: head.next_inode_id,
        snapshot_hint_seq: head.snapshot_hint_seq,
        retention_floor_seq: head.retention_floor_seq,
        metadata_state: ModelMetadataState::default(),
    }
}

fn model_progress_from_fixture(progress: &FixtureProgressObject) -> ModelProgressObject {
    let expected_key = loon_objectstore::keys::derived_progress(
        progress.payload.namespace_id.as_str(),
        &progress.payload.work_class,
    );
    assert_eq!(
        progress.key, expected_key,
        "fixture progress object key should match namespace/work-class"
    );

    ModelProgressObject {
        namespace_id: progress.payload.namespace_id.clone(),
        work_class: progress.payload.work_class.clone(),
        through_seq: progress.payload.through_seq,
    }
}

fn initial_progress_payload(initial: &QueueFixtureInitial) -> Option<&ProgressState> {
    initial.progress.as_ref().map(|progress| &progress.payload)
}

fn model_queue_from_fixture(fixture: &QueueFixtureState) -> ModelQueueShard {
    ModelQueueShard {
        work_class: model_work_class_from_queue(&fixture.work_class),
        shard_id: fixture.shard,
        broker: fixture.broker.as_ref().map(|broker| ModelQueueBroker {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
        }),
        jobs: fixture.jobs.iter().map(model_job_from_fixture).collect(),
    }
}

fn model_job_from_fixture(job: &QueueFixtureJob) -> ModelQueueJob {
    ModelQueueJob {
        job_id: job
            .job_id
            .clone()
            .unwrap_or_else(|| format!("fixture-job-{}", job.dedupe_key)),
        dedupe_key: job.dedupe_key.clone(),
        state: model_job_state_from_queue(&job.state),
        payload: ModelQueueSeqPayload {
            namespace_id: job.payload.namespace_id.clone(),
            through_seq: job.payload.through_seq,
        },
        follow_up: job.follow_up.as_ref().map(|payload| ModelQueueSeqPayload {
            namespace_id: payload.namespace_id.clone(),
            through_seq: payload.through_seq,
        }),
        claim: job.claim.as_ref().map(|claim| ModelQueueClaim {
            worker_id: claim.worker_id.clone(),
            claim_token: claim.claim_token.clone(),
            heartbeat_at_ms: claim.heartbeat_at_ms,
            timeout_at_ms: claim.timeout_at_ms,
        }),
        attempts: job.attempts,
    }
}

fn queue_from_fixture(fixture: &QueueFixtureState) -> QueueShardState {
    QueueShardState {
        work_class: fixture.work_class.clone(),
        shard_id: fixture.shard,
        broker: fixture.broker.clone(),
        jobs: fixture.jobs.iter().map(queue_job_from_fixture).collect(),
    }
}

fn queue_job_from_fixture(job: &QueueFixtureJob) -> QueueJob {
    QueueJob {
        job_id: job
            .job_id
            .clone()
            .unwrap_or_else(|| format!("fixture-job-{}", job.dedupe_key)),
        dedupe_key: job.dedupe_key.clone(),
        state: job.state.clone(),
        payload: job.payload.clone(),
        follow_up: job.follow_up.clone(),
        claim: job.claim.clone(),
        attempts: job.attempts,
    }
}

fn normalize_model_error(error: ModelError) -> QueueMutationError {
    match error {
        ModelError::QueueWorkClassMismatch { expected, actual } => {
            QueueMutationError::QueueWorkClassMismatch {
                expected: model_work_class_name(expected).to_owned(),
                actual: model_work_class_name(actual).to_owned(),
            }
        }
        ModelError::ProgressNamespaceMismatch {
            expected, actual, ..
        } => QueueMutationError::ProgressNamespaceMismatch { expected, actual },
        ModelError::ProgressWorkClassMismatch { expected, actual } => {
            QueueMutationError::ProgressWorkClassMismatch { expected, actual }
        }
        ModelError::BrokerLeaseHeldByOther {
            active_broker_id,
            active_epoch,
            lease_expires_at_ms,
            now_ms,
        } => QueueMutationError::BrokerLeaseHeldByOther {
            active_broker_id,
            active_epoch,
            lease_expires_at_ms,
            now_ms,
        },
        ModelError::MissingBrokerLease => QueueMutationError::MissingBrokerLease,
        ModelError::BrokerLeaseMismatch {
            expected_broker_id,
            expected_epoch,
            actual_broker_id,
            actual_epoch,
        } => QueueMutationError::BrokerLeaseMismatch {
            expected_broker_id,
            expected_epoch,
            actual_broker_id,
            actual_epoch,
        },
        ModelError::BrokerLeaseExpired {
            broker_id,
            epoch,
            lease_expires_at_ms,
            now_ms,
        } => QueueMutationError::BrokerLeaseExpired {
            broker_id,
            epoch,
            lease_expires_at_ms,
            now_ms,
        },
        ModelError::JobNotFound { job_id } => QueueMutationError::JobNotFound { job_id },
        ModelError::JobBusy {
            job_id,
            worker_id,
            timeout_at_ms,
            now_ms,
        } => QueueMutationError::JobBusy {
            job_id,
            worker_id,
            timeout_at_ms,
            now_ms,
        },
        ModelError::JobNotClaimed { job_id } => QueueMutationError::JobNotClaimed { job_id },
        ModelError::ClaimTokenMismatch { expected, actual } => {
            QueueMutationError::ClaimTokenMismatch { expected, actual }
        }
        other => panic!("unexpected model error in queue differential harness: {other:?}"),
    }
}

fn normalize_repair_error(error: SnapshotRepairError) -> QueueMutationError {
    match error {
        SnapshotRepairError::QueueWorkClassMismatch { expected, actual } => {
            QueueMutationError::QueueWorkClassMismatch {
                expected: expected.as_str().to_owned(),
                actual: actual.as_str().to_owned(),
            }
        }
        SnapshotRepairError::ProgressNamespaceMismatch { expected, actual } => {
            QueueMutationError::ProgressNamespaceMismatch { expected, actual }
        }
        SnapshotRepairError::ProgressWorkClassMismatch { expected, actual } => {
            QueueMutationError::ProgressWorkClassMismatch { expected, actual }
        }
    }
}

fn normalize_broker_error(error: BrokerLeaseError) -> QueueMutationError {
    match error {
        BrokerLeaseError::LeaseHeldByOther {
            active_broker_id,
            active_epoch,
            lease_expires_at_ms,
            now_ms,
        } => QueueMutationError::BrokerLeaseHeldByOther {
            active_broker_id,
            active_epoch,
            lease_expires_at_ms,
            now_ms,
        },
    }
}

fn normalize_worker_error(error: WorkerMutationError) -> QueueMutationError {
    match error {
        WorkerMutationError::Broker(error) => match error {
            loon_queue::broker::BrokerLeaseGuardError::MissingBrokerLease => {
                QueueMutationError::MissingBrokerLease
            }
            loon_queue::broker::BrokerLeaseGuardError::BrokerLeaseMismatch {
                expected_broker_id,
                expected_epoch,
                actual_broker_id,
                actual_epoch,
            } => QueueMutationError::BrokerLeaseMismatch {
                expected_broker_id,
                expected_epoch,
                actual_broker_id,
                actual_epoch,
            },
            loon_queue::broker::BrokerLeaseGuardError::BrokerLeaseExpired {
                broker_id,
                epoch,
                lease_expires_at_ms,
                now_ms,
            } => QueueMutationError::BrokerLeaseExpired {
                broker_id,
                epoch,
                lease_expires_at_ms,
                now_ms,
            },
        },
        WorkerMutationError::JobNotFound { job_id } => QueueMutationError::JobNotFound { job_id },
        WorkerMutationError::JobBusy {
            job_id,
            worker_id,
            timeout_at_ms,
            now_ms,
        } => QueueMutationError::JobBusy {
            job_id,
            worker_id,
            timeout_at_ms,
            now_ms,
        },
        WorkerMutationError::JobNotClaimed { job_id } => {
            QueueMutationError::JobNotClaimed { job_id }
        }
        WorkerMutationError::ClaimTokenMismatch { expected, actual } => {
            QueueMutationError::ClaimTokenMismatch { expected, actual }
        }
    }
}

impl From<ModelQueueRepairOutcome> for QueueOutcome {
    fn from(value: ModelQueueRepairOutcome) -> Self {
        Self::Repair(match value {
            ModelQueueRepairOutcome::NoRepairNeeded => RepairSnapshot::NoRepairNeeded,
            ModelQueueRepairOutcome::Enqueued { through_seq } => {
                RepairSnapshot::Enqueued { through_seq }
            }
            ModelQueueRepairOutcome::RaisedReadyJob { through_seq } => {
                RepairSnapshot::RaisedReadyJob { through_seq }
            }
            ModelQueueRepairOutcome::AttachedFollowUp { through_seq } => {
                RepairSnapshot::AttachedFollowUp { through_seq }
            }
        })
    }
}

impl From<SnapshotRepairOutcome> for QueueOutcome {
    fn from(value: SnapshotRepairOutcome) -> Self {
        Self::Repair(match value {
            SnapshotRepairOutcome::NoRepairNeeded => RepairSnapshot::NoRepairNeeded,
            SnapshotRepairOutcome::Enqueued { through_seq } => {
                RepairSnapshot::Enqueued { through_seq }
            }
            SnapshotRepairOutcome::RaisedReadyJob { through_seq } => {
                RepairSnapshot::RaisedReadyJob { through_seq }
            }
            SnapshotRepairOutcome::AttachedFollowUp { through_seq } => {
                RepairSnapshot::AttachedFollowUp { through_seq }
            }
        })
    }
}

impl From<ModelBrokerLeaseOutcome> for QueueOutcome {
    fn from(value: ModelBrokerLeaseOutcome) -> Self {
        Self::BrokerLease(match value {
            ModelBrokerLeaseOutcome::Acquired { epoch } => BrokerLeaseSnapshot::Acquired { epoch },
            ModelBrokerLeaseOutcome::Renewed { epoch } => BrokerLeaseSnapshot::Renewed { epoch },
            ModelBrokerLeaseOutcome::TakenOver { epoch } => {
                BrokerLeaseSnapshot::TakenOver { epoch }
            }
        })
    }
}

impl From<BrokerLeaseOutcome> for QueueOutcome {
    fn from(value: BrokerLeaseOutcome) -> Self {
        Self::BrokerLease(match value {
            BrokerLeaseOutcome::Acquired { epoch } => BrokerLeaseSnapshot::Acquired { epoch },
            BrokerLeaseOutcome::Renewed { epoch } => BrokerLeaseSnapshot::Renewed { epoch },
            BrokerLeaseOutcome::TakenOver { epoch } => BrokerLeaseSnapshot::TakenOver { epoch },
        })
    }
}

impl From<ModelJobClaimOutcome> for QueueOutcome {
    fn from(value: ModelJobClaimOutcome) -> Self {
        Self::Claim(match value {
            ModelJobClaimOutcome::Claimed { claim_token } => ClaimSnapshot::Claimed { claim_token },
            ModelJobClaimOutcome::Stolen { claim_token } => ClaimSnapshot::Stolen { claim_token },
        })
    }
}

impl From<JobClaimOutcome> for QueueOutcome {
    fn from(value: JobClaimOutcome) -> Self {
        Self::Claim(match value {
            JobClaimOutcome::Claimed { claim_token } => ClaimSnapshot::Claimed { claim_token },
            JobClaimOutcome::Stolen { claim_token } => ClaimSnapshot::Stolen { claim_token },
        })
    }
}

impl From<ModelJobCompleteOutcome> for QueueOutcome {
    fn from(value: ModelJobCompleteOutcome) -> Self {
        Self::Complete(match value {
            ModelJobCompleteOutcome::Removed => CompleteSnapshot::Removed,
            ModelJobCompleteOutcome::PromotedFollowUp { through_seq } => {
                CompleteSnapshot::PromotedFollowUp { through_seq }
            }
        })
    }
}

impl From<JobCompleteOutcome> for QueueOutcome {
    fn from(value: JobCompleteOutcome) -> Self {
        Self::Complete(match value {
            JobCompleteOutcome::Removed => CompleteSnapshot::Removed,
            JobCompleteOutcome::PromotedFollowUp { through_seq } => {
                CompleteSnapshot::PromotedFollowUp { through_seq }
            }
        })
    }
}

fn evaluate_queue_step_invariants(
    action: QueueActionRef<'_>,
    initial: &QueueFixtureInitial,
    now_ms: u64,
    before: &QueueSnapshot,
    after: &QueueSnapshot,
    outcome: &QueueOutcome,
    prior_stolen_claim_seen: bool,
) -> BackgroundWorkInvariantReport {
    match action {
        QueueActionRef::RepairLostEnqueue(action) => {
            let head = initial
                .head
                .as_ref()
                .expect("repair fixture should provide initial head state");
            let dedupe_key = build_snapshot_dedupe_key(&action.namespace_id);
            let after_job = after.jobs.iter().find(|job| job.dedupe_key == dedupe_key);
            evaluate_queue_repair_invariants(QueueRepairInvariantInputs {
                namespace_id: &action.namespace_id,
                head_seq: head.seq,
                progress_through_seq: initial
                    .progress
                    .as_ref()
                    .map(|progress| progress.payload.through_seq),
                outcome: queue_repair_outcome_kind(outcome),
                has_namespace_scoped_job_after: after_job.is_some(),
                ready_job_through_seq_after: after_job
                    .filter(|job| job.state == "Ready")
                    .map(|job| job.payload.through_seq),
                follow_up_through_seq_after: after_job
                    .and_then(|job| job.follow_up.as_ref().map(|payload| payload.through_seq)),
            })
        }
        QueueActionRef::BrokerRenewLease(action) => {
            evaluate_queue_broker_lease_invariants(QueueBrokerLeaseInvariantInputs {
                broker_id: &action.broker,
                before_broker_id: before
                    .broker
                    .as_ref()
                    .map(|broker| broker.broker_id.as_str()),
                before_epoch: before.broker.as_ref().map(|broker| broker.epoch),
                after_broker_id: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.broker_id.as_str()),
                after_epoch: after.broker.as_ref().map(|broker| broker.epoch),
                outcome: queue_broker_lease_outcome_kind(outcome),
            })
        }
        QueueActionRef::WorkerClaim(action) => {
            let before_job = before.jobs.iter().find(|job| job.job_id == action.job_id);
            let after_job = after.jobs.iter().find(|job| job.job_id == action.job_id);
            evaluate_queue_claim_invariants(QueueClaimInvariantInputs {
                broker_id: &action.broker,
                broker_epoch: action.broker_epoch,
                now_ms,
                claim_token: &action.claim_token,
                before_timeout_at_ms: before_job
                    .and_then(|job| job.claim.as_ref().map(|claim| claim.timeout_at_ms)),
                after_broker_id: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.broker_id.as_str()),
                after_broker_epoch: after.broker.as_ref().map(|broker| broker.epoch),
                after_broker_lease_expires_at_ms: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.lease_expires_at_ms),
                after_claim_token: after_job
                    .and_then(|job| job.claim.as_ref().map(|claim| claim.claim_token.as_str())),
                outcome: queue_claim_outcome_kind(outcome),
            })
        }
        QueueActionRef::WorkerHeartbeat(action) => {
            let after_job = after.jobs.iter().find(|job| job.job_id == action.job_id);
            evaluate_queue_heartbeat_invariants(QueueHeartbeatInvariantInputs {
                broker_id: &action.broker,
                broker_epoch: action.broker_epoch,
                now_ms,
                claim_token: &action.claim_token,
                after_broker_id: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.broker_id.as_str()),
                after_broker_epoch: after.broker.as_ref().map(|broker| broker.epoch),
                after_broker_lease_expires_at_ms: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.lease_expires_at_ms),
                after_claim_token: after_job
                    .and_then(|job| job.claim.as_ref().map(|claim| claim.claim_token.as_str())),
                after_timeout_at_ms: after_job
                    .and_then(|job| job.claim.as_ref().map(|claim| claim.timeout_at_ms)),
                outcome: queue_heartbeat_outcome_kind(outcome),
            })
        }
        QueueActionRef::WorkerComplete(action) => {
            let before_job = before.jobs.iter().find(|job| job.job_id == action.job_id);
            let after_job_present = after.jobs.iter().any(|job| job.job_id == action.job_id);
            evaluate_queue_complete_invariants(QueueCompleteInvariantInputs {
                broker_id: &action.broker,
                broker_epoch: action.broker_epoch,
                now_ms,
                provided_claim_token: &action.claim_token,
                before_claim_token: before_job
                    .and_then(|job| job.claim.as_ref().map(|claim| claim.claim_token.as_str())),
                after_broker_id: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.broker_id.as_str()),
                after_broker_epoch: after.broker.as_ref().map(|broker| broker.epoch),
                after_broker_lease_expires_at_ms: after
                    .broker
                    .as_ref()
                    .map(|broker| broker.lease_expires_at_ms),
                after_job_present,
                prior_stolen_claim_seen,
                outcome: queue_complete_outcome_kind(outcome),
            })
        }
        QueueActionRef::AdvanceTimeMs(_) => BackgroundWorkInvariantReport::default(),
    }
}

fn queue_repair_outcome_kind(outcome: &QueueOutcome) -> QueueRepairOutcomeKind {
    match outcome {
        QueueOutcome::Repair(RepairSnapshot::NoRepairNeeded) => {
            QueueRepairOutcomeKind::NoRepairNeeded
        }
        QueueOutcome::Repair(RepairSnapshot::Enqueued { through_seq }) => {
            QueueRepairOutcomeKind::Enqueued {
                through_seq: *through_seq,
            }
        }
        QueueOutcome::Repair(RepairSnapshot::RaisedReadyJob { through_seq }) => {
            QueueRepairOutcomeKind::RaisedReadyJob {
                through_seq: *through_seq,
            }
        }
        QueueOutcome::Repair(RepairSnapshot::AttachedFollowUp { through_seq }) => {
            QueueRepairOutcomeKind::AttachedFollowUp {
                through_seq: *through_seq,
            }
        }
        other => panic!("expected repair outcome, got {other:?}"),
    }
}

fn queue_broker_lease_outcome_kind(outcome: &QueueOutcome) -> QueueBrokerLeaseOutcomeKind {
    match outcome {
        QueueOutcome::BrokerLease(BrokerLeaseSnapshot::Acquired { epoch }) => {
            QueueBrokerLeaseOutcomeKind::Acquired { epoch: *epoch }
        }
        QueueOutcome::BrokerLease(BrokerLeaseSnapshot::Renewed { epoch }) => {
            QueueBrokerLeaseOutcomeKind::Renewed { epoch: *epoch }
        }
        QueueOutcome::BrokerLease(BrokerLeaseSnapshot::TakenOver { epoch }) => {
            QueueBrokerLeaseOutcomeKind::TakenOver { epoch: *epoch }
        }
        other => panic!("expected broker lease outcome, got {other:?}"),
    }
}

fn queue_claim_outcome_kind<'a>(outcome: &'a QueueOutcome) -> QueueClaimOutcomeKind<'a> {
    match outcome {
        QueueOutcome::Claim(ClaimSnapshot::Claimed { claim_token }) => {
            QueueClaimOutcomeKind::Claimed { claim_token }
        }
        QueueOutcome::Claim(ClaimSnapshot::Stolen { claim_token }) => {
            QueueClaimOutcomeKind::Stolen { claim_token }
        }
        QueueOutcome::Error(_) => QueueClaimOutcomeKind::Error,
        other => panic!("expected claim outcome, got {other:?}"),
    }
}

fn queue_heartbeat_outcome_kind<'a>(outcome: &'a QueueOutcome) -> QueueHeartbeatOutcomeKind<'a> {
    match outcome {
        QueueOutcome::Heartbeat(HeartbeatSnapshot {
            claim_token,
            timeout_at_ms,
        }) => QueueHeartbeatOutcomeKind::Heartbeat {
            claim_token,
            timeout_at_ms: *timeout_at_ms,
        },
        QueueOutcome::Error(_) => QueueHeartbeatOutcomeKind::Error,
        other => panic!("expected heartbeat outcome, got {other:?}"),
    }
}

fn queue_complete_outcome_kind(outcome: &QueueOutcome) -> QueueCompleteOutcomeKind {
    match outcome {
        QueueOutcome::Complete(CompleteSnapshot::Removed) => QueueCompleteOutcomeKind::Removed,
        QueueOutcome::Complete(CompleteSnapshot::PromotedFollowUp { through_seq }) => {
            QueueCompleteOutcomeKind::PromotedFollowUp {
                through_seq: *through_seq,
            }
        }
        QueueOutcome::Error(QueueMutationError::ClaimTokenMismatch { .. }) => {
            QueueCompleteOutcomeKind::ClaimTokenMismatch
        }
        QueueOutcome::Error(_) => QueueCompleteOutcomeKind::Error,
        other => panic!("expected complete outcome, got {other:?}"),
    }
}

fn assert_background_report_passes(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    label: &str,
    report: &BackgroundWorkInvariantReport,
    step: usize,
    observed_invariants: &mut Vec<String>,
) {
    trace.extend(report.render_trace_lines(label));
    for check in &report.checks {
        if !check.passed {
            panic!(
                "background invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        if !observed_invariants.iter().any(|value| value == &check.name) {
            observed_invariants.push(check.name.clone());
        }
    }
}

fn assert_background_reports_match_and_pass(
    scenario: &Scenario,
    trace: &mut Vec<String>,
    left_label: &str,
    left: &BackgroundWorkInvariantReport,
    right_label: &str,
    right: &BackgroundWorkInvariantReport,
    step: usize,
    observed_invariants: &mut Vec<String>,
) {
    trace.extend(left.render_trace_lines(left_label));
    trace.extend(right.render_trace_lines(right_label));

    if left.checks != right.checks {
        panic!(
            "background invariant report mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
    for check in &right.checks {
        if !check.passed {
            panic!(
                "background invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        if !observed_invariants.iter().any(|value| value == &check.name) {
            observed_invariants.push(check.name.clone());
        }
    }
}

fn snapshot_from_model_queue(queue: &ModelQueueShard) -> QueueSnapshot {
    QueueSnapshot {
        work_class: model_work_class_name(queue.work_class).to_owned(),
        shard_id: queue.shard_id,
        broker: queue.broker.as_ref().map(|broker| BrokerStateSnapshot {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
        }),
        jobs: queue
            .jobs
            .iter()
            .map(|job| JobStateSnapshot {
                job_id: job.job_id.clone(),
                dedupe_key: job.dedupe_key.clone(),
                state: model_job_state_name(job.state).to_owned(),
                payload: PayloadSnapshot {
                    namespace_id: job.payload.namespace_id.clone(),
                    through_seq: job.payload.through_seq,
                },
                follow_up: job.follow_up.as_ref().map(|payload| PayloadSnapshot {
                    namespace_id: payload.namespace_id.clone(),
                    through_seq: payload.through_seq,
                }),
                claim: job.claim.as_ref().map(|claim| ClaimStateSnapshot {
                    worker_id: claim.worker_id.clone(),
                    claim_token: claim.claim_token.clone(),
                    heartbeat_at_ms: claim.heartbeat_at_ms,
                    timeout_at_ms: claim.timeout_at_ms,
                }),
                attempts: job.attempts,
            })
            .collect(),
    }
}

fn snapshot_from_queue(queue: &QueueShardState) -> QueueSnapshot {
    QueueSnapshot {
        work_class: queue.work_class.as_str().to_owned(),
        shard_id: queue.shard_id,
        broker: queue.broker.as_ref().map(|broker| BrokerStateSnapshot {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
        }),
        jobs: queue
            .jobs
            .iter()
            .map(|job| JobStateSnapshot {
                job_id: job.job_id.clone(),
                dedupe_key: job.dedupe_key.clone(),
                state: job_state_name(&job.state).to_owned(),
                payload: PayloadSnapshot {
                    namespace_id: job.payload.namespace_id.clone(),
                    through_seq: job.payload.through_seq,
                },
                follow_up: job.follow_up.as_ref().map(|payload| PayloadSnapshot {
                    namespace_id: payload.namespace_id.clone(),
                    through_seq: payload.through_seq,
                }),
                claim: job.claim.as_ref().map(|claim| ClaimStateSnapshot {
                    worker_id: claim.worker_id.clone(),
                    claim_token: claim.claim_token.clone(),
                    heartbeat_at_ms: claim.heartbeat_at_ms,
                    timeout_at_ms: claim.timeout_at_ms,
                }),
                attempts: job.attempts,
            })
            .collect(),
    }
}

fn assert_queue_states_match(
    scenario: &Scenario,
    trace: &[String],
    step: usize,
    model_queue: &ModelQueueShard,
    queue: &QueueShardState,
) {
    let model_snapshot = snapshot_from_model_queue(model_queue);
    let queue_snapshot = snapshot_from_queue(queue);
    if model_snapshot != queue_snapshot {
        panic!(
            "queue differential state mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}

fn assert_fixture_queue_expectation(
    scenario: &Scenario,
    trace: &[String],
    expected: &QueueExpectState,
    actual: &QueueShardState,
) {
    if let Some(expected_jobs) = &expected.jobs {
        assert_eq!(
            actual.jobs.len(),
            expected_jobs.len(),
            "queue expectation job count mismatch:\n{}",
            render_trace(scenario, trace)
        );

        for (expected_job, actual_job) in expected_jobs.iter().zip(&actual.jobs) {
            assert_queue_job_expectation(scenario, trace, expected_job, actual_job);
        }
    }
}

fn assert_fixture_queue_expectation_against_model(
    scenario: &Scenario,
    trace: &[String],
    expected: &QueueExpectState,
    actual: &ModelQueueShard,
) {
    if let Some(expected_jobs) = &expected.jobs {
        assert_eq!(
            actual.jobs.len(),
            expected_jobs.len(),
            "model queue expectation job count mismatch:\n{}",
            render_trace(scenario, trace)
        );

        for (expected_job, actual_job) in expected_jobs.iter().zip(&actual.jobs) {
            assert_model_queue_job_expectation(scenario, trace, expected_job, actual_job);
        }
    }
}

fn assert_queue_job_expectation(
    scenario: &Scenario,
    trace: &[String],
    expected: &QueueExpectJob,
    actual: &QueueJob,
) {
    if let Some(expected_job_id) = &expected.job_id {
        assert_eq!(
            &actual.job_id,
            expected_job_id,
            "queue expectation job_id mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_dedupe_key) = &expected.dedupe_key {
        assert_eq!(
            &actual.dedupe_key,
            expected_dedupe_key,
            "queue expectation dedupe_key mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_state) = &expected.state {
        assert_eq!(
            &actual.state,
            expected_state,
            "queue expectation state mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_payload) = &expected.payload {
        assert_eq!(
            &actual.payload,
            expected_payload,
            "queue expectation payload mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_follow_up) = &expected.follow_up {
        assert_eq!(
            actual.follow_up.as_ref(),
            Some(expected_follow_up),
            "queue expectation follow_up mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_claim) = &expected.claim {
        assert_eq!(
            actual.claim.as_ref(),
            Some(expected_claim),
            "queue expectation claim mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_attempts) = expected.attempts {
        assert_eq!(
            actual.attempts,
            expected_attempts,
            "queue expectation attempts mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
}

fn assert_model_queue_job_expectation(
    scenario: &Scenario,
    trace: &[String],
    expected: &QueueExpectJob,
    actual: &ModelQueueJob,
) {
    if let Some(expected_job_id) = &expected.job_id {
        assert_eq!(
            &actual.job_id,
            expected_job_id,
            "model queue expectation job_id mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_dedupe_key) = &expected.dedupe_key {
        assert_eq!(
            &actual.dedupe_key,
            expected_dedupe_key,
            "model queue expectation dedupe_key mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_state) = &expected.state {
        assert_eq!(
            model_job_state_name(actual.state),
            job_state_name(expected_state),
            "model queue expectation state mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_payload) = &expected.payload {
        assert_eq!(
            actual.payload.namespace_id,
            expected_payload.namespace_id,
            "model queue expectation payload namespace mismatch:\n{}",
            render_trace(scenario, trace)
        );
        assert_eq!(
            actual.payload.through_seq,
            expected_payload.through_seq,
            "model queue expectation payload seq mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_follow_up) = &expected.follow_up {
        let actual_follow_up = actual
            .follow_up
            .as_ref()
            .expect("model queue should have expected follow-up");
        assert_eq!(
            actual_follow_up.namespace_id,
            expected_follow_up.namespace_id,
            "model queue expectation follow_up namespace mismatch:\n{}",
            render_trace(scenario, trace)
        );
        assert_eq!(
            actual_follow_up.through_seq,
            expected_follow_up.through_seq,
            "model queue expectation follow_up seq mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_claim) = &expected.claim {
        let actual_claim = actual
            .claim
            .as_ref()
            .expect("model queue should have expected claim");
        assert_eq!(
            actual_claim.worker_id,
            expected_claim.worker_id,
            "model queue expectation claim worker mismatch:\n{}",
            render_trace(scenario, trace)
        );
        assert_eq!(
            actual_claim.claim_token,
            expected_claim.claim_token,
            "model queue expectation claim token mismatch:\n{}",
            render_trace(scenario, trace)
        );
        assert_eq!(
            actual_claim.heartbeat_at_ms,
            expected_claim.heartbeat_at_ms,
            "model queue expectation claim heartbeat mismatch:\n{}",
            render_trace(scenario, trace)
        );
        assert_eq!(
            actual_claim.timeout_at_ms,
            expected_claim.timeout_at_ms,
            "model queue expectation claim timeout mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected_attempts) = expected.attempts {
        assert_eq!(
            actual.attempts,
            expected_attempts,
            "model queue expectation attempts mismatch:\n{}",
            render_trace(scenario, trace)
        );
    }
}

fn model_work_class_from_queue(work_class: &WorkClass) -> ModelQueueWorkClass {
    match work_class {
        WorkClass::BuildSnapshot => ModelQueueWorkClass::BuildSnapshot,
        other => panic!("unsupported work class in queue differential harness: {other:?}"),
    }
}

fn model_work_class_name(work_class: ModelQueueWorkClass) -> &'static str {
    match work_class {
        ModelQueueWorkClass::BuildSnapshot => "BuildSnapshot",
    }
}

fn model_job_state_from_queue(state: &JobState) -> ModelQueueJobState {
    match state {
        JobState::Ready => ModelQueueJobState::Ready,
        JobState::Claimed => ModelQueueJobState::Claimed,
    }
}

fn model_job_state_name(state: ModelQueueJobState) -> &'static str {
    match state {
        ModelQueueJobState::Ready => "Ready",
        ModelQueueJobState::Claimed => "Claimed",
    }
}

fn job_state_name(state: &JobState) -> &'static str {
    match state {
        JobState::Ready => "Ready",
        JobState::Claimed => "Claimed",
    }
}

fn queue_not_executable_reason(headline: &str) -> Option<String> {
    if headline.contains("repair fixture should provide initial head state") {
        return Some("repair_lost_enqueue requires an initial head state".to_owned());
    }
    None
}

fn panic_payload_to_headline(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => message.lines().next().unwrap_or("").to_owned(),
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => message.lines().next().unwrap_or("").to_owned(),
            Err(_) => "queue sim panicked".to_owned(),
        },
    }
}
