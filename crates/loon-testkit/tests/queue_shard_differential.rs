use loon_model::{
    ModelBrokerLeaseOutcome, ModelError, ModelJobClaimOutcome, ModelJobClaimParams,
    ModelJobCompleteOutcome, ModelMetadataState, ModelNamespace, ModelProgressObject,
    ModelQueueBroker, ModelQueueClaim, ModelQueueJob, ModelQueueJobState, ModelQueueRepairOutcome,
    ModelQueueSeqPayload, ModelQueueShard, ModelQueueWorkClass,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::ObjectStore;
use loon_queue::durable::{
    claim_job_in_store, complete_job_in_store, read_queue_shard, renew_broker_lease_in_store,
    repair_lost_snapshot_enqueue_in_store, DurableBrokerLeaseOutcome, DurableJobClaimOutcome,
    DurableJobCompleteOutcome, DurableSnapshotRepairOutcome, DurableWorkerMutationError,
};
use loon_queue::repair::build_snapshot_dedupe_key;
use loon_queue::types::{JobState, QueueJob, QueueShardEnvelope, QueueShardState, WorkClass};
use loon_queue::worker::{JobClaimRequest, JobCompleteRequest};
use loon_testkit::invariants::{
    evaluate_queue_broker_lease_invariants, evaluate_queue_claim_invariants,
    evaluate_queue_complete_invariants, evaluate_queue_repair_invariants,
    evaluate_queue_shard_object_invariants, BackgroundWorkInvariantReport,
    QueueBrokerLeaseInvariantInputs, QueueBrokerLeaseOutcomeKind, QueueClaimInvariantInputs,
    QueueClaimOutcomeKind, QueueCompleteInvariantInputs, QueueCompleteOutcomeKind,
    QueueRepairInvariantInputs, QueueRepairOutcomeKind, QueueShardObjectInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{ChangeSeq, ControlObjectKind, HeadState, NamespaceId, ProgressState};
use serde::Deserialize;

const TEST_WRITER_VERSION: &str = "loon-testkit-differential";

#[test]
fn lost_enqueue_repair_updates_queue_shard_object_fixture_matches_model_and_core() {
    let report = run_fixture_report("native/lost_enqueue_repair_updates_queue_shard_object.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn queue_shard_claim_timeout_then_steal_updates_object_fixture_matches_model_and_core() {
    let report =
        run_fixture_report("native/queue_shard_claim_timeout_then_steal_updates_object.yaml");
    assert!(!report.rendered_trace.is_empty());
}

#[test]
fn queue_shard_claim_timeout_then_steal_snapshot_matches_checked_in_artifact() {
    let report =
        run_fixture_report("native/queue_shard_claim_timeout_then_steal_updates_object.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/queue-shard-differential/native/queue_shard_claim_timeout_then_steal_updates_object.txt"
        )
    );
}

struct QueueShardFixtureRunReport {
    rendered_trace: String,
}

fn run_fixture_report(relative_path: &str) -> QueueShardFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: QueueShardFixtureInitial =
        scenario.decode_initial().expect("decode initial state");
    let actions: Vec<QueueShardActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: QueueShardFixtureExpect = scenario.decode_expect().expect("decode expect state");

    let initial_shard = initial
        .queue_shard
        .as_ref()
        .expect("native queue shard fixture should contain initial queue shard");
    let mut model_queue = model_queue_from_fixture(initial_shard);
    let model_namespace = initial.head.as_ref().map(model_namespace_from_head);
    let model_progress = initial.progress.as_ref().map(model_progress_from_fixture);

    let temp_dir = TestDir::new("queue-shard-differential");
    let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
    seed_queue_shard(&store, initial_shard);

    let mut trace = vec![format!(
        "initial model_queue={:?} core_queue={:?}",
        snapshot_from_model_queue(&model_queue),
        read_core_queue_snapshot(&store, initial_shard.payload.shard_id)
            .expect("read initial queue snapshot")
    )];
    let mut observed_invariants = Vec::new();
    let mut saw_stolen_claim = false;

    assert_queue_states_match(
        &scenario,
        &trace,
        0,
        &model_queue,
        &read_queue_shard(&store, initial_shard.payload.shard_id)
            .expect("read initial shard")
            .envelope
            .state,
    );

    for (index, action) in actions.iter().enumerate() {
        let before_core = read_core_queue_snapshot(&store, initial_shard.payload.shard_id)
            .expect("read queue shard before action");
        let prior_stolen_claim_seen = saw_stolen_claim;

        let (model_outcome, core_outcome) = match action.kind() {
            QueueShardActionRef::RepairLostEnqueue(action) => {
                let namespace = model_namespace
                    .as_ref()
                    .expect("repair fixture should provide head state");
                let head = initial
                    .head
                    .as_ref()
                    .expect("repair fixture should provide head state");
                let model_outcome = namespace
                    .repair_lost_snapshot_enqueue(&mut model_queue, model_progress.as_ref())
                    .map(NativeQueueOutcome::from)
                    .unwrap_or_else(|err| NativeQueueOutcome::Error(normalize_model_error(err)));
                let core_outcome = repair_lost_snapshot_enqueue_in_store(
                    &store,
                    action.shard_id,
                    head,
                    initial.progress.as_ref().map(|progress| &progress.payload),
                    TEST_WRITER_VERSION,
                )
                .map(NativeQueueOutcome::from)
                .unwrap_or_else(|err| panic!("queue shard repair should succeed: {err:?}"));
                (model_outcome, core_outcome)
            }
            QueueShardActionRef::RenewBrokerLease(action) => {
                let model_outcome = model_queue
                    .renew_broker_lease(&action.broker, action.now_ms, action.lease_duration_ms)
                    .map(NativeQueueOutcome::from)
                    .unwrap_or_else(|err| NativeQueueOutcome::Error(normalize_model_error(err)));
                let core_outcome = renew_broker_lease_in_store(
                    &store,
                    action.shard_id,
                    action.work_class.clone(),
                    &action.broker,
                    action.now_ms,
                    action.lease_duration_ms,
                    TEST_WRITER_VERSION,
                )
                .map(NativeQueueOutcome::from)
                .unwrap_or_else(|err| panic!("durable broker renewal should succeed: {err:?}"));
                (model_outcome, core_outcome)
            }
            QueueShardActionRef::ClaimQueueJob(action) => {
                let model_outcome = model_queue
                    .claim_job(
                        &action.job_id,
                        &ModelJobClaimParams {
                            broker_id: action.broker.clone(),
                            broker_epoch: action.broker_epoch,
                            worker_id: action.worker.clone(),
                            claim_token: action.claim_token.clone(),
                            now_ms: action.now_ms,
                            claim_timeout_ms: action.claim_timeout_ms,
                        },
                    )
                    .map(NativeQueueOutcome::from)
                    .unwrap_or_else(|err| NativeQueueOutcome::Error(normalize_model_error(err)));
                let core_outcome = claim_job_in_store(
                    &store,
                    action.shard_id,
                    &JobClaimRequest {
                        broker_id: &action.broker,
                        broker_epoch: action.broker_epoch,
                        worker_id: &action.worker,
                        claim_token: &action.claim_token,
                        job_id: &action.job_id,
                        now_ms: action.now_ms,
                        claim_timeout_ms: action.claim_timeout_ms,
                    },
                    TEST_WRITER_VERSION,
                )
                .map(NativeQueueOutcome::from)
                .unwrap_or_else(|err| panic!("durable claim should succeed: {err:?}"));
                if matches!(
                    core_outcome,
                    NativeQueueOutcome::Claim(ClaimSnapshot::Stolen { .. })
                ) {
                    saw_stolen_claim = true;
                }
                (model_outcome, core_outcome)
            }
            QueueShardActionRef::CompleteQueueJob(action) => {
                let model_outcome = model_queue
                    .complete_job(
                        &action.broker,
                        action.broker_epoch,
                        &action.job_id,
                        &action.claim_token,
                        action.now_ms,
                    )
                    .map(NativeQueueOutcome::from)
                    .unwrap_or_else(|err| NativeQueueOutcome::Error(normalize_model_error(err)));
                let core_outcome = complete_job_in_store(
                    &store,
                    action.shard_id,
                    &JobCompleteRequest {
                        broker_id: &action.broker,
                        broker_epoch: action.broker_epoch,
                        job_id: &action.job_id,
                        claim_token: &action.claim_token,
                        now_ms: action.now_ms,
                    },
                    TEST_WRITER_VERSION,
                )
                .map(NativeQueueOutcome::from)
                .unwrap_or_else(|err| {
                    NativeQueueOutcome::Error(normalize_durable_complete_error(err))
                });
                (model_outcome, core_outcome)
            }
        };

        let after_model = snapshot_from_model_queue(&model_queue);
        let after_loaded = read_queue_shard(&store, initial_shard.payload.shard_id)
            .expect("read queue shard object after action");
        let after_core = read_core_queue_snapshot(&store, initial_shard.payload.shard_id)
            .expect("read queue shard after action");
        trace.push(format!(
            "step={} action={} model_outcome={:?} core_outcome={:?} model_queue={:?} core_queue={:?}",
            index + 1,
            action.describe(),
            model_outcome,
            core_outcome,
            after_model,
            after_core
        ));

        if model_outcome != core_outcome {
            panic!(
                "native queue shard outcome mismatch at step {}:\n{}",
                index + 1,
                render_trace(&scenario, &trace)
            );
        }

        if !matches!(
            core_outcome,
            NativeQueueOutcome::Repair(RepairSnapshot::NoRepairNeeded)
        ) && !matches!(core_outcome, NativeQueueOutcome::Error(_))
        {
            let object_report =
                evaluate_queue_shard_object_invariants(QueueShardObjectInvariantInputs {
                    shard_index: after_loaded.envelope.state.shard_id,
                    payload_checksum_valid: true,
                    object_key: &after_loaded.object_key,
                    actual_shard_id: after_loaded.envelope.state.shard_id,
                    cas_protected: true,
                });
            assert_report_passes(
                &scenario,
                &mut trace,
                "queue-shard-object",
                &object_report,
                index + 1,
                &mut observed_invariants,
            );
        }

        let step_report = evaluate_native_queue_step_invariants(
            action.kind(),
            &initial,
            action.now_ms(),
            &before_core,
            &after_core,
            &core_outcome,
            prior_stolen_claim_seen,
        );
        assert_report_passes(
            &scenario,
            &mut trace,
            "queue-shard-core",
            &step_report,
            index + 1,
            &mut observed_invariants,
        );

        assert_queue_snapshots_match(&scenario, &trace, index + 1, &after_model, &after_core);
    }

    if let Some(expected_shard) = &expect.queue_shard {
        let expected_snapshot = snapshot_from_fixture_queue_shard(expected_shard);
        let actual_core = read_core_queue_snapshot(&store, expected_shard.payload.shard_id)
            .expect("read final queue shard");
        let actual_model = snapshot_from_model_queue(&model_queue);
        trace.push(format!(
            "final expected={:?} model={:?} core={:?}",
            expected_snapshot, actual_model, actual_core
        ));
        if actual_model != expected_snapshot || actual_core != expected_snapshot {
            panic!(
                "native queue shard expectation mismatch:\n{}",
                render_trace(&scenario, &trace)
            );
        }
    }

    for invariant in &expect.invariants {
        assert!(
            observed_invariants.iter().any(|value| value == invariant),
            "missing expected invariant `{invariant}`:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    QueueShardFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize)]
struct QueueShardFixtureInitial {
    #[serde(default)]
    head: Option<HeadState>,
    #[serde(default)]
    progress: Option<FixtureProgressObject>,
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
}

#[derive(Debug, Deserialize)]
struct QueueShardFixtureExpect {
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureProgressObject {
    payload: ProgressState,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureQueueShardObject {
    key: String,
    payload: FixtureQueueShardPayload,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureQueueShardPayload {
    work_class: WorkClass,
    shard_id: u32,
    #[serde(default)]
    broker: Option<loon_queue::types::QueueBroker>,
    #[serde(default)]
    jobs: Vec<FixtureQueueShardJob>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureQueueShardJob {
    #[serde(default)]
    job_id: Option<String>,
    dedupe_key: String,
    state: JobState,
    payload: loon_queue::types::SeqScopedPayload,
    #[serde(default)]
    follow_up: Option<loon_queue::types::SeqScopedPayload>,
    #[serde(default)]
    claim: Option<loon_queue::types::QueueClaim>,
    #[serde(default)]
    attempts: u32,
}

#[derive(Debug, Deserialize)]
struct QueueShardActionEnvelope {
    #[serde(default)]
    repair_lost_enqueue_to_queue_shard: Option<RepairLostEnqueueToQueueShardAction>,
    #[serde(default)]
    renew_broker_lease: Option<RenewBrokerLeaseAction>,
    #[serde(default)]
    claim_queue_job: Option<ClaimQueueJobAction>,
    #[serde(default)]
    complete_queue_job: Option<CompleteQueueJobAction>,
}

#[derive(Debug, Deserialize)]
struct RepairLostEnqueueToQueueShardAction {
    shard_id: u32,
    namespace_id: NamespaceId,
}

#[derive(Debug, Deserialize)]
struct RenewBrokerLeaseAction {
    shard_id: u32,
    work_class: WorkClass,
    broker: String,
    now_ms: u64,
    lease_duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ClaimQueueJobAction {
    shard_id: u32,
    broker: String,
    broker_epoch: u64,
    worker: String,
    claim_token: String,
    job_id: String,
    now_ms: u64,
    claim_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct CompleteQueueJobAction {
    shard_id: u32,
    broker: String,
    broker_epoch: u64,
    claim_token: String,
    job_id: String,
    now_ms: u64,
    #[serde(default)]
    expect_error: Option<String>,
}

#[derive(Clone, Copy)]
enum QueueShardActionRef<'a> {
    RepairLostEnqueue(&'a RepairLostEnqueueToQueueShardAction),
    RenewBrokerLease(&'a RenewBrokerLeaseAction),
    ClaimQueueJob(&'a ClaimQueueJobAction),
    CompleteQueueJob(&'a CompleteQueueJobAction),
}

impl QueueShardActionEnvelope {
    fn kind(&self) -> QueueShardActionRef<'_> {
        let mut matches = Vec::new();
        if let Some(action) = &self.repair_lost_enqueue_to_queue_shard {
            matches.push(QueueShardActionRef::RepairLostEnqueue(action));
        }
        if let Some(action) = &self.renew_broker_lease {
            matches.push(QueueShardActionRef::RenewBrokerLease(action));
        }
        if let Some(action) = &self.claim_queue_job {
            matches.push(QueueShardActionRef::ClaimQueueJob(action));
        }
        if let Some(action) = &self.complete_queue_job {
            matches.push(QueueShardActionRef::CompleteQueueJob(action));
        }
        assert_eq!(
            matches.len(),
            1,
            "native queue shard action should contain exactly one action"
        );
        matches
            .into_iter()
            .next()
            .expect("one native queue shard action")
    }

    fn describe(&self) -> String {
        match self.kind() {
            QueueShardActionRef::RepairLostEnqueue(action) => format!(
                "repair_lost_enqueue_to_queue_shard(shard_id={}, namespace_id={})",
                action.shard_id,
                action.namespace_id.as_str()
            ),
            QueueShardActionRef::RenewBrokerLease(action) => format!(
                "renew_broker_lease(shard_id={}, broker={}, now_ms={}, lease_duration_ms={})",
                action.shard_id, action.broker, action.now_ms, action.lease_duration_ms
            ),
            QueueShardActionRef::ClaimQueueJob(action) => format!(
                "claim_queue_job(shard_id={}, broker={}, broker_epoch={}, worker={}, claim_token={}, job_id={}, now_ms={}, claim_timeout_ms={})",
                action.shard_id,
                action.broker,
                action.broker_epoch,
                action.worker,
                action.claim_token,
                action.job_id,
                action.now_ms,
                action.claim_timeout_ms
            ),
            QueueShardActionRef::CompleteQueueJob(action) => format!(
                "complete_queue_job(shard_id={}, broker={}, broker_epoch={}, claim_token={}, job_id={}, now_ms={}, expect_error={:?})",
                action.shard_id,
                action.broker,
                action.broker_epoch,
                action.claim_token,
                action.job_id,
                action.now_ms,
                action.expect_error
            ),
        }
    }

    fn now_ms(&self) -> u64 {
        match self.kind() {
            QueueShardActionRef::RepairLostEnqueue(_) => 0,
            QueueShardActionRef::RenewBrokerLease(action) => action.now_ms,
            QueueShardActionRef::ClaimQueueJob(action) => action.now_ms,
            QueueShardActionRef::CompleteQueueJob(action) => action.now_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NativeQueueOutcome {
    Repair(RepairSnapshot),
    BrokerLease(BrokerLeaseSnapshot),
    Claim(ClaimSnapshot),
    Complete(CompleteSnapshot),
    Error(QueueMutationError),
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
enum CompleteSnapshot {
    Removed,
    PromotedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueueMutationError {
    ClaimTokenMismatch { expected: String, actual: String },
    Other(String),
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
    ModelProgressObject {
        namespace_id: progress.payload.namespace_id.clone(),
        work_class: progress.payload.work_class.clone(),
        through_seq: progress.payload.through_seq,
    }
}

fn model_queue_from_fixture(fixture: &FixtureQueueShardObject) -> ModelQueueShard {
    ModelQueueShard {
        work_class: model_work_class_from_queue(&fixture.payload.work_class),
        shard_id: fixture.payload.shard_id,
        broker: fixture
            .payload
            .broker
            .as_ref()
            .map(|broker| ModelQueueBroker {
                broker_id: broker.broker_id.clone(),
                epoch: broker.epoch,
                lease_expires_at_ms: broker.lease_expires_at_ms,
            }),
        jobs: fixture
            .payload
            .jobs
            .iter()
            .map(|job| ModelQueueJob {
                job_id: fixture_job_id(job, &fixture.payload.work_class),
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
            })
            .collect(),
    }
}

fn seed_queue_shard(store: &LocalFsStore, fixture: &FixtureQueueShardObject) {
    let envelope = QueueShardEnvelope::from_state(
        ControlObjectKind::QueueShard,
        TEST_WRITER_VERSION,
        queue_shard_state_from_fixture(&fixture.payload),
    )
    .expect("build queue shard envelope");
    store
        .put_if_absent(
            &fixture.key,
            &serde_json::to_vec(&envelope).expect("encode queue shard envelope"),
        )
        .expect("seed queue shard object");
}

fn read_core_queue_snapshot(store: &LocalFsStore, shard_id: u32) -> Result<QueueSnapshot, String> {
    let loaded = read_queue_shard(store, shard_id).map_err(|err| format!("{err:?}"))?;
    Ok(snapshot_from_core_queue(&loaded.envelope.state))
}

fn snapshot_from_fixture_queue_shard(fixture: &FixtureQueueShardObject) -> QueueSnapshot {
    snapshot_from_core_queue(&queue_shard_state_from_fixture(&fixture.payload))
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
                state: match job.state {
                    ModelQueueJobState::Ready => "Ready".to_owned(),
                    ModelQueueJobState::Claimed => "Claimed".to_owned(),
                },
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

fn snapshot_from_core_queue(queue: &QueueShardState) -> QueueSnapshot {
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
                state: match job.state {
                    JobState::Ready => "Ready".to_owned(),
                    JobState::Claimed => "Claimed".to_owned(),
                },
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

fn queue_shard_state_from_fixture(payload: &FixtureQueueShardPayload) -> QueueShardState {
    QueueShardState {
        work_class: payload.work_class.clone(),
        shard_id: payload.shard_id,
        broker: payload.broker.clone(),
        jobs: payload
            .jobs
            .iter()
            .map(|job| QueueJob {
                job_id: fixture_job_id(job, &payload.work_class),
                dedupe_key: job.dedupe_key.clone(),
                state: job.state.clone(),
                payload: job.payload.clone(),
                follow_up: job.follow_up.clone(),
                claim: job.claim.clone(),
                attempts: job.attempts,
            })
            .collect(),
    }
}

fn fixture_job_id(job: &FixtureQueueShardJob, work_class: &WorkClass) -> String {
    job.job_id.clone().unwrap_or_else(|| {
        if matches!(job.state, JobState::Ready | JobState::Claimed)
            && job.dedupe_key == format!("{}:{}", work_class.as_str(), job.payload.namespace_id)
        {
            format!(
                "repair-{}-{}",
                work_class.as_str(),
                job.payload.namespace_id
            )
        } else {
            format!("fixture-job-{}", job.dedupe_key)
        }
    })
}

fn evaluate_native_queue_step_invariants(
    action: QueueShardActionRef<'_>,
    initial: &QueueShardFixtureInitial,
    now_ms: u64,
    before: &QueueSnapshot,
    after: &QueueSnapshot,
    outcome: &NativeQueueOutcome,
    prior_stolen_claim_seen: bool,
) -> BackgroundWorkInvariantReport {
    match action {
        QueueShardActionRef::RepairLostEnqueue(action) => {
            let head = initial
                .head
                .as_ref()
                .expect("repair fixture should provide head state");
            let dedupe_key = build_snapshot_dedupe_key(&action.namespace_id);
            let after_job = after.jobs.iter().find(|job| job.dedupe_key == dedupe_key);
            evaluate_queue_repair_invariants(QueueRepairInvariantInputs {
                namespace_id: &action.namespace_id,
                head_seq: head.seq,
                progress_through_seq: initial
                    .progress
                    .as_ref()
                    .map(|progress| progress.payload.through_seq),
                outcome: match outcome {
                    NativeQueueOutcome::Repair(RepairSnapshot::NoRepairNeeded) => {
                        QueueRepairOutcomeKind::NoRepairNeeded
                    }
                    NativeQueueOutcome::Repair(RepairSnapshot::Enqueued { through_seq }) => {
                        QueueRepairOutcomeKind::Enqueued {
                            through_seq: *through_seq,
                        }
                    }
                    NativeQueueOutcome::Repair(RepairSnapshot::RaisedReadyJob { through_seq }) => {
                        QueueRepairOutcomeKind::RaisedReadyJob {
                            through_seq: *through_seq,
                        }
                    }
                    NativeQueueOutcome::Repair(RepairSnapshot::AttachedFollowUp {
                        through_seq,
                    }) => QueueRepairOutcomeKind::AttachedFollowUp {
                        through_seq: *through_seq,
                    },
                    other => panic!("expected repair outcome, got {other:?}"),
                },
                has_namespace_scoped_job_after: after_job.is_some(),
                ready_job_through_seq_after: after_job
                    .filter(|job| job.state == "Ready")
                    .map(|job| job.payload.through_seq),
                follow_up_through_seq_after: after_job
                    .and_then(|job| job.follow_up.as_ref().map(|payload| payload.through_seq)),
            })
        }
        QueueShardActionRef::RenewBrokerLease(action) => {
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
                outcome: match outcome {
                    NativeQueueOutcome::BrokerLease(BrokerLeaseSnapshot::Acquired { epoch }) => {
                        QueueBrokerLeaseOutcomeKind::Acquired { epoch: *epoch }
                    }
                    NativeQueueOutcome::BrokerLease(BrokerLeaseSnapshot::Renewed { epoch }) => {
                        QueueBrokerLeaseOutcomeKind::Renewed { epoch: *epoch }
                    }
                    NativeQueueOutcome::BrokerLease(BrokerLeaseSnapshot::TakenOver { epoch }) => {
                        QueueBrokerLeaseOutcomeKind::TakenOver { epoch: *epoch }
                    }
                    other => panic!("expected broker lease outcome, got {other:?}"),
                },
            })
        }
        QueueShardActionRef::ClaimQueueJob(action) => {
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
                outcome: match outcome {
                    NativeQueueOutcome::Claim(ClaimSnapshot::Claimed { claim_token }) => {
                        QueueClaimOutcomeKind::Claimed { claim_token }
                    }
                    NativeQueueOutcome::Claim(ClaimSnapshot::Stolen { claim_token }) => {
                        QueueClaimOutcomeKind::Stolen { claim_token }
                    }
                    NativeQueueOutcome::Error(_) => QueueClaimOutcomeKind::Error,
                    other => panic!("expected claim outcome, got {other:?}"),
                },
            })
        }
        QueueShardActionRef::CompleteQueueJob(action) => {
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
                outcome: match outcome {
                    NativeQueueOutcome::Complete(CompleteSnapshot::Removed) => {
                        QueueCompleteOutcomeKind::Removed
                    }
                    NativeQueueOutcome::Complete(CompleteSnapshot::PromotedFollowUp {
                        through_seq,
                    }) => QueueCompleteOutcomeKind::PromotedFollowUp {
                        through_seq: *through_seq,
                    },
                    NativeQueueOutcome::Error(QueueMutationError::ClaimTokenMismatch {
                        ..
                    }) => QueueCompleteOutcomeKind::ClaimTokenMismatch,
                    NativeQueueOutcome::Error(_) => QueueCompleteOutcomeKind::Error,
                    other => panic!("expected complete outcome, got {other:?}"),
                },
            })
        }
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
    let queue_snapshot = snapshot_from_core_queue(queue);
    if model_snapshot != queue_snapshot {
        panic!(
            "native queue shard state mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}

fn assert_queue_snapshots_match(
    scenario: &Scenario,
    trace: &[String],
    step: usize,
    model_snapshot: &QueueSnapshot,
    core_snapshot: &QueueSnapshot,
) {
    if model_snapshot != core_snapshot {
        panic!(
            "native queue shard snapshot mismatch at step {}:\n{}",
            step,
            render_trace(scenario, trace)
        );
    }
}

fn assert_report_passes(
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
                "native queue shard invariant failed at step {}: {}:\n{}",
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

fn model_work_class_from_queue(work_class: &WorkClass) -> ModelQueueWorkClass {
    match work_class {
        WorkClass::BuildSnapshot => ModelQueueWorkClass::BuildSnapshot,
        other => panic!("unsupported work class in native queue shard differential: {other:?}"),
    }
}

fn model_job_state_from_queue(state: &JobState) -> ModelQueueJobState {
    match state {
        JobState::Ready => ModelQueueJobState::Ready,
        JobState::Claimed => ModelQueueJobState::Claimed,
    }
}

fn model_work_class_name(work_class: ModelQueueWorkClass) -> &'static str {
    match work_class {
        ModelQueueWorkClass::BuildSnapshot => "BuildSnapshot",
    }
}

fn normalize_model_error(err: ModelError) -> QueueMutationError {
    match err {
        ModelError::ClaimTokenMismatch { expected, actual } => {
            QueueMutationError::ClaimTokenMismatch { expected, actual }
        }
        other => QueueMutationError::Other(format!("{other:?}")),
    }
}

fn normalize_durable_complete_error(err: DurableWorkerMutationError) -> QueueMutationError {
    match err {
        DurableWorkerMutationError::Mutation(
            loon_queue::worker::WorkerMutationError::ClaimTokenMismatch { expected, actual },
        ) => QueueMutationError::ClaimTokenMismatch { expected, actual },
        other => QueueMutationError::Other(format!("{other:?}")),
    }
}

impl From<ModelQueueRepairOutcome> for NativeQueueOutcome {
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

impl From<DurableSnapshotRepairOutcome> for NativeQueueOutcome {
    fn from(value: DurableSnapshotRepairOutcome) -> Self {
        match value {
            DurableSnapshotRepairOutcome::NoChange => Self::Repair(RepairSnapshot::NoRepairNeeded),
            DurableSnapshotRepairOutcome::Created(repair)
            | DurableSnapshotRepairOutcome::Updated(repair) => Self::Repair(match repair.repair {
                loon_queue::repair::SnapshotRepairOutcome::NoRepairNeeded => {
                    RepairSnapshot::NoRepairNeeded
                }
                loon_queue::repair::SnapshotRepairOutcome::Enqueued { through_seq } => {
                    RepairSnapshot::Enqueued { through_seq }
                }
                loon_queue::repair::SnapshotRepairOutcome::RaisedReadyJob { through_seq } => {
                    RepairSnapshot::RaisedReadyJob { through_seq }
                }
                loon_queue::repair::SnapshotRepairOutcome::AttachedFollowUp { through_seq } => {
                    RepairSnapshot::AttachedFollowUp { through_seq }
                }
            }),
        }
    }
}

impl From<ModelBrokerLeaseOutcome> for NativeQueueOutcome {
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

impl From<DurableBrokerLeaseOutcome> for NativeQueueOutcome {
    fn from(value: DurableBrokerLeaseOutcome) -> Self {
        match value {
            DurableBrokerLeaseOutcome::Created(mutation)
            | DurableBrokerLeaseOutcome::Updated(mutation) => {
                Self::BrokerLease(match mutation.mutation {
                    loon_queue::broker::BrokerLeaseOutcome::Acquired { epoch } => {
                        BrokerLeaseSnapshot::Acquired { epoch }
                    }
                    loon_queue::broker::BrokerLeaseOutcome::Renewed { epoch } => {
                        BrokerLeaseSnapshot::Renewed { epoch }
                    }
                    loon_queue::broker::BrokerLeaseOutcome::TakenOver { epoch } => {
                        BrokerLeaseSnapshot::TakenOver { epoch }
                    }
                })
            }
        }
    }
}

impl From<ModelJobClaimOutcome> for NativeQueueOutcome {
    fn from(value: ModelJobClaimOutcome) -> Self {
        Self::Claim(match value {
            ModelJobClaimOutcome::Claimed { claim_token } => ClaimSnapshot::Claimed { claim_token },
            ModelJobClaimOutcome::Stolen { claim_token } => ClaimSnapshot::Stolen { claim_token },
        })
    }
}

impl From<DurableJobClaimOutcome> for NativeQueueOutcome {
    fn from(value: DurableJobClaimOutcome) -> Self {
        match value {
            DurableJobClaimOutcome::Created(mutation)
            | DurableJobClaimOutcome::Updated(mutation) => Self::Claim(match mutation.mutation {
                loon_queue::worker::JobClaimOutcome::Claimed { claim_token } => {
                    ClaimSnapshot::Claimed { claim_token }
                }
                loon_queue::worker::JobClaimOutcome::Stolen { claim_token } => {
                    ClaimSnapshot::Stolen { claim_token }
                }
            }),
        }
    }
}

impl From<ModelJobCompleteOutcome> for NativeQueueOutcome {
    fn from(value: ModelJobCompleteOutcome) -> Self {
        Self::Complete(match value {
            ModelJobCompleteOutcome::Removed => CompleteSnapshot::Removed,
            ModelJobCompleteOutcome::PromotedFollowUp { through_seq } => {
                CompleteSnapshot::PromotedFollowUp { through_seq }
            }
        })
    }
}

impl From<DurableJobCompleteOutcome> for NativeQueueOutcome {
    fn from(value: DurableJobCompleteOutcome) -> Self {
        match value {
            DurableJobCompleteOutcome::Created(mutation)
            | DurableJobCompleteOutcome::Updated(mutation) => {
                Self::Complete(match mutation.mutation {
                    loon_queue::worker::JobCompleteOutcome::Removed => CompleteSnapshot::Removed,
                    loon_queue::worker::JobCompleteOutcome::PromotedFollowUp { through_seq } => {
                        CompleteSnapshot::PromotedFollowUp { through_seq }
                    }
                })
            }
        }
    }
}

type TestDir = loon_testkit::tempdir::TestDir;
