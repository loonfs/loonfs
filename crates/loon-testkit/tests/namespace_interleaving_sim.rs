use loon_client::executor::build_client_mutation_request_from_state;
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    ObservedRemoteInode, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::testing::{ClientExecutionHooks, FaultController};
use loon_core::checkpoint::{
    load_checkpoint, prepare_checkpoint, prepare_checkpoint_head_publish, publish_checkpoint_head,
    CheckpointHeadPublishRequest, CheckpointPublishError, PreparedCheckpoint,
    StoredCheckpointManifest, StoredCheckpointSegment,
};
use loon_core::commit::{
    build_commit_plan, CommitOp, CommitRequest, CommitValidationContext, CommitValidationError,
    Precondition,
};
use loon_core::metadata::MetadataState;
use loon_core::namespace::head_and_lease_fence_tokens_agree;
use loon_core::progress::{
    load_retention_authorizers, publish_progress, read_progress_object, ProgressPublishOutcome,
};
use loon_model::{
    ModelAction, ModelCheckpoint, ModelCheckpointPublishAuthorizers, ModelCommitValidationError,
    ModelCommitValidationRequest, ModelError, ModelNamespace, ModelProgressObject,
    ModelQueueBroker, ModelQueueClaim, ModelQueueJob, ModelQueueJobState, ModelQueueRepairOutcome,
    ModelQueueSeqPayload, ModelQueueShard, ModelQueueWorkClass,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{derived_progress, namespace_head, namespace_lease, queue_shard};
use loon_objectstore::ObjectStore;
use loon_queue::durable::{read_queue_shard, repair_lost_snapshot_enqueue_in_store};
use loon_queue::types::{
    JobState, QueueBroker, QueueClaim, QueueJob, QueueShardEnvelope, QueueShardState,
    SeqScopedPayload, WorkClass,
};
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_sim::SimRuntime;
use loon_testkit::client::seed_server_basis_for_request;
use loon_testkit::fixtures::load_fixture;
use loon_testkit::invariants::{
    evaluate_background_checkpoint_publish_invariants, evaluate_background_repair_invariants,
    evaluate_background_stale_writer_invariants, evaluate_checkpoint_head_publish_invariants,
    evaluate_client_retry_reuse_invariants, evaluate_duplicate_response_invariants,
    evaluate_late_remote_observation_invariants,
    evaluate_namespace_checkpoint_latest_head_invariants,
    evaluate_namespace_repair_latest_head_invariants,
    evaluate_namespace_stale_writer_inflight_request_invariants,
    evaluate_progress_publish_invariants, evaluate_queue_repair_invariants,
    evaluate_queue_shard_object_invariants, evaluate_response_after_newer_observation_invariants,
    evaluate_unified_namespace_sim_trace_determinism_invariants,
    BackgroundCheckpointPublishInvariantInputs, BackgroundRepairInvariantInputs,
    BackgroundStaleWriterInvariantInputs, BackgroundWorkInvariantReport,
    CheckpointHeadPublishInvariantInputs, CheckpointProgressAuthorizer,
    ClientRetryReuseInvariantInputs, DuplicateResponseInvariantInputs,
    LateRemoteObservationInvariantInputs, NamespaceCheckpointLatestHeadInvariantInputs,
    NamespaceRepairLatestHeadInvariantInputs, NamespaceStaleWriterInflightRequestInvariantInputs,
    ProgressInvariantSnapshot, ProgressPublishInvariantInputs, ProgressPublishOutcomeKind,
    QueueRepairInvariantInputs, QueueRepairOutcomeKind, QueueShardObjectInvariantInputs,
    ResponseAfterNewerObservationInvariantInputs,
    UnifiedNamespaceSimTraceDeterminismInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_testkit::tempdir::TestDir;
use loon_types::{
    ChangeSeq, ClientMutationResponse, ControlObjectEnvelope, ControlObjectKind, FenceToken,
    HeadState, HeadStateEnvelope, InodeId, LeaseState, LeaseStateEnvelope, NamespaceId,
    ProgressState,
};
use serde::Deserialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs;

const TEST_WRITER_VERSION: &str = "loon-testkit-sim";

#[test]
fn delayed_response_after_newer_authoritative_observation_is_idempotent() {
    let report =
        run_namespace_fixture_report("sim/namespace_delayed_response_after_newer_observation.yaml");
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_delayed_response_after_newer_observation.txt"
        )
    );
}

#[test]
fn checkpoint_publish_uses_latest_head_after_client_server_advance() {
    let report = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.txt"
        )
    );
}

#[test]
fn snapshot_repair_uses_latest_head_after_client_server_advance() {
    let report = run_namespace_fixture_report(
        "sim/namespace_repair_uses_latest_head_after_client_server_advance.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_repair_uses_latest_head_after_client_server_advance.txt"
        )
    );
}

#[test]
fn stale_writer_fence_survives_inflight_client_request() {
    let report = run_namespace_fixture_report(
        "sim/namespace_stale_writer_fence_survives_inflight_client_request.yaml",
    );
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/sim-interleavings/sim/namespace_stale_writer_fence_survives_inflight_client_request.txt"
        )
    );
}

#[test]
fn unified_namespace_sim_trace_order_is_seed_stable_for_checkpoint_fixture() {
    let first = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    let second = run_namespace_fixture_report(
        "sim/namespace_checkpoint_publish_uses_latest_head_after_client_server_advance.yaml",
    );
    let report = evaluate_unified_namespace_sim_trace_determinism_invariants(
        UnifiedNamespaceSimTraceDeterminismInvariantInputs {
            first_rendered_trace: &first.rendered_trace,
            second_rendered_trace: &second.rendered_trace,
        },
    );

    assert!(
        report
            .check("unified_namespace_sim_trace_order_is_seed_stable")
            .expect("check should exist")
            .passed
    );
}

struct NamespaceFixtureRunReport {
    rendered_trace: String,
}

fn run_namespace_fixture_report(relative_path: &str) -> NamespaceFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let initial: NamespaceSimInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<NamespaceSimActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: NamespaceSimExpect = scenario.decode_expect().expect("decode expectations");

    validate_initial_namespace_alignment(&initial);

    let temp_dir = TestDir::new("namespace-interleaving-sim");
    let db_path = temp_dir.path().join("client.sqlite3");
    let mirror_root = temp_dir.path().join("mirror");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&mirror_root).expect("create mirror root");
    fs::create_dir_all(&store_root).expect("create local objectstore root");
    let store = LocalFsStore::new(&store_root).expect("create local objectstore");

    seed_client_state(&db_path, &initial.client_state);
    seed_head_and_lease(
        &store,
        initial.client_state.head.as_ref(),
        initial.client_state.lease.as_ref(),
    );
    seed_progress_objects(&store, &initial.progress_objects);
    if let Some(queue_shard_fixture) = &initial.queue_shard {
        overwrite_queue_shard(
            &store,
            &queue_shard_state_from_fixture(&queue_shard_fixture.payload),
        );
    }

    let mut runtime = SimRuntime::new();
    let mut trace = Vec::new();
    let mut observed_invariants = Vec::<String>::new();
    let mut request_deliveries = BTreeMap::<u64, ClientMutationResponseOrRequest>::new();
    let mut response_deliveries = BTreeMap::<u64, ClientMutationResponse>::new();
    let mut observation_deliveries = BTreeMap::<u64, QueuedObservation>::new();
    let mut server_inbox = VecDeque::<ClientMutationResponseOrRequest>::new();
    let controller = FaultController::new(scenario.decode_fault_plan().expect("decode faults"));
    let mut client_generation = 0_u64;

    let mut core_head = initial
        .client_state
        .head
        .clone()
        .expect("unified namespace sim requires initial.client_state.head");
    let mut lease = initial
        .client_state
        .lease
        .clone()
        .expect("unified namespace sim requires initial.client_state.lease");
    let mut metadata_state = initial.metadata_state.clone();
    let mut model_namespace = model_namespace_from_head(&core_head, &metadata_state);
    let mut model_progress = initial
        .progress_objects
        .iter()
        .map(|progress| {
            (
                progress.payload.work_class.clone(),
                ModelProgressObject {
                    namespace_id: progress.payload.namespace_id.clone(),
                    work_class: progress.payload.work_class.clone(),
                    through_seq: progress.payload.through_seq,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut model_queue = initial
        .queue_shard
        .as_ref()
        .map(|fixture| model_queue_from_fixture(&fixture.payload));
    let mut cached_writer_view = None::<WriterView>;
    let mut prepared_checkpoint = None::<PreparedCheckpoint>;
    let mut prepared_model_checkpoint = None::<ModelCheckpoint>;
    let mut checkpoint_publish_history = Vec::<CheckpointPublishAttempt>::new();
    let mut latest_client_server_head_seq = None::<ChangeSeq>;
    let mut last_repair_tracking = None::<RepairTracking>;
    let mut last_successful_checkpoint_snapshot_hint = None::<Option<ChangeSeq>>;

    let mut delayed_retry_before_restart = None::<String>;
    let mut delayed_retry_after_retry = None::<String>;
    let mut delayed_retry_retried_request = None::<String>;
    let mut duplicate_second_apply_clean = false;
    let mut duplicate_winner_apply_count = 0_u64;
    let mut late_observation_response_ok = false;
    let mut late_observation_apply_ok = false;
    let mut late_observation_winner_apply_count = 0_u64;
    let mut newer_observation_apply_ok = false;
    let mut delayed_response_apply_ok = false;
    let mut delayed_response_changed_state = false;
    let mut newer_observation_winner_apply_count = 0_u64;
    let mut stale_publish_rejected = false;
    let mut inflight_client_request_present = false;
    let mut delayed_client_response_converged = false;

    runtime.record_actor_step(
        "fixture",
        format!(
            "initial client_state={} seeded_network_responses={} seeded_observations={} progress_objects={} queue_shard_present={} metadata_rows=(inodes={}, direntries={}, revisions={}, tombstones={}) mirror_root_ready={}",
            initial.client_state.describe(),
            initial.network.queued_responses.len(),
            initial.remote_observations.len() + initial.network.queued_remote_observations.len(),
            initial.progress_objects.len(),
            initial.queue_shard.is_some(),
            metadata_state.inodes.len(),
            metadata_state.direntries.len(),
            metadata_state.revisions.len(),
            metadata_state.subtree_tombstones.len(),
            mirror_root.exists(),
        ),
    );
    push_latest_runtime_trace_line(&mut trace, &runtime);

    for response in &initial.network.queued_responses {
        let delivery = runtime.enqueue_delivery("seed", "client", "client_response");
        push_latest_runtime_trace_line(&mut trace, &runtime);
        response_deliveries.insert(delivery.delivery_id, response.clone());
    }
    for observation in initial
        .remote_observations
        .iter()
        .chain(initial.network.queued_remote_observations.iter())
    {
        let delivery = runtime.enqueue_delivery("seed", "client", "remote_observation");
        push_latest_runtime_trace_line(&mut trace, &runtime);
        observation_deliveries.insert(delivery.delivery_id, observation.clone());
    }

    for (index, action) in actions.iter().enumerate() {
        match action.kind() {
            NamespaceSimActionRef::ClientTick(action) => {
                let mut db = SqliteStateDb::open(&db_path).expect("open client DB");
                let planned = db
                    .load_next_planned_local_only_action()
                    .expect("load next planned local-only action");
                let Some(planned) = planned else {
                    runtime.record_actor_step("client", "tick result=no_action");
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    continue;
                };
                assert_eq!(
                    planned.decision, "create_remote_dir",
                    "namespace sim currently supports local-only create_remote_dir only"
                );
                let existing_pending = db
                    .load_pending_client_mutation_for_client_file(&planned.client_file_id)
                    .expect("load pending client mutation for local-only state");
                if client_generation > 0 && delayed_retry_before_restart.is_none() {
                    delayed_retry_before_restart = existing_pending
                        .as_ref()
                        .map(|pending| pending.client_request_id.clone());
                }
                let pending = match existing_pending {
                    Some(pending) => pending,
                    None => {
                        let request_id = db
                            .allocate_client_request_id()
                            .expect("allocate client request id");
                        let request = build_client_mutation_request_from_state(
                            &db,
                            &request_id,
                            &planned.client_file_id,
                        )
                        .expect("build client mutation request from state");
                        db.record_pending_client_mutation(
                            &planned.client_file_id,
                            &request,
                            action.created_at_ms,
                        )
                        .expect("record pending client mutation")
                    }
                };
                let request = pending.request.clone();
                if client_generation > 0 {
                    delayed_retry_after_retry = Some(pending.client_request_id.clone());
                }
                if let Some(message) = controller.dispatch_error("dispatch.client.before_request") {
                    runtime.record_fault(format!(
                        "dispatch_error_once checkpoint_id=dispatch.client.before_request message={}",
                        message
                    ));
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    runtime.record_actor_step(
                        "client",
                        format!(
                            "tick result=dispatch_failed request_id={} message={}",
                            request.client_request_id, message
                        ),
                    );
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    continue;
                }

                let outstanding_request = request_deliveries
                    .values()
                    .any(|queued| queued.client_request_id() == request.client_request_id)
                    || server_inbox
                        .iter()
                        .any(|queued| queued.client_request_id() == request.client_request_id);
                let outstanding_response = response_deliveries
                    .values()
                    .any(|response| response.client_request_id == request.client_request_id);
                let result = if outstanding_request || outstanding_response {
                    "reused_pending_request_waiting"
                } else {
                    let delivery = runtime.enqueue_delivery("client", "server", "client_request");
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    request_deliveries.insert(
                        delivery.delivery_id,
                        ClientMutationResponseOrRequest::Request(request.clone()),
                    );
                    delayed_retry_retried_request = Some(request.client_request_id.clone());
                    "enqueued_request"
                };
                runtime.record_actor_step(
                    "client",
                    format!(
                        "tick result={} request_id={} generation={}",
                        result, request.client_request_id, client_generation
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::DeliverNextRequest => {
                let delivery = runtime
                    .take_next_matching_delivery(|delivery| {
                        delivery.kind == "client_request" && delivery.recipient.as_str() == "server"
                    })
                    .expect("client request delivery should exist");
                push_latest_runtime_trace_line(&mut trace, &runtime);
                let queued = request_deliveries
                    .remove(&delivery.delivery_id)
                    .expect("queued request payload should exist");
                let request = queued.into_request().expect("request payload");
                server_inbox.push_back(ClientMutationResponseOrRequest::Request(request.clone()));
                runtime.record_actor_step(
                    "network",
                    format!(
                        "deliver_next_request request_id={} delivery_id={}",
                        request.client_request_id, delivery.delivery_id
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::ServerHandleNextRequest(action) => {
                let request = server_inbox
                    .pop_front()
                    .expect("server inbox should contain a request")
                    .into_request()
                    .expect("server inbox request payload");
                let writer_id = action
                    .writer_id
                    .clone()
                    .or_else(|| {
                        initial
                            .server_basis
                            .as_ref()
                            .map(|basis| basis.writer_id.clone())
                    })
                    .unwrap_or_else(|| "writer-a".to_owned());
                let writer_version = action
                    .writer_version
                    .clone()
                    .or_else(|| {
                        initial
                            .server_basis
                            .as_ref()
                            .map(|basis| basis.writer_version.clone())
                    })
                    .unwrap_or_else(|| "loon-server-test".to_owned());

                if metadata_state == MetadataState::default() {
                    seed_server_basis_for_request(&store, &request, &writer_version);
                } else {
                    seed_verified_basis(&store, &core_head, &metadata_state, &writer_version);
                }

                let executed = execute_client_mutation(
                    &store,
                    &request,
                    &ClientMutationExecutionParams {
                        writer_id: writer_id.clone(),
                        writer_version: writer_version.clone(),
                        now_ms: action.now_ms,
                    },
                )
                .expect("execute server mutation");
                let response = executed.response.clone();
                metadata_state = executed.resulting_metadata_state.clone();
                core_head = executed.head_publish.resulting_head.clone();
                model_namespace = model_namespace_from_head(&core_head, &metadata_state);
                latest_client_server_head_seq = Some(core_head.seq);

                let response_delivery =
                    runtime.enqueue_delivery("server", "client", "client_response");
                push_latest_runtime_trace_line(&mut trace, &runtime);
                response_deliveries.insert(response_delivery.delivery_id, response.clone());
                if let Some(observation) = observed_from_response(&response) {
                    let observation_delivery =
                        runtime.enqueue_delivery("server", "client", "remote_observation");
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    observation_deliveries.insert(
                        observation_delivery.delivery_id,
                        QueuedObservation {
                            remote_observation: observation,
                            applied_at_ms: action.now_ms,
                        },
                    );
                }
                runtime.record_actor_step(
                    "server",
                    format!(
                        "handle_next_request request_id={} committed_seq={} writer_id={} writer_version={} resulting_head=(seq={}, fence={}, next_inode={})",
                        request.client_request_id,
                        response.committed_seq.0,
                        writer_id,
                        writer_version,
                        core_head.seq.0,
                        core_head.active_fence_token.0,
                        core_head.next_inode_id.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::DeliverNextResponse => {
                let delivery = runtime
                    .take_next_matching_delivery(|delivery| {
                        delivery.kind == "client_response"
                            && delivery.recipient.as_str() == "client"
                    })
                    .expect("client response delivery should exist");
                push_latest_runtime_trace_line(&mut trace, &runtime);
                let response = response_deliveries
                    .remove(&delivery.delivery_id)
                    .expect("queued response payload should exist");
                let mut db = SqliteStateDb::open(&db_path).expect("open client DB");
                let before = response_target(&response).map(|(namespace_id, inode_id)| {
                    db.load_file_sync_views(&namespace_id, inode_id)
                        .expect("load response target before apply")
                });
                let apply_result = if response.created_inode.is_some() {
                    db.apply_client_mutation_response(&response)
                        .map(|bound| format!("bound inode_id={}", bound.inode_id.0))
                } else {
                    db.apply_inode_mutation_response(&response)
                        .map(|applied| format!("applied inode_id={}", applied.inode_id.0))
                };
                let outcome_detail = match apply_result {
                    Ok(detail) => detail,
                    Err(error) => panic!("deliver_next_response should succeed: {error}"),
                };
                let after = response_target(&response).map(|(namespace_id, inode_id)| {
                    db.load_file_sync_views(&namespace_id, inode_id)
                        .expect("load response target after apply")
                });
                let changed = before != after;
                if changed && response.created_inode.is_some() {
                    late_observation_winner_apply_count =
                        late_observation_winner_apply_count.saturating_add(1);
                    newer_observation_winner_apply_count =
                        newer_observation_winner_apply_count.saturating_add(1);
                } else if changed {
                    duplicate_winner_apply_count = duplicate_winner_apply_count.saturating_add(1);
                }
                delayed_response_apply_ok = true;
                delayed_response_changed_state = changed;
                if response.replaced_file.is_some() {
                    duplicate_second_apply_clean = true;
                } else {
                    late_observation_response_ok = true;
                }
                delayed_client_response_converged = !db
                    .load_next_planned_local_only_action()
                    .expect("load next planned local-only action after response")
                    .and_then(|planned| {
                        db.load_pending_client_mutation_for_client_file(&planned.client_file_id)
                            .expect("load pending client mutation after response")
                    })
                    .is_some();
                runtime.record_actor_step(
                    "client",
                    format!(
                        "deliver_next_response request_id={} changed={} {}",
                        response.client_request_id, changed, outcome_detail
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::DeliverRemoteObservation(action) => {
                let delivery = runtime
                    .take_next_matching_delivery(|delivery| {
                        delivery.kind == "remote_observation"
                            && delivery.recipient.as_str() == "client"
                    })
                    .expect("remote observation delivery should exist");
                push_latest_runtime_trace_line(&mut trace, &runtime);
                let seeded = observation_deliveries
                    .remove(&delivery.delivery_id)
                    .expect("queued observation should exist");
                let applied_at_ms = action.applied_at_ms.unwrap_or(seeded.applied_at_ms);
                let mut db = SqliteStateDb::open(&db_path).expect("open client DB");
                let before = db
                    .load_file_sync_views(
                        &seeded.remote_observation.namespace_id,
                        seeded.remote_observation.inode_id,
                    )
                    .expect("load views before observation");
                let applied = db
                    .apply_remote_observation(&seeded.remote_observation, applied_at_ms)
                    .expect("apply remote observation");
                let after = db
                    .load_file_sync_views(
                        &seeded.remote_observation.namespace_id,
                        seeded.remote_observation.inode_id,
                    )
                    .expect("load views after observation");
                if before != after {
                    late_observation_winner_apply_count =
                        late_observation_winner_apply_count.saturating_add(1);
                    newer_observation_winner_apply_count =
                        newer_observation_winner_apply_count.saturating_add(1);
                }
                late_observation_apply_ok = matches!(
                    applied,
                    AppliedRemoteObservation::BoundLocalOnly { .. }
                        | AppliedRemoteObservation::ConvergedBoundInode { .. }
                        | AppliedRemoteObservation::UpdatedBoundRemoteState { .. }
                );
                newer_observation_apply_ok = late_observation_apply_ok;
                runtime.record_actor_step(
                    "client",
                    format!(
                        "deliver_remote_observation inode_id={} applied={:?} applied_at_ms={}",
                        seeded.remote_observation.inode_id.0, applied, applied_at_ms
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::RestartClient => {
                client_generation = client_generation.saturating_add(1);
                runtime.record_restart("client");
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::WriterReadCurrentHead => {
                let view = WriterView {
                    namespace_id: core_head.namespace_id.clone(),
                    writer_id: lease.holder_id.clone(),
                    planned_head_seq: core_head.seq,
                    writer_fence_token: core_head.active_fence_token,
                };
                cached_writer_view = Some(view.clone());
                runtime.record_actor_step(
                    "stale_writer",
                    format!(
                        "step={} action={} cached_view=(writer_id={}, planned_head_seq={}, writer_fence_token={})",
                        index + 1,
                        action.describe(),
                        view.writer_id,
                        view.planned_head_seq.0,
                        view.writer_fence_token.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::LeaseHandover(action) => {
                lease = LeaseState {
                    namespace_id: lease.namespace_id.clone(),
                    holder_id: action.new_holder_id.clone(),
                    fence_token: action.new_fence_token,
                    lease_expires_at_ms: action.lease_expires_at_ms,
                };
                overwrite_lease(&store, &lease);
                runtime.record_actor_step(
                    "writer",
                    format!(
                        "step={} action={} resulting_lease=(holder={}, fence={}, expires_at_ms={})",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_lease_handover(action),
                        lease.holder_id,
                        lease.fence_token.0,
                        lease.lease_expires_at_ms
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::WriterPublishHead(action) => {
                apply_model_head_publish(&mut model_namespace, action);
                core_head = HeadState {
                    namespace_id: core_head.namespace_id.clone(),
                    seq: action.seq,
                    active_fence_token: action.active_fence_token,
                    next_inode_id: action.next_inode_id,
                    snapshot_hint_seq: core_head.snapshot_hint_seq,
                    retention_floor_seq: core_head.retention_floor_seq,
                };
                overwrite_head(&store, &core_head);
                runtime.record_actor_step(
                    "writer",
                    format!(
                        "step={} action={} resulting_head=(seq={}, fence={}, next_inode={})",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_writer_publish(action),
                        core_head.seq.0,
                        core_head.active_fence_token.0,
                        core_head.next_inode_id.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::WriterAttemptCommit(action) => {
                let view = cached_writer_view
                    .as_ref()
                    .expect("writer_attempt_commit requires writer_read_current_head first");
                let now_ms = runtime
                    .now_ms()
                    .max(lease.lease_expires_at_ms.saturating_sub(1));
                let model_outcome = model_namespace
                    .validate_commit_attempt(
                        &ModelCommitValidationRequest {
                            namespace_id: view.namespace_id.clone(),
                            writer_id: view.writer_id.clone(),
                            writer_fence_token: action.writer_fence_token,
                            planned_head_seq: action.planned_head_seq,
                        },
                        &lease,
                        now_ms,
                    )
                    .map(|outcome| CommitAttemptOutcome::Accepted {
                        next_seq: outcome.next_seq,
                    })
                    .unwrap_or_else(|err| {
                        CommitAttemptOutcome::Rejected(normalize_model_error(err))
                    });
                let core_outcome = build_commit_plan(
                    &CommitRequest {
                        namespace_id: view.namespace_id.clone(),
                        request_id: "namespace-sim-stale-writer".to_owned(),
                        writer_id: view.writer_id.clone(),
                        writer_fence_token: action.writer_fence_token,
                        planned_head_seq: action.planned_head_seq,
                        ops: vec![CommitOp::Rename {
                            inode_id: InodeId(42),
                            new_parent_inode: InodeId(2),
                            new_display_name: "renamed.txt".to_owned(),
                        }],
                        preconditions: vec![Precondition::HeadSeqIs(action.planned_head_seq)],
                    },
                    &CommitValidationContext {
                        head: core_head.clone(),
                        lease: lease.clone(),
                        now_ms,
                        metadata_state: metadata_state.clone(),
                    },
                )
                .map(|plan| CommitAttemptOutcome::Accepted {
                    next_seq: plan.next_seq,
                })
                .unwrap_or_else(|err| CommitAttemptOutcome::Rejected(normalize_core_error(err)));

                if model_outcome != core_outcome {
                    panic!(
                        "namespace sim stale-writer model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                stale_publish_rejected = matches!(core_outcome, CommitAttemptOutcome::Rejected(_));
                inflight_client_request_present = !request_deliveries.is_empty()
                    || !server_inbox.is_empty()
                    || !response_deliveries.is_empty()
                    || SqliteStateDb::open(&db_path)
                        .expect("open client DB for inflight check")
                        .load_next_planned_local_only_action()
                        .expect("load next planned local-only action for inflight check")
                        .and_then(|planned| {
                            SqliteStateDb::open(&db_path)
                                .expect("reopen client DB for pending check")
                                .load_pending_client_mutation_for_client_file(
                                    &planned.client_file_id,
                                )
                                .expect("load pending client mutation for inflight check")
                        })
                        .is_some();

                runtime.record_actor_step(
                    "stale_writer",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} now_ms={} head=(seq={}, fence={}) lease=(holder={}, fence={})",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_writer_attempt(action),
                        model_outcome,
                        core_outcome,
                        now_ms,
                        core_head.seq.0,
                        core_head.active_fence_token.0,
                        lease.holder_id,
                        lease.fence_token.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::CheckpointBuild => {
                let model_checkpoint = model_namespace.checkpoint();
                let core_checkpoint =
                    prepare_checkpoint(&core_head, &metadata_state, TEST_WRITER_VERSION)
                        .expect("build checkpoint");
                let model_snapshot = checkpoint_build_snapshot_from_model(&model_checkpoint);
                let core_snapshot = checkpoint_build_snapshot_from_core(&core_checkpoint);
                if model_snapshot != core_snapshot {
                    panic!(
                        "namespace sim checkpoint build model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }
                prepared_model_checkpoint = Some(model_checkpoint);
                prepared_checkpoint = Some(core_checkpoint);
                runtime.record_actor_step(
                    "checkpoint_builder",
                    format!(
                        "step={} action=checkpoint_build manifest_key={} segment_keys={:?}",
                        index + 1,
                        core_snapshot.manifest_key,
                        core_snapshot.segment_keys
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
            NamespaceSimActionRef::CheckpointPublish(action) => {
                let before_head = core_head.clone();
                let prepared_core = prepared_checkpoint
                    .as_ref()
                    .expect("checkpoint_publish requires a prior checkpoint_build action");
                let prepared_model = prepared_model_checkpoint
                    .as_ref()
                    .expect("checkpoint_publish requires a prior checkpoint_build action");
                let model_authorizers = checkpoint_model_authorizers(
                    &model_progress,
                    &action.required_progress_work_classes,
                    &action.retention_policy_work_class,
                );
                let mut model_after = model_namespace.clone();
                let model_outcome = model_after
                    .publish_checkpoint(
                        prepared_model,
                        &model_available_segment_keys(prepared_model),
                        action.requested_retention_floor_seq,
                        Some(&model_authorizers),
                    )
                    .map(|_| NormalizedCheckpointPublishOutcome::Published {
                        snapshot_hint_seq: model_after.snapshot_hint_seq,
                        retention_floor_seq: model_after.retention_floor_seq,
                    })
                    .unwrap_or_else(normalize_model_checkpoint_publish_error);

                let loaded_checkpoint = load_checkpoint(
                    &core_head.namespace_id,
                    &StoredCheckpointManifest {
                        object_key: prepared_core.manifest.object_key.clone(),
                        encoded_bytes: prepared_core.manifest.encoded_bytes.clone(),
                    },
                    &prepared_core
                        .segments
                        .iter()
                        .map(|segment| StoredCheckpointSegment {
                            object_key: segment.object_key.clone(),
                            encoded_bytes: segment.encoded_bytes.clone(),
                        })
                        .collect::<Vec<_>>(),
                )
                .expect("load prepared checkpoint");
                let retention_authorizers = load_retention_authorizers(
                    &store,
                    &core_head.namespace_id,
                    &action.required_progress_work_classes,
                    &action.retention_policy_work_class,
                )
                .expect("load retention authorizers");
                let core_outcome = prepare_checkpoint_head_publish(
                    &core_head,
                    &loaded_checkpoint,
                    &CheckpointHeadPublishRequest {
                        requested_retention_floor_seq: action.requested_retention_floor_seq,
                        retention_authorizers: Some(retention_authorizers.clone()),
                    },
                    TEST_WRITER_VERSION,
                )
                .and_then(|prepared_head| {
                    let etag = current_head_etag(&store, &core_head.namespace_id);
                    publish_checkpoint_head(&store, &etag, &prepared_head)?;
                    Ok(NormalizedCheckpointPublishOutcome::Published {
                        snapshot_hint_seq: prepared_head.resulting_head.snapshot_hint_seq,
                        retention_floor_seq: prepared_head.resulting_head.retention_floor_seq,
                    })
                })
                .unwrap_or_else(normalize_core_checkpoint_publish_error);

                if model_outcome != core_outcome {
                    panic!(
                        "namespace sim checkpoint publish model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                if let NormalizedCheckpointPublishOutcome::Published {
                    snapshot_hint_seq, ..
                } = core_outcome
                {
                    model_namespace = model_after;
                    core_head = read_head(&store, &core_head.namespace_id);
                    last_successful_checkpoint_snapshot_hint = Some(snapshot_hint_seq);
                }

                runtime.record_actor_step(
                    "checkpoint_publisher",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} resulting_head=(seq={}, snapshot_hint={:?}, retention_floor={})",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_checkpoint_publish(action),
                        model_outcome,
                        core_outcome,
                        core_head.seq.0,
                        core_head.snapshot_hint_seq.map(|seq| seq.0),
                        core_head.retention_floor_seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                if let NormalizedCheckpointPublishOutcome::Published { .. } = core_outcome {
                    let core_required = retention_authorizers
                        .required_progress
                        .iter()
                        .map(core_authorizer)
                        .collect::<Vec<_>>();
                    let checkpoint_report = evaluate_checkpoint_head_publish_invariants(
                        CheckpointHeadPublishInvariantInputs {
                            current_head: &before_head,
                            checkpoint_namespace: &loaded_checkpoint.manifest.payload.namespace_id,
                            checkpoint_seq: loaded_checkpoint.manifest.payload.checkpoint_seq,
                            checkpoint_verified: loaded_checkpoint.manifest.payload.verified,
                            checkpoint_segments_verified: loaded_checkpoint
                                .checked_invariants
                                .iter()
                                .any(|name| {
                                    name == "checkpoint_segment_descriptor_matches_payload"
                                }),
                            requested_retention_floor_seq: action.requested_retention_floor_seq,
                            required_progress: &core_required,
                            retention_policy: Some(core_authorizer(
                                &retention_authorizers.retention_policy,
                            )),
                            resulting_head: &core_head,
                        },
                    );
                    assert_background_report_passes(
                        &scenario,
                        &mut trace,
                        "checkpoint-publish",
                        &checkpoint_report,
                        index + 1,
                        &mut observed_invariants,
                    );
                }

                let after_head = core_head.clone();
                checkpoint_publish_history.push(CheckpointPublishAttempt {
                    before_head,
                    after_head,
                    outcome: core_outcome.clone(),
                });
            }
            NamespaceSimActionRef::PublishProgress(action) => {
                let before_model = model_progress
                    .get(&action.work_class)
                    .map(|progress| progress.through_seq);
                let expected_model = model_namespace
                    .publish_progress(
                        model_progress.get(&action.work_class),
                        &action.work_class,
                        action.through_seq,
                    )
                    .expect("model progress publish should succeed");
                let model_outcome = classify_model_progress_outcome(
                    before_model,
                    expected_model.through_seq,
                    action.through_seq,
                );
                model_progress.insert(action.work_class.clone(), expected_model);

                let core_outcome = publish_progress(
                    &store,
                    &action.namespace_id,
                    &action.work_class,
                    action.through_seq,
                    TEST_WRITER_VERSION,
                )
                .expect("core progress publish should succeed");
                let core_loaded =
                    read_progress_object(&store, &action.namespace_id, &action.work_class)
                        .expect("progress should load after publish");
                let normalized_core_outcome = normalize_core_progress_outcome(&core_outcome);

                if model_outcome != normalized_core_outcome {
                    panic!(
                        "namespace sim progress publish model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }

                runtime.record_actor_step(
                    "progress_publisher",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} resulting_through_seq={}",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_publish_progress(action),
                        model_outcome,
                        normalized_core_outcome,
                        core_loaded.envelope.state.through_seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                let progress_report =
                    evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
                        expected_namespace: &action.namespace_id,
                        expected_work_class: &action.work_class,
                        before_through_seq: before_model,
                        requested_through_seq: action.through_seq,
                        outcome: progress_outcome_kind(&normalized_core_outcome),
                        after_progress: &ProgressInvariantSnapshot {
                            object_key: derived_progress(
                                action.namespace_id.as_str(),
                                &action.work_class,
                            ),
                            namespace_id: core_loaded.envelope.state.namespace_id.clone(),
                            work_class: core_loaded.envelope.state.work_class.clone(),
                            through_seq: core_loaded.envelope.state.through_seq,
                            payload_checksum_valid: true,
                        },
                    });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "progress",
                    &progress_report,
                    index + 1,
                    &mut observed_invariants,
                );
            }
            NamespaceSimActionRef::RepairLostEnqueueToQueueShard(action) => {
                let queue_before = model_queue.clone().unwrap_or_else(|| ModelQueueShard {
                    work_class: ModelQueueWorkClass::BuildSnapshot,
                    shard_id: action.shard_id,
                    broker: None,
                    jobs: Vec::new(),
                });
                let mut next_model_queue = queue_before.clone();
                let progress = model_progress.get(WorkClass::BuildSnapshot.as_str());
                let model_outcome = model_namespace
                    .repair_lost_snapshot_enqueue(&mut next_model_queue, progress)
                    .map(NormalizedRepairOutcome::from)
                    .unwrap_or_else(|err| panic!("model repair should succeed: {err:?}"));
                let core_outcome = repair_lost_snapshot_enqueue_in_store(
                    &store,
                    action.shard_id,
                    &core_head,
                    read_progress_state_opt(
                        &store,
                        &core_head.namespace_id,
                        WorkClass::BuildSnapshot.as_str(),
                    )
                    .as_ref(),
                    TEST_WRITER_VERSION,
                )
                .map(NormalizedRepairOutcome::from)
                .unwrap_or_else(|err| panic!("core repair should succeed: {err:?}"));

                if model_outcome != core_outcome {
                    panic!(
                        "namespace sim repair model/core divergence at step {}:\n{}",
                        index + 1,
                        render_trace(&scenario, &trace)
                    );
                }
                model_queue = Some(next_model_queue);

                runtime.record_actor_step(
                    "repair",
                    format!(
                        "step={} action={} model_outcome={:?} core_outcome={:?} head_seq={}",
                        index + 1,
                        NamespaceSimActionEnvelope::describe_repair(action),
                        model_outcome,
                        core_outcome,
                        core_head.seq.0
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);

                let loaded_queue = read_queue_shard(&store, action.shard_id)
                    .expect("queue shard should load after repair");
                let queue_report = evaluate_queue_repair_invariants(QueueRepairInvariantInputs {
                    namespace_id: &action.namespace_id,
                    head_seq: core_head.seq,
                    progress_through_seq: read_progress_state_opt(
                        &store,
                        &core_head.namespace_id,
                        WorkClass::BuildSnapshot.as_str(),
                    )
                    .map(|progress| progress.through_seq),
                    outcome: queue_repair_outcome_kind(&core_outcome),
                    has_namespace_scoped_job_after: loaded_queue.envelope.state.jobs.iter().any(
                        |job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        },
                    ),
                    ready_job_through_seq_after: loaded_queue
                        .envelope
                        .state
                        .jobs
                        .iter()
                        .find(|job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        })
                        .filter(|job| matches!(job.state, JobState::Ready))
                        .map(|job| job.payload.through_seq),
                    follow_up_through_seq_after: loaded_queue
                        .envelope
                        .state
                        .jobs
                        .iter()
                        .find(|job| {
                            job.dedupe_key
                                == format!(
                                    "{}:{}",
                                    WorkClass::BuildSnapshot.as_str(),
                                    action.namespace_id
                                )
                        })
                        .and_then(|job| job.follow_up.as_ref().map(|payload| payload.through_seq)),
                });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "queue-repair",
                    &queue_report,
                    index + 1,
                    &mut observed_invariants,
                );
                let queue_object_report =
                    evaluate_queue_shard_object_invariants(QueueShardObjectInvariantInputs {
                        shard_index: action.shard_id,
                        payload_checksum_valid: true,
                        object_key: &loaded_queue.object_key,
                        actual_shard_id: loaded_queue.envelope.state.shard_id,
                        cas_protected: true,
                    });
                assert_background_report_passes(
                    &scenario,
                    &mut trace,
                    "queue-shard",
                    &queue_object_report,
                    index + 1,
                    &mut observed_invariants,
                );

                last_repair_tracking = Some(RepairTracking {
                    repaired_through_seq: core_outcome.repaired_through_seq(),
                    latest_visible_head_seq: core_head.seq,
                });
            }
            NamespaceSimActionRef::AdvanceTimeMs(delta_ms) => {
                runtime.advance_time(delta_ms);
                push_latest_runtime_trace_line(&mut trace, &runtime);
                runtime.record_actor_step(
                    "clock",
                    format!(
                        "step={} action=advance_time_ms(delta_ms={}) now_ms={}",
                        index + 1,
                        delta_ms,
                        runtime.now_ms()
                    ),
                );
                push_latest_runtime_trace_line(&mut trace, &runtime);
            }
        }
    }

    let mut sim_reports = Vec::new();
    if expect
        .invariants
        .iter()
        .any(|name| name == "client_retry_reuses_pending_request_after_delayed_response")
    {
        sim_reports.push(evaluate_client_retry_reuse_invariants(
            ClientRetryReuseInvariantInputs {
                pending_request_id_before_restart: delayed_retry_before_restart.as_deref(),
                pending_request_id_after_retry: delayed_retry_after_retry.as_deref(),
                retried_request_id: delayed_retry_retried_request.as_deref(),
                converged_once: expect.pending_client_mutation_present == Some(false),
            },
        ));
    }
    if expect
        .invariants
        .iter()
        .any(|name| name == "duplicate_response_is_idempotent")
    {
        sim_reports.push(evaluate_duplicate_response_invariants(
            DuplicateResponseInvariantInputs {
                winner_already_durable: true,
                second_delivery_applied_cleanly: duplicate_second_apply_clean,
                duplicate_winner_apply_count,
            },
        ));
    }
    if expect
        .invariants
        .iter()
        .any(|name| name == "late_remote_observation_does_not_duplicate_winner_apply")
    {
        sim_reports.push(evaluate_late_remote_observation_invariants(
            LateRemoteObservationInvariantInputs {
                response_apply_succeeded: late_observation_response_ok,
                observation_apply_succeeded: late_observation_apply_ok,
                winner_apply_count: late_observation_winner_apply_count,
            },
        ));
    }
    if expect
        .invariants
        .iter()
        .any(|name| name == "response_after_newer_observation_is_idempotent")
    {
        sim_reports.push(evaluate_response_after_newer_observation_invariants(
            ResponseAfterNewerObservationInvariantInputs {
                observation_apply_succeeded: newer_observation_apply_ok,
                delayed_response_applied_cleanly: delayed_response_apply_ok,
                delayed_response_changed_state,
                winner_apply_count: newer_observation_winner_apply_count,
            },
        ));
    }
    if expect.invariants.iter().any(|name| {
        name == "stale_writer_publish_remains_fenced_after_handover"
            || name == "stale_writer_fence_survives_inflight_client_request"
    }) {
        sim_reports.push(evaluate_background_stale_writer_invariants(
            BackgroundStaleWriterInvariantInputs {
                stale_publish_rejected,
                head_fence_matches_lease: head_and_lease_fence_tokens_agree(&core_head, &lease),
                stale_writer_fence_token: cached_writer_view
                    .as_ref()
                    .map(|view| view.writer_fence_token)
                    .unwrap_or(FenceToken(0)),
                active_fence_token: core_head.active_fence_token,
            },
        ));
        if expect
            .invariants
            .iter()
            .any(|name| name == "stale_writer_fence_survives_inflight_client_request")
        {
            sim_reports.push(evaluate_namespace_stale_writer_inflight_request_invariants(
                NamespaceStaleWriterInflightRequestInvariantInputs {
                    stale_publish_rejected,
                    inflight_client_request_present,
                    delayed_client_response_converged,
                },
            ));
        }
    }
    if expect.invariants.iter().any(|name| {
        name == "checkpoint_publish_waits_for_required_progress_under_interleaving"
            || name == "checkpoint_publish_preserves_monotonic_head_summary_under_interleaving"
    }) {
        assert!(
            checkpoint_publish_history.len() >= 2,
            "checkpoint publish interleaving invariants require at least two publish attempts:\n{}",
            render_trace(&scenario, &trace)
        );
        let first = &checkpoint_publish_history[0];
        let second = &checkpoint_publish_history[1];
        sim_reports.push(evaluate_background_checkpoint_publish_invariants(
            BackgroundCheckpointPublishInvariantInputs {
                first_publish_blocked: first.outcome.is_blocked_for_required_progress(),
                second_publish_succeeded: second.outcome.is_published(),
                snapshot_hint_before: first.before_head.snapshot_hint_seq,
                snapshot_hint_after_blocked: first.after_head.snapshot_hint_seq,
                snapshot_hint_after_success: second.after_head.snapshot_hint_seq,
                retention_floor_before: first.before_head.retention_floor_seq,
                retention_floor_after_blocked: first.after_head.retention_floor_seq,
                retention_floor_after_success: second.after_head.retention_floor_seq,
            },
        ));
    }
    if expect.invariants.iter().any(|name| {
        name == "repair_lost_enqueue_tracks_latest_visible_head_seq"
            || name == "repair_lost_enqueue_tracks_latest_visible_head_after_client_server_advance"
    }) {
        let repair_tracking = last_repair_tracking
            .expect("repair invariants require a prior repair_lost_enqueue_to_queue_shard action");
        if expect
            .invariants
            .iter()
            .any(|name| name == "repair_lost_enqueue_tracks_latest_visible_head_seq")
        {
            sim_reports.push(evaluate_background_repair_invariants(
                BackgroundRepairInvariantInputs {
                    repaired_through_seq: repair_tracking.repaired_through_seq,
                    latest_visible_head_seq: repair_tracking.latest_visible_head_seq,
                },
            ));
        }
        if expect.invariants.iter().any(|name| {
            name == "repair_lost_enqueue_tracks_latest_visible_head_after_client_server_advance"
        }) {
            sim_reports.push(evaluate_namespace_repair_latest_head_invariants(
                NamespaceRepairLatestHeadInvariantInputs {
                    repaired_through_seq: repair_tracking.repaired_through_seq,
                    latest_visible_head_seq: latest_client_server_head_seq.expect(
                        "latest client/server head seq should exist for repair latest-head invariant",
                    ),
                },
            ));
        }
    }
    if expect.invariants.iter().any(|name| {
        name == "checkpoint_publish_uses_latest_visible_head_after_client_server_advance"
    }) {
        sim_reports.push(evaluate_namespace_checkpoint_latest_head_invariants(
            NamespaceCheckpointLatestHeadInvariantInputs {
                published_snapshot_hint_seq: last_successful_checkpoint_snapshot_hint
                    .expect("successful checkpoint publish should exist"),
                latest_visible_head_seq: latest_client_server_head_seq.expect(
                    "latest client/server head seq should exist for checkpoint latest-head invariant",
                ),
            },
        ));
    }

    for report in &sim_reports {
        trace.extend(report.render_trace_lines("namespace-sim"));
        for check in &report.checks {
            if !check.passed {
                panic!(
                    "namespace sim invariant failed: {}:\n{}",
                    check.name,
                    render_trace(&scenario, &trace)
                );
            }
            add_invariant(&mut observed_invariants, &check.name);
        }
    }

    assert_client_final_expectations(&db_path, &expect, &trace, &scenario);
    assert_background_final_expectations(&store, &core_head, &expect, &trace, &scenario);

    for invariant in &expect.invariants {
        assert!(
            observed_invariants.iter().any(|value| value == invariant),
            "missing expected invariant `{invariant}`:\n{}",
            render_trace(&scenario, &trace)
        );
    }

    NamespaceFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

#[derive(Debug, Deserialize, Default)]
struct NamespaceSimInitial {
    #[serde(default)]
    client_state: ClientStateInitial,
    #[serde(default)]
    server_basis: Option<ServerBasisInitial>,
    #[serde(default)]
    metadata_state: MetadataState,
    #[serde(default)]
    progress_objects: Vec<FixtureProgressObject>,
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
    #[serde(default)]
    remote_observations: Vec<QueuedObservation>,
    #[serde(default)]
    network: ClientServerNetworkInitial,
}

#[derive(Debug, Deserialize, Default)]
struct NamespaceSimExpect {
    #[serde(default)]
    remote_state: Option<RemoteFileStateRow>,
    #[serde(default)]
    local_state: Option<LocalFileStateRow>,
    #[serde(default)]
    sync_anchor: Option<SyncAnchorRow>,
    #[serde(default)]
    planner_result: Option<PlannedActionRecord>,
    #[serde(default)]
    pending_client_mutation_present: Option<bool>,
    #[serde(default)]
    pending_inode_mutation_present: Option<bool>,
    #[serde(default)]
    head: Option<HeadState>,
    #[serde(default)]
    progress_objects: Vec<FixtureProgressObject>,
    #[serde(default)]
    queue_shard: Option<FixtureQueueShardObject>,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NamespaceSimActionEnvelope {
    #[serde(default)]
    client_tick: Option<ClientTickAction>,
    #[serde(default)]
    deliver_next_request: Option<bool>,
    #[serde(default)]
    server_handle_next_request: Option<ServerHandleNextRequestAction>,
    #[serde(default)]
    deliver_next_response: Option<bool>,
    #[serde(default)]
    deliver_remote_observation: Option<DeliverRemoteObservationAction>,
    #[serde(default)]
    restart_client: Option<bool>,
    #[serde(default)]
    writer_read_current_head: Option<bool>,
    #[serde(default)]
    lease_handover: Option<LeaseHandoverAction>,
    #[serde(default)]
    writer_publish_head: Option<PublishHeadAction>,
    #[serde(default)]
    writer_attempt_commit: Option<AttemptPublishAction>,
    #[serde(default)]
    checkpoint_build: Option<bool>,
    #[serde(default)]
    checkpoint_publish: Option<CheckpointPublishAction>,
    #[serde(default)]
    publish_progress: Option<PublishProgressAction>,
    #[serde(default)]
    repair_lost_enqueue_to_queue_shard: Option<RepairLostEnqueueToQueueShardAction>,
    #[serde(default)]
    advance_time_ms: Option<u64>,
}

impl NamespaceSimActionEnvelope {
    fn kind(&self) -> NamespaceSimActionRef<'_> {
        let mut matches = Vec::new();

        if let Some(action) = &self.client_tick {
            matches.push(NamespaceSimActionRef::ClientTick(action));
        }
        if self.deliver_next_request == Some(true) {
            matches.push(NamespaceSimActionRef::DeliverNextRequest);
        }
        if let Some(action) = &self.server_handle_next_request {
            matches.push(NamespaceSimActionRef::ServerHandleNextRequest(action));
        }
        if self.deliver_next_response == Some(true) {
            matches.push(NamespaceSimActionRef::DeliverNextResponse);
        }
        if let Some(action) = &self.deliver_remote_observation {
            matches.push(NamespaceSimActionRef::DeliverRemoteObservation(action));
        }
        if self.restart_client == Some(true) {
            matches.push(NamespaceSimActionRef::RestartClient);
        }
        if self.writer_read_current_head == Some(true) {
            matches.push(NamespaceSimActionRef::WriterReadCurrentHead);
        }
        if let Some(action) = &self.lease_handover {
            matches.push(NamespaceSimActionRef::LeaseHandover(action));
        }
        if let Some(action) = &self.writer_publish_head {
            matches.push(NamespaceSimActionRef::WriterPublishHead(action));
        }
        if let Some(action) = &self.writer_attempt_commit {
            matches.push(NamespaceSimActionRef::WriterAttemptCommit(action));
        }
        if self.checkpoint_build == Some(true) {
            matches.push(NamespaceSimActionRef::CheckpointBuild);
        }
        if let Some(action) = &self.checkpoint_publish {
            matches.push(NamespaceSimActionRef::CheckpointPublish(action));
        }
        if let Some(action) = &self.publish_progress {
            matches.push(NamespaceSimActionRef::PublishProgress(action));
        }
        if let Some(action) = &self.repair_lost_enqueue_to_queue_shard {
            matches.push(NamespaceSimActionRef::RepairLostEnqueueToQueueShard(action));
        }
        if let Some(delta_ms) = self.advance_time_ms {
            matches.push(NamespaceSimActionRef::AdvanceTimeMs(delta_ms));
        }

        assert_eq!(
            matches.len(),
            1,
            "namespace sim action envelope should contain exactly one action variant"
        );
        matches
            .into_iter()
            .next()
            .expect("one namespace sim action variant")
    }

    fn describe(&self) -> String {
        match self.kind() {
            NamespaceSimActionRef::ClientTick(action) => {
                format!("client_tick(created_at_ms={})", action.created_at_ms)
            }
            NamespaceSimActionRef::DeliverNextRequest => "deliver_next_request".to_owned(),
            NamespaceSimActionRef::ServerHandleNextRequest(action) => format!(
                "server_handle_next_request(now_ms={}, writer_id={:?}, writer_version={:?})",
                action.now_ms, action.writer_id, action.writer_version
            ),
            NamespaceSimActionRef::DeliverNextResponse => "deliver_next_response".to_owned(),
            NamespaceSimActionRef::DeliverRemoteObservation(action) => format!(
                "deliver_remote_observation(applied_at_ms={:?})",
                action.applied_at_ms
            ),
            NamespaceSimActionRef::RestartClient => "restart_client".to_owned(),
            NamespaceSimActionRef::WriterReadCurrentHead => "writer_read_current_head".to_owned(),
            NamespaceSimActionRef::LeaseHandover(action) => Self::describe_lease_handover(action),
            NamespaceSimActionRef::WriterPublishHead(action) => {
                Self::describe_writer_publish(action)
            }
            NamespaceSimActionRef::WriterAttemptCommit(action) => {
                Self::describe_writer_attempt(action)
            }
            NamespaceSimActionRef::CheckpointBuild => "checkpoint_build".to_owned(),
            NamespaceSimActionRef::CheckpointPublish(action) => {
                Self::describe_checkpoint_publish(action)
            }
            NamespaceSimActionRef::PublishProgress(action) => {
                Self::describe_publish_progress(action)
            }
            NamespaceSimActionRef::RepairLostEnqueueToQueueShard(action) => {
                Self::describe_repair(action)
            }
            NamespaceSimActionRef::AdvanceTimeMs(delta_ms) => {
                format!("advance_time_ms(delta_ms={delta_ms})")
            }
        }
    }

    fn describe_lease_handover(action: &LeaseHandoverAction) -> String {
        format!(
            "lease_handover(new_holder_id={}, new_fence_token={}, lease_expires_at_ms={})",
            action.new_holder_id, action.new_fence_token.0, action.lease_expires_at_ms
        )
    }

    fn describe_writer_publish(action: &PublishHeadAction) -> String {
        format!(
            "writer_publish_head(seq={}, active_fence_token={}, next_inode_id={})",
            action.seq.0, action.active_fence_token.0, action.next_inode_id.0
        )
    }

    fn describe_writer_attempt(action: &AttemptPublishAction) -> String {
        format!(
            "writer_attempt_commit(planned_head_seq={}, writer_fence_token={})",
            action.planned_head_seq.0, action.writer_fence_token.0
        )
    }

    fn describe_checkpoint_publish(action: &CheckpointPublishAction) -> String {
        format!(
            "checkpoint_publish(requested_retention_floor_seq={:?}, required_progress_work_classes={:?}, retention_policy_work_class={})",
            action.requested_retention_floor_seq.map(|seq| seq.0),
            action.required_progress_work_classes,
            action.retention_policy_work_class
        )
    }

    fn describe_publish_progress(action: &PublishProgressAction) -> String {
        format!(
            "publish_progress(namespace_id={}, work_class={}, through_seq={})",
            action.namespace_id.as_str(),
            action.work_class,
            action.through_seq.0
        )
    }

    fn describe_repair(action: &RepairLostEnqueueToQueueShardAction) -> String {
        format!(
            "repair_lost_enqueue_to_queue_shard(shard_id={}, namespace_id={})",
            action.shard_id,
            action.namespace_id.as_str()
        )
    }
}

enum NamespaceSimActionRef<'a> {
    ClientTick(&'a ClientTickAction),
    DeliverNextRequest,
    ServerHandleNextRequest(&'a ServerHandleNextRequestAction),
    DeliverNextResponse,
    DeliverRemoteObservation(&'a DeliverRemoteObservationAction),
    RestartClient,
    WriterReadCurrentHead,
    LeaseHandover(&'a LeaseHandoverAction),
    WriterPublishHead(&'a PublishHeadAction),
    WriterAttemptCommit(&'a AttemptPublishAction),
    CheckpointBuild,
    CheckpointPublish(&'a CheckpointPublishAction),
    PublishProgress(&'a PublishProgressAction),
    RepairLostEnqueueToQueueShard(&'a RepairLostEnqueueToQueueShardAction),
    AdvanceTimeMs(u64),
}

#[derive(Debug, Deserialize, Default)]
struct ClientStateInitial {
    #[serde(default)]
    head: Option<HeadState>,
    #[serde(default)]
    lease: Option<LeaseState>,
    #[serde(default)]
    local_only_state: Option<LocalOnlyFileStateRow>,
    #[serde(default)]
    planned_local_only_action: Option<LocalOnlyPlannedActionRow>,
    #[serde(default)]
    remote_state: Option<RemoteFileStateRow>,
    #[serde(default)]
    local_state: Option<LocalFileStateRow>,
    #[serde(default)]
    sync_anchor: Option<SyncAnchorRow>,
}

impl ClientStateInitial {
    fn describe(&self) -> String {
        format!(
            "local_only_present={} planned_local_only_present={} bound_present={}",
            self.local_only_state.is_some(),
            self.planned_local_only_action.is_some(),
            self.remote_state.is_some() && self.local_state.is_some() && self.sync_anchor.is_some()
        )
    }
}

#[derive(Debug, Deserialize, Default)]
struct ServerBasisInitial {
    writer_id: String,
    writer_version: String,
}

#[derive(Debug, Deserialize, Default)]
struct ClientServerNetworkInitial {
    #[serde(default)]
    queued_responses: Vec<ClientMutationResponse>,
    #[serde(default)]
    queued_remote_observations: Vec<QueuedObservation>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueuedObservation {
    remote_observation: ObservedRemoteInode,
    applied_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ClientTickAction {
    created_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ServerHandleNextRequestAction {
    now_ms: u64,
    #[serde(default)]
    writer_id: Option<String>,
    #[serde(default)]
    writer_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeliverRemoteObservationAction {
    #[serde(default)]
    applied_at_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LeaseHandoverAction {
    new_holder_id: String,
    new_fence_token: FenceToken,
    lease_expires_at_ms: u64,
}

#[derive(Debug, Deserialize)]
struct PublishHeadAction {
    seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
}

#[derive(Debug, Deserialize)]
struct AttemptPublishAction {
    planned_head_seq: ChangeSeq,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Deserialize)]
struct CheckpointPublishAction {
    requested_retention_floor_seq: Option<ChangeSeq>,
    required_progress_work_classes: Vec<String>,
    retention_policy_work_class: String,
}

#[derive(Debug, Deserialize)]
struct PublishProgressAction {
    namespace_id: NamespaceId,
    work_class: String,
    through_seq: ChangeSeq,
}

#[derive(Debug, Deserialize)]
struct RepairLostEnqueueToQueueShardAction {
    shard_id: u32,
    namespace_id: NamespaceId,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
struct FixtureProgressObject {
    key: String,
    payload: ProgressState,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardObject {
    key: String,
    payload: FixtureQueueShardPayload,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardPayload {
    work_class: WorkClass,
    shard_id: u32,
    #[serde(default)]
    broker: Option<QueueBroker>,
    #[serde(default)]
    jobs: Vec<FixtureQueueShardJob>,
}

#[derive(Debug, Deserialize, Clone)]
struct FixtureQueueShardJob {
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

#[derive(Debug, Clone)]
enum ClientMutationResponseOrRequest {
    Request(loon_types::ClientMutationRequest),
}

impl ClientMutationResponseOrRequest {
    fn client_request_id(&self) -> &str {
        match self {
            Self::Request(request) => &request.client_request_id,
        }
    }

    fn into_request(self) -> Option<loon_types::ClientMutationRequest> {
        match self {
            Self::Request(request) => Some(request),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriterView {
    namespace_id: NamespaceId,
    writer_id: String,
    planned_head_seq: ChangeSeq,
    writer_fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommitAttemptOutcome {
    Accepted { next_seq: ChangeSeq },
    Rejected(CommitAttemptError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommitAttemptError {
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointBuildSnapshot {
    manifest_key: String,
    checkpoint_seq: ChangeSeq,
    active_fence_token: FenceToken,
    next_inode_id: InodeId,
    retention_floor_seq: ChangeSeq,
    verified: bool,
    segment_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedCheckpointPublishOutcome {
    Published {
        snapshot_hint_seq: Option<ChangeSeq>,
        retention_floor_seq: ChangeSeq,
    },
    RequiredProgressLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    RetentionPolicyLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    Other(String),
}

impl NormalizedCheckpointPublishOutcome {
    fn is_published(&self) -> bool {
        matches!(self, Self::Published { .. })
    }

    fn is_blocked_for_required_progress(&self) -> bool {
        matches!(
            self,
            Self::RequiredProgressLag { .. } | Self::RetentionPolicyLag { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedProgressOutcome {
    Created { through_seq: ChangeSeq },
    Advanced { through_seq: ChangeSeq },
    NoChange { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedRepairOutcome {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

impl NormalizedRepairOutcome {
    fn repaired_through_seq(&self) -> Option<ChangeSeq> {
        match self {
            Self::NoRepairNeeded => None,
            Self::Enqueued { through_seq }
            | Self::RaisedReadyJob { through_seq }
            | Self::AttachedFollowUp { through_seq } => Some(*through_seq),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointPublishAttempt {
    before_head: HeadState,
    after_head: HeadState,
    outcome: NormalizedCheckpointPublishOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RepairTracking {
    repaired_through_seq: Option<ChangeSeq>,
    latest_visible_head_seq: ChangeSeq,
}

fn push_latest_runtime_trace_line(trace: &mut Vec<String>, runtime: &SimRuntime) {
    let line = runtime
        .trace()
        .last()
        .expect("runtime should contain at least one trace event")
        .render_line();
    trace.push(line);
}

fn add_invariant(observed_invariants: &mut Vec<String>, invariant: &str) {
    if !observed_invariants.iter().any(|value| value == invariant) {
        observed_invariants.push(invariant.to_owned());
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
                "namespace sim invariant failed at step {}: {}:\n{}",
                step,
                check.name,
                render_trace(scenario, trace)
            );
        }
        add_invariant(observed_invariants, &check.name);
    }
}

fn model_namespace_from_head(head: &HeadState, metadata_state: &MetadataState) -> ModelNamespace {
    let mut namespace = ModelNamespace::new(head.namespace_id.clone());
    namespace.head_seq = head.seq;
    namespace.active_fence_token = head.active_fence_token;
    namespace.next_inode_id = head.next_inode_id;
    namespace.snapshot_hint_seq = head.snapshot_hint_seq;
    namespace.retention_floor_seq = head.retention_floor_seq;
    namespace.metadata_state = loon_model::ModelMetadataState {
        inodes: metadata_state
            .inodes
            .iter()
            .map(|row| loon_model::ModelInodeRecord {
                inode_id: row.inode_id,
                inode_kind: row.inode_kind.clone(),
                created_seq: row.created_seq,
            })
            .collect(),
        direntries: metadata_state
            .direntries
            .iter()
            .map(|row| loon_model::ModelDirentryRecord {
                parent_inode_id: row.parent_inode_id,
                name_key: row.name_key.clone(),
                display_name: row.display_name.clone(),
                child_inode_id: row.child_inode_id,
                bind_seq: row.bind_seq,
                bind_op_index: row.bind_op_index,
            })
            .collect(),
        revisions: metadata_state
            .revisions
            .iter()
            .map(|row| loon_model::ModelRevisionRecord {
                inode_id: row.inode_id,
                revision_no: row.revision_no,
                committed_seq: row.committed_seq,
                revision_op_index: row.revision_op_index,
                content_manifest_digest: row.content_manifest_digest.clone(),
            })
            .collect(),
        subtree_tombstones: metadata_state
            .subtree_tombstones
            .iter()
            .map(|row| loon_model::ModelSubtreeTombstoneRecord {
                root_inode_id: row.root_inode_id,
                tombstone_seq: row.tombstone_seq,
                tombstone_op_index: row.tombstone_op_index,
            })
            .collect(),
    };
    namespace
}

fn apply_model_head_publish(namespace: &mut ModelNamespace, action: &PublishHeadAction) {
    assert_eq!(
        action.seq,
        ChangeSeq(namespace.head_seq.0 + 1),
        "fixture head publish should advance seq by exactly one"
    );
    if namespace.active_fence_token != action.active_fence_token {
        namespace
            .apply(ModelAction::RotateFence {
                new_fence_token: action.active_fence_token,
            })
            .expect("model fence rotation should succeed");
    }
    if action.next_inode_id == namespace.next_inode_id {
        namespace
            .apply(ModelAction::BumpSeq {
                writer_fence_token: action.active_fence_token,
            })
            .expect("model seq bump should succeed");
    } else {
        namespace
            .apply(ModelAction::CreateDir {
                inode_id: InodeId(action.next_inode_id.0.saturating_sub(1)),
                writer_fence_token: action.active_fence_token,
            })
            .expect("model head publish should succeed");
    }
}

fn normalize_model_error(error: ModelCommitValidationError) -> CommitAttemptError {
    match error {
        ModelCommitValidationError::NamespaceMismatch => CommitAttemptError::NamespaceMismatch,
        ModelCommitValidationError::HeadLeaseNamespaceMismatch => {
            CommitAttemptError::HeadLeaseNamespaceMismatch
        }
        ModelCommitValidationError::HeadLeaseFenceMismatch { head, lease } => {
            CommitAttemptError::HeadLeaseFenceMismatch { head, lease }
        }
        ModelCommitValidationError::PlannedHeadSeqMismatch { expected, actual } => {
            CommitAttemptError::PlannedHeadSeqMismatch { expected, actual }
        }
        ModelCommitValidationError::StaleWriterFenceToken { expected, actual } => {
            CommitAttemptError::StaleWriterFenceToken { expected, actual }
        }
        ModelCommitValidationError::LeaseHolderMismatch { expected, actual } => {
            CommitAttemptError::LeaseHolderMismatch { expected, actual }
        }
        ModelCommitValidationError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        } => CommitAttemptError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        },
        ModelCommitValidationError::SeqOverflow => CommitAttemptError::SeqOverflow,
    }
}

fn normalize_core_error(error: CommitValidationError) -> CommitAttemptError {
    match error {
        CommitValidationError::NamespaceMismatch => CommitAttemptError::NamespaceMismatch,
        CommitValidationError::HeadLeaseNamespaceMismatch => {
            CommitAttemptError::HeadLeaseNamespaceMismatch
        }
        CommitValidationError::HeadLeaseFenceMismatch { head, lease } => {
            CommitAttemptError::HeadLeaseFenceMismatch { head, lease }
        }
        CommitValidationError::PlannedHeadSeqMismatch { expected, actual } => {
            CommitAttemptError::PlannedHeadSeqMismatch { expected, actual }
        }
        CommitValidationError::StaleWriterFenceToken { active, requested } => {
            CommitAttemptError::StaleWriterFenceToken {
                expected: active,
                actual: requested,
            }
        }
        CommitValidationError::LeaseHolderMismatch { expected, actual } => {
            CommitAttemptError::LeaseHolderMismatch { expected, actual }
        }
        CommitValidationError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        } => CommitAttemptError::LeaseExpired {
            lease_expires_at_ms,
            now_ms,
        },
        CommitValidationError::SeqOverflow => CommitAttemptError::SeqOverflow,
        other => panic!("unexpected core commit validation error: {other:?}"),
    }
}

fn checkpoint_build_snapshot_from_model(checkpoint: &ModelCheckpoint) -> CheckpointBuildSnapshot {
    CheckpointBuildSnapshot {
        manifest_key: loon_objectstore::keys::snapshot_manifest(
            checkpoint.namespace_id.as_str(),
            checkpoint.checkpoint_seq.0,
        ),
        checkpoint_seq: checkpoint.checkpoint_seq,
        active_fence_token: checkpoint.active_fence_token,
        next_inode_id: checkpoint.next_inode_id,
        retention_floor_seq: checkpoint.retention_floor_seq,
        verified: checkpoint.verified,
        segment_keys: checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect(),
    }
}

fn checkpoint_build_snapshot_from_core(checkpoint: &PreparedCheckpoint) -> CheckpointBuildSnapshot {
    CheckpointBuildSnapshot {
        manifest_key: checkpoint.manifest.object_key.clone(),
        checkpoint_seq: checkpoint.manifest.envelope.payload.checkpoint_seq,
        active_fence_token: checkpoint.manifest.envelope.payload.active_fence_token,
        next_inode_id: checkpoint.manifest.envelope.payload.next_inode_id,
        retention_floor_seq: checkpoint.manifest.envelope.payload.retention_floor_seq,
        verified: checkpoint.manifest.envelope.payload.verified,
        segment_keys: checkpoint
            .segments
            .iter()
            .map(|segment| segment.object_key.clone())
            .collect(),
    }
}

fn normalize_model_checkpoint_publish_error(err: ModelError) -> NormalizedCheckpointPublishOutcome {
    match err {
        ModelError::RequiredProgressLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RequiredProgressLag {
            work_class,
            requested,
            available,
        },
        ModelError::RetentionPolicyLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RetentionPolicyLag {
            work_class,
            requested,
            available,
        },
        other => NormalizedCheckpointPublishOutcome::Other(format!("{other:?}")),
    }
}

fn normalize_core_checkpoint_publish_error(
    err: CheckpointPublishError,
) -> NormalizedCheckpointPublishOutcome {
    match err {
        CheckpointPublishError::RequiredProgressLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RequiredProgressLag {
            work_class,
            requested,
            available,
        },
        CheckpointPublishError::RetentionPolicyLag {
            work_class,
            requested,
            available,
        } => NormalizedCheckpointPublishOutcome::RetentionPolicyLag {
            work_class,
            requested,
            available,
        },
        other => NormalizedCheckpointPublishOutcome::Other(format!("{other:?}")),
    }
}

fn model_available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
    checkpoint
        .tables
        .iter()
        .flat_map(|table| {
            table
                .segments
                .iter()
                .map(|segment| segment.object_key.clone())
        })
        .collect()
}

fn checkpoint_model_authorizers(
    model_progress: &BTreeMap<String, ModelProgressObject>,
    required_progress_work_classes: &[String],
    retention_policy_work_class: &str,
) -> ModelCheckpointPublishAuthorizers {
    ModelCheckpointPublishAuthorizers {
        required_progress: required_progress_work_classes
            .iter()
            .map(|work_class| {
                model_progress
                    .get(work_class)
                    .unwrap_or_else(|| panic!("missing model progress for {work_class}"))
                    .clone()
            })
            .collect(),
        retention_policy: model_progress
            .get(retention_policy_work_class)
            .unwrap_or_else(|| {
                panic!("missing model retention policy progress for {retention_policy_work_class}")
            })
            .clone(),
    }
}

fn core_authorizer<'a>(
    progress: &'a loon_core::progress::LoadedProgressObject,
) -> CheckpointProgressAuthorizer<'a> {
    CheckpointProgressAuthorizer {
        namespace_id: &progress.envelope.state.namespace_id,
        work_class: &progress.envelope.state.work_class,
        through_seq: progress.envelope.state.through_seq,
    }
}

fn classify_model_progress_outcome(
    before_through_seq: Option<ChangeSeq>,
    after_through_seq: ChangeSeq,
    requested_through_seq: ChangeSeq,
) -> NormalizedProgressOutcome {
    match before_through_seq {
        None => NormalizedProgressOutcome::Created {
            through_seq: after_through_seq,
        },
        Some(before) if before >= requested_through_seq => NormalizedProgressOutcome::NoChange {
            through_seq: after_through_seq,
        },
        Some(_) => NormalizedProgressOutcome::Advanced {
            through_seq: after_through_seq,
        },
    }
}

fn normalize_core_progress_outcome(outcome: &ProgressPublishOutcome) -> NormalizedProgressOutcome {
    match outcome {
        ProgressPublishOutcome::Created(progress) => NormalizedProgressOutcome::Created {
            through_seq: progress.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::Advanced(progress) => NormalizedProgressOutcome::Advanced {
            through_seq: progress.progress.envelope.state.through_seq,
        },
        ProgressPublishOutcome::NoChange(progress) => NormalizedProgressOutcome::NoChange {
            through_seq: progress.envelope.state.through_seq,
        },
    }
}

fn progress_outcome_kind(outcome: &NormalizedProgressOutcome) -> ProgressPublishOutcomeKind {
    match outcome {
        NormalizedProgressOutcome::Created { .. } => ProgressPublishOutcomeKind::Created,
        NormalizedProgressOutcome::Advanced { .. } => ProgressPublishOutcomeKind::Advanced,
        NormalizedProgressOutcome::NoChange { .. } => ProgressPublishOutcomeKind::NoChange,
    }
}

fn queue_repair_outcome_kind(outcome: &NormalizedRepairOutcome) -> QueueRepairOutcomeKind {
    match outcome {
        NormalizedRepairOutcome::NoRepairNeeded => QueueRepairOutcomeKind::NoRepairNeeded,
        NormalizedRepairOutcome::Enqueued { through_seq } => QueueRepairOutcomeKind::Enqueued {
            through_seq: *through_seq,
        },
        NormalizedRepairOutcome::RaisedReadyJob { through_seq } => {
            QueueRepairOutcomeKind::RaisedReadyJob {
                through_seq: *through_seq,
            }
        }
        NormalizedRepairOutcome::AttachedFollowUp { through_seq } => {
            QueueRepairOutcomeKind::AttachedFollowUp {
                through_seq: *through_seq,
            }
        }
    }
}

impl From<ModelQueueRepairOutcome> for NormalizedRepairOutcome {
    fn from(value: ModelQueueRepairOutcome) -> Self {
        match value {
            ModelQueueRepairOutcome::NoRepairNeeded => Self::NoRepairNeeded,
            ModelQueueRepairOutcome::Enqueued { through_seq } => Self::Enqueued { through_seq },
            ModelQueueRepairOutcome::RaisedReadyJob { through_seq } => {
                Self::RaisedReadyJob { through_seq }
            }
            ModelQueueRepairOutcome::AttachedFollowUp { through_seq } => {
                Self::AttachedFollowUp { through_seq }
            }
        }
    }
}

impl From<loon_queue::durable::DurableSnapshotRepairOutcome> for NormalizedRepairOutcome {
    fn from(value: loon_queue::durable::DurableSnapshotRepairOutcome) -> Self {
        match value {
            loon_queue::durable::DurableSnapshotRepairOutcome::NoChange => Self::NoRepairNeeded,
            loon_queue::durable::DurableSnapshotRepairOutcome::Created(repair)
            | loon_queue::durable::DurableSnapshotRepairOutcome::Updated(repair) => {
                match repair.repair {
                    loon_queue::repair::SnapshotRepairOutcome::NoRepairNeeded => {
                        Self::NoRepairNeeded
                    }
                    loon_queue::repair::SnapshotRepairOutcome::Enqueued { through_seq } => {
                        Self::Enqueued { through_seq }
                    }
                    loon_queue::repair::SnapshotRepairOutcome::RaisedReadyJob { through_seq } => {
                        Self::RaisedReadyJob { through_seq }
                    }
                    loon_queue::repair::SnapshotRepairOutcome::AttachedFollowUp { through_seq } => {
                        Self::AttachedFollowUp { through_seq }
                    }
                }
            }
        }
    }
}

fn model_queue_from_fixture(fixture: &FixtureQueueShardPayload) -> ModelQueueShard {
    ModelQueueShard {
        work_class: match fixture.work_class {
            WorkClass::BuildSnapshot => ModelQueueWorkClass::BuildSnapshot,
            ref other => panic!("unsupported work class in namespace sim: {other:?}"),
        },
        shard_id: fixture.shard_id,
        broker: fixture.broker.as_ref().map(|broker| ModelQueueBroker {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
        }),
        jobs: fixture
            .jobs
            .iter()
            .map(|job| ModelQueueJob {
                job_id: fixture_job_id(job, &fixture.work_class),
                dedupe_key: job.dedupe_key.clone(),
                state: match job.state {
                    JobState::Ready => ModelQueueJobState::Ready,
                    JobState::Claimed => ModelQueueJobState::Claimed,
                },
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
        if job.dedupe_key == format!("{}:{}", work_class.as_str(), job.payload.namespace_id) {
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

fn overwrite_head(store: &LocalFsStore, head: &HeadState) {
    let envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        TEST_WRITER_VERSION,
        head.clone(),
    )
    .expect("build head envelope");
    store
        .put_overwrite(
            &namespace_head(head.namespace_id.as_str()),
            &serde_json::to_vec(&envelope).expect("encode head"),
        )
        .expect("write head");
}

fn overwrite_lease(store: &LocalFsStore, lease: &LeaseState) {
    let envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        TEST_WRITER_VERSION,
        lease.clone(),
    )
    .expect("build lease envelope");
    store
        .put_overwrite(
            &namespace_lease(lease.namespace_id.as_str()),
            &serde_json::to_vec(&envelope).expect("encode lease"),
        )
        .expect("write lease");
}

fn read_head(store: &LocalFsStore, namespace_id: &NamespaceId) -> HeadState {
    let bytes = store
        .get(&namespace_head(namespace_id.as_str()), None)
        .expect("read head bytes")
        .expect("head exists");
    let envelope: HeadStateEnvelope = serde_json::from_slice(&bytes).expect("decode head");
    envelope.state
}

fn current_head_etag(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    store
        .head(&namespace_head(namespace_id.as_str()))
        .expect("head metadata")
        .expect("head exists")
        .etag
        .expect("head etag")
}

fn seed_progress_objects(store: &LocalFsStore, progress_objects: &[FixtureProgressObject]) {
    for progress in progress_objects {
        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceProgress,
            TEST_WRITER_VERSION,
            progress.payload.clone(),
        )
        .expect("build progress envelope");
        store
            .put_if_absent(
                &progress.key,
                &serde_json::to_vec(&envelope).expect("encode progress"),
            )
            .expect("seed progress");
    }
}

fn overwrite_queue_shard(store: &LocalFsStore, state: &QueueShardState) {
    let envelope = QueueShardEnvelope::from_state(
        ControlObjectKind::QueueShard,
        TEST_WRITER_VERSION,
        state.clone(),
    )
    .expect("build queue shard envelope");
    store
        .put_overwrite(
            &queue_shard(state.shard_id),
            &serde_json::to_vec(&envelope).expect("encode queue shard"),
        )
        .expect("write queue shard");
}

fn read_progress_state_opt(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    work_class: &str,
) -> Option<ProgressState> {
    match store.get(&derived_progress(namespace_id.as_str(), work_class), None) {
        Ok(Some(bytes)) => {
            let envelope: ControlObjectEnvelope<ProgressState> =
                serde_json::from_slice(&bytes).expect("decode progress");
            Some(envelope.state)
        }
        Ok(None) => None,
        Err(err) => panic!("read progress object failed: {err:?}"),
    }
}

fn seed_verified_basis(
    store: &LocalFsStore,
    head: &HeadState,
    metadata_state: &MetadataState,
    writer_version: &str,
) {
    let basis_head = HeadState {
        snapshot_hint_seq: Some(head.seq),
        ..head.clone()
    };
    let checkpoint = prepare_checkpoint(&basis_head, metadata_state, writer_version)
        .expect("prepare checkpoint");
    store
        .put_overwrite(
            &checkpoint.manifest.object_key,
            &checkpoint.manifest.encoded_bytes,
        )
        .expect("seed checkpoint manifest");
    for segment in &checkpoint.segments {
        store
            .put_overwrite(&segment.object_key, &segment.encoded_bytes)
            .expect("seed checkpoint segment");
    }

    let head_envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::NamespaceHead, writer_version, basis_head)
            .expect("encode basis head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize basis head envelope");
    store
        .put_overwrite(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("overwrite head with checkpoint-backed basis");
}

fn validate_initial_namespace_alignment(initial: &NamespaceSimInitial) {
    let namespace_id = initial
        .client_state
        .head
        .as_ref()
        .map(|head| head.namespace_id.clone())
        .or_else(|| {
            initial
                .client_state
                .lease
                .as_ref()
                .map(|lease| lease.namespace_id.clone())
        })
        .or_else(|| {
            initial
                .client_state
                .local_only_state
                .as_ref()
                .map(|row| row.namespace_id.clone())
        })
        .or_else(|| {
            initial
                .client_state
                .remote_state
                .as_ref()
                .map(|row| row.namespace_id.clone())
        })
        .expect("unified namespace sim requires a namespace id in initial client state");

    if let Some(lease) = &initial.client_state.lease {
        assert_eq!(lease.namespace_id, namespace_id);
    }
    if let Some(local_only) = &initial.client_state.local_only_state {
        assert_eq!(local_only.namespace_id, namespace_id);
    }
    if let Some(planned) = &initial.client_state.planned_local_only_action {
        assert_eq!(planned.namespace_id, namespace_id);
    }
    if let Some(remote) = &initial.client_state.remote_state {
        assert_eq!(remote.namespace_id, namespace_id);
    }
    if let Some(local) = &initial.client_state.local_state {
        assert_eq!(local.namespace_id, namespace_id);
    }
    if let Some(anchor) = &initial.client_state.sync_anchor {
        assert_eq!(anchor.namespace_id, namespace_id);
    }
    for progress in &initial.progress_objects {
        assert_eq!(progress.payload.namespace_id, namespace_id);
    }
    for observation in initial
        .remote_observations
        .iter()
        .chain(initial.network.queued_remote_observations.iter())
    {
        assert_eq!(observation.remote_observation.namespace_id, namespace_id);
    }
}

fn seed_client_state(db_path: &std::path::Path, initial: &ClientStateInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client DB");
    db.planner_transaction("seed-namespace-sim-state", |tx| {
        if let Some(local_only) = &initial.local_only_state {
            tx.upsert_local_only_file(local_only)?;
        }
        if let Some(planned) = &initial.planned_local_only_action {
            tx.upsert_planned_local_only_action(planned)?;
        }
        if let Some(remote) = &initial.remote_state {
            tx.upsert_remote_file(remote)?;
        }
        if let Some(local) = &initial.local_state {
            tx.upsert_local_file(local)?;
        }
        if let Some(anchor) = &initial.sync_anchor {
            tx.upsert_sync_anchor(anchor)?;
        }
        Ok(())
    })
    .expect("seed namespace sim state");
}

fn seed_head_and_lease(store: &LocalFsStore, head: Option<&HeadState>, lease: Option<&LeaseState>) {
    if let Some(head) = head {
        let head_envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "sim-seed",
            head.clone(),
        )
        .expect("encode head envelope");
        let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
        store
            .put_overwrite(&namespace_head(head.namespace_id.as_str()), &head_bytes)
            .expect("seed namespace head");
    }
    if let Some(lease) = lease {
        let lease_envelope = LeaseStateEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "sim-seed",
            lease.clone(),
        )
        .expect("encode lease envelope");
        let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
        store
            .put_overwrite(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
            .expect("seed namespace lease");
    }
}

fn observed_from_response(response: &ClientMutationResponse) -> Option<ObservedRemoteInode> {
    response
        .created_inode
        .as_ref()
        .map(|created| ObservedRemoteInode {
            namespace_id: response.namespace_id.clone(),
            inode_id: created.inode_id,
            inode_kind: created.inode_kind.clone(),
            observed_seq: response.committed_seq,
            revision_no: created.revision_no,
            content_digest: created.content_digest.clone(),
            content_manifest_digest: created.content_digest.clone(),
            parent_inode_id: Some(created.parent_inode_id),
            display_name: created.display_name.clone(),
            is_deleted: false,
        })
}

fn response_target(response: &ClientMutationResponse) -> Option<(NamespaceId, InodeId)> {
    response
        .created_inode
        .as_ref()
        .map(|created| (response.namespace_id.clone(), created.inode_id))
        .or_else(|| {
            response
                .replaced_file
                .as_ref()
                .map(|replaced| (response.namespace_id.clone(), replaced.inode_id))
        })
}

fn assert_client_final_expectations(
    db_path: &std::path::Path,
    expect: &NamespaceSimExpect,
    trace: &[String],
    scenario: &Scenario,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client DB for final checks");
    if let Some(remote) = &expect.remote_state {
        let views = db
            .load_file_sync_views(&remote.namespace_id, remote.inode_id)
            .expect("load final file sync views");
        assert_eq!(
            views.remote,
            Some(remote.clone()),
            "unexpected final remote state:\n{}",
            render_trace(scenario, trace)
        );
        if let Some(local) = &expect.local_state {
            assert_eq!(
                views.local,
                Some(local.clone()),
                "unexpected final local state:\n{}",
                render_trace(scenario, trace)
            );
        }
        if let Some(anchor) = &expect.sync_anchor {
            assert_eq!(
                views.sync_anchor,
                Some(anchor.clone()),
                "unexpected final sync anchor:\n{}",
                render_trace(scenario, trace)
            );
        }
        if let Some(planner_result) = &expect.planner_result {
            assert_eq!(
                plan_file(
                    &mut db,
                    &planner_result.namespace_id,
                    planner_result.inode_id,
                    planner_result.created_at_ms,
                )
                .expect("plan final inode"),
                planner_result.clone(),
                "unexpected final planner result:\n{}",
                render_trace(scenario, trace)
            );
        }
    }
    if let Some(expected) = expect.pending_client_mutation_present {
        let actual = db
            .load_next_planned_local_only_action()
            .expect("load next planned local-only action")
            .and_then(|planned| {
                db.load_pending_client_mutation_for_client_file(&planned.client_file_id)
                    .expect("load pending client mutation")
            })
            .is_some();
        assert_eq!(
            actual,
            expected,
            "unexpected pending client mutation presence:\n{}",
            render_trace(scenario, trace)
        );
    }
    if let Some(expected) = expect.pending_inode_mutation_present {
        let actual = db
            .load_next_executable_planned_action()
            .expect("load next executable planned action")
            .and_then(|planned| {
                db.load_pending_inode_mutation_for_inode(&planned.namespace_id, planned.inode_id)
                    .expect("load pending inode mutation")
            })
            .is_some();
        assert_eq!(
            actual,
            expected,
            "unexpected pending inode mutation presence:\n{}",
            render_trace(scenario, trace)
        );
    }
}

fn assert_background_final_expectations(
    store: &LocalFsStore,
    core_head: &HeadState,
    expect: &NamespaceSimExpect,
    trace: &[String],
    scenario: &Scenario,
) {
    if let Some(expected_head) = &expect.head {
        let actual_head = read_head(store, &expected_head.namespace_id);
        if &actual_head != expected_head || core_head != expected_head {
            panic!(
                "namespace sim final head mismatch expected={expected_head:?} actual_store={actual_head:?} actual_runtime={core_head:?}:\n{}",
                render_trace(scenario, trace)
            );
        }
    }

    if !expect.progress_objects.is_empty() {
        let actual_progress = expect
            .progress_objects
            .iter()
            .map(|expected| FixtureProgressObject {
                key: expected.key.clone(),
                payload: read_progress_object(
                    store,
                    &expected.payload.namespace_id,
                    &expected.payload.work_class,
                )
                .expect("expected progress object should load")
                .envelope
                .state,
            })
            .collect::<Vec<_>>();
        if actual_progress != expect.progress_objects {
            panic!(
                "namespace sim final progress mismatch expected={:?} actual={actual_progress:?}:\n{}",
                expect.progress_objects,
                render_trace(scenario, trace)
            );
        }
    }

    if let Some(expected_queue_shard) = &expect.queue_shard {
        let actual_queue = read_queue_shard(store, expected_queue_shard.payload.shard_id)
            .expect("expected queue shard should load");
        let expected_state = queue_shard_state_from_fixture(&expected_queue_shard.payload);
        if actual_queue.object_key != expected_queue_shard.key
            || actual_queue.envelope.state != expected_state
        {
            panic!(
                "namespace sim final queue shard mismatch expected_key={} actual_key={} expected_state={:?} actual_state={:?}:\n{}",
                expected_queue_shard.key,
                actual_queue.object_key,
                expected_state,
                actual_queue.envelope.state,
                render_trace(scenario, trace)
            );
        }
    }
}
