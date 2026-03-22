use loon_client::executor::build_client_mutation_request_from_state;
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    ObservedRemoteInode, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::testing::{ClientExecutionHooks, FaultController};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_sim::SimRuntime;
use loon_testkit::client::seed_server_basis_for_request;
use loon_testkit::fixtures::load_fixture;
use loon_testkit::invariants::{
    evaluate_client_retry_reuse_invariants, evaluate_duplicate_response_invariants,
    evaluate_late_remote_observation_invariants, ClientRetryReuseInvariantInputs,
    DuplicateResponseInvariantInputs, LateRemoteObservationInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_testkit::tempdir::TestDir;
use loon_types::{
    ClientMutationResponse, ControlObjectKind, HeadState, HeadStateEnvelope, InodeId, LeaseState,
    LeaseStateEnvelope, NamespaceId,
};
use serde::Deserialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientServerSimRunStatus {
    Passed,
    Failed { failure_headline: String },
    NotExecutable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientServerExplorationCaseLabel {
    pub case_index: usize,
    pub seed: Option<u64>,
    pub fault_summary: String,
    pub permuted_action_order: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientServerSimRunOptions {
    pub exploration_case: Option<ClientServerExplorationCaseLabel>,
}

#[derive(Debug, Clone)]
pub struct ClientServerFixtureRunReport {
    pub rendered_trace: String,
    pub status: ClientServerSimRunStatus,
}

#[allow(dead_code)]
pub fn run_client_server_fixture_report(relative_path: &str) -> ClientServerFixtureRunReport {
    let scenario = load_fixture(relative_path);
    run_client_server_scenario_report(&scenario, ClientServerSimRunOptions::default())
}

pub fn run_client_server_scenario_report(
    scenario: &Scenario,
    options: ClientServerSimRunOptions,
) -> ClientServerFixtureRunReport {
    let initial: ClientServerSimInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<ClientServerSimActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: ClientServerSimExpect = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-server-sim");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local objectstore root");
    let store = LocalFsStore::new(&store_root).expect("create local objectstore");
    seed_client_state(&db_path, &initial.client_state);
    seed_head_and_lease(
        &store,
        initial.client_state.head.as_ref(),
        initial.client_state.lease.as_ref(),
    );

    let mut runtime = SimRuntime::new();
    let mut trace = Vec::new();
    let mut request_deliveries = BTreeMap::<u64, ClientMutationResponseOrRequest>::new();
    let mut response_deliveries = BTreeMap::<u64, ClientMutationResponse>::new();
    let mut observation_deliveries = BTreeMap::<u64, QueuedObservation>::new();
    let mut server_inbox = VecDeque::<ClientMutationResponseOrRequest>::new();
    let controller = FaultController::new(scenario.decode_fault_plan().expect("decode faults"));
    let mut client_generation = 0_u64;
    let mut delayed_retry_before_restart: Option<String> = None;
    let mut delayed_retry_after_retry: Option<String> = None;
    let mut delayed_retry_retried_request: Option<String> = None;
    let mut duplicate_second_apply_clean = false;
    let mut duplicate_winner_apply_count = 0_u64;
    let mut late_observation_response_ok = false;
    let mut late_observation_apply_ok = false;
    let mut late_observation_winner_apply_count = 0_u64;
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
                "initial client_state={} seeded_network_responses={} seeded_observations={}",
                initial.client_state.describe(),
                initial.network.queued_responses.len(),
                initial.remote_observations.len()
                    + initial.network.queued_remote_observations.len()
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

        for action in &actions {
            match action.kind() {
                ClientServerSimActionRef::ClientTick(action) => {
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
                        "sim harness currently supports local-only create_remote_dir only"
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
                    if let Some(message) =
                        controller.dispatch_error("dispatch.client.before_request")
                    {
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
                        let delivery =
                            runtime.enqueue_delivery("client", "server", "client_request");
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
                ClientServerSimActionRef::DeliverNextRequest => {
                    let delivery = runtime
                        .take_next_matching_delivery(|delivery| {
                            delivery.kind == "client_request"
                                && delivery.recipient.as_str() == "server"
                        })
                        .expect("client request delivery should exist");
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                    let queued = request_deliveries
                        .remove(&delivery.delivery_id)
                        .expect("queued request payload should exist");
                    let request = queued.into_request().expect("request payload");
                    server_inbox
                        .push_back(ClientMutationResponseOrRequest::Request(request.clone()));
                    runtime.record_actor_step(
                        "network",
                        format!(
                            "deliver_next_request request_id={} delivery_id={}",
                            request.client_request_id, delivery.delivery_id
                        ),
                    );
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                }
                ClientServerSimActionRef::ServerHandleNextRequest(action) => {
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
                    seed_server_basis_for_request(&store, &request, &writer_version);
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
                    let response = executed.response;
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
                        "handle_next_request request_id={} committed_seq={} writer_id={} writer_version={}",
                        request.client_request_id, response.committed_seq.0, writer_id, writer_version
                    ),
                );
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                }
                ClientServerSimActionRef::DeliverNextResponse => {
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
                    } else if changed {
                        duplicate_winner_apply_count =
                            duplicate_winner_apply_count.saturating_add(1);
                    }
                    if response.replaced_file.is_some() {
                        duplicate_second_apply_clean = true;
                    } else {
                        late_observation_response_ok = true;
                    }
                    runtime.record_actor_step(
                        "client",
                        format!(
                            "deliver_next_response request_id={} changed={} {}",
                            response.client_request_id, changed, outcome_detail
                        ),
                    );
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                }
                ClientServerSimActionRef::DeliverRemoteObservation(action) => {
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
                    }
                    late_observation_apply_ok = matches!(
                        applied,
                        AppliedRemoteObservation::BoundLocalOnly { .. }
                            | AppliedRemoteObservation::ConvergedBoundInode { .. }
                            | AppliedRemoteObservation::UpdatedBoundRemoteState { .. }
                    );
                    runtime.record_actor_step(
                        "client",
                        format!(
                            "deliver_remote_observation inode_id={} applied={:?} applied_at_ms={}",
                            seeded.remote_observation.inode_id.0, applied, applied_at_ms
                        ),
                    );
                    push_latest_runtime_trace_line(&mut trace, &runtime);
                }
                ClientServerSimActionRef::RestartClient => {
                    client_generation = client_generation.saturating_add(1);
                    runtime.record_restart("client");
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

        for report in &sim_reports {
            trace.extend(report.render_trace_lines("sim"));
        }

        assert_final_expectations(&db_path, &expect, &trace, scenario);
        for invariant in &expect.invariants {
            let passed = sim_reports
                .iter()
                .any(|report| report.check(invariant).is_some_and(|check| check.passed));
            assert!(
                passed,
                "missing expected invariant `{invariant}`:\n{}",
                render_trace(scenario, &trace)
            );
        }
    }));

    let status = match execution {
        Ok(()) => ClientServerSimRunStatus::Passed,
        Err(payload) => {
            let headline = panic_payload_to_headline(payload);
            if let Some(reason) = client_server_not_executable_reason(&headline) {
                ClientServerSimRunStatus::NotExecutable { reason }
            } else {
                ClientServerSimRunStatus::Failed {
                    failure_headline: headline,
                }
            }
        }
    };

    ClientServerFixtureRunReport {
        rendered_trace: render_trace(scenario, &trace),
        status,
    }
}

#[derive(Debug, Deserialize, Default)]
struct ClientServerSimInitial {
    #[serde(default)]
    client_state: ClientStateInitial,
    #[serde(default)]
    server_basis: Option<ServerBasisInitial>,
    #[serde(default)]
    remote_observations: Vec<QueuedObservation>,
    #[serde(default)]
    network: ClientServerNetworkInitial,
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

#[derive(Debug, Deserialize, Default)]
struct ClientServerSimExpect {
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
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ClientServerSimActionEnvelope {
    ClientTick {
        client_tick: ClientTickAction,
    },
    DeliverNextRequest {
        deliver_next_request: bool,
    },
    ServerHandleNextRequest {
        server_handle_next_request: ServerHandleNextRequestAction,
    },
    DeliverNextResponse {
        deliver_next_response: bool,
    },
    DeliverRemoteObservation {
        deliver_remote_observation: DeliverRemoteObservationAction,
    },
    RestartClient {
        restart_client: bool,
    },
}

impl ClientServerSimActionEnvelope {
    fn kind(&self) -> ClientServerSimActionRef<'_> {
        match self {
            Self::ClientTick { client_tick } => ClientServerSimActionRef::ClientTick(client_tick),
            Self::DeliverNextRequest {
                deliver_next_request,
            } => {
                assert!(
                    *deliver_next_request,
                    "deliver_next_request action must be true"
                );
                ClientServerSimActionRef::DeliverNextRequest
            }
            Self::ServerHandleNextRequest {
                server_handle_next_request,
            } => ClientServerSimActionRef::ServerHandleNextRequest(server_handle_next_request),
            Self::DeliverNextResponse {
                deliver_next_response,
            } => {
                assert!(
                    *deliver_next_response,
                    "deliver_next_response action must be true"
                );
                ClientServerSimActionRef::DeliverNextResponse
            }
            Self::DeliverRemoteObservation {
                deliver_remote_observation,
            } => ClientServerSimActionRef::DeliverRemoteObservation(deliver_remote_observation),
            Self::RestartClient { restart_client } => {
                assert!(*restart_client, "restart_client action must be true");
                ClientServerSimActionRef::RestartClient
            }
        }
    }
}

enum ClientServerSimActionRef<'a> {
    ClientTick(&'a ClientTickAction),
    DeliverNextRequest,
    ServerHandleNextRequest(&'a ServerHandleNextRequestAction),
    DeliverNextResponse,
    DeliverRemoteObservation(&'a DeliverRemoteObservationAction),
    RestartClient,
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

fn push_latest_runtime_trace_line(trace: &mut Vec<String>, runtime: &SimRuntime) {
    let line = runtime
        .trace()
        .last()
        .expect("runtime should contain at least one trace event")
        .render_line();
    trace.push(line);
}

fn seed_client_state(db_path: &std::path::Path, initial: &ClientStateInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client DB");
    db.planner_transaction("seed-client-sim-state", |tx| {
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
    .expect("seed client sim state");
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

fn assert_final_expectations(
    db_path: &std::path::Path,
    expect: &ClientServerSimExpect,
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

fn client_server_not_executable_reason(headline: &str) -> Option<String> {
    if headline.contains("client request delivery should exist") {
        return Some("deliver_next_request requires an enqueued client request".to_owned());
    }
    if headline.contains("server inbox should contain a request") {
        return Some("server_handle_next_request requires a delivered client request".to_owned());
    }
    if headline.contains("client response delivery should exist") {
        return Some("deliver_next_response requires an enqueued client response".to_owned());
    }
    if headline.contains("remote observation delivery should exist") {
        return Some(
            "deliver_remote_observation requires an enqueued remote observation".to_owned(),
        );
    }
    None
}

fn panic_payload_to_headline(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => message.lines().next().unwrap_or("").to_owned(),
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => message.lines().next().unwrap_or("").to_owned(),
            Err(_) => "client server sim panicked".to_owned(),
        },
    }
}
