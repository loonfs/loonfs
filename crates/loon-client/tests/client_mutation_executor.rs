use loon_client::executor::build_client_mutation_request;
use loon_client::planner::{PlannedLocalOnlyActionRecord, PlannerDecision, PlannerReason};
use loon_client::state_db::LocalOnlyFileStateRow;
use loon_testkit::scenario::Scenario;
use loon_types::{ClientMutationOp, ClientMutationRequest};
use serde::Deserialize;
use std::path::PathBuf;

#[test]
fn upload_local_create_fixture_translates_to_client_create_file_request() {
    let scenario = load_fixture("client/local_only_file_gets_temp_identity_and_upload_plan.yaml");
    let initial: LocalOnlyInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<PlannerLocalOnlyActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: LocalOnlyExpect = scenario.decode_expect().expect("decode expectations");

    let planner_tick = &actions[0].planner_tick_local_only;
    let planned = PlannedLocalOnlyActionRecord {
        client_file_id: expect.planned_local_only_action.client_file_id.clone(),
        namespace_id: expect.planned_local_only_action.namespace_id.clone(),
        decision: expect.planned_local_only_action.decision.clone(),
        reason: expect.planned_local_only_action.reason.clone(),
        created_at_ms: planner_tick.now_ms,
    };

    let request =
        build_client_mutation_request("client-req-0001", &initial.local_only_state, &planned)
            .expect("translate upload-local-create request");

    assert_eq!(
        request,
        ClientMutationRequest {
            namespace_id: loon_types::NamespaceId::from("ns-1"),
            client_request_id: "client-req-0001".to_owned(),
            op: ClientMutationOp::CreateFile {
                parent_inode_id: loon_types::InodeId(2),
                display_name: "draft.txt".to_owned(),
                content_manifest_digest: "sha256:new-local-file".to_owned(),
            },
        }
    );
}

#[test]
fn create_remote_dir_fixture_translates_to_client_create_dir_request() {
    let scenario = load_fixture("client/local_only_directory_gets_remote_create_plan.yaml");
    let initial: LocalOnlyInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<PlannerLocalOnlyActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: LocalOnlyExpect = scenario.decode_expect().expect("decode expectations");

    let planner_tick = &actions[0].planner_tick_local_only;
    let planned = PlannedLocalOnlyActionRecord {
        client_file_id: expect.planned_local_only_action.client_file_id.clone(),
        namespace_id: expect.planned_local_only_action.namespace_id.clone(),
        decision: expect.planned_local_only_action.decision.clone(),
        reason: expect.planned_local_only_action.reason.clone(),
        created_at_ms: planner_tick.now_ms,
    };

    let request =
        build_client_mutation_request("client-req-0002", &initial.local_only_state, &planned)
            .expect("translate create-remote-dir request");

    assert_eq!(
        request,
        ClientMutationRequest {
            namespace_id: loon_types::NamespaceId::from("ns-1"),
            client_request_id: "client-req-0002".to_owned(),
            op: ClientMutationOp::CreateDir {
                parent_inode_id: loon_types::InodeId(2),
                display_name: "drafts".to_owned(),
            },
        }
    );
}

#[derive(Debug, Deserialize)]
struct LocalOnlyInitial {
    local_only_state: LocalOnlyFileStateRow,
}

#[derive(Debug, Deserialize)]
struct PlannerLocalOnlyActionEnvelope {
    planner_tick_local_only: PlannerTickLocalOnlyAction,
}

#[derive(Debug, Deserialize)]
struct PlannerTickLocalOnlyAction {
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct LocalOnlyExpect {
    planned_local_only_action: ExpectedLocalOnlyPlannerAction,
}

#[derive(Debug, Deserialize)]
struct ExpectedLocalOnlyPlannerAction {
    client_file_id: loon_client::state_db::ClientFileId,
    namespace_id: loon_types::NamespaceId,
    decision: PlannerDecision,
    reason: PlannerReason,
}

fn load_fixture(relative_path: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}
