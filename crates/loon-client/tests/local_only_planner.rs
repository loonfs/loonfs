use loon_client::planner::{
    plan_local_only_file, PlannedLocalOnlyActionRecord, PlannerDecision, PlannerReason,
};
use loon_client::state_db::{LocalOnlyFileStateRow, SqliteStateDb};
use loon_testkit::scenario::Scenario;
use serde::Deserialize;

#[test]
fn local_only_fixture_persists_create_upload_plan() {
    let scenario = load_fixture("client/local_only_file_gets_temp_identity_and_upload_plan.yaml");
    let initial: LocalOnlyInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<PlannerLocalOnlyActionEnvelope> =
        scenario.decode_actions().expect("decode actions");
    let expect: LocalOnlyExpect = scenario.decode_expect().expect("decode expectations");

    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");
    db.planner_transaction("seed-local-only-fixture", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        Ok(())
    })
    .expect("seed local-only fixture");

    assert_eq!(actions.len(), 1, "fixture should contain one planner tick");
    let action = &actions[0].planner_tick_local_only;
    let planned = plan_local_only_file(&mut db, &action.client_file_id, action.now_ms)
        .expect("local-only planner should succeed");

    let persisted = db
        .load_planned_local_only_action(&action.client_file_id)
        .expect("load planned local-only action")
        .expect("planner should persist a non-noop local-only action");

    let expected = PlannedLocalOnlyActionRecord {
        client_file_id: expect.planned_local_only_action.client_file_id,
        namespace_id: expect.planned_local_only_action.namespace_id,
        decision: expect.planned_local_only_action.decision,
        reason: expect.planned_local_only_action.reason,
        created_at_ms: action.now_ms,
    };

    assert_eq!(planned, expected);
    assert_eq!(
        PlannedLocalOnlyActionRecord::try_from(persisted)
            .expect("decode persisted local-only action"),
        expected
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
    client_file_id: loon_client::state_db::ClientFileId,
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
    loon_testkit::fixtures::load_fixture(relative_path)
}
