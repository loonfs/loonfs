use loon_client::planner::{
    plan_local_only_inode, PlannedLocalOnlyActionRecord, PlannerDecision, PlannerReason,
};
use loon_client::state_db::{
    LocalFileStateRow, LocalOnlyFileStateRow, ObservedLocalOnlyInode, RemoteFileStateRow,
    SqliteStateDb, SyncAnchorRow,
};
use loon_testkit::scenario::Scenario;
use serde::Deserialize;

#[test]
fn bound_directory_child_fixture_persists_and_plans_file_create() {
    let scenario = load_fixture("client/bound_directory_child_file_gets_upload_plan.yaml");
    let initial: BoundDirectoryInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: BoundDirectoryExpect = scenario.decode_expect().expect("decode expectations");

    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");
    db.planner_transaction("seed-bound-directory-parent", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        Ok(())
    })
    .expect("seed bound directory parent");

    assert_eq!(
        actions.len(),
        2,
        "fixture should contain observe child then planner tick"
    );
    let observed = match &actions[0] {
        FixtureAction::Observe {
            observe_local_only_inode,
        } => observe_local_only_inode,
        other => panic!("expected observe action first, got {other:?}"),
    };
    let persisted = db
        .observe_local_only_inode_under_parent(observed)
        .expect("observe local-only child");

    assert_eq!(persisted, expect.local_only_state);
    assert_eq!(
        db.load_local_only_file(&persisted.client_file_id)
            .expect("load persisted child"),
        Some(expect.local_only_state.clone())
    );

    let planner_tick = match &actions[1] {
        FixtureAction::Planner {
            planner_tick_local_only,
        } => planner_tick_local_only,
        other => panic!("expected planner tick second, got {other:?}"),
    };
    let planned = plan_local_only_inode(&mut db, &planner_tick.client_file_id, planner_tick.now_ms)
        .expect("plan local-only child");

    let persisted_plan = db
        .load_planned_local_only_action(&planner_tick.client_file_id)
        .expect("load planned action")
        .expect("planner should persist a non-noop action");

    let expected_plan = PlannedLocalOnlyActionRecord {
        client_file_id: expect.planned_local_only_action.client_file_id,
        namespace_id: expect.planned_local_only_action.namespace_id,
        decision: expect.planned_local_only_action.decision,
        reason: expect.planned_local_only_action.reason,
        created_at_ms: planner_tick.now_ms,
    };

    assert_eq!(planned, expected_plan);
    assert_eq!(
        PlannedLocalOnlyActionRecord::try_from(persisted_plan).expect("decode planned action"),
        expected_plan
    );
}

#[derive(Debug, Deserialize)]
struct BoundDirectoryInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FixtureAction {
    Observe {
        observe_local_only_inode: ObservedLocalOnlyInode,
    },
    Planner {
        planner_tick_local_only: PlannerTickLocalOnlyAction,
    },
}

#[derive(Debug, Deserialize)]
struct PlannerTickLocalOnlyAction {
    client_file_id: loon_client::state_db::ClientFileId,
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct BoundDirectoryExpect {
    local_only_state: LocalOnlyFileStateRow,
    planned_local_only_action: ExpectedPlannerAction,
}

#[derive(Debug, Deserialize)]
struct ExpectedPlannerAction {
    client_file_id: loon_client::state_db::ClientFileId,
    namespace_id: loon_types::NamespaceId,
    decision: PlannerDecision,
    reason: PlannerReason,
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}
