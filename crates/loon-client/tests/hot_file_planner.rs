use loon_client::planner::{plan_file, PlannedActionRecord, PlannerDecision, PlannerReason};
use loon_client::state_db::{LocalFileStateRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow};
use loon_testkit::scenario::Scenario;
use loon_types::{InodeId, NamespaceId};
use serde::Deserialize;
use std::path::PathBuf;

#[test]
fn hot_file_fixture_persists_conflict_copy_plan() {
    let scenario = load_fixture("client/hot_file_conflict_copy_from_three_views.yaml");
    let initial: HotFileInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<PlannerActionEnvelope> = scenario.decode_actions().expect("decode actions");
    let expect: HotFileExpect = scenario.decode_expect().expect("decode expectations");

    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");
    db.planner_transaction("seed-fixture", |tx| {
        tx.upsert_remote_file(&initial.remote_state)?;
        tx.upsert_local_file(&initial.local_state)?;
        tx.upsert_sync_anchor(&initial.sync_anchor)?;
        Ok(())
    })
    .expect("seed hot-file fixture");

    assert_eq!(actions.len(), 1, "fixture should contain one planner tick");
    let action = &actions[0].planner_tick;
    let planned = plan_file(
        &mut db,
        &action.namespace_id,
        action.inode_id,
        action.now_ms,
    )
    .expect("planner should succeed");

    let persisted = db
        .load_planned_action(&action.namespace_id, action.inode_id)
        .expect("load planned action")
        .expect("planner should persist a non-noop action");

    let expected = PlannedActionRecord {
        namespace_id: expect.planner_action.namespace_id,
        inode_id: expect.planner_action.inode_id,
        decision: expect.planner_action.decision,
        reason: expect.planner_action.reason,
        created_at_ms: action.now_ms,
    };

    assert_eq!(planned, expected);
    assert_eq!(
        PlannedActionRecord::try_from(persisted).expect("decode persisted action"),
        expected
    );
}

#[derive(Debug, Deserialize)]
struct HotFileInitial {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
}

#[derive(Debug, Deserialize)]
struct PlannerActionEnvelope {
    planner_tick: PlannerTickAction,
}

#[derive(Debug, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct HotFileExpect {
    planner_action: ExpectedPlannerAction,
}

#[derive(Debug, Deserialize)]
struct ExpectedPlannerAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    decision: PlannerDecision,
    reason: PlannerReason,
}

fn load_fixture(relative_path: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}
