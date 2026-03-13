use loon_client::planner::{plan_file, PlannedActionRecord, PlannerDecision, PlannerReason};
use loon_client::state_db::{
    LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow, RemoteFileStateRow,
    SqliteStateDb, SyncAnchorRow,
};
use loon_testkit::scenario::Scenario;
use serde::Deserialize;
use std::path::PathBuf;

#[test]
fn local_only_binding_fixture_migrates_temp_identity_into_inode_state() {
    let scenario = load_fixture("client/local_only_create_binds_to_remote_inode.yaml");
    let initial: LocalOnlyBindInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: LocalOnlyBindExpect = scenario.decode_expect().expect("decode expectations");

    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");
    db.planner_transaction("seed-local-only-bind-fixture", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action.into_row())?;
        Ok(())
    })
    .expect("seed bind fixture");

    assert_eq!(
        actions.len(),
        2,
        "fixture should contain bind then planner tick"
    );
    let bind = match &actions[0] {
        FixtureAction::Bind {
            bind_local_only_identity,
        } => bind_local_only_identity,
        other => panic!("expected bind action first, got {other:?}"),
    };
    db.bind_local_only_file_to_remote(&bind.client_file_id, &bind.remote_state)
        .expect("bind local-only identity");

    assert_eq!(
        db.load_file_sync_views(&bind.remote_state.namespace_id, bind.remote_state.inode_id)
            .expect("load bound views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: expect.remote_state.namespace_id.clone(),
            inode_id: expect.remote_state.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_local_only_file(&bind.client_file_id)
            .expect("load local-only state after bind")
            .is_none(),
        expect.local_only_state_cleared
    );
    assert_eq!(
        db.load_planned_local_only_action(&bind.client_file_id)
            .expect("load planned local-only action after bind")
            .is_none(),
        expect.planned_local_only_action_cleared
    );

    let planner_tick = match &actions[1] {
        FixtureAction::Planner { planner_tick } => planner_tick,
        other => panic!("expected planner tick second, got {other:?}"),
    };
    let planned = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan bound inode");

    assert_eq!(
        planned,
        PlannedActionRecord {
            namespace_id: expect.planner_result.namespace_id,
            inode_id: expect.planner_result.inode_id,
            decision: expect.planner_result.decision,
            reason: expect.planner_result.reason,
            created_at_ms: planner_tick.now_ms,
        }
    );
    assert_eq!(
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action after converged planner tick"),
        None
    );
}

#[derive(Debug, Deserialize)]
struct LocalOnlyBindInitial {
    local_only_state: LocalOnlyFileStateRow,
    planned_local_only_action: InitialLocalOnlyPlannedAction,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FixtureAction {
    Bind {
        bind_local_only_identity: BindLocalOnlyIdentityAction,
    },
    Planner {
        planner_tick: PlannerTickAction,
    },
}

#[derive(Debug, Deserialize)]
struct BindLocalOnlyIdentityAction {
    client_file_id: loon_client::state_db::ClientFileId,
    remote_state: RemoteFileStateRow,
}

#[derive(Debug, Deserialize)]
struct PlannerTickAction {
    namespace_id: loon_types::NamespaceId,
    inode_id: loon_types::InodeId,
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct LocalOnlyBindExpect {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    planned_local_only_action_cleared: bool,
    planner_result: ExpectedPlannerResult,
}

#[derive(Debug, Deserialize)]
struct ExpectedPlannerResult {
    namespace_id: loon_types::NamespaceId,
    inode_id: loon_types::InodeId,
    decision: PlannerDecision,
    reason: PlannerReason,
}

#[derive(Debug, Deserialize)]
struct InitialLocalOnlyPlannedAction {
    client_file_id: loon_client::state_db::ClientFileId,
    namespace_id: loon_types::NamespaceId,
    decision: String,
    reason: String,
    created_at_ms: u64,
}

impl InitialLocalOnlyPlannedAction {
    fn into_row(self) -> LocalOnlyPlannedActionRow {
        LocalOnlyPlannedActionRow {
            client_file_id: self.client_file_id,
            namespace_id: self.namespace_id,
            decision: self.decision,
            reason: self.reason,
            created_at_ms: self.created_at_ms,
        }
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}
