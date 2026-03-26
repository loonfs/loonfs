use loon_client::planner::{
    plan_local_only_file, PlannedLocalOnlyActionRecord, PlannerDecision, PlannerReason,
};
use loon_client::state_db::{
    LocalFileStateRow, LocalOnlyFileStateRow, PlannedActionRow, SqliteStateDb, SyncAnchorRow,
};
use loon_testkit::scenario::Scenario;
use loon_types::{ChangeSeq, InodeId, InodeKind, NamespaceId, RevisionNo};
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

#[test]
fn local_only_planner_marks_same_path_bound_file_replacement_as_waiting() {
    let namespace_id = NamespaceId::from("ns-1");
    let replacement_id = loon_client::state_db::ClientFileId::from("tmp:ns-1:00000000000000000042");
    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");

    db.planner_transaction("seed-waiting-file-replacement", |tx| {
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_sync_anchor(&SyncAnchorRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            synced_seq: ChangeSeq(0),
            revision_no: RevisionNo(0),
            content_digest: None,
            content_manifest_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:notes-v1".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "notes".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 2_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            decision: PlannerDecision::DeleteFile.as_str().to_owned(),
            reason: PlannerReason::LocalObservedWithoutAnchor
                .as_str()
                .to_owned(),
            created_at_ms: 3_000,
        })?;
        tx.upsert_local_only_file(&LocalOnlyFileStateRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::Dir,
            parent_inode_id: Some(InodeId(1)),
            display_name: "notes".to_owned(),
            content_digest: None,
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 4_000,
        })?;
        Ok(())
    })
    .expect("seed file replacement state");

    let planned =
        plan_local_only_file(&mut db, &replacement_id, 5_000).expect("plan waiting state");

    assert_eq!(planned.decision, PlannerDecision::WaitForExactPathVacate);
    assert_eq!(
        planned.reason,
        PlannerReason::ExactPathBlockedByBoundOccupant
    );
}

#[test]
fn local_only_planner_marks_same_path_bound_rename_replacement_as_waiting() {
    let namespace_id = NamespaceId::from("ns-1");
    let replacement_id = loon_client::state_db::ClientFileId::from("tmp:ns-1:00000000000000000043");
    let mut db = SqliteStateDb::open_in_memory().expect("open state DB");

    db.planner_transaction("seed-waiting-rename-replacement", |tx| {
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            content_digest: None,
            parent_inode_id: None,
            display_name: String::new(),
            exists_on_disk: true,
            dirty: false,
            last_local_change_ms: 1_000,
        })?;
        tx.upsert_local_file(&LocalFileStateRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            content_digest: Some("sha256:notes-v1".to_owned()),
            parent_inode_id: Some(InodeId(1)),
            display_name: "notes".to_owned(),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 2_000,
        })?;
        tx.upsert_planned_action(&PlannedActionRow {
            namespace_id: namespace_id.clone(),
            inode_id: InodeId(2),
            decision: PlannerDecision::Rename.as_str().to_owned(),
            reason: PlannerReason::LocalDiffersFromAnchor.as_str().to_owned(),
            created_at_ms: 3_000,
        })?;
        tx.upsert_local_only_file(&LocalOnlyFileStateRow {
            client_file_id: replacement_id.clone(),
            namespace_id: namespace_id.clone(),
            inode_kind: InodeKind::Dir,
            parent_inode_id: Some(InodeId(1)),
            display_name: "notes".to_owned(),
            content_digest: None,
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: 4_000,
        })?;
        Ok(())
    })
    .expect("seed rename replacement state");

    let planned =
        plan_local_only_file(&mut db, &replacement_id, 5_000).expect("plan waiting state");

    assert_eq!(planned.decision, PlannerDecision::WaitForExactPathVacate);
    assert_eq!(
        planned.reason,
        PlannerReason::ExactPathBlockedByBoundOccupant
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
