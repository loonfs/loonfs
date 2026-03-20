use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedRemoteObservation, FileSyncViews, LocalFileStateRow, ObservedRemoteInode,
    PlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_testkit::invariants::{
    evaluate_remote_only_discovery_invariants, ClientReconciliationInvariantReport,
    RemoteOnlyDiscoveryInvariantInputs,
};
use loon_testkit::render::render_trace;
use loon_testkit::scenario::Scenario;
use loon_types::{InodeId, NamespaceId};
use serde::Deserialize;

#[test]
fn remote_observation_discovers_remote_only_file_and_restarts_plannable() {
    run_remote_only_discovery_scenario(
        "client/client_remote_observation_discovers_remote_only_file.yaml",
        "client-remote-only-discovery-file",
    );
}

#[test]
fn remote_observation_discovers_remote_only_directory_and_restarts_plannable() {
    run_remote_only_discovery_scenario(
        "client/client_remote_observation_discovers_remote_only_directory.yaml",
        "client-remote-only-discovery-dir",
    );
}

#[test]
fn remote_only_file_discovery_invariant_trace_matches_checked_in_artifact() {
    let report = run_remote_only_discovery_invariant_report(
        "client/client_remote_observation_discovers_remote_only_file.yaml",
        "client-remote-only-discovery-file-invariants",
    );

    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/client-reconciliation-invariants/client/client_remote_observation_discovers_remote_only_file.txt"
        )
    );
}

fn run_remote_only_discovery_scenario(relative_path: &str, temp_dir_name: &str) {
    let scenario = load_fixture(relative_path);
    let _initial: RemoteOnlyDiscoveryInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: RemoteOnlyDiscoveryExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new(temp_dir_name);
    let db_path = temp_dir.path().join("client.sqlite3");

    let observe = actions[0].apply().expect("apply action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let outcome = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation")
    };

    assert_eq!(outcome, expect.outcome.clone().into_outcome());

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan discovered remote-only inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load discovered views"),
        FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: expect.sync_anchor.clone(),
        }
    );
    assert_eq!(
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action"),
        expect.planned_action.clone()
    );

    let report = evaluate_remote_only_discovery_invariants(RemoteOnlyDiscoveryInvariantInputs {
        inode_kind: expect.local_state.inode_kind.clone(),
        local_exists_on_disk_after: expect.local_state.exists_on_disk,
        local_dirty_after: expect.local_state.dirty,
        sync_anchor_present_after: expect.sync_anchor.is_some(),
        planned_action_decision_after: expect
            .planned_action
            .as_ref()
            .map(|row| row.decision.as_str()),
    });
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);
}

fn run_remote_only_discovery_invariant_report(
    relative_path: &str,
    temp_dir_name: &str,
) -> ReconciliationInvariantFixtureRunReport {
    let scenario = load_fixture(relative_path);
    let _initial: RemoteOnlyDiscoveryInitial = scenario.decode_initial().expect("decode initial");
    let actions: Vec<RemoteObservationFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: RemoteOnlyDiscoveryExpect = scenario.decode_expect().expect("decode expect");
    let temp_dir = TestDir::new(temp_dir_name);
    let db_path = temp_dir.path().join("client.sqlite3");

    let observe = actions[0].apply().expect("apply action first");
    let planner_tick = actions[2].planner().expect("planner action third");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.apply_remote_observation(&observe.remote_observation, observe.applied_at_ms)
            .expect("apply remote observation");
    }

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after observation");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan discovered remote-only inode after restart");
    let report = evaluate_remote_only_discovery_invariants(RemoteOnlyDiscoveryInvariantInputs {
        inode_kind: expect.local_state.inode_kind.clone(),
        local_exists_on_disk_after: expect.local_state.exists_on_disk,
        local_dirty_after: expect.local_state.dirty,
        sync_anchor_present_after: expect.sync_anchor.is_some(),
        planned_action_decision_after: expect
            .planned_action
            .as_ref()
            .map(|row| row.decision.as_str()),
    });

    assert_eq!(planner_result, expect.planner_result);
    assert_expected_reconciliation_invariants(&scenario, &report, &expect.invariants);

    let trace = vec![
        format!("outcome={:?}", expect.outcome.clone().into_outcome()),
        format!("planner_result={planner_result:?}"),
    ]
    .into_iter()
    .chain(report.render_trace_lines("remote-only-discovery"))
    .collect::<Vec<_>>();

    ReconciliationInvariantFixtureRunReport {
        rendered_trace: render_trace(&scenario, &trace),
    }
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

#[derive(Debug, Deserialize)]
struct RemoteOnlyDiscoveryInitial {}

#[derive(Debug, Deserialize)]
struct RemoteOnlyDiscoveryExpect {
    outcome: RawAppliedRemoteObservation,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: Option<SyncAnchorRow>,
    planned_action: Option<PlannedActionRow>,
    planner_result: PlannedActionRecord,
    #[serde(default)]
    invariants: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RemoteObservationFixtureAction {
    ApplyRemoteObservation {
        apply_remote_observation: ApplyRemoteObservationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl RemoteObservationFixtureAction {
    fn apply(&self) -> Option<ApplyRemoteObservationAction> {
        match self {
            Self::ApplyRemoteObservation {
                apply_remote_observation,
            } => Some(apply_remote_observation.clone()),
            _ => None,
        }
    }

    fn is_restart(&self) -> bool {
        matches!(
            self,
            Self::RestartClientStateDb {
                restart_client_state_db: true
            }
        )
    }

    fn planner(&self) -> Option<PlannerTickAction> {
        match self {
            Self::PlannerTick { planner_tick } => Some(planner_tick.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ApplyRemoteObservationAction {
    applied_at_ms: u64,
    remote_observation: ObservedRemoteInode,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawAppliedRemoteObservation {
    DiscoveredRemoteOnly {
        discovered_remote_only: RawAppliedRemoteObservationTarget,
    },
}

impl RawAppliedRemoteObservation {
    fn into_outcome(self) -> AppliedRemoteObservation {
        match self {
            Self::DiscoveredRemoteOnly {
                discovered_remote_only,
            } => AppliedRemoteObservation::DiscoveredRemoteOnly {
                namespace_id: discovered_remote_only.namespace_id,
                inode_id: discovered_remote_only.inode_id,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAppliedRemoteObservationTarget {
    namespace_id: NamespaceId,
    inode_id: InodeId,
}

fn assert_expected_reconciliation_invariants(
    scenario: &Scenario,
    report: &ClientReconciliationInvariantReport,
    expected: &[String],
) {
    for name in expected {
        let check = report
            .check(name)
            .unwrap_or_else(|| panic!("{} missing invariant `{name}`", scenario.name));
        assert!(
            check.passed,
            "{} invariant `{name}` failed: {}",
            scenario.name, check.detail
        );
    }
}

#[derive(Debug)]
struct ReconciliationInvariantFixtureRunReport {
    rendered_trace: String,
}

type TestDir = loon_testkit::tempdir::TestDir;
