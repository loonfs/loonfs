#[path = "common/support.rs"]
mod support;

use loon_client::executor::build_client_mutation_request_from_state;
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    BoundLocalOnlyFile, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
};
use loon_client::upload::upload_small_file_from_path;
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_testkit::scenario::Scenario;
use loon_types::{
    ClientMutationResponse, ControlObjectKind, HeadState, HeadStateEnvelope, LeaseState,
    LeaseStateEnvelope,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn client_create_file_response_binds_and_restarts_converged() {
    run_fixture("client/client_create_file_response_binds_and_restarts_converged.yaml");
}

#[test]
fn client_create_dir_response_binds_and_restarts_converged() {
    run_fixture("client/client_create_dir_response_binds_and_restarts_converged.yaml");
}

fn run_fixture(relative_path: &str) {
    let scenario = load_fixture(relative_path);
    let initial: RoundTripInitial = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: RoundTripExpect = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-mutation-round-trip");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    assert_eq!(
        actions.len(),
        7,
        "fixture should contain the full round-trip"
    );
    let build = actions[0].build().expect("build action first");
    let record = actions[1].record().expect("record action second");
    let execute = actions[2].execute().expect("execute action third");
    assert!(actions[3].is_restart(), "restart should be fourth");
    assert!(actions[4].is_apply_response(), "apply should be fifth");
    assert!(actions[5].is_restart(), "restart should be sixth");
    let planner_tick = actions[6].planner().expect("planner tick seventh");

    let uploaded_content = initial.local_file.as_ref().map(|local_file| {
        let source_path = source_root.join(&local_file.relative_path);
        fs::create_dir_all(source_path.parent().expect("source file parent"))
            .expect("create source file parent");
        fs::write(&source_path, local_file.content_utf8.as_bytes()).expect("write source file");
        upload_small_file_from_path(&store, &initial.local_only_state.namespace_id, &source_path)
            .expect("upload local file before request build")
    });

    if let Some(uploaded) = uploaded_content.as_ref() {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB for upload ledger");
        db.record_local_only_upload(
            &initial.local_only_state.client_file_id,
            uploaded,
            initial.planned_local_only_action.created_at_ms,
        )
        .expect("record uploaded content in client state");
    }

    let request = {
        let db = SqliteStateDb::open(&db_path).expect("open client state DB for request build");
        build_client_mutation_request_from_state(
            &db,
            &build.client_request_id,
            &initial.local_only_state.client_file_id,
        )
        .expect("build client mutation request")
    };

    let response = {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        db.record_pending_client_mutation(
            &initial.local_only_state.client_file_id,
            &request,
            record.created_at_ms,
        )
        .expect("record pending client mutation");
        drop(db);

        support::seed_server_basis_for_request(&store, &request, &execute.writer_version);
        let executed = execute_client_mutation(
            &store,
            &request,
            &ClientMutationExecutionParams {
                writer_id: execute.writer_id,
                writer_version: execute.writer_version,
                now_ms: execute.now_ms,
            },
        )
        .expect("execute authoritative client mutation");

        executed.response
    };

    assert_eq!(response, expect.mutation_response);

    let bound_identity = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after restart");
        let bound = db
            .apply_client_mutation_response(&response)
            .expect("apply client mutation response");
        drop(db);
        bound
    };

    assert_eq!(bound_identity, expect.bound_identity);

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after bind");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan bound inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views"),
        loon_client::state_db::FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_local_only_file(&initial.local_only_state.client_file_id)
            .expect("load local-only state after bind"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load local-only plan after bind"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&expect.mutation_response.client_request_id)
            .expect("load pending client mutation after bind"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending mutation to clear");
        }
    );
    assert_eq!(
        db.load_local_only_upload(&initial.local_only_state.client_file_id)
            .expect("load local-only upload after bind"),
        None
    );
}

#[derive(Debug, Deserialize)]
struct RoundTripInitial {
    local_only_state: LocalOnlyFileStateRow,
    local_file: Option<FixtureLocalFile>,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct RoundTripExpect {
    mutation_response: ClientMutationResponse,
    bound_identity: BoundLocalOnlyFile,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_only_state_cleared: bool,
    planned_local_only_action_cleared: bool,
    pending_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FixtureAction {
    BuildClientMutation {
        build_client_mutation_request: BuildClientMutationRequestAction,
    },
    RecordPendingClientMutation {
        record_pending_client_mutation: RecordPendingClientMutationAction,
    },
    ExecuteClientMutation {
        execute_client_mutation: ExecuteClientMutationAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    ApplyClientMutationResponse {
        apply_client_mutation_response: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl FixtureAction {
    fn build(&self) -> Option<BuildClientMutationRequestAction> {
        match self {
            Self::BuildClientMutation {
                build_client_mutation_request,
            } => Some(build_client_mutation_request.clone()),
            _ => None,
        }
    }

    fn record(&self) -> Option<RecordPendingClientMutationAction> {
        match self {
            Self::RecordPendingClientMutation {
                record_pending_client_mutation,
            } => Some(record_pending_client_mutation.clone()),
            _ => None,
        }
    }

    fn execute(&self) -> Option<ExecuteClientMutationAction> {
        match self {
            Self::ExecuteClientMutation {
                execute_client_mutation,
            } => Some(execute_client_mutation.clone()),
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

    fn is_apply_response(&self) -> bool {
        matches!(
            self,
            Self::ApplyClientMutationResponse {
                apply_client_mutation_response: true
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
struct BuildClientMutationRequestAction {
    client_request_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RecordPendingClientMutationAction {
    created_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteClientMutationAction {
    writer_id: String,
    writer_version: String,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: loon_types::NamespaceId,
    inode_id: loon_types::InodeId,
    now_ms: u64,
}

fn seed_client_state(db_path: &Path, initial: &RoundTripInitial) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-round-trip-initial-state", |tx| {
        tx.upsert_local_only_file(&initial.local_only_state)?;
        tx.upsert_planned_local_only_action(&initial.planned_local_only_action)?;
        Ok(())
    })
    .expect("seed initial client state");
}

fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        "loon-server-test",
        head.clone(),
    )
    .expect("encode head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
    store
        .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("seed head object");

    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        "loon-server-test",
        lease.clone(),
    )
    .expect("encode lease envelope");
    let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
    store
        .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
        .expect("seed lease object");
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

type TestDir = loon_testkit::tempdir::TestDir;
