#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_create_remote_dir, ExecuteCreateRemoteDirError, ExecutedCreateRemoteDir,
};
use loon_client::planner::PlannedActionRecord;
use loon_client::state_db::{
    BoundLocalOnlyFile, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyPlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, StateDbError, SyncAnchorRow,
};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_testkit::scenario::Scenario;
use loon_types::{
    ClientMutationOp, ClientMutationRequest, ClientMutationResponse, ControlObjectKind, HeadState,
    HeadStateEnvelope, InodeId, LeaseState, LeaseStateEnvelope, NamespaceId,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn create_remote_dir_binds_and_restarts_converged() {
    let scenario =
        load_fixture("client/create_remote_dir_executor_binds_and_restarts_converged.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-create-remote-dir");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let executed = run_execute(
        &db_path,
        &store,
        &initial.local_only_state.client_file_id,
        &execute,
    );

    assert_eq!(
        executed.reused_pending_request,
        expect.reused_pending_request
    );
    assert_eq!(
        executed.dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(
        executed.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(
        executed.dispatched.response,
        expect.mutation_response.clone()
    );
    assert_eq!(
        executed.dispatched.bound_identity,
        expect.bound_identity.clone()
    );

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = loon_client::planner::plan_file(
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
            .expect("load temp local-only state"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&initial.local_only_state.client_file_id)
            .expect("load temp planned action"),
        if expect.planned_local_only_action_cleared {
            None
        } else {
            Some(initial.planned_local_only_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_client_mutation(&expect.pending_request_id)
            .expect("load pending mutation"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending mutation to clear");
        }
    );

    let error = execute_create_remote_dir(
        &mut db,
        &initial.local_only_state.client_file_id,
        execute.created_at_ms,
        |_request| unreachable!("second call should fail before dispatch"),
    )
    .expect_err("second call should return explicit temp-identity error");
    assert!(matches!(
        error,
        ExecuteCreateRemoteDirError::StateDb(StateDbError::LocalOnlyFileMissing { .. })
    ));
}

#[test]
fn create_remote_dir_retry_reuses_pending_request_without_reloading_plan() {
    let scenario =
        load_fixture("client/create_remote_dir_executor_binds_and_restarts_converged.yaml");
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let temp_dir = TestDir::new("client-create-remote-dir-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_client_state(&db_path, &initial);

    let execute = actions[0].execute().expect("execute action first");

    {
        let mut db = SqliteStateDb::open(&db_path).expect("open client state DB");
        let error = execute_create_remote_dir(
            &mut db,
            &initial.local_only_state.client_file_id,
            execute.created_at_ms,
            |_request| Err("temporary dispatch failure".to_owned()),
        )
        .expect_err("first dispatch should fail");
        let message = error.to_string();
        assert!(message.contains("temporary dispatch failure"));
    }

    {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after failure");
        db.planner_transaction("delete-local-only-plan-before-dir-retry", |tx| {
            tx.delete_planned_local_only_action(&initial.local_only_state.client_file_id)?;
            Ok(())
        })
        .expect("delete planned action before retry");
    }

    let executed = run_execute(
        &db_path,
        &store,
        &initial.local_only_state.client_file_id,
        &execute,
    );

    assert!(executed.reused_pending_request);
    assert_eq!(
        executed.dispatched.pending.client_request_id,
        "client-req-00000000000000000001"
    );
    assert_eq!(
        executed.dispatched.request.client_request_id,
        "client-req-00000000000000000001"
    );
}

fn run_execute(
    db_path: &Path,
    store: &LocalFsStore,
    client_file_id: &loon_client::state_db::ClientFileId,
    action: &ExecuteCreateRemoteDirAction,
) -> ExecutedCreateRemoteDir {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_create_remote_dir(&mut db, client_file_id, action.created_at_ms, |request| {
        execute_client_mutation(
            store,
            request,
            &ClientMutationExecutionParams {
                writer_id: action.writer_id.clone(),
                writer_version: action.writer_version.clone(),
                now_ms: action.now_ms,
                metadata_state: support::server_metadata_for_request(request),
            },
        )
        .map(|executed| executed.response)
        .map_err(|err| err.to_string())
    })
    .expect("execute create_remote_dir")
}

#[derive(Debug, Deserialize)]
struct InitialState {
    local_only_state: LocalOnlyFileStateRow,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct ExpectedState {
    reused_pending_request: bool,
    pending_request_id: String,
    request: ExpectedRequest,
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
#[serde(untagged)]
enum FixtureAction {
    ExecuteCreateRemoteDir {
        execute_create_remote_dir: ExecuteCreateRemoteDirAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl FixtureAction {
    fn execute(&self) -> Option<ExecuteCreateRemoteDirAction> {
        match self {
            Self::ExecuteCreateRemoteDir {
                execute_create_remote_dir,
            } => Some(execute_create_remote_dir.clone()),
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
struct ExecuteCreateRemoteDirAction {
    created_at_ms: u64,
    writer_id: String,
    writer_version: String,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: ExpectedRequestOp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ExpectedRequestOp {
    CreateDir { create_dir: ExpectedCreateDirOp },
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateDirOp {
    parent_inode_id: InodeId,
    display_name: String,
}

impl ExpectedRequest {
    fn into_request(self) -> ClientMutationRequest {
        let op = match self.op {
            ExpectedRequestOp::CreateDir { create_dir } => ClientMutationOp::CreateDir {
                parent_inode_id: create_dir.parent_inode_id,
                display_name: create_dir.display_name,
            },
        };

        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op,
        }
    }
}

fn seed_client_state(db_path: &Path, initial: &InitialState) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-create-remote-dir-initial-state", |tx| {
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
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/scenarios")
        .join(relative_path);
    Scenario::load(&path).unwrap_or_else(|err| panic!("load fixture {}: {err}", path.display()))
}

#[derive(Debug)]
struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(label: &str) -> Self {
        static NEXT_TEST_DIR_ID: AtomicU64 = AtomicU64::new(1);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "loondb-client-{label}-{}-{id}-{stamp}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
