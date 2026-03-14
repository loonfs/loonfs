#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_next_local_only_create, ExecutedLocalOnlyCreate, ExecutedNextLocalOnlyCreate,
};
use loon_client::planner::PlannedActionRecord;
use loon_client::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
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
fn execute_next_local_only_create_file_binds_and_restarts_converged() {
    run_fixture("client/execute_next_local_only_create_file_binds_and_restarts_converged.yaml");
}

#[test]
fn execute_next_local_only_create_dir_binds_and_restarts_converged() {
    run_fixture("client/execute_next_local_only_create_dir_binds_and_restarts_converged.yaml");
}

#[test]
fn execute_next_local_only_create_returns_none_without_work() {
    let temp_dir = TestDir::new("client-execute-next-local-only-create-empty");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let mut db = SqliteStateDb::open(&db_path).expect("open empty client state DB");

    let result = execute_next_local_only_create(
        &mut db,
        &store,
        |_client_file_id| unreachable!("source path resolver should not run without work"),
        1_700_000_400_000,
        1_700_000_401_000,
        |_request| unreachable!("dispatch should not run without work"),
    )
    .expect("empty queue should return Ok(None)");

    assert_eq!(result, None);
}

fn run_fixture(relative_path: &str) {
    let scenario = load_fixture(relative_path);
    let initial: InitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<FixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: ExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-execute-next-local-only-create");
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
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let source_path = match execute.source_path_relative.as_ref() {
        Some(relative_path) => Some(write_source_file(
            &source_root,
            relative_path,
            initial
                .local_file
                .as_ref()
                .expect("file fixture should include local_file"),
        )),
        None => None,
    };

    let executed = run_execute(&db_path, &store, source_path.as_deref(), &execute)
        .expect("one planned row should execute");

    assert_eq!(
        executed.planned_action.client_file_id,
        expect.selected_client_file_id
    );

    let dispatched = match (&expect.executed, executed.executed) {
        (
            ExpectedExecution::UploadLocalCreate {
                upload_local_create,
            },
            ExecutedLocalOnlyCreate::UploadLocalCreate(result),
        ) => {
            assert_eq!(result.upload_reused, upload_local_create.upload_reused);
            result.dispatched
        }
        (
            ExpectedExecution::CreateRemoteDir { create_remote_dir },
            ExecutedLocalOnlyCreate::CreateRemoteDir(result),
        ) => {
            assert_eq!(
                result.reused_pending_request,
                create_remote_dir.reused_pending_request
            );
            result.dispatched
        }
        (expected, actual) => {
            panic!("execution branch mismatch: expected {expected:?}, got {actual:?}")
        }
    };

    assert_eq!(
        dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(dispatched.request, expect.request.clone().into_request());
    assert_eq!(dispatched.response, expect.mutation_response.clone());
    assert_eq!(dispatched.bound_identity, expect.bound_identity.clone());

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
        db.load_local_only_file(&expect.selected_client_file_id)
            .expect("load temp local-only state"),
        if expect.local_only_state_cleared {
            None
        } else {
            Some(initial.local_only_state.clone())
        }
    );
    assert_eq!(
        db.load_planned_local_only_action(&expect.selected_client_file_id)
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
}

fn run_execute(
    db_path: &Path,
    store: &LocalFsStore,
    source_path: Option<&Path>,
    action: &ExecuteNextLocalOnlyCreateAction,
) -> Option<ExecutedNextLocalOnlyCreate> {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_next_local_only_create(
        &mut db,
        store,
        |_client_file_id| source_path.map(Path::to_path_buf),
        action.uploaded_at_ms,
        action.created_at_ms,
        |request| {
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
        },
    )
    .expect("execute next local-only create")
}

#[derive(Debug, Deserialize)]
struct InitialState {
    local_only_state: LocalOnlyFileStateRow,
    local_file: Option<FixtureLocalFile>,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct ExpectedState {
    selected_client_file_id: ClientFileId,
    executed: ExpectedExecution,
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
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FixtureAction {
    ExecuteNextLocalOnlyCreate {
        execute_next_local_only_create: ExecuteNextLocalOnlyCreateAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl FixtureAction {
    fn execute(&self) -> Option<ExecuteNextLocalOnlyCreateAction> {
        match self {
            Self::ExecuteNextLocalOnlyCreate {
                execute_next_local_only_create,
            } => Some(execute_next_local_only_create.clone()),
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
struct ExecuteNextLocalOnlyCreateAction {
    source_path_relative: Option<PathBuf>,
    uploaded_at_ms: u64,
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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExpectedExecution {
    UploadLocalCreate {
        upload_local_create: ExpectedUploadLocalCreate,
    },
    CreateRemoteDir {
        create_remote_dir: ExpectedCreateRemoteDir,
    },
}

#[derive(Debug, Deserialize)]
struct ExpectedUploadLocalCreate {
    upload_reused: bool,
}

#[derive(Debug, Deserialize)]
struct ExpectedCreateRemoteDir {
    reused_pending_request: bool,
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
    CreateFile { create_file: ExpectedCreateFileOp },
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateDirOp {
    parent_inode_id: InodeId,
    display_name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedCreateFileOp {
    parent_inode_id: InodeId,
    display_name: String,
    content_manifest_digest: String,
}

impl ExpectedRequest {
    fn into_request(self) -> ClientMutationRequest {
        let op = match self.op {
            ExpectedRequestOp::CreateDir { create_dir } => ClientMutationOp::CreateDir {
                parent_inode_id: create_dir.parent_inode_id,
                display_name: create_dir.display_name,
            },
            ExpectedRequestOp::CreateFile { create_file } => ClientMutationOp::CreateFile {
                parent_inode_id: create_file.parent_inode_id,
                display_name: create_file.display_name,
                content_manifest_digest: create_file.content_manifest_digest,
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
    db.planner_transaction("seed-execute-next-local-only-create-initial-state", |tx| {
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

fn write_source_file(
    source_root: &Path,
    relative_path: &Path,
    local_file: &FixtureLocalFile,
) -> PathBuf {
    assert_eq!(
        relative_path,
        local_file.relative_path.as_path(),
        "fixture source path should match local_file.relative_path"
    );
    let source_path = source_root.join(relative_path);
    fs::create_dir_all(source_path.parent().expect("source file parent"))
        .expect("create source file parent");
    fs::write(&source_path, local_file.content_utf8.as_bytes()).expect("write source file");
    source_path
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
