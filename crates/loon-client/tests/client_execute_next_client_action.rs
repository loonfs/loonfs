#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_next_client_action, ExecutedNextLocalOnlyCreate, NextClientAction,
};
use loon_client::planner::PlannedActionRecord;
use loon_client::state_db::{
    BoundLocalOnlyFile, ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyPlannedActionRow, PlannedActionRow, RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
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

#[test]
fn execute_next_client_action_local_only_create_binds_and_restarts_converged() {
    let scenario = load_fixture(
        "client/execute_next_client_action_local_only_create_binds_and_restarts_converged.yaml",
    );
    let initial: LocalOnlyInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<LocalOnlyFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: LocalOnlyExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-execute-next-client-action-local-only");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_local_only_client_state(
        &db_path,
        &initial.local_only_state,
        &initial.planned_local_only_action,
    );

    assert_eq!(
        actions.len(),
        3,
        "fixture should contain execute, restart, planner"
    );
    let execute = actions[0].execute().expect("execute action first");
    assert!(actions[1].is_restart(), "restart should be second");
    let planner_tick = actions[2].planner().expect("planner action third");

    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("local-only fixture should include source path"),
        &initial.local_file,
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(source_path.as_path()), &execute)
            .expect("one action should be scheduled");

    let local_only = match executed {
        NextClientAction::ExecutedLocalOnlyCreate(result) => result,
        other => panic!("expected executed local-only create, got {other:?}"),
    };

    assert_eq!(
        local_only.planned_action.client_file_id,
        expect.selected_client_file_id
    );
    let dispatched = match (&expect.executed, local_only.executed) {
        (
            LocalOnlyExpectedExecution::UploadLocalCreate {
                upload_local_create,
            },
            loon_client::executor::ExecutedLocalOnlyCreate::UploadLocalCreate(result),
        ) => {
            assert_eq!(result.upload_reused, upload_local_create.upload_reused);
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

#[test]
fn execute_next_client_action_selects_inode_planned_action() {
    let scenario =
        load_fixture("client/execute_next_client_action_selects_inode_planned_action.yaml");
    let initial: PlannedActionInitialState =
        scenario.decode_initial().expect("decode initial state");
    let actions: Vec<PlannedActionFixtureAction> =
        scenario.decode_actions().expect("decode actions");
    let expect: PlannedActionExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-execute-next-client-action-planned");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_planned_action_state(&db_path, &initial.planned_action);

    let execute = actions[0].execute().expect("execute action first");
    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        &ExecuteNextClientActionAction {
            source_path_relative: None,
            uploaded_at_ms: execute.uploaded_at_ms,
            created_at_ms: execute.created_at_ms,
            writer_id: "unused".to_owned(),
            writer_version: "unused".to_owned(),
            now_ms: 0,
        },
    )
    .expect("planned action should be selected");

    assert_eq!(
        executed,
        NextClientAction::SelectedPlannedAction(expect.selected_planned_action)
    );
}

#[test]
fn execute_next_client_action_returns_none_without_work() {
    let temp_dir = TestDir::new("client-execute-next-client-action-empty");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    fs::create_dir_all(&store_root).expect("create local object store root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let mut db = SqliteStateDb::open(&db_path).expect("open empty client state DB");

    let result = execute_next_client_action(
        &mut db,
        &store,
        |_client_file_id| unreachable!("source path resolver should not run without work"),
        |_namespace_id, _inode_id| {
            unreachable!("inode source path resolver should not run without work")
        },
        1_700_000_500_000,
        1_700_000_501_000,
        |_request| unreachable!("dispatch should not run without work"),
    )
    .expect("empty queue should return Ok(None)");

    assert_eq!(result, None);
}

#[test]
fn execute_next_client_action_prefers_local_only_create_over_older_inode_action() {
    let scenario = load_fixture(
        "client/execute_next_client_action_local_only_create_binds_and_restarts_converged.yaml",
    );
    let initial: LocalOnlyInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<LocalOnlyFixtureAction> = scenario.decode_actions().expect("decode actions");
    let execute = actions[0].execute().expect("execute action first");
    let temp_dir = TestDir::new("client-execute-next-client-action-tie");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_local_only_client_state(
        &db_path,
        &initial.local_only_state,
        &initial.planned_local_only_action,
    );
    {
        let mut db =
            SqliteStateDb::open(&db_path).expect("open client state DB to seed inode action");
        db.planner_transaction("seed-inode-planned-action", |tx| {
            tx.upsert_planned_action(&PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(77),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: initial.planned_local_only_action.created_at_ms - 1_000,
            })?;
            Ok(())
        })
        .expect("seed inode planned action");
    }

    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("tie fixture should include source path"),
        &initial.local_file,
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(source_path.as_path()), &execute)
            .expect("one action should be scheduled");

    match executed {
        NextClientAction::ExecutedLocalOnlyCreate(ExecutedNextLocalOnlyCreate {
            planned_action,
            ..
        }) => {
            assert_eq!(
                planned_action.client_file_id,
                initial.local_only_state.client_file_id
            );
        }
        other => panic!("expected local-only create to win tie, got {other:?}"),
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB after tick");
    assert_eq!(
        db.load_next_planned_action()
            .expect("load remaining inode planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(77),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: initial.planned_local_only_action.created_at_ms - 1_000,
        })
    );
}

#[test]
fn execute_next_client_action_prefers_local_only_create_on_equal_created_at_ms() {
    let scenario = load_fixture(
        "client/execute_next_client_action_local_only_create_binds_and_restarts_converged.yaml",
    );
    let initial: LocalOnlyInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<LocalOnlyFixtureAction> = scenario.decode_actions().expect("decode actions");
    let execute = actions[0].execute().expect("execute action first");
    let temp_dir = TestDir::new("client-execute-next-client-action-tie");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_local_only_client_state(
        &db_path,
        &initial.local_only_state,
        &initial.planned_local_only_action,
    );
    {
        let mut db =
            SqliteStateDb::open(&db_path).expect("open client state DB to seed inode action");
        db.planner_transaction("seed-inode-planned-action", |tx| {
            tx.upsert_planned_action(&PlannedActionRow {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(77),
                decision: "download_remote_edit".to_owned(),
                reason: "remote_differs_from_anchor".to_owned(),
                created_at_ms: initial.planned_local_only_action.created_at_ms,
            })?;
            Ok(())
        })
        .expect("seed inode planned action");
    }

    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("tie fixture should include source path"),
        &initial.local_file,
    );

    let executed =
        run_execute_next_client_action(&db_path, &store, Some(source_path.as_path()), &execute)
            .expect("one action should be scheduled");

    match executed {
        NextClientAction::ExecutedLocalOnlyCreate(ExecutedNextLocalOnlyCreate {
            planned_action,
            ..
        }) => {
            assert_eq!(
                planned_action.client_file_id,
                initial.local_only_state.client_file_id
            );
        }
        other => panic!("expected local-only create to win tie, got {other:?}"),
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen client state DB after tick");
    assert_eq!(
        db.load_next_planned_action()
            .expect("load remaining inode planned action"),
        Some(PlannedActionRow {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(77),
            decision: "download_remote_edit".to_owned(),
            reason: "remote_differs_from_anchor".to_owned(),
            created_at_ms: initial.planned_local_only_action.created_at_ms,
        })
    );
}

fn run_execute_next_client_action(
    db_path: &Path,
    store: &LocalFsStore,
    source_path: Option<&Path>,
    action: &ExecuteNextClientActionAction,
) -> Option<NextClientAction> {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_next_client_action(
        &mut db,
        store,
        |_client_file_id| source_path.map(Path::to_path_buf),
        |_namespace_id, _inode_id| None,
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
    .expect("execute next client action")
}

#[derive(Debug, Deserialize)]
struct LocalOnlyInitialState {
    local_only_state: LocalOnlyFileStateRow,
    local_file: FixtureLocalFile,
    planned_local_only_action: LocalOnlyPlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct LocalOnlyExpectedState {
    selected_client_file_id: ClientFileId,
    executed: LocalOnlyExpectedExecution,
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
enum LocalOnlyExpectedExecution {
    UploadLocalCreate {
        upload_local_create: ExpectedUploadLocalCreate,
    },
}

#[derive(Debug, Deserialize)]
struct ExpectedUploadLocalCreate {
    upload_reused: bool,
}

#[derive(Debug, Deserialize)]
struct PlannedActionInitialState {
    planned_action: PlannedActionRow,
}

#[derive(Debug, Deserialize)]
struct PlannedActionExpectedState {
    selected_planned_action: PlannedActionRow,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: PathBuf,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum LocalOnlyFixtureAction {
    ExecuteNextClientAction {
        execute_next_client_action: ExecuteNextClientActionAction,
    },
    RestartClientStateDb {
        restart_client_state_db: bool,
    },
    PlannerTick {
        planner_tick: PlannerTickAction,
    },
}

impl LocalOnlyFixtureAction {
    fn execute(&self) -> Option<ExecuteNextClientActionAction> {
        match self {
            Self::ExecuteNextClientAction {
                execute_next_client_action,
            } => Some(execute_next_client_action.clone()),
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

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PlannedActionFixtureAction {
    ExecuteNextClientAction {
        execute_next_client_action: PlannedOnlyAction,
    },
}

impl PlannedActionFixtureAction {
    fn execute(&self) -> Option<PlannedOnlyAction> {
        match self {
            Self::ExecuteNextClientAction {
                execute_next_client_action,
            } => Some(execute_next_client_action.clone()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteNextClientActionAction {
    source_path_relative: Option<PathBuf>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    writer_id: String,
    writer_version: String,
    now_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlannedOnlyAction {
    uploaded_at_ms: u64,
    created_at_ms: u64,
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
    CreateFile { create_file: ExpectedCreateFileOp },
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

fn seed_local_only_client_state(
    db_path: &Path,
    local_only_state: &LocalOnlyFileStateRow,
    planned_local_only_action: &LocalOnlyPlannedActionRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-execute-next-client-action-local-only-state", |tx| {
        tx.upsert_local_only_file(local_only_state)?;
        tx.upsert_planned_local_only_action(planned_local_only_action)?;
        Ok(())
    })
    .expect("seed local-only client state");
}

fn seed_planned_action_state(db_path: &Path, planned_action: &PlannedActionRow) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-execute-next-client-action-planned-state", |tx| {
        tx.upsert_planned_action(planned_action)?;
        Ok(())
    })
    .expect("seed planned action state");
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
    loon_testkit::fixtures::load_fixture(relative_path)
}

type TestDir = loon_testkit::tempdir::TestDir;
