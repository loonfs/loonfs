#[path = "common/support.rs"]
mod support;

use loon_client::executor::{execute_next_client_action, NextClientAction};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedInodeMutation, FileSyncViews, InodeUploadRow, LocalFileStateRow, PlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow,
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
fn execute_next_client_action_upload_local_edit_updates_bound_inode() {
    let scenario = load_fixture(
        "client/execute_next_client_action_upload_local_edit_updates_bound_inode.yaml",
    );
    let initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: EditExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-edit");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
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
            .expect("edit fixture should include source path"),
        &initial.local_file,
    );

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        &execute,
    )
    .expect("one action should be scheduled");

    let edit = match executed {
        Some(NextClientAction::ExecutedUploadLocalEdit(result)) => result,
        other => panic!("expected executed upload_local_edit, got {other:?}"),
    };

    assert_eq!(edit.ensured_upload, Some(expect.inode_upload.clone()));
    assert!(
        !edit.upload_reused,
        "first edit should upload fresh content"
    );
    assert_eq!(
        edit.dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(
        edit.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(edit.dispatched.response, expect.mutation_response.clone());
    assert_eq!(
        edit.dispatched.applied,
        expect.applied_inode_mutation.clone()
    );

    let mut db = SqliteStateDb::open(&db_path).expect("reopen client state DB after execute");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan edited inode after restart");

    assert_eq!(planner_result, expect.planner_result);
    assert_eq!(
        db.load_file_sync_views(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load converged views"),
        FileSyncViews {
            namespace_id: planner_tick.namespace_id.clone(),
            inode_id: planner_tick.inode_id,
            remote: Some(expect.remote_state.clone()),
            local: Some(expect.local_state.clone()),
            sync_anchor: Some(expect.sync_anchor.clone()),
        }
    );
    assert_eq!(
        db.load_inode_upload(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load inode upload row"),
        Some(expect.inode_upload.clone())
    );
    assert_eq!(
        db.load_planned_action(&planner_tick.namespace_id, planner_tick.inode_id)
            .expect("load planned action after success"),
        if expect.planned_action_cleared {
            None
        } else {
            Some(initial.planned_action.clone())
        }
    );
    assert_eq!(
        db.load_pending_inode_mutation(&expect.pending_request_id)
            .expect("load pending inode mutation"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending inode mutation to clear");
        }
    );
}

#[test]
fn execute_next_client_action_retries_upload_local_edit_without_source_path_once_pending() {
    let scenario = load_fixture(
        "client/execute_next_client_action_upload_local_edit_updates_bound_inode.yaml",
    );
    let initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: EditExpectedState = scenario.decode_expect().expect("decode expectations");
    let execute = actions[0].execute().expect("execute action first");
    let temp_dir = TestDir::new("client-upload-local-edit-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
    );

    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("retry fixture should include source path"),
        &initial.local_file,
    );

    let failing_action = ExecuteNextClientActionAction {
        source_path_relative: execute.source_path_relative.clone(),
        uploaded_at_ms: execute.uploaded_at_ms,
        created_at_ms: execute.created_at_ms,
        writer_id: "dispatch-fails".to_owned(),
        writer_version: execute.writer_version.clone(),
        now_ms: execute.now_ms,
    };

    let first_error = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        &failing_action,
    )
    .expect_err("first dispatch should fail");
    let first_error_message = first_error.to_string();
    assert!(
        first_error_message.contains("dispatch_failed"),
        "unexpected first error: {first_error_message}"
    );

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after failed first dispatch");
    let pending = db
        .load_pending_inode_mutation(&expect.pending_request_id)
        .expect("load persisted pending inode mutation")
        .expect("pending inode mutation should persist after failed dispatch");
    assert_eq!(pending.request, expect.request.clone().into_request());
    assert_eq!(
        db.load_inode_upload(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load persisted inode upload"),
        Some(expect.inode_upload.clone())
    );
    drop(db);

    let executed = {
        let mut db = SqliteStateDb::open(&db_path).expect("reopen DB for retry");
        execute_next_client_action(
            &mut db,
            &store,
            |_client_file_id| None,
            |_namespace_id, _inode_id| None,
            execute.uploaded_at_ms,
            execute.created_at_ms,
            |request| {
                execute_client_mutation(
                    &store,
                    request,
                    &ClientMutationExecutionParams {
                        writer_id: execute.writer_id.clone(),
                        writer_version: execute.writer_version.clone(),
                        now_ms: execute.now_ms,
                        metadata_state: support::server_metadata_for_request(request),
                    },
                )
                .map(|executed| executed.response)
                .map_err(|err| err.to_string())
            },
        )
        .expect("retry should succeed")
        .expect("retry should execute one action")
    };

    let edit = match executed {
        NextClientAction::ExecutedUploadLocalEdit(result) => result,
        other => panic!("expected executed upload_local_edit on retry, got {other:?}"),
    };

    assert_eq!(
        edit.dispatched.pending.client_request_id,
        expect.pending_request_id
    );
    assert_eq!(
        edit.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(edit.dispatched.response, expect.mutation_response.clone());
    assert_eq!(edit.dispatched.applied, expect.applied_inode_mutation);
    assert!(edit.upload_reused, "retry should reuse uploaded content");
    assert_eq!(edit.ensured_upload, Some(expect.inode_upload));
}

fn run_execute_next_client_action(
    db_path: &Path,
    store: &LocalFsStore,
    local_only_source_path: Option<&Path>,
    inode_source_path: Option<&Path>,
    action: &ExecuteNextClientActionAction,
) -> Result<Option<NextClientAction>, loon_client::executor::ExecuteNextClientActionError> {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    execute_next_client_action(
        &mut db,
        store,
        |_client_file_id| local_only_source_path.map(Path::to_path_buf),
        |_namespace_id, _inode_id| inode_source_path.map(Path::to_path_buf),
        action.uploaded_at_ms,
        action.created_at_ms,
        |request| {
            if action.writer_id == "dispatch-fails" {
                return Err("transient dispatcher failure".to_owned());
            }
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
}

fn seed_bound_edit_state(
    db_path: &Path,
    remote: &RemoteFileStateRow,
    local: &LocalFileStateRow,
    sync_anchor: &SyncAnchorRow,
    planned_action: &PlannedActionRow,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-bound-edit-state", |tx| {
        tx.upsert_remote_file(remote)?;
        tx.upsert_local_file(local)?;
        tx.upsert_sync_anchor(sync_anchor)?;
        tx.upsert_planned_action(planned_action)?;
        Ok(())
    })
    .expect("seed bound file edit state");
}

fn write_source_file(root: &Path, relative_path: &str, local_file: &FixtureLocalFile) -> PathBuf {
    assert_eq!(
        local_file.relative_path, relative_path,
        "fixture local_file.relative_path should match action source path"
    );
    let path = root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create source file parent directories");
    }
    fs::write(&path, local_file.content_utf8.as_bytes()).expect("write source file");
    path
}

fn load_fixture(relative_path: &str) -> Scenario {
    loon_testkit::fixtures::load_fixture(relative_path)
}

fn seed_head_and_lease(store: &LocalFsStore, head: &HeadState, lease: &LeaseState) {
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        "loon-client-test",
        head.clone(),
    )
    .expect("encode head envelope");
    let head_bytes = serde_json::to_vec(&head_envelope).expect("serialize head envelope");
    store
        .put_if_absent(&namespace_head(head.namespace_id.as_str()), &head_bytes)
        .expect("seed head object");

    let lease_envelope = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        "loon-client-test",
        lease.clone(),
    )
    .expect("encode lease envelope");
    let lease_bytes = serde_json::to_vec(&lease_envelope).expect("serialize lease envelope");
    store
        .put_if_absent(&namespace_lease(lease.namespace_id.as_str()), &lease_bytes)
        .expect("seed lease object");
}

#[derive(Debug, Deserialize)]
struct EditInitialState {
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    local_file: FixtureLocalFile,
    planned_action: PlannedActionRow,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: String,
    content_utf8: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EditFixtureAction {
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

impl EditFixtureAction {
    fn execute(&self) -> Option<&ExecuteNextClientActionAction> {
        match self {
            Self::ExecuteNextClientAction {
                execute_next_client_action,
            } => Some(execute_next_client_action),
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

    fn planner(&self) -> Option<&PlannerTickAction> {
        match self {
            Self::PlannerTick { planner_tick } => Some(planner_tick),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteNextClientActionAction {
    source_path_relative: Option<String>,
    uploaded_at_ms: u64,
    created_at_ms: u64,
    writer_id: String,
    writer_version: String,
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct PlannerTickAction {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    now_ms: u64,
}

#[derive(Debug, Deserialize)]
struct EditExpectedState {
    pending_request_id: String,
    request: RawClientMutationRequest,
    mutation_response: ClientMutationResponse,
    applied_inode_mutation: AppliedInodeMutation,
    inode_upload: InodeUploadRow,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    pending_mutation_cleared: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationRequest {
    namespace_id: NamespaceId,
    client_request_id: String,
    op: RawClientMutationOp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum RawClientMutationOp {
    ReplaceFile { replace_file: ExpectedReplaceFileOp },
}

#[derive(Debug, Clone, Deserialize)]
struct ExpectedReplaceFileOp {
    inode_id: InodeId,
    base_revision_no: loon_types::RevisionNo,
    content_manifest_digest: String,
}

impl RawClientMutationRequest {
    fn into_request(self) -> ClientMutationRequest {
        ClientMutationRequest {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            op: match self.op {
                RawClientMutationOp::ReplaceFile { replace_file } => {
                    ClientMutationOp::ReplaceFile {
                        inode_id: replace_file.inode_id,
                        base_revision_no: replace_file.base_revision_no,
                        content_manifest_digest: replace_file.content_manifest_digest,
                    }
                }
            },
        }
    }
}

type TestDir = loon_testkit::tempdir::TestDir;
