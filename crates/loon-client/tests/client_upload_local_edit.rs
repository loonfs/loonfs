#[path = "common/support.rs"]
mod support;

use loon_client::executor::{
    execute_next_client_action, ExecuteNextClientActionError, ExecuteUploadLocalEditError,
    NextClientAction, UploadLocalEditExecution,
};
use loon_client::planner::{plan_file, PlannedActionRecord};
use loon_client::state_db::{
    AppliedInodeMutation, FileSyncViews, InodeUploadRow, LocalFileStateRow, PlannedActionRow,
    RemoteFileStateRow, SqliteStateDb, SyncAnchorRow, TransferDirection, TransferLedgerRow,
    TransferState,
};
use loon_client::upload::{upload_small_file_from_path, UploadError, UploadedContent};
use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_head, namespace_lease};
use loon_objectstore::ObjectStore;
use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
use loon_testkit::scenario::Scenario;
use loon_types::{
    ChangeSeq, ClientMutationOp, ClientMutationRequest, ClientMutationResponse, ControlObjectKind,
    HeadState, HeadStateEnvelope, InodeId, LeaseState, LeaseStateEnvelope, NamespaceId,
    ReplacedRemoteFile, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn execute_next_client_action_upload_local_edit_updates_bound_inode() {
    run_upload_local_edit_fixture(
        "client/execute_next_client_action_upload_local_edit_updates_bound_inode.yaml",
        "client-upload-local-edit",
    );
}

#[test]
fn execute_next_client_action_resumes_upload_local_edit_from_transfer_ledger() {
    run_upload_local_edit_fixture(
        "client/execute_next_client_action_resumes_upload_local_edit_from_transfer_ledger.yaml",
        "client-upload-local-edit-resume",
    );
}

#[test]
fn execute_next_client_action_multitick_upload_local_edit_survives_restart() {
    let scenario = load_fixture(
        "client/execute_next_client_action_multitick_upload_local_edit_survives_restart.yaml",
    );
    let mut initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: EditExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-edit-multitick");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    let first_execute = actions[0].execute().expect("first execute");
    assert!(actions[1].is_restart(), "restart should be second");
    let second_execute = actions[2].execute().expect("second execute");
    assert!(actions[3].is_restart(), "restart should be fourth");
    let planner_tick = actions[4].planner().expect("planner tick fifth");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    let source_path = write_source_file(
        &source_root,
        first_execute
            .source_path_relative
            .as_ref()
            .expect("fixture should include source path"),
        &initial.local_file,
    );
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.remote_state.namespace_id,
        &source_path,
    )
    .expect("plan expected uploaded content");
    fill_upload_expectations(&mut initial, &mut expect, &expected_upload);
    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
        None,
    );

    let first = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        first_execute,
    )
    .expect("first action should execute");
    let first_transfer = match first {
        Some(NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progressed,
        ))) => progressed.transfer,
        other => panic!("expected progressed upload_local_edit on first tick, got {other:?}"),
    };
    assert_eq!(first_transfer.block_index, 1);
    assert_eq!(first_transfer.block_count, 2);

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after first tick");
    assert_eq!(
        db.load_transfer_ledger_for_inode(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            TransferDirection::Upload,
        )
        .expect("load transfer ledger after first tick"),
        Some(first_transfer.clone())
    );
    assert_eq!(
        db.load_inode_upload(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load inode upload after first tick"),
        None
    );
    assert_eq!(
        db.load_pending_inode_mutation(
            expect
                .pending_request_id
                .as_ref()
                .expect("expected pending request id"),
        )
        .expect("load pending inode mutation after first tick"),
        None
    );
    drop(db);

    let second = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        second_execute,
    )
    .expect("second action should execute");
    match second {
        Some(NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Completed(
            edit,
        ))) => {
            assert_eq!(
                edit.ensured_upload,
                Some(expect.inode_upload.clone().into_row())
            );
        }
        other => panic!("expected completed upload_local_edit on second tick, got {other:?}"),
    }

    let mut db = SqliteStateDb::open(&db_path).expect("reopen DB after second tick");
    let planner_result = plan_file(
        &mut db,
        &planner_tick.namespace_id,
        planner_tick.inode_id,
        planner_tick.now_ms,
    )
    .expect("plan converged inode after restart");
    assert_eq!(planner_result, expect.planner_result);
}

#[test]
fn execute_next_client_action_stale_upload_local_edit_transfer_resets_and_records_issue() {
    let scenario = load_fixture(
        "client/execute_next_client_action_stale_upload_local_edit_transfer_resets_and_records_issue.yaml",
    );
    let initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: EditProgressExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new("client-upload-local-edit-reset");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    let execute = actions[0].execute().expect("execute action first");
    seed_head_and_lease(&store, &initial.head, &initial.lease);
    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("fixture should include source path"),
        &initial.local_file,
    );
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.remote_state.namespace_id,
        &source_path,
    )
    .expect("plan expected uploaded content");
    let transfer_row = initial
        .transfer_ledger
        .as_ref()
        .map(|seed| seed_transfer_ledger_row(&initial, seed, &expected_upload));
    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
        transfer_row.as_ref(),
    );
    if let Some(seed) = &initial.transfer_ledger {
        seed_uploaded_prefix_for_transfer(&store, &source_path, &expected_upload, seed.block_index);
    }

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        execute,
    )
    .expect("one action should execute");
    let transfer = match executed {
        Some(NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progressed,
        ))) => progressed.transfer,
        other => panic!("expected progressed upload_local_edit after reset, got {other:?}"),
    };
    assert_eq!(transfer.block_index, expect.transfer_ledger.block_index);
    assert_eq!(transfer.block_count, expect.transfer_ledger.block_count);

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after reset");
    let issues = db
        .load_conflicts_and_errors(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
        )
        .expect("load transfer reset issue");
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].kind, expect.issue.kind);
    assert_eq!(
        issues[0].detail_json["reason"].as_str(),
        Some(expect.issue.reason.as_str())
    );
}

#[test]
fn execute_next_client_action_retries_upload_local_edit_without_source_path_once_pending() {
    let scenario = load_fixture(
        "client/execute_next_client_action_upload_local_edit_updates_bound_inode.yaml",
    );
    let mut initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: EditExpectedState = scenario.decode_expect().expect("decode expectations");
    let execute = actions[0].execute().expect("execute action first");
    let temp_dir = TestDir::new("client-upload-local-edit-retry");
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);
    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("retry fixture should include source path"),
        &initial.local_file,
    );
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.remote_state.namespace_id,
        &source_path,
    )
    .expect("plan expected uploaded content");
    fill_upload_expectations(&mut initial, &mut expect, &expected_upload);

    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
        None,
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
        .load_pending_inode_mutation(
            expect
                .pending_request_id
                .as_ref()
                .expect("expected pending request id"),
        )
        .expect("load persisted pending inode mutation")
        .expect("pending inode mutation should persist after failed dispatch");
    assert_eq!(pending.request, expect.request.clone().into_request());
    assert_eq!(
        db.load_inode_upload(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load persisted inode upload"),
        Some(expect.inode_upload.clone().into_row())
    );
    assert_eq!(
        db.load_transfer_ledger_for_inode(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            TransferDirection::Upload,
        )
        .expect("load upload transfer ledger after completed upload"),
        None
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
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Completed(result)) => {
            result
        }
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progress,
        )) => {
            panic!("expected completed upload_local_edit on retry, got {progress:?}")
        }
        other => panic!("expected executed upload_local_edit on retry, got {other:?}"),
    };

    assert_eq!(
        edit.dispatched.pending.client_request_id,
        expect
            .pending_request_id
            .as_deref()
            .expect("expected pending request id")
    );
    assert_eq!(
        edit.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(
        edit.dispatched.response,
        expect.mutation_response.clone().into_response()
    );
    assert_eq!(
        edit.dispatched.applied,
        expect
            .applied_inode_mutation
            .clone()
            .expect("expected applied inode mutation")
    );
    assert!(edit.upload_reused, "retry should reuse uploaded content");
    assert_eq!(edit.ensured_upload, Some(expect.inode_upload.into_row()));
}

#[test]
fn execute_next_client_action_upload_local_edit_missing_file_records_issue_and_clears_on_recovery()
{
    let scenario = load_fixture(
        "client/execute_next_client_action_upload_local_edit_missing_file_records_issue.yaml",
    );
    let initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let expect: EditFailureExpectedState = scenario.decode_expect().expect("decode expectations");
    let execute = actions[0].execute().expect("execute action first");
    let temp_dir = TestDir::new("client-upload-local-edit-missing-file");
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
        None,
    );

    let missing_source_path = source_root.join(
        execute
            .source_path_relative
            .as_deref()
            .expect("failure fixture should include source path"),
    );

    let error = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(missing_source_path.as_path()),
        execute,
    )
    .expect_err("missing source file should fail");
    assert!(matches!(
        error,
        ExecuteNextClientActionError::UploadLocalEdit(ExecuteUploadLocalEditError::Upload(
            UploadError::LocalFileRead { .. }
        ))
    ));

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after missing-file failure");
    assert_eq!(
        db.load_planned_action(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load planned action after missing-file failure"),
        if expect.planned_action_retained {
            Some(initial.planned_action.clone())
        } else {
            None
        }
    );
    assert_eq!(
        db.load_inode_upload(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load inode upload after missing-file failure"),
        None
    );
    let issues = db
        .load_conflicts_and_errors(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
        )
        .expect("load persisted upload failure issue");
    assert_eq!(
        issues.len(),
        1,
        "expected one persisted upload failure issue"
    );
    let issue = &issues[0];
    assert_eq!(issue.kind, expect.issue.kind);
    assert_eq!(issue.summary, expect.issue.summary);
    assert_eq!(
        issue.detail_json["failure"].as_str(),
        Some(expect.issue.failure.as_str())
    );
    let recorded_path = issue.detail_json["path"]
        .as_str()
        .expect("upload failure issue path should be a string");
    assert!(
        recorded_path.ends_with(
            execute
                .source_path_relative
                .as_deref()
                .expect("failure fixture should include source path")
        ),
        "expected `{recorded_path}` to end with source path"
    );
    drop(db);

    let source_path = write_source_file(
        &source_root,
        execute
            .source_path_relative
            .as_ref()
            .expect("failure fixture should include source path"),
        &initial.local_file,
    );

    let recovered = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        execute,
    )
    .expect("retry after recreating source file should succeed")
    .expect("retry should execute one action");

    match recovered {
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Completed(_)) => {}
        NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progress,
        )) => {
            panic!("expected completed upload_local_edit on recovery, got {progress:?}")
        }
        other => panic!("expected executed upload_local_edit on recovery, got {other:?}"),
    }

    let db = SqliteStateDb::open(&db_path).expect("reopen DB after recovered upload");
    assert_eq!(
        db.load_conflicts_and_errors(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load conflicts and errors after successful recovery"),
        Vec::new()
    );
    assert!(
        db.load_inode_upload(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load inode upload after successful recovery")
        .is_some(),
        "successful recovery should record inode upload"
    );
    assert_eq!(
        db.load_planned_action(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id
        )
        .expect("load planned action after successful recovery"),
        None
    );
}

fn run_upload_local_edit_fixture(relative_path: &str, temp_label: &str) {
    let scenario = load_fixture(relative_path);
    let mut initial: EditInitialState = scenario.decode_initial().expect("decode initial state");
    let actions: Vec<EditFixtureAction> = scenario.decode_actions().expect("decode actions");
    let mut expect: EditExpectedState = scenario.decode_expect().expect("decode expectations");
    let temp_dir = TestDir::new(temp_label);
    let db_path = temp_dir.path().join("client.sqlite3");
    let store_root = temp_dir.path().join("objectstore");
    let scratch_store_root = temp_dir.path().join("scratch-objectstore");
    let source_root = temp_dir.path().join("source");
    fs::create_dir_all(&store_root).expect("create local object store root");
    fs::create_dir_all(&scratch_store_root).expect("create scratch object store root");
    fs::create_dir_all(&source_root).expect("create local source root");
    let store = LocalFsStore::new(&store_root).expect("create local object store");
    let scratch_store =
        LocalFsStore::new(&scratch_store_root).expect("create scratch local object store");

    seed_head_and_lease(&store, &initial.head, &initial.lease);

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
    let expected_upload = upload_small_file_from_path(
        &scratch_store,
        &initial.remote_state.namespace_id,
        &source_path,
    )
    .expect("plan expected uploaded content");
    fill_upload_expectations(&mut initial, &mut expect, &expected_upload);

    let transfer_row = initial
        .transfer_ledger
        .as_ref()
        .map(|seed| seed_transfer_ledger_row(&initial, seed, &expected_upload));
    seed_bound_edit_state(
        &db_path,
        &initial.remote_state,
        &initial.local_state,
        &initial.sync_anchor,
        &initial.planned_action,
        transfer_row.as_ref(),
    );
    if let Some(seed) = &initial.transfer_ledger {
        seed_uploaded_prefix_for_transfer(&store, &source_path, &expected_upload, seed.block_index);
    }

    let executed = run_execute_next_client_action(
        &db_path,
        &store,
        None,
        Some(source_path.as_path()),
        execute,
    )
    .expect("one action should be scheduled");

    let edit = match executed {
        Some(NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Completed(
            result,
        ))) => result,
        Some(NextClientAction::ExecutedUploadLocalEdit(UploadLocalEditExecution::Progressed(
            progress,
        ))) => {
            panic!("expected completed upload_local_edit, got {progress:?}")
        }
        other => panic!("expected executed upload_local_edit, got {other:?}"),
    };

    assert_eq!(
        edit.ensured_upload,
        Some(expect.inode_upload.clone().into_row())
    );
    assert_eq!(edit.upload_reused, expect.upload_reused);
    assert_eq!(
        edit.dispatched.pending.client_request_id,
        expect
            .pending_request_id
            .as_deref()
            .expect("expected pending request id")
    );
    assert_eq!(
        edit.dispatched.request,
        expect.request.clone().into_request()
    );
    assert_eq!(
        edit.dispatched.response,
        expect.mutation_response.clone().into_response()
    );
    assert_eq!(
        edit.dispatched.applied,
        expect
            .applied_inode_mutation
            .clone()
            .expect("expected applied inode mutation")
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
        Some(expect.inode_upload.clone().into_row())
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
        db.load_pending_inode_mutation(
            expect
                .pending_request_id
                .as_deref()
                .expect("expected pending request id"),
        )
        .expect("load pending inode mutation"),
        if expect.pending_mutation_cleared {
            None
        } else {
            panic!("fixture currently expects pending inode mutation to clear");
        }
    );
    if expect.transfer_ledger_cleared {
        assert_eq!(
            db.load_transfer_ledger_for_inode(
                &planner_tick.namespace_id,
                planner_tick.inode_id,
                TransferDirection::Upload,
            )
            .expect("load upload transfer ledger after success"),
            None
        );
    }
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
    transfer_ledger: Option<&TransferLedgerRow>,
) {
    let mut db = SqliteStateDb::open(db_path).expect("open client state DB");
    db.planner_transaction("seed-bound-edit-state", |tx| {
        tx.upsert_remote_file(remote)?;
        tx.upsert_local_file(local)?;
        tx.upsert_sync_anchor(sync_anchor)?;
        tx.upsert_planned_action(planned_action)?;
        if let Some(transfer_ledger) = transfer_ledger {
            tx.upsert_transfer_ledger(transfer_ledger)?;
        }
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
    fs::write(&path, local_file.file_bytes()).expect("write source file");
    path
}

fn fill_upload_expectations(
    initial: &mut EditInitialState,
    expect: &mut EditExpectedState,
    uploaded: &UploadedContent,
) {
    match &initial.local_state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => initial.local_state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &mut expect.request.op {
        RawClientMutationOp::ReplaceFile { replace_file } => {
            match &replace_file.content_manifest_digest {
                Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
                None => {
                    replace_file.content_manifest_digest =
                        Some(uploaded.content_manifest_digest.clone())
                }
            }
        }
    }
    match &expect.mutation_response.replaced_file.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => {
            expect.mutation_response.replaced_file.content_digest =
                Some(uploaded.file_digest_sha256.clone())
        }
    }
    expect.inode_upload.fill_from_uploaded(
        &initial.remote_state.namespace_id,
        initial.remote_state.inode_id,
        uploaded,
    );
    fill_remote_state_digests(&mut expect.remote_state, uploaded);
    fill_local_state_digest(&mut expect.local_state, uploaded);
    fill_sync_anchor_digests(&mut expect.sync_anchor, uploaded);
}

fn fill_remote_state_digests(state: &mut RemoteFileStateRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
        None => state.content_manifest_digest = Some(uploaded.content_manifest_digest.clone()),
    }
}

fn fill_local_state_digest(state: &mut LocalFileStateRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
}

fn fill_sync_anchor_digests(state: &mut SyncAnchorRow, uploaded: &UploadedContent) {
    match &state.content_digest {
        Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
        None => state.content_digest = Some(uploaded.file_digest_sha256.clone()),
    }
    match &state.content_manifest_digest {
        Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
        None => state.content_manifest_digest = Some(uploaded.content_manifest_digest.clone()),
    }
}

fn seed_transfer_ledger_row(
    initial: &EditInitialState,
    seed: &FixtureTransferLedgerSeed,
    uploaded: &UploadedContent,
) -> TransferLedgerRow {
    TransferLedgerRow {
        namespace_id: initial.remote_state.namespace_id.clone(),
        inode_id: initial.remote_state.inode_id,
        transfer_id: upload_transfer_id(
            &initial.remote_state.namespace_id,
            initial.remote_state.inode_id,
            &uploaded.content_manifest_digest,
        ),
        direction: seed.direction,
        object_key: uploaded.manifest_object_key.clone(),
        block_index: seed.block_index,
        block_count: seed.block_count,
        state: seed.state,
        updated_at_ms: seed.updated_at_ms,
    }
}

fn seed_uploaded_prefix_for_transfer(
    store: &LocalFsStore,
    source_path: &Path,
    uploaded: &UploadedContent,
    block_index: u64,
) {
    let bytes = fs::read(source_path).expect("read source bytes for transfer seeding");
    for (block_object, block_bytes) in uploaded
        .block_objects
        .iter()
        .zip(bytes.chunks(CONTENT_BLOCK_SIZE_BYTES as usize))
        .take(usize::try_from(block_index).expect("block index should fit"))
    {
        store
            .put_if_absent(&block_object.object_key, block_bytes)
            .expect("seed already-uploaded block object");
    }
}

fn upload_transfer_id(
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    content_manifest_digest: &str,
) -> String {
    format!(
        "upload:{}:{}:{}",
        namespace_id.as_str(),
        inode_id.0,
        content_manifest_digest
    )
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
    #[serde(default)]
    transfer_ledger: Option<FixtureTransferLedgerSeed>,
    head: HeadState,
    lease: LeaseState,
}

#[derive(Debug, Deserialize)]
struct FixtureLocalFile {
    relative_path: String,
    #[serde(default)]
    content_utf8: Option<String>,
    #[serde(default)]
    generated_two_block: Option<GeneratedTwoBlockFile>,
}

impl FixtureLocalFile {
    fn file_bytes(&self) -> Vec<u8> {
        if let Some(content_utf8) = &self.content_utf8 {
            return content_utf8.as_bytes().to_vec();
        }
        let generated = self
            .generated_two_block
            .as_ref()
            .expect("fixture local file should provide content");
        generated.file_bytes()
    }
}

#[derive(Debug, Deserialize)]
struct GeneratedTwoBlockFile {
    first_block_fill_byte: String,
    second_block_utf8: String,
}

impl GeneratedTwoBlockFile {
    fn file_bytes(&self) -> Vec<u8> {
        let first_byte = self
            .first_block_fill_byte
            .as_bytes()
            .first()
            .copied()
            .expect("first_block_fill_byte should not be empty");
        let mut bytes = vec![first_byte; CONTENT_BLOCK_SIZE_BYTES as usize];
        bytes.extend_from_slice(self.second_block_utf8.as_bytes());
        bytes
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureTransferLedgerSeed {
    direction: TransferDirection,
    block_index: u64,
    block_count: u64,
    state: TransferState,
    updated_at_ms: u64,
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
    #[serde(default)]
    pending_request_id: Option<String>,
    request: RawClientMutationRequest,
    mutation_response: RawClientMutationResponse,
    #[serde(default)]
    applied_inode_mutation: Option<AppliedInodeMutation>,
    inode_upload: RawInodeUploadRow,
    remote_state: RemoteFileStateRow,
    local_state: LocalFileStateRow,
    sync_anchor: SyncAnchorRow,
    planned_action_cleared: bool,
    pending_mutation_cleared: bool,
    #[serde(default)]
    transfer_ledger_cleared: bool,
    #[serde(default)]
    upload_reused: bool,
    planner_result: PlannedActionRecord,
}

#[derive(Debug, Deserialize)]
struct EditFailureExpectedState {
    issue: RawUploadFailureIssueExpect,
    planned_action_retained: bool,
}

#[derive(Debug, Deserialize)]
struct EditProgressExpectedState {
    transfer_ledger: FixtureTransferLedgerSeed,
    issue: RawTransferResetIssueExpect,
    planned_action_retained: bool,
    inode_upload_absent: bool,
    pending_inode_mutation_absent: bool,
}

#[derive(Debug, Deserialize)]
struct RawUploadFailureIssueExpect {
    kind: String,
    summary: String,
    failure: String,
}

#[derive(Debug, Deserialize)]
struct RawTransferResetIssueExpect {
    kind: String,
    reason: String,
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
    ReplaceFile {
        replace_file: RawExpectedReplaceFileOp,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct RawExpectedReplaceFileOp {
    inode_id: InodeId,
    base_revision_no: RevisionNo,
    content_manifest_digest: Option<String>,
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
                        content_manifest_digest: replace_file
                            .content_manifest_digest
                            .expect("expected replace_file manifest digest"),
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawClientMutationResponse {
    namespace_id: NamespaceId,
    client_request_id: String,
    committed_seq: ChangeSeq,
    replaced_file: RawReplacedRemoteFile,
}

impl RawClientMutationResponse {
    fn into_response(self) -> ClientMutationResponse {
        ClientMutationResponse {
            namespace_id: self.namespace_id,
            client_request_id: self.client_request_id,
            committed_seq: self.committed_seq,
            created_inode: None,
            replaced_file: Some(self.replaced_file.into_row()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawReplacedRemoteFile {
    inode_id: InodeId,
    inode_kind: loon_types::InodeKind,
    revision_no: RevisionNo,
    content_digest: Option<String>,
}

impl RawReplacedRemoteFile {
    fn into_row(self) -> ReplacedRemoteFile {
        ReplacedRemoteFile {
            inode_id: self.inode_id,
            inode_kind: self.inode_kind,
            revision_no: self.revision_no,
            content_digest: self
                .content_digest
                .expect("expected replaced file content digest"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawInodeUploadRow {
    namespace_id: NamespaceId,
    inode_id: InodeId,
    file_digest_sha256: Option<String>,
    content_manifest_digest: Option<String>,
    manifest_object_key: Option<String>,
    file_size_bytes: u64,
    uploaded_at_ms: u64,
}

impl RawInodeUploadRow {
    fn fill_from_uploaded(
        &mut self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        uploaded: &UploadedContent,
    ) {
        assert_eq!(&self.namespace_id, namespace_id);
        assert_eq!(self.inode_id, inode_id);
        match &self.file_digest_sha256 {
            Some(digest) => assert_eq!(digest, &uploaded.file_digest_sha256),
            None => self.file_digest_sha256 = Some(uploaded.file_digest_sha256.clone()),
        }
        match &self.content_manifest_digest {
            Some(digest) => assert_eq!(digest, &uploaded.content_manifest_digest),
            None => self.content_manifest_digest = Some(uploaded.content_manifest_digest.clone()),
        }
        match &self.manifest_object_key {
            Some(object_key) => assert_eq!(object_key, &uploaded.manifest_object_key),
            None => self.manifest_object_key = Some(uploaded.manifest_object_key.clone()),
        }
        assert_eq!(self.file_size_bytes, uploaded.file_size_bytes);
    }

    fn into_row(self) -> InodeUploadRow {
        InodeUploadRow {
            namespace_id: self.namespace_id,
            inode_id: self.inode_id,
            file_digest_sha256: self
                .file_digest_sha256
                .expect("expected inode upload file digest"),
            content_manifest_digest: self
                .content_manifest_digest
                .expect("expected inode upload manifest digest"),
            manifest_object_key: self
                .manifest_object_key
                .expect("expected inode upload manifest object key"),
            file_size_bytes: self.file_size_bytes,
            uploaded_at_ms: self.uploaded_at_ms,
        }
    }
}

type TestDir = loon_testkit::tempdir::TestDir;
